// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! The `stdio` carrier, driven through its own table the way the host's connector drives it, over a
//! SCRIPTED host I/O table (`abi::host::io`): one frame per line in (a line split across reads, a
//! blank line, the final unterminated line, the line ceiling), one line per frame out (gathered to
//! its end, refused when it cannot be one line), the spawn of exactly the program it is lent, and
//! every op of a role it does not play refused. An integration test, so the crate itself keeps
//! `#![deny(unsafe_code)]`: driving a raw table is the host's side. The real host's I/O is driven by
//! busbar's conformance suite (`tests/conformance.rs`).

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::ffi::c_void;
use std::mem::{size_of, zeroed};
use std::sync::{Mutex, PoisonError};

use busbar_contract::abi::host::io::{
    HandleIn, IoSlots, ReadIn as IoReadIn, SpawnIn, WriteIn as IoWriteIn, SLOTS,
};
use busbar_contract::abi::host::service::ServiceOut;
use busbar_contract::abi::mechanism::call::{
    AbiStr, Blob, InHead, Op, OutHead, Outcome, RawOutcome, BLOB_JSON,
};
use busbar_contract::abi::mechanism::door::Door;
use busbar_contract::abi::mechanism::extensions::declares;
use busbar_contract::abi::mechanism::lifecycle::{
    slot as life, OpenIn, OpenOut, RefreshIn, ReleaseIn, ValidateIn,
};
use busbar_contract::abi::mechanism::ticket::{HostCtx, HostTables, Ticket};
use busbar_contract::abi::transport::check::check_tail;
use busbar_contract::abi::transport::{
    slot, AcceptIn, AcceptOut, AdoptIn, ArrivalIn, ArrivalOut, BeginIn, ConnIn, ConnOut,
    Destination, DialIn, EmitIn, EncodeIn, FinishIn, FramerOut, FramingIn, IngestIn, IoOut,
    ListenIn, ListenOut, LocateIn, LocateOut, Ops, ReadIn, RefuseIn, ShutIn, TransportTail,
    WriteIn, DEST_AUTHORITY, DEST_PROGRAM, READ_END_OF_FRAME, ROLE_CARRIER, SETTING_COUNT,
    UNIT0_FIRST_LINE, WRITE_END_OF_FRAME,
};
use busbar_transport_stdio::door::{
    door, FRAME_TOO_LONG, MAX_LINE_BYTES, NOT_A_COUNT, NOT_ONE_LINE, STATEMENT, TOO_LONG,
};

fn z<T>() -> T {
    // SAFETY: every `in`/`out` here is plain C data; all-zero is a valid value of each.
    unsafe { zeroed() }
}

fn s(t: &str) -> AbiStr {
    AbiStr {
        ptr: t.as_ptr(),
        len: t.len(),
    }
}

/// `json` as a settings blob (`None`: absent, as the host hands a door that declares none).
fn settings(json: Option<&str>) -> Blob {
    let mut b: Blob = z();
    if let Some(j) = json {
        b.ptr = j.as_ptr();
        b.len = j.len();
        b.fmt = BLOB_JSON;
    }
    b
}

/// The request-body bound `n`, as the host hands it in a settings blob.
fn body_max(n: usize) -> String {
    format!("{{\"limits.request_body_max_bytes\":{n}}}")
}

fn text(t: AbiStr) -> String {
    if t.len == 0 {
        return String::new();
    }
    // SAFETY: the carrier's string, live for the call.
    String::from_utf8(unsafe { std::slice::from_raw_parts(t.ptr, t.len) }.to_vec()).unwrap()
}

fn ops() -> &'static Ops {
    let d: *const Door = door();
    // SAFETY: the door answers a `'static` door whose table is this kind's `Ops`.
    unsafe { &*(*d).ops().cast::<Ops>() }
}

fn call<I, O>(op: Option<Op>, inst: *mut c_void, i: &mut I, o: &mut O, index: u32) -> Outcome {
    // SAFETY: `I` leads with an `InHead`, `O` with an `OutHead` (the table's own structs).
    unsafe {
        let ih = std::ptr::from_mut(i).cast::<InHead>();
        (*ih).size = size_of::<I>() as u32;
        (*ih).op = index;
        (*ih).ticket = Ticket {
            slot: 1,
            generation: 1,
        };
        let oh = std::ptr::from_mut(o).cast::<OutHead>();
        (*oh).size = size_of::<O>() as u32;
    }
    (op.expect("every slot is filled"))(
        inst,
        std::ptr::from_ref(i).cast(),
        std::ptr::from_mut(o).cast(),
    )
    .outcome()
}

// ── the allocation witness ───────────────────────────────────────────────────────────────────────

thread_local! {
    /// Whether this thread's allocations are being counted.
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    /// How many allocations this thread made while counting.
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
}

/// The system allocator, counting the allocations a test thread makes while it is armed.
struct Witness;

impl Witness {
    fn count() {
        if COUNTING.try_with(Cell::get).unwrap_or(false) {
            let _ = ALLOCATIONS.try_with(|n| n.set(n.get() + 1));
        }
    }
}

// SAFETY: every call is forwarded to `System` unchanged; the count touches only const thread-locals.
unsafe impl GlobalAlloc for Witness {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        Self::count();
        // SAFETY: the caller's contract, forwarded.
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        Self::count();
        // SAFETY: the caller's contract, forwarded.
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        Self::count();
        // SAFETY: the caller's contract, forwarded.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the caller's contract, forwarded.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static WITNESS: Witness = Witness;

/// How many allocations `f` makes on this thread.
fn allocations<R>(f: impl FnOnce() -> R) -> (R, u64) {
    ALLOCATIONS.with(|n| n.set(0));
    COUNTING.with(|c| c.set(true));
    let r = f();
    COUNTING.with(|c| c.set(false));
    (r, ALLOCATIONS.with(Cell::get))
}

// ── the scripted host ────────────────────────────────────────────────────────────────────────────

#[derive(Default)]
struct Script {
    /// The program `io.spawn` was asked for, with its arguments.
    spawned: Vec<(String, Vec<String>)>,
    /// What `io.read` answers next, in order: `None` = PENDING; an empty chunk = the end.
    reads: Vec<Option<Vec<u8>>>,
    /// Every byte `io.write` took.
    written: Vec<u8>,
    /// Handles closed.
    closed: Vec<u64>,
    /// `Some(k)`: the pipe TRICKLES, taking at most `k` bytes on one `io.write` and answering
    /// PENDING on the next. `None`: it takes everything.
    trickle: Option<usize>,
    /// Whether the next trickling `io.write` answers PENDING.
    pend_next: bool,
}

static SCRIPT: Mutex<Option<Script>> = Mutex::new(None);
static ONE: Mutex<()> = Mutex::new(());

fn with<R>(f: impl FnOnce(&mut Script) -> R) -> R {
    f(SCRIPT
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get_or_insert_with(Script::default))
}

fn answer(out: *mut ServiceOut, o: Outcome, value: u64, len: u64) -> RawOutcome {
    // SAFETY: the carrier's `out`, live for the call.
    unsafe {
        (*out).outcome = RawOutcome::of(o);
        (*out).value = value;
        (*out).len = len;
    }
    RawOutcome::of(o)
}

extern "C" fn io_spawn(_: HostCtx, input: *const c_void, out: *mut ServiceOut) -> RawOutcome {
    // SAFETY: the carrier's `in`.
    let i = unsafe { input.cast::<SpawnIn>().read() };
    // SAFETY: the host-lent list the carrier passed through.
    let args = unsafe { std::slice::from_raw_parts(i.args, i.args_len) };
    let args = args.iter().map(|a| text(*a)).collect();
    with(|sc| sc.spawned.push((text(i.program), args)));
    answer(out, Outcome::Ready, 7, 0)
}

extern "C" fn io_read(_: HostCtx, input: *const c_void, out: *mut ServiceOut) -> RawOutcome {
    // SAFETY: the carrier's `in`.
    let i = unsafe { input.cast::<IoReadIn>().read() };
    match with(|sc| (!sc.reads.is_empty()).then(|| sc.reads.remove(0))) {
        None | Some(None) => answer(out, Outcome::Pending, 0, 0),
        Some(Some(bytes)) => {
            assert!(bytes.len() <= i.cap);
            // SAFETY: the carrier's buffer, `cap` bytes.
            unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), i.buf, bytes.len()) };
            answer(out, Outcome::Ready, 0, bytes.len() as u64)
        }
    }
}

extern "C" fn io_write(_: HostCtx, input: *const c_void, out: *mut ServiceOut) -> RawOutcome {
    // SAFETY: the carrier's `in`.
    let i = unsafe { input.cast::<IoWriteIn>().read() };
    // SAFETY: the carrier's bytes.
    let bytes = unsafe { std::slice::from_raw_parts(i.bytes, i.len) };
    let taken = with(|sc| {
        let Some(k) = sc.trickle else {
            sc.written.extend_from_slice(bytes);
            return Some(bytes.len());
        };
        sc.pend_next = !sc.pend_next;
        if sc.pend_next {
            let n = k.min(bytes.len());
            sc.written.extend_from_slice(&bytes[..n]);
            Some(n)
        } else {
            None
        }
    });
    match taken {
        Some(n) => answer(out, Outcome::Ready, 0, n as u64),
        None => answer(out, Outcome::Pending, 0, 0),
    }
}

extern "C" fn io_close(_: HostCtx, input: *const c_void, out: *mut ServiceOut) -> RawOutcome {
    // SAFETY: the carrier's `in`.
    let i = unsafe { input.cast::<HandleIn>().read() };
    with(|sc| sc.closed.push(i.handle));
    answer(out, Outcome::Ready, 0, 0)
}

static IO: IoSlots = IoSlots {
    size: size_of::<IoSlots>() as u32,
    slots: SLOTS,
    open: None,
    listen: None,
    accept: None,
    read: Some(io_read),
    write: Some(io_write),
    ready: None,
    shut: None,
    close: Some(io_close),
    spawn: Some(io_spawn),
    ends: None,
};

struct Host {
    inst: *mut c_void,
    _tables: Box<HostTables>,
    conn: u64,
}

impl Host {
    fn new(reads: Vec<Option<Vec<u8>>>) -> Self {
        Self::with_settings(None, reads)
    }

    /// The carrier opened with the settings blob `json`, its program dialed.
    fn with_settings(json: Option<&str>, reads: Vec<Option<Vec<u8>>>) -> Self {
        *SCRIPT.lock().unwrap_or_else(PoisonError::into_inner) = Some(Script {
            reads,
            ..Script::default()
        });
        let tables = Box::new(HostTables {
            size: size_of::<HostTables>() as u32,
            _reserved: 0,
            ctx: HostCtx::NONE,
            wake: None,
            conns: std::ptr::null(),
            services: std::ptr::null(),
            io: &IO,
        });
        let mut i: OpenIn = z();
        i.host = &*tables;
        i.settings = settings(json);
        let mut o: OpenOut = z();
        let r = call(
            ops().head.open,
            std::ptr::null_mut(),
            &mut i,
            &mut o,
            life::OPEN,
        );
        assert_eq!(r, Outcome::Ready);
        let args = [s("--stdio")];
        let mut dest: Destination = z();
        dest.kind = DEST_PROGRAM;
        dest.program = s("/usr/bin/server");
        dest.args = args.as_ptr();
        dest.args_len = args.len();
        let mut di: DialIn = z();
        di.dest = &dest;
        let mut co: ConnOut = z();
        assert_eq!(
            call(ops().dial, o.instance, &mut di, &mut co, slot::DIAL),
            Outcome::Ready
        );
        Self {
            inst: o.instance,
            _tables: tables,
            conn: co.conn,
        }
    }

    fn read(&self, cap: usize) -> (Outcome, Vec<u8>, bool, String) {
        let mut buf = vec![0_u8; cap];
        let mut i: ReadIn = z();
        i.conn = self.conn;
        i.buf = buf.as_mut_ptr();
        i.cap = cap;
        let mut o: IoOut = z();
        let r = call(ops().read, self.inst, &mut i, &mut o, slot::READ);
        buf.truncate(o.len as usize);
        (r, buf, o.flags & READ_END_OF_FRAME != 0, text(o.head.error))
    }

    /// A read into the caller's buffer: the outcome, the bytes answered and the frame bit, and no
    /// allocation of its own.
    fn read_into(&self, buf: &mut [u8]) -> (Outcome, usize, bool) {
        let mut i: ReadIn = z();
        i.conn = self.conn;
        i.buf = buf.as_mut_ptr();
        i.cap = buf.len();
        let mut o: IoOut = z();
        let r = call(ops().read, self.inst, &mut i, &mut o, slot::READ);
        (r, o.len as usize, o.flags & READ_END_OF_FRAME != 0)
    }

    /// A write that ends its frame, with no allocation of its own.
    fn write_frame(&self, bytes: &[u8]) -> Outcome {
        let mut i: WriteIn = z();
        i.conn = self.conn;
        i.bytes = bytes.as_ptr();
        i.len = bytes.len();
        i.flags = WRITE_END_OF_FRAME;
        let mut o: IoOut = z();
        call(ops().write, self.inst, &mut i, &mut o, slot::WRITE)
    }

    /// Refresh the instance onto the settings blob `json`.
    fn refresh(&self, json: &str) -> Outcome {
        let mut i: RefreshIn = z();
        i.generation = 2;
        i.settings = settings(Some(json));
        let mut o: OutHead = z();
        call(ops().head.refresh, self.inst, &mut i, &mut o, life::REFRESH)
    }

    fn write(&self, bytes: &[u8], end: bool) -> (Outcome, String) {
        let mut i: WriteIn = z();
        i.conn = self.conn;
        i.bytes = bytes.as_ptr();
        i.len = bytes.len();
        i.flags = if end { WRITE_END_OF_FRAME } else { 0 };
        let mut o: IoOut = z();
        let r = call(ops().write, self.inst, &mut i, &mut o, slot::WRITE);
        (r, text(o.head.error))
    }
}

// ── the tests ────────────────────────────────────────────────────────────────────────────────────

#[test]
fn the_tail_is_a_carrier_composing_over_nothing() {
    // SAFETY: the Statement's kind tail is this crate's `'static` `TransportTail`.
    let tail = unsafe { &*STATEMENT.kind_tail.cast::<TransportTail>() };
    assert_eq!(tail.role, ROLE_CARRIER);
    assert_eq!(tail.composes_over_len, 0);
    assert_eq!(check_tail(tail), Ok(()));
}

/// The claim row states the facts the root reads for a stdio session: a session, bound to its
/// principal, whose first unit opens on its first line, and no selector forms; and the linked
/// entry's `SESSION` says the same.
#[test]
fn the_claim_row_states_a_line_session_and_the_linked_entry_agrees() {
    // SAFETY: the Statement's kind tail is this crate's `'static` `TransportTail`.
    let tail = unsafe { &*STATEMENT.kind_tail.cast::<TransportTail>() };
    assert_eq!(tail.claim_rows_len, 1, "one claim row: stdio");
    // SAFETY: the tail's own `'static` list, `claim_rows_len` long.
    let claim = unsafe { &*tail.claim_rows };
    assert_eq!(claim.session, 1, "it carries sessions");
    assert_eq!(claim.session_bound, 1, "a session caches its principal");
    assert_eq!(
        claim.unit0_trigger, UNIT0_FIRST_LINE,
        "the first unit opens on its first line"
    );
    assert_eq!(text(claim.selector_forms), "", "no selector forms");
    assert_eq!(
        text(claim.egress_selector_forms),
        "",
        "no outbound selector forms"
    );
    assert_eq!(
        busbar_transport_stdio::linked::SESSION,
        claim.session == 1,
        "linked::SESSION agrees with the claim row"
    );
}

#[test]
fn a_dial_spawns_exactly_the_program_it_is_lent() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let _h = Host::new(Vec::new());
    with(|sc| {
        assert_eq!(
            sc.spawned,
            [("/usr/bin/server".to_owned(), vec!["--stdio".to_owned()])]
        );
    });
}

#[test]
fn each_line_is_one_frame_whatever_the_reads_cut() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let h = Host::new(vec![
        Some(b"{\"a\":".to_vec()),
        None,
        Some(b"1}\r\n\nsecond".to_vec()),
        Some(b" line\nlast".to_vec()),
        Some(Vec::new()),
    ]);
    // Half a line, then nothing ready: PENDING, nothing moved.
    assert_eq!(h.read(64).0, Outcome::Pending);
    assert_eq!(
        h.read(64),
        (Outcome::Ready, b"{\"a\":1}".to_vec(), true, String::new())
    );
    // A blank line is an empty frame.
    assert_eq!(
        h.read(64),
        (Outcome::Ready, Vec::new(), true, String::new())
    );
    // A line longer than the host's buffer comes in several reads; the last carries the frame.
    assert_eq!(
        h.read(4),
        (Outcome::Ready, b"seco".to_vec(), false, String::new())
    );
    assert_eq!(
        h.read(64),
        (Outcome::Ready, b"nd line".to_vec(), true, String::new())
    );
    // The final unterminated line is a frame at the end, then the clean end.
    assert_eq!(
        h.read(64),
        (Outcome::Ready, b"last".to_vec(), true, String::new())
    );
    assert_eq!(
        h.read(64),
        (Outcome::Ready, Vec::new(), false, String::new())
    );
}

#[test]
fn a_line_past_the_ceiling_fails_the_read() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let long = vec![b'x'; MAX_LINE_BYTES + 1];
    let mut reads: Vec<Option<Vec<u8>>> =
        long.chunks(16 * 1024).map(|c| Some(c.to_vec())).collect();
    reads.push(None);
    let h = Host::new(reads);
    let (r, _, _, why) = h.read(64);
    assert_eq!((r, why.as_str()), (Outcome::Failed, TOO_LONG));
}

#[test]
fn a_frame_goes_out_as_one_line_and_one_that_cannot_be_one_is_refused() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let h = Host::new(Vec::new());
    assert_eq!(h.write(b"{\"id\":", false).0, Outcome::Ready);
    with(|sc| {
        assert!(
            sc.written.is_empty(),
            "nothing leaves before the frame ends"
        )
    });
    assert_eq!(h.write(b"1}", true).0, Outcome::Ready);
    assert_eq!(h.write(b"", true).0, Outcome::Ready);
    with(|sc| assert_eq!(sc.written, b"{\"id\":1}\n\n"));
    assert_eq!(
        h.write(b"two\nlines", true),
        (Outcome::Failed, NOT_ONE_LINE.to_owned())
    );
    assert_eq!(
        h.write(b"a return\r", true),
        (Outcome::Failed, NOT_ONE_LINE.to_owned())
    );
    with(|sc| {
        assert_eq!(
            sc.written, b"{\"id\":1}\n\n",
            "a refused frame writes nothing"
        )
    });
    let mut i: ConnIn = z();
    i.conn = h.conn;
    let mut o: OutHead = z();
    assert_eq!(
        call(ops().flush, h.inst, &mut i, &mut o, slot::FLUSH),
        Outcome::Ready
    );
}

/// A frame refused while it was gathered (it holds a newline, or it ran past the ceiling) leaves
/// nothing gathered: the next good frame goes out as its own line, no refused byte before it.
#[test]
fn a_refused_frame_leaves_nothing_gathered_for_the_next() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let h = Host::with_settings(Some(&body_max(64)), Vec::new());
    assert_eq!(h.write(b"two\n", false).0, Outcome::Ready);
    assert_eq!(
        h.write(b"lines", true),
        (Outcome::Failed, NOT_ONE_LINE.to_owned())
    );
    assert_eq!(h.write(b"first", true).0, Outcome::Ready);
    assert_eq!(h.write(&[b'x'; 40], false).0, Outcome::Ready);
    assert_eq!(
        h.write(&[b'y'; 40], false),
        (Outcome::Failed, FRAME_TOO_LONG.to_owned())
    );
    assert_eq!(h.write(b"second", true).0, Outcome::Ready);
    with(|sc| assert_eq!(sc.written, b"first\nsecond\n"));
}

#[test]
fn shut_closes_the_program_and_an_authority_or_a_listen_is_refused() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let h = Host::new(Vec::new());
    let mut i: ShutIn = z();
    i.conn = h.conn;
    let mut o: OutHead = z();
    assert_eq!(
        call(ops().shut, h.inst, &mut i, &mut o, slot::SHUT),
        Outcome::Ready
    );
    with(|sc| assert_eq!(sc.closed, [7]));
    assert_eq!(
        call(ops().shut, h.inst, &mut i, &mut o, slot::SHUT),
        Outcome::Ready
    );

    let mut dest: Destination = z();
    dest.kind = DEST_AUTHORITY;
    dest.authority = s("127.0.0.1:1");
    let mut di: DialIn = z();
    di.dest = &dest;
    let mut co: ConnOut = z();
    assert_eq!(
        call(ops().dial, h.inst, &mut di, &mut co, slot::DIAL),
        Outcome::Refused
    );
    let mut li: ListenIn = z();
    let mut lo: ListenOut = z();
    assert_eq!(
        call(ops().listen, h.inst, &mut li, &mut lo, slot::LISTEN),
        Outcome::Refused
    );
}

/// THE DATA PATH ALLOCATES NOTHING once a connection's buffers are warm (`BUSBAR-1.6.0.md` §11.2,
/// "no allocation and no blocking on the hot path"): a line already read from the pipe is answered
/// straight out of the carrier's buffer, and a whole frame is queued as its line without a fresh
/// buffer per frame.
#[test]
fn a_buffered_line_and_a_whole_frame_allocate_nothing() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let h = Host::new(vec![
        Some(b"the first line, which warms the buffer\n{\"id\":2}\nthird\n".to_vec()),
        None,
    ]);
    with(|sc| sc.written.reserve(4096));
    let mut buf = [0_u8; 256];
    assert_eq!(h.read_into(&mut buf), (Outcome::Ready, 38, true));
    assert_eq!(
        h.write_frame(b"a frame longer than the one measured, which warms the queue"),
        Outcome::Ready
    );

    let (read, n) = allocations(|| h.read_into(&mut buf));
    assert_eq!(read, (Outcome::Ready, 8, true));
    assert_eq!(&buf[..8], b"{\"id\":2}");
    assert_eq!(n, 0, "reading a line already buffered allocated {n} times");

    let (wrote, n) = allocations(|| h.write_frame(b"{\"id\":3}"));
    assert_eq!(wrote, Outcome::Ready);
    assert_eq!(n, 0, "writing a whole frame allocated {n} times");
    with(|sc| assert!(sc.written.ends_with(b"{\"id\":3}\n")));
}

/// A LINE OF 100 KiB IS ONE FRAME: the carrier's line bound is the host's own bound on one
/// inbound frame of a byte channel (the request-body bound), not a 64 KiB cursor budget, so a
/// stdio program's large message (a long tool list, a resource read carrying base64) is read whole.
#[test]
fn a_line_of_a_hundred_kib_is_one_frame() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let mut line = vec![b'x'; 100 * 1024];
    line.push(b'\n');
    let mut reads: Vec<Option<Vec<u8>>> =
        line.chunks(16 * 1024).map(|c| Some(c.to_vec())).collect();
    reads.push(None);
    let h = Host::new(reads);
    let (r, bytes, end, why) = h.read(200 * 1024);
    assert_eq!(
        (r, bytes.len(), end, why.as_str()),
        (Outcome::Ready, 100 * 1024, true, ""),
        "a 100 KiB line must be read as one frame"
    );
}

/// A FRAME IS BOUNDED LIKE A LINE: the bytes gathered for one outbound frame stop at the line
/// ceiling ([`MAX_LINE_BYTES`]), as an inbound line does; past it the write FAILS, not gathered
/// without bound. RED (Q128 transport-stdio NIT): the gathered frame had no ceiling. FAILED, as a
/// frame that cannot be one line is: one outcome class for every frame the carrier will not carry,
/// the class the contract's carrier conformance script states for it (`Want::Starts("Failed ")`).
/// RED (row 159, stdio NIT): this one was REFUSED while the newline's FAILED.
#[test]
fn a_gathered_frame_past_the_line_ceiling_fails() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let h = Host::new(Vec::new());
    let chunk = vec![b'x'; 1024 * 1024];
    let mut answered = Outcome::Ready;
    for _ in 0..=MAX_LINE_BYTES / chunk.len() {
        answered = h.write(&chunk, false).0;
        if answered != Outcome::Ready {
            break;
        }
    }
    assert_eq!(answered, Outcome::Failed, "a frame past the ceiling fails");
}

/// THE LINE CEILING IS THE OPERATOR'S REQUEST-BODY BOUND: the tail declares the one setting the
/// carrier reads, at its 1.5.5 path (`limits.request_body_max_bytes`), a count whose declared
/// default is the default ceiling. RED (row 159, stdio MEDIUM M2): the carrier read no setting.
#[test]
fn the_tail_declares_the_request_body_bound_it_reads() {
    // SAFETY: the Statement's kind tail is this crate's `'static` `TransportTail`.
    let tail = unsafe { &*STATEMENT.kind_tail.cast::<TransportTail>() };
    assert_eq!(tail.settings_len, 1, "one setting declared");
    // SAFETY: the tail's own `'static` list, `settings_len` long.
    let decl = unsafe { &*tail.settings };
    assert_eq!(text(decl.path), "limits.request_body_max_bytes");
    assert_eq!(decl.kind, SETTING_COUNT);
    assert_eq!(text(decl.default), MAX_LINE_BYTES.to_string());
    assert_eq!(check_tail(tail), Ok(()));
}

/// A deployment's request-body bound is the line ceiling both ways: a line or a frame of exactly
/// the bound crosses, one byte more fails the read or the write. RED (row 159, stdio
/// MEDIUM M2): the ceiling was a fixed 32 MiB whatever the deployment set.
#[test]
fn the_line_ceiling_is_the_deployments_request_body_bound() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let mut out = vec![b'a'; 64];
    out.push(b'\n');
    out.extend_from_slice(&[b'b'; 65]);
    out.push(b'\n');
    let h = Host::with_settings(Some(&body_max(64)), vec![Some(out), None]);
    assert_eq!(
        h.read(256),
        (Outcome::Ready, vec![b'a'; 64], true, String::new()),
        "a line of exactly the bound is one frame"
    );
    let (r, _, _, why) = h.read(256);
    assert_eq!((r, why.as_str()), (Outcome::Failed, TOO_LONG));
    assert_eq!(h.write(&[b'c'; 64], true).0, Outcome::Ready);
    assert_eq!(
        h.write(&[b'd'; 65], true).0,
        Outcome::Failed,
        "a frame one byte past the bound"
    );
    with(|sc| assert_eq!(sc.written.len(), 65, "only the frame inside the bound left"));
}

/// A deployment that RAISED the bound past the default carries a line past the default whole.
/// RED (row 159, stdio MEDIUM M2): a line the deployment's bound admits failed the read.
#[test]
fn a_raised_request_body_bound_carries_a_line_past_the_default() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let mut line = vec![b'x'; MAX_LINE_BYTES + 1];
    line.push(b'\n');
    let mut reads: Vec<Option<Vec<u8>>> =
        line.chunks(16 * 1024).map(|c| Some(c.to_vec())).collect();
    reads.push(None);
    let h = Host::with_settings(Some(&body_max(MAX_LINE_BYTES + 1024)), reads);
    let (r, bytes, end, why) = h.read(64);
    assert_eq!(
        (r, bytes.len(), end, why.as_str()),
        (Outcome::Ready, 64, false, ""),
        "the line is a frame, answered in pieces"
    );
}

/// A config apply moves the ceiling: the refresh's bound holds from the next op on. RED (row 159,
/// stdio MEDIUM M2).
#[test]
fn a_refresh_moves_the_line_ceiling() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let h = Host::new(Vec::new());
    assert_eq!(h.refresh(&body_max(64)), Outcome::Ready);
    assert_eq!(
        h.write(&[b'x'; 100], true).0,
        Outcome::Failed,
        "the refreshed bound holds"
    );
}

/// A bound that is no count of bytes is FAILED at `validate`, before any instance opens on it:
/// a string, a negative number, a fraction and a null alike. RED (row 159, stdio MEDIUM M2): the
/// carrier validated nothing.
#[test]
fn a_request_body_bound_that_is_no_count_fails_validation() {
    for bound in ["\"big\"", "-1", "1.5", "null"] {
        let json = format!("{{\"limits.request_body_max_bytes\":{bound}}}");
        let mut i: ValidateIn = z();
        i.settings = settings(Some(&json));
        let mut o: OutHead = z();
        assert_eq!(
            call(
                ops().head.validate,
                std::ptr::null_mut(),
                &mut i,
                &mut o,
                life::VALIDATE
            ),
            Outcome::Failed,
            "{bound} is no count"
        );
        assert_eq!(text(o.error), NOT_A_COUNT, "{bound} is no count");
    }
}

/// An op on a connection that is gone FAILS: after the shut, a read and a write find no such
/// connection (not a clean end, not a taken frame).
#[test]
fn a_read_or_write_on_a_shut_connection_fails() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let h = Host::new(Vec::new());
    let mut i: ShutIn = z();
    i.conn = h.conn;
    let mut o: OutHead = z();
    assert_eq!(
        call(ops().shut, h.inst, &mut i, &mut o, slot::SHUT),
        Outcome::Ready
    );
    let (r, _, _, why) = h.read(64);
    assert_eq!(
        (r, why.as_str()),
        (Outcome::Failed, "read: no such connection"),
        "read"
    );
    let (r, why) = h.write(b"x", true);
    assert_eq!(
        (r, why.as_str()),
        (Outcome::Failed, "write: no such connection"),
        "write"
    );
}

/// `arrival` answers READY for a live connection and FAILS for one the carrier never made.
#[test]
fn arrival_answers_for_a_live_connection_only() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let h = Host::new(Vec::new());
    let mut peer = [0_u8; 16];
    let mut arrival = |conn: u64| {
        let mut i: ArrivalIn = z();
        i.conn = conn;
        i.peer_buf = peer.as_mut_ptr();
        i.peer_cap = peer.len();
        let mut o: ArrivalOut = z();
        call(ops().arrival, h.inst, &mut i, &mut o, slot::ARRIVAL)
    };
    assert_eq!(arrival(h.conn), Outcome::Ready, "a live connection");
    assert_eq!(arrival(h.conn + 1000), Outcome::Failed, "an unknown one");
}

/// A line of exactly the ceiling ended by CRLF is one frame whichever read its CR and its LF
/// arrive in: a CR still waiting for its LF is the terminator's, not the line's. RED (row 159,
/// stdio LOW): the read failed when the CR and the LF came in separate reads.
#[test]
fn a_crlf_line_at_the_ceiling_is_one_frame_however_its_reads_split() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let line = vec![b'x'; MAX_LINE_BYTES];
    let mut reads: Vec<Option<Vec<u8>>> =
        line.chunks(16 * 1024).map(|c| Some(c.to_vec())).collect();
    reads.push(Some(b"\r".to_vec()));
    reads.push(Some(b"\n".to_vec()));
    reads.push(None);
    let h = Host::new(reads);
    let (r, bytes, end, why) = h.read(64);
    assert_eq!(
        (r, bytes.len(), end, why.as_str()),
        (Outcome::Ready, 64, false, ""),
        "a line at the ceiling is a frame"
    );
}

/// A SLOW READER'S QUEUE IS BOUNDED: a program that reads stdin slower than frames arrive leaves the
/// pipe never fully drained when the next frame is written, yet the bytes the pipe took leave the
/// queue, so it holds a working size (not every byte ever written) and the data path allocates
/// nothing once the queue sits at its cap. Every byte still arrives, each frame as one line, in order.
/// RED (codeaudit 0aa4f6d, stdio MEDIUM): the written prefix stayed queued until the pipe took
/// everything, so a slow reader's queue grew by every byte written.
#[test]
fn a_program_that_reads_slower_than_frames_arrive_holds_a_bounded_queue() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    const FRAME: usize = 1000;
    const WARM: usize = 3000;
    const WINDOW: usize = 6000;
    let frames: Vec<Vec<u8>> = (0..WARM + WINDOW)
        .map(|n| vec![b'a' + (n % 26) as u8; FRAME])
        .collect();
    let h = Host::new(Vec::new());
    with(|sc| {
        sc.written.reserve(2 * (WARM + WINDOW) * (FRAME + 1));
        // Each write op takes less than the 1001 bytes a frame adds.
        sc.trickle = Some(400);
    });
    // The host offers the frame again while the write answers PENDING.
    let send = |frame: &[u8]| {
        while h.write_frame(frame) == Outcome::Pending {}
    };
    for frame in &frames[..WARM] {
        send(frame);
    }
    let ((), n) = allocations(|| {
        for frame in &frames[WARM..] {
            send(frame);
        }
    });
    assert_eq!(n, 0, "writing to a slow reader allocated {n} times");

    // The pipe speeds up: everything queued drains, and all of it is the frames, in order.
    with(|sc| sc.trickle = None);
    let mut i: ConnIn = z();
    i.conn = h.conn;
    let mut o: OutHead = z();
    while call(ops().flush, h.inst, &mut i, &mut o, slot::FLUSH) == Outcome::Pending {}
    let want: Vec<u8> = frames
        .iter()
        .flat_map(|f| f.iter().copied().chain(std::iter::once(b'\n')))
        .collect();
    with(|sc| {
        assert!(
            sc.written == want,
            "the bytes the program got are the frames"
        )
    });
}

// ── sibling canon: settings, role stubs, prefixes, close ────────────────────────────────────────

fn validate_text(json: &str) -> (Outcome, String) {
    let mut reason = [0_u8; 256];
    let mut i: ValidateIn = z();
    i.err_buf = reason.as_mut_ptr();
    i.err_cap = reason.len();
    i.settings = settings(Some(json));
    let mut o: OutHead = z();
    let r = call(
        ops().head.validate,
        std::ptr::null_mut(),
        &mut i,
        &mut o,
        life::VALIDATE,
    );
    (r, text(o.error))
}

#[test]
fn settings_refuse_undeclared_keys_zero_and_bad_counts_with_the_canonical_texts() {
    assert_eq!(
        validate_text("{\"nope\":1}"),
        (
            Outcome::Failed,
            "settings: nope is not a setting this transport reads".to_owned()
        )
    );
    let above_0 = "settings: limits.request_body_max_bytes must be a count of bytes above 0";
    for bound in ["0", "\"big\"", "-1", "1.5", "null"] {
        assert_eq!(
            validate_text(&format!("{{\"limits.request_body_max_bytes\":{bound}}}")),
            (Outcome::Failed, above_0.to_owned()),
            "{bound}"
        );
    }
    assert_eq!(validate_text("[]").1, "settings: must be a JSON object");
    assert_eq!(validate_text("{}").0, Outcome::Ready);
    assert_eq!(validate_text(&body_max(5)).0, Outcome::Ready);
}

#[test]
fn open_and_refresh_refuse_undeclared_keys() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let h = Host::new(Vec::new());
    assert_eq!(h.refresh("{\"nope\":true}"), Outcome::Failed);
    assert_eq!(
        h.refresh("{\"limits.request_body_max_bytes\":0}"),
        Outcome::Failed
    );
    let mut i: OpenIn = z();
    i.settings = settings(Some("{\"nope\":1}"));
    let mut o: OpenOut = z();
    assert_eq!(
        call(
            ops().head.open,
            std::ptr::null_mut(),
            &mut i,
            &mut o,
            life::OPEN
        ),
        Outcome::Failed
    );
}

/// Each role stub answers REFUSED with its own op's text.
#[test]
fn every_role_stub_is_refused_with_its_own_op_text() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let h = Host::new(Vec::new());
    macro_rules! stub {
        ($field:ident, $idx:ident, $in:ty, $out:ty, $name:literal) => {{
            let mut i: $in = z();
            let mut o: $out = z();
            let r = call(ops().$field, h.inst, &mut i, &mut o, slot::$idx);
            assert_eq!(
                (r, text(o.head.error)),
                (
                    Outcome::Refused,
                    concat!($name, ": the stdio carrier does not serve this op").to_owned()
                ),
                $name
            );
        }};
    }
    stub!(listen, LISTEN, ListenIn, ListenOut, "listen");
    stub!(accept, ACCEPT, AcceptIn, AcceptOut, "accept");
    stub!(locate, LOCATE, LocateIn, LocateOut, "locate");
    stub!(begin, BEGIN, BeginIn, FramerOut, "begin");
    stub!(ingest, INGEST, IngestIn, FramerOut, "ingest");
    stub!(emit, EMIT, EmitIn, FramerOut, "emit");
    stub!(encode, ENCODE, EncodeIn, FramerOut, "encode");
    stub!(refuse, REFUSE, RefuseIn, FramerOut, "refuse");
    stub!(finish, FINISH, FinishIn, FramerOut, "finish");
    stub!(detach, DETACH, FramingIn, FramerOut, "detach");
    stub!(adopt, ADOPT, AdoptIn, FramerOut, "adopt");
    stub!(timer, TIMER, FramingIn, FramerOut, "timer");
}

#[test]
fn an_op_with_no_open_instance_fails_with_its_prefix() {
    let mut i: ConnIn = z();
    let mut o: OutHead = z();
    let r = call(
        ops().flush,
        std::ptr::null_mut(),
        &mut i,
        &mut o,
        slot::FLUSH,
    );
    assert_eq!(
        (r, text(o.error)),
        (Outcome::Failed, "flush: no open instance".to_owned())
    );
}

#[test]
fn line_failure_texts_carry_the_op_prefix() {
    assert!(TOO_LONG.starts_with("read: "), "{TOO_LONG}");
    assert!(busbar_transport_stdio::door::FRAME_TOO_LONG.starts_with("write: "));
    assert!(NOT_ONE_LINE.starts_with("write: "));
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let h = Host::new(Vec::new());
    let mut i: ConnIn = z();
    i.conn = h.conn + 99;
    let mut o: OutHead = z();
    assert_eq!(
        call(ops().flush, h.inst, &mut i, &mut o, slot::FLUSH),
        Outcome::Failed
    );
    assert_eq!(text(o.error), "flush: no such connection");
}

/// Instance close closes every connection handle still held (the program is killed with it).
#[test]
fn instance_close_closes_every_held_connection() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let h = Host::new(Vec::new());
    with(|sc| assert!(sc.closed.is_empty()));
    let mut i: InHead = z();
    let mut o: OutHead = z();
    assert_eq!(
        call(ops().head.close, h.inst, &mut i, &mut o, life::CLOSE),
        Outcome::Ready
    );
    with(|sc| assert_eq!(sc.closed, [7], "the held program handle was closed"));
}

#[test]
fn the_statement_carries_the_declares_document() {
    let ext = STATEMENT.extensions;
    // SAFETY: the Statement's own `'static` extensions blob, `len` bytes.
    let bytes = unsafe { std::slice::from_raw_parts(ext.ptr, ext.len) };
    assert_eq!(
        declares(bytes),
        Some(&include_bytes!("../declares.json")[..]),
        "declares.json rides the Statement, whole, under its key"
    );
}

/// `arrival` with no open instance says so, in the op's own words (the same as tcp's), and an
/// unknown connection on an open instance keeps `no such connection`.
#[test]
fn arrival_with_no_instance_says_no_open_instance() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let mut i: ArrivalIn = z();
    let mut o: ArrivalOut = z();
    assert_eq!(
        call(
            ops().arrival,
            std::ptr::null_mut(),
            &mut i,
            &mut o,
            slot::ARRIVAL
        ),
        Outcome::Failed
    );
    assert_eq!(text(o.head.error), "arrival: no open instance");
    let h = Host::new(Vec::new());
    let mut i: ArrivalIn = z();
    i.conn = h.conn + 1000;
    let mut o: ArrivalOut = z();
    assert_eq!(
        call(ops().arrival, h.inst, &mut i, &mut o, slot::ARRIVAL),
        Outcome::Failed
    );
    assert_eq!(text(o.head.error), "arrival: no such connection");
}

/// Releasing a lease this carrier never handed out is REFUSED in the SDK's words.
#[test]
fn releasing_a_lease_never_handed_out_is_refused() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let h = Host::new(Vec::new());
    let mut i: ReleaseIn = z();
    i.lease = 7;
    let mut o: OutHead = z();
    assert_eq!(
        call(ops().head.release, h.inst, &mut i, &mut o, life::RELEASE),
        Outcome::Refused
    );
    assert_eq!(text(o.error), "release: no such lease");
}

/// A carrier states no op facts: the bits are framer terms.
#[test]
fn the_carrier_states_no_op_facts() {
    // SAFETY: the Statement's kind tail is this crate's `'static` `TransportTail`.
    let tail = unsafe { &*STATEMENT.kind_tail.cast::<TransportTail>() };
    assert_eq!(tail.facts, 0);
}
