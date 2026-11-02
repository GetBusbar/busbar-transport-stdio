// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! The stdio transport owns its program supervisor, driven
//! through the carrier's own table over a SCRIPTED host: its I/O (a spawn that answers or fails, a
//! read that answers the program's end or PENDING while it runs) and its one clock (`clock.now`,
//! monotonic, set by the test). Its compatible crash-loop policy (the connector's
//! `program.rs`, the MCP-COMPAT baseline): a program that ends is held down under a backoff
//! doubling from 100 ms, and five ends inside a minute stop its restarts until its member is
//! declared anew. A dial the policy holds down is REFUSED.
//!
//! The policy is kept per SUPERVISION KEY (`DialIn::supervision_key`), the member identity the host
//! mints, so:
//! (i) two members running one identical program trip independently;
//! (ii) a member dropped and re-added is a new key, fresh;
//! (iii) a rotated secret value (a different environment value under the same member) does NOT
//!      reset a trip.
//! A clean shut of a running program is not an end; a key of `0` (no member) is not supervised. A
//! read the program fails (a line past the ceiling) is an end, as predev counted every end of a
//! generation the member did not ask for, failures included.
//!

use std::collections::VecDeque;
use std::ffi::c_void;
use std::mem::{size_of, zeroed};
use std::sync::{Mutex, PoisonError};

use busbar_contract::abi::host::io::{
    HandleIn, IoSlots, ReadIn as IoReadIn, WriteIn as IoWriteIn, SLOTS,
};
use busbar_contract::abi::host::service::{ClockNowIn, HostSlots, ServiceOut, SERVICES};
use busbar_contract::abi::mechanism::call::{
    AbiStr, Field, InHead, Op, OutHead, Outcome, RawOutcome,
};
use busbar_contract::abi::mechanism::door::Door;
use busbar_contract::abi::mechanism::lifecycle::{slot as life, OpenIn, OpenOut};
use busbar_contract::abi::mechanism::ticket::{HostCtx, HostTables, Ticket};
use busbar_contract::abi::transport::{
    slot, ConnIn, ConnOut, Destination, DialIn, IoOut, Ops, ReadIn, ShutIn, WriteIn, DEST_PROGRAM,
    WRITE_END_OF_FRAME,
};
use busbar_transport_stdio::door::{door, MAX_LINE_BYTES, TOO_LONG};

const MS: u64 = 1_000_000;

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
    // SAFETY: each private fixture caller selects this slot's exact input/output types.
    // Both aligned frames and their nested loans stay live through the synchronous crossing;
    // `inst` is null for open or owned by the live fixture until its matching close returns.
    unsafe {
        (op.expect("every slot is filled"))(
            inst,
            std::ptr::from_ref(i).cast(),
            std::ptr::from_mut(o).cast(),
        )
        .outcome()
    }
}

struct Script {
    /// The host's monotonic clock, nanoseconds.
    mono_ns: u64,
    /// Clock snapshots delivered late after other connection notifications acquired the lock.
    clock_snapshots: VecDeque<u64>,
    /// Whether the next spawn fails.
    spawn_fails: bool,
    /// Whether the next spawn is REFUSED by the host's policy (not a system failure).
    spawn_refused: bool,
    /// Time spent by the host attempting the spawn, before it returns its answer.
    spawn_elapsed_ns: u64,
    /// Whether a read FAILS (the pipe errors) rather than answering.
    read_fails: bool,
    /// Whether a read finds the program ended (`true`) or running (`false`: PENDING).
    ended: bool,
    /// Independently ended pipes while the script's other handles are still running.
    ended_handles: Vec<u64>,
    /// The supervised parent exited, independently of a descendant holding its stdout pipe.
    parent_exited: bool,
    /// The handle the next spawn answers.
    next_handle: u64,
    /// Spawns answered.
    spawns: u32,
    /// Whether `clock.now` fails.
    clock_fails: bool,
    /// Full reads of output (`x`, as many bytes as the carrier lends) answered before the read
    flood: usize,
    /// Whether a pipe write FAILS (the program's stdin is broken: EPIPE).
    write_fails: bool,
    /// Whether a pipe write answers PENDING (the program reads nothing yet).
    write_pends: bool,
    /// Explicit successful write counts; after these, writes take the full offered suffix.
    write_counts: VecDeque<usize>,
    write_calls: usize,
    written: Vec<u8>,
    closed: Vec<u64>,
}

static SCRIPT: Mutex<Script> = Mutex::new(Script {
    mono_ns: 1_000 * MS,
    clock_snapshots: VecDeque::new(),
    spawn_fails: false,
    spawn_refused: false,
    spawn_elapsed_ns: 0,
    read_fails: false,
    ended: true,
    ended_handles: Vec::new(),
    parent_exited: false,
    next_handle: 1,
    spawns: 0,
    clock_fails: false,
    flood: 0,
    write_fails: false,
    write_pends: false,
    write_counts: VecDeque::new(),
    write_calls: 0,
    written: Vec::new(),
    closed: Vec::new(),
});
static ONE: Mutex<()> = Mutex::new(());

fn with<R>(f: impl FnOnce(&mut Script) -> R) -> R {
    f(&mut SCRIPT.lock().unwrap_or_else(PoisonError::into_inner))
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

extern "C" fn io_spawn(_: HostCtx, _: *const c_void, out: *mut ServiceOut) -> RawOutcome {
    let (fails, refused, h) = with(|sc| {
        sc.spawns += 1;
        sc.mono_ns += sc.spawn_elapsed_ns;
        sc.next_handle += 1;
        (sc.spawn_fails, sc.spawn_refused, sc.next_handle)
    });
    if refused {
        answer(out, Outcome::Refused, 0, 0)
    } else if fails {
        answer(out, Outcome::Failed, 0, 0)
    } else {
        answer(out, Outcome::Ready, h, 0)
    }
}

extern "C" fn io_read(_: HostCtx, input: *const c_void, out: *mut ServiceOut) -> RawOutcome {
    // SAFETY: the carrier's `in`.
    let i = unsafe { input.cast::<IoReadIn>().read() };
    if with(|sc| sc.read_fails) {
        return answer(out, Outcome::Failed, 0, 0);
    }
    if with(|sc| sc.flood.checked_sub(1).map(|n| sc.flood = n).is_some()) {
        // SAFETY: the carrier's buffer, `cap` bytes.
        unsafe { std::ptr::write_bytes(i.buf, b'x', i.cap) };
        return answer(out, Outcome::Ready, 0, i.cap as u64);
    }
    if with(|sc| sc.ended || sc.ended_handles.contains(&i.handle)) {
        answer(out, Outcome::Ready, 0, 0)
    } else {
        answer(out, Outcome::Pending, 0, 0)
    }
}

extern "C" fn io_write(_: HostCtx, input: *const c_void, out: *mut ServiceOut) -> RawOutcome {
    // SAFETY: the carrier's `in`.
    let i = unsafe { input.cast::<IoWriteIn>().read() };
    let (fails, pends) = with(|sc| {
        sc.write_calls += 1;
        (sc.write_fails, sc.write_pends)
    });
    if fails {
        answer(out, Outcome::Failed, 0, 0)
    } else if pends {
        answer(out, Outcome::Pending, 0, 0)
    } else {
        let n = with(|sc| {
            let n = sc.write_counts.pop_front().unwrap_or(i.len);
            assert!(n <= i.len);
            // SAFETY: the carrier lends exactly these initialized bytes during this call.
            let bytes = unsafe { std::slice::from_raw_parts(i.bytes, n) };
            sc.written.extend_from_slice(bytes);
            n
        });
        answer(out, Outcome::Ready, 0, n as u64)
    }
}

extern "C" fn io_close(_: HostCtx, input: *const c_void, out: *mut ServiceOut) -> RawOutcome {
    // SAFETY: the carrier's `in`.
    let i = unsafe { input.cast::<HandleIn>().read() };
    with(|sc| sc.closed.push(i.handle));
    answer(out, Outcome::Ready, 0, 0)
}

extern "C" fn io_ends(_: HostCtx, _: *const c_void, out: *mut ServiceOut) -> RawOutcome {
    // SAFETY: the caller lends its ServiceOut for this service crossing.
    unsafe { (*out).exited = u8::from(with(|sc| sc.parent_exited)) };
    answer(out, Outcome::Ready, 0, 0)
}

extern "C" fn clock_now(_: HostCtx, input: *const c_void, out: *mut ServiceOut) -> RawOutcome {
    if with(|sc| sc.clock_fails) {
        return answer(out, Outcome::Failed, 0, 0);
    }
    // SAFETY: the carrier's `in`, and the reading slot it names.
    unsafe {
        let i = input.cast::<ClockNowIn>().read();
        (*i.reading).wall_ns = 1_700_000_000_000_000_000;
        (*i.reading).mono_ns = with(|sc| sc.clock_snapshots.pop_front().unwrap_or(sc.mono_ns));
    }
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
    ends: Some(io_ends),
};

fn services() -> &'static HostSlots {
    static TABLE: std::sync::OnceLock<HostSlots> = std::sync::OnceLock::new();
    TABLE.get_or_init(|| {
        let mut t: HostSlots = z();
        t.size = size_of::<HostSlots>() as u32;
        t.slots = SERVICES;
        t.clock_now = Some(clock_now);
        t
    })
}

/// One opened carrier instance over the scripted host.
struct Carrier {
    inst: *mut c_void,
    _tables: Box<HostTables>,
    _io: Box<IoSlots>,
}

impl Carrier {
    fn open() -> Self {
        Self::with_exit_reporting(true)
    }

    fn with_exit_reporting(report_exits: bool) -> Self {
        *SCRIPT.lock().unwrap_or_else(PoisonError::into_inner) = Script {
            mono_ns: 1_000 * MS,
            clock_snapshots: VecDeque::new(),
            spawn_fails: false,
            spawn_refused: false,
            spawn_elapsed_ns: 0,
            read_fails: false,
            ended: true,
            ended_handles: Vec::new(),
            parent_exited: false,
            next_handle: 1,
            spawns: 0,
            clock_fails: false,
            flood: 0,
            write_fails: false,
            write_pends: false,
            write_counts: VecDeque::new(),
            write_calls: 0,
            written: Vec::new(),
            closed: Vec::new(),
        };
        let io = Box::new(IoSlots {
            ends: report_exits.then_some(io_ends),
            ..IO
        });
        let tables = Box::new(HostTables {
            size: size_of::<HostTables>() as u32,
            _reserved: 0,
            ctx: HostCtx::NONE,
            wake: None,
            conns: std::ptr::null(),
            services: services(),
            io: &*io,
        });
        let mut i: OpenIn = z();
        i.host = &*tables;
        let mut o: OpenOut = z();
        assert_eq!(
            call(
                ops().head.open,
                std::ptr::null_mut(),
                &mut i,
                &mut o,
                life::OPEN
            ),
            Outcome::Ready
        );
        Self {
            inst: o.instance,
            _tables: tables,
            _io: io,
        }
    }

    /// Dial `/usr/bin/server` for the member `key`, its environment `TOKEN=<token>`: the outcome
    fn dial(&self, key: u64, token: &str) -> (Outcome, u64) {
        let (r, conn, _) = self.dial_why(key, token);
        (r, conn)
    }

    /// As [`Carrier::dial`], and why (the dial's error text, empty when it gave none).
    fn dial_why(&self, key: u64, token: &str) -> (Outcome, u64, String) {
        let env = [Field {
            name: s("TOKEN"),
            value: s(token),
        }];
        let mut dest: Destination = z();
        dest.kind = DEST_PROGRAM;
        dest.program = s("/usr/bin/server");
        dest.env = env.as_ptr();
        dest.env_len = env.len();
        let mut di: DialIn = z();
        di.dest = &dest;
        di.supervision_key = key;
        let mut co: ConnOut = z();
        let r = call(ops().dial, self.inst, &mut di, &mut co, slot::DIAL);
        let why = if co.head.error.len == 0 {
            String::new()
        } else {
            // SAFETY: the carrier's string, live for the call.
            let b = unsafe { std::slice::from_raw_parts(co.head.error.ptr, co.head.error.len) };
            String::from_utf8_lossy(b).into_owned()
        };
        (r, co.conn, why)
    }

    /// Read `conn` once: with the script's `ended`, the program's end (a clean `len` `0`).
    fn read(&self, conn: u64) -> Outcome {
        self.read_why(conn).0
    }

    /// Read `conn` once: the outcome and why.
    fn read_why(&self, conn: u64) -> (Outcome, String) {
        let mut buf = [0_u8; 64];
        let mut i: ReadIn = z();
        i.conn = conn;
        i.buf = buf.as_mut_ptr();
        i.cap = buf.len();
        let mut o: IoOut = z();
        let r = call(ops().read, self.inst, &mut i, &mut o, slot::READ);
        let why = if o.head.error.len == 0 {
            String::new()
        } else {
            // SAFETY: the carrier's string, live for the call.
            let b = unsafe { std::slice::from_raw_parts(o.head.error.ptr, o.head.error.len) };
            String::from_utf8_lossy(b).into_owned()
        };
        (r, why)
    }

    /// Assert the member `key`'s dial is REFUSED with the hold-down reason (`msg` says why).
    fn assert_held_down(&self, key: u64, token: &str, msg: &str) {
        let spawns = with(|sc| sc.spawns);
        let (r, _, why) = self.dial_why(key, token);
        assert_eq!(
            with(|sc| sc.spawns),
            spawns,
            "{msg}: a held-down dial must not spawn"
        );
        assert_eq!(r, Outcome::Refused, "{msg}");
        assert!(
            why.contains("held down"),
            "{msg}: the reason is the hold-down: {why:?}"
        );
    }

    /// Write one whole frame to `conn`.
    fn write_frame(&self, conn: u64, bytes: &[u8]) -> Outcome {
        self.write_frame_why(conn, bytes).0
    }

    fn write_frame_why(&self, conn: u64, bytes: &[u8]) -> (Outcome, String) {
        let mut i: WriteIn = z();
        i.conn = conn;
        i.bytes = bytes.as_ptr();
        i.len = bytes.len();
        i.flags = WRITE_END_OF_FRAME;
        let mut o: IoOut = z();
        let r = call(ops().write, self.inst, &mut i, &mut o, slot::WRITE);
        let why = if o.head.error.len == 0 {
            String::new()
        } else {
            // SAFETY: the carrier's error bytes remain live until release or instance close.
            let bytes = unsafe { std::slice::from_raw_parts(o.head.error.ptr, o.head.error.len) };
            String::from_utf8_lossy(bytes).into_owned()
        };
        (r, why)
    }

    /// Flush `conn`.
    fn flush(&self, conn: u64) -> Outcome {
        let mut i: ConnIn = z();
        i.conn = conn;
        let mut o: OutHead = z();
        call(ops().flush, self.inst, &mut i, &mut o, slot::FLUSH)
    }

    fn shut(&self, conn: u64) {
        let mut i: ShutIn = z();
        i.conn = conn;
        let mut o: OutHead = z();
        assert_eq!(
            call(ops().shut, self.inst, &mut i, &mut o, slot::SHUT),
            Outcome::Ready
        );
    }

    /// Run the member `key`'s program once to its end: dialed, read to its end, shut. Panics when
    fn run_to_end(&self, key: u64, token: &str) {
        let (r, conn) = self.dial(key, token);
        assert_eq!(r, Outcome::Ready, "member {key} may run");
        with(|sc| sc.ended = true);
        assert_eq!(self.read(conn), Outcome::Ready);
        self.shut(conn);
    }

    /// Five ends of the member `key`'s program, each after its backoff: it trips.
    fn trip(&self, key: u64, token: &str) {
        for backoff in [100, 200, 400, 800, 0] {
            self.run_to_end(key, token);
            advance(backoff * MS);
        }
    }
}

fn advance(ns: u64) {
    with(|sc| sc.mono_ns += ns);
}

/// A program that ends is held down for 100 ms, doubling; the fifth end inside a minute stops its
#[test]
fn a_program_that_keeps_ending_is_held_down_then_stopped() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let c = Carrier::open();
    c.run_to_end(1, "a");
    c.assert_held_down(1, "a", "held down inside the backoff");
    advance(100 * MS);
    c.run_to_end(1, "a");
    advance(100 * MS);
    c.assert_held_down(1, "a", "the backoff doubled to 200 ms");
    advance(100 * MS);
    c.run_to_end(1, "a");
    advance(400 * MS);
    c.run_to_end(1, "a");
    advance(800 * MS);
    c.run_to_end(1, "a");
    advance(3_600_000 * MS);
    c.assert_held_down(1, "a", "five ends in a minute: stopped");
}

/// A spawn that fails is counted like any end.
#[test]
fn a_spawn_that_fails_counts_as_an_end() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let c = Carrier::open();
    with(|sc| sc.spawn_fails = true);
    assert_eq!(
        c.dial(2, "a").0,
        Outcome::Failed,
        "a system failure of the spawn is FAILED"
    );
    with(|sc| sc.spawn_fails = false);
    let (r, _, why) = c.dial_why(2, "a");
    assert_eq!(r, Outcome::Refused, "held down after the failed spawn");
    assert!(
        why.contains("held down"),
        "the reason is the hold-down: {why:?}"
    );
}

#[test]
fn a_slow_failed_or_refused_spawn_starts_its_backoff_when_the_failure_returns() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    for policy_refused in [false, true] {
        let c = Carrier::open();
        with(|sc| {
            sc.spawn_elapsed_ns = 500 * MS;
            sc.spawn_fails = !policy_refused;
            sc.spawn_refused = policy_refused;
        });
        let expected = if policy_refused {
            Outcome::Refused
        } else {
            Outcome::Failed
        };
        assert_eq!(c.dial(2, "a").0, expected);
        with(|sc| {
            sc.spawn_elapsed_ns = 0;
            sc.spawn_fails = false;
            sc.spawn_refused = false;
        });
        c.assert_held_down(
            2,
            "a",
            "the full backoff follows the completed spawn attempt",
        );
        advance(100 * MS - 1);
        c.assert_held_down(2, "a", "one nanosecond remains in the backoff");
        advance(1);
        assert_eq!(c.dial(2, "a").0, Outcome::Ready);
    }
}

#[test]
fn a_shut_probe_does_not_drain_an_unbounded_running_program() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let c = Carrier::open();
    let (r, conn) = c.dial(81, "a");
    assert_eq!(r, Outcome::Ready);
    with(|sc| {
        sc.ended = false;
        sc.flood = 128;
    });
    c.shut(conn);
    assert!(
        with(|sc| sc.flood) > 0,
        "the probe must leave ongoing output unread"
    );
    assert_eq!(
        c.dial(81, "a").0,
        Outcome::Ready,
        "a still-running program is not an end"
    );
}

#[test]
fn a_pipe_error_first_seen_at_shut_counts_as_an_end() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let c = Carrier::open();
    let (r, conn) = c.dial(82, "a");
    assert_eq!(r, Outcome::Ready);
    with(|sc| {
        sc.ended = false;
        sc.read_fails = true;
    });
    c.shut(conn);
    c.assert_held_down(82, "a", "the shut probe observed the failed pipe");
}

#[test]
fn a_clean_shut_on_a_host_without_exit_reporting_is_not_an_end() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let c = Carrier::with_exit_reporting(false);
    with(|sc| sc.ended = false);
    for _ in 0..10 {
        let (r, conn) = c.dial(83, "a");
        assert_eq!(r, Outcome::Ready);
        c.shut(conn);
    }
}

#[test]
fn failed_and_refused_keyless_spawns_never_require_a_clock_or_trip_a_shared_key() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let c = Carrier::open();
    with(|sc| {
        sc.clock_fails = true;
        sc.spawn_fails = true;
    });
    for _ in 0..6 {
        assert_eq!(c.dial(0, "a").0, Outcome::Failed);
    }
    with(|sc| {
        sc.spawn_fails = false;
        sc.spawn_refused = true;
    });
    for _ in 0..6 {
        assert_eq!(c.dial(0, "a").0, Outcome::Refused);
    }
    with(|sc| sc.spawn_refused = false);
    assert_eq!(c.dial(0, "a").0, Outcome::Ready);
    assert_eq!(
        with(|sc| sc.spawns),
        13,
        "every keyless attempt reached the host"
    );
}

/// A spawn the host's POLICY refuses answers REFUSED, not FAILED: the host's refusal is not a
#[test]
fn a_spawn_the_host_refuses_is_refused() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let c = Carrier::open();
    with(|sc| sc.spawn_refused = true);
    assert_eq!(c.dial(3, "a").0, Outcome::Refused);
    with(|sc| sc.spawn_refused = false);
    c.assert_held_down(3, "a", "the refused spawn was counted as an end");
}

/// A read the pipe fails (the host answers a failure, not a line past the ceiling) is an END of
#[test]
fn a_failed_pipe_read_is_an_end() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let c = Carrier::open();
    let (r, conn) = c.dial(4, "a");
    assert_eq!(r, Outcome::Ready);
    with(|sc| {
        sc.ended = false;
        sc.read_fails = true;
    });
    assert_eq!(c.read(conn), Outcome::Failed);
    with(|sc| sc.read_fails = false);
    c.assert_held_down(4, "a", "counted at the failed read, before the shut");
    c.shut(conn);
    let (r, _, why) = c.dial_why(4, "a");
    assert_eq!(r, Outcome::Refused, "held down after the failed read");
    assert!(
        why.contains("held down"),
        "the reason is the hold-down: {why:?}"
    );
}

/// A write the pipe fails (a program that died breaks its stdin: EPIPE) is an END of the program,
#[test]
fn a_failed_pipe_write_is_an_end() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let c = Carrier::open();
    let (r, conn) = c.dial(5, "a");
    assert_eq!(r, Outcome::Ready);
    with(|sc| {
        sc.ended = false;
        sc.write_fails = true;
    });
    assert_eq!(c.write_frame(conn, b"{}"), Outcome::Failed);
    c.assert_held_down(5, "a", "counted at the failed write, before the shut");
    with(|sc| sc.write_fails = false);
    c.shut(conn);
    c.assert_held_down(5, "a", "still held down after the shut");
}

/// A flush the pipe fails (the frame was queued while the program read nothing, then its stdin
#[test]
fn a_failed_flush_is_an_end() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let c = Carrier::open();
    let (r, conn) = c.dial(6, "a");
    assert_eq!(r, Outcome::Ready);
    with(|sc| {
        sc.ended = false;
        sc.write_pends = true;
    });
    assert_eq!(c.write_frame(conn, b"{}"), Outcome::Ready, "queued");
    with(|sc| {
        sc.write_pends = false;
        sc.write_fails = true;
    });
    assert_eq!(c.flush(conn), Outcome::Failed);
    c.assert_held_down(6, "a", "counted at the failed flush, before the shut");
    with(|sc| sc.write_fails = false);
    c.shut(conn);
    c.assert_held_down(6, "a", "still held down after the shut");
}

#[test]
fn positive_writes_and_a_pending_queue_flush_preserve_every_byte_and_close_once() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let c = Carrier::open();
    with(|sc| sc.ended = false);
    let (outcome, conn) = c.dial(90, "a");
    assert_eq!(outcome, Outcome::Ready);
    assert_eq!(c.write_frame(conn, b"first"), Outcome::Ready);
    with(|sc| sc.write_pends = true);
    assert_eq!(c.write_frame(conn, b"second"), Outcome::Ready);
    assert_eq!(c.flush(conn), Outcome::Pending);
    with(|sc| {
        sc.write_pends = false;
        sc.write_counts = [2, 1].into();
    });
    assert_eq!(c.flush(conn), Outcome::Ready);
    with(|sc| assert_eq!(sc.written, b"first\nsecond\n"));
    c.shut(conn);
    c.shut(conn);
    with(|sc| assert_eq!(sc.closed.len(), 1));
    assert_eq!(c.dial(90, "a").0, Outcome::Ready, "a clean close is no end");
}

#[test]
fn a_zero_write_fails_once_and_counts_an_end_with_or_without_an_accepted_prefix() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    for prefix in [0, 2] {
        let c = Carrier::open();
        with(|sc| sc.ended = false);
        let (outcome, conn) = c.dial(91, "a");
        assert_eq!(outcome, Outcome::Ready);
        with(|sc| {
            sc.write_counts = if prefix == 0 {
                [0].into()
            } else {
                [prefix, 0].into()
            };
        });
        let (outcome, why) = c.write_frame_why(conn, b"frame");
        assert_eq!(outcome, Outcome::Failed);
        assert_eq!(
            why,
            "write: the program's pipe accepted no bytes (write zero)"
        );
        with(|sc| {
            assert_eq!(sc.write_calls, if prefix == 0 { 1 } else { 2 });
            assert_eq!(sc.written, &b"frame"[..prefix]);
        });
        c.assert_held_down(91, "a", "write zero faults the generation");
        c.shut(conn);
        c.shut(conn);
        with(|sc| assert_eq!(sc.closed.len(), 1));
        advance(100 * MS);
        assert_eq!(
            c.dial(91, "a").0,
            Outcome::Ready,
            "the end was counted once"
        );
    }
}

#[test]
fn a_queued_flush_that_writes_a_prefix_then_zero_fails_and_counts_one_end() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let c = Carrier::open();
    with(|sc| sc.ended = false);
    let (outcome, conn) = c.dial(92, "a");
    assert_eq!(outcome, Outcome::Ready);
    with(|sc| sc.write_pends = true);
    assert_eq!(c.write_frame(conn, b"queued"), Outcome::Ready);
    with(|sc| {
        sc.write_pends = false;
        sc.write_counts = [1, 0].into();
    });
    assert_eq!(c.flush(conn), Outcome::Failed);
    with(|sc| {
        assert_eq!(sc.write_calls, 3, "one Pending, one prefix, one zero");
        assert_eq!(sc.written, b"q");
    });
    c.assert_held_down(92, "a", "flush write zero faults the generation");
    c.shut(conn);
    c.shut(conn);
    with(|sc| assert_eq!(sc.closed.len(), 1));
    advance(100 * MS);
    assert_eq!(
        c.dial(92, "a").0,
        Outcome::Ready,
        "the end was counted once"
    );
}

/// (i) Two members running one identical program trip independently.
#[test]
fn two_members_running_one_program_trip_independently() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let c = Carrier::open();
    c.trip(10, "a");
    c.assert_held_down(10, "a", "the first member tripped");
    assert_eq!(
        c.dial(11, "a").0,
        Outcome::Ready,
        "its sibling runs the same program"
    );
}

#[test]
fn overlapping_handles_of_one_member_count_only_the_pipe_that_ended() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let c = Carrier::open();
    with(|sc| sc.ended = false);
    let (r, ended) = c.dial(12, "a");
    assert_eq!(r, Outcome::Ready);
    let ended_handle = with(|sc| sc.next_handle);
    let (r, running) = c.dial(12, "a");
    assert_eq!(r, Outcome::Ready);
    assert_ne!(ended, running);
    assert_ne!(ended_handle, with(|sc| sc.next_handle));
    with(|sc| sc.ended_handles.push(ended_handle));
    assert_eq!(c.read(running), Outcome::Pending);
    assert_eq!(c.read(ended), Outcome::Ready);
    c.assert_held_down(12, "a", "one overlapping pipe ended");
    c.shut(running);
    c.shut(ended);
    advance(100 * MS - 1);
    c.assert_held_down(12, "a", "the first end's backoff still holds");
    advance(1);
    let (r, next) = c.dial(12, "a");
    assert_eq!(r, Outcome::Ready, "neither shut counted another end");
    c.shut(next);
}

#[test]
fn a_delayed_overlapping_end_counts_only_inside_the_newest_crash_window() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let window = busbar_transport_stdio::supervisor::CRASH_WINDOW_NS;
    for outside in [true, false] {
        let c = Carrier::open();
        with(|sc| sc.mono_ns = 0);
        let conns: Vec<_> = (0..5)
            .map(|_| {
                let (outcome, conn) = c.dial(93, "a");
                assert_eq!(outcome, Outcome::Ready);
                conn
            })
            .collect();
        let latest = 2 * window;
        with(|sc| sc.mono_ns = latest);
        for &conn in &conns[..4] {
            assert_eq!(c.read(conn), Outcome::Ready);
        }

        with(|sc| {
            sc.clock_snapshots
                .push_back(if outside { 0 } else { latest - window });
        });
        assert_eq!(c.read(conns[4]), Outcome::Ready);
        for conn in conns {
            c.shut(conn);
        }
        advance(800 * MS);
        if outside {
            assert_eq!(c.dial(93, "a").0, Outcome::Ready, "only four recent ends");
        } else {
            c.assert_held_down(93, "a", "the inclusive boundary supplied the fifth end");
        }
    }
}

/// (ii) A member dropped and re-added is a new member: a new key, fresh supervision.
#[test]
fn a_member_dropped_and_re_added_is_fresh() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let c = Carrier::open();
    c.trip(20, "a");
    c.assert_held_down(20, "a", "the tripped member");
    assert_eq!(
        c.dial(21, "a").0,
        Outcome::Ready,
        "re-added: the host's new key"
    );
}

/// (iii) A rotated secret value under an unchanged member does NOT reset a trip.
#[test]
fn a_rotated_secret_value_does_not_reset_a_trip() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let c = Carrier::open();
    c.trip(30, "old-secret");
    c.assert_held_down(
        30,
        "rotated-secret",
        "the same member, a new environment value: still stopped",
    );
}

/// A clean shut of a running program is not an end: shut ten times over, it still dials at once.
#[test]
fn a_clean_shut_of_a_running_program_is_not_an_end() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let c = Carrier::open();
    with(|sc| sc.ended = false);
    for _ in 0..10 {
        let (r, conn) = c.dial(40, "a");
        assert_eq!(r, Outcome::Ready);
        c.shut(conn);
    }
}

/// A program found ended when it is shut (the host's lease found it exited) is an end.
#[test]
fn a_program_found_ended_at_its_shut_is_an_end() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let c = Carrier::open();
    let (r, conn) = c.dial(50, "a");
    assert_eq!(r, Outcome::Ready);
    with(|sc| sc.ended = true);
    c.shut(conn);
    c.assert_held_down(50, "a", "its end was counted at the shut");
}

#[test]
fn an_exited_parent_is_supervised_while_a_descendant_keeps_stdout_open() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let c = Carrier::open();
    with(|sc| sc.ended = false);
    for backoff in [100, 200, 400, 800, 0] {
        with(|sc| sc.parent_exited = false);
        let (outcome, conn) = c.dial(51, "a");
        assert_eq!(outcome, Outcome::Ready, "the previous backoff has elapsed");
        with(|sc| sc.parent_exited = true);
        c.shut(conn);
        c.assert_held_down(51, "a", "parent exit counts even while stdout is Pending");
        advance(backoff * MS);
    }
    advance(3_600_000 * MS);
    c.assert_held_down(51, "a", "five parent exits trip the existing supervisor");
}

/// A key of `0` names no member (a one-shot program line): it is not supervised.
#[test]
fn a_program_no_member_runs_is_not_supervised() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let c = Carrier::open();
    for _ in 0..8 {
        c.run_to_end(0, "a");
    }
    assert_eq!(c.dial(0, "a").0, Outcome::Ready);
}

/// A member's program is never run unsupervised: with a clock that does not answer its dial fails
#[test]
fn a_member_is_not_run_when_the_clock_does_not_answer() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let c = Carrier::open();
    with(|sc| sc.clock_fails = true);
    let before = with(|sc| sc.spawns);
    let (r, _, why) = c.dial_why(60, "a");
    assert_eq!(r, Outcome::Failed);
    assert_eq!(with(|sc| sc.spawns), before, "no program was spawned");
    assert!(why.contains("did not answer"), "reason was: {why}");
    assert_eq!(c.dial(0, "a").0, Outcome::Ready);
}

/// An end met while the host's clock does not answer is not lost: it is counted once the clock
#[test]
fn an_end_met_without_a_clock_is_counted_once_the_clock_answers() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let c = Carrier::open();
    let (r, conn) = c.dial(60, "a");
    assert_eq!(r, Outcome::Ready);
    with(|sc| {
        sc.ended = true;
        sc.clock_fails = true;
    });
    let _ = c.read(conn);
    with(|sc| sc.clock_fails = false);
    c.shut(conn);
    c.assert_held_down(60, "a", "the end was counted at the shut");
}

/// Full reads of output (the carrier reads 16 KiB at a time) that carry a member's line past the
fn past_the_ceiling() -> usize {
    MAX_LINE_BYTES / (16 * 1024) + 1
}

/// A program whose line runs past the ceiling fails the read, and that failure is an END of its
#[test]
fn a_line_past_the_ceiling_is_an_end() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let c = Carrier::open();
    let (r, conn) = c.dial(70, "a");
    assert_eq!(r, Outcome::Ready);
    with(|sc| {
        sc.ended = false;
        sc.flood = past_the_ceiling();
    });
    assert_eq!(
        c.read_why(conn),
        (Outcome::Failed, TOO_LONG.to_owned()),
        "the line ran past the ceiling"
    );
    c.assert_held_down(70, "a", "counted at the failed read, before the shut");
    c.shut(conn);
    c.assert_held_down(70, "a", "held down after the read the program failed");
}

/// A read the program failed while the host's clock did not answer is counted once it answers
#[test]
fn a_line_past_the_ceiling_met_without_a_clock_is_counted_at_the_shut() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let c = Carrier::open();
    let (r, conn) = c.dial(71, "a");
    assert_eq!(r, Outcome::Ready);
    with(|sc| {
        sc.ended = false;
        sc.clock_fails = true;
        sc.flood = past_the_ceiling();
    });
    assert_eq!(c.read(conn), Outcome::Failed);
    with(|sc| sc.clock_fails = false);
    c.shut(conn);
    c.assert_held_down(71, "a", "the failed read was counted at the shut");
}

/// A program that exited behind a FULL pipe (1 MiB, the most an unprivileged program can size its
#[test]
fn a_program_that_exited_behind_a_full_pipe_is_found_ended_at_its_shut() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let c = Carrier::open();
    let (r, conn) = c.dial(80, "a");
    assert_eq!(r, Outcome::Ready);
    with(|sc| {
        sc.ended = true;
        sc.flood = 1024 * 1024 / (16 * 1024);
    });
    c.shut(conn);
    c.assert_held_down(
        80,
        "a",
        "its end behind a full pipe was counted at the shut",
    );
}
