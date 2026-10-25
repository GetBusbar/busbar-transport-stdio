// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! THE STDIO SUPERVISOR IS THE STDIO TRANSPORT'S (`BUSBAR-1.6.0.md` THE DESIGN §2, l.388), driven
//! through the carrier's own table over a SCRIPTED host: its I/O (a spawn that answers or fails, a
//! read that answers the program's end or PENDING while it runs) and its one clock (`clock.now`,
//! monotonic, set by the test). The policy is predev's crash-loop policy (the connector's
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
//! RED before the supervisor moved here: the carrier spawned whatever it was asked to, every time.

use std::ffi::c_void;
use std::mem::{size_of, zeroed};
use std::sync::Mutex;

use busbar_contract::abi::host::io::{HandleIn, IoSlots, ReadIn as IoReadIn, SLOTS};
use busbar_contract::abi::host::service::{ClockNowIn, HostSlots, ServiceOut, SERVICES};
use busbar_contract::abi::mechanism::call::{
    AbiStr, Field, InHead, Op, OutHead, Outcome, RawOutcome,
};
use busbar_contract::abi::mechanism::door::Door;
use busbar_contract::abi::mechanism::lifecycle::{slot as life, OpenIn, OpenOut};
use busbar_contract::abi::mechanism::ticket::{HostCtx, HostTables, Ticket};
use busbar_contract::abi::transport::{
    slot, ConnOut, Destination, DialIn, IoOut, Ops, ReadIn, ShutIn, DEST_PROGRAM,
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
    unsafe { &*(*d).ops.cast::<Ops>() }
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

// ── the scripted host ────────────────────────────────────────────────────────────────────────────

struct Script {
    /// The host's monotonic clock, nanoseconds.
    mono_ns: u64,
    /// Whether the next spawn fails.
    spawn_fails: bool,
    /// Whether a read finds the program ended (`true`) or running (`false`: PENDING).
    ended: bool,
    /// The handle the next spawn answers.
    next_handle: u64,
    /// Spawns answered.
    spawns: u32,
    /// Whether `clock.now` fails.
    clock_fails: bool,
    /// Full reads of output (`x`, as many bytes as the carrier lends) answered before the read
    /// that finds the program ended or running.
    flood: usize,
}

static SCRIPT: Mutex<Script> = Mutex::new(Script {
    mono_ns: 1_000 * MS,
    spawn_fails: false,
    ended: true,
    next_handle: 1,
    spawns: 0,
    clock_fails: false,
    flood: 0,
});
static ONE: Mutex<()> = Mutex::new(());

fn with<R>(f: impl FnOnce(&mut Script) -> R) -> R {
    f(&mut SCRIPT.lock().unwrap())
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
    let (fails, h) = with(|sc| {
        sc.spawns += 1;
        sc.next_handle += 1;
        (sc.spawn_fails, sc.next_handle)
    });
    if fails {
        answer(out, Outcome::Failed, 0, 0)
    } else {
        answer(out, Outcome::Ready, h, 0)
    }
}

extern "C" fn io_read(_: HostCtx, input: *const c_void, out: *mut ServiceOut) -> RawOutcome {
    // SAFETY: the carrier's `in`.
    let i = unsafe { input.cast::<IoReadIn>().read() };
    if with(|sc| sc.flood.checked_sub(1).map(|n| sc.flood = n).is_some()) {
        // SAFETY: the carrier's buffer, `cap` bytes.
        unsafe { std::ptr::write_bytes(i.buf, b'x', i.cap) };
        return answer(out, Outcome::Ready, 0, i.cap as u64);
    }
    if with(|sc| sc.ended) {
        answer(out, Outcome::Ready, 0, 0)
    } else {
        answer(out, Outcome::Pending, 0, 0)
    }
}

extern "C" fn io_close(_: HostCtx, input: *const c_void, out: *mut ServiceOut) -> RawOutcome {
    // SAFETY: the carrier's `in`.
    let _ = unsafe { input.cast::<HandleIn>().read() };
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
        (*i.reading).mono_ns = with(|sc| sc.mono_ns);
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
    write: None,
    ready: None,
    shut: None,
    close: Some(io_close),
    spawn: Some(io_spawn),
    ends: None,
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
}

impl Carrier {
    fn open() -> Self {
        *SCRIPT.lock().unwrap() = Script {
            mono_ns: 1_000 * MS,
            spawn_fails: false,
            ended: true,
            next_handle: 1,
            spawns: 0,
            clock_fails: false,
            flood: 0,
        };
        let tables = Box::new(HostTables {
            size: size_of::<HostTables>() as u32,
            _reserved: 0,
            ctx: HostCtx::NONE,
            wake: None,
            conns: std::ptr::null(),
            services: services(),
            io: &IO,
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
        }
    }

    /// Dial `/usr/bin/server` for the member `key`, its environment `TOKEN=<token>`: the outcome
    /// and the connection.
    fn dial(&self, key: u64, token: &str) -> (Outcome, u64) {
        let (r, conn, _) = self.dial_why(key, token);
        (r, conn)
    }

    /// As [`Carrier::dial`], and why when it fails.
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
    /// the dial is not READY.
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

// ── the tests ────────────────────────────────────────────────────────────────────────────────────

/// A program that ends is held down for 100 ms, doubling; the fifth end inside a minute stops its
/// restarts for good (an hour later it is still refused).
#[test]
fn a_program_that_keeps_ending_is_held_down_then_stopped() {
    let _one = ONE.lock().unwrap();
    let c = Carrier::open();
    c.run_to_end(1, "a");
    assert_eq!(
        c.dial(1, "a").0,
        Outcome::Refused,
        "held down inside the backoff"
    );
    advance(100 * MS);
    c.run_to_end(1, "a");
    advance(100 * MS);
    assert_eq!(
        c.dial(1, "a").0,
        Outcome::Refused,
        "the backoff doubled to 200 ms"
    );
    advance(100 * MS);
    c.run_to_end(1, "a");
    advance(400 * MS);
    c.run_to_end(1, "a");
    advance(800 * MS);
    c.run_to_end(1, "a");
    advance(3_600_000 * MS);
    assert_eq!(
        c.dial(1, "a").0,
        Outcome::Refused,
        "five ends in a minute: stopped"
    );
}

/// A spawn that fails is counted like any end.
#[test]
fn a_spawn_that_fails_counts_as_an_end() {
    let _one = ONE.lock().unwrap();
    let c = Carrier::open();
    with(|sc| sc.spawn_fails = true);
    assert_ne!(c.dial(2, "a").0, Outcome::Ready);
    with(|sc| sc.spawn_fails = false);
    assert_eq!(
        c.dial(2, "a").0,
        Outcome::Refused,
        "held down after the failed spawn"
    );
}

/// (i) Two members running one identical program trip independently.
#[test]
fn two_members_running_one_program_trip_independently() {
    let _one = ONE.lock().unwrap();
    let c = Carrier::open();
    c.trip(10, "a");
    assert_eq!(
        c.dial(10, "a").0,
        Outcome::Refused,
        "the first member tripped"
    );
    assert_eq!(
        c.dial(11, "a").0,
        Outcome::Ready,
        "its sibling runs the same program"
    );
}

/// (ii) A member dropped and re-added is a new member: a new key, fresh supervision.
#[test]
fn a_member_dropped_and_re_added_is_fresh() {
    let _one = ONE.lock().unwrap();
    let c = Carrier::open();
    c.trip(20, "a");
    assert_eq!(c.dial(20, "a").0, Outcome::Refused);
    assert_eq!(
        c.dial(21, "a").0,
        Outcome::Ready,
        "re-added: the host's new key"
    );
}

/// (iii) A rotated secret value under an unchanged member does NOT reset a trip.
#[test]
fn a_rotated_secret_value_does_not_reset_a_trip() {
    let _one = ONE.lock().unwrap();
    let c = Carrier::open();
    c.trip(30, "old-secret");
    assert_eq!(
        c.dial(30, "rotated-secret").0,
        Outcome::Refused,
        "the same member, a new environment value: still stopped"
    );
}

/// A clean shut of a running program is not an end: shut ten times over, it still dials at once.
#[test]
fn a_clean_shut_of_a_running_program_is_not_an_end() {
    let _one = ONE.lock().unwrap();
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
    let _one = ONE.lock().unwrap();
    let c = Carrier::open();
    let (r, conn) = c.dial(50, "a");
    assert_eq!(r, Outcome::Ready);
    with(|sc| sc.ended = true);
    c.shut(conn);
    assert_eq!(
        c.dial(50, "a").0,
        Outcome::Refused,
        "its end was counted at the shut"
    );
}

/// A key of `0` names no member (a one-shot program line): it is not supervised.
#[test]
fn a_program_no_member_runs_is_not_supervised() {
    let _one = ONE.lock().unwrap();
    let c = Carrier::open();
    for _ in 0..8 {
        c.run_to_end(0, "a");
    }
    assert_eq!(c.dial(0, "a").0, Outcome::Ready);
}

/// A member's program is never run unsupervised: with a clock that does not answer its dial fails
/// without a spawn, and says the clock did not answer (it was lent). An unsupervised program
/// (key `0`) needs no clock. RED (codeaudit 0aa4f6d, stdio LOW): the reason claimed the host lent
/// no clock.
#[test]
fn a_member_is_not_run_when_the_clock_does_not_answer() {
    let _one = ONE.lock().unwrap();
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
/// answers (here, at the connection's shut). RED (Q128 transport-stdio NIT): the end was marked
/// counted without being counted.
#[test]
fn an_end_met_without_a_clock_is_counted_once_the_clock_answers() {
    let _one = ONE.lock().unwrap();
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
    assert_eq!(
        c.dial(60, "a").0,
        Outcome::Refused,
        "the end was counted at the shut"
    );
}

/// Full reads of output (the carrier reads 16 KiB at a time) that carry a member's line past the
/// stdio line ceiling ([`MAX_LINE_BYTES`]).
fn past_the_ceiling() -> usize {
    MAX_LINE_BYTES / (16 * 1024) + 1
}

/// A program whose line runs past the ceiling fails the read, and that failure is an END of its
/// program, counted then: the host ends the generation and shuts a program that is still running,
/// which would otherwise read as a clean shut. RED (row 159, stdio MEDIUM M1): the read failed
/// uncounted, so a program writing an endless line was respawned on every lease with no backoff
/// and never held down.
#[test]
fn a_line_past_the_ceiling_is_an_end() {
    let _one = ONE.lock().unwrap();
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
    c.shut(conn);
    assert_eq!(
        c.dial(70, "a").0,
        Outcome::Refused,
        "held down after the read the program failed"
    );
}

/// A read the program failed while the host's clock did not answer is counted once it answers
/// (here, at the shut of the still-running program). RED (row 159, stdio MEDIUM M1).
#[test]
fn a_line_past_the_ceiling_met_without_a_clock_is_counted_at_the_shut() {
    let _one = ONE.lock().unwrap();
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
    assert_eq!(
        c.dial(71, "a").0,
        Outcome::Refused,
        "the failed read was counted at the shut"
    );
}

/// A program that exited behind a FULL pipe (1 MiB, the most an unprivileged program can size its
/// own pipe to, which the host reads directly and holds none of) is found ended at its shut: the
/// probe reads the pipe empty and then the read that finds the end. RED (row 159, stdio LOW): the
/// probe stopped after exactly 1 MiB, one read before the end, and read the program as running.
#[test]
fn a_program_that_exited_behind_a_full_pipe_is_found_ended_at_its_shut() {
    let _one = ONE.lock().unwrap();
    let c = Carrier::open();
    let (r, conn) = c.dial(80, "a");
    assert_eq!(r, Outcome::Ready);
    with(|sc| {
        sc.ended = true;
        sc.flood = 1024 * 1024 / (16 * 1024);
    });
    c.shut(conn);
    assert_eq!(
        c.dial(80, "a").0,
        Outcome::Refused,
        "its end behind a full pipe was counted at the shut"
    );
}
