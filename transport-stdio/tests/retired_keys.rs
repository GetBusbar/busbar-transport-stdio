// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! Retirement through the exported carrier door, using the existing destination wire value.

use std::ffi::c_void;
use std::mem::{size_of, zeroed};
use std::sync::{Mutex, PoisonError};

use busbar_contract::abi::host::io::{IoSlots, WriteIn as IoWriteIn, SLOTS};
use busbar_contract::abi::host::service::{ClockNowIn, HostSlots, ServiceOut, SERVICES};
use busbar_contract::abi::mechanism::call::{AbiStr, InHead, Op, OutHead, Outcome, RawOutcome};
use busbar_contract::abi::mechanism::lifecycle::{slot as life, OpenIn, OpenOut};
use busbar_contract::abi::mechanism::ticket::{HostCtx, HostTables, Ticket};
use busbar_contract::abi::transport::{
    slot, ConnOut, Destination, DialIn, Ops, WriteIn, DEST_AUTHORITY, DEST_PROGRAM,
    WRITE_END_OF_FRAME,
};

// DEST_RETIRED is wire kind 2 in the existing core contract; the pinned dev.2 SDK predates its name.
const RETIRED: u32 = 2;
static ONE: Mutex<()> = Mutex::new(());
static SCRIPT: Mutex<Option<Script>> = Mutex::new(None);

#[derive(Default)]
struct Script {
    mono_ns: u64,
    clocks: usize,
    spawns: usize,
    writes: Vec<u8>,
    closes: usize,
    allow_spawn: bool,
}

fn with<R>(f: impl FnOnce(&mut Script) -> R) -> R {
    f(SCRIPT
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .as_mut()
        .expect("an opened fixture"))
}

fn zero<T>() -> T {
    // SAFETY: callers use only the ABI's integer, pointer and optional-function shapes.
    unsafe { zeroed() }
}

fn text(value: AbiStr) -> String {
    if value.len == 0 {
        return String::new();
    }
    // SAFETY: the output's error text remains lent until its next call or instance close.
    String::from_utf8(unsafe { std::slice::from_raw_parts(value.ptr, value.len) }.to_vec()).unwrap()
}

fn answer(out: *mut ServiceOut, outcome: Outcome, value: u64, len: u64) -> RawOutcome {
    // SAFETY: every callback receives the SDK's writable ServiceOut for this call.
    unsafe {
        (*out).outcome = RawOutcome::of(outcome);
        (*out).value = value;
        (*out).len = len;
    }
    RawOutcome::of(outcome)
}

extern "C" fn clock(_: HostCtx, input: *const c_void, out: *mut ServiceOut) -> RawOutcome {
    let mono_ns = with(|script| {
        script.clocks += 1;
        script.mono_ns
    });
    // SAFETY: clock_now receives a ClockNowIn naming the SDK's live writable reading.
    unsafe { (*(*input.cast::<ClockNowIn>()).reading).mono_ns = mono_ns };
    answer(out, Outcome::Ready, 0, 0)
}

extern "C" fn spawn(_: HostCtx, _: *const c_void, out: *mut ServiceOut) -> RawOutcome {
    let (allowed, handle) = with(|script| {
        script.spawns += 1;
        (script.allow_spawn, script.spawns as u64)
    });
    answer(
        out,
        if allowed {
            Outcome::Ready
        } else {
            Outcome::Failed
        },
        if allowed { handle } else { 0 },
        0,
    )
}

extern "C" fn write(_: HostCtx, input: *const c_void, out: *mut ServiceOut) -> RawOutcome {
    // SAFETY: io.write receives the SDK's live IoWriteIn and its initialized byte slice.
    let input = unsafe { &*input.cast::<IoWriteIn>() };
    // SAFETY: these are the initialized bytes named by that live input.
    let bytes = unsafe { std::slice::from_raw_parts(input.bytes, input.len) };
    with(|script| script.writes.extend_from_slice(bytes));
    answer(out, Outcome::Ready, 0, bytes.len() as u64)
}

extern "C" fn close(_: HostCtx, _: *const c_void, out: *mut ServiceOut) -> RawOutcome {
    with(|script| script.closes += 1);
    answer(out, Outcome::Ready, 0, 0)
}

static IO: IoSlots = IoSlots {
    size: size_of::<IoSlots>() as u32,
    slots: SLOTS,
    open: None,
    listen: None,
    accept: None,
    read: None,
    write: Some(write),
    ready: None,
    shut: None,
    close: Some(close),
    spawn: Some(spawn),
    ends: None,
};

fn ops() -> &'static Ops {
    // SAFETY: this carrier exports a static transport door with a transport Ops table.
    unsafe { &*(*busbar_transport_stdio::door::door()).ops().cast::<Ops>() }
}

fn call<I, O>(
    op: Option<Op>,
    instance: *mut c_void,
    input: &mut I,
    out: &mut O,
    slot: u32,
) -> Outcome {
    // SAFETY: all callers pair the table's exact ABI input and output, beginning with these heads.
    unsafe {
        let head = &mut *std::ptr::from_mut(input).cast::<InHead>();
        head.size = size_of::<I>() as u32;
        head.op = slot;
        head.ticket = Ticket {
            slot: 1,
            generation: 1,
        };
        (*std::ptr::from_mut(out).cast::<OutHead>()).size = size_of::<O>() as u32;
    }
    op.expect("the slot is served")(
        instance,
        std::ptr::from_ref(input).cast(),
        std::ptr::from_mut(out).cast(),
    )
    .outcome()
}

struct Opened {
    instance: *mut c_void,
    _services: Box<HostSlots>,
    _tables: Box<HostTables>,
}

impl Opened {
    fn new(host: bool, io: bool) -> Self {
        *SCRIPT.lock().unwrap_or_else(PoisonError::into_inner) = Some(Script::default());
        let mut services: Box<HostSlots> = Box::new(zero());
        services.size = size_of::<HostSlots>() as u32;
        services.slots = SERVICES;
        services.clock_now = Some(clock);
        let tables = Box::new(HostTables {
            size: size_of::<HostTables>() as u32,
            _reserved: 0,
            ctx: HostCtx::NONE,
            wake: None,
            conns: std::ptr::null(),
            services: if io { &*services } else { std::ptr::null() },
            io: if io { &IO } else { std::ptr::null() },
        });
        let mut input: OpenIn = zero();
        input.host = if host { &*tables } else { std::ptr::null() };
        let mut out: OpenOut = zero();
        assert_eq!(
            call(
                ops().head.open,
                std::ptr::null_mut(),
                &mut input,
                &mut out,
                life::OPEN
            ),
            Outcome::Ready
        );
        Self {
            instance: out.instance,
            _services: services,
            _tables: tables,
        }
    }

    fn dial(&self, kind: u32, key: u64) -> (Outcome, u64, String) {
        dial(self.instance, kind, key)
    }

    fn trip(&self, key: u64) {
        for _ in 0..busbar_transport_stdio::supervisor::CRASH_THRESHOLD {
            with(|script| script.mono_ns += 1_000_000_000);
            assert_eq!(self.dial(DEST_PROGRAM, key).0, Outcome::Failed);
        }
        let before = with(|script| script.spawns);
        assert_eq!(self.dial(DEST_PROGRAM, key).0, Outcome::Refused);
        assert_eq!(
            with(|script| script.spawns),
            before,
            "a tripped key cannot spawn"
        );
    }
}

fn dial(instance: *mut c_void, kind: u32, key: u64) -> (Outcome, u64, String) {
    let mut dest: Destination = zero();
    dest.kind = kind;
    if kind == DEST_PROGRAM {
        dest.program = AbiStr {
            ptr: b"/fixture/program".as_ptr(),
            len: b"/fixture/program".len(),
        };
    }
    let mut input: DialIn = zero();
    input.dest = &dest;
    input.supervision_key = key;
    let mut out: ConnOut = zero();
    out.conn = u64::MAX;
    let outcome = call(ops().dial, instance, &mut input, &mut out, slot::DIAL);
    (outcome, out.conn, text(out.head.error))
}

impl Drop for Opened {
    fn drop(&mut self) {
        let mut input: InHead = zero();
        let mut out: OutHead = zero();
        assert_eq!(
            call(
                ops().head.close,
                self.instance,
                &mut input,
                &mut out,
                life::CLOSE
            ),
            Outcome::Ready
        );
    }
}

fn activity() -> (usize, usize, usize, usize) {
    with(|script| {
        (
            script.clocks,
            script.spawns,
            script.writes.len(),
            script.closes,
        )
    })
}

#[test]
fn retirement_forgets_only_its_key_and_preserves_a_live_sibling() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    let host = Opened::new(true, true);
    host.trip(7);
    host.trip(8);
    with(|script| script.allow_spawn = true);
    let (outcome, live, _) = host.dial(DEST_PROGRAM, 9);
    assert_eq!(outcome, Outcome::Ready);
    let before = activity();
    for key in [7, 7, 1234, 0] {
        assert_eq!(host.dial(RETIRED, key), (Outcome::Ready, 0, String::new()));
    }
    assert_eq!(
        activity(),
        before,
        "retirement uses neither I/O nor the clock"
    );
    assert_eq!(
        host.dial(DEST_PROGRAM, 8).0,
        Outcome::Refused,
        "the sibling stays tripped"
    );
    assert_eq!(
        host.dial(DEST_PROGRAM, 7).0,
        Outcome::Ready,
        "the retired key's old trip was forgotten"
    );
    let mut input: WriteIn = zero();
    input.conn = live;
    input.bytes = b"still live".as_ptr();
    input.len = b"still live".len();
    input.flags = WRITE_END_OF_FRAME;
    let mut out = zero::<busbar_contract::abi::transport::IoOut>();
    assert_eq!(
        call(
            ops().write,
            host.instance,
            &mut input,
            &mut out,
            slot::WRITE
        ),
        Outcome::Ready
    );
    with(|script| assert_eq!(script.writes, b"still live\n"));
}

#[test]
fn retirement_needs_an_open_instance_but_no_host_io_or_clock() {
    let _one = ONE.lock().unwrap_or_else(PoisonError::into_inner);
    for host_lent in [false, true] {
        let host = Opened::new(host_lent, false);
        assert_eq!(host.dial(RETIRED, 77), (Outcome::Ready, 0, String::new()));
        assert_eq!(activity(), (0, 0, 0, 0));
        assert_eq!(host.dial(DEST_PROGRAM, 0).0, Outcome::Failed);
        assert_eq!(host.dial(DEST_AUTHORITY, 0).0, Outcome::Refused);
    }
    let answer = dial(std::ptr::null_mut(), RETIRED, 77);
    assert_eq!(answer.0, Outcome::Failed);
    assert_eq!(answer.2, "dial: no open instance");
}
