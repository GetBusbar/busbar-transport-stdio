// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! THE `stdio` DOOR: this transport as a LINE FRAMER on the transport kind's table
//! (`busbar_contract::abi::transport`), compiled in or dropped in through the one door. Every slot
//! is a [`SafeSlot`] over the SDK's generic lifecycle (`life(Framings)`): no `unsafe` in this crate.
//!
//! The pipe is the host's (`BUSBAR-1.6.0.md` THE DESIGN, §5): the host owns the process's own
//! stdin/stdout or the child it spawned, and moves the bytes. This framer composes over nothing and
//! frames those bytes exactly as the 1.5.5 carrier did:
//!
//! * `ingest` splits the bytes the far side sent on `0x0A`. Each line, with its terminator and one
//!   trailing `0x0D` removed, is ONE frame on stream `0`; a blank line carries no frame. A line is
//!   at most [`MAX_LINE_BYTES`] before its terminator: a longer one is a framing error (the rest of
//!   it, through its `0x0A`, is discarded and the next line reads normally). The far side's end
//!   after a whole line is `YIELD_ENDED`; its end in the middle of a line is a framing error.
//! * `emit` takes a frame's bytes (across calls, until `end_of_frame`) and answers them as the
//!   wire bytes followed by `0x0A`. A frame holding a `0x0A`, or ending in `0x0D`, cannot be one
//!   line and is refused before any byte is written.
//! * `refuse` answers its bytes and a `0x0A` as the wire bytes (the last line of a connection).
//! * `encode` renders an envelope as its body alone (the line has no head; fields are not written),
//!   refusing a body that could not be one line.
//! * `locate`, `detach`, `adopt` and every carrier op are REFUSED: the pipe, the program and the
//!   handoff are the host's; a line framer composes over nothing and hands nothing up.
//!
//! No op pends, no op asks for a deadline, and a full sink is back-pressure (`YIELD_MORE`): the host
//! calls again, with no new bytes, once it has drained what it was given.

use std::collections::{HashMap, VecDeque};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};

use busbar_contract::abi::mechanism::call::{AbiStr, OutHead, Outcome};
use busbar_contract::abi::mechanism::door::{KindTailHead, Statement};
use busbar_contract::abi::sdk::door::{abi_str, statement, AbiIn, AbiOut};
use busbar_contract::abi::sdk::life::{Held, Life, Refreshed, Refusal};
use busbar_contract::abi::sdk::transport::form_codes;
use busbar_contract::abi::sdk::{HostBuf, Instance, Lent, Out, Safe, SafeSlot};
use busbar_contract::abi::transport::{
    AcceptIn, AcceptOut, AdoptIn, ArrivalIn, ArrivalOut, BeginIn, Claim, ConnIn, ConnOut, DialIn,
    EmitIn, EncodeIn, FinishIn, FramePiece, FramerOut, FramerSink, FramingIn, IngestIn, IoOut,
    ListenIn, ListenOut, LocateIn, LocateOut, Ops, ReadIn, RefuseIn, ShutIn, TransportTail,
    WriteIn, CANCEL_NOTHING_MOVED, FRAMING_STREAM, PIECE_END_OF_FRAME, ROLE_FRAMER,
    UNIT0_FIRST_MESSAGE, YIELD_ENDED, YIELD_MORE,
};
use busbar_contract::SelectorForm;

// ── the statement ────────────────────────────────────────────────────────────────────────────────

/// The claim this entry answers for.
pub const KEY: &str = "stdio";

/// The selector forms a `stdio` claim reads: none, the claim is the whole channel.
const SELECTOR_FORMS: &[SelectorForm] = &[];
const SELECTOR_CODES: [u8; SELECTOR_FORMS.len()] = form_codes(SELECTOR_FORMS);

const FACTS: &[AbiStr] = &[];

const NONE: AbiStr = AbiStr {
    ptr: std::ptr::null(),
    len: 0,
};

/// The schemes `stdio` claims, by name: the Statement's `claims`, the one place they are stated.
const CLAIM_NAMES: &[AbiStr] = &[abi_str(KEY)];

/// Each claimed scheme's row, by index into [`CLAIM_NAMES`].
const CLAIMS: &[Claim] = &[Claim {
    selector_forms: AbiStr {
        ptr: SELECTOR_CODES.as_ptr(),
        len: SELECTOR_CODES.len(),
    },
    egress_selector_forms: abi_str(""),
    facts: FACTS.as_ptr(),
    facts_len: FACTS.len(),
    status_namespace: NONE,
    session: 1,
    session_bound: 1,
    unit0_trigger: UNIT0_FIRST_MESSAGE,
    status_at: 0,
    _reserved: 0,
}];

/// The transport kind's tail: a framer over the host's pipe, composing over nothing.
const TAIL: TransportTail = TransportTail {
    head: KindTailHead {
        size: std::mem::size_of::<TransportTail>() as u32,
        _reserved: 0,
    },
    role: ROLE_FRAMER,
    framing: FRAMING_STREAM,
    facts: 0,
    handshake_max_steps: 0,
    composes_over: std::ptr::null(),
    composes_over_len: 0,
    claim_rows: CLAIMS.as_ptr(),
    claim_rows_len: CLAIMS.len(),
    upgrades_to: std::ptr::null(),
    upgrades_to_len: 0,
    handoff_from: NONE,
    handoff_to: NONE,
    handoff_binding_fact: NONE,
    handshake_frame_kind: NONE,
    status_rows: std::ptr::null(),
    status_rows_len: 0,
    settings: std::ptr::null(),
    settings_len: 0,
};

/// The door's Statement: the `stdio` line framer.
pub const STATEMENT: Statement = Statement {
    kind_tail: (&TAIL as *const TransportTail).cast::<KindTailHead>(),
    claims: CLAIM_NAMES.as_ptr(),
    claims_len: CLAIM_NAMES.len(),
    ..statement(KEY, env!("CARGO_PKG_VERSION"), 64)
};

// ── the instance ─────────────────────────────────────────────────────────────────────────────────

/// The most bytes one line may carry before its terminator: the design's per-connection reading
/// budget (one line is one frame on this wire, and a frame is what the budget measures), the figure
/// the 1.5.5 carrier bounded its reads with.
pub const MAX_LINE_BYTES: usize = busbar_contract::MAX_CURSOR_BYTES;

/// The framings one instance holds: the state the SDK's lifecycle opens and closes.
pub struct Framings {
    framings: Mutex<HashMap<u64, Framing>>,
    next: AtomicU64,
}

/// What every slot reads: the SDK's lifecycle state over [`Framings`].
type State = Held<Framings>;

impl Life for Framings {
    /// No framer op pends, so a cancel finds nothing in flight.
    const CANCEL: u32 = CANCEL_NOTHING_MOVED;

    /// This framer reads no setting.
    fn validate(_: &[u8]) -> Result<(), Refusal> {
        Ok(())
    }

    fn open(_: &[u8], _: &[&[u8]], _: u64) -> Result<Self, Refusal> {
        Ok(Self {
            framings: Mutex::new(HashMap::new()),
            next: AtomicU64::new(1),
        })
    }

    fn refresh(&self, _: &[u8], _: &[&[u8]], _: u64) -> Result<Refreshed, Refusal> {
        Ok(Refreshed::default())
    }
}

impl Framings {
    fn lock(&self) -> MutexGuard<'_, HashMap<u64, Framing>> {
        self.framings.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Install `f` under a fresh framing token, and answer the token.
    fn begin(&self, f: Framing) -> u64 {
        let token = self.next.fetch_add(1, Ordering::Relaxed);
        self.lock().insert(token, f);
        token
    }
}

/// What the far side's bytes made, waiting for room in the host's sink: a whole line, or the
/// framing error found at that point (so an error is answered in its place among the frames).
struct Ready {
    bytes: Vec<u8>,
    /// How much of it the host has already been given.
    given: usize,
    /// Instead of a line: the framing error to answer.
    error: Option<&'static str>,
}

/// One connection's framing.
#[derive(Default)]
struct Framing {
    /// The line the far side is part-way through sending (its terminator not yet seen).
    line: Vec<u8>,
    /// Discarding the rest of a line that ran past [`MAX_LINE_BYTES`], through its `0x0A`.
    skipping: bool,
    /// Whole lines and framing errors, in the order the far side sent them, not yet answered.
    ready: VecDeque<Ready>,
    /// The far side ended.
    ended: bool,
    /// The frame the host is emitting, so far (no terminator yet).
    emitting: Vec<u8>,
    /// Bytes owed to the far side, not yet answered as wire bytes.
    outbound: VecDeque<u8>,
}

/// The open instance, or the refusal a call on none earns.
fn framings<'a>(i: &Instance<'a, State>) -> Result<&'a Framings, Refusal> {
    i.get()
        .map(Held::life)
        .ok_or_else(|| Refusal::failed("no open instance"))
}

// ── the ops a line framer does not have: refused ─────────────────────────────────────────────────

/// A carrier, locate or handoff op: REFUSED. The pipe, the program and the handoff are the host's.
pub struct Refused<I, O>(PhantomData<(I, O)>);

impl<I: AbiIn, O: AbiOut> SafeSlot for Refused<I, O> {
    type In = I;
    type Out = O;
    type State = State;
    fn call(_: Instance<'_, State>, _: Lent<'_, I>, _: Out<'_, O>) -> Outcome {
        Outcome::Refused
    }
}

// ── the framer ───────────────────────────────────────────────────────────────────────────────────

/// `begin`: either side; nothing is owed to the far side first.
pub struct Begin;
impl SafeSlot for Begin {
    type In = BeginIn;
    type Out = FramerOut;
    type State = State;
    fn call(inst: Instance<'_, State>, _: Lent<'_, BeginIn>, mut o: Out<'_, FramerOut>) -> Outcome {
        let s = match framings(&inst) {
            Ok(s) => s,
            Err(r) => return o.fail(r),
        };
        let token = s.begin(Framing::default());
        o.set(|o| &o.framing, token);
        Outcome::Ready
    }
}

/// Run `op` on the framing `token` names and answer what it owes into `sink`.
fn with(
    inst: &Instance<'_, State>,
    token: u64,
    sink: Lent<'_, FramerSink>,
    o: &mut Out<'_, FramerOut>,
    op: impl FnOnce(&mut Framing) -> Result<(), Refusal>,
) -> Outcome {
    let s = match framings(inst) {
        Ok(s) => s,
        Err(r) => return o.fail(r),
    };
    let mut framings = s.lock();
    let Some(f) = framings.get_mut(&token) else {
        return o.fail(Refusal::failed("no such framing"));
    };
    if let Err(r) = op(f) {
        return o.fail(r);
    }
    f.answer(sink, o)
}

/// `ingest`: split the bytes on `0x0A`; the far side's end ends the connection.
pub struct Ingest;
impl SafeSlot for Ingest {
    type In = IngestIn;
    type Out = FramerOut;
    type State = State;
    fn call(
        inst: Instance<'_, State>,
        i: Lent<'_, IngestIn>,
        mut o: Out<'_, FramerOut>,
    ) -> Outcome {
        let bytes = i.bytes();
        with(&inst, i.framing, i.field(|x| &x.sink), &mut o, |f| {
            f.read(bytes, i.end != 0);
            Ok(())
        })
    }
}

/// `emit`: the bytes are a frame (across calls, to `end_of_frame`); the wire gets them and a `0x0A`.
pub struct Emit;
impl SafeSlot for Emit {
    type In = EmitIn;
    type Out = FramerOut;
    type State = State;
    fn call(inst: Instance<'_, State>, i: Lent<'_, EmitIn>, mut o: Out<'_, FramerOut>) -> Outcome {
        let bytes = i.bytes();
        with(&inst, i.framing, i.field(|x| &x.sink), &mut o, |f| {
            f.emitting.extend_from_slice(bytes);
            if i.end_of_frame != 0 && !f.emitting.is_empty() {
                let line = std::mem::take(&mut f.emitting);
                if !one_line(&line) {
                    return Err(Refusal::failed(
                        "emit: a frame holding a newline, or ending in a carriage return, is not one line",
                    ));
                }
                f.outbound.extend(line);
                f.outbound.push_back(b'\n');
            }
            Ok(())
        })
    }
}

/// `refuse`: the refusal's bytes and a `0x0A` are the wire bytes.
pub struct Refuse;
impl SafeSlot for Refuse {
    type In = RefuseIn;
    type Out = FramerOut;
    type State = State;
    fn call(
        inst: Instance<'_, State>,
        i: Lent<'_, RefuseIn>,
        mut o: Out<'_, FramerOut>,
    ) -> Outcome {
        let bytes = i.bytes();
        with(&inst, i.framing, i.field(|x| &x.sink), &mut o, |f| {
            if !bytes.is_empty() {
                f.outbound.extend(bytes);
                f.outbound.push_back(b'\n');
            }
            Ok(())
        })
    }
}

/// `timer`: this framer asks for no deadline; a call answers what is still owed.
pub struct Timer;
impl SafeSlot for Timer {
    type In = FramingIn;
    type Out = FramerOut;
    type State = State;
    fn call(
        inst: Instance<'_, State>,
        i: Lent<'_, FramingIn>,
        mut o: Out<'_, FramerOut>,
    ) -> Outcome {
        with(&inst, i.framing, i.field(|x| &x.sink), &mut o, |_| Ok(()))
    }
}

/// `finish`: the framing is forgotten; no frame follows.
pub struct Finish;
impl SafeSlot for Finish {
    type In = FinishIn;
    type Out = FramerOut;
    type State = State;
    fn call(
        inst: Instance<'_, State>,
        i: Lent<'_, FinishIn>,
        mut o: Out<'_, FramerOut>,
    ) -> Outcome {
        let s = match framings(&inst) {
            Ok(s) => s,
            Err(r) => return o.fail(r),
        };
        if s.lock().remove(&i.framing).is_none() {
            return o.fail(Refusal::failed("finish: no such framing"));
        }
        o.set(|o| &o.yielded.flags, YIELD_ENDED);
        Outcome::Ready
    }
}

/// `encode`: a line has no head, so the envelope is its body; fields are not written. A body that
/// could not be one line is refused where the plane can still do something about it.
pub struct Encode;
impl SafeSlot for Encode {
    type In = EncodeIn;
    type Out = FramerOut;
    type State = State;
    fn call(_: Instance<'_, State>, i: Lent<'_, EncodeIn>, mut o: Out<'_, FramerOut>) -> Outcome {
        let body = i.body();
        if !one_line(body) {
            return o.fail(Refusal::failed(
                "encode: a body holding a newline, or ending in a carriage return, is not one line",
            ));
        }
        let mut wire = i.field(|x| &x.sink).wire();
        if body.len() > wire.cap() {
            // `encode` renders a whole message at once: the host gives it room for the body.
            return o.fail(Refusal::failed(
                "encode: the wire buffer is smaller than the body",
            ));
        }
        wire.extend(body);
        o.set(|o| &o.yielded.wire_len, body.len() as u64);
        Outcome::Ready
    }
}

/// Whether `bytes` can be ONE line on this wire. A `0x0A` in them is the byte that ends a frame, so
/// writing one through would let the sender choose where a frame ends; a trailing `0x0D` is stripped
/// by the reader, so it would come back a byte short of what was written.
fn one_line(bytes: &[u8]) -> bool {
    !bytes.contains(&b'\n') && bytes.last() != Some(&b'\r')
}

/// Move as many bytes off the front of `from` as `to` has room for; answers how many.
fn drain_into(from: &mut VecDeque<u8>, to: &mut HostBuf<'_, u8>) -> usize {
    let (a, b) = from.as_slices();
    let mut n = to.stream(a);
    if n == a.len() {
        n += to.stream(b);
    }
    from.drain(..n);
    n
}

impl Framing {
    /// Take the far side's bytes: whole lines become ready frames; a line that is too long, or cut
    /// by the far side's end, becomes a framing error.
    fn read(&mut self, bytes: &[u8], end: bool) {
        if self.ended {
            return;
        }
        for &b in bytes {
            if self.skipping {
                self.skipping = b != b'\n';
                continue;
            }
            if b == b'\n' {
                let mut line = std::mem::take(&mut self.line);
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                // A BLANK line carries no frame; a line of spaces is a payload.
                if !line.is_empty() {
                    self.ready.push_back(Ready {
                        bytes: line,
                        given: 0,
                        error: None,
                    });
                }
            } else if self.line.len() == MAX_LINE_BYTES {
                self.line = Vec::new();
                self.skipping = true;
                self.error("ingest: a line ran past the reading budget");
            } else {
                self.line.push(b);
            }
        }
        if end {
            self.ended = true;
            // The far side stopped partway through a line: where it ended is not guessed.
            self.skipping = false;
            if !self.line.is_empty() {
                self.line = Vec::new();
                self.error("ingest: the far side ended mid-line");
            }
        }
    }

    fn error(&mut self, why: &'static str) {
        self.ready.push_back(Ready {
            bytes: Vec::new(),
            given: 0,
            error: Some(why),
        });
    }

    /// Answer into `sink` what this framing owes: wire bytes, then ready lines as frames (the piece
    /// that drains one ends it), then the framing error once every frame before it is out, then the
    /// connection's end once nothing is left.
    fn answer(&mut self, sink: Lent<'_, FramerSink>, o: &mut Out<'_, FramerOut>) -> Outcome {
        let wire = drain_into(&mut self.outbound, &mut sink.wire());
        o.set(|o| &o.yielded.wire_len, wire as u64);
        let mut pieces = sink.pieces();
        let mut frame = sink.frame();
        while let Some(front) = self.ready.front_mut() {
            if front.error.is_some() {
                break;
            }
            let room = frame.cap().saturating_sub(frame.asked());
            if pieces.asked() >= pieces.cap() || room == 0 {
                break;
            }
            let at = frame.asked();
            let n = frame.stream(&front.bytes[front.given..]);
            front.given += n;
            let done = front.given == front.bytes.len();
            pieces.push(FramePiece {
                stream: 0,
                offset: at as u64,
                len: n as u64,
                code: 0,
                status_class: 0,
                flags: if done { PIECE_END_OF_FRAME } else { 0 },
                _reserved: 0,
                retry_after_secs: 0,
            });
            if done {
                self.ready.pop_front();
            }
        }
        o.set(|o| &o.yielded.frame_len, frame.asked() as u64);
        o.set(|o| &o.yielded.pieces_len, pieces.asked() as u64);
        if !self.outbound.is_empty() {
            o.set(|o| &o.yielded.flags, YIELD_MORE);
            return Outcome::Ready;
        }
        if let Some(why) = self.ready.front().map(|r| r.error) {
            // Frames after the error wait behind it; frames before it are out first.
            let Some(why) = why else {
                o.set(|o| &o.yielded.flags, YIELD_MORE);
                return Outcome::Ready;
            };
            if wire != 0 || frame.asked() != 0 {
                o.set(|o| &o.yielded.flags, YIELD_MORE);
                return Outcome::Ready;
            }
            self.ready.pop_front();
            return o.fail(Refusal::failed(why));
        }
        o.set(
            |o| &o.yielded.flags,
            if self.ended { YIELD_ENDED } else { 0 },
        );
        Outcome::Ready
    }
}

busbar_contract::plugin_door! {
    ops: Ops,
    statement: STATEMENT,
    lifecycle: life(Framings),
    kind_ops: {
        listen: Safe<Refused<ListenIn, ListenOut>>,
        accept: Safe<Refused<AcceptIn, AcceptOut>>,
        dial: Safe<Refused<DialIn, ConnOut>>,
        read: Safe<Refused<ReadIn, IoOut>>,
        write: Safe<Refused<WriteIn, IoOut>>,
        flush: Safe<Refused<ConnIn, OutHead>>,
        shut: Safe<Refused<ShutIn, OutHead>>,
        arrival: Safe<Refused<ArrivalIn, ArrivalOut>>,
        locate: Safe<Refused<LocateIn, LocateOut>>,
        begin: Safe<Begin>,
        ingest: Safe<Ingest>,
        emit: Safe<Emit>,
        encode: Safe<Encode>,
        refuse: Safe<Refuse>,
        finish: Safe<Finish>,
        detach: Safe<Refused<FramingIn, FramerOut>>,
        adopt: Safe<Refused<AdoptIn, FramerOut>>,
        timer: Safe<Timer>,
    },
}
