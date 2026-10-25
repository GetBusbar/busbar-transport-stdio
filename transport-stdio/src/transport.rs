// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! The `stdio` CARRIER, as the kind's own file (`BUSBAR-1.6.0.md` THE DESIGN, §2 and §5;
//! TRANSPORT-STACK (2)): the carrier ops over the HOST'S I/O (`busbar_contract::abi::host::io`,
//! `io.*`), and the framer ops refused. The host owns the program and its pipes; this carrier holds
//! only the host's opaque handle and owns the policy over it:
//!
//! * `dial` spawns the PROGRAM it is lent — an absolute path, its arguments and its whole
//!   environment, no shell — through the host (`io.spawn`, which spawns only what it admitted). An
//!   authority is not this carrier's destination, and it listens on nothing.
//! * `read` answers ONE FRAME PER LINE: the bytes up to each `0x0A`, the newline (and one carriage
//!   return before it) stripped; the read that completes a line carries `READ_END_OF_FRAME`, and a
//!   line longer than the host's buffer comes in several reads. A blank line is an empty frame. A
//!   line past the LINE CEILING fails the read. At the program's end a final unterminated line is
//!   a frame, then the clean end (`len` `0`, no frame bit).
//! * `write` gathers a frame's bytes until `WRITE_END_OF_FRAME`, then queues them as ONE line, the
//!   `0x0A` appended. A frame past the line ceiling, holding a newline, or ending in a carriage
//!   return cannot be one line: each FAILS before a byte of it is written, the one class the
//!   contract's carrier script states for a frame the carrier will not carry. What the pipe has not
//!   taken is written by the next `write` or `flush`.
//! * THE LINE CEILING is the deployment's request-body bound, the one setting this carrier reads
//!   ([`BODY_MAX_BYTES`], at its 1.5.5 path; [`MAX_LINE_BYTES`] when the host hands none), taken at
//!   `open` and moved by each `refresh`: the host holds one inbound message of a byte channel to
//!   that bound, so a message the gateway takes through its front door crosses a stdio pipe whole,
//!   either way.
//! * `flush` writes what is queued; `shut` closes the handle (the host kills the program) and
//!   forgets the connection; `arrival` answers no far end and no port.
//! * THE SUPERVISOR is this carrier's (THE DESIGN §2, l.388; [`crate::supervisor`]): a dial for a
//!   member (`DialIn::supervision_key` not `0`) is REFUSED while that member's program is held
//!   down, and every end of it — its output closed, a failed pipe, a line past the ceiling, a failed
//!   spawn, or found ended when it is shut — is counted on the host's one clock. A clean shut of a
//!   running program is no end.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::Poll;

use crate::supervisor::Supervisor;
use busbar_contract::abi::mechanism::call::{OutHead, Outcome};
use busbar_contract::abi::mechanism::ticket::CompletionHandle;
use busbar_contract::abi::sdk::door::{AbiIn, AbiOut};
use busbar_contract::abi::sdk::io::{Io, IoFailure};
use busbar_contract::abi::sdk::life::{settings_object, Held, Life, Refreshed, Refusal};
use busbar_contract::abi::sdk::{Instance, Lent, Out, SafeSlot};
use busbar_contract::abi::transport::{
    ArrivalIn, ArrivalOut, ConnIn, ConnOut, DialIn, IoOut, ReadIn, ShutIn, WriteIn,
    CANCEL_NOTHING_MOVED, DEST_PROGRAM, READ_END_OF_FRAME, WRITE_END_OF_FRAME,
};
use busbar_contract::transport::registry::DEFAULT_REQUEST_BODY_MAX_BYTES;

/// The setting this carrier reads, at its 1.5.5 path: the deployment's request-body bound, which
/// is its LINE CEILING, the longest line it frames before its terminator (a line is one frame on
/// this wire). Past it the line is a framing fault, not a big frame.
pub const BODY_MAX_BYTES: &str = "limits.request_body_max_bytes";

/// The line ceiling when the host hands no [`BODY_MAX_BYTES`]: the host's own default request-body
/// bound (32 MiB), the default the tail declares for the setting.
pub const MAX_LINE_BYTES: usize = DEFAULT_REQUEST_BODY_MAX_BYTES;

/// The queued bytes past which a `write` takes nothing more and waits for the pipe to drain. It
/// gates further writes; the queue itself holds at most this plus one line (at most the line
/// ceiling), the frame that crossed it.
pub const MAX_QUEUED_BYTES: usize = 256 * 1024;

/// The capacity a connection's buffer keeps once it is empty: what one large message grew it past
/// this is given back, so a long-lived connection holds a working size, not its high-water mark.
/// A buffer within it is kept whole, so the data path allocates nothing once warm.
const KEEP_BYTES: usize = 256 * 1024;

/// How many bytes one pipe read takes.
const READ_CHUNK: usize = 16 * 1024;

/// Why `validate`, `open` or `refresh` fails when the request-body bound is no count of bytes.
pub const NOT_A_COUNT: &str = "settings: limits.request_body_max_bytes must be a count of bytes";

/// The line ceiling `settings` states: its [`BODY_MAX_BYTES`], or [`MAX_LINE_BYTES`] when it states
/// none.
fn ceiling(settings: &[u8]) -> Result<usize, Refusal> {
    match settings_object(settings)?.get(BODY_MAX_BYTES) {
        None => Ok(MAX_LINE_BYTES),
        Some(v) => v
            .as_u64()
            .and_then(|n| usize::try_from(n).ok())
            .ok_or_else(|| Refusal::failed(NOT_A_COUNT)),
    }
}

/// `buf` emptied, giving back what it grew past [`KEEP_BYTES`].
fn emptied(buf: &mut Vec<u8>) {
    buf.clear();
    buf.shrink_to(KEEP_BYTES);
}

/// Why a member's dial is refused while its program is held down.
pub const HELD_DOWN: &str =
    "dial: the program keeps ending; its restarts are held down by the stdio supervisor";
/// Why a member's dial fails when the host's clock does not answer (none lent, or it failed): a
/// member's program is never run unsupervised.
pub const NO_CLOCK: &str =
    "dial: the host's clock did not answer; a member's program is not run unsupervised";

/// The most output a program that exited can have left unread: one full pipe. The host reads the
/// program's pipe itself and holds none of it, and a pipe is 64 KiB unless the program resizes its
/// own, at most to 1 MiB (Linux `fs.pipe-max-size`, its default).
const PIPE_MAX_BYTES: usize = 1024 * 1024;

/// Reads a program's output may take to find its end at a shut, at most: a full pipe, then the read
/// that finds its end. Past it, the program is still writing: it is running.
const SHUT_PROBE_READS: usize = PIPE_MAX_BYTES / READ_CHUNK + 1;

/// Whether the program behind `handle` has ENDED: its remaining output read and discarded (the
/// connection is being shut) until its end, an error, or PENDING (running).
fn found_ended(io: &mut Io<'_>, handle: u64) -> bool {
    let mut chunk = [0_u8; READ_CHUNK];
    for _ in 0..SHUT_PROBE_READS {
        match io.read(handle, &mut chunk) {
            Poll::Pending => return false,
            Poll::Ready(Ok(0)) | Poll::Ready(Err(_)) => return true,
            Poll::Ready(Ok(_)) => {}
        }
    }
    false
}

/// Why a read fails.
pub const TOO_LONG: &str = "a line ran past the stdio line ceiling without a newline";
/// Why a write fails when the frame it gathers passes the line ceiling.
pub const FRAME_TOO_LONG: &str = "a frame ran past the stdio line ceiling";
/// Why a write fails when its frame cannot be one line.
pub const NOT_ONE_LINE: &str =
    "a message holding a newline, or ending in a carriage return, is not one line";

/// The line being answered, as positions in [`Conn::inbound`].
#[derive(Clone, Copy)]
struct Line {
    /// The next byte to answer.
    at: usize,
    /// Where the line ends (its terminator excluded).
    end: usize,
    /// Where the bytes after it begin (its terminator included).
    next: usize,
}

// ── the instance ─────────────────────────────────────────────────────────────────────────────────

/// What one instance carries: its connections, each by the token it minted, the supervision of
/// every member it has run a program for, by the member's supervision key, and its line ceiling.
pub struct Carried {
    conns: Mutex<HashMap<u64, Arc<Mutex<Conn>>>>,
    next: AtomicU64,
    supervised: Mutex<HashMap<u64, Supervisor>>,
    /// The line ceiling: the deployment's request-body bound, as `open` or the last `refresh` was
    /// handed it.
    ceiling: AtomicUsize,
}

/// One program's connection: the host's handle, the line being cut on the way in, the frame being
/// gathered and the bytes queued on the way out. A line is answered straight out of `inbound`, so
/// the data path allocates nothing once its buffers are warm (THE DESIGN §11.2).
#[derive(Default)]
struct Conn {
    io: u64,
    /// The member's supervision key (`0`: unsupervised).
    key: u64,
    /// Its program's end was counted.
    counted: bool,
    /// An op on it failed on its program's side (a failed pipe, a line past the ceiling): its
    /// generation ends, an end like its output closing.
    faulted: bool,
    /// Bytes the program wrote; those before `head` are answered.
    inbound: Vec<u8>,
    /// Where the bytes not yet answered begin in `inbound`.
    head: usize,
    /// How far `inbound` is known to hold no newline: the next scan starts there.
    scanned: usize,
    /// The line being answered.
    line: Option<Line>,
    /// The program's output ended.
    ended: bool,
    /// The frame being written, gathered until its end.
    message: Vec<u8>,
    /// Bytes owed to the program, from `sent` on.
    outbound: Vec<u8>,
    sent: usize,
}

/// What every slot reads: the SDK's lifecycle state over [`Carried`].
type State = Held<Carried>;

impl Life for Carried {
    /// A PENDING carrier op moved nothing, so a cancel finds nothing moved.
    const CANCEL: u32 = CANCEL_NOTHING_MOVED;

    /// The one setting it reads, [`BODY_MAX_BYTES`], is a count of bytes.
    fn validate(settings: &[u8]) -> Result<(), Refusal> {
        ceiling(settings).map(|_| ())
    }

    fn open(settings: &[u8], _: &[&[u8]], _: u64) -> Result<Self, Refusal> {
        Ok(Self {
            conns: Mutex::new(HashMap::new()),
            next: AtomicU64::new(1),
            supervised: Mutex::new(HashMap::new()),
            ceiling: AtomicUsize::new(ceiling(settings)?),
        })
    }

    /// A config apply moves the line ceiling; it holds from each connection's next op.
    fn refresh(&self, settings: &[u8], _: &[&[u8]], _: u64) -> Result<Refreshed, Refusal> {
        self.ceiling.store(ceiling(settings)?, Ordering::Relaxed);
        Ok(Refreshed::default())
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Carried {
    fn conn(&self, token: u64) -> Option<Arc<Mutex<Conn>>> {
        lock(&self.conns).get(&token).cloned()
    }

    /// The member `key`'s program ended at `now`: counted once per connection.
    /// An end met while the host's clock does not answer stays uncounted, so the
    /// connection's shut counts it.
    fn ended(&self, c: &mut Conn, now: Option<u64>) {
        if c.key == 0 || c.counted {
            return;
        }
        let Some(now) = now else {
            return;
        };
        c.counted = true;
        lock(&self.supervised)
            .entry(c.key)
            .or_default()
            .crashed(now);
    }

    /// An op on the member's connection failed on its program's side (a failed pipe, a line past
    /// the ceiling): the host ends the generation, so it is an end, counted now or, met while the
    /// clock does not answer, at the connection's shut.
    fn faulted(&self, c: &mut Conn, now: Option<u64>) {
        c.faulted = true;
        self.ended(c, now);
    }

    /// The line ceiling now.
    fn ceiling(&self) -> usize {
        self.ceiling.load(Ordering::Relaxed)
    }
}

/// The host's monotonic clock now (THE DESIGN §1: one clock, lent to transports, l.291); `None`
/// when the host lends no clock.
fn now(i: &Instance<'_, State>) -> Option<u64> {
    let services = i.get()?.host()?.services()?;
    let handle = CompletionHandle {
        ticket: i.ticket(),
        seq: 0,
        _reserved: 0,
    };
    services.clock_now(handle).ok().map(|r| r.mono_ns)
}

/// The open instance and the host's I/O for this op, or the refusal a call on none earns.
fn carried<'a>(i: &Instance<'a, State>) -> Result<(&'a Carried, Io<'a>), Refusal> {
    let held = i.get().ok_or_else(|| Refusal::failed("no open instance"))?;
    let io = held
        .host()
        .map(|h| h.io(i.ticket()))
        .filter(Io::armed)
        .ok_or_else(|| Refusal::failed("the host lent this carrier no I/O"))?;
    Ok((held.life(), io))
}

/// The host's answer, as this op's refusal: the host's policy is REFUSED, the system's FAILED.
fn refusal(e: &IoFailure) -> Refusal {
    match e {
        IoFailure::Refused(t) => Refusal::refused(t.clone()),
        other => Refusal::failed(other.text().to_owned()),
    }
}

/// Run `body` over the instance, its I/O and the connection `conn`, failing the op without one.
macro_rules! over {
    ($inst:expr, $o:expr, $conn:expr, |$carried:ident, $io:ident, $c:ident| $body:block) => {{
        let ($carried, mut $io) = match carried(&$inst) {
            Ok(x) => x,
            Err(r) => return $o.fail(r),
        };
        let Some(conn) = $carried.conn($conn) else {
            return $o.fail(Refusal::failed("no such connection"));
        };
        let mut $c = lock(&conn);
        $body
    }};
}

impl Conn {
    /// The next whole line in `inbound` (the newline and a carriage return before it stripped);
    /// at the program's end, the unterminated rest. Scans only the bytes no earlier scan saw.
    /// `Err` for a line past the ceiling `max`.
    fn next_line(&mut self, max: usize) -> Result<Option<Line>, &'static str> {
        let from = self.scanned.max(self.head);
        match self.inbound[from..].iter().position(|&b| b == b'\n') {
            Some(rel) => {
                let newline = from + rel;
                let mut end = newline;
                if end > self.head && self.inbound[end - 1] == b'\r' {
                    end -= 1;
                }
                if end - self.head > max {
                    return Err(TOO_LONG);
                }
                self.scanned = newline + 1;
                Ok(Some(Line {
                    at: self.head,
                    end,
                    next: newline + 1,
                }))
            }
            None => {
                self.scanned = self.inbound.len();
                let rest = self.inbound.len() - self.head;
                // A carriage return at the end of a running program's output may be the CR of a
                // CRLF whose LF has not arrived: it is the terminator's, not the line's.
                let cr =
                    usize::from(!self.ended && rest > 0 && self.inbound.last() == Some(&b'\r'));
                if rest - cr > max {
                    Err(TOO_LONG)
                } else if self.ended && rest > 0 {
                    Ok(Some(Line {
                        at: self.head,
                        end: self.inbound.len(),
                        next: self.inbound.len(),
                    }))
                } else {
                    Ok(None)
                }
            }
        }
    }

    /// The line answered: the bytes after it are next. Answered bytes are dropped once nothing is
    /// left after them (the buffer giving back what a large line grew it past [`KEEP_BYTES`]), and
    /// moved out of the way before the next pipe read, keeping the buffer.
    fn answered(&mut self, next: usize) {
        self.line = None;
        self.head = next;
        if self.head == self.inbound.len() {
            emptied(&mut self.inbound);
            self.head = 0;
            self.scanned = 0;
        }
    }

    /// Room for the next pipe read: the answered bytes leave the front of `inbound`, and the
    /// buffer gives back what a large line grew it past [`KEEP_BYTES`].
    fn compact(&mut self) {
        if self.head > 0 {
            self.inbound.drain(..self.head);
            self.scanned -= self.head.min(self.scanned);
            self.head = 0;
            self.inbound.shrink_to(KEEP_BYTES);
        }
    }

    /// Write what is queued, as far as the pipe takes it now: `Pending` while some is left.
    fn drain(&mut self, io: &mut Io<'_>) -> Poll<Result<(), IoFailure>> {
        while self.sent < self.outbound.len() {
            match io.write(self.io, &self.outbound[self.sent..]) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(n)) => self.sent += n,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            }
        }
        emptied(&mut self.outbound);
        self.sent = 0;
        Poll::Ready(Ok(()))
    }
}

// ── the roles it does not play ──────────────────────────────────────────────────────────────────

/// A slot of a role this carrier does not play (every framer op; `listen` and `accept`: a stdio
/// carrier listens on nothing): REFUSED.
pub struct Refused<I, O>(PhantomData<(I, O)>);

impl<I: AbiIn, O: AbiOut> SafeSlot for Refused<I, O> {
    type In = I;
    type Out = O;
    type State = State;
    fn call(_: Instance<'_, State>, _: Lent<'_, I>, _: Out<'_, O>) -> Outcome {
        Outcome::Refused
    }
}

// ── the carrier ──────────────────────────────────────────────────────────────────────────────────

/// `dial`: spawn the program, through the host. An authority is not this carrier's destination.
pub struct Dial;
impl SafeSlot for Dial {
    type In = DialIn;
    type Out = ConnOut;
    type State = State;
    fn call(inst: Instance<'_, State>, i: Lent<'_, DialIn>, mut o: Out<'_, ConnOut>) -> Outcome {
        let Some(dest) = i.dest() else {
            return o.fail(Refusal::failed("dial: no destination"));
        };
        if dest.kind != DEST_PROGRAM {
            return o.fail(Refusal::refused(
                "dial: a stdio carrier reaches a program, never an authority",
            ));
        }
        let (c, mut io) = match carried(&inst) {
            Ok(x) => x,
            Err(r) => return o.fail(r),
        };
        // THE SUPERVISOR: a member's program is spawned only when its policy lets it restart, on
        // the host's clock; a member with no clock to be supervised by is not run unsupervised.
        let key = i.supervision_key;
        let at = if key == 0 {
            None
        } else {
            let Some(at) = now(&inst) else {
                return o.fail(Refusal::failed(NO_CLOCK));
            };
            let held_down = {
                let mut supervised = lock(&c.supervised);
                // A key whose state holds nothing reads as a fresh one: dropped, so the map holds
                // only members inside their window and members held down until declared anew.
                supervised.retain(|_, s| !s.idle(at));
                supervised.get(&key).is_some_and(|s| !s.may_restart(at))
            };
            if held_down {
                return o.fail(Refusal::refused(HELD_DOWN));
            }
            Some(at)
        };
        match io.spawn_dest(dest) {
            Ok(h) => {
                let token = c.next.fetch_add(1, Ordering::Relaxed);
                let conn = Conn {
                    io: h,
                    key,
                    ..Conn::default()
                };
                lock(&c.conns).insert(token, Arc::new(Mutex::new(conn)));
                o.set(|o| &o.conn, token);
                Outcome::Ready
            }
            Err(e) => {
                // A spawn that fails repeats on every attempt: it is counted like any end.
                if let Some(at) = at {
                    lock(&c.supervised).entry(key).or_default().crashed(at);
                }
                o.fail(refusal(&e))
            }
        }
    }
}

/// `read`: the next line, or the rest of the one being answered, into the host's buffer.
pub struct Read;
impl SafeSlot for Read {
    type In = ReadIn;
    type Out = IoOut;
    type State = State;
    fn call(inst: Instance<'_, State>, i: Lent<'_, ReadIn>, mut o: Out<'_, IoOut>) -> Outcome {
        over!(inst, o, i.conn, |carried, io, c| {
            let mut buf = i.buf();
            let max = carried.ceiling();
            loop {
                if let Some(mut line) = c.line {
                    let n = buf.stream(&c.inbound[line.at..line.end]);
                    line.at += n;
                    let done = line.at == line.end;
                    if done {
                        c.answered(line.next);
                    } else {
                        c.line = Some(line);
                    }
                    o.set(|o| &o.len, n as u64);
                    o.set(|o| &o.flags, if done { READ_END_OF_FRAME } else { 0 });
                    return Outcome::Ready;
                }
                match c.next_line(max) {
                    Ok(Some(line)) => {
                        c.line = Some(line);
                        continue;
                    }
                    Ok(None) if c.ended => {
                        o.set(|o| &o.len, 0);
                        return Outcome::Ready;
                    }
                    Ok(None) => {}
                    Err(why) => {
                        // The program's line ran past the ceiling: the host ends the generation
                        // and shuts the program still running, so the end is counted here.
                        carried.faulted(&mut c, now(&inst));
                        return o.fail(Refusal::failed(why));
                    }
                }
                c.compact();
                let mut chunk = [0_u8; READ_CHUNK];
                match io.read(c.io, &mut chunk) {
                    Poll::Pending => return Outcome::Pending,
                    Poll::Ready(Ok(0)) => {
                        c.ended = true;
                        carried.ended(&mut c, now(&inst));
                    }
                    Poll::Ready(Ok(n)) => c.inbound.extend_from_slice(&chunk[..n]),
                    Poll::Ready(Err(e)) => {
                        carried.faulted(&mut c, now(&inst));
                        return o.fail(refusal(&e));
                    }
                }
            }
        })
    }
}

/// `write`: a frame's bytes gathered until its end, then queued as one line and written as far as
/// the pipe takes them.
pub struct Write;
impl SafeSlot for Write {
    type In = WriteIn;
    type Out = IoOut;
    type State = State;
    fn call(inst: Instance<'_, State>, i: Lent<'_, WriteIn>, mut o: Out<'_, IoOut>) -> Outcome {
        let bytes = i.bytes();
        let end = i.flags & WRITE_END_OF_FRAME != 0;
        over!(inst, o, i.conn, |carried, io, c| {
            // What the pipe has not taken goes first; past the cap, nothing more is taken.
            match c.drain(&mut io) {
                Poll::Ready(Err(e)) => {
                    carried.faulted(&mut c, now(&inst));
                    return o.fail(refusal(&e));
                }
                Poll::Pending if c.outbound.len() - c.sent >= MAX_QUEUED_BYTES => {
                    return Outcome::Pending;
                }
                _ => {}
            }
            // A frame is bounded as a line is: past the ceiling it fails, never gathered.
            if c.message.len() + bytes.len() > carried.ceiling() {
                emptied(&mut c.message);
                return o.fail(Refusal::failed(FRAME_TOO_LONG));
            }
            c.message.extend_from_slice(bytes);
            if end {
                // The frame is checked and queued in place; its buffer is kept for the next one,
                // up to [`KEEP_BYTES`].
                let c = &mut *c;
                let one_line = !c.message.contains(&b'\n') && c.message.last() != Some(&b'\r');
                if one_line {
                    c.outbound.extend_from_slice(&c.message);
                    c.outbound.push(b'\n');
                }
                emptied(&mut c.message);
                if !one_line {
                    return o.fail(Refusal::failed(NOT_ONE_LINE));
                }
                if let Poll::Ready(Err(e)) = c.drain(&mut io) {
                    carried.faulted(c, now(&inst));
                    return o.fail(refusal(&e));
                }
            }
            o.set(|o| &o.len, bytes.len() as u64);
            Outcome::Ready
        })
    }
}

/// `flush`: what is queued, written: READY once the pipe took it all.
pub struct Flush;
impl SafeSlot for Flush {
    type In = ConnIn;
    type Out = OutHead;
    type State = State;
    fn call(inst: Instance<'_, State>, i: Lent<'_, ConnIn>, mut o: Out<'_, OutHead>) -> Outcome {
        over!(inst, o, i.conn, |carried, io, c| {
            match c.drain(&mut io) {
                Poll::Pending => Outcome::Pending,
                Poll::Ready(Ok(())) => Outcome::Ready,
                Poll::Ready(Err(e)) => {
                    carried.faulted(&mut c, now(&inst));
                    o.fail(refusal(&e))
                }
            }
        })
    }
}

/// `shut`: the handle is closed (the host kills the program) and the connection forgotten. An
/// unknown connection is already closed.
pub struct Shut;
impl SafeSlot for Shut {
    type In = ShutIn;
    type Out = OutHead;
    type State = State;
    fn call(inst: Instance<'_, State>, i: Lent<'_, ShutIn>, mut o: Out<'_, OutHead>) -> Outcome {
        let (c, mut io) = match carried(&inst) {
            Ok(x) => x,
            Err(r) => return o.fail(r),
        };
        let gone = lock(&c.conns).remove(&i.conn);
        if let Some(held) = gone {
            let mut conn = lock(&held);
            // A member's program found ENDED at its shut (the host's lease found it exited) is an
            // end, as is one whose op failed on its side; one still running (its output PENDING)
            // is a clean shut, no end.
            let open = !conn.ended && !conn.faulted;
            if conn.key != 0 && !conn.counted && open && found_ended(&mut io, conn.io) {
                conn.ended = true;
            }
            if conn.ended || conn.faulted {
                c.ended(&mut conn, now(&inst));
            }
            let _ = io.close(conn.io);
        }
        Outcome::Ready
    }
}

/// `arrival`: a program has no far end and arrived on no port.
pub struct Arrival;
impl SafeSlot for Arrival {
    type In = ArrivalIn;
    type Out = ArrivalOut;
    type State = State;
    fn call(
        inst: Instance<'_, State>,
        i: Lent<'_, ArrivalIn>,
        mut o: Out<'_, ArrivalOut>,
    ) -> Outcome {
        let known = inst.get().is_some_and(|h| h.life().conn(i.conn).is_some());
        if known {
            Outcome::Ready
        } else {
            o.fail(Refusal::failed("arrival: no such connection"))
        }
    }
}

#[cfg(test)]
#[path = "tests/transport_tests.rs"]
mod tests;
