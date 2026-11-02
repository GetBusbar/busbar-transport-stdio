// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! The `stdio` CARRIER: the carrier ops over the HOST'S I/O (`busbar_contract::abi::host::io`,

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::Poll;

use crate::supervisor::Supervisor;
use busbar_contract::abi::mechanism::call::{OutHead, Outcome};
use busbar_contract::abi::mechanism::ticket::{CompletionHandle, Ticket};
use busbar_contract::abi::sdk::conn::Host;
use busbar_contract::abi::sdk::io::{Io, IoFailure};
use busbar_contract::abi::sdk::life::{Held, Life, Refreshed, Refusal};
use busbar_contract::abi::sdk::transport::settings_reading;
use busbar_contract::abi::sdk::{Instance, Lent, Out, SafeSlot};
use busbar_contract::abi::transport::{
    AcceptIn, AcceptOut, AdoptIn, ArrivalIn, ArrivalOut, BeginIn, ConnIn, ConnOut, DialIn, EmitIn,
    EncodeIn, FinishIn, FramerOut, FramingIn, IngestIn, IoOut, ListenIn, ListenOut, LocateIn,
    LocateOut, ReadIn, RefuseIn, ShutIn, WriteIn, CANCEL_NOTHING_MOVED, DEST_PROGRAM, DEST_RETIRED,
    READ_END_OF_FRAME, WRITE_END_OF_FRAME,
};
use busbar_contract::carried;
use busbar_contract::transport::registry::DEFAULT_REQUEST_BODY_MAX_BYTES;

/// The setting this carrier reads: the deployment's request-body bound, which

pub const BODY_MAX_BYTES: &str = "limits.request_body_max_bytes";

/// The line ceiling when the host hands no [`BODY_MAX_BYTES`]: the host's own default request-body

pub const MAX_LINE_BYTES: usize = DEFAULT_REQUEST_BODY_MAX_BYTES;

/// The queued bytes past which a `write` takes nothing more and waits for the pipe to drain. It

pub const MAX_QUEUED_BYTES: usize = 256 * 1024;

/// The capacity a connection's buffer keeps once it is empty: what one large message grew it past

const KEEP_BYTES: usize = 256 * 1024;

/// How many bytes one pipe read takes.
const READ_CHUNK: usize = 16 * 1024;

/// Why `validate`, `open` or `refresh` fails when the request-body bound is no count of bytes.
pub const NOT_A_COUNT: &str =
    "settings: limits.request_body_max_bytes must be a count of bytes above 0";

/// The shared count validator's wording, translated to this carrier's existing byte-count text.
const SDK_NOT_A_COUNT: &str = "settings: limits.request_body_max_bytes must be a count above 0";

/// The line ceiling `settings` states: its [`BODY_MAX_BYTES`], or [`MAX_LINE_BYTES`] when it states

fn ceiling(settings: &[u8]) -> Result<usize, Refusal> {
    let settings = settings_reading(settings, &crate::meta::SETTING_ROWS).map_err(|refusal| {
        if refusal.text() == Some(SDK_NOT_A_COUNT) {
            Refusal::failed(NOT_A_COUNT)
        } else {
            refusal
        }
    })?;
    match settings.get(BODY_MAX_BYTES) {
        None => Ok(MAX_LINE_BYTES),
        Some(v) => v
            .as_u64()
            .and_then(|n| usize::try_from(n).ok())
            .filter(|&n| n > 0)
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

pub const NO_CLOCK: &str =
    "dial: the host's clock did not answer; a member's program is not run unsupervised";

/// The most output a program that exited can have left unread: one full pipe. The host reads the

const PIPE_MAX_BYTES: usize = 1024 * 1024;

/// Reads a program's output may take to find its end at a shut, at most: a full pipe, then the read

const SHUT_PROBE_READS: usize = PIPE_MAX_BYTES / READ_CHUNK + 1;

/// Check the parent process independently of inherited pipes, then use the pipe probe as a

fn found_ended(io: &mut Io<'_>, handle: u64) -> bool {
    if matches!(io.program_exit(handle), Ok(Some(_))) {
        return true;
    }
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
pub const TOO_LONG: &str = "read: a line ran past the stdio line ceiling";
/// Why a write fails when the frame it gathers passes the line ceiling.
pub const FRAME_TOO_LONG: &str = "write: a frame ran past the stdio line ceiling";
/// Why a write fails when its frame cannot be one line.
pub const NOT_ONE_LINE: &str =
    "write: a message holding a newline, or ending in a carriage return, is not one line";

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

/// What one instance carries: its connections, each by the token it minted, the supervision of

pub struct Carried {
    conns: Mutex<HashMap<u64, Arc<Mutex<Conn>>>>,
    next: AtomicU64,
    supervised: Mutex<HashMap<u64, Supervisor>>,
    /// The line ceiling: the deployment's request-body bound, as `open` or the last `refresh` was
    ceiling: AtomicUsize,
}

/// One program's connection: the host's handle, the line being cut on the way in, the frame being
/// gathered and the bytes queued on the way out. A buffered line is answered straight out of
/// `inbound`. Buffers reuse retained capacity; capacity above `KEEP_BYTES` is released when empty.
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
    /// The connection was shut: its handle is closed and it is out of the table.
    shut: bool,
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

    /// `close` (the instance's): every connection handle still held is closed through the host's

    fn close(&self, host: Option<&Host>) {
        let conns: Vec<_> = lock(&self.conns).drain().map(|(_, c)| c).collect();
        if let Some(mut io) = host.map(|h| h.io(Ticket::NONE)).filter(Io::armed) {
            for conn in conns {
                let _ = io.close(lock(&conn).io);
            }
        }
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Carried {
    fn conn(&self, token: u64) -> Option<Arc<Mutex<Conn>>> {
        lock(&self.conns).get(&token).cloned()
    }

    /// The connection `token`, taken out of the table for its shut and marked shut under its lock:

    fn forget(&self, token: u64) -> Option<Arc<Mutex<Conn>>> {
        let held = lock(&self.conns).remove(&token)?;
        lock(&held).shut = true;
        Some(held)
    }

    /// The program under `c.key` ended at `now`: counted once per supervised connection.

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

    fn faulted(&self, c: &mut Conn, now: Option<u64>) {
        c.faulted = true;
        self.ended(c, now);
    }

    /// Whether the member `key`'s program is held down at `at`. A key whose state holds nothing

    fn held_down(&self, key: u64, at: u64) -> bool {
        let mut supervised = lock(&self.supervised);
        supervised.retain(|_, s| !s.idle(at));
        supervised.get(&key).is_some_and(|s| !s.may_restart(at))
    }

    /// The line ceiling now.
    fn ceiling(&self) -> usize {
        self.ceiling.load(Ordering::Relaxed)
    }
}

/// The host's monotonic clock now (one clock, lent to transports); `None`

fn now(i: &Instance<'_, State>) -> Option<u64> {
    let services = i.get()?.host()?.services()?;
    let handle = CompletionHandle {
        ticket: i.ticket(),
        seq: 0,
        _reserved: 0,
    };
    services.clock_now(handle).ok().map(|r| r.mono_ns)
}

/// The host's answer, as this op's refusal: the host's policy is REFUSED, the system's FAILED.
fn refusal(e: &IoFailure) -> Refusal {
    match e {
        IoFailure::Refused(t) => Refusal::refused(t.clone()),
        other => Refusal::failed(other.text().to_owned()),
    }
}

/// The connection an op took out of the table, locked; `None` when it was shut while the op waited

fn live(conn: &Mutex<Conn>) -> Option<MutexGuard<'_, Conn>> {
    let c = lock(conn);
    (!c.shut).then_some(c)
}

/// Run `body` over the instance, its I/O and the connection `conn`, failing the op without one.
macro_rules! over {
    ($op:literal, $inst:expr, $o:expr, $conn:expr, |$carried:ident, $io:ident, $c:ident| $body:block) => {{
        let ($carried, mut $io) = match carried!($inst, $op) {
            Ok(x) => x,
            Err(r) => return $o.fail(r),
        };
        let Some(conn) = $carried.conn($conn) else {
            return $o.fail(Refusal::failed(concat!($op, ": no such connection")));
        };
        let Some(mut $c) = live(&conn) else {
            return $o.fail(Refusal::failed(concat!($op, ": no such connection")));
        };
        $body
    }};
}

impl Conn {
    /// The next whole line in `inbound` (the newline and a carriage return before it stripped);

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
                Poll::Pending => {
                    if self.sent >= self.outbound.len() / 2 {
                        self.outbound.drain(..self.sent);
                        self.sent = 0;
                    }
                    return Poll::Pending;
                }
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(IoFailure::Failed(
                        "write: the program's pipe accepted no bytes (write zero)".to_owned(),
                    )));
                }
                Poll::Ready(Ok(n)) => self.sent += n,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            }
        }
        emptied(&mut self.outbound);
        self.sent = 0;
        Poll::Ready(Ok(()))
    }
}

busbar_contract::not_served! {
    state: State, role: "stdio carrier";
    /// `listen`: a stdio carrier listens on nothing.
    Listen(ListenIn => ListenOut) "listen";
    /// `accept`: nothing is listened on.
    Accept(AcceptIn => AcceptOut) "accept";
    /// `locate`: a framer op.
    Locate(LocateIn => LocateOut) "locate";
    /// `begin`: a framer op.
    Begin(BeginIn => FramerOut) "begin";
    /// `ingest`: a framer op.
    Ingest(IngestIn => FramerOut) "ingest";
    /// `emit`: a framer op, including an `EMIT_TRAILERS` final field block; always refused.
    Emit(EmitIn => FramerOut) "emit";
    /// `encode`: a framer op.
    Encode(EncodeIn => FramerOut) "encode";
    /// `refuse`: a framer op.
    RefuseFrame(RefuseIn => FramerOut) "refuse";
    /// `finish`: a framer op.
    Finish(FinishIn => FramerOut) "finish";
    /// `detach`: a framer op.
    Detach(FramingIn => FramerOut) "detach";
    /// `adopt`: a framer op.
    Adopt(AdoptIn => FramerOut) "adopt";
    /// `timer`: a framer op.
    Timer(FramingIn => FramerOut) "timer";
}

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
        if dest.kind == DEST_RETIRED {
            let Some(held) = inst.get() else {
                return o.fail(Refusal::failed("dial: no open instance"));
            };
            let retired = {
                let mut supervised = lock(&held.life().supervised);
                supervised.remove(&i.supervision_key)
            };
            drop(retired);
            o.set(|o| &o.conn, 0);
            return Outcome::Ready;
        }
        if dest.kind != DEST_PROGRAM {
            return o.fail(Refusal::refused(
                "dial: a stdio carrier reaches a program, never an authority",
            ));
        }
        let (c, mut io) = match carried!(inst, "dial") {
            Ok(x) => x,
            Err(r) => return o.fail(r),
        };
        let key = i.supervision_key;
        let at = if key == 0 {
            None
        } else {
            let Some(at) = now(&inst) else {
                return o.fail(Refusal::failed(NO_CLOCK));
            };
            if c.held_down(key, at) {
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
                if let Some(at) = at {
                    let at = now(&inst).unwrap_or(at);
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
        over!("read", inst, o, i.conn, |carried, io, c| {
            let mut buf = i.buf();
            let max = carried.ceiling();
            // Zeroed once per call, not per pipe read: the code forbids `unsafe`, and a read
            // overwrites what it fills.
            let mut chunk = [0_u8; READ_CHUNK];
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

pub struct Write;
impl SafeSlot for Write {
    type In = WriteIn;
    type Out = IoOut;
    type State = State;
    fn call(inst: Instance<'_, State>, i: Lent<'_, WriteIn>, mut o: Out<'_, IoOut>) -> Outcome {
        let bytes = i.bytes();
        let end = i.flags & WRITE_END_OF_FRAME != 0;
        over!("write", inst, o, i.conn, |carried, io, c| {
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
            if c.message.len() + bytes.len() > carried.ceiling() {
                emptied(&mut c.message);
                return o.fail(Refusal::failed(FRAME_TOO_LONG));
            }
            if !end {
                c.message.extend_from_slice(bytes);
            } else {
                let c = &mut *c;
                // A frame that arrives whole in one write is checked where it lies and queued
                // straight to `outbound`, copied once; a gathered one is its gathered bytes.
                let frame: &[u8] = if c.message.is_empty() {
                    bytes
                } else {
                    c.message.extend_from_slice(bytes);
                    &c.message
                };
                let one_line = !frame.contains(&b'\n') && frame.last() != Some(&b'\r');
                if one_line {
                    c.outbound.extend_from_slice(frame);
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
        over!("flush", inst, o, i.conn, |carried, io, c| {
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

pub struct Shut;
impl SafeSlot for Shut {
    type In = ShutIn;
    type Out = OutHead;
    type State = State;
    fn call(inst: Instance<'_, State>, i: Lent<'_, ShutIn>, mut o: Out<'_, OutHead>) -> Outcome {
        let (c, mut io) = match carried!(inst, "shut") {
            Ok(x) => x,
            Err(r) => return o.fail(r),
        };
        if let Some(held) = c.forget(i.conn) {
            let mut conn = lock(&held);

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
        let Some(h) = inst.get() else {
            return o.fail(Refusal::failed("arrival: no open instance"));
        };
        if h.life().conn(i.conn).is_some() {
            Outcome::Ready
        } else {
            o.fail(Refusal::failed("arrival: no such connection"))
        }
    }
}

#[cfg(test)]
#[path = "tests/transport_tests.rs"]
mod tests;
