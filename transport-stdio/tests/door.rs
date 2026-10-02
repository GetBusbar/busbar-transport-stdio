// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! The line framer, driven through its own table the way the host drives it: every answer is
//! judged by the kind's `check_framer`, a full sink is back-pressure re-called with no new bytes,
//! and every byte comes out exactly once, in order. (An integration test, so the crate itself stays
//! `#![forbid(unsafe_code)]`: driving a raw table is the host's side, and needs it.)

use std::ffi::c_void;
use std::mem::{size_of, zeroed};

use busbar_contract::abi::mechanism::call::{AbiStr, Field, InHead, Op, OutHead, Outcome};
use busbar_contract::abi::mechanism::door::Door;
use busbar_contract::abi::mechanism::lifecycle::{slot as life, OpenIn, OpenOut};
use busbar_contract::abi::transport::check::{check_framer, check_tail};
use busbar_contract::abi::transport::{
    slot, AdoptIn, BeginIn, ConnOut, DialIn, EmitIn, EncodeIn, FinishIn, FramePiece, FramerOut,
    FramerSink, FramingIn, IngestIn, LocateIn, LocateOut, Ops, RefuseIn, TransportTail,
    PIECE_END_OF_FRAME, ROLE_FRAMER, SIDE_ACCEPT, UNIT0_FIRST_MESSAGE, YIELD_ENDED, YIELD_MORE,
};
use busbar_transport_stdio::door::{door, MAX_LINE_BYTES, STATEMENT};

fn z<T>() -> T {
    // SAFETY: every `in`/`out` here is plain C data; all-zero is a valid value of each.
    unsafe { zeroed() }
}

fn s(t: &'static str) -> AbiStr {
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

/// The host: an open instance, one framing, and a sink of the given capacities.
struct Host {
    inst: *mut c_void,
    framing: u64,
    wire: Vec<u8>,
    frame: Vec<u8>,
    pieces: Vec<FramePiece>,
    /// Everything the framer answered: wire bytes, and frame bytes with their pieces' flags.
    wire_log: Vec<u8>,
    frames: Vec<(Vec<u8>, u16)>,
    flags: u32,
}

impl Host {
    fn new(wire: usize, frame: usize, pieces: usize) -> Self {
        let mut i: OpenIn = z();
        let mut o: OpenOut = z();
        let r = call(
            ops().head.open,
            std::ptr::null_mut(),
            &mut i,
            &mut o,
            life::OPEN,
        );
        assert_eq!(r, Outcome::Ready);
        let mut h = Self {
            inst: o.instance,
            framing: 0,
            wire: vec![0; wire],
            frame: vec![0; frame],
            pieces: vec![z(); pieces],
            wire_log: Vec::new(),
            frames: Vec::new(),
            flags: 0,
        };
        let mut i: BeginIn = z();
        i.side = SIDE_ACCEPT;
        i.sink = h.sink();
        let mut o: FramerOut = z();
        let r = call(ops().begin, h.inst, &mut i, &mut o, slot::BEGIN);
        h.framing = o.framing;
        h.take(r, &o);
        h
    }

    fn sink(&mut self) -> FramerSink {
        FramerSink {
            wire: self.wire.as_mut_ptr(),
            wire_cap: self.wire.len(),
            frame: self.frame.as_mut_ptr(),
            frame_cap: self.frame.len(),
            pieces: self.pieces.as_mut_ptr(),
            pieces_cap: self.pieces.len(),
            now_monotonic_ns: 1,
            now_unix_ns: 1,
            heads: std::ptr::null_mut(),
            heads_cap: 0,
        }
    }

    fn take(&mut self, r: Outcome, o: &FramerOut) -> Outcome {
        if r != Outcome::Ready {
            return r;
        }
        let n = o.yielded.pieces_len as usize;
        check_framer(
            r,
            o,
            &self.pieces[..n],
            self.wire.len() as u64,
            self.frame.len() as u64,
            self.pieces.len() as u64,
        )
        .expect("the answer passes the kind's check");
        self.wire_log
            .extend_from_slice(&self.wire[..o.yielded.wire_len as usize]);
        for p in &self.pieces[..n] {
            assert_eq!(p.stream, 0, "a byte stream has one stream");
            let at = p.offset as usize;
            self.frames
                .push((self.frame[at..at + p.len as usize].to_vec(), p.flags));
        }
        self.flags = o.yielded.flags;
        r
    }

    /// Re-call `index` with no new bytes until it stops owing.
    fn drain(&mut self, index: u32) {
        while self.flags & YIELD_MORE != 0 {
            let mut o: FramerOut = z();
            let r = if index == slot::INGEST {
                let mut i: IngestIn = z();
                i.framing = self.framing;
                i.sink = self.sink();
                call(ops().ingest, self.inst, &mut i, &mut o, index)
            } else {
                let mut i: EmitIn = z();
                i.framing = self.framing;
                i.sink = self.sink();
                call(ops().emit, self.inst, &mut i, &mut o, index)
            };
            assert_eq!(self.take(r, &o), Outcome::Ready);
        }
    }

    /// Ingest and re-call until nothing is owed; the outcome that stopped it (`Failed` is a framing error).
    fn ingest_until(&mut self, bytes: &[u8], end: bool) -> Outcome {
        let mut i: IngestIn = z();
        i.framing = self.framing;
        i.bytes = bytes.as_ptr();
        i.len = bytes.len();
        i.end = u32::from(end);
        i.sink = self.sink();
        let mut o: FramerOut = z();
        let mut r = call(ops().ingest, self.inst, &mut i, &mut o, slot::INGEST);
        loop {
            if self.take(r, &o) != Outcome::Ready || self.flags & YIELD_MORE == 0 {
                return r;
            }
            let mut i: IngestIn = z();
            i.framing = self.framing;
            i.sink = self.sink();
            o = z();
            r = call(ops().ingest, self.inst, &mut i, &mut o, slot::INGEST);
        }
    }

    fn ingest(&mut self, bytes: &[u8], end: bool) {
        let mut i: IngestIn = z();
        i.framing = self.framing;
        i.bytes = bytes.as_ptr();
        i.len = bytes.len();
        i.end = u32::from(end);
        i.sink = self.sink();
        let mut o: FramerOut = z();
        let r = call(ops().ingest, self.inst, &mut i, &mut o, slot::INGEST);
        assert_eq!(self.take(r, &o), Outcome::Ready);
        self.drain(slot::INGEST);
    }

    fn emit_raw(&mut self, bytes: &[u8], end_of_frame: bool) -> Outcome {
        let mut i: EmitIn = z();
        i.framing = self.framing;
        i.bytes = bytes.as_ptr();
        i.len = bytes.len();
        i.end_of_frame = u32::from(end_of_frame);
        i.sink = self.sink();
        let mut o: FramerOut = z();
        let r = call(ops().emit, self.inst, &mut i, &mut o, slot::EMIT);
        let r = self.take(r, &o);
        if r == Outcome::Ready {
            self.drain(slot::EMIT);
        }
        r
    }

    fn emit(&mut self, bytes: &[u8]) {
        let mut i: EmitIn = z();
        i.framing = self.framing;
        i.bytes = bytes.as_ptr();
        i.len = bytes.len();
        i.end_of_frame = 1;
        i.sink = self.sink();
        let mut o: FramerOut = z();
        let r = call(ops().emit, self.inst, &mut i, &mut o, slot::EMIT);
        assert_eq!(self.take(r, &o), Outcome::Ready);
        self.drain(slot::EMIT);
    }
}

fn lines(h: &Host) -> Vec<Vec<u8>> {
    h.frames.iter().map(|(b, _)| b.clone()).collect()
}

#[test]
fn the_tail_is_a_line_framer_over_the_host_pipe() {
    let st = STATEMENT;
    // SAFETY: the Statement's kind tail is this crate's `'static` `TransportTail`.
    let tail = unsafe { &*st.kind_tail.cast::<TransportTail>() };
    assert_eq!(tail.role, ROLE_FRAMER);
    assert_eq!(tail.composes_over_len, 0);
    assert_eq!(check_tail(tail), Ok(()));
    // The 1.5.5 transports row: a session, bound, opened by the first message; no selector forms.
    // SAFETY: `claim_rows` is this crate's `'static` one-row table.
    let claim = unsafe { &*tail.claim_rows };
    assert_eq!(tail.claim_rows_len, 1);
    assert_eq!(
        (claim.session, claim.session_bound, claim.unit0_trigger),
        (1, 1, UNIT0_FIRST_MESSAGE)
    );
    assert_eq!((claim.selector_forms.len, claim.facts_len), (0, 0));
}

#[test]
fn a_line_is_a_frame_byte_exact_and_in_order() {
    let mut h = Host::new(64, 64, 8);
    h.ingest(b"{\"id\":1}\n{\"id\":2}\nthird\n", false);
    assert_eq!(
        lines(&h),
        vec![
            b"{\"id\":1}".to_vec(),
            b"{\"id\":2}".to_vec(),
            b"third".to_vec()
        ]
    );
    assert!(h.frames.iter().all(|(_, f)| f & PIECE_END_OF_FRAME != 0));
    assert_eq!(h.flags & YIELD_ENDED, 0);
}

#[test]
fn a_line_split_across_reads_is_one_frame() {
    let mut h = Host::new(64, 64, 8);
    h.ingest(b"hel", false);
    assert!(h.frames.is_empty(), "no terminator yet, no frame");
    h.ingest(b"lo\nwor", false);
    h.ingest(b"ld\n", false);
    assert_eq!(lines(&h), vec![b"hello".to_vec(), b"world".to_vec()]);
}

#[test]
fn a_carriage_return_before_the_terminator_is_stripped_once() {
    let mut h = Host::new(64, 64, 8);
    h.ingest(b"crlf\r\nbare\rmid\nboth\r\r\n", false);
    assert_eq!(
        lines(&h),
        vec![b"crlf".to_vec(), b"bare\rmid".to_vec(), b"both\r".to_vec()]
    );
}

#[test]
fn a_line_of_whitespace_is_a_frame_but_an_empty_line_is_not() {
    let mut h = Host::new(64, 64, 8);
    h.ingest(b"\n\r\n   \n\nx\n", false);
    assert_eq!(lines(&h), vec![b"   ".to_vec(), b"x".to_vec()]);
}

#[test]
fn a_full_sink_is_back_pressure_and_every_line_comes_out_once_in_order() {
    let mut h = Host::new(64, 3, 1);
    let mut sent = Vec::new();
    let mut want = Vec::new();
    for n in 0..20_u8 {
        let line: Vec<u8> = (0..=n).map(|b| b'a' + b % 26).collect();
        sent.extend_from_slice(&line);
        sent.push(b'\n');
        want.push(line);
    }
    h.ingest(&sent, false);
    let mut got: Vec<Vec<u8>> = Vec::new();
    let mut open = false;
    for (b, f) in &h.frames {
        if open {
            got.last_mut().unwrap().extend_from_slice(b);
        } else {
            got.push(b.clone());
        }
        open = f & PIECE_END_OF_FRAME == 0;
    }
    assert_eq!(
        got, want,
        "every line, once, in order, pieces ended by the last"
    );
}

#[test]
fn the_far_sides_end_after_a_whole_line_ends_the_connection() {
    let mut h = Host::new(64, 64, 4);
    assert_eq!(h.ingest_until(b"last\n", true), Outcome::Ready);
    assert_eq!(lines(&h), vec![b"last".to_vec()]);
    assert_eq!(h.flags, YIELD_ENDED);
    let mut g = Host::new(64, 64, 4);
    assert_eq!(g.ingest_until(b"", true), Outcome::Ready);
    assert_eq!(g.flags, YIELD_ENDED, "a clean end on a line boundary");
}

#[test]
fn an_unterminated_final_line_is_a_framing_error_after_the_lines_before_it() {
    let mut h = Host::new(64, 64, 4);
    assert_eq!(h.ingest_until(b"whole\npart", true), Outcome::Failed);
    assert_eq!(lines(&h), vec![b"whole".to_vec()]);
}

#[test]
fn a_line_past_the_maximum_is_a_framing_error_and_the_next_line_is_delivered() {
    let mut h = Host::new(64, 64, 4);
    let mut over = vec![b'x'; MAX_LINE_BYTES + 1];
    over.extend_from_slice(b"\nnext\n");
    assert_eq!(h.ingest_until(&over, false), Outcome::Failed);
    assert!(h.frames.is_empty(), "the over-long line is not a frame");
    // The connection is neither closed nor stuck: the line after it reads normally.
    assert_eq!(h.ingest_until(b"", false), Outcome::Ready);
    assert_eq!(lines(&h), vec![b"next".to_vec()]);
}

#[test]
fn a_line_of_exactly_the_maximum_is_still_a_frame() {
    let mut h = Host::new(64, 1 << 17, 4);
    let mut line = vec![b'y'; MAX_LINE_BYTES];
    line.push(b'\n');
    assert_eq!(h.ingest_until(&line, false), Outcome::Ready);
    assert_eq!(h.frames.len(), 1);
    assert_eq!(h.frames[0].0.len(), MAX_LINE_BYTES);
    // One more byte of line, with its CR, is past it.
    let mut g = Host::new(64, 1 << 17, 4);
    let mut line = vec![b'y'; MAX_LINE_BYTES];
    line.extend_from_slice(b"\r\n");
    assert_eq!(g.ingest_until(&line, false), Outcome::Failed);
}

#[test]
fn emit_is_the_line_on_the_wire_and_a_full_wire_sink_is_back_pressure() {
    let mut h = Host::new(5, 8, 1);
    let sent: Vec<u8> = (b'a'..=b'z').collect();
    h.emit(&sent);
    h.emit(b"second");
    let mut want = sent.clone();
    want.push(b'\n');
    want.extend_from_slice(b"second\n");
    assert_eq!(h.wire_log, want);
    assert!(h.frames.is_empty());
}

#[test]
fn a_frame_is_one_line_across_emit_calls() {
    let mut h = Host::new(64, 8, 1);
    assert_eq!(h.emit_raw(b"half ", false), Outcome::Ready);
    assert!(
        h.wire_log.is_empty(),
        "nothing is written before the frame completes"
    );
    assert_eq!(h.emit_raw(b"and half", true), Outcome::Ready);
    assert_eq!(h.wire_log, b"half and half\n");
}

#[test]
fn a_payload_carrying_the_delimiter_is_refused_before_a_byte_is_written() {
    let mut h = Host::new(64, 8, 1);
    for bad in [&b"{\"id\":1}\n{\"method\":\"admin\"}"[..], b"ends\r", b"\n"] {
        assert_eq!(h.emit_raw(bad, true), Outcome::Failed, "{bad:?}");
    }
    assert!(h.wire_log.is_empty());
    // The framing is neither poisoned nor closed: the next frame goes out.
    h.emit(b"fine");
    assert_eq!(h.wire_log, b"fine\n");
}

#[test]
fn refuse_is_a_last_line_on_the_wire() {
    let mut h = Host::new(64, 8, 1);
    let mut i: RefuseIn = z();
    i.framing = h.framing;
    i.bytes = b"{\"error\":\"no\"}".as_ptr();
    i.len = 14;
    i.sink = h.sink();
    let mut o: FramerOut = z();
    let r = call(ops().refuse, h.inst, &mut i, &mut o, slot::REFUSE);
    assert_eq!(h.take(r, &o), Outcome::Ready);
    assert_eq!(h.wire_log, b"{\"error\":\"no\"}\n");
}

fn encode(h: &mut Host, body: &[u8], fields: &[Field]) -> (Outcome, Vec<u8>) {
    let mut i: EncodeIn = z();
    i.body = body.as_ptr();
    i.body_len = body.len();
    i.fields = fields.as_ptr();
    i.fields_len = fields.len();
    i.sink = h.sink();
    let mut o: FramerOut = z();
    let r = call(ops().encode, h.inst, &mut i, &mut o, slot::ENCODE);
    (r, h.wire[..o.yielded.wire_len as usize].to_vec())
}

#[test]
fn encode_is_the_body_and_refuses_either_half_of_the_delimiter_on_its_own() {
    let mut h = Host::new(64, 8, 1);
    assert_eq!(
        encode(&mut h, b"raw bytes", &[]),
        (Outcome::Ready, b"raw bytes".to_vec())
    );
    // A line has no head: fields are not written.
    let fields = [Field {
        name: s("host"),
        value: s("x"),
    }];
    assert_eq!(
        encode(&mut h, b"body", &fields),
        (Outcome::Ready, b"body".to_vec())
    );
    assert_eq!(encode(&mut h, b"a\nb", &[]).0, Outcome::Failed);
    assert_eq!(encode(&mut h, b"tail\r", &[]).0, Outcome::Failed);
    assert_eq!(encode(&mut h, b"mid\rdle", &[]).0, Outcome::Ready);
}

#[test]
fn finish_forgets_the_framing() {
    let mut h = Host::new(8, 8, 1);
    let mut i: FinishIn = z();
    i.framing = h.framing;
    i.sink = h.sink();
    let mut o: FramerOut = z();
    assert_eq!(
        call(ops().finish, h.inst, &mut i, &mut o, slot::FINISH),
        Outcome::Ready
    );
    assert_eq!(o.yielded.flags, YIELD_ENDED);
    let mut o: FramerOut = z();
    assert_eq!(
        call(ops().finish, h.inst, &mut i, &mut o, slot::FINISH),
        Outcome::Failed
    );
}

#[test]
fn the_pipe_the_program_and_the_handoff_are_the_hosts_so_those_ops_are_refused() {
    let h = Host::new(1, 1, 1);
    let mut i: DialIn = z();
    let mut o: ConnOut = z();
    assert_eq!(
        call(ops().dial, h.inst, &mut i, &mut o, slot::DIAL),
        Outcome::Refused
    );
    let mut i: LocateIn = z();
    i.target = s("/usr/bin/some-server");
    let mut o: LocateOut = z();
    assert_eq!(
        call(ops().locate, h.inst, &mut i, &mut o, slot::LOCATE),
        Outcome::Refused
    );
    // stdio composes over nothing: a handoff onto it is a mismatch, and it hands nothing up.
    let mut i: AdoptIn = z();
    let mut o: FramerOut = z();
    assert_eq!(
        call(ops().adopt, h.inst, &mut i, &mut o, slot::ADOPT),
        Outcome::Refused
    );
    let mut i: FramingIn = z();
    i.framing = h.framing;
    let mut o: FramerOut = z();
    assert_eq!(
        call(ops().detach, h.inst, &mut i, &mut o, slot::DETACH),
        Outcome::Refused
    );
}
