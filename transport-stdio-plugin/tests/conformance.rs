// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! **ONE `stdio` DOOR, BOTH WAYS IN**: the linked door (`busbar_transport_stdio::linked::door`) and this
//! crate's built cdylib (the same door behind the one `export_door!`), each admitted through the
//! loader's ONE door validation and driven through the ONE dispatcher's crossing, give the same
//! Statement and the same answers, byte for byte. Run against the busbar rev this repo pins
//! (`.busbar-ref`).
//!
//! THE RED ARMS, same file: the door asked for as another kind is refused, linked (by the door's
//! own kind) and dropped in (by the stated kind, before `dlopen`). A missing cdylib PANICS: this
//! test IS the dropped-in door's proof, and never skips.

use std::mem::zeroed;
use std::sync::Arc;

use busbar_contract::abi::mechanism::call::{Blob, Outcome, BLOB_ABSENT};
use busbar_contract::abi::mechanism::lifecycle::{slot as life, OpenIn, OpenOut};
use busbar_contract::abi::mechanism::KindCode;
use busbar_contract::abi::transport::{
    slot, BeginIn, EmitIn, FramePiece, FramerOut, FramerSink, IngestIn, PIECE_END_OF_FRAME,
    SIDE_DIAL, YIELD_ENDED,
};
use busbar_plugin_loader::dispatch::kinds::hook::Hook;
use busbar_plugin_loader::dispatch::kinds::transport::Transport;
use busbar_plugin_loader::dispatch::{
    in_head, load_dropped, load_linked, out_head, Bind, DispatchConfig, Dispatcher, Frame,
    LinkedRow, LoadError, NoSink, Plugin,
};
use busbar_transport_stdio_plugin::linked;

fn z<T>() -> T {
    // SAFETY: every `in`/`out` here is plain C data; all-zero is a valid value of each.
    unsafe { zeroed() }
}

/// This crate's built cdylib (uplifted or under `deps`, newest wins). A missing artifact is a
/// failure, never a skip: this test IS the dropped-in door's proof.
fn cdylib() -> std::path::PathBuf {
    let exe = std::env::current_exe().expect("the test binary has a path");
    let profile = exe
        .parent()
        .and_then(|d| d.parent())
        .expect("target/<profile>");
    let file = busbar_plugin_loader::plugin_library_filename("busbar_transport_stdio_plugin");
    [profile.join(&file), profile.join("deps").join(&file)]
        .into_iter()
        .filter_map(|p| Some((std::fs::metadata(&p).ok()?.modified().ok()?, p)))
        .max()
        .map(|(_, p)| p)
        .unwrap_or_else(|| panic!("the busbar-transport-stdio-plugin cdylib ({file}) is not built"))
}

/// The row a compiled-in build holds for this door.
fn row() -> LinkedRow {
    LinkedRow::of(linked::door).expect("the door states itself")
}

fn bind(d: &Dispatcher) -> Bind {
    Bind {
        instance: Arc::from("the-instance"),
        max_inflight_cap: 64,
        sink: Arc::new(NoSink),
        dispatcher: d.adopter(),
        conns: None,
    }
}

fn open(p: &Plugin<Transport>) {
    let mut i: OpenIn = z();
    i.head = in_head();
    i.settings = Blob {
        ptr: std::ptr::null(),
        len: 0,
        fmt: BLOB_ABSENT,
        flags: 0,
    };
    let mut o: OpenOut = z();
    o.head = out_head();
    let mut f = Frame::new(i, o);
    assert_eq!(p.call(life::OPEN, &mut f).outcome, Outcome::Ready);
}

/// What one script saw: the wire bytes, the frame bytes with their pieces' flags, the last flags.
type Script = (Vec<u8>, Vec<(Vec<u8>, u16)>, u32);

/// One scripted exchange through the dispatcher: begin, emit, ingest (tight sink), ingest the end.
/// What comes back: the wire bytes, the frame bytes with their pieces' flags, the last flags.
fn script(p: &Plugin<Transport>) -> Script {
    let mut wire = vec![0_u8; 4];
    let mut frame = vec![0_u8; 3];
    let mut pieces: Vec<FramePiece> = vec![z(); 4];
    let base = FramerSink {
        wire: wire.as_mut_ptr(),
        wire_cap: wire.len(),
        frame: frame.as_mut_ptr(),
        frame_cap: frame.len(),
        pieces: pieces.as_mut_ptr(),
        pieces_cap: pieces.len(),
        now_monotonic_ns: 1,
        now_unix_ns: 1,
        heads: std::ptr::null_mut(),
        heads_cap: 0,
    };
    let sink = || base;
    let mut i: BeginIn = z();
    i.head = in_head();
    i.side = SIDE_DIAL;
    i.sink = sink();
    let mut o: FramerOut = z();
    o.head = out_head();
    let mut f = Frame::new(i, o);
    assert_eq!(p.call(slot::BEGIN, &mut f).outcome, Outcome::Ready);
    let framing = f.out.framing;

    let (mut wire_log, mut frames, mut flags) = (Vec::new(), Vec::new(), 0);
    let steps: [(u32, &[u8], bool); 3] = [
        (slot::EMIT, b"to the far side", false),
        (slot::INGEST, b"from the far\r\n\nside, one", false),
        (slot::INGEST, b" line\nlast\n", true),
    ];
    for (op, bytes, end) in steps {
        let mut first = true;
        loop {
            let give: &[u8] = if first { bytes } else { &[] };
            first = false;
            let (outcome, o) = if op == slot::EMIT {
                let mut i: EmitIn = z();
                i.head = in_head();
                i.framing = framing;
                i.bytes = give.as_ptr();
                i.len = give.len();
                i.end_of_frame = 1;
                i.sink = sink();
                let mut o: FramerOut = z();
                o.head = out_head();
                let mut f = Frame::new(i, o);
                (p.call(op, &mut f).outcome, f.out)
            } else {
                let mut i: IngestIn = z();
                i.head = in_head();
                i.framing = framing;
                i.bytes = give.as_ptr();
                i.len = give.len();
                i.end = u32::from(end);
                i.sink = sink();
                let mut o: FramerOut = z();
                o.head = out_head();
                let mut f = Frame::new(i, o);
                (p.call(op, &mut f).outcome, f.out)
            };
            assert_eq!(outcome, Outcome::Ready);
            wire_log.extend_from_slice(&wire[..o.yielded.wire_len as usize]);
            for piece in &pieces[..o.yielded.pieces_len as usize] {
                let at = piece.offset as usize;
                frames.push((frame[at..at + piece.len as usize].to_vec(), piece.flags));
            }
            flags = o.yielded.flags;
            if flags & busbar_contract::abi::transport::YIELD_MORE == 0 {
                break;
            }
        }
    }
    (wire_log, frames, flags)
}

#[test]
fn the_linked_and_the_dropped_in_door_are_one_line_framer() {
    let d = Dispatcher::new(DispatchConfig::default());
    let linked: Plugin<Transport> = load_linked(&row(), bind(&d)).expect("the linked door loads");
    let dropped: Plugin<Transport> =
        load_dropped(&cdylib(), &row().statement, bind(&d)).expect("the dropped-in door loads");
    assert_eq!(linked.name(), linked::KEY);
    assert_eq!(dropped.name(), linked.name());
    open(&linked);
    open(&dropped);

    let a = script(&linked);
    let b = script(&dropped);
    assert_eq!(
        a.0, b"to the far side\n",
        "emit is the line on the wire, byte for byte"
    );
    let mut got: Vec<Vec<u8>> = Vec::new();
    let mut open = false;
    for (bytes, flags) in &a.1 {
        if open {
            got.last_mut().unwrap().extend_from_slice(bytes);
        } else {
            got.push(bytes.clone());
        }
        open = flags & PIECE_END_OF_FRAME == 0;
    }
    assert_eq!(
        got,
        vec![
            b"from the far".to_vec(),
            b"side, one line".to_vec(),
            b"last".to_vec()
        ],
        "a line is a frame: CRLF stripped, the blank line none, a split line one"
    );
    assert_eq!(a.2, YIELD_ENDED, "the far side's end ends the connection");
    assert_eq!(a, b, "both doors answer alike");
}

#[test]
fn the_door_asked_for_as_another_kind_is_refused_both_ways() {
    let d = Dispatcher::new(DispatchConfig::default());
    let want = (KindCode::Transport, KindCode::Hook);
    match load_linked::<Hook>(&row(), bind(&d)) {
        Err(LoadError::WrongKind { door, want: asked }) => assert_eq!((door, asked), want),
        other => panic!("the linked door loaded as a hook: {:?}", other.err()),
    }
    match load_dropped::<Hook>(&cdylib(), &row().statement, bind(&d)) {
        Err(LoadError::ManifestKind {
            stated,
            want: asked,
        }) => assert_eq!((stated, asked), want),
        other => panic!("the dropped-in door loaded as a hook: {:?}", other.err()),
    }
}

#[test]
fn a_door_that_framed_differently_would_be_seen_by_the_comparison() {
    // THE RED ARM of the comparison itself: the same script over a door that altered one byte of the
    // wire must DIFFER from the real door's answer, so `a == b` above proves the two doors alike
    // and not that the comparison cannot tell.
    let d = Dispatcher::new(DispatchConfig::default());
    let linked: Plugin<Transport> = load_linked(&row(), bind(&d)).expect("the linked door loads");
    open(&linked);
    let real = script(&linked);
    let mut altered = real.clone();
    altered.0[0] ^= 1;
    assert_ne!(real, altered);
    let mut cut = real.clone();
    cut.1.pop();
    assert_ne!(real, cut);
}
