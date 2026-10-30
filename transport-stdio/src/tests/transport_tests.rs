// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

use super::*;

/// A connection's buffers give back what one large line grew them to once it is answered: a
/// long-lived member connection holds a working size, not the high-water mark of its largest
/// message. RED (row 159, stdio LOW): `clear` kept the capacity for the life of the program.
#[test]
fn an_answered_line_gives_back_what_it_grew_the_buffer_to() {
    let mut inbound = vec![b'x'; 4 * 1024 * 1024];
    inbound.push(b'\n');
    let mut c = Conn {
        inbound,
        ..Conn::default()
    };
    let next = c.inbound.len();
    c.answered(next);
    assert!(c.inbound.is_empty());
    assert!(
        c.inbound.capacity() <= 256 * 1024,
        "kept {} bytes after the line was answered",
        c.inbound.capacity()
    );
}

/// Compacting gives back what a large line grew the buffer to while the next line is still
/// arriving: lines split across reads never wholly empty the buffer. RED (codeaudit 0aa4f6d,
/// stdio LOW): compact kept the high-water capacity while lines kept arriving split across reads.
#[test]
fn a_compacted_buffer_gives_back_what_a_large_line_grew_it_to() {
    let mut inbound = vec![b'x'; 4 * 1024 * 1024];
    inbound.push(b'\n');
    inbound.extend_from_slice(b"abc");
    let mut c = Conn {
        inbound,
        ..Conn::default()
    };
    c.answered(4 * 1024 * 1024 + 1);
    assert!(!c.inbound.is_empty(), "the next line is partly here");
    c.compact();
    assert_eq!(c.inbound, b"abc");
    assert!(
        c.inbound.capacity() <= 256 * 1024,
        "kept {} bytes after compact",
        c.inbound.capacity()
    );
}

/// An op that took a connection out of the table before its shut, and waited for its lock while
/// the shut closed it, finds it gone as an op after the shut does: it never runs on the closed
/// handle, so a failure there never books a clean shut as the program's end. RED (codeaudit
/// 94ae3e3, stdio ST-C1 LOW): the late op locked the shut connection and ran on it.
#[test]
fn an_op_that_took_a_connection_before_its_shut_finds_it_gone() {
    let carried = Carried::open(b"", &[], 0).unwrap();
    let conn = Conn {
        io: 7,
        key: 3,
        ..Conn::default()
    };
    lock(&carried.conns).insert(1, Arc::new(Mutex::new(conn)));
    let taken = carried.conn(1).expect("the op takes the connection");
    assert!(carried.forget(1).is_some(), "the shut takes it out");
    assert!(carried.conn(1).is_none(), "an op after the shut finds none");
    assert!(
        live(&taken).is_none(),
        "the op that took it before the shut finds it gone"
    );
}

/// A dial sweeps out every key whose supervisor holds nothing (it reads as a fresh one), so the
/// carrier holds only members inside their window or held down, not one entry per member it ever
/// ran. RED (codeaudit 94ae3e3, stdio tests LOW): only `Supervisor::idle` was tested, not the sweep.
#[test]
fn a_dial_drops_the_supervision_of_every_idle_member() {
    let carried = Carried::open(b"", &[], 0).unwrap();
    let window = crate::supervisor::CRASH_WINDOW_NS;
    {
        let mut supervised = lock(&carried.supervised);
        supervised.entry(1).or_default().crashed(0);
        supervised.entry(2).or_default().crashed(window);
    }
    let at = window + window / 2;
    assert!(!carried.held_down(3, at), "a member never ended runs");
    let supervised = lock(&carried.supervised);
    assert!(
        !supervised.contains_key(&1),
        "the idle member's state was dropped"
    );
    assert!(
        supervised.contains_key(&2),
        "the member inside its window is kept"
    );
}
