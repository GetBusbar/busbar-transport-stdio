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
