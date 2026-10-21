// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

use super::*;

/// A connection's buffers give back what one large line grew them to once it is answered: a
/// long-lived member connection holds a working size, not the high-water mark of its largest
/// message. RED (row 159, stdio LOW): `clear` kept the capacity for the life of the program.
#[test]
fn an_answered_line_gives_back_what_it_grew_the_buffer_to() {
    let mut c = Conn::default();
    c.inbound = vec![b'x'; 4 * 1024 * 1024];
    c.inbound.push(b'\n');
    let next = c.inbound.len();
    c.answered(next);
    assert!(c.inbound.is_empty());
    assert!(
        c.inbound.capacity() <= 256 * 1024,
        "kept {} bytes after the line was answered",
        c.inbound.capacity()
    );
}
