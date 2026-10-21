// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! What this transport declares about itself, as the kind's own file (`BUSBAR-1.6.0.md` THE
//! DESIGN, §2): the key it answers on and the transport kind's tail its door states. Read once at
//! registration and sealed, so it is data, held apart from the carrier code it describes.

use busbar_contract::abi::mechanism::call::AbiStr;
use busbar_contract::abi::transport::{
    TransportTail, FRAME_PIECE_SIZE, FRAMING_STREAM, ROLE_CARRIER,
};

/// The claim this entry answers for: the stdio row's key.
pub const KEY: &str = "stdio";

/// The absent string: a slot this transport states nothing in.
pub(crate) const NONE: AbiStr = AbiStr::ABSENT;

/// The transport kind's tail: the CARRIER of a program's pipes (its role stated, ARCHITECT ruling
/// Q128 U7), composing over nothing.
pub(crate) const TAIL: TransportTail = {
    let mut t = TransportTail::EMPTY.with_claim_rows(crate::claims::CLAIMS);
    t.role = ROLE_CARRIER;
    t.framing = FRAMING_STREAM;
    t.frame_piece_size = FRAME_PIECE_SIZE;
    t
};
