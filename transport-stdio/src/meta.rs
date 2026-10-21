// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! What this transport declares about itself, as the kind's own file (`BUSBAR-1.6.0.md` THE
//! DESIGN, §2): the key it answers on, the one setting it reads and the transport kind's tail its
//! door states. Read once at registration and sealed, so it is data, held apart from the carrier
//! code it describes.

use busbar_contract::abi::mechanism::call::AbiStr;
use busbar_contract::abi::mechanism::door::KindTailHead;
use busbar_contract::abi::sdk::door::abi_str;
use busbar_contract::abi::transport::{
    SettingDecl, TransportTail, FRAME_PIECE_SIZE, FRAMING_STREAM, ROLE_CARRIER, SETTING_COUNT,
};

use crate::transport::{BODY_MAX_BYTES, MAX_LINE_BYTES};

/// The claim this entry answers for: the stdio row's key.
pub const KEY: &str = "stdio";

/// The absent string: a slot this transport states nothing in.
pub(crate) const NONE: AbiStr = AbiStr {
    ptr: std::ptr::null(),
    len: 0,
};

/// The line ceiling's declared default, as the tail states it: [`MAX_LINE_BYTES`].
const DEFAULT_BODY_MAX: &str = "33554432";

/// `text` read as a decimal count.
const fn count(text: &str) -> usize {
    let b = text.as_bytes();
    let mut n = 0;
    let mut i = 0;
    while i < b.len() {
        n = n * 10 + (b[i] - b'0') as usize;
        i += 1;
    }
    n
}

// The declared default is the ceiling the carrier falls back to: one fact, held equal.
const _: () = assert!(count(DEFAULT_BODY_MAX) == MAX_LINE_BYTES);

/// The one setting this transport reads, at its 1.5.5 path: the deployment's request-body bound,
/// its line ceiling (THE DESIGN §5: a transport's knobs reach it as the settings its door declares).
const SETTINGS: &[SettingDecl] = &[SettingDecl {
    path: abi_str(BODY_MAX_BYTES),
    kind: SETTING_COUNT,
    _reserved: 0,
    default: abi_str(DEFAULT_BODY_MAX),
}];

/// The transport kind's tail: the CARRIER of a program's pipes (its role stated, ARCHITECT ruling
/// Q128 U7), composing over nothing.
pub(crate) const TAIL: TransportTail = TransportTail {
    head: KindTailHead {
        size: std::mem::size_of::<TransportTail>() as u32,
        _reserved: 0,
    },
    role: ROLE_CARRIER,
    framing: FRAMING_STREAM,
    facts: 0,
    handshake_max_steps: 0,
    frame_piece_size: FRAME_PIECE_SIZE,
    _reserved: 0,
    composes_over: std::ptr::null(),
    composes_over_len: 0,
    claim_rows: crate::claims::CLAIMS.as_ptr(),
    claim_rows_len: crate::claims::CLAIMS.len(),
    upgrades_to: std::ptr::null(),
    upgrades_to_len: 0,
    handoff_from: NONE,
    handoff_to: NONE,
    handoff_binding_fact: NONE,
    handshake_frame_kind: NONE,
    status_rows: std::ptr::null(),
    status_rows_len: 0,
    settings: SETTINGS.as_ptr(),
    settings_len: SETTINGS.len(),
    fault_rows: std::ptr::null(),
    fault_rows_len: 0,
};
