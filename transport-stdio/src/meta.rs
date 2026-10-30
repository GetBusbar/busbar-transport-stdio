// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! What this transport declares about itself: the key it answers on, the one setting it reads and
//! the transport kind's tail its door states. Read once at registration and sealed, so it is data,
//! held apart from the carrier code it describes.

use busbar_contract::abi::sdk::door::abi_str;
use busbar_contract::abi::sdk::transport::DEFAULT_BODY_MAX;
use busbar_contract::abi::transport::{
    SettingDecl, TransportTail, FRAME_PIECE_SIZE, FRAMING_STREAM, ROLE_CARRIER, SETTING_COUNT,
};

use crate::transport::BODY_MAX_BYTES;

/// The claim this entry answers for: the stdio row's key.
pub const KEY: &str = "stdio";

/// Whether this transport carries sessions (its claim row states `session = 1`).
pub(crate) const SESSION: bool = true;

/// The one setting this transport reads: the deployment's request-body bound, its line ceiling (a
/// transport's knobs reach it as the settings its door declares).
const SETTINGS: &[SettingDecl] = &[SettingDecl {
    path: abi_str(BODY_MAX_BYTES),
    kind: SETTING_COUNT,
    _reserved: 0,
    default: abi_str(DEFAULT_BODY_MAX),
}];

/// The transport kind's tail: the CARRIER of a program's pipes, composing over nothing.
pub(crate) const TAIL: TransportTail = {
    let mut t = TransportTail::EMPTY
        .with_claim_rows(crate::claims::CLAIMS)
        .with_settings(SETTINGS);
    t.role = ROLE_CARRIER;
    t.framing = FRAMING_STREAM;
    t.frame_piece_size = FRAME_PIECE_SIZE;
    t
};
