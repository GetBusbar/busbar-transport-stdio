// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! What this transport declares about itself: the key it answers on, the one setting it reads and

use busbar_contract::abi::sdk::transport::{setting_decls, SettingRow, DEFAULT_BODY_MAX};
use busbar_contract::abi::transport::{
    SettingDecl, TransportTail, FRAME_PIECE_SIZE, FRAMING_STREAM, ROLE_CARRIER, SETTING_COUNT,
};

use crate::transport::BODY_MAX_BYTES;

/// The claim this entry answers for: the stdio row's key.
pub const KEY: &str = "stdio";

/// Whether this transport carries sessions (its claim row states `session = 1`).
pub(crate) const SESSION: bool = true;

/// The one setting this transport reads: the deployment's request-body bound, its line ceiling (a

pub(crate) const SETTING_ROWS: [SettingRow; 1] = [SettingRow {
    path: BODY_MAX_BYTES,
    kind: SETTING_COUNT,
    default: DEFAULT_BODY_MAX,
}];
const SETTINGS: &[SettingDecl] = &setting_decls(&SETTING_ROWS);

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
