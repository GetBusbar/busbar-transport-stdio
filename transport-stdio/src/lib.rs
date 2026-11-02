// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! The `stdio` transport: the CARRIER of a spawned program's pipes, one frame per line.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod claims;
pub mod door;
mod meta;
pub mod supervisor;
mod transport;

/// The transport axis entry the composition root folds for this transport: its key, the layers it
pub mod linked {
    /// The row's registry key.
    pub const KEY: &str = crate::door::KEY;
    /// The layers this transport declares it can be built over: none, it is the bottom of its stack.
    pub const COMPOSES_OVER: &[&str] = &[];
    /// Whether this transport carries sessions.
    pub const SESSION: bool = crate::meta::SESSION;
    pub use crate::door::{door, MACRO_DOOR};
}
