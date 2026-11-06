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

/// The transport axis entry the composition root folds for this transport: its door.
pub mod linked {
    pub use crate::door::door;
}
