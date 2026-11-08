// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! The `stdio` transport: the CARRIER of a spawned program's pipes, one frame per line.

#![deny(unsafe_code)]
#![deny(missing_docs)]

mod claims;
mod door;
mod meta;
mod supervisor;
mod transport;

/// The transport axis entry the composition root folds for this transport: its door.
pub mod linked {
    pub use crate::door::door;
}

/// The validated entry and generated registration descriptor.
pub use door::{door, MACRO_DOOR};

#[cfg(test)]
#[allow(unsafe_code)]
#[path = "tests/door_abi.rs"]
mod door_abi;

#[cfg(test)]
#[allow(unsafe_code)]
#[path = "tests/supervisor_abi.rs"]
mod supervisor_abi;

#[cfg(test)]
#[allow(unsafe_code)]
#[path = "tests/retired_keys_abi.rs"]
mod retired_keys_abi;
