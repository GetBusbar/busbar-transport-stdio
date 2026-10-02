// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! The `stdio` transport: one frame per line, and nothing else.
//!
//! The pipe is the host's (`BUSBAR-1.6.0.md` THE DESIGN, §5, the governed raw connection): the host
//! owns the process's own stdin/stdout or the spawned child (an absolute program path, no shell, an
//! `env_clear()`ed environment), reads and writes the bytes, and hands them here. This crate is the
//! framer that sits on that pipe ([`door`]): bytes split on `0x0A` are the frames, and a frame
//! handed to it goes out with a `0x0A` after it, as the 1.5.5 carrier framed. It opens no pipe,
//! spawns no process and reads no clock. It knows no protocol, no plane and no principal.
//!
//! One door, two ways in: a build that links this crate names [`linked::door`]; the sibling
//! `busbar-transport-stdio-plugin` cdylib exports the same door as its image's one symbol
//! (`export_door!`). This crate exports nothing, so linking it adds no door symbol.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod door;

/// THE TRANSPORT AXIS ENTRY: what the composition root folds for this transport: its key, the
/// layers it declares and its door. The root names none of them.
pub mod linked {
    /// The row's registry key.
    pub const KEY: &str = crate::door::KEY;
    /// The layers this transport declares it can be built over: none, it frames the host's pipe.
    pub const COMPOSES_OVER: &[&str] = &[];
    /// Whether this transport carries sessions.
    pub const SESSION: bool = true;
    pub use crate::door::door;
}
