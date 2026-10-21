// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! The `stdio` transport: the CARRIER of a spawned program's pipes, one frame per line.
//!
//! No plugin opens a socket or a pipe (`BUSBAR-1.6.0.md` THE DESIGN, §5): the host spawns the
//! program (an absolute path, its arguments and its whole environment, no shell), owns its pipes and
//! kills it when the connection closes. This crate holds only the host's opaque handle ([`door`])
//! and owns the policy over it: the spawn it asks for, one frame per line on the way in, one line
//! per frame on the way out, the close, and the supervision of a member's program that keeps ending
//! ([`supervisor`], on the host's one clock). It spawns no thread. It knows no protocol, no plane
//! and no principal.
//!
//! One door, two ways in: a build that links this crate names [`door`]; the sibling
//! `busbar-transport-stdio-plugin` cdylib exports the same door as its image's one symbol
//! (`export_door!`). This crate exports nothing, so linking it adds no door symbol.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

// THE KIND'S SHAPE (`BUSBAR-1.6.0.md` THE DESIGN, §2), the same files every transport twin
// carries: what it declares (`meta`), what it claims (`claims`), the entry (`transport`), and the
// door that states them.
mod claims;
mod door;
mod meta;
mod supervisor;
mod transport;

/// THE DOOR: this plugin's one public item (BUSBAR-1.6.0.md l.3567, Crate shape).
pub use door::door;
