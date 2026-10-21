// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! THE `stdio` DOOR: this transport as a CARRIER on the transport kind's table
//! (`busbar_contract::abi::transport`), compiled in or dropped in through the one door. Every slot
//! is a [`SafeSlot`](busbar_contract::abi::sdk::SafeSlot) over the SDK's generic lifecycle
//! (`life(Carried)`): no `unsafe` here.
//!
//! The program and its pipes are the host's (`BUSBAR-1.6.0.md` THE DESIGN, §5): this carrier spawns,
//! reads, writes and closes through the host's I/O (`io.*`) and owns the line semantics over them
//! ([`crate::transport`]): one frame per line in, one line per frame out. `listen`, `accept` and
//! every framer op are REFUSED.
//!
//! The door wires the kind's own files together: the key and tail [`crate::meta`] declares, the
//! claims [`crate::claims`] declares, and the carrier [`crate::transport`] runs.

use busbar_contract::abi::mechanism::door::{KindTailHead, Statement};
use busbar_contract::abi::sdk::door::statement;
use busbar_contract::abi::sdk::Safe;
use busbar_contract::abi::transport::{
    AcceptIn, AcceptOut, AdoptIn, AuthPoints, BeginIn, EmitIn, EncodeIn, FinishIn, FramerOut,
    FramingIn, IngestIn, ListenIn, ListenOut, LocateIn, LocateOut, Ops, RefuseIn, TransportTail,
    EXT_OFFERED_POINTS,
};

use crate::claims::CLAIM_NAMES;
pub use crate::meta::KEY;
use crate::meta::TAIL;
pub use crate::transport::{Arrival, Carried, Dial, Flush, Read, Refused, Shut, Write};

/// THE AUTH POINTS THIS CARRIER CALLS (THE DESIGN §6, "Auth points and guest lists"): none. It
/// has no head, so it calls no `Head` or `HeadBody`, and no auth call reaches a carrier at its
/// spawn, so it calls no `Peer`. A transport that states no record is read as offering
/// `{Head, HeadBody}` (`abi::transport::EXT_OFFERED_POINTS`), so the empty set is stated: a
/// guest-list line whose auth needs a point is refused at the seal, never sealed and left uncalled.
pub const OFFERED_POINTS: AuthPoints = AuthPoints::EMPTY;

/// [`OFFERED_POINTS`] as its record's four little-endian bytes.
const OFFERED_POINTS_RECORD: [u8; 4] = OFFERED_POINTS.0.to_le_bytes();

busbar_contract::statement_extensions!(
    /// The Statement's extensions: this plugin's whole `declares` document (BUSBAR-1.6.0.md l.246)
    /// and the auth points it offers.
    EXTENSIONS: [
        (
            busbar_contract::abi::mechanism::extensions::EXT_DECLARES,
            include_bytes!("../declares.json")
        ),
        (EXT_OFFERED_POINTS, &OFFERED_POINTS_RECORD),
    ]
);

/// The door's Statement: the `stdio` carrier.
pub const STATEMENT: Statement = Statement {
    extensions: EXTENSIONS,
    kind_tail: (&TAIL as *const TransportTail).cast::<KindTailHead>(),
    claims: CLAIM_NAMES.as_ptr(),
    claims_len: CLAIM_NAMES.len(),
    ..statement(KEY, env!("CARGO_PKG_VERSION"), 64)
};

busbar_contract::plugin_door! {
    ops: Ops,
    statement: STATEMENT,
    lifecycle: life(Carried),
    kind_ops: {
        listen: Safe<Refused<ListenIn, ListenOut>>,
        accept: Safe<Refused<AcceptIn, AcceptOut>>,
        dial: Safe<Dial>,
        read: Safe<Read>,
        write: Safe<Write>,
        flush: Safe<Flush>,
        shut: Safe<Shut>,
        arrival: Safe<Arrival>,
        locate: Safe<Refused<LocateIn, LocateOut>>,
        begin: Safe<Refused<BeginIn, FramerOut>>,
        ingest: Safe<Refused<IngestIn, FramerOut>>,
        emit: Safe<Refused<EmitIn, FramerOut>>,
        encode: Safe<Refused<EncodeIn, FramerOut>>,
        refuse: Safe<Refused<RefuseIn, FramerOut>>,
        finish: Safe<Refused<FinishIn, FramerOut>>,
        detach: Safe<Refused<FramingIn, FramerOut>>,
        adopt: Safe<Refused<AdoptIn, FramerOut>>,
        timer: Safe<Refused<FramingIn, FramerOut>>,
    },
}
