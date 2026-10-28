// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! THE `stdio` DOOR: this transport as a CARRIER on the transport kind's table
//! (`busbar_contract::abi::transport`), compiled in or dropped in through the one door. Every slot
//! is a [`SafeSlot`](busbar_contract::abi::sdk::SafeSlot) over the SDK's generic lifecycle
//! (`life(Carried)`): no `unsafe` here.
//!
//! The program and its pipes are the host's: this carrier spawns,
//! reads, writes and closes through the host's I/O (`io.*`) and owns the line semantics over them
//! ([`crate::transport`]): one frame per line in, one line per frame out. `listen`, `accept` and
//! every framer op are REFUSED.
//!
//! The door wires the kind's own files together: the key and tail [`crate::meta`] declares, the
//! claims [`crate::claims`] declares, and the carrier [`crate::transport`] runs.

use busbar_contract::abi::mechanism::door::Statement;
use busbar_contract::abi::sdk::door::statement;
use busbar_contract::abi::sdk::life::{
    Cancel, Drive, Open, Refresh, Release, Retire, Tick, Validate,
};
use busbar_contract::abi::sdk::Safe;
use busbar_contract::abi::transport::Ops;

use crate::claims::CLAIM_NAMES;
pub use crate::meta::KEY;
use crate::meta::TAIL;
pub use crate::transport::{
    Accept, Adopt, Arrival, Begin, Carried, Close, Detach, Dial, Emit, Encode, Finish, Flush,
    Ingest, Listen, Locate, Read, RefuseFrame, Shut, Timer, Write, BODY_MAX_BYTES, FRAME_TOO_LONG,
    MAX_LINE_BYTES, NOT_A_COUNT, NOT_ONE_LINE, TOO_LONG,
};

busbar_contract::statement_extensions!(
    /// The Statement's extensions: this plugin's whole `declares` document.
    EXTENSIONS: [(
        busbar_contract::abi::mechanism::extensions::EXT_DECLARES,
        include_bytes!("../declares.json")
    )]
);

/// The door's Statement: the `stdio` carrier.
pub const STATEMENT: Statement = {
    let mut s = statement(KEY, env!("CARGO_PKG_VERSION"), 64)
        .with_kind_tail(&TAIL)
        .with_claims(CLAIM_NAMES);
    s.extensions = EXTENSIONS;
    s
};

busbar_contract::plugin_door! {
    ops: Ops,
    statement: STATEMENT,
    lifecycle: {
        validate: Safe<Validate<Carried>>,
        open: Safe<Open<Carried>>,
        refresh: Safe<Refresh<Carried>>,
        retire: Safe<Retire<Carried>>,
        tick: Safe<Tick<Carried>>,
        drive: Safe<Drive<Carried>>,
        cancel: Safe<Cancel<Carried>>,
        release: Safe<Release<Carried>>,
        close: Safe<Close>,
    },
    kind_ops: {
        listen: Safe<Listen>,
        accept: Safe<Accept>,
        dial: Safe<Dial>,
        read: Safe<Read>,
        write: Safe<Write>,
        flush: Safe<Flush>,
        shut: Safe<Shut>,
        arrival: Safe<Arrival>,
        locate: Safe<Locate>,
        begin: Safe<Begin>,
        ingest: Safe<Ingest>,
        emit: Safe<Emit>,
        encode: Safe<Encode>,
        refuse: Safe<RefuseFrame>,
        finish: Safe<Finish>,
        detach: Safe<Detach>,
        adopt: Safe<Adopt>,
        timer: Safe<Timer>,
    },
}
