// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! **THE PUBLISHED CONFORMANCE SUITE, RUN BY THIS PLUGIN** (busbar TODO ABI-b4; OWNER 2026-10-03:
//! plugins test themselves against busbar). busbar's suite, at the commit this repo pins
//! (`.busbar-ref`), drives the `stdio` CARRIER two ways through the one loader: LINKED (the logic
//! crate's `door::door`) and DROPPED IN (this crate's built cdylib), over the transport kind's
//! carrier script with the inputs in `conformance.json`: a spawned program as the far end, one frame
//! per line both ways, a frame that cannot be one line refused, a pending read the host's wake
//! resumes, and the host's guard (a program the host did not admit is refused). Every step's
//! crossings exactly at the script's pin, the two folds equal, and the suite's RED arms kept.
//! `plugin-ci.yml` runs it under `--release`.

busbar_plugin_loader::conformance_suite! {
    door: busbar_transport_stdio::door::door,
    cdylib: "busbar_transport_stdio_plugin",
    inputs: include_str!("conformance.json"),
}
