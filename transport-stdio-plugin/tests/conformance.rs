// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! **THE PUBLISHED CONFORMANCE SUITE, RUN BY THIS PLUGIN** (busbar TODO ABI-b4; OWNER 2026-10-03:

busbar_test_kit::conformance_suite! {
    door: busbar_transport_stdio::door::door,
    cdylib: "busbar_transport_stdio_plugin",
    inputs: include_str!("conformance.json"),
}
