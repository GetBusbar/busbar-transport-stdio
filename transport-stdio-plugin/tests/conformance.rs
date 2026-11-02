// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! The published conformance suite, run by this stdio plugin.

busbar_test_kit::conformance_suite! {
    door: busbar_transport_stdio::door::door,
    cdylib: "busbar_transport_stdio_plugin",
    inputs: include_str!("conformance.json"),
}
