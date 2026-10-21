// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! **THE PUBLISHED CONFORMANCE SUITE, RUN BY THIS PLUGIN** (busbar TODO ABI-b4; OWNER 2026-10-03:
//! plugins test themselves against busbar). busbar's suite, at the commit this repo pins
//! (`.busbar-ref`), drives the `stdio` CARRIER two ways through the one loader: LINKED (the logic
//! crate's `door`) and DROPPED IN (this crate's built cdylib), over the transport kind's
//! carrier script with the inputs in `conformance.json`: a spawned program as the far end, one frame
//! per line both ways, a frame that cannot be one line refused, a pending read the host's wake
//! resumes, and the host's guard (a program the host did not admit is refused). Every step's
//! crossings exactly at the script's pin, the two folds equal, and the suite's RED arms kept.
//! `plugin-ci.yml` runs it under `--release`.

busbar_plugin_loader::conformance_suite! {
    door: busbar_transport_stdio::door,
    cdylib: "busbar_transport_stdio_plugin",
    inputs: include_str!("conformance.json"),
}

#[test]
fn the_statement_carries_the_declares_document() {
    let row = busbar_plugin_loader::dispatch::LinkedRow::of(busbar_transport_stdio::door).unwrap();
    let read = busbar_contract::abi::mechanism::rendering::read(&row.statement).unwrap();
    assert_eq!(
        busbar_contract::abi::mechanism::extensions::declares(&read.extensions.bytes),
        Some(&include_bytes!("../../transport-stdio/declares.json")[..]),
    );
}

/// THE POINTS THE DOOR STATES, as the loader reads them (`dispatch::kinds::transport::stated`, the
/// reader a bind uses): exactly the auth points the carrier calls, which is none. RED: the door
/// stated no offered-points record, so the loader read `{Head, HeadBody}`, points a carrier with
/// no head never calls, and a guest-list line needing them would seal and never be called.
#[test]
fn the_loader_reads_the_points_the_door_calls_and_no_head_point() {
    use busbar_contract::abi::transport::{AuthPoint, AuthPoints};
    let facts =
        busbar_plugin_loader::dispatch::kinds::transport::stated(busbar_transport_stdio::door)
            .expect("the door's Statement is read");
    assert!(!facts.offered.has(AuthPoint::Head), "no head point");
    assert!(
        !facts.offered.has(AuthPoint::HeadBody),
        "no head-and-body point"
    );
    assert_eq!(facts.offered, AuthPoints::EMPTY);
}
