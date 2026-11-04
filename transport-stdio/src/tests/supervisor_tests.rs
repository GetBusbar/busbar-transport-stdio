// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

use super::*;

#[test]
fn a_delayed_end_notification_does_not_shorten_an_existing_hold_down() {
    let mut s = Supervisor::default();
    s.crashed(5_000 * MS);

    s.crashed(4_000 * MS);
    assert!(!s.may_restart(5_100 * MS - 1));
    assert!(s.may_restart(5_100 * MS));

    s.crashed(5_100 * MS);
    assert!(!s.may_restart(5_500 * MS - 1));
    assert!(s.may_restart(5_500 * MS));
}

#[test]
fn a_delayed_end_outside_the_latest_window_does_not_trip_or_extend_the_hold_down() {
    let mut s = Supervisor::default();
    let latest = 2 * CRASH_WINDOW_NS;
    for _ in 0..CRASH_THRESHOLD - 1 {
        s.crashed(latest);
    }
    s.crashed(0);
    assert!(!s.may_restart(latest + 800 * MS - 1));
    assert!(s.may_restart(latest + 800 * MS));
    assert!(s.idle(latest + CRASH_WINDOW_NS + 1));
}

#[test]
fn a_delayed_end_exactly_at_the_latest_window_boundary_still_counts() {
    let mut s = Supervisor::default();
    let latest = 2 * CRASH_WINDOW_NS;
    for _ in 0..CRASH_THRESHOLD - 1 {
        s.crashed(latest);
    }
    s.crashed(latest - CRASH_WINDOW_NS);
    assert!(!s.may_restart(latest + 3_600_000 * MS));
}

/// The crash-loop policy of predev's connector, figure for figure: a backoff doubling from 100
/// ms, and the fifth end inside a minute stopping the restarts for good.
#[test]
fn the_backoff_doubles_and_the_fifth_end_in_a_minute_trips() {
    let mut s = Supervisor::default();
    assert!(s.may_restart(0), "a program never ended may start");
    let mut now = 1_000 * MS;
    for (n, backoff) in [100, 200, 400, 800].into_iter().enumerate() {
        s.crashed(now);
        assert!(!s.may_restart(now + backoff * MS - 1), "end {n}: held down");
        assert!(
            s.may_restart(now + backoff * MS),
            "end {n}: released after {backoff} ms"
        );
        now += backoff * MS;
    }
    s.crashed(now);
    assert!(
        !s.may_restart(now + 3_600_000 * MS),
        "the fifth end trips it"
    );
}

/// Ends outside the window are forgotten: four ends, a quiet minute, and the next end restarts
/// the count.
#[test]
fn ends_outside_the_window_are_forgotten() {
    let mut s = Supervisor::default();
    for k in 0..4 {
        s.crashed(k * MS);
    }
    let later = 4 * MS + CRASH_WINDOW_NS + 1;
    s.crashed(later);
    assert!(
        s.may_restart(later + BASE_BACKOFF_NS),
        "one end in the window: base backoff"
    );
}

#[test]
fn an_end_exactly_one_window_old_still_counts_and_is_not_idle() {
    let mut one = Supervisor::default();
    one.crashed(0);
    assert!(!one.idle(CRASH_WINDOW_NS));
    assert!(one.idle(CRASH_WINDOW_NS + 1));
    let mut trip = Supervisor::default();
    for _ in 0..4 {
        trip.crashed(0);
    }
    trip.crashed(CRASH_WINDOW_NS);
    assert!(!trip.may_restart(CRASH_WINDOW_NS + 3600 * MS));
}

/// A supervisor holding no end inside the window and no pending backoff is idle (it reads as a
/// fresh one, so its key's state may be dropped); a tripped one never is.
#[test]
fn a_supervisor_with_nothing_to_hold_is_idle_and_a_tripped_one_is_not() {
    let mut s = Supervisor::default();
    assert!(s.idle(0));
    s.crashed(0);
    assert!(!s.idle(BASE_BACKOFF_NS), "an end inside the window");
    assert!(
        s.idle(CRASH_WINDOW_NS + 1),
        "the end aged out and the backoff passed"
    );
    let mut t = Supervisor::default();
    for k in 0..5 {
        t.crashed(k * MS);
    }
    assert!(
        !t.idle(10 * CRASH_WINDOW_NS),
        "a tripped member is held until declared anew"
    );
}

#[test]
fn a_tripped_supervisor_does_not_retain_more_end_notifications() {
    let mut supervisor = Supervisor::default();
    for _ in 0..CRASH_THRESHOLD {
        supervisor.crashed(0);
    }
    for _ in 0..1024 {
        supervisor.crashed(0);
    }
    assert_eq!(supervisor.crashes.len(), CRASH_THRESHOLD);
    assert!(!supervisor.may_restart(10 * CRASH_WINDOW_NS));
}

#[test]
fn deferred_ends_are_bounded_and_cannot_be_swept_as_idle() {
    let mut supervisor = Supervisor::default();
    for _ in 0..1024 {
        supervisor.defer_end();
    }
    assert_eq!(supervisor.pending_ends, CRASH_THRESHOLD);
    assert!(!supervisor.idle(10 * CRASH_WINDOW_NS));
    supervisor.resolve_deferred(10 * CRASH_WINDOW_NS);
    assert_eq!(supervisor.pending_ends, 0);
    assert!(!supervisor.may_restart(100 * CRASH_WINDOW_NS));
}
