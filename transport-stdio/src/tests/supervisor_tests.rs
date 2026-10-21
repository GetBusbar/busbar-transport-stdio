// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

use super::*;

/// The crash-loop policy of predev's connector, figure for figure: a backoff doubling from 100 ms,
/// and the fifth end inside a minute stopping the restarts for good.
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

/// A supervisor holding no end inside the window and no pending backoff is idle (it reads as a
/// fresh one, so its key's state may be dropped); a tripped one never is. RED (Q128
/// transport-stdio LOW): the carrier kept every key's state for good.
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
