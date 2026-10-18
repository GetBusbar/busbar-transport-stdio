// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

use super::*;

/// 1.5.5's crash-loop policy, figure for figure: a backoff doubling from 100 ms, and the fifth end
/// inside a minute stopping the restarts for good.
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
