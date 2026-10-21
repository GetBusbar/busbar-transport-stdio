// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

use super::*;

impl Supervisor {
    /// Whether nothing holds the program down at `now`.
    fn may_start(&self, now: u64) -> bool {
        self.hold(now).is_none()
    }
}

/// `predev`'s restart policy (the pre-fold stdio MCP client's), figure for figure: a backoff
/// doubling from 100 ms, and the fifth end inside a minute stopping the restarts for good.
#[test]
fn the_backoff_doubles_and_the_fifth_end_in_a_minute_trips() {
    let mut s = Supervisor::default();
    assert!(s.may_start(0), "a program never ended may start");
    let mut now = 1_000 * MS;
    for (n, backoff) in [100, 200, 400, 800].into_iter().enumerate() {
        s.crashed(now);
        assert!(!s.may_start(now + backoff * MS - 1), "end {n}: held down");
        assert!(
            s.may_start(now + backoff * MS),
            "end {n}: released after {backoff} ms"
        );
        now += backoff * MS;
    }
    s.crashed(now);
    assert!(!s.may_start(now + 3_600_000 * MS), "the fifth end trips it");
}

/// Ends outside the window are forgotten: four ends, a quiet minute, and the next end restarts
/// the count.
#[test]
fn ends_outside_the_window_are_forgotten() {
    let mut s = Supervisor::default();
    for k in 0..4 {
        s.crashed(k * MS);
    }
    let after = 4 * MS + CRASH_WINDOW_NS + 1;
    s.crashed(after);
    assert!(
        s.may_start(after + BASE_BACKOFF_NS),
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

/// The hold names what holds the program down: the ends counted inside the window and the backoff
/// left, or the trip. RED: one text said "keeps ending" after a single end.
#[test]
fn the_hold_counts_the_ends_and_tells_a_backoff_from_a_trip() {
    let mut s = Supervisor::default();
    assert_eq!(s.hold(0), None);
    s.crashed(0);
    assert_eq!(
        s.hold(30 * MS),
        Some(Hold::Backoff {
            ends: 1,
            remaining_ns: 70 * MS
        })
    );
    assert_eq!(s.hold(BASE_BACKOFF_NS), None, "the backoff passed");
    for k in 1..5 {
        s.crashed(k * MS);
    }
    assert_eq!(
        s.hold(3_600_000 * MS),
        Some(Hold::Tripped { ends: 5 }),
        "five ends in a minute: tripped"
    );
}
