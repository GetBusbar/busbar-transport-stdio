// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

use super::*;

impl Supervisor {
    /// Whether nothing holds the program down at `now`.
    fn may_start(&self, now: u64) -> bool {
        self.hold(now).is_none()
    }
}

/// The crash-loop policy of predev's connector, figure for figure: a backoff doubling from 100 ms,
/// and the fifth end inside a minute stopping the restarts for good.
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
/// fresh one, so its key's state may be dropped); a tripped one never is, however long after, and
/// one with a program running never is.
#[test]
fn a_supervisor_with_nothing_to_hold_is_idle_and_a_trip_never_is() {
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
        !t.idle(1_000 * CRASH_WINDOW_NS),
        "a trip is never dropped on a timeout"
    );
    let running = Supervisor {
        live: 1,
        ..Supervisor::default()
    };
    assert!(
        !running.idle(10 * CRASH_WINDOW_NS),
        "a member with a program running is kept"
    );
}

impl Supervised {
    /// How many keys it holds.
    fn keys(&self) -> usize {
        self.by_key.len()
    }
}

/// Trip the member `key`: five ends a second apart, each after its backoff. Answers the instant
/// after the trip.
fn trip(m: &mut Supervised, key: u64, mut now: u64) -> u64 {
    for _ in 0..CRASH_THRESHOLD {
        assert_eq!(m.admit(key, now), None, "key {key} may start");
        m.spawned(key);
        m.crashed(key, now);
        m.closed(key);
        now += 1_000 * MS;
    }
    now
}

/// A TRIP NEVER TIMES OUT AND LASTS UNTIL ITS KEY CHANGES (`predev`'s stdio client, busbar
/// `732ba319c0`, stdio.rs:1013-1027, reset a child only when its own `command:` changed). A
/// tripped key is kept and refused after an hour of silence and a sweep, while an idle entry is
/// swept; a member's changed program is a new key and is admitted. RED: a trip was cleared by
/// the config apply, or dropped on a timeout.
#[test]
fn a_trip_lasts_until_its_key_changes_and_only_idle_entries_are_swept() {
    let mut m = Supervised::default();
    let mut now = 1_000 * MS;
    // A member whose program runs.
    assert_eq!(m.admit(1, now), None);
    m.spawned(1);
    // A member whose program ended once and is idle after its window.
    m.spawned(2);
    m.crashed(2, now);
    m.closed(2);
    // Ten tripped members.
    for key in 100..110 {
        now = trip(&mut m, key, now);
        assert!(matches!(m.admit(key, now), Some(Hold::Tripped { .. })));
    }
    // An hour of silence: every trip still holds, and is kept; the idle entry is swept.
    now += 3_600_000 * MS;
    m.sweep(now);
    assert_eq!(m.keys(), 11, "the live member and every trip");
    assert!(matches!(m.admit(100, now), Some(Hold::Tripped { .. })));
    // The member's command changed: a new key, admitted.
    assert_eq!(m.admit(200, now), None, "a changed program is a new key");
    // Its program shut, and nothing held, the live member's entry goes; the trips stay.
    m.closed(1);
    m.sweep(now);
    assert_eq!(m.keys(), 10);
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
