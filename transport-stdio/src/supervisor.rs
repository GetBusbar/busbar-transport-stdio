// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! The stdio supervisor: the restart policy of a program that keeps ending. A circuit breaker, not a
//! retry loop: each end is timed on the host's one clock (monotonic, `clock.now`); the next spawn
//! waits a backoff that starts at [`BASE_BACKOFF_NS`] and doubles with each end inside
//! [`CRASH_WINDOW_NS`] (100, 200, 400, 800 ms), and the [`CRASH_THRESHOLD`]th end inside the window
//! stops the restarts until the member is declared anew. The trip, not a ceiling, ends the doubling.
//!
//! One supervisor per SUPERVISION KEY (`DialIn::supervision_key`): the host mints a key for each
//! member and keeps it while the member's program is unchanged across re-declarations, so the
//! supervision resets exactly when the member is new — never for a sibling member running the same
//! program, never for a changed secret value under an unchanged member.

/// Nanoseconds in a millisecond.
const MS: u64 = 1_000_000;

/// The first restart's backoff after a program ended.
pub const BASE_BACKOFF_NS: u64 = 100 * MS;
/// How many ends inside [`CRASH_WINDOW_NS`] stop the restarts.
pub const CRASH_THRESHOLD: usize = 5;
/// The window the ends are counted over.
pub const CRASH_WINDOW_NS: u64 = 60_000 * MS;

/// The restart policy of one member's program, on the host's monotonic clock.
#[derive(Debug, Default)]
pub struct Supervisor {
    /// The ends inside the window, oldest first.
    crashes: Vec<u64>,
    /// No spawn before this instant.
    restart_at: Option<u64>,
    /// [`CRASH_THRESHOLD`] ends inside the window: no restart until the member is declared anew.
    tripped: bool,
}

impl Supervisor {
    /// The program ended (or could not be spawned) at `now`.
    pub fn crashed(&mut self, now: u64) {
        self.crashes
            .retain(|t| now.saturating_sub(*t) <= CRASH_WINDOW_NS);
        self.crashes.push(now);
        if self.crashes.len() >= CRASH_THRESHOLD {
            self.tripped = true;
            return;
        }
        // Short of the trip the window holds 1 to CRASH_THRESHOLD - 1 ends: at most 3 doublings.
        let backoff = BASE_BACKOFF_NS << (self.crashes.len() - 1);
        self.restart_at = Some(now.saturating_add(backoff));
    }

    /// Whether it holds nothing at `now`: no end inside the window, no backoff pending, not
    /// tripped. An idle supervisor reads exactly as a fresh one, so its key's state may be dropped;
    /// a tripped one holds its member down until the member is declared anew.
    pub fn idle(&self, now: u64) -> bool {
        !self.tripped
            && self.restart_at.is_none_or(|at| now >= at)
            && self
                .crashes
                .iter()
                .all(|t| now.saturating_sub(*t) > CRASH_WINDOW_NS)
    }

    /// Whether the program may be spawned at `now`.
    pub fn may_restart(&self, now: u64) -> bool {
        !self.tripped && self.restart_at.is_none_or(|at| now >= at)
    }
}

#[cfg(test)]
#[path = "tests/supervisor_tests.rs"]
mod tests;
