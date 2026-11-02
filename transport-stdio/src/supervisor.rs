// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! The stdio supervisor: the restart policy of a program that keeps ending. A circuit breaker, not a

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
    /// The ends inside the window, in the order their notifications acquired the lock.
    crashes: Vec<u64>,
    /// The newest end observed, even when an older notification arrives after it.
    latest_crash: Option<u64>,
    /// No spawn before this instant.
    restart_at: Option<u64>,
    /// [`CRASH_THRESHOLD`] ends inside the window: no restart until the member is declared anew.
    tripped: bool,
}

impl Supervisor {
    /// The program ended (or could not be spawned) at `now`.
    pub fn crashed(&mut self, now: u64) {
        let latest = self.latest_crash.map_or(now, |at| at.max(now));
        self.latest_crash = Some(latest);
        self.crashes
            .retain(|t| latest.saturating_sub(*t) <= CRASH_WINDOW_NS);

        if latest.saturating_sub(now) > CRASH_WINDOW_NS {
            return;
        }
        self.crashes.push(now);
        if self.crashes.len() >= CRASH_THRESHOLD {
            self.tripped = true;
            return;
        }

        let backoff = BASE_BACKOFF_NS << (self.crashes.len() - 1);

        let restart_at = now.saturating_add(backoff);
        self.restart_at = Some(self.restart_at.map_or(restart_at, |at| at.max(restart_at)));
    }

    /// Whether it holds nothing at `now`: no end inside the window, no backoff pending, not

    pub fn idle(&self, now: u64) -> bool {
        self.may_restart(now)
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
