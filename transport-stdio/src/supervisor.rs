// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! THE STDIO SUPERVISOR (`BUSBAR-1.6.0.md` THE DESIGN §2, l.388: "the stdio supervisor is the stdio
//! transport's"): the restart policy of a program that keeps ending, new in 1.6.0 (1.5.5 spawned no
//! program). Its figures are `predev`'s, the baseline of the MCP path (THE DESIGN l.380): the
//! pre-fold stdio MCP client's restart policy (a 100 ms backoff doubling to 30 s, five ends inside a
//! minute tripping its breaker). A circuit breaker, not a retry loop: each end is
//! timed on the host's one clock (monotonic, `clock.now`, THE DESIGN §1 and l.291); the next spawn
//! waits a backoff doubling from [`BASE_BACKOFF_NS`] to [`MAX_BACKOFF_NS`], and [`CRASH_THRESHOLD`]
//! ends inside [`CRASH_WINDOW_NS`] stop the restarts until the member is declared anew.
//!
//! One supervisor per SUPERVISION KEY (`DialIn::supervision_key`): the host mints a key for each
//! member and keeps it while the member's program is unchanged across re-declarations, so the
//! supervision resets exactly when the member is new — never for a sibling member running the same
//! program, never for a changed secret value under an unchanged member.

/// Nanoseconds in a millisecond.
const MS: u64 = 1_000_000;

/// The first restart's backoff after a program ended.
pub const BASE_BACKOFF_NS: u64 = 100 * MS;
/// The ceiling the doubling backoff stops at.
pub const MAX_BACKOFF_NS: u64 = 30_000 * MS;
/// How many ends inside [`CRASH_WINDOW_NS`] stop the restarts.
pub const CRASH_THRESHOLD: usize = 5;
/// The window the ends are counted over.
pub const CRASH_WINDOW_NS: u64 = 60_000 * MS;

/// Why a member's program may not be spawned now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Hold {
    /// Inside the backoff after `ends` ends in the window: `remaining_ns` until it may restart.
    Backoff {
        /// The ends counted inside the window.
        ends: usize,
        /// How long until the backoff passes.
        remaining_ns: u64,
    },
    /// `ends` ends inside the window tripped it: no restart until the member is declared anew.
    Tripped {
        /// The ends counted inside the window when it tripped.
        ends: usize,
    },
}

/// THE RESTART POLICY of one member's program, on the host's monotonic clock.
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
        let shift = u32::try_from(self.crashes.len().saturating_sub(1))
            .unwrap_or(u32::MAX)
            .min(20);
        let backoff = BASE_BACKOFF_NS
            .saturating_mul(1_u64 << shift)
            .min(MAX_BACKOFF_NS);
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

    /// What holds the program down at `now`, if anything: the ends counted and how long.
    pub fn hold(&self, now: u64) -> Option<Hold> {
        if self.tripped {
            return Some(Hold::Tripped {
                ends: self.crashes.len(),
            });
        }
        let at = self.restart_at.filter(|at| now < *at)?;
        Some(Hold::Backoff {
            ends: self
                .crashes
                .iter()
                .filter(|t| now.saturating_sub(**t) <= CRASH_WINDOW_NS)
                .count(),
            remaining_ns: at - now,
        })
    }
}

#[cfg(test)]
#[path = "tests/supervisor_tests.rs"]
mod tests;
