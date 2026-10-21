// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! THE STDIO SUPERVISOR (`BUSBAR-1.6.0.md` THE DESIGN §2, l.388: "the stdio supervisor is the stdio
//! transport's"): the restart policy of a program that keeps ending, the crash-loop policy predev's
//! connector held (`program.rs`, the MCP baseline, FOLD-MCP F25) before it moved here; v1.5.5 had
//! no stdio transport and no program supervisor. A circuit breaker, not a retry loop: each end is
//! timed on the host's one clock (monotonic, `clock.now`, THE DESIGN §1 and l.291); the next spawn
//! waits a backoff doubling from [`BASE_BACKOFF_NS`] to [`MAX_BACKOFF_NS`], and [`CRASH_THRESHOLD`]
//! ends inside [`CRASH_WINDOW_NS`] TRIP it: the restarts stop, never on a timeout, whether or not
//! the member keeps asking. A trip lasts until that member's supervision key changes. `predev`'s
//! stdio client (busbar `732ba319c0`, stdio.rs:1013-1027) reset a child only when its own
//! `command:` changed, \"so touching an unrelated key does not re-arm a fork bomb\"; a config apply
//! that leaves the member's program alone therefore clears nothing here either.
//!
//! One supervisor per SUPERVISION KEY (`DialIn::supervision_key`): the host mints a key for each
//! member and keeps it while the member's program is unchanged across re-declarations, so the
//! supervision resets exactly when the member's program changes or the member is new — never for a
//! sibling member running the same program, never for a changed secret value under an unchanged
//! member, never for a config apply that touches an unrelated key.

use std::collections::HashMap;

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
    /// `ends` ends inside the window tripped it: no restart while the member's key stands.
    Tripped {
        /// The ends counted inside the window at the instant it tripped.
        ends: usize,
    },
}

/// THE RESTART POLICY of one member's program, on the host's monotonic clock.
#[derive(Debug, Default)]
pub struct Supervisor {
    /// Its programs running now: connections dialled and not yet shut.
    live: usize,
    /// The ends inside the window, oldest first.
    crashes: Vec<u64>,
    /// No spawn before this instant.
    restart_at: Option<u64>,
    /// [`CRASH_THRESHOLD`] ends inside the window: no restart while this key stands.
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

    /// Whether it holds nothing at `now`, so its key's state may be dropped: no program of its
    /// member running, no backoff pending, no end inside the window, and not tripped. A trip is
    /// kept for as long as its key stands, never dropped on a timeout.
    pub fn idle(&self, now: u64) -> bool {
        self.live == 0
            && !self.tripped
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

/// EVERY MEMBER'S SUPERVISION, by its supervision key. An entry is kept while its member has a
/// program running, a hold running or a trip ([`Supervisor::idle`]); only idle entries are swept.
/// A config apply never touches the map: a trip lasts until the member's key changes.
#[derive(Debug, Default)]
pub struct Supervised {
    by_key: HashMap<u64, Supervisor>,
}

impl Supervised {
    /// Drop every key that holds nothing at `now`.
    pub fn sweep(&mut self, now: u64) {
        self.by_key.retain(|_, s| !s.idle(now));
    }

    /// A dial for the member `key` at `now`: what holds it down, if anything.
    pub fn admit(&mut self, key: u64, now: u64) -> Option<Hold> {
        self.sweep(now);
        self.by_key.get(&key).and_then(|s| s.hold(now))
    }

    /// The member `key`'s program was spawned: it runs until its connection is shut.
    pub fn spawned(&mut self, key: u64) {
        self.by_key.entry(key).or_default().live += 1;
    }

    /// The member `key`'s program ended (or could not be spawned) at `now`.
    pub fn crashed(&mut self, key: u64, now: u64) {
        self.by_key.entry(key).or_default().crashed(now);
    }

    /// The member `key`'s connection was shut: its program no longer runs.
    pub fn closed(&mut self, key: u64) {
        if let Some(s) = self.by_key.get_mut(&key) {
            s.live = s.live.saturating_sub(1);
        }
    }
}

#[cfg(test)]
#[path = "tests/supervisor_tests.rs"]
mod tests;
