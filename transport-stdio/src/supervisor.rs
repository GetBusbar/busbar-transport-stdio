// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! THE STDIO SUPERVISOR (`BUSBAR-1.6.0.md` THE DESIGN §2, l.388: "the stdio supervisor is the stdio
//! transport's"): the restart policy of a program that keeps ending, new in 1.6.0 (1.5.5 spawned no
//! program). Its figures are `predev`'s, the baseline of the MCP path (THE DESIGN l.380): the
//! pre-fold stdio MCP client's restart policy (a 100 ms backoff doubling to 30 s, five ends inside a
//! minute tripping its breaker). A circuit breaker, not a retry loop: each end is
//! timed on the host's one clock (monotonic, `clock.now`, THE DESIGN §1 and l.291); the next spawn
//! waits a backoff doubling from [`BASE_BACKOFF_NS`] to [`MAX_BACKOFF_NS`], and [`CRASH_THRESHOLD`]
//! ends inside [`CRASH_WINDOW_NS`] stop the restarts while the member keeps asking for them: every
//! dial the trip refuses renews it.
//!
//! One supervisor per SUPERVISION KEY (`DialIn::supervision_key`): the host mints a key for each
//! member and keeps it while the member's program is unchanged across re-declarations, so the
//! supervision resets exactly when the member is new — never for a sibling member running the same
//! program, never for a changed secret value under an unchanged member.

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
    /// `ends` ends inside the window tripped it: no restart while the member keeps asking.
    Tripped {
        /// The ends counted inside the window when it tripped.
        ends: usize,
    },
}

/// THE RESTART POLICY of one member's program, on the host's monotonic clock.
#[derive(Debug, Default)]
pub struct Supervisor {
    /// Its programs running now: connections dialled and not yet shut.
    live: usize,
    /// The last dial a trip refused: the trip holds a full window past it.
    asked: u64,
    /// The ends inside the window, oldest first.
    crashes: Vec<u64>,
    /// No spawn before this instant.
    restart_at: Option<u64>,
    /// [`CRASH_THRESHOLD`] ends inside the window: no restart while the member keeps asking.
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
    /// member running, and no hold running. A hold runs while a backoff is pending or an end is
    /// inside the window; a trip runs for a whole window past its last end and past the last dial
    /// it refused, so a member that keeps asking stays held down. A member declared anew dials
    /// under a fresh key and its old key is never dialled again: its state is dropped once its
    /// hold has run out, and the map holds only live members and running holds.
    pub fn idle(&self, now: u64) -> bool {
        let quiet = |t: u64| now.saturating_sub(t) > CRASH_WINDOW_NS;
        self.live == 0
            && self.restart_at.is_none_or(|at| now >= at)
            && self.crashes.iter().all(|t| quiet(*t))
            && (!self.tripped || quiet(self.asked))
    }

    /// What holds the program down at `now`, if anything: the ends counted and how long. A trip
    /// that refuses this dial is renewed by it.
    pub fn admit(&mut self, now: u64) -> Option<Hold> {
        let hold = self.hold(now);
        if matches!(hold, Some(Hold::Tripped { .. })) {
            self.asked = self.asked.max(now);
        }
        hold
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

/// EVERY MEMBER'S SUPERVISION, by its supervision key: an entry only while its member has a
/// program running or a hold running ([`Supervisor::idle`]), so the map is bounded by the live
/// members and their running holds, however many times members are declared anew.
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
        self.by_key.get_mut(&key).and_then(|s| s.admit(now))
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
