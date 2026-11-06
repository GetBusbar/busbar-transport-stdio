// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors
//! Native subprocess fixtures for real host-I/O conformance on every supported OS.
use std::io::{Read, Write};
use std::process::{Command, Stdio};
use std::time::Duration;

fn main() {
    match std::env::args().nth(1).as_deref() {
        Some("echo") => {
            let mut input = std::io::stdin().lock();
            let mut output = std::io::stdout().lock();
            let mut bytes = [0_u8; 4096];
            loop {
                let len = input.read(&mut bytes).unwrap();
                if len == 0 {
                    break;
                }
                output.write_all(&bytes[..len]).unwrap();
                output.flush().unwrap();
            }
        }
        Some("exit-open") => {
            // Exit independently of stdout EOF: the descendant inherits the pipe.
            // It expires just as the original shell fixture's background sleep did.
            let _descendant = Command::new(std::env::current_exe().unwrap())
                .arg("hold-open")
                .stdin(Stdio::null())
                .stdout(Stdio::inherit())
                .stderr(Stdio::null())
                .spawn()
                .unwrap();
            std::process::exit(3);
        }
        Some("hold-open") => std::thread::sleep(Duration::from_secs(5)),
        _ => panic!("expected echo, exit-open, or hold-open"),
    }
}
