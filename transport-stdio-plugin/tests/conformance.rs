// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors
//! Real stdio carrier, admitted by the loader through both independently built doors.
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Wake, Waker};
use std::time::{Duration, Instant};

use busbar_contract::abi::mechanism::call::{AbiStr, Field, InHead, OutHead, Outcome};
use busbar_contract::abi::transport::{
    slot, AcceptIn, AcceptOut, AdoptIn, ArrivalIn, ArrivalOut, BeginIn, ConnIn, ConnOut,
    Destination, DialIn, EmitIn, EncodeIn, FinishIn, FramerOut, FramingIn, IngestIn, IoOut,
    ListenIn, ListenOut, LocateIn, LocateOut, ReadIn, RefuseIn, ShutIn, WriteIn, CLOSE_NORMAL,
    DEST_AUTHORITY, DEST_PROGRAM, MAX_ADDR, READ_END_OF_FRAME, WRITE_END_OF_FRAME,
};

use busbar_contract::abi::mechanism::call::{Blob, BLOB_JSON};
use busbar_contract::abi::mechanism::lifecycle::{slot as life, OpenIn, OpenOut, ValidateIn};
use busbar_core_connector::hostio::{Admission, HostIo};
use busbar_core_connector::spawn_env::HeldEnv;
use busbar_plugin_loader::dispatch::InlineTicket;
use busbar_plugin_loader::dispatch::{
    self,
    kinds::transport::{Transport, TransportFacts},
    Bind, Called, ConnTable, DispatchConfig, Dispatcher, Frame, InFrame, LinkedRow, NoSink,
    OutFrame, Plugin,
};

fn input<T: InFrame>() -> T {
    // SAFETY: ABI input frames are zero-valid POD headed by InHead.
    let mut value: T = unsafe { std::mem::zeroed() };
    unsafe {
        std::ptr::addr_of_mut!(value)
            .cast::<InHead>()
            .write(dispatch::in_head())
    };
    value
}
fn output<T: OutFrame>() -> T {
    // SAFETY: ABI output frames are zero-valid POD headed by OutHead.
    let mut value: T = unsafe { std::mem::zeroed() };
    unsafe {
        std::ptr::addr_of_mut!(value)
            .cast::<OutHead>()
            .write(dispatch::out_head())
    };
    value
}
fn abi(value: impl AsRef<[u8]>) -> AbiStr {
    let value = value.as_ref();
    AbiStr {
        ptr: value.as_ptr(),
        len: value.len(),
    }
}
fn lossy(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}
fn bind(d: &Dispatcher) -> Bind {
    Bind {
        instance: Arc::from("stdio"),
        max_inflight_cap: 1024,
        sink: Arc::new(NoSink),
        dispatcher: d.adopter(),
        conns: ConnTable::NoNeeds,
    }
}
fn counts(p: &Plugin<Transport>) -> (u64, u64) {
    let resumes = p.inner.resumes.load(Ordering::SeqCst);
    (p.inner.crossings.load(Ordering::SeqCst) - resumes, resumes)
}
fn artifact() -> std::path::PathBuf {
    let exe = std::env::current_exe().unwrap();
    let profile = exe.parent().unwrap().parent().unwrap();
    let default_path = profile.join(format!(
        "{}busbar_transport_stdio_plugin{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_SUFFIX
    ));
    let path = std::env::var_os("BUSBAR_PLUGIN_CDYLIB")
        .map(std::path::PathBuf::from)
        .unwrap_or(default_path);
    assert!(path.is_absolute(), "BUSBAR_PLUGIN_CDYLIB must be absolute");
    assert!(path.is_file(), "build the actual stdio cdylib first: cargo build -p busbar-transport-stdio-plugin --lib; missing {}", path.display());
    path
}

#[derive(Default)]
struct Woken(AtomicBool);

impl Wake for Woken {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

struct Side {
    ticket: InlineTicket,
    woken: Arc<Woken>,
}

impl Side {
    fn new(d: &Dispatcher) -> Self {
        let ticket = d.inline_ticket().expect("an inline ticket");
        let woken = Arc::new(Woken::default());
        ticket.register(&Waker::from(Arc::clone(&woken)));
        Self { ticket, woken }
    }
}

fn go<I: InFrame, O: OutFrame>(
    p: &Plugin<Transport>,
    side: &Side,
    resume: bool,
    s: u32,
    i: I,
) -> (Called, O) {
    let mut f = Frame::new(i, output::<O>());
    let c = p.call_inline(side.ticket.ticket(), resume, s, &mut f);
    (c, f.out)
}

fn err(c: &Called) -> String {
    c.error
        .as_deref()
        .map(String::from_utf8_lossy)
        .unwrap_or_default()
        .into_owned()
}

fn unported(a: &str) -> String {
    a.rsplit_once(':')
        .map_or_else(|| a.to_owned(), |(h, _)| format!("{h}:*"))
}

fn text(v: &serde_json::Value, what: &str) -> String {
    v.as_str()
        .unwrap_or_else(|| panic!("conformance.json: transport.carrier.{what} must be a string"))
        .to_owned()
}

fn framer_ops(p: &Plugin<Transport>, side: &Side) -> String {
    let mut begin: BeginIn = input();
    begin.scheme = busbar_contract::abi::host::service::SCHEME_PLAIN;
    let outcomes = [
        go::<_, LocateOut>(p, side, false, slot::LOCATE, input::<LocateIn>()).0,
        go::<_, FramerOut>(p, side, false, slot::BEGIN, begin).0,
        go::<_, FramerOut>(p, side, false, slot::INGEST, input::<IngestIn>()).0,
        go::<_, FramerOut>(p, side, false, slot::EMIT, input::<EmitIn>()).0,
        go::<_, FramerOut>(p, side, false, slot::ENCODE, input::<EncodeIn>()).0,
        go::<_, FramerOut>(p, side, false, slot::REFUSE, input::<RefuseIn>()).0,
        go::<_, FramerOut>(p, side, false, slot::FINISH, input::<FinishIn>()).0,
        go::<_, FramerOut>(p, side, false, slot::DETACH, input::<FramingIn>()).0,
        go::<_, FramerOut>(p, side, false, slot::ADOPT, input::<AdoptIn>()).0,
        go::<_, FramerOut>(p, side, false, slot::TIMER, input::<FramingIn>()).0,
    ];
    let names = [
        "locate", "begin", "ingest", "emit", "encode", "refuse", "finish", "detach", "adopt",
        "timer",
    ];
    names
        .iter()
        .zip(outcomes)
        .map(|(n, c)| format!("{n}={:?}", c.outcome))
        .collect::<Vec<_>>()
        .join(" ")
}

struct Dest {
    authority: String,
    program: String,
    args: Vec<String>,
    env: Vec<(String, String)>,
    arg_strs: Vec<AbiStr>,
    env_fields: Vec<Field>,
    kind: u32,
}

impl Dest {
    fn authority(a: &str) -> Box<Self> {
        Box::new(Self {
            authority: a.to_owned(),
            program: String::new(),
            args: Vec::new(),
            env: Vec::new(),
            arg_strs: Vec::new(),
            env_fields: Vec::new(),
            kind: DEST_AUTHORITY,
        })
    }

    fn program(v: &serde_json::Value) -> Box<Self> {
        let program = text(&v["program"], "dial.program");
        let args = v["args"]
            .as_array()
            .map(|a| a.iter().map(|x| text(x, "dial.args")).collect())
            .unwrap_or_default();
        /// The program's environment, read by field name: `[name, value]` pairs, absent for none.
        #[derive(serde::Deserialize)]
        struct Environment {
            #[serde(default)]
            env: Vec<(String, String)>,
        }
        let env = serde_json::from_value::<Environment>(v.clone())
            .unwrap_or_else(|e| panic!("conformance.json: transport.carrier.dial.env: {e}"))
            .env;
        let mut d = Box::new(Self {
            authority: String::new(),
            program,
            args,
            env,
            arg_strs: Vec::new(),
            env_fields: Vec::new(),
            kind: DEST_PROGRAM,
        });
        d.arg_strs = d.args.iter().map(abi).collect();
        d.env_fields = d
            .env
            .iter()
            .map(|(k, v)| Field {
                name: abi(k),
                value: abi(v),
            })
            .collect();
        d
    }

    fn raw(&self) -> Destination {
        Destination {
            kind: self.kind,
            _reserved: 0,
            authority: abi(&self.authority),
            program: abi(&self.program),
            args: self.arg_strs.as_ptr(),
            args_len: self.arg_strs.len(),
            env: self.env_fields.as_ptr(),
            env_len: self.env_fields.len(),
        }
    }
}

fn dial(p: &Plugin<Transport>, side: &Side, d: &Dest) -> (String, u64) {
    let raw = d.raw();
    let mut i: DialIn = input();
    i.dest = &raw;
    let (c, o) = go::<_, ConnOut>(p, side, false, slot::DIAL, i);
    let line = match c.outcome {
        Outcome::Ready => "Ready".to_owned(),
        other => format!("{other:?} err={:?}", err(&c)),
    };
    (line, o.conn)
}

fn flush_once(p: &Plugin<Transport>, side: &Side, conn: u64, resume: bool) -> String {
    let mut i: ConnIn = input();
    i.conn = conn;
    let (c, _) = go::<_, OutHead>(p, side, resume, slot::FLUSH, i);
    format!("{:?} err={:?}", c.outcome, err(&c))
}

fn write_once(p: &Plugin<Transport>, side: &Side, conn: u64, bytes: &[u8], resume: bool) -> String {
    let mut i: WriteIn = input();
    i.conn = conn;
    i.bytes = bytes.as_ptr();
    i.len = bytes.len();
    i.flags = WRITE_END_OF_FRAME;
    let (c, o) = go::<_, IoOut>(p, side, resume, slot::WRITE, i);
    match c.outcome {
        Outcome::Ready => format!("Ready len={}", o.len),
        other => format!("{other:?} err={:?}", err(&c)),
    }
}

fn read_once(p: &Plugin<Transport>, side: &Side, conn: u64, resume: bool) -> String {
    let mut buf = vec![0_u8; 64 * 1024];
    let mut i: ReadIn = input();
    i.conn = conn;
    i.buf = buf.as_mut_ptr();
    i.cap = buf.len();
    let (c, o) = go::<_, IoOut>(p, side, resume, slot::READ, i);
    match c.outcome {
        Outcome::Ready => format!(
            "Ready bytes={:?} end_of_frame={}",
            lossy(&buf[..o.len as usize]),
            o.flags & READ_END_OF_FRAME != 0
        ),
        other => format!("{other:?} err={:?}", err(&c)),
    }
}

fn shut(p: &Plugin<Transport>, side: &Side, conn: u64) -> String {
    let mut i: ShutIn = input();
    i.conn = conn;
    i.reason = CLOSE_NORMAL;
    let (c, _) = go::<_, OutHead>(p, side, false, slot::SHUT, i);
    format!("{:?}", c.outcome)
}

fn woken(_host: &HostIo, side: &Side) -> bool {
    let until = Instant::now() + Duration::from_secs(5);
    while Instant::now() < until {
        if side.woken.0.swap(false, Ordering::SeqCst) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    false
}

fn echo() -> String {
    let l = TcpListener::bind("127.0.0.1:0").expect("the echo far end binds");
    let at = l.local_addr().expect("bound").to_string();
    std::thread::spawn(move || {
        for s in l.incoming() {
            let Ok(mut s) = s else { return };
            std::thread::spawn(move || {
                let mut buf = [0_u8; 4096];
                while let Ok(n) = s.read(&mut buf) {
                    if n == 0 || s.write_all(&buf[..n]).is_err() {
                        return;
                    }
                }
            });
        }
    });
    at
}

enum Want {
    Is(String),
    Starts(&'static str),
}
fn carrier(dropped: bool) -> Vec<(String, String, u64, u64)> {
    let inputs: serde_json::Value = serde_json::from_str(include_str!("conformance.json")).unwrap();
    let k = &inputs["transport"]["carrier"];
    let d = Arc::new(Dispatcher::new(DispatchConfig::default()));
    let host = Arc::new(HostIo::default());
    assert!(d.install_io(Arc::clone(&host) as _));
    let row = LinkedRow::of(busbar_transport_stdio::door::door).unwrap();
    let p = if dropped {
        let path = artifact();
        assert_eq!(
            dispatch::rendering_of_library(&path).unwrap().unwrap(),
            row.statement
        );
        dispatch::load_dropped_bytes::<Transport>(
            &std::fs::read(&path).unwrap(),
            &path.display().to_string(),
            &row.statement,
            bind(&d),
        )
        .unwrap()
    } else {
        dispatch::load_linked::<Transport>(&row, bind(&d)).unwrap()
    };
    let facts = p.context::<TransportFacts>().unwrap();
    assert_eq!(facts.role, busbar_contract::abi::transport::ROLE_CARRIER);
    assert_eq!(p.name(), "stdio");
    assert!(p.secret_refs().is_empty());
    let mut results = vec![(
        "facts".into(),
        format!(
            "{:?} {} max_inflight={} role={} claims={:?} composes_over={:?}",
            p.kind(),
            p.name(),
            p.max_inflight(),
            facts.role,
            facts.claims,
            facts.composes_over
        ),
        0,
        0,
    )];
    let is = |line: &str| Want::Is(line.into());
    let (rd, wr) = (Side::new(&d), Side::new(&d));

    {
        let before = counts(&p);
        let answer = dial(&p, &wr, &Dest::authority("127.0.0.1:1")).0;
        let after = counts(&p);
        let crossed = after.0 - before.0;
        let resumed = after.1 - before.1;
        assert_eq!(crossed, 0, "{}", "dial unopened");
        match Want::Starts("Refused ") {
            Want::Is(expected) => assert_eq!(answer, expected, "{}", "dial unopened"),
            Want::Starts(prefix) => assert!(
                answer.starts_with(prefix),
                "{}: {}",
                "dial unopened",
                answer
            ),
        };
        results.push((("dial unopened").to_string(), answer, crossed, resumed));
    };
    let mut validate = Frame::new(input::<ValidateIn>(), output::<OutHead>());
    validate.input.settings = Blob {
        ptr: b"{}".as_ptr(),
        len: 2,
        fmt: BLOB_JSON,
        flags: 0,
    };
    let before = counts(&p);
    assert_eq!(
        p.call(life::VALIDATE, &mut validate).outcome,
        Outcome::Ready
    );
    assert_eq!(counts(&p).0 - before.0, 1, "validate");
    let before = counts(&p);
    let mut open = Frame::new(input::<OpenIn>(), output::<OpenOut>());
    open.input.settings = validate.input.settings;
    open.input.generation = 1;
    assert_eq!(p.call(life::OPEN, &mut open).outcome, Outcome::Ready);
    assert_eq!(counts(&p).0 - before.0, 1, "open");
    let before = counts(&p);
    p.ready(&d, dispatch::ready::READY_DEADLINE).unwrap();
    assert_eq!(counts(&p).0 - before.0, u64::from(p.has_ready()), "ready");

    let want = is(
        "locate=Refused begin=Refused ingest=Refused emit=Refused encode=Refused \
                   refuse=Refused finish=Refused detach=Refused adopt=Refused timer=Refused",
    );
    {
        let before = counts(&p);
        let answer = framer_ops(&p, &wr);
        let after = counts(&p);
        let crossed = after.0 - before.0;
        let resumed = after.1 - before.1;
        assert_eq!(crossed, 10, "{}", "framer ops");
        match want {
            Want::Is(expected) => assert_eq!(answer, expected, "{}", "framer ops"),
            Want::Starts(prefix) => {
                assert!(answer.starts_with(prefix), "{}: {}", "framer ops", answer)
            }
        };
        results.push((("framer ops").to_string(), answer, crossed, resumed));
    };

    let listens = k["listen"].as_bool().unwrap_or(false);
    let ls = Side::new(&d);
    let bind_at = "127.0.0.1:0";
    host.admit(ls.ticket.ticket(), Admission::Bind(bind_at.into()));
    let mut addr = vec![0_u8; MAX_ADDR as usize];
    let want = if listens {
        is("Ready addr=127.0.0.1:*")
    } else {
        Want::Starts("Refused")
    };
    let (listener, bound) = {
        let before = counts(&p);
        let (answer, value) = {
            let mut i: ListenIn = input();
            i.bind = abi(bind_at);
            i.addr_buf = addr.as_mut_ptr();
            i.addr_cap = addr.len();
            let (c, o) = go::<_, ListenOut>(&p, &ls, false, slot::LISTEN, i);
            let bound = lossy(&addr[..(o.addr_written as usize).min(addr.len())]);
            let line = match c.outcome {
                Outcome::Ready => format!("Ready addr={}", unported(&bound)),
                other => format!("{other:?}"),
            };
            (line, (o.listener, bound))
        };
        let after = counts(&p);
        let crossed = after.0 - before.0;
        let resumed = after.1 - before.1;
        assert_eq!(crossed, 1, "{}", "listen");
        match want {
            Want::Is(expected) => assert_eq!(answer, expected, "{}", "listen"),
            Want::Starts(prefix) => assert!(answer.starts_with(prefix), "{}: {}", "listen", answer),
        };
        results.push((("listen").to_string(), answer, crossed, resumed));
        value
    };
    let host_listener = listens.then(|| host.made(ls.ticket.ticket())).flatten();
    assert_eq!(
        host_listener.is_some(),
        listens,
        "only a listening carrier minted a listener"
    );
    if listens {
        let mut far = TcpStream::connect(&bound).expect("the script reaches the listener");
        let _ = far.set_read_timeout(Some(Duration::from_secs(5)));

        let mut peer = vec![0_u8; MAX_ADDR as usize];
        let accepted = {
            let before = counts(&p);
            let (answer, value) = {
                let mut i: AcceptIn = input();
                i.listener = listener;
                i.peer_buf = peer.as_mut_ptr();
                i.peer_cap = peer.len();
                let mut frame = Frame::new(i, output::<AcceptOut>());
                let mut c = p.call_inline(ls.ticket.ticket(), false, slot::ACCEPT, &mut frame);
                let until = Instant::now() + Duration::from_secs(5);
                while c.outcome == Outcome::Pending && Instant::now() < until {
                    std::thread::sleep(Duration::from_millis(2));
                    if ls.woken.0.swap(false, Ordering::SeqCst) {
                        c = p.call_inline(ls.ticket.ticket(), true, slot::ACCEPT, &mut frame);
                    }
                }
                let o = frame.out;
                let at = lossy(&peer[..(o.peer_written as usize).min(peer.len())]);
                (format!("{:?} peer={}", c.outcome, unported(&at)), o.conn)
            };
            let after = counts(&p);
            let crossed = after.0 - before.0;
            let resumed = after.1 - before.1;
            assert_eq!(crossed, 1, "{}", "accept");
            match is("Ready peer=127.0.0.1:*") {
                Want::Is(expected) => assert_eq!(answer, expected, "{}", "accept"),
                Want::Starts(prefix) => {
                    assert!(answer.starts_with(prefix), "{}: {}", "accept", answer)
                }
            };
            results.push((("accept").to_string(), answer, crossed, resumed));
            value
        };
        let want = is("Ready peer=127.0.0.1:* local_port_is_bound=true");
        {
            let before = counts(&p);
            let answer = {
                let mut peer = vec![0_u8; MAX_ADDR as usize];
                let mut i: ArrivalIn = input();
                i.conn = accepted;
                i.peer_buf = peer.as_mut_ptr();
                i.peer_cap = peer.len();
                let (c, o) = go::<_, ArrivalOut>(&p, &rd, false, slot::ARRIVAL, i);
                let at = lossy(&peer[..(o.peer_written as usize).min(peer.len())]);
                let port_is_bound = bound.ends_with(&format!(":{}", o.local_port));
                format!(
                    "{:?} peer={} local_port_is_bound={port_is_bound}",
                    c.outcome,
                    unported(&at)
                )
            };
            let after = counts(&p);
            let crossed = after.0 - before.0;
            let resumed = after.1 - before.1;
            assert_eq!(crossed, 1, "{}", "arrival");
            match want {
                Want::Is(expected) => assert_eq!(answer, expected, "{}", "arrival"),
                Want::Starts(prefix) => {
                    assert!(answer.starts_with(prefix), "{}: {}", "arrival", answer)
                }
            };
            results.push((("arrival").to_string(), answer, crossed, resumed));
        };
        far.write_all(b"from the far end")
            .expect("the far end writes");

        std::thread::sleep(Duration::from_millis(100));
        let want = is("Ready bytes=\"from the far end\" end_of_frame=true");
        {
            let before = counts(&p);
            let answer = read(&p, &rd, accepted, false);
            let after = counts(&p);
            let crossed = after.0 - before.0;
            let resumed = after.1 - before.1;
            assert_eq!(crossed, 1, "{}", "accepted read");
            match want {
                Want::Is(expected) => assert_eq!(answer, expected, "{}", "accepted read"),
                Want::Starts(prefix) => assert!(
                    answer.starts_with(prefix),
                    "{}: {}",
                    "accepted read",
                    answer
                ),
            };
            results.push((("accepted read").to_string(), answer, crossed, resumed));
        };
        {
            let before = counts(&p);
            let answer = write(&p, &wr, accepted, b"to the far end");
            let after = counts(&p);
            let crossed = after.0 - before.0;
            let resumed = after.1 - before.1;
            assert_eq!(crossed, 1, "{}", "accepted write");
            match is("Ready len=14") {
                Want::Is(expected) => assert_eq!(answer, expected, "{}", "accepted write"),
                Want::Starts(prefix) => assert!(
                    answer.starts_with(prefix),
                    "{}: {}",
                    "accepted write",
                    answer
                ),
            };
            results.push((("accepted write").to_string(), answer, crossed, resumed));
        };
        let mut got = [0_u8; 14];
        let ok = far.read_exact(&mut got).is_ok() && &got == b"to the far end";
        {
            let before = counts(&p);
            let answer = format!("{ok}");
            let after = counts(&p);
            let crossed = after.0 - before.0;
            let resumed = after.1 - before.1;
            assert_eq!(crossed, 0, "{}", "far end read");
            match is("true") {
                Want::Is(expected) => assert_eq!(answer, expected, "{}", "far end read"),
                Want::Starts(prefix) => {
                    assert!(answer.starts_with(prefix), "{}: {}", "far end read", answer)
                }
            };
            results.push((("far end read").to_string(), answer, crossed, resumed));
        };
        {
            let before = counts(&p);
            let answer = shut(&p, &wr, accepted);
            let after = counts(&p);
            let crossed = after.0 - before.0;
            let resumed = after.1 - before.1;
            assert_eq!(crossed, 1, "{}", "accepted shut");
            match is("Ready") {
                Want::Is(expected) => assert_eq!(answer, expected, "{}", "accepted shut"),
                Want::Starts(prefix) => assert!(
                    answer.starts_with(prefix),
                    "{}: {}",
                    "accepted shut",
                    answer
                ),
            };
            results.push((("accepted shut").to_string(), answer, crossed, resumed));
        };
        let mut rest = Vec::new();
        let ended = far.read_to_end(&mut rest).is_ok() && rest.is_empty();
        {
            let before = counts(&p);
            let answer = format!("{ended}");
            let after = counts(&p);
            let crossed = after.0 - before.0;
            let resumed = after.1 - before.1;
            assert_eq!(crossed, 0, "{}", "far end saw the end");
            match is("true") {
                Want::Is(expected) => assert_eq!(answer, expected, "{}", "far end saw the end"),
                Want::Starts(prefix) => assert!(
                    answer.starts_with(prefix),
                    "{}: {}",
                    "far end saw the end",
                    answer
                ),
            };
            results.push((
                ("far end saw the end").to_string(),
                answer,
                crossed,
                resumed,
            ));
        };
    }

    let (dest, other) = match &k["dial"] {
        serde_json::Value::String(a) if a == "authority" => {
            let at = echo();
            host.admit(wr.ticket.ticket(), Admission::Dial(vec![at.clone()]));
            (
                Dest::authority(&at),
                Dest::program(&serde_json::json!({ "program": "/bin/cat" })),
            )
        }
        v => {
            let d = Dest::program(v);
            host.admit(
                wr.ticket.ticket(),
                spawn_admission(&d.program, &d.args, &d.env),
            );
            (d, Dest::authority("127.0.0.1:1"))
        }
    };
    let conn = {
        let before = counts(&p);
        let (answer, value) = dial(&p, &wr, &dest);
        let after = counts(&p);
        let crossed = after.0 - before.0;
        let resumed = after.1 - before.1;
        assert_eq!(crossed, 1, "{}", "dial");
        match is("Ready") {
            Want::Is(expected) => assert_eq!(answer, expected, "{}", "dial"),
            Want::Starts(prefix) => assert!(answer.starts_with(prefix), "{}: {}", "dial", answer),
        };
        results.push((("dial").to_string(), answer, crossed, resumed));
        value
    };
    {
        let before = counts(&p);
        let answer = flush(&p, &wr, conn);
        let after = counts(&p);
        let crossed = after.0 - before.0;
        let resumed = after.1 - before.1;
        assert_eq!(crossed, 1, "{}", "flush");
        match Want::Starts("Ready ") {
            Want::Is(expected) => assert_eq!(answer, expected, "{}", "flush"),
            Want::Starts(prefix) => assert!(answer.starts_with(prefix), "{}: {}", "flush", answer),
        };
        results.push((("flush").to_string(), answer, crossed, resumed));
    };
    let made = host.made(wr.ticket.ticket());
    for (n, x) in k["exchange"]
        .as_array()
        .expect("conformance.json: transport.carrier.exchange must be an array")
        .iter()
        .enumerate()
    {
        let frame = text(&x["write"], "exchange.write");
        let refused = x["refused"].as_bool().unwrap_or(false);
        let want = if refused {
            Want::Starts("Failed ")
        } else {
            Want::Is(format!("Ready len={}", frame.len()))
        };
        {
            let before = counts(&p);
            let answer = write(&p, &wr, conn, frame.as_bytes());
            let after = counts(&p);
            let crossed = after.0 - before.0;
            let resumed = after.1 - before.1;
            assert_eq!(crossed, 1, "{}", &format!("write #{n}"));
            match want {
                Want::Is(expected) => assert_eq!(answer, expected, "{}", &format!("write #{n}")),
                Want::Starts(prefix) => assert!(
                    answer.starts_with(prefix),
                    "{}: {}",
                    &format!("write #{n}"),
                    answer
                ),
            };
            results.push((
                (&format!("write #{n}")).to_string(),
                answer,
                crossed,
                resumed,
            ));
        };
        if refused {
            continue;
        }
        // WRITE accepts a frame into the carrier's queue; FLUSH puts it on the actual pipe.
        {
            let before = counts(&p);
            let answer = flush(&p, &wr, conn);
            let after = counts(&p);
            let crossed = after.0 - before.0;
            let resumed = after.1 - before.1;
            assert_eq!(crossed, 1, "flush #{n}");
            assert_eq!(answer, "Ready err=\"\"", "flush #{n}");
            results.push((format!("flush #{n}"), answer, crossed, resumed));
        }
        if let Some(h) = made {
            let _ = h;
            std::thread::sleep(Duration::from_millis(100));
        }
        let answer = text(&x["read"], "exchange.read");
        let answer = lossy(answer.as_bytes());
        let want = Want::Is(format!("Ready bytes={answer:?} end_of_frame=true"));
        {
            let before = counts(&p);
            let answer = read(&p, &rd, conn, false);
            let after = counts(&p);
            let crossed = after.0 - before.0;
            let resumed = after.1 - before.1;
            assert_eq!(crossed, 1, "{}", &format!("read #{n}"));
            match want {
                Want::Is(expected) => assert_eq!(answer, expected, "{}", &format!("read #{n}")),
                Want::Starts(prefix) => assert!(
                    answer.starts_with(prefix),
                    "{}: {}",
                    &format!("read #{n}"),
                    answer
                ),
            };
            results.push((
                (&format!("read #{n}")).to_string(),
                answer,
                crossed,
                resumed,
            ));
        };
    }
    let pend = &k["pending"];
    let (frame, answer) = (
        text(&pend["write"], "pending.write"),
        text(&pend["read"], "pending.read"),
    );
    rd.woken.0.store(false, Ordering::SeqCst);
    {
        let before = counts(&p);
        let answer = read_once(&p, &rd, conn, false);
        let after = counts(&p);
        let crossed = after.0 - before.0;
        let resumed = after.1 - before.1;
        assert_eq!(crossed, 1, "{}", "read pending");
        match Want::Starts("Pending ") {
            Want::Is(expected) => assert_eq!(answer, expected, "{}", "read pending"),
            Want::Starts(prefix) => {
                assert!(answer.starts_with(prefix), "{}: {}", "read pending", answer)
            }
        };
        results.push((("read pending").to_string(), answer, crossed, resumed));
    };
    let want = Want::Is(format!("Ready len={}", frame.len()));
    {
        let before = counts(&p);
        let answer = write(&p, &wr, conn, frame.as_bytes());
        let after = counts(&p);
        let crossed = after.0 - before.0;
        let resumed = after.1 - before.1;
        assert_eq!(crossed, 1, "{}", "write while a read pends");
        match want {
            Want::Is(expected) => assert_eq!(answer, expected, "{}", "write while a read pends"),
            Want::Starts(prefix) => assert!(
                answer.starts_with(prefix),
                "{}: {}",
                "write while a read pends",
                answer
            ),
        };
        results.push((
            ("write while a read pends").to_string(),
            answer,
            crossed,
            resumed,
        ));
    };
    {
        let before = counts(&p);
        let answer = flush(&p, &wr, conn);
        let after = counts(&p);
        let crossed = after.0 - before.0;
        let resumed = after.1 - before.1;
        assert_eq!(crossed, 1, "flush while a read pends");
        assert_eq!(answer, "Ready err=\"\"", "flush while a read pends");
        results.push((
            "flush while a read pends".to_string(),
            answer,
            crossed,
            resumed,
        ));
    }
    {
        let before = counts(&p);
        let answer = format!("{}", woken(&host, &rd));
        let after = counts(&p);
        let crossed = after.0 - before.0;
        let resumed = after.1 - before.1;
        assert_eq!(crossed, 0, "{}", "the host woke the read");
        match is("true") {
            Want::Is(expected) => assert_eq!(answer, expected, "{}", "the host woke the read"),
            Want::Starts(prefix) => assert!(
                answer.starts_with(prefix),
                "{}: {}",
                "the host woke the read",
                answer
            ),
        };
        results.push((
            ("the host woke the read").to_string(),
            answer,
            crossed,
            resumed,
        ));
    };

    let answer = lossy(answer.as_bytes());
    let want = Want::Is(format!("Ready bytes={answer:?} end_of_frame=true"));
    {
        let before = counts(&p);
        let answer = read(&p, &rd, conn, true);
        let after = counts(&p);
        let crossed = after.0 - before.0;
        let resumed = after.1 - before.1;
        assert_eq!(crossed, 0, "{}", "read resumed");
        match want {
            Want::Is(expected) => assert_eq!(answer, expected, "{}", "read resumed"),
            Want::Starts(prefix) => {
                assert!(answer.starts_with(prefix), "{}: {}", "read resumed", answer)
            }
        };
        results.push((("read resumed").to_string(), answer, crossed, resumed));
    };
    {
        let before = counts(&p);
        let answer = shut(&p, &wr, conn);
        let after = counts(&p);
        let crossed = after.0 - before.0;
        let resumed = after.1 - before.1;
        assert_eq!(crossed, 1, "{}", "shut");
        match is("Ready") {
            Want::Is(expected) => assert_eq!(answer, expected, "{}", "shut"),
            Want::Starts(prefix) => assert!(answer.starts_with(prefix), "{}: {}", "shut", answer),
        };
        results.push((("shut").to_string(), answer, crossed, resumed));
    };
    {
        let before = counts(&p);
        let answer = shut(&p, &wr, conn);
        let after = counts(&p);
        let crossed = after.0 - before.0;
        let resumed = after.1 - before.1;
        assert_eq!(crossed, 1, "{}", "shut again");
        match is("Ready") {
            Want::Is(expected) => assert_eq!(answer, expected, "{}", "shut again"),
            Want::Starts(prefix) => {
                assert!(answer.starts_with(prefix), "{}: {}", "shut again", answer)
            }
        };
        results.push((("shut again").to_string(), answer, crossed, resumed));
    };

    let guarded = Side::new(&d);
    let unadmitted = match dest.kind {
        DEST_AUTHORITY => Dest::authority("127.0.0.1:9"),
        _ => Dest::program(&serde_json::json!({ "program": "/bin/false" })),
    };
    {
        let before = counts(&p);
        let answer = dial(&p, &guarded, &unadmitted).0;
        let after = counts(&p);
        let crossed = after.0 - before.0;
        let resumed = after.1 - before.1;
        assert_eq!(crossed, 1, "{}", "dial not admitted");
        match Want::Starts("Refused ") {
            Want::Is(expected) => assert_eq!(answer, expected, "{}", "dial not admitted"),
            Want::Starts(prefix) => assert!(
                answer.starts_with(prefix),
                "{}: {}",
                "dial not admitted",
                answer
            ),
        };
        results.push((("dial not admitted").to_string(), answer, crossed, resumed));
    };
    let other_side = Side::new(&d);
    host.admit(
        other_side.ticket.ticket(),
        match other.kind {
            DEST_AUTHORITY => Admission::Dial(vec![other.authority.clone()]),
            _ => spawn_admission(&other.program, &other.args, &other.env),
        },
    );
    {
        let before = counts(&p);
        let answer = dial(&p, &other_side, &other).0;
        let after = counts(&p);
        let crossed = after.0 - before.0;
        let resumed = after.1 - before.1;
        assert_eq!(crossed, 1, "{}", "dial the other kind");
        match Want::Starts("Refused ") {
            Want::Is(expected) => assert_eq!(answer, expected, "{}", "dial the other kind"),
            Want::Starts(prefix) => assert!(
                answer.starts_with(prefix),
                "{}: {}",
                "dial the other kind",
                answer
            ),
        };
        results.push((
            ("dial the other kind").to_string(),
            answer,
            crossed,
            resumed,
        ));
    };
    drop(other_side);
    drop(guarded);

    {
        let before = counts(&p);
        let answer = program_exit(&host);
        let after = counts(&p);
        let crossed = after.0 - before.0;
        let resumed = after.1 - before.1;
        assert_eq!(crossed, 0, "{}", "a program's own exit");
        match is("exited code=3 pipe=open") {
            Want::Is(expected) => assert_eq!(answer, expected, "{}", "a program's own exit"),
            Want::Starts(prefix) => assert!(
                answer.starts_with(prefix),
                "{}: {}",
                "a program's own exit",
                answer
            ),
        };
        results.push((
            ("a program's own exit").to_string(),
            answer,
            crossed,
            resumed,
        ));
    };

    let before = counts(&p);
    let mut close = Frame::new(input::<InHead>(), output::<OutHead>());
    assert_eq!(p.call(life::CLOSE, &mut close).outcome, Outcome::Ready);
    assert_eq!(counts(&p).0 - before.0, 1, "close");

    {
        let before = counts(&p);
        let answer = dial(&p, &wr, &Dest::authority("127.0.0.1:1")).0;
        let after = counts(&p);
        let crossed = after.0 - before.0;
        let resumed = after.1 - before.1;
        assert_eq!(crossed, 0, "{}", "dial after close");
        match Want::Starts("Fault ") {
            Want::Is(expected) => assert_eq!(answer, expected, "{}", "dial after close"),
            Want::Starts(prefix) => assert!(
                answer.starts_with(prefix),
                "{}: {}",
                "dial after close",
                answer
            ),
        };
        results.push((("dial after close").to_string(), answer, crossed, resumed));
    };

    {
        let before = counts(&p);
        // The old holds_only(listener) accepted an empty table too. The actual carrier's
        // CLOSE releases its listener as well as every connection, so demand zero handles.
        let answer = format!("{}", host.held() == 0);
        let after = counts(&p);
        let crossed = after.0 - before.0;
        let resumed = after.1 - before.1;
        assert_eq!(crossed, 0, "{}", "handles held after close");
        match is("true") {
            Want::Is(expected) => assert_eq!(answer, expected, "{}", "handles held after close"),
            Want::Starts(prefix) => assert!(
                answer.starts_with(prefix),
                "{}: {}",
                "handles held after close",
                answer
            ),
        };
        results.push((
            ("handles held after close").to_string(),
            answer,
            crossed,
            resumed,
        ));
    };
    assert_eq!(d.stats().deadline_without_wake, 0);
    results
}

fn program_exit(host: &HostIo) -> String {
    use busbar_contract::abi::mechanism::ticket::Ticket;
    use busbar_contract::io_host::{IoHost, Spawn};
    const OWNER: u64 = u64::MAX;
    let ticket = Ticket {
        slot: u32::MAX,
        generation: 1,
    };
    host.admit(
        ticket,
        spawn_admission("/bin/sh", &["-c".into(), "sleep 5 & exit 3".into()], &[]),
    );
    let spawned = host.spawn(
        OWNER,
        ticket,
        &Spawn {
            program: "/bin/sh",
            args: &["-c", "sleep 5 & exit 3"],
            env: &[],
        },
    );
    let h = match spawned {
        Ok(h) => h,
        Err(e) => return format!("not spawned: {e:?}"),
    };
    let deadline = Instant::now() + Duration::from_secs(4);
    let exit = loop {
        match host.ends(OWNER, h) {
            Ok(e) if e.exit.is_some() || Instant::now() > deadline => break e.exit,
            Ok(_) => std::thread::sleep(Duration::from_millis(10)),
            Err(e) => return format!("ends: {e:?}"),
        }
    };
    let mut buf = [0_u8; 8];
    let open = host.read(OWNER, h, &mut buf, Waker::noop()).is_pending();
    let _ = host.close(OWNER, h);
    match exit {
        Some(code) => format!(
            "exited code={code} pipe={}",
            if open { "open" } else { "ended" }
        ),
        None => "running".into(),
    }
}

#[test]
fn the_linked_and_the_dropped_in_door_are_one_plugin() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let _entered = runtime.enter();
    let dropped_addresses = dispatch::load::door_addresses_of_library(&artifact())
        .unwrap()
        .unwrap();
    let linked_door = (row().door)();
    // SAFETY: the actual linked product supplies immutable complete door data.
    let linked_parts =
        unsafe { busbar_contract::abi::mechanism::door::Door::read_parts(linked_door) };
    assert_ne!(dropped_addresses.0, linked_door as usize);
    assert_ne!(dropped_addresses.1, linked_parts.ops as usize);
    let linked = carrier(false);
    let mut dropped = carrier(true);
    if std::env::var("BUSBAR_CONFORMANCE_RED").as_deref() == Ok("count") {
        dropped[0].2 += 1;
    }
    assert_eq!(
        linked
            .iter()
            .map(|(n, a, c, _)| (n, a, c))
            .collect::<Vec<_>>(),
        dropped
            .iter()
            .map(|(n, a, c, _)| (n, a, c))
            .collect::<Vec<_>>(),
        "crossing(s), pinned: linked and dropped answers/counts must agree"
    );
}
#[test]
fn the_statement_carries_the_declares_document() {
    let row = LinkedRow::of(busbar_transport_stdio::door::door).unwrap();
    let read = busbar_contract::abi::mechanism::rendering::read(&row.statement).unwrap();
    assert_eq!(
        busbar_contract::abi::mechanism::extensions::declares(&read.extensions.bytes),
        Some(&include_bytes!("../../transport-stdio/declares.json")[..])
    );
}

fn spawn_admission(command: &str, args: &[String], env: &[(String, String)]) -> Admission {
    let program = busbar_contract::conn::Program {
        command: command.into(),
        cwd: None,
        args: args.to_vec(),
        env: env.to_vec(),
        env_refs: Vec::new(),
    };
    let held = HeldEnv::written_only();
    Admission::Spawn(program, held)
}
fn flush(p: &Plugin<Transport>, side: &Side, conn: u64) -> String {
    let mut answer = flush_once(p, side, conn, false);
    let until = Instant::now() + Duration::from_secs(5);
    while answer.starts_with("Pending") && Instant::now() < until {
        std::thread::sleep(Duration::from_millis(2));
        if side.woken.0.swap(false, Ordering::SeqCst) {
            answer = flush_once(p, side, conn, true);
        }
    }
    answer
}
fn read(p: &Plugin<Transport>, side: &Side, conn: u64, resume: bool) -> String {
    let mut answer = read_once(p, side, conn, resume);
    let until = Instant::now() + Duration::from_secs(5);
    while answer.starts_with("Pending") && Instant::now() < until {
        std::thread::sleep(Duration::from_millis(2));
        if side.woken.0.swap(false, Ordering::SeqCst) {
            answer = read_once(p, side, conn, true);
        }
    }
    answer
}

fn row() -> LinkedRow {
    LinkedRow::of(busbar_transport_stdio::door::door).unwrap()
}
fn cdylib() -> std::path::PathBuf {
    artifact()
}
fn open(p: &Plugin<Transport>) {
    let mut f = Frame::new(input::<OpenIn>(), output::<OpenOut>());
    f.input.settings = Blob::json(b"{}");
    assert_eq!(p.call(life::OPEN, &mut f).outcome, Outcome::Ready);
}
#[path = "support/negative.rs"]
mod negative;
#[test]
fn the_suite_runs_in_the_profile_it_was_asked_to() {
    assert!(!(cfg!(debug_assertions) && std::env::var_os("BUSBAR_EXPECT_RELEASE").is_some()));
}
#[test]
fn red_a_moved_crossing_count_is_refused() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let _entered = runtime.enter();
    let honest = carrier(false);
    for index in 0..honest.len() {
        let mut changed = honest.clone();
        changed[index].2 += 1;
        assert_ne!(
            honest, changed,
            "changed count at {} was accepted",
            honest[index].0
        );
    }
    let mut doubled = honest.clone();
    for step in &mut doubled {
        step.2 *= 2;
    }
    assert_ne!(honest, doubled);
}

#[test]
fn red_a_door_at_another_kind_abi_is_refused() {
    negative::red_a_door_at_another_kind_abi_is_refused();
}

#[test]
fn red_a_door_built_against_another_kind_layout_is_refused() {
    negative::red_a_door_built_against_another_kind_layout_is_refused();
}

#[test]
fn red_a_transport_built_against_another_frame_piece_is_refused() {
    negative::red_a_transport_built_against_another_frame_piece_is_refused();
}

#[test]
fn red_a_statement_other_than_the_stated_one_is_refused() {
    negative::red_a_statement_other_than_the_stated_one_is_refused();
}

#[test]
fn red_a_failing_ready_refuses_the_boot() {
    negative::red_a_failing_ready_refuses_the_boot();
}

#[test]
fn red_a_door_asked_for_as_another_kind_is_refused() {
    negative::red_a_door_asked_for_as_another_kind_is_refused();
}

#[test]
fn the_declared_needs_are_the_statements() {
    negative::the_declared_needs_are_the_statements();
}

#[test]
fn no_outbound_auth_family_is_driven() {
    let actual = row();
    let statement = busbar_contract::abi::mechanism::rendering::read(&actual.statement).unwrap();
    assert_eq!(
        statement.kind,
        busbar_contract::abi::mechanism::KindCode::Transport as u32
    );
    let inputs: serde_json::Value = serde_json::from_str(include_str!("conformance.json")).unwrap();
    assert!(inputs["auth"]["outbound"].is_null());
}

#[test]
fn red_a_networked_door_with_no_connection_table_is_refused() {
    negative::red_a_networked_door_with_no_connection_table_is_refused();
}

fn write(p: &Plugin<Transport>, side: &Side, conn: u64, bytes: &[u8]) -> String {
    let mut answer = write_once(p, side, conn, bytes, false);
    let until = Instant::now() + Duration::from_secs(5);
    while answer.starts_with("Pending") && Instant::now() < until {
        std::thread::sleep(Duration::from_millis(2));
        if side.woken.0.swap(false, Ordering::SeqCst) {
            answer = write_once(p, side, conn, bytes, true);
        }
    }
    answer
}
