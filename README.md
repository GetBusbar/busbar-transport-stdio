<!-- fleet:header:begin (rendered by `busbar-release plugin heal` from GetBusbar/busbar-release template/ and busbar's plugins.yaml; edit it there) -->
# busbar-transport-stdio

First-party signed kind:transport plugin cdylib: the stdio transport, packaged as a droppable busbar plugin. Drop the signed tarball into plugins/.

| kind | alias | crate | busbar | license |
|---|---|---|---|---|
| `transport` | `stdio` | `busbar-transport-stdio-plugin` | 1.6.0 (pinned in `.busbar-ref`) | Apache-2.0 |

[![ci](https://github.com/GetBusbar/busbar-transport-stdio/actions/workflows/ci.yml/badge.svg?branch=dev)](https://github.com/GetBusbar/busbar-transport-stdio/actions/workflows/ci.yml)
<!-- fleet:header:end -->

## What it is for

`busbar-transport-stdio` carries a configured program's standard input and output. The host
starts the absolute program path and owns its pipes; this plugin frames each output line and
writes each outbound frame as one line. It invokes no shell.

## Config

The carrier reads `limits.request_body_max_bytes` as its line and frame ceiling, defaulting to
32 MiB. Inbound lines lose their LF and at most one CR immediately before it. A blank line is an
empty frame, and a final unterminated line is a frame when the program exits. Interior CR bytes
are preserved. A line over the ceiling fails with `read: a line ran past the stdio line ceiling`.

Outbound frames may arrive in pieces. A frame over the ceiling fails with
`write: a frame ran past the stdio line ceiling`; a frame containing LF or ending in CR fails
before that frame writes any bytes. A valid completed frame gains one LF.

Supervision is per host-issued member key. Program exits, failed spawns and failed pipe I/O
start backoffs of 100, 200, 400 and 800 ms. Five ends inside 60 seconds hold the member down until
it is declared anew. A sibling member has its own state, and rotating a secret under an unchanged
member does not reset the trip. A supervised dial requires the host's monotonic clock and fails
without spawning when that clock is unavailable. A clean close of a running program does not
count as an end; a key of zero names an unsupervised program.

## Build

```bash
cargo build --release -p busbar-transport-stdio-plugin
```

## Tests

```bash
cargo test --workspace --locked
```

## License

Apache-2.0. See [LICENSE](LICENSE).
