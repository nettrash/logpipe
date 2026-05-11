# os-driver

A small **userspace driver** (systemd service) for Ubuntu / Linux that
exposes a writable FIFO at `/dev/os-driver` and streams every line written
to it into a configured [OpenSearch](https://opensearch.org/) index via the
`_bulk` API.

The Rust implementation lives in [`drv/`](./drv).

## Usage from the customer's side

Once installed, any local process can ship a log line by simply writing
to the FIFO:

```sh
# Plain text
echo "hello world" > /dev/os-driver

# Multiple lines from a file
cat my.log > /dev/os-driver

# Structured payload (JSON object on a single line)
printf '{"level":"error","message":"oom","tag":"checkout"}\n' > /dev/os-driver

# From C / any language
int fd = open("/dev/os-driver", O_WRONLY);
write(fd, "msg\n", 4);
close(fd);
```

- Plain text becomes `{"message": "<line>"}`.
- JSON-object lines are merged into the document; `"message"` and `"tag"` fields
  are promoted to the document's top-level fields.

Each newline marks a record boundary.

## What it does

- Creates and owns a FIFO (named pipe) at `/dev/os-driver` (configurable).
- Opens the FIFO with `O_RDWR` so it never sees EOF even when no customer
  is currently writing — a single long-lived reader serves every writer.
- Reads one line at a time, batches events, and POSTs them to
  OpenSearch `/_bulk` with HTTP Basic auth over TLS (rustls).
- Retries transient OpenSearch errors with exponential backoff.
- Runs as a sandboxed systemd unit under a dedicated unprivileged user.

## Repository layout

```
.
├── README.md
└── drv/                       # Rust crate
    ├── Cargo.toml
    ├── config.example.toml    # Annotated example config
    ├── systemd/
    │   └── os-driver.service  # Hardened systemd unit
    └── src/
        ├── main.rs            # Entry point, signal handling, task wiring
        ├── config.rs          # TOML config schema + validation
        ├── event.rs           # Common event document
        ├── opensearch.rs      # _bulk client with retries
        ├── batcher.rs         # Size / time based batcher
        └── sources/
            ├── mod.rs
            └── fifo.rs        # FIFO ingress (mkfifo + tail)
```

## Building

Requires a recent stable Rust toolchain (1.74+ recommended) on Ubuntu 22.04
or 24.04.

```bash
cd drv
cargo build --release
```

The binary will be at `drv/target/release/os-driver`.

## Installing

```bash
# 1. Install the binary
sudo install -m 0755 drv/target/release/os-driver /usr/local/bin/os-driver

# 2. Create the service user
sudo useradd --system --no-create-home --shell /usr/sbin/nologin os-driver

# 3. Install config and systemd unit
sudo install -d -o os-driver -g os-driver -m 0750 /etc/os-driver
sudo install -m 0640 -o os-driver -g os-driver \
    drv/config.example.toml /etc/os-driver/config.toml
sudo $EDITOR /etc/os-driver/config.toml

sudo install -m 0644 drv/systemd/os-driver.service \
    /etc/systemd/system/os-driver.service

# 4. Start it
sudo systemctl daemon-reload
sudo systemctl enable --now os-driver
sudo systemctl status os-driver
journalctl -u os-driver -f

# 5. Smoke test
echo "first message from $(whoami)" > /dev/os-driver
```

The systemd unit creates the FIFO at start-up (as root, then chowns to the
`os-driver` user) and removes it at stop.

## Event shape

Every document indexed into OpenSearch looks like:

```json
{
  "@timestamp": "2026-05-11T12:34:56.789Z",
  "source":     "fifo",
  "host":       "myhost",
  "tag":        "customer",
  "message":    "hello world",
  "fifo":       "/dev/os-driver"
}
```

Customer-supplied JSON-object payloads are merged on top of those base fields,
so e.g. `{"level":"error","message":"oom","trace_id":"abc"}` produces a
document with `level`, `message`, and `trace_id` all present.

Create an OpenSearch index template ahead of time if you want strict
mappings — the driver does not create the index for you.

## Security notes

- The default permission mask is `0o622` (rw--w--w-): everyone can write,
  only the daemon reads. Tighten with a dedicated UNIX group if you don't
  want every local user to be able to inject events.
- `PrivateDevices=true` cannot be used while the FIFO lives under `/dev`,
  because that would hide the FIFO from the rest of the system. Move the
  FIFO under `/run/os-driver/` (set `sources.fifo.path` in the config) and
  re-enable `PrivateDevices=true` in the unit file if you want tighter
  sandboxing.
- Anyone who can write to the FIFO can choose their own `tag` via a JSON
  payload. The daemon does not strip or sanitise customer-supplied fields.
  Use OpenSearch index permissions / pipelines if you need server-side
  validation.

## License

MIT OR Apache-2.0
