# logpipe

A small **log-forwarding service** for Ubuntu / Linux that exposes a writable
**named pipe (FIFO)** at `/dev/logpipe` and streams every line written to it
into a configured [OpenSearch](https://opensearch.org/) index via the `_bulk`
API.

The Rust implementation lives in [`app/`](./app).

## Usage from the customer's side

Once installed, any local process can ship a log line by simply writing
to the FIFO. Each newline marks one record.

### Plain text

```sh
echo "hello world" > /dev/logpipe

# Stream a whole file (one line == one document)
cat my.log > /dev/logpipe
tail -F /var/log/myapp.log > /dev/logpipe
```

A plain line `hello world` becomes:

```json
{ "@timestamp": "…", "source": "fifo", "host": "myhost",
  "tag": "customer", "fifo": "/dev/logpipe", "message": "hello world" }
```

### Structured fields (JSON document)

Write a **single-line JSON object** and its fields are merged into the
document. Watch the shell quoting — wrap the whole payload in **single
quotes** so the inner `"` aren't eaten by the shell:

```sh
# Right:
echo '{"level":"error","trace_id":"abc123","message":"oom","tag":"checkout"}' > /dev/logpipe

# Wrong: the shell strips the inner quotes, logpipe sees a non-JSON line
# and stores it verbatim in "message".
echo "{"level":"error",...}" > /dev/logpipe

# Already have pretty-printed JSON? Compact it to one line first:
jq -c . event.json > /dev/logpipe
```

The first command above indexes:

```json
{ "@timestamp": "…", "source": "fifo", "host": "myhost", "fifo": "/dev/logpipe",
  "level": "error", "trace_id": "abc123", "message": "oom", "tag": "checkout" }
```

Rules for JSON lines:

- All keys are merged onto the document as top-level fields.
- A string `"message"` / `"tag"` is *promoted* — and `"tag"` overrides the
  default tag from the config.
- `@timestamp`, `source`, `host`, and `fifo` are owned by the daemon; copies
  of these keys in your JSON are ignored. (This is what keeps the indexed
  document free of duplicate keys, which OpenSearch would otherwise reject.)
- A line that doesn't parse as a JSON **object** (invalid JSON, a bare array,
  a number, …) is treated as plain text and stored verbatim in `message`.

### From a program

```python
import json
with open("/dev/logpipe", "w") as f:                 # plain line
    f.write("user logged in\n")
    f.write(json.dumps({"level": "info", "event": "login",   # JSON line
                        "user_id": 42, "message": "user logged in",
                        "tag": "auth"}) + "\n")
```

```c
int fd = open("/dev/logpipe", O_WRONLY);
dprintf(fd, "msg\n");                                          /* plain line */
dprintf(fd, "{\"level\":\"error\",\"message\":\"oom\"}\n");    /* JSON line  */
close(fd);
```

## What it does

- Creates and owns a FIFO (named pipe) at `/dev/logpipe` (configurable).
- Opens the FIFO with `O_RDWR` so it never sees EOF even when no customer
  is currently writing — a single long-lived reader serves every writer.
- Reads one line at a time, batches events, and POSTs them to
  OpenSearch `/_bulk` with HTTP Basic auth over TLS (rustls).
- Retries transient OpenSearch errors with exponential backoff.
- Runs as a sandboxed systemd unit under a dedicated unprivileged user.

## Repository layout

```text
.
├── README.md
└── app/                       # Rust crate
    ├── Cargo.toml
    ├── config.example.toml    # Annotated example config
    ├── systemd/
    │   └── logpipe.service    # Hardened systemd unit
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
cd app
cargo build --release
```

The binary will be at `app/target/release/logpipe`.

## Installing

```bash
# 1. Install the binary
sudo install -m 0755 app/target/release/logpipe /usr/local/bin/logpipe

# 2. Create the service user
sudo useradd --system --no-create-home --shell /usr/sbin/nologin logpipe

# 3. Install config and systemd unit
sudo install -d -o logpipe -g logpipe -m 0750 /etc/logpipe
sudo install -m 0640 -o logpipe -g logpipe \
    app/config.example.toml /etc/logpipe/config.toml
sudo $EDITOR /etc/logpipe/config.toml

sudo install -m 0644 app/systemd/logpipe.service \
    /etc/systemd/system/logpipe.service

# 4. Start it
sudo systemctl daemon-reload
sudo systemctl enable --now logpipe
sudo systemctl status logpipe
journalctl -u logpipe -f

# 5. Smoke test
echo "first message from $(whoami)" > /dev/logpipe
```

The systemd unit creates the FIFO at start-up (as root, then chowns to the
`logpipe` user) and removes it at stop.

## Event shape

Every document indexed into OpenSearch looks like:

```json
{
  "@timestamp": "2026-05-11T12:34:56.789Z",
  "source":     "fifo",
  "host":       "myhost",
  "tag":        "customer",
  "message":    "hello world",
  "fifo":       "/dev/logpipe"
}
```

Customer-supplied JSON-object payloads are merged on top of those base fields,
so e.g. `{"level":"error","message":"oom","trace_id":"abc"}` produces a
document with `level`, `message`, and `trace_id` all present.

Create an OpenSearch index template ahead of time if you want strict
mappings — the service does not create the index for you.

## Security notes

- The default permission mask is `0o622` (rw--w--w-): everyone can write,
  only the daemon reads. Tighten with a dedicated UNIX group if you don't
  want every local user to be able to inject events.
- `PrivateDevices=true` cannot be used while the FIFO lives under `/dev`,
  because that would hide the FIFO from the rest of the system. Move the
  FIFO under `/run/logpipe/` (set `sources.fifo.path` in the config) and
  re-enable `PrivateDevices=true` in the unit file if you want tighter
  sandboxing.
- Anyone who can write to the FIFO can choose their own `tag` via a JSON
  payload. The daemon does not strip or sanitise customer-supplied fields.
  Use OpenSearch index permissions / pipelines if you need server-side
  validation.

## License

[MIT](./LICENSE)
