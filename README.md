# mod_audio_stream

FreeSWITCH module: **feeder/sink only**. Captures live-call PCM, sends it over a duplex WebSocket, receives PCM on the same socket, and injects it into the call.

Intelligence (STT / LLM / TTS / policy) lives in **aiorchestrator**, not here.

## Capability

| Direction | Transport | Format |
|---|---|---|
| Call → peer | WebSocket binary | PCM16LE |
| Peer → call | WebSocket binary (preferred) or JSON `streamAudio` | PCM16LE |
| Control | WebSocket text JSON | `hello`, `flush`, `dtmf`, `stop` |

**One duplex WebSocket per call.** No shared multiplex bus. No AI in this process.

Wire contract: [`docs/WIRE_PROTOCOL.md`](docs/WIRE_PROTOCOL.md)  
Implementation plan: [`docs/FEEDER_SINK_IMPLEMENTATION_PLAN.md`](docs/FEEDER_SINK_IMPLEMENTATION_PLAN.md)

## API

```
uuid_audio_stream <uuid> start <ws-uri> [mono|mixed|stereo] [8000|16000] [metadata]
uuid_audio_stream <uuid> stop [text]
uuid_audio_stream <uuid> pause
uuid_audio_stream <uuid> resume
uuid_audio_stream <uuid> send_text <text>
```

Example (orchestrator edge):

```
uuid_audio_stream ${uuid} start wss://orch.example/edge/fs?token=... mono 8000
```

v1: **`mono` only**. `mixed` / `stereo` are rejected with `-ERR`.

## Channel variables

| Variable | Meaning |
|---|---|
| `STREAM_BUFFER_SIZE` | Uplink burst size in ms (multiple of 20; default 20) |
| `STREAM_INJECT_BUFFER_MS` | Inject queue depth in ms (default 500) |
| `STREAM_HEART_BEAT` | WebSocket ping interval (seconds) |
| `STREAM_TLS_CA_FILE` / `STREAM_TLS_KEY_FILE` / `STREAM_TLS_CERT_FILE` | Client TLS |
| `STREAM_TLS_DISABLE_HOSTNAME_VALIDATION` | Dev only |
| `STREAM_EXTRA_HEADERS` | JSON object of extra WS request headers |
| `STREAM_SUPPRESS_LOG` | Reduce payload logging |
| `STREAM_MESSAGE_DEFLATE` | Enable WS compression (usually off for PCM) |

## Build

Requires FreeSWITCH headers, SpeexDSP, and submodule `libs/libwsc`:

```bash
git submodule update --init --recursive
mkdir build && cd build
cmake ..
make
```

See `CMakeLists.txt` for `FREESWITCH_SRC_ROOT` / pkg-config options.

## Architecture note

```
FreeSWITCH ──mod_audio_stream──WSS──► aiorchestrator (Go)
                                         └─ Listen / Think / Speak gateways
```

Do not point the WebSocket URL at vendor Realtime APIs from this module.
