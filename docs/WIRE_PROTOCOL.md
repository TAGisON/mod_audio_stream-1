# Wire protocol — mod_audio_stream ↔ peer (aiorchestrator)

**Module version:** 2.0.0 (feeder/sink-only)  
**Aligned with:** aiorchestrator `docs/architecture/EDGE_FS.md`

One **duplex WebSocket per FreeSWITCH call**. Auth is via URL token (issued by orchestrator / Coral). This module does not implement crypto.

---

## Connection

```
uuid_audio_stream <uuid> start wss://orch-host/edge/fs?token=<signed> mono 8000
```

On TCP/TLS WebSocket open:

1. Module fires `mod_audio_stream::connect`
2. Module sends **hello** (text JSON)
3. Module begins binary PCM uplink
4. Peer may send binary PCM and/or `streamAudio` JSON and control messages

On disconnect / connect error: module tears down **this call only** (fail-closed).

---

## hello (module → peer)

Sent once after connect:

```json
{
  "type": "hello",
  "uuid": "<fs-call-uuid>",
  "sampleRate": 8000,
  "channels": 1,
  "encoding": "pcm_s16le",
  "frameMs": 20,
  "metadata": "<optional string from dialplan>"
}
```

Peer must pin peer sample rate / channels from this (or reject and close).

---

## Uplink media (module → peer)

- WebSocket **binary** frames
- PCM signed 16-bit little-endian
- Rate / channels as declared in `hello`
- Nominal 20 ms framing (optional burst via `STREAM_BUFFER_SIZE`)

---

## Downlink media (peer → module)

### Preferred: binary

Raw PCM16LE at the **hello sampleRate** (and matching channel layout).  
Queued into the per-call inject buffer → `WRITE_REPLACE` on the call.

### Compatibility: JSON `streamAudio`

```json
{
  "type": "streamAudio",
  "data": {
    "audioDataType": "raw",
    "sampleRate": 8000,
    "channels": 1,
    "audioData": "<base64 pcm_s16le>"
  }
}
```

- `audioDataType` must be `raw`
- `sampleRate` required; Speex resampling applied if ≠ session rate
- `file` field is **rejected** (removed)
- Oversized base64 rejected

---

## Control

| Direction | Message | Behaviour |
|---|---|---|
| peer → module | `{"type":"flush"}` | Clear inject buffer (barge-in) |
| peer → module | `{"type":"stop"}` | Close media bug / end stream |
| module → peer | optional text on `stop` API | Opaque; peer may ignore |
| either | unknown JSON `type` | Ignored (no disconnect) |

---

## Inject buffer

- Default depth: **500 ms** (`STREAM_INJECT_BUFFER_MS`)
- Overflow: **drop-oldest**, 20 ms aligned
- Underrun: full-frame **silence** (never partial PCM)
- Media thread: **trylock only** (never block)

---

## FreeSWITCH events

| Event | When |
|---|---|
| `mod_audio_stream::connect` | WS open |
| `mod_audio_stream::disconnect` | WS close |
| `mod_audio_stream::error` | WS error (bug closed) |
| `mod_audio_stream::play` | Inject chunk accepted (JSON path) |
| `mod_audio_stream::json` | Other JSON / flush ack |

---

## Orchestrator peer requirements

1. Accept `hello` and pin peer rate before treating session as Running.
2. Send `flush` before barge-in Speak so stale PCM is not played.
3. Prefer binary inject at peer rate; keep `streamAudio` until binary is live.
4. One WSS attachment per session; sticky LB to session owner.
