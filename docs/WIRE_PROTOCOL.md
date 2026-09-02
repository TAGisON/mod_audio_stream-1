# Wire protocol — mod_audio_stream ↔ peer (aiorchestrator)

**Module version:** 2.1.0 (feeder/sink + call control)  
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
| peer → module | `{"type":"flush"}` | **Dumb** clear of inject buffer only. Orchestrator owns barge/VAD; the module never decides product barge — it only drops queued TTS PCM when told. |
| peer → module | `{"type":"stop"}` | Close media bug / end stream |
| peer → module | `{"type":"hangup",…}` | Release the call after playout drains |
| peer → module | `{"type":"transfer",…}` | Blind-transfer the leg after playout drains |
| module → peer | optional text on `stop` API | Opaque; peer may ignore |
| either | unknown JSON `type` | Ignored (no disconnect) |

### hangup (2.1.0)

```json
{ "type": "hangup", "cause": "NORMAL_CLEARING", "drainMs": 15000 }
```

- `cause` — any FreeSWITCH cause string; invalid values fall back to `NORMAL_CLEARING`
- `drainMs` — how long the module waits for queued playout before releasing.
  Clamped to `0…15000`. Default 15000.

Used by the orchestrator to play an operator fallback prompt and then end the
call, so the caller never hears dead air on an engine outage.

### transfer (2.1.0)

```json
{ "type": "transfer", "dest": "1001", "dialplan": "XML",
  "context": "calltransfer", "drainMs": 15000 }
```

- `dest` — **required**; `[A-Za-z0-9_.+*#-]`, max 127 chars
- `dialplan` — default `XML`, identifier chars only
- `context` — default `calltransfer`, identifier chars only
- Equivalent to `uuid_transfer <call-uuid> <dest> <dialplan> <context>`, executed
  in-process on the session thread (no second session lookup).

### Semantics shared by both verbs

- **Deferred, not immediate.** The WebSocket thread only *arms* the action;
  the media thread runs it once the inject buffer has drained below one frame,
  or `drainMs` elapses. A closing prompt is therefore never truncated.
- **One action per connection.** A second `hangup`/`transfer` is rejected with
  `{"status":"error"}` — the first one wins, so a retry storm cannot retarget a
  transfer that is already in flight.
- **Never executed on the WebSocket thread.** `libwsc` services send and receive
  on a single libevent thread; touching channel state there would stall media in
  both directions.
- Before acting the module sets channel variables so the dialplan and CDR can
  see the intent even if the channel disappears:
  `ai_action` (`hangup`|`transfer`), `ai_hangup_cause`, `ai_transfer_dest`,
  `ai_transfer_dialplan`, `ai_transfer_context`.

Acks: `{"type":"hangup","status":"armed"}` / `{"type":"transfer","status":"armed"}`.

---

## Inject buffer

- Default depth: **500 ms** (`STREAM_INJECT_BUFFER_MS`)
- Overflow: **drop-oldest**, 20 ms aligned, counted and logged (throttled to 1/s)
- Underrun: full-frame **silence** (never partial PCM)
- Media thread: **trylock only** (never block)

### Peer pacing is part of the contract

The buffer is a jitter buffer, not a queue. **The peer must deliver downlink
audio at realtime pace.** A peer that stalls and then flushes a backlog will
overflow a 500 ms buffer and the caller will lose that audio permanently —
drop-oldest means a forward jump mid-utterance, not a delay.

If `inject overflow` appears in the log, fix the sender's pacing; do not simply
raise `STREAM_INJECT_BUFFER_MS`, which trades barge-in latency for a symptom.

### End-of-call counters

`destroy_tech_pvt` logs one line per call:

```
inject stats: written=<produced> read=<played> starved=<frames> lock_miss=<n> overflow_drops=<bytes>
```

- `written` — bytes accepted into the inject buffer (produced by the peer)
- `read` — bytes actually played out to the caller
- `starved` — 20 ms slots with no audio available (silence; normal when idle)
- `overflow_drops` — **bytes of agent speech discarded**; should be 0

`written − read ≈ overflow_drops` on a healthy call.

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
