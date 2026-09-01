# Implementation plan — feeder/sink-only `mod_audio_stream`

**Branch:** `feature/feeder-sink-only`  
**Status:** EXECUTED (Phases A–E: module feeder/sink + orch hello/flush/binary)  
**Date:** 31 August 2026  
**Peer contract:** `com.coraltele.aiorchestrator` → `docs/architecture/EDGE_FS.md`  
**Wire doc:** `docs/WIRE_PROTOCOL.md`

---

## 0. Goal

Turn this FreeSWITCH module into a **pure media feeder/sink**:

1. Capture live-call PCM and send it on **one duplex WebSocket per call**.
2. Receive PCM (and minimal control) on **that same socket** and inject into **that** call only.
3. **Zero AI** in this process (no OpenAI, no VAD, no LLM/TTS, no Python bridge).

Intelligence lives only in **aiorchestrator**. This module is the telephony POS: bytes in, bytes out.

### Non-goals

- Shared multiplex WebSocket across calls
- Dual media sockets (separate send URL + subscribe URL) for v1
- Silent auto-reconnect mid-call
- Changing FreeSWITCH SIP/RTP or Coral ACD
- Changing orchestrator routers/gateways (only FS dialect coordination)

---

## 1. Locked architecture

```
Per live call (FreeSWITCH UUID):
  uuid_audio_stream <uuid> start wss://orch/.../edge/fs?token=... [rate]

  ┌──────────── FS media thread ────────────┐
  │ READ:  PCM → Speex? → binary WS uplink  │
  │ WRITE: inject_buffer → WRITE_REPLACE    │
  └──────────────────┬──────────────────────┘
                     │ 1× duplex WSS
                     ▼
              aiorchestrator edge
```

| Decision | Choice | Why |
|---|---|---|
| Concurrency | **1 duplex WS per call** | Socket identity = demux; no cross-call HOL |
| Dual send/inject URLs | **Rejected for v1** | 2N sockets, 2× TLS/auth, bind races |
| Shared N-call WS | **Rejected** | Blast radius, demux bugs, backpressure coupling |
| Media format uplink | Binary PCM16LE | Lowest overhead |
| Media format downlink | Binary preferred; `streamAudio` JSON kept for orch compat | CPU at scale |
| Control | Small JSON on same socket | `hello`, `flush`, `dtmf`, `stop`, errors |
| Disconnect | Fail-closed (tear down this call’s bug) | Matches orch Terminal |
| Auth | Token in URL (issued by orch/Coral); module does not invent crypto | EDGE_FS lock |

---

## 2. Wire protocol (module ↔ orchestrator)

### 2.1 On WebSocket open (module → orch)

Send **exactly one** text hello before or with first media (prefer immediately on CONNECT):

```json
{
  "type": "hello",
  "uuid": "<fs-call-uuid>",
  "sampleRate": 8000,
  "channels": 1,
  "encoding": "pcm_s16le",
  "frameMs": 20,
  "metadata": "<optional dialplan metadata string or object>"
}
```

Orchestrator must pin **peer rate / channels** from this (or from token + hello consistency check).

### 2.2 Uplink media (module → orch)

- WebSocket **binary** frames only for PCM.
- PCM16LE, interleaved if stereo (v1 target: **mono**).
- Nominal cadence: **20 ms** (configurable burst via existing `STREAM_BUFFER_SIZE`).
- No per-frame session id — connection binding is the key.

### 2.3 Downlink media (orch → module)

**Preferred (perf):** WebSocket **binary** PCM16LE at the peer rate declared in `hello` (or documented override).

**Compatibility (required until orch ships binary):** JSON text:

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

Rules:

- If binary arrives: treat as peer-rate PCM (or rate from last negotiated hello); **no base64**.
- If JSON `streamAudio`: decode, optional Speex resample to session rate, queue.
- Reject / ignore unknown `type` without killing the socket (log at DEBUG).
- **Remove** `file` payload support (arbitrary local file read).

### 2.4 Control (both directions, JSON text)

| Direction | Type | Meaning |
|---|---|---|
| orch → FS | `flush` | Clear this session’s inject buffer (barge-in) |
| orch → FS | `stop` (optional) | Request orderly stream stop |
| FS → orch | `dtmf` | `{"type":"dtmf","digit":"1"}` when DTMF detected |
| FS → orch | `stop` | Stream stopping / cleanup notice (optional body) |
| either | error-shaped events | Prefer close + FS custom events; avoid chatty errors |

`flush` is **mandatory** for correct barge-in with orch.

### 2.5 FreeSWITCH custom events (unchanged roles)

Keep firing (stable for dialplan/apps):

- `mod_audio_stream::connect`
- `mod_audio_stream::disconnect`
- `mod_audio_stream::error`
- `mod_audio_stream::json` (non-audio JSON)
- `mod_audio_stream::play` (optional; only if useful after inject accept — prefer reducing log noise)

Reserve `EVENT_PLAY` on load if still used (today it is defined but not reserved — fix).

---

## 3. Public API / config surface

### 3.1 API (keep, clarify)

```
uuid_audio_stream <uuid> start <wss-uri> [mono|mixed|stereo] [8000|16000] [metadata]
uuid_audio_stream <uuid> stop [text]
uuid_audio_stream <uuid> pause
uuid_audio_stream <uuid> resume
uuid_audio_stream <uuid> send_text <json-or-text>
```

v1 behaviour:

| Arg | Behaviour |
|---|---|
| `wss-uri` | Required; `ws://` or `wss://` only |
| `mono` | Default; implement correctly |
| `mixed` / `stereo` | Either implement with correct `SMBF_*` flags **or** reject with clear `-ERR` (do not silently ignore) |
| rate | Peer streaming rate; drive Speex uplink + hello |
| metadata | Copied into hello / optional first text |

### 3.2 Channel variables (keep / document)

| Variable | Role |
|---|---|
| `STREAM_BUFFER_SIZE` | Uplink burst ms (multiple of 20) |
| `STREAM_HEART_BEAT` | WS ping interval |
| `STREAM_TLS_*` | Client TLS files / hostname validation |
| `STREAM_EXTRA_HEADERS` | JSON object → WS request headers |
| `STREAM_SUPPRESS_LOG` | Reduce payload logging |
| `STREAM_INJECT_BUFFER_MS` | **New** — inject queue depth (default 500) |
| `STREAM_MEDIA_FORMAT` | **New** — `json` \| `binary` \| `auto` (accept both; prefer binary when set) |

No OpenAI / model / voice variables. Delete any docs that imply AI in-module.

### 3.3 Single URL

v1: **one** URI from `start`. Uplink and downlink share that connection.  
Do not add dual-URL config until a real split-topology requirement exists.

---

## 4. Internal design (industrial principles)

### 4.1 Layering

| Layer | Responsibility | Files (target) |
|---|---|---|
| FS API / media bug | Dialplan API, bug lifecycle, WRITE_REPLACE consume | `mod_audio_stream.c/h` |
| Session state | Buffers, resamplers, flags, atomics | `private_t` in header |
| Transport | One `AudioStreamer` / WS client per session | `audio_streamer_glue.cpp` + `libwsc` |
| Protocol | hello / flush / streamAudio / binary / dtmf parse | Small focused helpers (split from mega-`processMessage` if needed) |
| Audio DSP | Speex resample, mono/stereo convert, frame align | Pure functions, no FS I/O |

Principles:

- **No AI, no business policy** in any layer.
- **Media thread never blocks** (trylock; silence / skip on miss).
- **WS callback never blocks long** (trylock inject; drop on contention).
- **One owner for inject_buffer** under `inject_mutex`.
- **Fail closed** on auth/connect failure for that session only.
- **RAII / shared_ptr** for streamer lifetime; `markCleanedUp` before disconnect (keep proven pattern).
- **Logging:** rate-limited INFO stats; never log full base64 audio in production paths.

### 4.2 Threading model (do not break)

| Thread | May do | Must not do |
|---|---|---|
| FS media | read bug, trylock, write WS binary (or queue-to-send), consume inject | `mutex_lock` that can wait; alloc storms; JSON parse |
| WS client | parse control/media, trylock inject write, fire FS events | Block on FS; hold inject_mutex across resample+large decode without bounds |
| Control / API | start/stop/pause/send_text | Touch inject without locks |

Uplink optimization: prefer **copy-out under short lock, send after unlock** (already partially done via `pending_send`). Keep that pattern; never send while holding `inject_mutex`.

### 4.3 Performance rules (hard)

1. **Hot path = binary PCM** both ways when orch supports it.
2. Speex only when `call_rate != peer_rate` (uplink) or `inject_rate != session_rate` (JSON path).
3. Cap inject buffer (`STREAM_INJECT_BUFFER_MS`); **drop-oldest**, frame-aligned (20 ms).
4. Cap max JSON `audioData` size (keep `MAX_AUDIO_BASE64_LEN` or lower for realtime frames).
5. No per-frame `std::string` growth unbounded; reuse scratch where safe.
6. Periodic stats (1–5 s), not per-frame INFO logs.
7. Avoid `switch_core_session_locate` storms: locate once per callback batch where possible (binary inject already locates once).
8. Prefers **peer rate == session inject rate** so binary path skips Speex entirely (orch responsibility to match hello).

### 4.4 Memory / lifecycle

- All `private_t` from session pool.
- Destroy resamplers and mutexes exactly once in cleanup.
- Double-cleanup guard (`cleanup_started`) retained.
- Channel hangup vs API `stop` both covered (existing `channelIsClosing` path).
- After `markCleanedUp`, ignore late WS callbacks (retain).

---

## 5. Error & edge-case matrix

### 5.1 Connection / auth

| Scenario | Module behaviour |
|---|---|
| Invalid URI scheme | `start` → `-ERR`; no bug |
| DNS / TCP / TLS failure | `CONNECT_ERROR` event; close bug; no half-open capture |
| HTTP 401/403 from orch | Treat as connect error; tear down |
| Token expired mid-call | Orch closes WS → disconnect path → cleanup |
| Orch refuses hello (rate mismatch) | Orch closes; module cleanup; log ERROR |

### 5.2 Concurrent calls (N sessions)

| Scenario | Behaviour |
|---|---|
| N simultaneous starts | N independent streamers; no global audio lock |
| One call’s WS dies | Only that UUID cleaned; others untouched |
| Inject on WS-A | Can only touch tech_pvt for A (locate by streamer’s bound uuid) |
| Lock contention on one call | That call underruns/drops; never blocks other calls |

### 5.3 Media uplink

| Scenario | Behaviour |
|---|---|
| `pause` | Stop sending; keep WS up; flush bug read path as today |
| `resume` | Resume send |
| WS not yet connected | Drop uplink frames (or short coalesce); do not block media |
| Resampler error at init | Fail `start` |
| Odd byte length frame | Truncate to even |
| Extreme CPU | Prefer drop uplink coalescing over media-thread stall |

### 5.4 Media downlink / inject

| Scenario | Behaviour |
|---|---|
| Underrun | Write silence for full frame (never partial PCM) |
| Overflow | Drop-oldest, 20 ms aligned |
| `inject_mutex` busy | Drop this chunk (WS path) or silence (media path) |
| Wrong sampleRate in JSON | Resample via Speex; if init fails, drop + ERROR log |
| Binary at wrong rate | If no rate in band: assume hello rate; misconfig = audible pitch error — document; orch must match |
| Huge JSON blob | Reject over max; do not OOM |
| `file` field | **Removed** — reject |
| `flush` | Clear inject_buffer under mutex; fire optional event |
| Barge-in without flush | Stale AI audio continues — **orch must send flush** |
| Stereo/mono mismatch | Explicit upmix/downmix (keep); v1 prefer mono end-to-end |

### 5.5 Lifecycle races

| Scenario | Behaviour |
|---|---|
| `stop` while WS callback active | cleanup_started + markCleanedUp; callbacks no-op |
| Hangup during inject | locate fails → drop; cleanup from CLOSE |
| Double `stop` | Idempotent success |
| `start` twice on same channel | Reject second (`MY_BUG_NAME` private already set) |
| `send_text` after cleanup | `-ERR` |

### 5.6 Protocol / peer bugs

| Scenario | Behaviour |
|---|---|
| Non-JSON text | Ignore / `EVENT_JSON` if desired; do not crash |
| Unknown JSON type | Ignore |
| Mixed binary+JSON flood | Process independently; cap work per callback |
| Compression / deflate | Keep channel-var controlled; default off for PCM |

### 5.7 Operational

| Scenario | Behaviour |
|---|---|
| Clock skew / bursty orch | Buffer + drop-oldest absorbs; silence fills gaps |
| LB sends WS to wrong orch instance | Session fail at orch; module sees disconnect — ops/sticky issue, document |
| libwsc submodule missing | Build fails clearly in CI docs |
| Heartbeat timeout | Disconnect → cleanup |

---

## 6. Implementation phases

### Phase A — Strip AI & freeze scope

1. Delete entire `Ai_code/` tree (Python OpenAI bridge, tests, `.env.example`, pem used only for OpenAI).
2. Remove AI references from README / any docs; add feeder-sink README section.
3. Ensure `.gitignore` no longer implies AI runtime; never commit secrets.
4. Confirm submodule `libs/libwsc` present and buildable.
5. **Exit:** tree has no OpenAI/vendor AI code; module still builds (or documented build deps).

### Phase B — Protocol hardening (same duplex socket)

1. Emit `hello` on CONNECT_SUCCESS (replace ad-hoc initialMetadata-only behaviour; metadata folds into hello).
2. Implement `flush` JSON handler (clear inject_buffer).
3. Remove `file` inject path.
4. Fix `EVENT_PLAY` reserve if kept; reduce per-frame INFO noise.
5. Fix mono|mixed|stereo: implement or hard-reject.
6. Optional: DTMF → JSON uplink (feature-flag or always-on if cheap).
7. Document wire contract in `docs/WIRE_PROTOCOL.md` (mirror EDGE_FS; note binary preference).
8. **Exit:** can talk to a mock WS that expects hello + binary + flush; no AI.

### Phase C — Performance pass

1. Binary inject as primary path; JSON `streamAudio` secondary.
2. Add `STREAM_INJECT_BUFFER_MS`; validate drop-oldest alignment.
3. Ensure uplink send-after-unlock; no inject_mutex on uplink.
4. Rate-limit stats; atomic counters retained.
5. Avoid Speex when rates equal (both directions).
6. **Exit:** load smoke with concurrent fake calls (tooling or scripted WS peers); no media-thread blocking under contention.

### Phase D — Lifecycle & edge-case hardening

1. Audit cleanup vs hangup vs connect-error (table in §5).
2. Idempotent stop; late-callback safety.
3. Cap JSON size; reject malformed base64 without throw across FS boundary (catch already present — keep).
4. TLS failure paths tested manually once.
5. **Exit:** written test checklist signed off; no known crash races in review.

### Phase E — Orchestrator alignment (coord, may be separate PR)

1. Confirm orch accepts `hello` and pins peer rate.
2. Orch sends `flush` on barge-in before new Speak.
3. Plan orch binary inject (compat JSON until then).
4. Dialplan examples: point `uuid_audio_stream` at orch WSS + token.
5. **Exit:** lab call: FS ↔ orch edge, silence/tone inject, no `Ai_code`.

---

## 7. Coding standards for this change

- C11 / C++11 as today; match existing FreeSWITCH module style.
- Prefer **small pure helpers** over growing `processMessage` further; consider splitting protocol parse vs buffer write.
- No new third-party deps beyond SpeexDSP + libwsc.
- No heap in media callback beyond what FS already requires; pre-sized buffers.
- All new public behaviour documented in README + WIRE_PROTOCOL.
- Every error path: log with **uuid**, no secrets (tokens redacted if logged).
- Do not “fix” by shipping another Python AI bridge — use a **dumb echo/tone WS** test peer if needed.

---

## 8. Test plan (module-side)

| Test | How | Pass |
|---|---|---|
| Build | CMake + FreeSWITCH headers | shared object links |
| Hello | Mock WS records first text | schema + rate match start args |
| Uplink | Mock receives binary 20 ms | size = `rate * 2 * 0.02` (± burst) |
| JSON inject | Send `streamAudio` | audible/replace frames; buffer grows then consumes |
| Binary inject | Send raw PCM | queued without JSON CPU |
| Flush | Fill buffer → `flush` → underrun silence | buffer empty |
| N calls | 10+ parallel mock sessions | no cross-talk; kill one, others live |
| Bad URI / TLS | start failures | `-ERR` + error event |
| Double start | second start | `-ERR` |
| Stop / hangup races | fuzz stop during inject | no crash |
| Overflow | flood inject | drop-oldest; bounded memory |
| Pause/resume | | uplink stops/starts |
| No `file` | send file field | rejected |
| No Ai_code | tree grep | zero OpenAI / bridge |

Lab with live orch is Phase E, not a substitute for mock protocol tests.

---

## 9. Rollout

1. Land module changes on `feature/feeder-sink-only`.
2. Deploy module to lab FS; keep orch JSON inject initially.
3. Point dialplan WSS from old `Ai_code` host → orch edge.
4. Enable binary inject only after orch supports it.
5. Remove any production dependency on `Ai_code`.

Rollback: repoint dialplan URL; module remains backward-compatible with `streamAudio` JSON.

---

## 10. Success criteria

- [ ] No AI code in repository runtime path  
- [ ] One duplex WSS per call; send + inject  
- [ ] Hello + flush + binary/JSON media as specified  
- [ ] Media thread non-blocking under load  
- [ ] N-call isolation proven  
- [ ] EDGE_FS-compatible; lab path FS → aiorchestrator without Python bridge  
- [ ] Documented errors/edge cases from §5 handled  

---

## 11. Implementation order when coding starts

1. Phase A (delete AI)  
2. Phase B (protocol)  
3. Phase D critical lifecycle fixes interleaved with B  
4. Phase C (perf)  
5. Phase E with orch team  

Do not start orch-wide refactors from this repo; only document required peer changes (`hello`, `flush`, optional binary).
