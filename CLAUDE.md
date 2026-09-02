# CLAUDE.md

Guidance for Claude Code when working in this repository.

## What this is

A FreeSWITCH loadable module (`mod_audio_stream`) that acts as a **feeder/sink only**: it
captures live-call PCM from a FreeSWITCH channel, streams it out over a duplex WebSocket, and
injects whatever PCM comes back into the call. All intelligence (STT/LLM/TTS/policy) lives
outside this module, in `com.coraltele.aiorchestrator` (see that repo and
`../.workspace-ai-docs/`) — this module deliberately does not talk to vendor Realtime APIs
directly. This is a **customized fork** of the open-source `amigniter/mod_audio_stream`
project, heavily adapted for CoralTele's own "feeder/sink" wire protocol and FreeSWITCH
dialplan integration (see `mod_audio_stream-upstream/CLAUDE.md` for how the two repos relate).

## Stack

- C / C++17, FreeSWITCH module API (`mod_audio_stream.c`), CMake build.
- `libwsc` (git submodule at `libs/libwsc`, github.com/amigniter/libwsc) — the WebSocket client.
- SpeexDSP for resampling.
- Lua/Python/XML under `fs/` for FreeSWITCH dialplan integration (not part of the C module build).

## Important files

- `CMakeLists.txt` — build config; see it for `FREESWITCH_SRC_ROOT` / pkg-config options.
- `mod_audio_stream.c` / `mod_audio_stream.h` — FreeSWITCH module entry point and API commands
  (`uuid_audio_stream ... start|stop|pause|resume|send_text`).
- `audio_streamer_glue.cpp` / `.h` — the bulk of the streaming logic; substantially larger and
  more heavily modified here (1330 lines) than in `mod_audio_stream-upstream` (927 lines).
- `docs/WIRE_PROTOCOL.md` — the CoralTele-specific wire contract to aiorchestrator's
  `/edge/fs` endpoint (hello/media/control message shapes); module version
  **2.1.0 (feeder/sink + call control)**, aligned with aiorchestrator's
  `docs/architecture/EDGE_FS.md`. Keep `MOD_AUDIO_STREAM_VERSION` (in
  `audio_streamer_glue.cpp`), `CMakeLists.txt` `VERSION`, and this doc in step —
  the version line printed on each call is how deploys are verified.
- `docs/FEEDER_SINK_IMPLEMENTATION_PLAN.md` — design/implementation plan for this fork's
  feeder/sink model.
- `fs/ai_voice_bot.lua`, `fs/ai_profiles.conf`, `fs/dialplan_101.xml`, `fs/ai_dialplan_101.py`,
  `fs/README.md` — dialplan glue deployed to the sipserver: resolves an AI profile per DID,
  calls aiorchestrator's `/v1/sessions` REST API, then starts `uuid_audio_stream` to
  `/edge/fs`.
- `libs/libwsc` — git submodule; run `git submodule update --init --recursive` before building.

## Architecture

- FreeSWITCH loads the module; `uuid_audio_stream <uuid> start <ws-uri> mono 8000 [metadata]`
  attaches a media bug to the channel and opens one duplex WebSocket per call via `libwsc`.
- Call → peer: WebSocket binary frames, PCM16LE, mono only in v1 (`mixed`/`stereo` are rejected
  with `-ERR`, per README).
- Peer → call: binary PCM16LE preferred, or JSON `streamAudio` messages; text JSON control
  messages (`hello`, `flush`, `dtmf`, `stop`) run alongside.
- The FreeSWITCH-side integration (`fs/ai_voice_bot.lua`) resolves a per-DID AI profile, then
  drives a session lifecycle against aiorchestrator (`POST /v1/sessions` →
  `uuid_audio_stream ... start` → `POST /v1/sessions/{id}/answer` → on hangup
  `POST /v1/sessions/{id}/stop`), deployed to `/etc/coraltele/sipserver/scripts/` on the
  `sipserver` (voip) host.

## Use this project for / relation to other CoralTele repos

- Touch this repo for the FreeSWITCH-side transport: media bug capture/injection, the
  WebSocket wire protocol to aiorchestrator, buffering/jitter/heartbeat channel variables, or
  the `fs/` dialplan scripts that wire a DID to an AI profile.
- Do not add STT/LLM/TTS logic here — that belongs in `com.coraltele.aiorchestrator`.
- **`mod_audio_stream-upstream` is a separate sibling checkout of the original, unmodified
  open-source project** (different GitHub remote, no shared commit history) — use it as a
  read-only reference for diffing against upstream behavior, not as a branch/remote of this
  repo. See `mod_audio_stream-upstream/CLAUDE.md`.
- Unrelated to `coral-voicebridge`, which is a separate, non-FreeSWITCH SIP-to-voicebot bridge
  for a different vendor (NJ Group / Exotel protocol) — don't conflate the two AI-voice paths.

## Change guidance

- Origin remote is `github.com/TAGisON/mod_audio_stream-1` with `upstream` pointing at
  `github.com/Rahulcse79/mod_audio_stream-1` — **neither of these is `amigniter/mod_audio_stream`**,
  the original project. This repo's `upstream` remote is a fork-network ancestor, not the
  pristine open-source project; use the sibling `mod_audio_stream-upstream` checkout for that.
- Commit history here (`git log`) shows CoralTele-specific work on top of an "initial commit":
  dialplan Lua, DID→profile mapping, an `ai_voice_bot` welcome-timing fix, a build fix for
  atomic counters, and a "poll orchestrator media_phase ready before POST /answer" change —
  read recent commits before assuming a clean-room implementation.
- v1 only supports `mono` mix-type; `mixed`/`stereo` are rejected deliberately — don't silently
  "fix" that without checking whether it's still a stated constraint in `docs/WIRE_PROTOCOL.md`.
- Building requires FreeSWITCH headers/SpeexDSP and the `libs/libwsc` submodule initialized —
  a clone without `git submodule update --init --recursive` will fail to configure.

## Call control (2.1.0)

Peer → module `hangup` and `transfer` verbs are **deferred**: the WebSocket thread only
records the request, and the media bug READ path executes it once the inject buffer drains
(or `drainMs` expires). This ordering matters and must be preserved:

- `libwsc` runs send and receive on **one** libevent thread. Touching channel state there
  stalls media in both directions.
- The media bug READ callback runs on the session's own thread (inside `switch_ivr_sleep`
  for a Lua-only leg), which is the only safe place to call `switch_ivr_session_transfer` /
  `switch_channel_hangup`.
- Draining first is what keeps a closing prompt from being cut off mid-word.

See `stream_service_pending_action()` and `arm_pending_action()`.

## Known failure mode: inject overflow = lost speech

`inject_overflow_drops` in the end-of-call `inject stats:` line counts **agent audio the
caller never heard**. It is always a peer-pacing bug (orchestrator bursting after a stall),
never a reason to raise `STREAM_INJECT_BUFFER_MS` — that only trades barge-in latency for a
hidden symptom. `written − read ≈ overflow_drops` on a healthy call.

## Commands

```bash
git submodule update --init --recursive
mkdir -p build && cd build
export PKG_CONFIG_PATH=/usr/local/freeswitch/lib/pkgconfig:${PKG_CONFIG_PATH:-}
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j"$(nproc)"
```

**Build out of source.** There is no `Makefile` in the repo root: running `make` there fails
with "No rule to make target", the old `.so` stays installed, and `reload mod_audio_stream`
still answers `+OK` — a silent no-op deploy. Always confirm the version line on the next
call. There is also no `libfreeswitch-dev` Debian package; CMake locates FreeSWITCH through
`pkg-config`, hence `PKG_CONFIG_PATH`.

(`build-mod-audio-stream.sh` at the repo root wraps a similar scripted build/install flow —
read it before running, it is meant for the target Linux FreeSWITCH host, not this Windows
workspace.)
