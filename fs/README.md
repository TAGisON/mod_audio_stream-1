# FreeSWITCH dialplan — auto session per call

Each call using `ai_voice_bot.lua`:

1. Resolve **profile** from DID map (or `ai_profile_id`)
2. `POST /v1/sessions` with **caller** + **metadata** (ANI, dest, FS UUID, SIP Call-ID)
   — on failure: play a local fallback prompt and release (see below)
3. `uuid_audio_stream` → WebSocket to `/edge/fs`
4. Fixed **800 ms** media settle (`session:sleep`) — do **not** poll `media_phase` from Lua (curl keep-alive can stall ~60s on voip)
5. `POST /v1/sessions/{id}/answer` via `bgapi` — must run **while the call is still up**
6. Wait, watching `ai_action` for a peer-requested hangup/transfer
7. Hangup → `POST /v1/sessions/{id}/stop` with the real reason

**Do not** `uuid_broadcast silence_stream://-1` — even via `bgapi` it starts `playback` on the channel and freezes `session:sleep` in this Lua script until hangup.

On **unbridged Lua-only** legs (no bridge, no playback), TTS playout needs **`STREAM_INJECT_READ=1`** (set in `ai_voice_bot.lua`). That drains the inject buffer on the media bug **READ** path and `write_frame`s PCM to the caller. Without it, `WRITE_REPLACE` may never run and the inject queue stays full (`inuse=8000`) with silence heard.

`/answer` is dispatched via **`bgapi`** so welcome TTS is not blocked by a synchronous curl (~30s stalls were observed on voip).

## Install on sipserver (voip)

```bash
cd /usr/src/mod_Audio_stream-1
git pull origin master

cp fs/ai_voice_bot.lua /etc/coraltele/sipserver/scripts/ai_voice_bot.lua
cp fs/ai_profiles.conf /etc/coraltele/sipserver/scripts/ai_profiles.conf
cp fs/ai_dialplan_101.py /etc/coraltele/sipserver/scripts/ai_dialplan_101.py
chmod +x /etc/coraltele/sipserver/scripts/ai_dialplan_101.py

# Rebuild the module when C/C++ changes.
# This project is CMake, built OUT OF SOURCE — there is no Makefile in the repo
# root, so `make` there fails with "No rule to make target" and silently leaves
# the OLD .so installed. `reload` then reports +OK while still running the old
# binary. Always build in build/ and check the version line on the next call.
git submodule update --init --recursive
mkdir -p build && cd build
export PKG_CONFIG_PATH=/usr/local/freeswitch/lib/pkgconfig:${PKG_CONFIG_PATH:-}
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j"$(nproc)"
ls -la mod_audio_stream.so          # must exist before installing

cp mod_audio_stream.so /usr/lib/freeswitch/mod/mod_audio_stream.so
fs_cli -x "reload mod_audio_stream"
# confirm on the next call: "mod_audio_stream 2.1.0 feeder/sink init"
```

Build dependencies (Debian/Ubuntu): `cmake g++ libssl-dev zlib1g-dev libspeexdsp-dev`
plus FreeSWITCH development headers. There is **no** `libfreeswitch-dev` package on
Debian — CMake finds FreeSWITCH via `pkg-config`, which is why `PKG_CONFIG_PATH`
must include the FreeSWITCH install prefix when it lives under `/usr/local`.

### xml_curl dialplan service (port 8099) — required after reboot

FreeSWITCH asks `http://127.0.0.1:8099/dialplan` **before** static XML. If this service is down,
calls to **101** fall through to Coral outbound routing → `denyacl` / invalid number (Lua never runs).

**One-time systemd unit:**

```bash
cat > /etc/systemd/system/coral-ai-dialplan.service << 'EOF'
[Unit]
Description=Coral AI xml_curl dialplan interceptor (101)
After=network.target

[Service]
Type=simple
ExecStart=/usr/bin/python3 /etc/coraltele/sipserver/scripts/ai_dialplan_101.py
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now coral-ai-dialplan
```

**Verify after reboot:**

```bash
curl -s -o /dev/null -w "8099 HTTP %{http_code}\n" -X POST http://127.0.0.1:8099/dialplan
# expect 404 (no dest in body) — proves service is up

fs_cli -x "module_exists mod_audio_stream"
fs_cli -x "module_exists mod_lua"
# if mod_audio_stream missing: fs_cli -x "load mod_audio_stream"

curl -s http://192.168.25.130:8011/v1/platform/status
# orchestrator must be reachable from voip
```

Call **101** again — log must show `EXECUTE ... lua(ai_voice_bot.lua)`, not `transfer(101 XML denyacl)`.

Edit `ai_profiles.conf` for each inbound number:

```
101=coral-xfer
1800123456=coral-xfer
```

Default profile when the DID is unmapped: `coral-xfer` (simple transfer desk). Legacy
`coral-tfn` remains installable on the orchestrator for guided TFN trees.

Reload is not required for Lua/file edits (next call picks them up). After changing `xml_curl.conf.xml`, `reload mod_xml_curl`.

## Config

| Source | Meaning |
|---|---|
| `ai_profile_id` | Explicit profile (wins) |
| `ai_profile_map` | `101=coral-xfer,102=other` or JSON `{"101":"coral-xfer"}` |
| `ai_profiles_file` | Alternate path to dest→profile file |
| `/etc/coraltele/sipserver/scripts/ai_profiles.conf` | Default DID map |
| default | `coral-xfer` (Coral Telecom simple transfer desk) |
| `ai_orch_url` | default `http://192.168.25.130:8011` (override per deploy) |
| `ai_peer_rate` | default `8000` |

Posted on session create (stored + `session.started` audit):

- **caller:** ANI, name, network addr, SIP from
- **metadata:** `call_uuid` (FS), `sip_call_id`, destination, context, direction, accountcode, profile_source

## Test

Call **101**. Supervisor: http://192.168.25.130:8011/supervisor/ — profile `coral-xfer`, caller/metadata on the session JSON.

```bash
grep ai_voice_bot /etc/coraltele/sipserver/log/freeswitch.log | tail -20
```

Expected on a good call (stay on line 20+ s):

```
[ai_voice_bot] uuid_audio_stream: +OK
mod_audio_stream 2.1.0 feeder/sink init
STREAM_INJECT_READ enabled (READ-path playout)
[ai_voice_bot] media settle wait 800ms
[ai_voice_bot] answer dispatched (bgapi)
```

Debug: inject `inuse=` should **fluctuate** (not stuck at 8000). On hangup the
`inject stats:` line is the single best health check for a call:

```
inject stats: written=846720 read=409280 starved=2581 lock_miss=0 overflow_drops=29440
                                                                  ^^^^^^^^^^^^^^^^^^^^
```

`overflow_drops` is **agent speech the caller never heard**, in bytes
(÷16000 = seconds at 8 kHz). It must be **0**. Anything else means the
orchestrator delivered audio faster than realtime — see "voice drops" below.

**Bad signs:**

| FS log | Meaning |
|---|---|
| `playback(silence_stream://-1)` at depth=1 | Old script — blocks Lua until hangup |
| `media ready (poll N/10)` | Old poll loop — redeploy `ai_voice_bot.lua` |
| `inject accepted (v2.0.x)`, or version line < 2.1.0 | Old `.so` — the build did not install; rebuild in `build/` |
| `inuse=8000` pegged whole call | Inject not draining — need `STREAM_INJECT_READ` + a current module |
| `answer ok` at hangup after ~30s | Old sync curl — redeploy Lua with bgapi answer |
| `inject overflow: dropped N bytes` | **Voice drops.** Orchestrator is bursting, not pacing |
| `overflow_drops` > 0 at hangup | Same, seen after the fact |

### Voice drops (choppy agent audio)

Overflow is always a **sender pacing** problem, never a buffer-size problem.
The inject buffer is 500 ms deep and drops the *oldest* audio when full, so a
burst makes the caller hear a forward jump mid-sentence.

Check the orchestrator's own log for the same window: a stalled `live listen
uplink` line followed by a catch-up line (frame count jumping far more than
wall-clock allows) is the signature. Raising `STREAM_INJECT_BUFFER_MS` hides it
at the cost of barge-in latency — fix the sender instead.

## Call control (module 2.1.0)

The orchestrator can end or transfer the call over the same WebSocket. Both are
deferred until queued playout drains, so closing prompts are never cut off:

- `{"type":"hangup","cause":"…"}` → module releases the leg
- `{"type":"transfer","dest":"1001","dialplan":"XML","context":"calltransfer"}`
  → in-process equivalent of `uuid_transfer <uuid> 1001 XML calltransfer`

Both set channel variables before acting (`ai_action`, `ai_hangup_cause`,
`ai_transfer_dest`, `ai_transfer_dialplan`, `ai_transfer_context`) so the CDR
records what happened. `ai_voice_bot.lua` watches `ai_action` to report an
accurate stop reason (`transferred` / `peer_hangup`).

Expected log on a transfer:

```
(uuid) transfer armed dest=1001 dialplan=XML context=calltransfer drain_ms=15000
(uuid) executing transfer (uuid_transfer <uuid> 1001 XML calltransfer)
```

## Local fallback prompts (orchestrator unreachable)

If the orchestrator cannot be reached at all, the Lua script plays a local
prompt and releases the call instead of dropping it silently:

```
/usr/local/share/ai-orchestrator/fallback/ai_unavailable.wav
/usr/local/share/ai-orchestrator/fallback/credits_exhausted.wav
/usr/local/share/ai-orchestrator/fallback/internal_error.wav
/usr/local/share/ai-orchestrator/fallback/generic.wav      # covers the rest
```

Override the directory with the `ai_fallback_dir` channel variable. A missing
prompt degrades to a bare hangup with the right cause — never an error.

Failures that happen *after* the session is up (engine down, credits exhausted,
timeouts) are handled by the orchestrator, which streams its own uploaded prompt
over the media path and then sends `hangup`. Those prompts are managed with
`PUT /v1/tenant/fallback/{scenario}` and are not files on this host.

After redeploy, confirm deployed file has **no** `uuid_broadcast` / `wait_media_ready`:

```bash
grep -E 'uuid_broadcast|wait_media_ready|media settle wait' /etc/coraltele/sipserver/scripts/ai_voice_bot.lua
# must show: media settle wait
# must NOT show: uuid_broadcast or wait_media_ready
```
