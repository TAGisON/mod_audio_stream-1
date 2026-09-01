# FreeSWITCH dialplan — auto session per call

Each call using `ai_voice_bot.lua`:

1. Resolve **profile** from DID map (or `ai_profile_id`)
2. `POST /v1/sessions` with **caller** + **metadata** (ANI, dest, FS UUID, SIP Call-ID)
3. `uuid_audio_stream` → WebSocket to `/edge/fs`
4. Fixed **1500 ms** media settle (`session:sleep`) — do **not** poll `media_phase` from Lua (curl keep-alive can stall ~60s on voip)
5. `POST /v1/sessions/{id}/answer` — must run **while the call is still up**
6. Hangup → `POST /v1/sessions/{id}/stop`

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

# Rebuild module when C++ changes (STREAM_INJECT_READ / READ-path playout):
make clean && make
cp mod_audio_stream.so /usr/lib/freeswitch/mod/mod_audio_stream.so
fs_cli -x "reload mod_audio_stream"
# confirm v2.0.1 in log on next call: "mod_audio_stream 2.0.1 feeder/sink init"
```

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
101=coral-tfn
1800123456=coral-tfn
```

If dialplan is **xml_curl**, keep the 101 interceptor but **do not hardcode** `ai_profile_id` unless you want to override the file. Lua reads destination and the map.

Reload is not required for Lua/file edits (next call picks them up). After changing `xml_curl.conf.xml`, `reload mod_xml_curl`.

## Config

| Source | Meaning |
|---|---|
| `ai_profile_id` | Explicit profile (wins) |
| `ai_profile_map` | `101=coral-tfn,102=other` or JSON `{"101":"coral-tfn"}` |
| `ai_profiles_file` | Alternate path to dest→profile file |
| `/etc/coraltele/sipserver/scripts/ai_profiles.conf` | Default DID map |
| default | `coral-tfn` (Coral Telecom Toll-Free Desk) |
| `ai_orch_url` | default `http://192.168.25.130:8011` (override per deploy) |
| `ai_peer_rate` | default `8000` |

Posted on session create (stored + `session.started` audit):

- **caller:** ANI, name, network addr, SIP from
- **metadata:** `call_uuid` (FS), `sip_call_id`, destination, context, direction, accountcode, profile_source

## Test

Call **101**. Supervisor: http://192.168.25.130:8011/supervisor/ — profile `coral-tfn`, caller/metadata on the session JSON.

```bash
grep ai_voice_bot /etc/coraltele/sipserver/log/freeswitch.log | tail -20
```

Expected on a good call (stay on line 20+ s):

```
[ai_voice_bot] uuid_audio_stream: +OK
mod_audio_stream 2.0.1 feeder/sink init
STREAM_INJECT_READ enabled (READ-path playout)
[ai_voice_bot] media settle wait 800ms
[ai_voice_bot] answer dispatched (bgapi)
```

Debug: inject `inuse=` should **fluctuate** (not stuck at 8000). On hangup, `inject stats: written=...` should show bytes written > 0.

**Bad signs:**

| FS log | Meaning |
|---|---|
| `playback(silence_stream://-1)` at depth=1 | Old script — blocks Lua until hangup |
| `media ready (poll N/10)` | Old poll loop — redeploy `ai_voice_bot.lua` |
| `inject accepted (v2.0.0)` only, no `STREAM_INJECT_READ` | Old `.so` — rebuild and reload module |
| `inuse=8000` pegged whole call | Inject not draining — need `STREAM_INJECT_READ` + v2.0.1 module |
| `answer ok` at hangup after ~30s | Old sync curl — redeploy Lua with bgapi answer |

After redeploy, confirm deployed file has **no** `uuid_broadcast` / `wait_media_ready`:

```bash
grep -E 'uuid_broadcast|wait_media_ready|media settle wait' /etc/coraltele/sipserver/scripts/ai_voice_bot.lua
# must show: media settle wait
# must NOT show: uuid_broadcast or wait_media_ready
```
