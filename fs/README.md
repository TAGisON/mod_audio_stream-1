# FreeSWITCH dialplan — auto session per call

Each call to **101** (or any extension using `ai_voice_bot.lua`):

1. `POST /v1/sessions` on aiorchestrator → `session_id` + `edge_token`
2. `uuid_audio_stream` → WebSocket to `/edge/fs`
3. `POST /v1/sessions/{id}/answer` → bot greeting + live STT/TTS
4. On hangup → `POST /v1/sessions/{id}/stop`

## Install on sipserver (voip)

```bash
cd /usr/src/mod_Audio_stream-1
git pull origin master

cp fs/ai_voice_bot.lua /etc/coraltele/sipserver/scripts/ai_voice_bot.lua
```

Add the extension from `fs/dialplan_101.xml` to your dialplan, or include:

```xml
<X-PRE-PROCESS cmd="include" data="ai_101.xml"/>
```

If dialplan is served by **xml_curl**, add the same extension in your dialplan DB/API.

Reload dialplan:

```bash
fs_cli -x "reloadxml"
```

## Config (channel variables)

| Variable | Default | Meaning |
|---|---|---|
| `ai_orch_url` | `http://192.168.100.150:8011` | Control API base |
| `ai_profile_id` | `coral-cc` | Published profile |
| `ai_peer_rate` | `8000` | PCM rate for mod_audio_stream |

## Test

1. Orchestrator running on `192.168.100.150:8011`, `ready_for_sessions: true`
2. `mod_audio_stream` loaded
3. Call **101** from a phone
4. Supervisor: http://192.168.100.150:8011/supervisor/ — new session with transcript

## Troubleshooting

```bash
grep ai_voice_bot /etc/coraltele/sipserver/log/freeswitch.log | tail -20
fs_cli -x "show channels"
curl -s http://192.168.100.150:8011/v1/platform/status
```
