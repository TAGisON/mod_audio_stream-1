# FreeSWITCH dialplan — auto session per call

Each call using `ai_voice_bot.lua`:

1. Resolve **profile** from DID map (or `ai_profile_id`)
2. `POST /v1/sessions` with **caller** + **metadata** (ANI, dest, FS UUID, SIP Call-ID)
3. `uuid_audio_stream` → WebSocket to `/edge/fs`
4. `POST /v1/sessions/{id}/answer`
5. Hangup → `POST /v1/sessions/{id}/stop`

## Install on sipserver (voip)

```bash
cd /usr/src/mod_Audio_stream-1
git pull origin master

cp fs/ai_voice_bot.lua /etc/coraltele/sipserver/scripts/ai_voice_bot.lua
cp fs/ai_profiles.conf /etc/coraltele/sipserver/scripts/ai_profiles.conf
```

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
| `ai_orch_url` | default `http://192.168.100.150:8011` |
| `ai_peer_rate` | default `8000` |

Posted on session create (stored + `session.started` audit):

- **caller:** ANI, name, network addr, SIP from
- **metadata:** `call_uuid` (FS), `sip_call_id`, destination, context, direction, accountcode, profile_source

## Test

Call **101**. Supervisor: http://192.168.100.150:8011/supervisor/ — profile `coral-tfn`, caller/metadata on the session JSON.

```bash
grep ai_voice_bot /etc/coraltele/sipserver/log/freeswitch.log | tail -20
```
