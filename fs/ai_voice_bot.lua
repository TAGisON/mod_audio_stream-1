--[[
  Coral AI voice bot — one orchestrator session per call.

  Dialplan (extension 101 or any inbound):
    <action application="lua" data="ai_voice_bot.lua"/>

  Optional channel vars (set before lua):
    ai_orch_url      default http://192.168.100.150:8011
    ai_profile_id    default coral-tfn
    ai_peer_rate     default 8000
]]

local ORCH_DEFAULT = "http://192.168.100.150:8011"
local PROFILE_DEFAULT = "coral-tfn"
local PEER_RATE_DEFAULT = "8000"

local function log(level, msg)
  freeswitch.consoleLog(level, "[ai_voice_bot] " .. msg .. "\n")
end

local function json_escape(s)
  if not s then return "" end
  return (tostring(s):gsub("\\", "\\\\"):gsub('"', '\\"'):gsub("\n", "\\n"))
end

local function curl_post(url, body)
  local cmd = string.format(
    "curl -sS -m 8 -X POST '%s' -H 'Content-Type: application/json' -d '%s' 2>&1",
    url:gsub("'", "'\\''"),
    body:gsub("'", "'\\''")
  )
  local handle = io.popen(cmd)
  if not handle then
    return nil, "io.popen failed"
  end
  local resp = handle:read("*a") or ""
  local ok, _, code = handle:close()
  if not ok then
    return nil, "curl exit " .. tostring(code) .. ": " .. resp
  end
  return resp, nil
end

if not session then
  log("ERR", "no session — run from dialplan only")
  return
end

local orch = session:getVariable("ai_orch_url") or ORCH_DEFAULT
orch = orch:gsub("/+$", "")
local profile = session:getVariable("ai_profile_id") or PROFILE_DEFAULT
local peer_rate = session:getVariable("ai_peer_rate") or PEER_RATE_DEFAULT
local uuid = session:get_uuid()
local ani = session:getVariable("caller_id_number") or ""

if not session:answered() then
  session:answer()
end

local create_body = string.format(
  '{"profile_id":"%s","profile_version":"latest","clock":"live","caller":"%s"}',
  json_escape(profile),
  json_escape(ani)
)

log("INFO", "creating session profile=" .. profile .. " ani=" .. ani)
local resp, err = curl_post(orch .. "/v1/sessions", create_body)
if err or not resp or resp == "" then
  log("ERR", "session create failed: " .. tostring(err or "empty"))
  session:hangup("NORMAL_TEMPORARY_FAILURE")
  return
end

local sid = resp:match('"session_id"%s*:%s*"([^"]+)"')
local tok = resp:match('"edge_token"%s*:%s*"([^"]+)"')
if not sid or not tok then
  log("ERR", "parse session create: " .. resp:sub(1, 400))
  session:hangup("NORMAL_TEMPORARY_FAILURE")
  return
end

session:setVariable("ai_session_id", sid)
log("INFO", "session " .. sid)

-- Must be set before uuid_audio_stream start (read at init).
session:setVariable("STREAM_INJECT_BUFFER_MS", "8000")

local orch_host = orch:match("^https?://([^/]+)")
if not orch_host then
  log("ERR", "invalid ai_orch_url: " .. orch)
  session:hangup("NORMAL_TEMPORARY_FAILURE")
  return
end

local ws_uri = string.format(
  "ws://%s/edge/fs?token=%s&rate=%s&call_uuid=%s",
  orch_host, tok, peer_rate, uuid
)

local api = freeswitch.API()
local stream_cmd = string.format("%s start %s mono %s", uuid, ws_uri, peer_rate)
local stream_res = api:execute("uuid_audio_stream", stream_cmd)
log("INFO", "uuid_audio_stream: " .. tostring(stream_res))
if not stream_res or stream_res:match("^%-ERR") then
  log("ERR", "audio stream start failed")
  curl_post(orch .. "/v1/sessions/" .. sid .. "/stop", '{"reason":"stream_failed"}')
  session:hangup("NORMAL_TEMPORARY_FAILURE")
  return
end

-- WRITE_REPLACE only fires while the channel has a write stream.
api:execute("uuid_broadcast", uuid .. " silence_stream://-1 aleg")

local ans_resp, ans_err = curl_post(orch .. "/v1/sessions/" .. sid .. "/answer", "{}")
if ans_err then
  log("WARNING", "answer failed: " .. tostring(ans_err))
else
  log("INFO", "answer ok")
end

-- Keep call up until caller hangs up; mod_audio_stream owns media.
session:setAutoHangup(false)
while session:ready() do
  session:sleep(500)
end

log("INFO", "call ended, stopping session " .. sid)
curl_post(orch .. "/v1/sessions/" .. sid .. "/stop", '{"reason":"hangup"}')
api:execute("uuid_audio_stream", uuid .. " stop")