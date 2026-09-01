--[[
  Coral AI voice bot — one orchestrator session per call.

  Dialplan:
    <action application="lua" data="ai_voice_bot.lua"/>

  Channel vars (optional, set before lua):
    ai_orch_url        default http://192.168.100.150:8011
    ai_profile_id      explicit profile; wins over DID map
    ai_profile_map     "101=coral-tfn,1800=coral-tfn"  or JSON {"101":"coral-tfn"}
    ai_profiles_file   path to dest→profile file (see ai_profiles.conf)
    ai_peer_rate       default 8000
]]

local ORCH_DEFAULT = "http://192.168.100.150:8011"
local PROFILE_DEFAULT = "coral-tfn"
local PEER_RATE_DEFAULT = "8000"
local DEFAULT_MAP_FILE = "/etc/coraltele/sipserver/scripts/ai_profiles.conf"

local function log(level, msg)
  freeswitch.consoleLog(level, "[ai_voice_bot] " .. msg .. "\n")
end

local function json_escape(s)
  if not s then return "" end
  return (tostring(s):gsub("\\", "\\\\"):gsub('"', '\\"'):gsub("\n", "\\n"):gsub("\r", ""))
end

local function jstr(s)
  return '"' .. json_escape(s or "") .. '"'
end

local function var(name)
  local v = session:getVariable(name)
  if v == nil or v == "" then return "" end
  return tostring(v)
end

-- timeout_sec: curl -m (create=8, answer=45 — welcome TTS may take several seconds)
local function curl_post(url, body, timeout_sec)
  local m = tonumber(timeout_sec) or 8
  local cmd = string.format(
    "curl -sS -m %d -X POST '%s' -H 'Content-Type: application/json' -d '%s' 2>&1",
    m,
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

local function add_pair(map, dest, pid)
  dest = dest and dest:gsub("^%s+", ""):gsub("%s+$", "") or ""
  pid = pid and pid:gsub("^%s+", ""):gsub("%s+$", "") or ""
  if dest ~= "" and pid ~= "" then
    map[dest] = pid
  end
end

local function parse_profile_map(s, map)
  if not s or s == "" then return map end
  s = s:gsub("^%s+", ""):gsub("%s+$", "")
  if s:sub(1, 1) == "{" then
    for dest, pid in s:gmatch('"([^"]+)"%s*:%s*"([^"]+)"') do
      add_pair(map, dest, pid)
    end
    return map
  end
  for pair in (s .. ","):gmatch("([^,]+),") do
    local dest, pid = pair:match("^%s*([^=]+)%s*=%s*(%S+)%s*$")
    add_pair(map, dest, pid)
  end
  return map
end

local function load_profile_file(path, map)
  if not path or path == "" then return map end
  local f = io.open(path, "r")
  if not f then return map end
  for line in f:lines() do
    line = line:gsub("#.*$", ""):gsub("^%s+", ""):gsub("%s+$", "")
    if line ~= "" then
      local dest, pid = line:match("^([^=%s]+)%s*=%s*(%S+)$")
      if not dest then
        dest, pid = line:match("^(%S+)%s+(%S+)$")
      end
      add_pair(map, dest, pid)
    end
  end
  f:close()
  return map
end

local function resolve_profile(dest)
  local explicit = var("ai_profile_id")
  if explicit ~= "" then
    return explicit, "channel:ai_profile_id"
  end
  local map = {}
  load_profile_file(var("ai_profiles_file"), map)
  load_profile_file(DEFAULT_MAP_FILE, map)
  parse_profile_map(var("ai_profile_map"), map)
  if dest ~= "" and map[dest] then
    return map[dest], "did_map:" .. dest
  end
  return PROFILE_DEFAULT, "default"
end

if not session then
  log("ERR", "no session — run from dialplan only")
  return
end

local orch = var("ai_orch_url")
if orch == "" then orch = ORCH_DEFAULT end
orch = orch:gsub("/+$", "")

local peer_rate = var("ai_peer_rate")
if peer_rate == "" then peer_rate = PEER_RATE_DEFAULT end

local uuid = session:get_uuid() or ""
local ani = var("caller_id_number")
local dest = var("destination_number")
if dest == "" then dest = var("sip_to_user") end
local profile, profile_src = resolve_profile(dest)

local sip_call_id = var("sip_call_id")
if sip_call_id == "" then sip_call_id = var("sip_h_X-coral_sbc_callid") end

local caller_json = string.format(
  '{"ani":%s,"caller_id_name":%s,"network_addr":%s,"sip_from_user":%s,"sip_from_host":%s}',
  jstr(ani),
  jstr(var("caller_id_name")),
  jstr(var("network_addr") ~= "" and var("network_addr") or var("sip_network_ip")),
  jstr(var("sip_from_user")),
  jstr(var("sip_from_host"))
)

local meta_json = string.format(
  '{"edge":"mod_audio_stream","call_uuid":%s,"sip_call_id":%s,"destination":%s,"context":%s,"direction":%s,"accountcode":%s,"chan_name":%s,"profile_source":%s,"peer_rate":%s}',
  jstr(uuid),
  jstr(sip_call_id),
  jstr(dest),
  jstr(var("context")),
  jstr(var("call_direction") ~= "" and var("call_direction") or var("coral_call_direction")),
  jstr(var("accountcode") ~= "" and var("accountcode") or var("coral_accountcode")),
  jstr(var("chan_name")),
  jstr(profile_src),
  jstr(peer_rate)
)

if not session:answered() then
  session:answer()
end

local create_body = string.format(
  '{"profile_id":%s,"profile_version":"latest","clock":"live","caller":%s,"metadata":%s}',
  jstr(profile),
  caller_json,
  meta_json
)

log("INFO", "creating session profile=" .. profile .. " via=" .. profile_src ..
  " ani=" .. ani .. " dest=" .. dest .. " uuid=" .. uuid)
local resp, err = curl_post(orch .. "/v1/sessions", create_body, 8)
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
session:setVariable("ai_profile_id", profile)
log("INFO", "session " .. sid)

-- Keep inject buffer small so barge-in flush clears residual playout quickly (module: 40..5000).
session:setVariable("STREAM_INJECT_BUFFER_MS", "500")

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
if not stream_res or stream_res:match("^%-ERR") or stream_res:match("INVALID COMMAND") then
  log("ERR", "audio stream start failed: " .. tostring(stream_res))
  curl_post(orch .. "/v1/sessions/" .. sid .. "/stop", '{"reason":"stream_failed"}', 8)
  session:hangup("NORMAL_TEMPORARY_FAILURE")
  return
end

-- MUST be non-blocking. Synchronous uuid_broadcast of silence_stream://-1 holds Lua until
-- hangup, so /answer never runs during the call (welcome never plays).
api:execute("bgapi", "uuid_broadcast " .. uuid .. " silence_stream://-1 aleg")

-- Brief wait so edge WSS can attach; poll orch until media ready (LIVE_TALK WP0).
local function curl_get(url, timeout_sec)
  local m = tonumber(timeout_sec) or 5
  local cmd = string.format(
    "curl -sS -m %d -X GET '%s' 2>&1",
    m,
    url:gsub("'", "'\\''")
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

local function wait_media_ready(sid, timeout_ms)
  local deadline = (os.time() * 1000) + (tonumber(timeout_ms) or 5000)
  while (os.time() * 1000) < deadline do
    local body, err = curl_get(orch .. "/v1/sessions/" .. sid, 3)
    if body and body:match('"media_phase"%s*:%s*"ready"') then
      return true
    end
    if body and body:match('"media_phase"%s*:%s*"welcoming"') then
      return true
    end
    if body and body:match('"media_phase"%s*:%s*"conversing"') then
      return true
    end
    session:sleep(100)
  end
  log("WARNING", "media ready poll timed out; proceeding with settle fallback")
  session:sleep(500)
  return false
end

wait_media_ready(sid, 5000)

local ans_body, ans_err = curl_post(orch .. "/v1/sessions/" .. sid .. "/answer", "{}", 30)
if ans_err then
  log("WARNING", "answer failed: " .. tostring(ans_err))
elseif ans_body and ans_body:match('"error"') then
  log("WARNING", "answer rejected: " .. tostring(ans_body):sub(1, 300))
else
  log("INFO", "answer ok")
end

session:setAutoHangup(false)
while session:ready() do
  session:sleep(500)
end

log("INFO", "call ended, stopping session " .. sid)
curl_post(orch .. "/v1/sessions/" .. sid .. "/stop", '{"reason":"hangup"}', 8)
api:execute("uuid_audio_stream", uuid .. " stop")
