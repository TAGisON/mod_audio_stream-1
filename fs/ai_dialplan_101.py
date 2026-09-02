#!/usr/bin/env python3
"""xml_curl front door: dest 101 → AI bot Lua; anything else → 404 so Coral handles it.

Do not set ai_profile_id here. Lua + /etc/coraltele/sipserver/scripts/ai_profiles.conf
choose the desk (101 → coral-xfer).
"""
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs

XML = """<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<document type="freeswitch/xml">
  <section name="dialplan" description="coral-ai-101">
    <context name="{ctx}">
      <extension name="coral_ai_voice_101">
        <condition field="destination_number" expression="^101$">
          <action application="set" data="hangup_after_bridge=false"/>
          <action application="set" data="ai_orch_url=http://192.168.100.150:8011"/>
          <action application="set" data="ai_peer_rate=8000"/>
          <action application="lua" data="ai_voice_bot.lua"/>
        </condition>
      </extension>
    </context>
  </section>
</document>
"""

class H(BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(n).decode("utf-8", "replace")
        q = parse_qs(body)
        dest = (q.get("Hunt-Destination-Number") or q.get("Caller-Destination-Number")
                or q.get("variable_destination_number") or [""])[0]
        ctx = (q.get("Hunt-Context") or q.get("Caller-Context") or ["default"])[0] or "default"
        if str(dest).split("@")[0] == "101":
            data = XML.format(ctx=ctx).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/xml")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *a):
        return

if __name__ == "__main__":
    HTTPServer(("127.0.0.1", 8099), H).serve_forever()
