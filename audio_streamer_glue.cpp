#include <string>
#include <cstring>
#include "mod_audio_stream.h"
#include "WebSocketClient.h"
#include <switch_json.h>
#include <switch_buffer.h>
#include <atomic>
#include <vector>
#include <memory>
#include <mutex>
#include "base64.h"
#include <algorithm>
#include <climits>
#include <cstdint>
#include <cctype>
#include <cstdlib>

#define MOD_AUDIO_STREAM_VERSION "2.1.0"

#define FRAME_SIZE_8000  320
#define MAX_AUDIO_BASE64_LEN (256 * 1024) /* realtime frames only; reject huge blobs */

/* Shared by the WebSocket thread (arming actions) and the media thread (running
 * them), so it lives at file scope rather than inside AudioStreamer.
 */
static inline void stream_copy_field(char* dst, size_t cap, const char* src) {
    if (!dst || cap == 0) return;
    if (!src) { dst[0] = '\0'; return; }
    std::strncpy(dst, src, cap - 1);
    dst[cap - 1] = '\0';
}

class AudioStreamer {
public:
    static std::shared_ptr<AudioStreamer> create(
        const char* uuid, const char* wsUri, responseHandler_t callback, int deflate, int heart_beat,
        bool suppressLog, const char* extra_headers, const char* tls_cafile, const char* tls_keyfile,
        const char* tls_certfile, bool tls_disable_hostname_validation) {

        std::shared_ptr<AudioStreamer> sp(new AudioStreamer(
            uuid, wsUri, callback, deflate, heart_beat,
            suppressLog, extra_headers, tls_cafile, tls_keyfile,
            tls_certfile, tls_disable_hostname_validation
        ));

        sp->bindCallbacks(std::weak_ptr<AudioStreamer>(sp));
        sp->client.connect();
        return sp;
    }

    ~AudioStreamer() = default;

    void disconnect() {
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "disconnecting...\n");
        client.disconnect();
    }

    bool isConnected() {
        return client.isConnected();
    }

    void writeBinary(uint8_t* buffer, size_t len) {
        if (!this->isConnected()) return;
        client.sendBinary(buffer, len);
    }

    void writeText(const char* text) {
        if (!this->isConnected()) return;
        client.sendMessage(text, strlen(text));
    }

    void markCleanedUp() {
        m_cleanedUp.store(true, std::memory_order_release);
        client.setMessageCallback({});
        client.setBinaryCallback({});
        client.setOpenCallback({});
        client.setErrorCallback({});
        client.setCloseCallback({});
    }

    bool isCleanedUp() const {
        return m_cleanedUp.load(std::memory_order_acquire);
    }

private:
    AudioStreamer(
        const char* uuid, const char* wsUri, responseHandler_t callback, int deflate, int heart_beat,
        bool suppressLog, const char* extra_headers, const char* tls_cafile, const char* tls_keyfile,
        const char* tls_certfile, bool tls_disable_hostname_validation
    ) : m_sessionId(uuid), m_notify(callback), m_suppress_log(suppressLog),
        m_extra_headers(extra_headers) {

        WebSocketHeaders hdrs;
        WebSocketTLSOptions tls;

        if (m_extra_headers) {
            cJSON *headers_json = cJSON_Parse(m_extra_headers);
            if (headers_json) {
                cJSON *iterator = headers_json->child;
                while (iterator) {
                    if (iterator->type == cJSON_String && iterator->valuestring != nullptr) {
                        hdrs.set(iterator->string, iterator->valuestring);
                    }
                    iterator = iterator->next;
                }
                cJSON_Delete(headers_json);
            }
        }

        client.setUrl(wsUri);

        if (tls_cafile) {
            tls.caFile = tls_cafile;
        }
        if (tls_keyfile) {
            tls.keyFile = tls_keyfile;
        }
        if (tls_certfile) {
            tls.certFile = tls_certfile;
        }

        tls.disableHostnameValidation = tls_disable_hostname_validation;
        client.setTLSOptions(tls);

        if (heart_beat)
            client.setPingInterval(heart_beat);

        if (deflate)
            client.enableCompression(false);

        if (!hdrs.empty())
            client.setHeaders(hdrs);
    }

    struct ProcessResult {
        switch_bool_t ok = SWITCH_FALSE;
        std::string rewrittenJsonData;
        std::vector<std::string> errors;
        bool handled_control = false;
    };

    static inline void push_err(ProcessResult& out, const std::string& sid, const std::string& s) {
        out.errors.push_back("(" + sid + ") " + s);
    }

    /* Optional integer field with a default; tolerates string or number JSON. */
    static inline int json_int(cJSON* root, const char* key, int fallback) {
        cJSON* item = cJSON_GetObjectItem(root, key);
        if (!item) return fallback;
        /* cJSON ORs flags such as cJSON_IsReference into type; mask them off. */
        const int type = item->type & 0xFF;
        if (type == cJSON_Number) return item->valueint;
        if (type == cJSON_String && item->valuestring) {
            char* end = nullptr;
            long v = std::strtol(item->valuestring, &end, 10);
            if (end && end != item->valuestring) return (int)v;
        }
        return fallback;
    }

    /* Dialplan extensions we are willing to hand to the transfer app. Deliberately
     * narrow: digits, letters and the punctuation FreeSWITCH extensions actually use.
     */
    static inline bool valid_extension(const char* s) {
        if (!s || !*s) return false;
        size_t n = std::strlen(s);
        if (n >= STREAM_DEST_MAX) return false;
        for (size_t i = 0; i < n; i++) {
            const unsigned char c = (unsigned char)s[i];
            if (std::isalnum(c)) continue;
            if (c == '_' || c == '-' || c == '.' || c == '+' || c == '*' || c == '#') continue;
            return false;
        }
        return true;
    }

    /* Dialplan / context names: identifier characters only. cap is the
     * destination field size, so a name that would be truncated is rejected
     * rather than silently shortened into a different context.
     */
    static inline bool valid_token(const char* s, size_t cap) {
        if (!s || !*s) return false;
        size_t n = std::strlen(s);
        if (n >= cap) return false;
        for (size_t i = 0; i < n; i++) {
            const unsigned char c = (unsigned char)s[i];
            if (std::isalnum(c) || c == '_' || c == '-') continue;
            return false;
        }
        return true;
    }

    /* Record a deferred hangup/transfer. Never touches session state here: the
     * WebSocket event thread is shared with media receive, so it must not block or
     * re-enter FreeSWITCH channel APIs. The media thread picks this up.
     */
    bool arm_pending_action(switch_core_session_t* psession, ProcessResult& out, int action,
                            int drain_ms, const char* cause, const char* dest,
                            const char* dialplan, const char* context) {
        switch_media_bug_t* bug = get_media_bug(psession);
        if (!bug) {
            push_err(out, m_sessionId, "action - no media bug");
            return false;
        }
        auto* tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);
        if (!tech_pvt) {
            push_err(out, m_sessionId, "action - no tech_pvt");
            return false;
        }
        if (drain_ms < 0) drain_ms = 0;
        if (drain_ms > STREAM_ACTION_MAX_DRAIN_MS) drain_ms = STREAM_ACTION_MAX_DRAIN_MS;

        switch_mutex_t* im = tech_pvt->inject_mutex ? tech_pvt->inject_mutex : tech_pvt->mutex;
        switch_mutex_lock(im);
        if (tech_pvt->pending_action != STREAM_ACTION_NONE) {
            /* First request wins; a second one must not retarget a transfer in flight. */
            switch_mutex_unlock(im);
            push_err(out, m_sessionId, "action - already pending");
            return false;
        }
        tech_pvt->pending_action = action;
        tech_pvt->pending_action_published = 0;
        tech_pvt->pending_action_deadline = switch_micro_time_now() + (switch_time_t)drain_ms * 1000;
        stream_copy_field(tech_pvt->pending_cause, sizeof(tech_pvt->pending_cause), cause);
        stream_copy_field(tech_pvt->pending_dest, sizeof(tech_pvt->pending_dest), dest);
        stream_copy_field(tech_pvt->pending_dialplan, sizeof(tech_pvt->pending_dialplan), dialplan);
        stream_copy_field(tech_pvt->pending_context, sizeof(tech_pvt->pending_context), context);
        switch_mutex_unlock(im);

        if (action == STREAM_ACTION_TRANSFER) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(psession), SWITCH_LOG_INFO,
                              "(%s) transfer armed dest=%s dialplan=%s context=%s drain_ms=%d\n",
                              m_sessionId.c_str(), dest ? dest : "", dialplan ? dialplan : "",
                              context ? context : "", drain_ms);
        } else {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(psession), SWITCH_LOG_INFO,
                              "(%s) hangup armed cause=%s drain_ms=%d\n",
                              m_sessionId.c_str(), cause ? cause : "", drain_ms);
        }
        return true;
    }

    void bindCallbacks(std::weak_ptr<AudioStreamer> wp) {
        client.setMessageCallback([wp](const std::string& message) {
            auto self = wp.lock();
            if (!self || self->isCleanedUp()) return;
            self->eventCallback(MESSAGE, message.c_str());
        });

        /*
         * Binary inject: raw PCM16LE at session peer rate (from hello).
         * Preferred path — no JSON/base64 overhead.
         */
        client.setBinaryCallback([wp](const void* data, size_t len) {
            auto self = wp.lock();
            if (!self || self->isCleanedUp()) return;
            self->eventCallbackBinary(data, len);
        });

        client.setOpenCallback([wp]() {
            auto self = wp.lock();
            if (!self || self->isCleanedUp()) return;

            cJSON* root = cJSON_CreateObject();
            cJSON_AddStringToObject(root, "status", "connected");
            char* json_str = cJSON_PrintUnformatted(root);

            self->eventCallback(CONNECT_SUCCESS, json_str);

            cJSON_Delete(root);
            switch_safe_free(json_str);
        });

        client.setErrorCallback([wp](int code, const std::string& msg) {
            auto self = wp.lock();
            if (!self || self->isCleanedUp()) return;

            cJSON* root = cJSON_CreateObject();
            cJSON_AddStringToObject(root, "status", "error");
            cJSON* message = cJSON_CreateObject();
            cJSON_AddNumberToObject(message, "code", code);
            cJSON_AddStringToObject(message, "error", msg.c_str());
            cJSON_AddItemToObject(root, "message", message);

            char* json_str = cJSON_PrintUnformatted(root);
            self->eventCallback(CONNECT_ERROR, json_str);

            cJSON_Delete(root);
            switch_safe_free(json_str);
        });

        client.setCloseCallback([wp](int code, const std::string& reason) {
            auto self = wp.lock();
            if (!self || self->isCleanedUp()) return;

            cJSON* root = cJSON_CreateObject();
            cJSON_AddStringToObject(root, "status", "disconnected");
            cJSON* message = cJSON_CreateObject();
            cJSON_AddNumberToObject(message, "code", code);
            cJSON_AddStringToObject(message, "reason", reason.c_str());
            cJSON_AddItemToObject(root, "message", message);

            char* json_str = cJSON_PrintUnformatted(root);
            self->eventCallback(CONNECTION_DROPPED, json_str);

            cJSON_Delete(root);
            switch_safe_free(json_str);
        });
    }

    static inline size_t pcm16_bytes_per_ms(int sampleRate, int channels) {
        if (sampleRate <= 0 || channels <= 0) return 0;
        return (size_t)sampleRate * 2u * (size_t)channels / 1000u;
    }

    static inline bool host_is_little_endian() {
        const uint16_t x = 1;
        return *((const uint8_t*)&x) == 1;
    }

    static inline void byteswap_inplace_16(std::string& s) {
        const size_t n = s.size() & ~size_t(1);
        for (size_t i = 0; i < n; i += 2) {
            std::swap(s[i], s[i + 1]);
        }
    }

    static inline std::string downmix_stereo_to_mono_pcm16le(const uint8_t* in, size_t in_bytes) {
        const size_t frames = (in_bytes / 4);
        std::string out;
        out.resize(frames * 2);
        for (size_t i = 0; i < frames; ++i) {
            int16_t l, r;
            std::memcpy(&l, in + i * 4, 2);
            std::memcpy(&r, in + i * 4 + 2, 2);
            const int32_t m = ((int32_t)l + (int32_t)r) / 2;
            const int16_t mono = (int16_t)std::max<int32_t>(-32768, std::min<int32_t>(32767, m));
            std::memcpy(&out[i * 2], &mono, 2);
        }
        return out;
    }

    static inline std::string upmix_mono_to_stereo_pcm16le(const uint8_t* in, size_t in_bytes) {
        const size_t frames = in_bytes / 2;
        std::string out;
        out.resize(frames * 4);
        for (size_t i = 0; i < frames; ++i) {
            int16_t m;
            std::memcpy(&m, in + i * 2, 2);
            std::memcpy(&out[i * 4], &m, 2);
            std::memcpy(&out[i * 4 + 2], &m, 2);
        }
        return out;
    }

    static inline std::string resample_pcm16le_speex(const uint8_t* in, size_t in_bytes, int channels,
                                                     int in_sr, int out_sr, SpeexResamplerState* resampler) {
        if (!resampler || in_sr == out_sr) {
            return std::string((const char*)in, (const char*)in + in_bytes);
        }
        const spx_uint32_t in_frames = (spx_uint32_t)(in_bytes / (size_t)(channels * 2));
        if (in_frames == 0) return {};

        const uint64_t nom = (uint64_t)in_frames * (uint64_t)out_sr;
        const uint32_t out_frames_cap = (uint32_t)(nom / (uint64_t)in_sr + 16);
        std::vector<spx_int16_t> out;
        out.resize((size_t)out_frames_cap * (size_t)channels);

        spx_uint32_t in_len = in_frames;
        spx_uint32_t out_len = out_frames_cap;
        int err = 0;

        if (channels == 1) {
            err = speex_resampler_process_int(resampler, 0,
                                              (const spx_int16_t*)in, &in_len,
                                              out.data(), &out_len);
        } else {
            err = speex_resampler_process_interleaved_int(resampler,
                                                          (const spx_int16_t*)in, &in_len,
                                                          out.data(), &out_len);
        }
        if (err != RESAMPLER_ERR_SUCCESS) {
            return std::string((const char*)in, (const char*)in + in_bytes);
        }
        const size_t out_bytes = (size_t)out_len * (size_t)channels * 2;
        return std::string((const char*)out.data(), (const char*)out.data() + out_bytes);
    }

    static inline void drop_oldest_from_buffer(switch_buffer_t* buf, switch_size_t bytes) {
        if (!buf || bytes == 0) return;
        std::vector<uint8_t> tmp;
        tmp.resize((size_t)bytes);
        switch_buffer_read(buf, tmp.data(), bytes);
    }

    static inline void clear_inject_buffer(switch_buffer_t* buf) {
        if (!buf) return;
        switch_buffer_zero(buf);
    }

    bool queue_pcm_locked(private_t* tech_pvt, const uint8_t* pcm, size_t nbytes, int out_sr, int out_channels) {
        if (!tech_pvt || !tech_pvt->inject_buffer || !pcm || nbytes == 0) return false;

        const size_t frame_bytes_20ms = pcm16_bytes_per_ms(out_sr, out_channels) * 20u;
        const switch_size_t inuse_before = switch_buffer_inuse(tech_pvt->inject_buffer);
        const switch_size_t free_before = switch_buffer_freespace(tech_pvt->inject_buffer);
        const size_t max_bytes = (size_t)inuse_before + (size_t)free_before;

        if (max_bytes > 0 && (size_t)inuse_before + nbytes > max_bytes) {
            size_t over = ((size_t)inuse_before + nbytes) - max_bytes;
            size_t drop = over;
            if (frame_bytes_20ms > 0) {
                drop = ((over + frame_bytes_20ms - 1) / frame_bytes_20ms) * frame_bytes_20ms;
            }
            if (drop > (size_t)inuse_before) drop = (size_t)inuse_before;
            if (drop > 0) {
                drop_oldest_from_buffer(tech_pvt->inject_buffer, (switch_size_t)drop);
                __atomic_fetch_add(&tech_pvt->inject_overflow_drops, (uint64_t)drop, __ATOMIC_RELAXED);
                /* Overflow means the peer delivered faster than realtime playout — the
                 * caller loses audio here, so surface it live (throttled) instead of
                 * only in the end-of-call summary.
                 */
                const switch_time_t now = switch_micro_time_now();
                if (now - tech_pvt->last_overflow_warn > 1000000) {
                    tech_pvt->last_overflow_warn = now;
                    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
                                      "%s inject overflow: dropped %" SWITCH_SIZE_T_FMT
                                      " bytes (buffer %" SWITCH_SIZE_T_FMT " ms is full; "
                                      "peer is sending faster than realtime)\n",
                                      tech_pvt->sessionId, (switch_size_t)drop,
                                      (switch_size_t)tech_pvt->inject_buffer_ms);
                }
            }
        }

        switch_buffer_write(tech_pvt->inject_buffer, pcm, (switch_size_t)nbytes);
        __atomic_fetch_add(&tech_pvt->inject_bytes_written, (uint64_t)nbytes, __ATOMIC_RELAXED);
        return true;
    }

    void eventCallbackBinary(const void* data, size_t len) {
        if (!data || len == 0) return;

        switch_core_session_t* psession = switch_core_session_locate(m_sessionId.c_str());
        if (!psession) return;

        switch_media_bug_t* bug = get_media_bug(psession);
        if (!bug) {
            switch_core_session_rwunlock(psession);
            return;
        }
        auto* tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);
        if (!tech_pvt || !tech_pvt->inject_buffer || tech_pvt->cleanup_started) {
            switch_core_session_rwunlock(psession);
            return;
        }

        size_t n = len & ~size_t(1);
        if (n == 0) {
            switch_core_session_rwunlock(psession);
            return;
        }

        const int out_channels = (tech_pvt->channels == 2) ? 2 : 1;
        const int out_sr = tech_pvt->sampling > 0 ? tech_pvt->sampling : tech_pvt->inject_sample_rate;
        if (out_sr <= 0) {
            switch_core_session_rwunlock(psession);
            return;
        }

        std::string decoded((const char*)data, (const char*)data + n);
        if (out_channels == 2) {
            decoded = upmix_mono_to_stereo_pcm16le((const uint8_t*)decoded.data(), decoded.size());
        }

        const size_t frame_bytes_20ms = pcm16_bytes_per_ms(out_sr, out_channels) * 20u;
        if (frame_bytes_20ms > 0 && decoded.size() >= frame_bytes_20ms) {
            decoded.resize(decoded.size() - (decoded.size() % frame_bytes_20ms));
        }
        if (decoded.empty()) {
            switch_core_session_rwunlock(psession);
            return;
        }

        if (switch_mutex_trylock(tech_pvt->inject_mutex ? tech_pvt->inject_mutex : tech_pvt->mutex) != SWITCH_STATUS_SUCCESS) {
            __atomic_fetch_add(&tech_pvt->inject_lock_misses, 1, __ATOMIC_RELAXED);
            switch_core_session_rwunlock(psession);
            return;
        }

        queue_pcm_locked(tech_pvt, (const uint8_t*)decoded.data(), decoded.size(), out_sr, out_channels);
        switch_mutex_unlock(tech_pvt->inject_mutex ? tech_pvt->inject_mutex : tech_pvt->mutex);

        switch_core_session_rwunlock(psession);
    }

    switch_media_bug_t *get_media_bug(switch_core_session_t *session) {
        switch_channel_t *channel = switch_core_session_get_channel(session);
        if (!channel) {
            return nullptr;
        }
        return (switch_media_bug_t *) switch_channel_get_private(channel, MY_BUG_NAME);
    }

    inline void media_bug_close(switch_core_session_t *session) {
        auto *bug = get_media_bug(session);
        if (bug) {
            auto* tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);
            if (tech_pvt) {
                tech_pvt->close_requested = 1;
            }
            switch_core_media_bug_close(&bug, SWITCH_FALSE);
        }
    }

    void send_hello(switch_core_session_t *session) {
        auto *bug = get_media_bug(session);
        if (!bug) return;
        auto* tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);
        if (!tech_pvt || tech_pvt->hello_sent) return;

        cJSON* root = cJSON_CreateObject();
        cJSON_AddStringToObject(root, "type", "hello");
        cJSON_AddStringToObject(root, "uuid", tech_pvt->sessionId);
        cJSON_AddNumberToObject(root, "sampleRate", tech_pvt->sampling);
        cJSON_AddNumberToObject(root, "channels", tech_pvt->channels);
        cJSON_AddStringToObject(root, "encoding", "pcm_s16le");
        cJSON_AddNumberToObject(root, "frameMs", tech_pvt->frame_ms > 0 ? tech_pvt->frame_ms : FRAME_MS_DEFAULT);
        if (tech_pvt->initialMetadata[0] != '\0') {
            /* Prefer raw string; peer may JSON-parse if it looks like an object. */
            cJSON_AddStringToObject(root, "metadata", tech_pvt->initialMetadata);
        }

        char* json_str = cJSON_PrintUnformatted(root);
        if (json_str) {
            writeText(json_str);
            tech_pvt->hello_sent = 1;
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_INFO,
                              "(%s) sent hello sampleRate=%d channels=%d\n",
                              tech_pvt->sessionId, tech_pvt->sampling, tech_pvt->channels);
            switch_safe_free(json_str);
        }
        cJSON_Delete(root);
    }

    void eventCallback(notifyEvent_t event, const char* message) {
        std::string msg = message ? message : "";

        switch_core_session_t* psession = switch_core_session_locate(m_sessionId.c_str());
        if (!psession) {
            return;
        }

        ProcessResult pr;
        if (event == MESSAGE) {
            pr = processMessage(psession, msg);
            if (pr.ok == SWITCH_TRUE && !pr.handled_control) {
                msg = pr.rewrittenJsonData;
            }
        }

        for (const auto& e : pr.errors) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(psession), SWITCH_LOG_ERROR, "%s\n", e.c_str());
        }

        switch (event) {
            case CONNECT_SUCCESS:
                send_hello(psession);
                m_notify(psession, EVENT_CONNECT, msg.c_str());
                break;

            case CONNECTION_DROPPED:
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(psession), SWITCH_LOG_INFO,
                                  "(%s) connection closed\n", m_sessionId.c_str());
                m_notify(psession, EVENT_DISCONNECT, msg.c_str());
                break;

            case CONNECT_ERROR:
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(psession), SWITCH_LOG_ERROR,
                                  "(%s) connection error\n", m_sessionId.c_str());
                m_notify(psession, EVENT_ERROR, msg.c_str());
                media_bug_close(psession);
                break;

            case MESSAGE:
                if (pr.handled_control) {
                    /* flush/stop already applied — optional JSON event */
                    if (!pr.rewrittenJsonData.empty()) {
                        m_notify(psession, EVENT_JSON, pr.rewrittenJsonData.c_str());
                    }
                } else if (pr.ok == SWITCH_TRUE) {
                    m_notify(psession, EVENT_PLAY, msg.c_str());
                } else if (!msg.empty()) {
                    m_notify(psession, EVENT_JSON, msg.c_str());
                }

                if (!m_suppress_log && pr.ok == SWITCH_TRUE && !pr.handled_control) {
                    switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(psession), SWITCH_LOG_DEBUG,
                                      "(%s) inject accepted (v%s)\n", m_sessionId.c_str(), MOD_AUDIO_STREAM_VERSION);
                }
                break;
        }

        switch_core_session_rwunlock(psession);
    }

    ProcessResult processMessage(switch_core_session_t* psession, const std::string& message) {
        ProcessResult out;

        using jsonPtr = std::unique_ptr<cJSON, decltype(&cJSON_Delete)>;
        jsonPtr root(cJSON_Parse(message.c_str()), &cJSON_Delete);
        if (!root) {
            return out;
        }

        const char* jsonType = cJSON_GetObjectCstr(root.get(), "type");

        /* ---- control: flush ---- */
        if (jsonType && std::strcmp(jsonType, "flush") == 0) {
            out.handled_control = true;
            switch_media_bug_t* bug = get_media_bug(psession);
            if (!bug) {
                push_err(out, m_sessionId, "flush - no media bug");
                return out;
            }
            auto* tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);
            if (!tech_pvt || !tech_pvt->inject_buffer) {
                push_err(out, m_sessionId, "flush - inject_buffer missing");
                return out;
            }
            /* Barge-in must clear — take the lock (may wait briefly). */
            switch_mutex_t *im = tech_pvt->inject_mutex ? tech_pvt->inject_mutex : tech_pvt->mutex;
            switch_mutex_lock(im);
            clear_inject_buffer(tech_pvt->inject_buffer);
            switch_mutex_unlock(im);
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(psession), SWITCH_LOG_INFO,
                              "(%s) inject buffer flushed\n", m_sessionId.c_str());
            out.rewrittenJsonData = "{\"type\":\"flush\",\"status\":\"ok\"}";
            out.ok = SWITCH_TRUE;
            return out;
        }

        /* ---- control: stop (peer requests orderly end) ---- */
        if (jsonType && std::strcmp(jsonType, "stop") == 0) {
            out.handled_control = true;
            out.rewrittenJsonData = message;
            out.ok = SWITCH_TRUE;
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(psession), SWITCH_LOG_INFO,
                              "(%s) peer requested stop\n", m_sessionId.c_str());
            media_bug_close(psession);
            return out;
        }

        /* ---- control: hangup (peer ends the call after playout drains) ---- */
        if (jsonType && std::strcmp(jsonType, "hangup") == 0) {
            out.handled_control = true;
            const char* cause = cJSON_GetObjectCstr(root.get(), "cause");
            int drain_ms = json_int(root.get(), "drainMs", STREAM_ACTION_MAX_DRAIN_MS);
            if (!arm_pending_action(psession, out, STREAM_ACTION_HANGUP, drain_ms,
                                    cause && *cause ? cause : "NORMAL_CLEARING",
                                    nullptr, nullptr, nullptr)) {
                return out;
            }
            out.rewrittenJsonData = "{\"type\":\"hangup\",\"status\":\"armed\"}";
            out.ok = SWITCH_TRUE;
            return out;
        }

        /* ---- control: transfer (uuid_transfer equivalent, run by the dialplan app) ---- */
        if (jsonType && std::strcmp(jsonType, "transfer") == 0) {
            out.handled_control = true;
            const char* dest = cJSON_GetObjectCstr(root.get(), "dest");
            if (!dest) dest = cJSON_GetObjectCstr(root.get(), "destination");
            const char* dialplan = cJSON_GetObjectCstr(root.get(), "dialplan");
            const char* context = cJSON_GetObjectCstr(root.get(), "context");
            int drain_ms = json_int(root.get(), "drainMs", STREAM_ACTION_MAX_DRAIN_MS);

            if (!dest || !*dest) {
                push_err(out, m_sessionId, "transfer - missing dest");
                out.rewrittenJsonData = "{\"type\":\"transfer\",\"status\":\"error\",\"error\":\"missing dest\"}";
                return out;
            }
            if (!valid_extension(dest)) {
                push_err(out, m_sessionId, "transfer - illegal dest");
                out.rewrittenJsonData = "{\"type\":\"transfer\",\"status\":\"error\",\"error\":\"illegal dest\"}";
                return out;
            }
            if (dialplan && *dialplan && !valid_token(dialplan, STREAM_DIALPLAN_MAX)) {
                push_err(out, m_sessionId, "transfer - illegal dialplan");
                out.rewrittenJsonData = "{\"type\":\"transfer\",\"status\":\"error\",\"error\":\"illegal dialplan\"}";
                return out;
            }
            if (context && *context && !valid_token(context, STREAM_CONTEXT_MAX)) {
                push_err(out, m_sessionId, "transfer - illegal context");
                out.rewrittenJsonData = "{\"type\":\"transfer\",\"status\":\"error\",\"error\":\"illegal context\"}";
                return out;
            }
            if (!arm_pending_action(psession, out, STREAM_ACTION_TRANSFER, drain_ms,
                                    nullptr, dest,
                                    dialplan && *dialplan ? dialplan : "XML",
                                    context && *context ? context : "calltransfer")) {
                return out;
            }
            out.rewrittenJsonData = "{\"type\":\"transfer\",\"status\":\"armed\"}";
            out.ok = SWITCH_TRUE;
            return out;
        }

        cJSON* jsonData = nullptr;

        if (!jsonType || std::strcmp(jsonType, "streamAudio") != 0) {
            if (cJSON_GetObjectItem(root.get(), "audioData") || cJSON_GetObjectItem(root.get(), "audioDataType")) {
                jsonData = root.get();
            } else {
                return out;
            }
        } else {
            jsonData = cJSON_GetObjectItem(root.get(), "data");
            if (!jsonData) {
                push_err(out, m_sessionId, "processMessage - no data in streamAudio");
                return out;
            }
        }

        const char* jsAudioDataType = cJSON_GetObjectCstr(jsonData, "audioDataType");
        if (!jsAudioDataType) jsAudioDataType = "";

        /* Reject legacy file inject (security). */
        if (cJSON_GetObjectItem(jsonData, "file")) {
            push_err(out, m_sessionId, "processMessage - 'file' inject is not supported");
            return out;
        }

        jsonPtr jsonAudio(cJSON_DetachItemFromObject(jsonData, "audioData"), &cJSON_Delete);

        std::string decoded;

        if (jsonAudio && cJSON_IsString(jsonAudio.get()) && jsonAudio->valuestring) {
            const size_t b64len = std::strlen(jsonAudio->valuestring);
            if (b64len == 0) {
                push_err(out, m_sessionId, "processMessage - 'audioData' is empty");
                return out;
            }
            if (b64len > MAX_AUDIO_BASE64_LEN) {
                push_err(out, m_sessionId, "processMessage - 'audioData' too large");
                return out;
            }

            try {
                decoded = base64_decode(jsonAudio->valuestring);
            } catch (const std::exception& e) {
                push_err(out, m_sessionId, "processMessage - base64 decode error: " + std::string(e.what()));
                return out;
            }
        } else {
            push_err(out, m_sessionId, "processMessage - streamAudio missing 'audioData'");
            return out;
        }

        int sampleRate = 0;
        if (cJSON* jsonSampleRate = cJSON_GetObjectItem(jsonData, "sampleRate")) {
            sampleRate = jsonSampleRate->valueint;
        }

        if (std::strcmp(jsAudioDataType, "raw") != 0) {
            push_err(out, m_sessionId, "processMessage - unsupported audioDataType (need raw)");
            return out;
        }

        if (sampleRate <= 0) {
            push_err(out, m_sessionId, "processMessage - missing/invalid sampleRate");
            return out;
        }

        if (decoded.empty()) {
            push_err(out, m_sessionId, "processMessage - decoded audio is empty");
            return out;
        }

        if (decoded.size() % 2u != 0u) {
            decoded.resize(decoded.size() - 1);
        }

        switch_media_bug_t* bug = get_media_bug(psession);
        if (!bug) {
            push_err(out, m_sessionId, "processMessage - no media bug for injection");
            return out;
        }
        auto* tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);
        if (!tech_pvt || !tech_pvt->inject_buffer || tech_pvt->cleanup_started) {
            push_err(out, m_sessionId, "processMessage - session not ready for injection");
            return out;
        }

        if (!host_is_little_endian()) {
            byteswap_inplace_16(decoded);
        }

        int in_channels = 1;
        if (cJSON* jsonCh = cJSON_GetObjectItem(jsonData, "channels")) {
            if (cJSON_IsNumber(jsonCh) && jsonCh->valueint > 0) {
                in_channels = jsonCh->valueint;
            }
        }
        if (in_channels != 1 && in_channels != 2) {
            push_err(out, m_sessionId, "processMessage - unsupported channels (must be 1 or 2)");
            return out;
        }

        const int out_channels = (tech_pvt->channels == 2) ? 2 : 1;

        if (in_channels == 2 && out_channels == 1) {
            decoded = downmix_stereo_to_mono_pcm16le((const uint8_t*)decoded.data(), decoded.size());
        } else if (in_channels == 1 && out_channels == 2) {
            decoded = upmix_mono_to_stereo_pcm16le((const uint8_t*)decoded.data(), decoded.size());
        }

        const int out_sr = tech_pvt->sampling > 0 ? tech_pvt->sampling : tech_pvt->inject_sample_rate;
        if (out_sr <= 0) {
            push_err(out, m_sessionId, "processMessage - invalid output sample rate");
            return out;
        }

        if (sampleRate != out_sr) {
            switch_mutex_lock(tech_pvt->inject_mutex);
            if (!tech_pvt->inject_resampler) {
                int err = 0;
                tech_pvt->inject_resampler = speex_resampler_init(out_channels, sampleRate, out_sr,
                                                                  SWITCH_RESAMPLE_QUALITY, &err);
                if (err != 0 || !tech_pvt->inject_resampler) {
                    switch_mutex_unlock(tech_pvt->inject_mutex);
                    push_err(out, m_sessionId, "processMessage - failed to init inject resampler");
                    return out;
                }
            } else {
                spx_uint32_t in_r = 0, out_r = 0;
                speex_resampler_get_rate(tech_pvt->inject_resampler, &in_r, &out_r);
                if ((int)in_r != sampleRate || (int)out_r != out_sr) {
                    speex_resampler_destroy(tech_pvt->inject_resampler);
                    tech_pvt->inject_resampler = nullptr;
                    int err = 0;
                    tech_pvt->inject_resampler = speex_resampler_init(out_channels, sampleRate, out_sr,
                                                                      SWITCH_RESAMPLE_QUALITY, &err);
                    if (err != 0 || !tech_pvt->inject_resampler) {
                        switch_mutex_unlock(tech_pvt->inject_mutex);
                        push_err(out, m_sessionId, "processMessage - failed to reinit inject resampler");
                        return out;
                    }
                }
            }

            SpeexResamplerState* inj_rs = tech_pvt->inject_resampler;
            switch_mutex_unlock(tech_pvt->inject_mutex);

            decoded = resample_pcm16le_speex((const uint8_t*)decoded.data(), decoded.size(), out_channels,
                                            sampleRate, out_sr, inj_rs);
            sampleRate = out_sr;

            if (decoded.empty()) {
                push_err(out, m_sessionId, "processMessage - resample produced empty output");
                return out;
            }
        }

        const size_t frame_align = (size_t)out_channels * 2u;
        if (frame_align > 0 && (decoded.size() % frame_align) != 0) {
            decoded.resize(decoded.size() - (decoded.size() % frame_align));
        }

        const size_t frame_bytes_20ms = pcm16_bytes_per_ms(out_sr, out_channels) * 20u;
        if (frame_bytes_20ms > 0 && decoded.size() >= frame_bytes_20ms) {
            decoded.resize(decoded.size() - (decoded.size() % frame_bytes_20ms));
        }

        if (switch_mutex_trylock(tech_pvt->inject_mutex) != SWITCH_STATUS_SUCCESS) {
            push_err(out, m_sessionId, "processMessage - injector busy");
            __atomic_fetch_add(&tech_pvt->inject_lock_misses, 1, __ATOMIC_RELAXED);
            return out;
        }

        tech_pvt->inject_sample_rate = out_sr;
        tech_pvt->inject_bytes_per_sample = 2;
        queue_pcm_locked(tech_pvt, (const uint8_t*)decoded.data(), decoded.size(), out_sr, out_channels);
        const switch_size_t inuse_after = switch_buffer_inuse(tech_pvt->inject_buffer);
        switch_mutex_unlock(tech_pvt->inject_mutex);

        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(psession), SWITCH_LOG_DEBUG,
                          "(%s) streamAudio queued bytes=%zu inuse=%u\n",
                          m_sessionId.c_str(), decoded.size(), (unsigned)inuse_after);

        cJSON_AddNumberToObject(jsonData, "bytes", (double)decoded.size());

        char* jsonString = cJSON_PrintUnformatted(jsonData);
        if (!jsonString) {
            push_err(out, m_sessionId, "processMessage - cJSON_PrintUnformatted failed");
            return out;
        }

        out.rewrittenJsonData.assign(jsonString);
        std::free(jsonString);
        out.ok = SWITCH_TRUE;
        return out;
    }

private:
    std::string m_sessionId;
    responseHandler_t m_notify;
    WebSocketClient client;
    bool m_suppress_log;
    const char* m_extra_headers;
    std::atomic<bool> m_cleanedUp{false};
};


namespace {

    static inline size_t pcm16_bytes_per_ms(int sampleRate, int channels) {
        if (sampleRate <= 0 || channels <= 0) return 0;
        return (size_t)sampleRate * 2u * (size_t)channels / 1000u;
    }

    switch_status_t stream_data_init(private_t *tech_pvt, switch_core_session_t *session, char *wsUri,
                                     uint32_t sampling, int desiredSampling, int channels, char *metadata, responseHandler_t responseHandler,
                                     int deflate, int heart_beat, bool suppressLog, int rtp_packets, int inject_buffer_ms, const char* extra_headers,
                                     const char *tls_cafile, const char *tls_keyfile, const char *tls_certfile,
                                     bool tls_disable_hostname_validation)
    {
        int err;

        switch_memory_pool_t *pool = switch_core_session_get_pool(session);

        const char* _uuid_log = switch_core_session_get_uuid(session);
        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_INFO,
                          "(%s) mod_audio_stream %s feeder/sink init\n",
                          _uuid_log, MOD_AUDIO_STREAM_VERSION);

        memset(tech_pvt, 0, sizeof(private_t));

        strncpy(tech_pvt->sessionId, switch_core_session_get_uuid(session), MAX_SESSION_ID);
        tech_pvt->sessionId[MAX_SESSION_ID - 1] = '\0';
        strncpy(tech_pvt->ws_uri, wsUri, MAX_WS_URI);
        tech_pvt->ws_uri[MAX_WS_URI - 1] = '\0';
        tech_pvt->sampling = desiredSampling;
        tech_pvt->responseHandler = responseHandler;
        tech_pvt->rtp_packets = rtp_packets;
        tech_pvt->channels = channels;
        tech_pvt->audio_paused = 0;
        tech_pvt->frame_ms = FRAME_MS_DEFAULT;
        tech_pvt->inject_buffer_ms = inject_buffer_ms > 0 ? inject_buffer_ms : INJECT_BUFFER_MS_DEFAULT;

        if (metadata) {
            strncpy(tech_pvt->initialMetadata, metadata, MAX_METADATA_LEN);
            tech_pvt->initialMetadata[MAX_METADATA_LEN - 1] = '\0';
        }

        const size_t buflen = (FRAME_SIZE_8000 * desiredSampling / 8000 * channels * rtp_packets);

        switch_mutex_init(&tech_pvt->mutex, SWITCH_MUTEX_NESTED, pool);
        switch_mutex_init(&tech_pvt->inject_mutex, SWITCH_MUTEX_NESTED, pool);

        if (switch_buffer_create(pool, &tech_pvt->sbuffer, buflen) != SWITCH_STATUS_SUCCESS) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR,
                "%s: Error creating switch buffer.\n", tech_pvt->sessionId);
            return SWITCH_STATUS_FALSE;
        }

        tech_pvt->inject_sample_rate = desiredSampling;
        tech_pvt->inject_bytes_per_sample = 2;
        const size_t inject_bytes_per_ms = pcm16_bytes_per_ms(desiredSampling, channels);
        const size_t inject_buflen = std::max<size_t>(
            inject_bytes_per_ms * (size_t)tech_pvt->inject_buffer_ms, 3200u);

        if (switch_buffer_create(pool, &tech_pvt->inject_buffer, inject_buflen) != SWITCH_STATUS_SUCCESS) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR,
                "%s: Error creating inject buffer.\n", tech_pvt->sessionId);
            return SWITCH_STATUS_FALSE;
        }

        auto sp = AudioStreamer::create(tech_pvt->sessionId, wsUri, responseHandler, deflate, heart_beat,
                                        suppressLog, extra_headers, tls_cafile, tls_keyfile,
                                        tls_certfile, tls_disable_hostname_validation);

        tech_pvt->pAudioStreamer = new std::shared_ptr<AudioStreamer>(sp);

        if (desiredSampling != (int)sampling) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG,
                              "(%s) resampling from %u to %u\n", tech_pvt->sessionId, sampling, desiredSampling);
            tech_pvt->resampler = speex_resampler_init(channels, sampling, desiredSampling, SWITCH_RESAMPLE_QUALITY, &err);
            if (0 != err) {
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR,
                                  "Error initializing resampler: %s.\n", speex_resampler_strerror(err));
                return SWITCH_STATUS_FALSE;
            }
        } else {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG,
                              "(%s) no uplink resampling needed\n", tech_pvt->sessionId);
        }

        tech_pvt->inject_resampler = nullptr;

        return SWITCH_STATUS_SUCCESS;
    }

    void destroy_tech_pvt(private_t* tech_pvt) {
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "%s destroy_tech_pvt\n", tech_pvt->sessionId);
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
            "%s inject stats: written=%llu read=%llu starved=%llu lock_miss=%llu overflow_drops=%llu\n",
            tech_pvt->sessionId,
            (unsigned long long)__atomic_load_n(&tech_pvt->inject_bytes_written, __ATOMIC_RELAXED),
            (unsigned long long)__atomic_load_n(&tech_pvt->inject_bytes_read, __ATOMIC_RELAXED),
            (unsigned long long)__atomic_load_n(&tech_pvt->inject_frames_starved, __ATOMIC_RELAXED),
            (unsigned long long)__atomic_load_n(&tech_pvt->inject_lock_misses, __ATOMIC_RELAXED),
            (unsigned long long)__atomic_load_n(&tech_pvt->inject_overflow_drops, __ATOMIC_RELAXED));
        if (tech_pvt->inject_play_codec_ready) {
            switch_core_codec_destroy(&tech_pvt->inject_play_codec);
            tech_pvt->inject_play_codec_ready = 0;
        }
        if (tech_pvt->resampler) {
            speex_resampler_destroy(tech_pvt->resampler);
            tech_pvt->resampler = nullptr;
        }
        if (tech_pvt->inject_resampler) {
            speex_resampler_destroy(tech_pvt->inject_resampler);
            tech_pvt->inject_resampler = nullptr;
        }
        if (tech_pvt->mutex) {
            switch_mutex_destroy(tech_pvt->mutex);
            tech_pvt->mutex = nullptr;
        }
        if (tech_pvt->inject_mutex) {
            switch_mutex_destroy(tech_pvt->inject_mutex);
            tech_pvt->inject_mutex = nullptr;
        }
        tech_pvt->inject_buffer = nullptr;
        tech_pvt->sbuffer = nullptr;
    }

}

extern "C" {
    int validate_ws_uri(const char* url, char* wsUri) {
        const char* scheme = nullptr;
        const char* hostStart = nullptr;
        const char* hostEnd = nullptr;
        const char* portStart = nullptr;

        if (!url || !wsUri) return 0;

        if (strncmp(url, "ws://", 5) == 0) {
            scheme = "ws";
            hostStart = url + 5;
        } else if (strncmp(url, "wss://", 6) == 0) {
            scheme = "wss";
            hostStart = url + 6;
        } else {
            return 0;
        }
        (void)scheme;

        hostEnd = hostStart;
        while (*hostEnd && *hostEnd != ':' && *hostEnd != '/') {
            const unsigned char ch = (unsigned char)*hostEnd;
            if (!std::isalnum(ch) && *hostEnd != '-' && *hostEnd != '.') {
                return 0;
            }
            ++hostEnd;
        }

        if (hostStart == hostEnd) {
            return 0;
        }

        if (*hostEnd == ':') {
            portStart = hostEnd + 1;
            if (!*portStart || *portStart == '/') {
                return 0;
            }
            while (*portStart && *portStart != '/') {
                const unsigned char ch = (unsigned char)*portStart;
                if (!std::isdigit(ch)) {
                    return 0;
                }
                ++portStart;
            }
        }

        std::strncpy(wsUri, url, MAX_WS_URI);
        wsUri[MAX_WS_URI - 1] = '\0';
        return 1;
    }

    switch_status_t is_valid_utf8(const char *str) {
        switch_status_t status = SWITCH_STATUS_FALSE;
        while (*str) {
            if ((*str & 0x80) == 0x00) {
                str++;
            } else if ((*str & 0xE0) == 0xC0) {
                if ((str[1] & 0xC0) != 0x80) {
                    return status;
                }
                str += 2;
            } else if ((*str & 0xF0) == 0xE0) {
                if ((str[1] & 0xC0) != 0x80 || (str[2] & 0xC0) != 0x80) {
                    return status;
                }
                str += 3;
            } else if ((*str & 0xF8) == 0xF0) {
                if ((str[1] & 0xC0) != 0x80 || (str[2] & 0xC0) != 0x80 || (str[3] & 0xC0) != 0x80) {
                    return status;
                }
                str += 4;
            } else {
                return status;
            }
        }
        return SWITCH_STATUS_SUCCESS;
    }

    switch_status_t stream_session_send_text(switch_core_session_t *session, char* text) {
        switch_channel_t *channel = switch_core_session_get_channel(session);
        auto *bug = (switch_media_bug_t*) switch_channel_get_private(channel, MY_BUG_NAME);
        if (!bug) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR,
                              "stream_session_send_text failed because no bug\n");
            return SWITCH_STATUS_FALSE;
        }
        auto *tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);

        if (!tech_pvt || tech_pvt->cleanup_started) return SWITCH_STATUS_FALSE;

        std::shared_ptr<AudioStreamer> streamer;

        switch_mutex_lock(tech_pvt->mutex);

        if (tech_pvt->pAudioStreamer) {
            auto sp_wrap = static_cast<std::shared_ptr<AudioStreamer>*>(tech_pvt->pAudioStreamer);
            if (sp_wrap && *sp_wrap) {
                streamer = *sp_wrap;
            }
        }

        switch_mutex_unlock(tech_pvt->mutex);

        if (streamer) {
            streamer->writeText(text);
            return SWITCH_STATUS_SUCCESS;
        }

        return SWITCH_STATUS_FALSE;
    }

    switch_status_t stream_session_pauseresume(switch_core_session_t *session, int pause) {
        switch_channel_t *channel = switch_core_session_get_channel(session);
        auto *bug = (switch_media_bug_t*) switch_channel_get_private(channel, MY_BUG_NAME);
        if (!bug) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR,
                              "stream_session_pauseresume failed because no bug\n");
            return SWITCH_STATUS_FALSE;
        }
        auto *tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);

        if (!tech_pvt) return SWITCH_STATUS_FALSE;

        switch_core_media_bug_flush(bug);
        tech_pvt->audio_paused = pause;
        return SWITCH_STATUS_SUCCESS;
    }

    switch_status_t stream_session_init(switch_core_session_t *session,
                                        responseHandler_t responseHandler,
                                        uint32_t samples_per_second,
                                        char *wsUri,
                                        int sampling,
                                        int channels,
                                        char* metadata,
                                        void **ppUserData)
    {
        int deflate = 0;
        int heart_beat = 0;
        bool suppressLog = false;
        const char* buffer_size;
        const char* extra_headers;
        int rtp_packets = 1;
        int inject_buffer_ms = INJECT_BUFFER_MS_DEFAULT;
        const char* tls_cafile = NULL;
        const char* tls_keyfile = NULL;
        const char* tls_certfile = NULL;
        bool tls_disable_hostname_validation = false;

        switch_channel_t *channel = switch_core_session_get_channel(session);

        if (switch_channel_var_true(channel, "STREAM_MESSAGE_DEFLATE")) {
            deflate = 1;
        }

        if (switch_channel_var_true(channel, "STREAM_SUPPRESS_LOG")) {
            suppressLog = true;
        }

        tls_cafile = switch_channel_get_variable(channel, "STREAM_TLS_CA_FILE");
        tls_keyfile = switch_channel_get_variable(channel, "STREAM_TLS_KEY_FILE");
        tls_certfile = switch_channel_get_variable(channel, "STREAM_TLS_CERT_FILE");

        if (switch_channel_var_true(channel, "STREAM_TLS_DISABLE_HOSTNAME_VALIDATION")) {
            tls_disable_hostname_validation = true;
        }

        const char* heartBeat = switch_channel_get_variable(channel, "STREAM_HEART_BEAT");
        if (heartBeat) {
            char *endptr;
            long value = strtol(heartBeat, &endptr, 10);
            if (*endptr == '\0' && value <= INT_MAX && value >= INT_MIN) {
                heart_beat = (int) value;
            }
        }

        if ((buffer_size = switch_channel_get_variable(channel, "STREAM_BUFFER_SIZE"))) {
            int bSize = atoi(buffer_size);
            if (bSize % 20 != 0) {
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_WARNING,
                                  "%s: Buffer size of %s is not a multiple of 20ms. Using default 20ms.\n",
                                  switch_channel_get_name(channel), buffer_size);
            } else if (bSize >= 20) {
                rtp_packets = bSize / 20;
            }
        }

        const char* inj_ms = switch_channel_get_variable(channel, "STREAM_INJECT_BUFFER_MS");
        if (inj_ms) {
            int v = atoi(inj_ms);
            if (v >= 40 && v <= 5000) {
                inject_buffer_ms = v;
            } else {
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_WARNING,
                                  "%s: STREAM_INJECT_BUFFER_MS=%s out of range; using %d\n",
                                  switch_channel_get_name(channel), inj_ms, INJECT_BUFFER_MS_DEFAULT);
            }
        }

        extra_headers = switch_channel_get_variable(channel, "STREAM_EXTRA_HEADERS");

        auto* tech_pvt = (private_t *) switch_core_session_alloc(session, sizeof(private_t));

        if (!tech_pvt) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "error allocating memory!\n");
            return SWITCH_STATUS_FALSE;
        }
        if (SWITCH_STATUS_SUCCESS != stream_data_init(tech_pvt, session, wsUri, samples_per_second, sampling, channels,
                                                        metadata, responseHandler, deflate, heart_beat, suppressLog, rtp_packets,
                                                        inject_buffer_ms, extra_headers, tls_cafile, tls_keyfile, tls_certfile,
                                                        tls_disable_hostname_validation)) {
            destroy_tech_pvt(tech_pvt);
            return SWITCH_STATUS_FALSE;
        }

        if (switch_channel_var_true(channel, "STREAM_INJECT_READ")) {
            tech_pvt->inject_read_mode = 1;
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_INFO,
                              "(%s) STREAM_INJECT_READ enabled (READ-path playout)\n",
                              switch_core_session_get_uuid(session));
        }

        *ppUserData = tech_pvt;

        return SWITCH_STATUS_SUCCESS;
    }

    switch_bool_t stream_frame(switch_media_bug_t *bug) {
        auto *tech_pvt = (private_t *)switch_core_media_bug_get_user_data(bug);
        if (!tech_pvt) return SWITCH_TRUE;
        if (tech_pvt->audio_paused || tech_pvt->cleanup_started) return SWITCH_TRUE;

        std::shared_ptr<AudioStreamer> streamer;
        std::vector<std::vector<uint8_t>> pending_send;

        SpeexResamplerState *resampler = nullptr;
        int channels = 1;
        int rtp_packets = 1;
        switch_buffer_t *sbuffer = nullptr;

        if (switch_mutex_trylock(tech_pvt->mutex) != SWITCH_STATUS_SUCCESS) {
            return SWITCH_TRUE;
        }

        if (!tech_pvt->pAudioStreamer) {
            switch_mutex_unlock(tech_pvt->mutex);
            return SWITCH_TRUE;
        }

        auto sp_ptr = static_cast<std::shared_ptr<AudioStreamer>*>(tech_pvt->pAudioStreamer);
        if (!sp_ptr || !(*sp_ptr)) {
            switch_mutex_unlock(tech_pvt->mutex);
            return SWITCH_TRUE;
        }

        streamer = *sp_ptr;

        resampler = tech_pvt->resampler;
        channels = tech_pvt->channels;
        rtp_packets = tech_pvt->rtp_packets;
        sbuffer = tech_pvt->sbuffer;

        switch_mutex_unlock(tech_pvt->mutex);

        if (nullptr == resampler) {

            uint8_t data_buf[SWITCH_RECOMMENDED_BUFFER_SIZE];
            switch_frame_t frame = {};
            frame.data = data_buf;
            frame.buflen = SWITCH_RECOMMENDED_BUFFER_SIZE;

            while (switch_core_media_bug_read(bug, &frame, SWITCH_TRUE) == SWITCH_STATUS_SUCCESS) {
                if (!frame.datalen) {
                    continue;
                }

                if (rtp_packets == 1) {
                    pending_send.emplace_back((uint8_t*)frame.data, (uint8_t*)frame.data + frame.datalen);
                    continue;
                }

                switch_mutex_lock(tech_pvt->mutex);
                size_t freespace = switch_buffer_freespace(sbuffer);

                if (freespace >= frame.datalen) {
                    switch_buffer_write(sbuffer, static_cast<uint8_t *>(frame.data), frame.datalen);
                }

                if (switch_buffer_freespace(sbuffer) == 0) {
                    switch_size_t inuse = switch_buffer_inuse(sbuffer);
                    if (inuse > 0) {
                        std::vector<uint8_t> tmp(inuse);
                        switch_buffer_read(sbuffer, tmp.data(), inuse);
                        switch_buffer_zero(sbuffer);
                        pending_send.emplace_back(std::move(tmp));
                    }
                }

                switch_mutex_unlock(tech_pvt->mutex);
            }

        } else {

            uint8_t data[SWITCH_RECOMMENDED_BUFFER_SIZE];
            switch_frame_t frame = {};
            frame.data = data;
            frame.buflen = SWITCH_RECOMMENDED_BUFFER_SIZE;

            while (switch_core_media_bug_read(bug, &frame, SWITCH_TRUE) == SWITCH_STATUS_SUCCESS) {
                if (!frame.datalen) {
                    continue;
                }

                const size_t freespace = switch_buffer_freespace(sbuffer);
                spx_uint32_t in_len = frame.samples;
                spx_uint32_t out_len = (freespace / (channels * sizeof(spx_int16_t)));

                if (out_len == 0) {
                    if (freespace == 0) {
                        switch_size_t inuse = switch_buffer_inuse(sbuffer);
                        if (inuse > 0) {
                            std::vector<uint8_t> tmp(inuse);
                            switch_buffer_read(sbuffer, tmp.data(), inuse);
                            switch_buffer_zero(sbuffer);
                            pending_send.emplace_back(std::move(tmp));
                        }
                    }
                    continue;
                }

                std::vector<spx_int16_t> out;
                out.resize((size_t)out_len * (size_t)channels);

                if (channels == 1) {
                    speex_resampler_process_int(resampler,
                                    0,
                                    (const spx_int16_t *)frame.data,
                                    &in_len,
                                    out.data(),
                                    &out_len);
                } else {
                    speex_resampler_process_interleaved_int(resampler,
                                    (const spx_int16_t *)frame.data,
                                    &in_len,
                                    out.data(),
                                    &out_len);
                }

                if (out_len > 0) {
                    const size_t bytes_written = (size_t)out_len * (size_t)channels * sizeof(spx_int16_t);

                    if (rtp_packets == 1) {
                        const uint8_t* p = (const uint8_t*)out.data();
                        pending_send.emplace_back(p, p + bytes_written);
                        continue;
                    }

                    if (bytes_written <= switch_buffer_freespace(tech_pvt->sbuffer)) {
                        switch_buffer_write(sbuffer, (const uint8_t *)out.data(), bytes_written);
                    }
                }

                if (switch_buffer_freespace(sbuffer) == 0) {
                    switch_size_t inuse = switch_buffer_inuse(sbuffer);
                    if (inuse > 0) {
                        std::vector<uint8_t> tmp(inuse);
                        switch_buffer_read(sbuffer, tmp.data(), inuse);
                        switch_buffer_zero(sbuffer);
                        pending_send.emplace_back(std::move(tmp));
                    }
                }
            }
        }

        if (!streamer || !streamer->isConnected()) return SWITCH_TRUE;

        for (auto &chunk : pending_send) {
            if (!chunk.empty()) {
                streamer->writeBinary(chunk.data(), chunk.size());
            }
        }

        return SWITCH_TRUE;
    }

    switch_bool_t playout_inject_frame(switch_media_bug_t *bug) {
        switch_core_session_t *session = switch_core_media_bug_get_session(bug);
        private_t *tech_pvt = (private_t *)switch_core_media_bug_get_user_data(bug);

        if (!session || !tech_pvt || tech_pvt->cleanup_started || !tech_pvt->inject_buffer) {
            return SWITCH_TRUE;
        }

        const int rate = tech_pvt->inject_sample_rate > 0 ? tech_pvt->inject_sample_rate : 8000;
        const int ch = tech_pvt->channels > 0 ? tech_pvt->channels : 1;
        const int frame_ms = tech_pvt->frame_ms > 0 ? tech_pvt->frame_ms : FRAME_MS_DEFAULT;
        const size_t frame_bytes = (size_t)rate * 2u * (size_t)ch / 1000u * (size_t)frame_ms;
        if (frame_bytes == 0 || frame_bytes > SWITCH_RECOMMENDED_BUFFER_SIZE) {
            return SWITCH_TRUE;
        }

        uint8_t pcm[SWITCH_RECOMMENDED_BUFFER_SIZE];
        switch_size_t got = 0;

        if (switch_mutex_trylock(tech_pvt->inject_mutex ? tech_pvt->inject_mutex : tech_pvt->mutex)
            == SWITCH_STATUS_SUCCESS) {
            if (switch_buffer_inuse(tech_pvt->inject_buffer) >= frame_bytes) {
                got = switch_buffer_read(tech_pvt->inject_buffer, pcm, frame_bytes);
            }
            switch_mutex_unlock(tech_pvt->inject_mutex ? tech_pvt->inject_mutex : tech_pvt->mutex);
        } else {
            __atomic_fetch_add(&tech_pvt->inject_lock_misses, 1, __ATOMIC_RELAXED);
        }

        if (got == 0) {
            __atomic_fetch_add(&tech_pvt->inject_frames_starved, 1, __ATOMIC_RELAXED);
            return SWITCH_TRUE;
        }

        if (!tech_pvt->inject_play_codec_ready) {
            switch_memory_pool_t *pool = switch_core_session_get_pool(session);
            if (switch_core_codec_init(&tech_pvt->inject_play_codec,
                                       "L16",
                                       NULL,
                                       NULL,
                                       rate,
                                       frame_ms,
                                       ch,
                                       SWITCH_CODEC_FLAG_ENCODE | SWITCH_CODEC_FLAG_DECODE,
                                       NULL,
                                       pool) != SWITCH_STATUS_SUCCESS) {
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR,
                                  "(%s) inject playout codec init failed\n", tech_pvt->sessionId);
                return SWITCH_TRUE;
            }
            tech_pvt->inject_play_codec_ready = 1;
        }

        switch_frame_t write_frame = {};
        write_frame.data = pcm;
        write_frame.buflen = sizeof(pcm);
        write_frame.datalen = got;
        write_frame.samples = (uint32_t)(got / (2u * (size_t)ch));
        write_frame.rate = rate;
        write_frame.channels = ch;
        write_frame.codec = &tech_pvt->inject_play_codec;

        if (switch_core_session_write_frame(session, &write_frame, SWITCH_IO_FLAG_NONE, 0)
            == SWITCH_STATUS_SUCCESS) {
            /* Played out, not produced: this is the read side of the inject buffer. */
            __atomic_fetch_add(&tech_pvt->inject_bytes_read, (uint64_t)got, __ATOMIC_RELAXED);
        }

        return SWITCH_TRUE;
    }

    /* Run a deferred hangup/transfer once queued playout has drained (or the guard
     * deadline expires). Called from the media bug READ path, i.e. the session's own
     * thread — the only context where it is safe to change channel state.
     *
     * Returns SWITCH_FALSE when the bug should be torn down (action executed).
     */
    switch_bool_t stream_service_pending_action(switch_media_bug_t *bug) {
        switch_core_session_t *session = switch_core_media_bug_get_session(bug);
        private_t *tech_pvt = (private_t *)switch_core_media_bug_get_user_data(bug);

        if (!session || !tech_pvt || tech_pvt->cleanup_started) {
            return SWITCH_TRUE;
        }
        if (tech_pvt->pending_action == STREAM_ACTION_NONE || tech_pvt->pending_action_published) {
            return SWITCH_TRUE;
        }

        const int rate = tech_pvt->inject_sample_rate > 0 ? tech_pvt->inject_sample_rate : 8000;
        const int ch = tech_pvt->channels > 0 ? tech_pvt->channels : 1;
        const int frame_ms = tech_pvt->frame_ms > 0 ? tech_pvt->frame_ms : FRAME_MS_DEFAULT;
        const size_t frame_bytes = pcm16_bytes_per_ms(rate, ch) * (size_t)frame_ms;

        const switch_time_t now = switch_micro_time_now();
        const switch_bool_t expired = (tech_pvt->pending_action_deadline > 0 &&
                                       now >= tech_pvt->pending_action_deadline)
                                      ? SWITCH_TRUE : SWITCH_FALSE;

        int action = STREAM_ACTION_NONE;
        char cause[STREAM_CAUSE_MAX] = {0};
        char dest[STREAM_DEST_MAX] = {0};
        char dialplan[STREAM_DIALPLAN_MAX] = {0};
        char context[STREAM_CONTEXT_MAX] = {0};

        switch_mutex_t *im = tech_pvt->inject_mutex ? tech_pvt->inject_mutex : tech_pvt->mutex;
        if (switch_mutex_trylock(im) != SWITCH_STATUS_SUCCESS) {
            /* Contended: try again on the next 20 ms tick. */
            return SWITCH_TRUE;
        }
        const switch_size_t inuse = tech_pvt->inject_buffer
                                    ? switch_buffer_inuse(tech_pvt->inject_buffer) : 0;
        const switch_bool_t drained = (frame_bytes == 0 || inuse < frame_bytes)
                                      ? SWITCH_TRUE : SWITCH_FALSE;
        if (drained == SWITCH_TRUE || expired == SWITCH_TRUE) {
            action = tech_pvt->pending_action;
            stream_copy_field(cause, sizeof(cause), tech_pvt->pending_cause);
            stream_copy_field(dest, sizeof(dest), tech_pvt->pending_dest);
            stream_copy_field(dialplan, sizeof(dialplan), tech_pvt->pending_dialplan);
            stream_copy_field(context, sizeof(context), tech_pvt->pending_context);
            tech_pvt->pending_action_published = 1;
        }
        switch_mutex_unlock(im);

        if (action == STREAM_ACTION_NONE) {
            return SWITCH_TRUE;
        }

        switch_channel_t *channel = switch_core_session_get_channel(session);
        if (!channel) {
            return SWITCH_TRUE;
        }

        /* Publish intent as channel variables + an event before acting, so the
         * dialplan app and CDRs can see what the peer asked for even if the
         * channel disappears immediately afterwards.
         */
        if (action == STREAM_ACTION_TRANSFER) {
            switch_channel_set_variable(channel, STREAM_DEST_VAR, dest);
            switch_channel_set_variable(channel, STREAM_DIALPLAN_VAR, dialplan);
            switch_channel_set_variable(channel, STREAM_CONTEXT_VAR, context);
            switch_channel_set_variable(channel, STREAM_ACTION_VAR, "transfer");
        } else {
            switch_channel_set_variable(channel, STREAM_CAUSE_VAR, cause);
            switch_channel_set_variable(channel, STREAM_ACTION_VAR, "hangup");
        }

        if (expired == SWITCH_TRUE && drained == SWITCH_FALSE) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_WARNING,
                              "(%s) action drain deadline hit with %" SWITCH_SIZE_T_FMT
                              " bytes still queued; proceeding\n",
                              tech_pvt->sessionId, inuse);
        }

        tech_pvt->close_requested = 1;

        /* The caller may have hung up while we were draining. Publishing the
         * variables above is still useful for the CDR, but there is nothing left
         * to transfer or release.
         */
        if (!switch_channel_up(channel)) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_INFO,
                              "(%s) channel already down; skipping %s\n", tech_pvt->sessionId,
                              action == STREAM_ACTION_TRANSFER ? "transfer" : "hangup");
            return SWITCH_FALSE;
        }

        if (action == STREAM_ACTION_TRANSFER) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_NOTICE,
                              "(%s) executing transfer (uuid_transfer %s %s %s %s)\n",
                              tech_pvt->sessionId, tech_pvt->sessionId, dest, dialplan, context);
            /* In-process equivalent of `uuid_transfer <uuid> <dest> <dialplan> <context>`.
             * We are already on the session thread, so this is the same context the
             * `transfer` dialplan app runs in — no second session lookup required.
             */
            switch_ivr_session_transfer(session, dest, dialplan, context);
        } else {
            switch_call_cause_t c = switch_channel_str2cause(cause);
            if (c == SWITCH_CAUSE_NONE) {
                c = SWITCH_CAUSE_NORMAL_CLEARING;
            }
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_NOTICE,
                              "(%s) executing hangup cause=%s\n", tech_pvt->sessionId, cause);
            switch_channel_hangup(channel, c);
        }

        /* Detach the bug: this call leg is no longer ours. */
        return SWITCH_FALSE;
    }

    switch_status_t stream_session_cleanup(switch_core_session_t *session, char* text, int channelIsClosing) {
        switch_channel_t *channel = switch_core_session_get_channel(session);
        auto *bug = (switch_media_bug_t*) switch_channel_get_private(channel, MY_BUG_NAME);
        if (bug)
        {
            auto* tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);
            char sessionId[MAX_SESSION_ID];
            strcpy(sessionId, tech_pvt->sessionId);

            std::shared_ptr<AudioStreamer>* sp_wrap = nullptr;
            std::shared_ptr<AudioStreamer> streamer;

            switch_mutex_lock(tech_pvt->mutex);

            if (tech_pvt->cleanup_started) {
                switch_mutex_unlock(tech_pvt->mutex);
                return SWITCH_STATUS_SUCCESS;
            }
            tech_pvt->cleanup_started = 1;

            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG,
                              "(%s) stream_session_cleanup\n", sessionId);

            switch_channel_set_private(channel, MY_BUG_NAME, nullptr);

            sp_wrap = static_cast<std::shared_ptr<AudioStreamer>*>(tech_pvt->pAudioStreamer);
            tech_pvt->pAudioStreamer = nullptr;

            if (sp_wrap && *sp_wrap) {
                streamer = *sp_wrap;
            }

            switch_mutex_unlock(tech_pvt->mutex);

            if (!channelIsClosing) {
                switch_core_media_bug_remove(session, &bug);
            }

            if (sp_wrap) {
                delete sp_wrap;
                sp_wrap = nullptr;
            }

            if (streamer) {
                if (text) streamer->writeText(text);

                streamer->markCleanedUp();
                streamer->disconnect();
            }

            destroy_tech_pvt(tech_pvt);

            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_INFO,
                              "(%s) stream_session_cleanup: connection closed\n", sessionId);
            return SWITCH_STATUS_SUCCESS;
        }

        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG,
                          "stream_session_cleanup: no bug - websocket already closed\n");
        return SWITCH_STATUS_FALSE;
    }

    switch_status_t stream_session_abort(switch_core_session_t *session, void *pUserData) {
        auto *tech_pvt = (private_t *)pUserData;
        if (!tech_pvt) return SWITCH_STATUS_FALSE;

        char sessionId[MAX_SESSION_ID];
        strncpy(sessionId, tech_pvt->sessionId, MAX_SESSION_ID);
        sessionId[MAX_SESSION_ID - 1] = '\0';

        switch_mutex_lock(tech_pvt->mutex);
        if (tech_pvt->cleanup_started) {
            switch_mutex_unlock(tech_pvt->mutex);
            return SWITCH_STATUS_SUCCESS;
        }
        tech_pvt->cleanup_started = 1;

        std::shared_ptr<AudioStreamer>* sp_wrap =
            static_cast<std::shared_ptr<AudioStreamer>*>(tech_pvt->pAudioStreamer);
        tech_pvt->pAudioStreamer = nullptr;
        std::shared_ptr<AudioStreamer> streamer;
        if (sp_wrap && *sp_wrap) {
            streamer = *sp_wrap;
        }
        switch_mutex_unlock(tech_pvt->mutex);

        if (sp_wrap) {
            delete sp_wrap;
        }
        if (streamer) {
            streamer->markCleanedUp();
            streamer->disconnect();
        }
        destroy_tech_pvt(tech_pvt);

        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_WARNING,
                          "(%s) stream_session_abort: media bug add failed; WS torn down\n",
                          sessionId);
        return SWITCH_STATUS_SUCCESS;
    }
}
