#ifndef MOD_AUDIO_STREAM_H
#define MOD_AUDIO_STREAM_H

#include <switch.h>
#include <speex/speex_resampler.h>
#include <stdint.h>

#define MY_BUG_NAME        "audio_stream"
#define MAX_SESSION_ID     (256)
#define MAX_WS_URI         (4096)
#define MAX_METADATA_LEN   (8192)

#define EVENT_CONNECT      "mod_audio_stream::connect"
#define EVENT_DISCONNECT   "mod_audio_stream::disconnect"
#define EVENT_ERROR        "mod_audio_stream::error"
#define EVENT_JSON         "mod_audio_stream::json"
#define EVENT_PLAY         "mod_audio_stream::play"

#define INJECT_BUFFER_MS_DEFAULT 500
#define FRAME_MS_DEFAULT         20

/* Deferred call-control actions requested by the peer. The WebSocket thread only
 * records the request; the dialplan app (which owns the session thread) performs
 * it once queued playout has drained or the guard deadline expires.
 */
#define STREAM_ACTION_NONE     0
#define STREAM_ACTION_HANGUP   1
#define STREAM_ACTION_TRANSFER 2

/* Upper bound on how long we hold a hangup/transfer waiting for playout to drain. */
#define STREAM_ACTION_MAX_DRAIN_MS 15000

#define STREAM_CAUSE_MAX    64
#define STREAM_DEST_MAX     128
#define STREAM_DIALPLAN_MAX 32
#define STREAM_CONTEXT_MAX  64

#define STREAM_ACTION_VAR       "ai_action"
#define STREAM_CAUSE_VAR        "ai_hangup_cause"
#define STREAM_DEST_VAR         "ai_transfer_dest"
#define STREAM_DIALPLAN_VAR     "ai_transfer_dialplan"
#define STREAM_CONTEXT_VAR      "ai_transfer_context"

typedef void (*responseHandler_t)(
    switch_core_session_t* session,
    const char* eventName,
    const char* json
);

struct private_data {

    switch_mutex_t *mutex;
    /* Dedicated mutex for injection (WS -> FS) buffer/resampler.
     * Media callback must stay non-blocking (trylock only).
     */
    switch_mutex_t *inject_mutex;

    char sessionId[MAX_SESSION_ID];
    char ws_uri[MAX_WS_URI];

    int sampling;
    int channels;
    int frame_ms;

    SpeexResamplerState *resampler;
    SpeexResamplerState *inject_resampler;

    responseHandler_t responseHandler;
    void *pAudioStreamer;

    int audio_paused:1;
    int close_requested:1;
    int cleanup_started:1;
    int hello_sent:1;

    char initialMetadata[MAX_METADATA_LEN];

    switch_buffer_t *sbuffer;
    int rtp_packets;

    switch_buffer_t *inject_buffer;
    int inject_sample_rate;
    int inject_bytes_per_sample;
    int inject_buffer_ms;
    /* When set, drain inject_buffer on READ and write_frame to caller (unbridged Lua legs). */
    int inject_read_mode:1;
    switch_codec_t inject_play_codec;
    int inject_play_codec_ready:1;

    /* Deferred call control (see STREAM_ACTION_* above). Written by the WebSocket
     * thread under inject_mutex, consumed by the media/session thread.
     */
    int pending_action;
    int pending_action_published:1;
    switch_time_t pending_action_deadline;
    char pending_cause[STREAM_CAUSE_MAX];
    char pending_dest[STREAM_DEST_MAX];
    char pending_dialplan[STREAM_DIALPLAN_MAX];
    char pending_context[STREAM_CONTEXT_MAX];

    /* Throttle for the overflow warning so a burst cannot flood the log. */
    switch_time_t last_overflow_warn;

    /* Diagnostics counters (best-effort; __atomic_* in .c / .cpp). */
    uint64_t inject_bytes_written;
    uint64_t inject_bytes_read;
    uint64_t inject_frames_starved;
    uint64_t inject_lock_misses;
    uint64_t inject_overflow_drops;
};

typedef struct private_data private_t;

enum notifyEvent_t {
    CONNECT_SUCCESS,
    CONNECT_ERROR,
    CONNECTION_DROPPED,
    MESSAGE
};

#endif
