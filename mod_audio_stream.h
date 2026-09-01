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
