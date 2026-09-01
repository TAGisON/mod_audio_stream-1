#include "mod_audio_stream.h"
#include "audio_streamer_glue.h"

SWITCH_MODULE_SHUTDOWN_FUNCTION(mod_audio_stream_shutdown);
SWITCH_MODULE_LOAD_FUNCTION(mod_audio_stream_load);

SWITCH_MODULE_DEFINITION(
    mod_audio_stream,
    mod_audio_stream_load,
    mod_audio_stream_shutdown,
    NULL
);

static void responseHandler(switch_core_session_t* session,
                            const char* eventName,
                            const char* json)
{
    switch_event_t *event;
    switch_channel_t *channel = switch_core_session_get_channel(session);

    switch_event_create_subclass(&event, SWITCH_EVENT_CUSTOM, eventName);
    switch_channel_event_set_data(channel, event);
    if (json) {
        switch_event_add_body(event, "%s", json);
    }
    switch_event_fire(&event);
}

static switch_bool_t capture_callback(switch_media_bug_t *bug,
                                      void *user_data,
                                      switch_abc_type_t type)
{
    switch_core_session_t *session =
        switch_core_media_bug_get_session(bug);
    private_t *tech_pvt = (private_t *) user_data;

    switch (type) {

    case SWITCH_ABC_TYPE_INIT:
        break;

    case SWITCH_ABC_TYPE_CLOSE:
        {
            int channel_closing =
                tech_pvt->close_requested ? 0 : 1;

            stream_session_cleanup(session, NULL, channel_closing);
        }
        break;

    case SWITCH_ABC_TYPE_READ:
        if (tech_pvt->close_requested) {
            return SWITCH_FALSE;
        }
        return stream_frame(bug);

    case SWITCH_ABC_TYPE_WRITE_REPLACE:
        {
            switch_frame_t *frame =
                switch_core_media_bug_get_write_replace_frame(bug);

            /* Per-session counters only — avoid statics (wrong under N calls). */
            if (!frame || !frame->data || frame->datalen == 0 || !tech_pvt || !tech_pvt->inject_buffer) {
                break;
            }

            switch_size_t need = frame->datalen;
            switch_size_t got = 0;
            switch_size_t inuse_now = 0;

            /*
             * CRITICAL: FS media timing thread. Never block.
             * Only consume a full frame; otherwise inject silence.
             */
            if (switch_mutex_trylock(tech_pvt->inject_mutex ? tech_pvt->inject_mutex : tech_pvt->mutex) == SWITCH_STATUS_SUCCESS) {
                if (switch_buffer_inuse(tech_pvt->inject_buffer) >= need) {
                    got = switch_buffer_read(tech_pvt->inject_buffer, frame->data, need);
                }
                inuse_now = switch_buffer_inuse(tech_pvt->inject_buffer);
                switch_mutex_unlock(tech_pvt->inject_mutex ? tech_pvt->inject_mutex : tech_pvt->mutex);
            } else {
                __atomic_fetch_add(&tech_pvt->inject_lock_misses, 1, __ATOMIC_RELAXED);
            }

            if (got < need) {
                memset(((unsigned char *)frame->data) + got, 0, need - got);
                __atomic_fetch_add(&tech_pvt->inject_frames_starved, 1, __ATOMIC_RELAXED);
            }

            switch_core_media_bug_set_write_replace_frame(bug, frame);

            if (got > 0) {
                __atomic_fetch_add(&tech_pvt->inject_bytes_read, (uint64_t)got, __ATOMIC_RELAXED);
            }

            /* Rate-limited stats via channel private timestamp would need extra state;
             * use DEBUG only when starved grows — INFO every ~5s via atomic snapshot elsewhere.
             */
            (void)inuse_now;
            (void)session;
        }
        break;

    default:
        break;
    }

    return SWITCH_TRUE;
}

static switch_status_t start_capture(switch_core_session_t *session,
                                     switch_media_bug_flag_t flags,
                                     char* wsUri,
                                     int sampling,
                                     char* metadata)
{
    switch_channel_t *channel =
        switch_core_session_get_channel(session);

    switch_media_bug_t *bug;
    switch_codec_t *read_codec;
    void *pUserData = NULL;

    int channels = (flags & SMBF_STEREO) ? 2 : 1;

    if (switch_channel_get_private(channel, MY_BUG_NAME)) {
        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR,
                          "audio_stream already started on this channel\n");
        return SWITCH_STATUS_FALSE;
    }

    if (switch_channel_pre_answer(channel)
        != SWITCH_STATUS_SUCCESS) {
        return SWITCH_STATUS_FALSE;
    }

    read_codec = switch_core_session_get_read_codec(session);
    if (!read_codec || !read_codec->implementation) {
        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR,
                          "audio_stream: no read codec\n");
        return SWITCH_STATUS_FALSE;
    }

    if (stream_session_init(
            session,
            responseHandler,
            read_codec->implementation->actual_samples_per_second,
            wsUri,
            sampling,
            channels,
            metadata,
            &pUserData
        ) != SWITCH_STATUS_SUCCESS) {
        return SWITCH_STATUS_FALSE;
    }

    flags |= SMBF_READ_STREAM;
    flags |= SMBF_WRITE_REPLACE;

    if (switch_core_media_bug_add(
            session,
            MY_BUG_NAME,
            NULL,
            capture_callback,
            pUserData,
            0,
            flags,
            &bug
        ) != SWITCH_STATUS_SUCCESS) {
        stream_session_abort(session, pUserData);
        return SWITCH_STATUS_FALSE;
    }

    switch_channel_set_private(channel, MY_BUG_NAME, bug);
    return SWITCH_STATUS_SUCCESS;
}

static switch_status_t do_stop(switch_core_session_t *session, char* text)
{
    return stream_session_cleanup(session, text, 0);
}

static switch_status_t do_pauseresume(switch_core_session_t *session, int pause)
{
    return stream_session_pauseresume(session, pause);
}

static switch_status_t send_text(switch_core_session_t *session, char* text)
{
    return stream_session_send_text(session, text);
}

static int is_all_digits(const char *s)
{
    if (!s || !*s) return 0;
    for (; *s; ++s) {
        if (*s < '0' || *s > '9') return 0;
    }
    return 1;
}

#define STREAM_API_SYNTAX \
"<uuid> start <ws-uri> [mono|mixed|stereo] [8000|16000] [metadata]"

SWITCH_STANDARD_API(stream_function)
{
    char *argv[6] = { 0 };
    char *mycmd = NULL;
    int argc = 0;
    switch_status_t status = SWITCH_STATUS_FALSE;

    if (!zstr(cmd) && (mycmd = strdup(cmd))) {
        argc = switch_separate_string(mycmd, ' ', argv, 6);
    }

    if (argc < 2) {
        stream->write_function(stream, "-USAGE: %s\n", STREAM_API_SYNTAX);
        goto done;
    }

    switch_core_session_t *lsession =
        switch_core_session_locate(argv[0]);

    if (!lsession) goto done;

    if (!strcasecmp(argv[1], "stop")) {
        status = do_stop(lsession, argc > 2 ? argv[2] : NULL);
    }
    else if (!strcasecmp(argv[1], "pause")) {
        status = do_pauseresume(lsession, 1);
    }
    else if (!strcasecmp(argv[1], "resume")) {
        status = do_pauseresume(lsession, 0);
    }
    else if (!strcasecmp(argv[1], "send_text")) {
        if (argc < 3 || zstr(argv[2])) {
            stream->write_function(stream, "-USAGE: <uuid> send_text <text>\n");
            switch_core_session_rwunlock(lsession);
            switch_safe_free(mycmd);
            return SWITCH_STATUS_SUCCESS;
        }
        status = send_text(lsession, argv[2]);
    }
    else if (!strcasecmp(argv[1], "start")) {

        char wsUri[MAX_WS_URI];
        int sampling = 8000;
        switch_media_bug_flag_t flags = 0;
        char *metadata = NULL;
        int mix_ok = 1;
        int argi = 3;

        if (argc < 3 || !validate_ws_uri(argv[2], wsUri)) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(lsession), SWITCH_LOG_ERROR,
                              "audio_stream start: invalid ws-uri\n");
            goto start_done;
        }

        /* Optional mix mode. v1: mono only; mixed/stereo rejected explicitly. */
        if (argi < argc) {
            if (!strcasecmp(argv[argi], "mono")) {
                flags = 0;
                argi++;
            } else if (!strcasecmp(argv[argi], "mixed") || !strcasecmp(argv[argi], "stereo")) {
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(lsession), SWITCH_LOG_ERROR,
                                  "audio_stream: '%s' not supported in feeder/sink v1 (mono only)\n",
                                  argv[argi]);
                mix_ok = 0;
                argi++;
            } else if (is_all_digits(argv[argi])) {
                /* mix mode omitted; argv[3] is sample rate */
            } else {
                /* treat as metadata if no rate follows — unusual */
                metadata = argv[argi];
                argi++;
            }
        }

        if (!mix_ok) {
            goto start_done;
        }

        if (argi < argc && is_all_digits(argv[argi])) {
            sampling = atoi(argv[argi]);
            argi++;
            if (sampling != 8000 && sampling != 16000 && sampling != 24000 && sampling != 48000) {
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(lsession), SWITCH_LOG_WARNING,
                                  "audio_stream: unusual sample rate %d\n", sampling);
            }
            if (sampling <= 0) {
                goto start_done;
            }
        }

        if (argi < argc && !metadata) {
            metadata = argv[argi];
        }

        status = start_capture(
            lsession,
            flags,
            wsUri,
            sampling,
            metadata
        );

start_done:
        ;
    }

    switch_core_session_rwunlock(lsession);

done:
    if (status == SWITCH_STATUS_SUCCESS) {
        stream->write_function(stream, "+OK\n");
    } else {
        stream->write_function(stream, "-ERR\n");
    }

    switch_safe_free(mycmd);
    return SWITCH_STATUS_SUCCESS;
}

SWITCH_MODULE_LOAD_FUNCTION(mod_audio_stream_load)
{
    switch_api_interface_t *api_interface;

    *module_interface =
        switch_loadable_module_create_module_interface(pool, modname);

    switch_event_reserve_subclass(EVENT_JSON);
    switch_event_reserve_subclass(EVENT_CONNECT);
    switch_event_reserve_subclass(EVENT_ERROR);
    switch_event_reserve_subclass(EVENT_DISCONNECT);
    switch_event_reserve_subclass(EVENT_PLAY);

    SWITCH_ADD_API(
        api_interface,
        "uuid_audio_stream",
        "audio_stream feeder/sink",
        stream_function,
        STREAM_API_SYNTAX
    );

    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE,
                      "mod_audio_stream loaded (feeder/sink-only)\n");

    return SWITCH_STATUS_SUCCESS;
}

SWITCH_MODULE_SHUTDOWN_FUNCTION(mod_audio_stream_shutdown)
{
    switch_event_free_subclass(EVENT_JSON);
    switch_event_free_subclass(EVENT_CONNECT);
    switch_event_free_subclass(EVENT_DISCONNECT);
    switch_event_free_subclass(EVENT_ERROR);
    switch_event_free_subclass(EVENT_PLAY);
    return SWITCH_STATUS_SUCCESS;
}
