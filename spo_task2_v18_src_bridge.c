#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif
#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 700
#endif

#include <errno.h>
#include <stdint.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

#define main spo_task2_v18_embedded_main
#include "spo_task2_v18.c"
#undef main

typedef long long hlvm_i64;
typedef hlvm_i64 (*bridge_proc_fn_t)(hlvm_i64);

typedef enum {
    BRIDGE_REC_UNKNOWN = 0,
    BRIDGE_REC_TYPE = 1,
    BRIDGE_REC_VEDOMOST = 2,
    BRIDGE_REC_PERSON = 3,
    BRIDGE_REC_STUDY = 4,
    BRIDGE_REC_STUDENT = 5,
    BRIDGE_REC_RESULT = 6
} bridge_record_kind_t;

typedef enum {
    BRIDGE_PIPE_MEMORY = 1,
    BRIDGE_PIPE_RESULT = 2
} bridge_pipe_kind_t;

typedef enum {
    BRIDGE_OWN_PIPE = 1,
    BRIDGE_OWN_STREAM = 2,
    BRIDGE_OWN_PARAM = 3
} bridge_owned_kind_t;

typedef struct {
    bridge_owned_kind_t kind;
    void *ptr;
} bridge_owned_item_t;

typedef struct bridge_pipe {
    bridge_pipe_kind_t kind;
    bridge_record_kind_t record_kind;
    int stream_ready;
    byte_stream_t stream;
    char path[512];
} bridge_pipe_t;

typedef struct bridge_stream {
    bridge_pipe_t *pipe;
    int is_output;
} bridge_stream_t;

typedef struct {
    bridge_stream_t *input;
    bridge_stream_t *output;
    bool nonblock;
    uint64_t spin_seed;
} bridge_source_params_t;

typedef struct {
    bridge_record_kind_t record_kind;
    char field_name[64];
    char op;
    int value;
} bridge_filter_int_ctx_t;

typedef struct {
    filter_arg_t base;
    bridge_stream_t *input_stream;
    bridge_stream_t *output_stream;
    bridge_filter_int_ctx_t ctx;
} bridge_filter_int_params_t;

typedef struct {
    bridge_record_kind_t record_kind;
    char field_name[64];
    char op;
    char value[128];
} bridge_filter_text_ctx_t;

typedef struct {
    filter_arg_t base;
    bridge_stream_t *input_stream;
    bridge_stream_t *output_stream;
    bridge_filter_text_ctx_t ctx;
} bridge_filter_text_params_t;

typedef struct {
    bridge_stream_t *input;
    bridge_stream_t *output;
    bool nonblock;
} bridge_sink_params_t;

typedef struct {
    bridge_stream_t *left;
    bridge_stream_t *right;
    bridge_stream_t *output;
    bool nonblock;
} bridge_two_input_params_t;

typedef struct {
    bridge_proc_fn_t proc;
    hlvm_i64 params;
} bridge_thread_arg_t;

enum { BRIDGE_MAX_OWNED = 1024 };

static dataset_t g_bridge_dataset;
static bool g_bridge_dataset_loaded = false;
static runtime_t g_bridge_runtime;
static bool g_bridge_runtime_ready = false;
static bridge_owned_item_t g_bridge_owned[BRIDGE_MAX_OWNED];
static size_t g_bridge_owned_count = 0u;

hlvm_i64 CsvSourceProc(hlvm_i64 params_handle);
hlvm_i64 FilterIntFieldProc(hlvm_i64 params_handle);
hlvm_i64 FilterTextFieldProc(hlvm_i64 params_handle);
hlvm_i64 JoinTypesVedomostiProc(hlvm_i64 params_handle);
hlvm_i64 JoinPeopleStudiesStudentsProc(hlvm_i64 params_handle);
hlvm_i64 UniqueBirthdaysProc(hlvm_i64 params_handle);
hlvm_i64 PatronymicCountProc(hlvm_i64 params_handle);
hlvm_i64 GroupAgeProc(hlvm_i64 params_handle);
hlvm_i64 EnrollmentListProc(hlvm_i64 params_handle);
hlvm_i64 ExcellentCountProc(hlvm_i64 params_handle);
hlvm_i64 Group2011Proc(hlvm_i64 params_handle);
hlvm_i64 NonStudentsProc(hlvm_i64 params_handle);
hlvm_i64 ResultsSinkProc(hlvm_i64 params_handle);

static const char *bridge_record_kind_name(bridge_record_kind_t kind) {
    switch (kind) {
    case BRIDGE_REC_TYPE:
        return "type";
    case BRIDGE_REC_VEDOMOST:
        return "vedomost";
    case BRIDGE_REC_PERSON:
        return "person";
    case BRIDGE_REC_STUDY:
        return "study";
    case BRIDGE_REC_STUDENT:
        return "student";
    case BRIDGE_REC_RESULT:
        return "result";
    default:
        return "unknown";
    }
}

static const char *bridge_proc_name(bridge_proc_fn_t proc) {
    if (proc == CsvSourceProc) {
        return "CsvSourceProc";
    }
    if (proc == FilterIntFieldProc) {
        return "FilterIntFieldProc";
    }
    if (proc == FilterTextFieldProc) {
        return "FilterTextFieldProc";
    }
    if (proc == JoinTypesVedomostiProc) {
        return "JoinTypesVedomostiProc";
    }
    if (proc == JoinPeopleStudiesStudentsProc) {
        return "JoinPeopleStudiesStudentsProc";
    }
    if (proc == UniqueBirthdaysProc) {
        return "UniqueBirthdaysProc";
    }
    if (proc == PatronymicCountProc) {
        return "PatronymicCountProc";
    }
    if (proc == GroupAgeProc) {
        return "GroupAgeProc";
    }
    if (proc == EnrollmentListProc) {
        return "EnrollmentListProc";
    }
    if (proc == ExcellentCountProc) {
        return "ExcellentCountProc";
    }
    if (proc == Group2011Proc) {
        return "Group2011Proc";
    }
    if (proc == NonStudentsProc) {
        return "NonStudentsProc";
    }
    if (proc == ResultsSinkProc) {
        return "ResultsSinkProc";
    }
    return "unknown_proc";
}

static bridge_pipe_t *bridge_pipe_from_byte_stream(byte_stream_t *stream) {
    if (stream == NULL) {
        return NULL;
    }
    return (bridge_pipe_t *)((char *)stream - offsetof(bridge_pipe_t, stream));
}

hlvm_i64 openCsvPipe(const char *file_name);
hlvm_i64 openResultPipe(const char *file_name);
hlvm_i64 openTypesCsvPipe(void);
hlvm_i64 openVedomostiCsvPipe(void);
hlvm_i64 openPeopleCsvPipe(void);
hlvm_i64 openStudiesCsvPipe(void);
hlvm_i64 openStudentsCsvPipe(void);
hlvm_i64 open48_1ResultPipe(void);
hlvm_i64 open48_2ResultPipe(void);
hlvm_i64 open48_3ResultPipe(void);
hlvm_i64 open48_4ResultPipe(void);
hlvm_i64 open48_5ResultPipe(void);
hlvm_i64 open48_6ResultPipe(void);
hlvm_i64 open48_7ResultPipe(void);
hlvm_i64 open78_1ResultPipe(void);
hlvm_i64 open78_2ResultPipe(void);
hlvm_i64 open78_3ResultPipe(void);
hlvm_i64 open78_4ResultPipe(void);
hlvm_i64 open78_5ResultPipe(void);
hlvm_i64 open78_6ResultPipe(void);
hlvm_i64 open78_7ResultPipe(void);
hlvm_i64 createPipe(void);
hlvm_i64 getInputStream(hlvm_i64 pipe_handle);
hlvm_i64 getOutputStream(hlvm_i64 pipe_handle);
hlvm_i64 createThread(hlvm_i64 proc_id, hlvm_i64 params);
hlvm_i64 waitAllThreads(void);
hlvm_i64 makeSourceParams(hlvm_i64 input, hlvm_i64 output, hlvm_i64 nonblock);
hlvm_i64 makeFilterIntParams(hlvm_i64 input, hlvm_i64 output, hlvm_i64 nonblock,
                             const char *field_name, hlvm_i64 op, hlvm_i64 value);
hlvm_i64 makeFilterTextParams(hlvm_i64 input, hlvm_i64 output, hlvm_i64 nonblock,
                              const char *field_name, hlvm_i64 op, const char *value);
hlvm_i64 makeSinkParams(hlvm_i64 input, hlvm_i64 output, hlvm_i64 nonblock);
hlvm_i64 makeSingleInputParams(hlvm_i64 input, hlvm_i64 output, hlvm_i64 nonblock);
hlvm_i64 makeTwoInputParams(hlvm_i64 left, hlvm_i64 right, hlvm_i64 output, hlvm_i64 nonblock);
hlvm_i64 makeThreeInputParams(hlvm_i64 first, hlvm_i64 second, hlvm_i64 third, hlvm_i64 output,
                              hlvm_i64 nonblock);
hlvm_i64 makeJoinTypesParams(hlvm_i64 types_input, hlvm_i64 veds_input, hlvm_i64 output,
                             hlvm_i64 nonblock, hlvm_i64 output_date);
hlvm_i64 makeJoinPeopleStudiesStudentsParams(hlvm_i64 people_input, hlvm_i64 studies_input,
                                             hlvm_i64 students_input, hlvm_i64 output,
                                             hlvm_i64 nonblock, hlvm_i64 output_nzk);
hlvm_i64 makeGroupAgeParams(hlvm_i64 people_input, hlvm_i64 students_input, hlvm_i64 output,
                            hlvm_i64 nonblock, const char *reference_group,
                            hlvm_i64 less_than_reference);
hlvm_i64 makeEnrollmentParams(hlvm_i64 people_input, hlvm_i64 studies_input,
                              hlvm_i64 students_input, hlvm_i64 output, hlvm_i64 nonblock,
                              hlvm_i64 expelled_after);
hlvm_i64 make48_1_types_filter_params(hlvm_i64 input, hlvm_i64 output);
hlvm_i64 make48_1_veds_filter_params(hlvm_i64 input, hlvm_i64 output);
hlvm_i64 make48_1_join_params(hlvm_i64 types_input, hlvm_i64 veds_input, hlvm_i64 output);
hlvm_i64 make48_2_people_filter_params(hlvm_i64 input, hlvm_i64 output);
hlvm_i64 make48_2_studies_filter_params(hlvm_i64 input, hlvm_i64 output);
hlvm_i64 make48_2_students_filter_params(hlvm_i64 input, hlvm_i64 output);
hlvm_i64 make48_2_join_params(hlvm_i64 people_input, hlvm_i64 studies_input,
                              hlvm_i64 students_input, hlvm_i64 output);
hlvm_i64 make48_5_group_age_params(hlvm_i64 people_input, hlvm_i64 students_input,
                                   hlvm_i64 output);
hlvm_i64 make48_6_enrollment_params(hlvm_i64 people_input, hlvm_i64 studies_input,
                                    hlvm_i64 students_input, hlvm_i64 output);
hlvm_i64 make78_1_types_filter_params(hlvm_i64 input, hlvm_i64 output);
hlvm_i64 make78_1_veds_filter1_params(hlvm_i64 input, hlvm_i64 output);
hlvm_i64 make78_1_veds_filter2_params(hlvm_i64 input, hlvm_i64 output);
hlvm_i64 make78_1_join_params(hlvm_i64 types_input, hlvm_i64 veds_input, hlvm_i64 output);
hlvm_i64 make78_2_people_filter_params(hlvm_i64 input, hlvm_i64 output);
hlvm_i64 make78_2_studies_filter_params(hlvm_i64 input, hlvm_i64 output);
hlvm_i64 make78_2_join_params(hlvm_i64 people_input, hlvm_i64 studies_input,
                              hlvm_i64 students_input, hlvm_i64 output);
hlvm_i64 make78_5_group_age_params(hlvm_i64 people_input, hlvm_i64 students_input,
                                   hlvm_i64 output);
hlvm_i64 make78_6_enrollment_params(hlvm_i64 people_input, hlvm_i64 studies_input,
                                    hlvm_i64 students_input, hlvm_i64 output);
hlvm_i64 CsvSourceProc(hlvm_i64 params_handle);
hlvm_i64 FilterIntFieldProc(hlvm_i64 params_handle);
hlvm_i64 FilterTextFieldProc(hlvm_i64 params_handle);
hlvm_i64 JoinTypesVedomostiProc(hlvm_i64 params_handle);
hlvm_i64 JoinPeopleStudiesStudentsProc(hlvm_i64 params_handle);
hlvm_i64 UniqueBirthdaysProc(hlvm_i64 params_handle);
hlvm_i64 PatronymicCountProc(hlvm_i64 params_handle);
hlvm_i64 GroupAgeProc(hlvm_i64 params_handle);
hlvm_i64 EnrollmentListProc(hlvm_i64 params_handle);
hlvm_i64 ExcellentCountProc(hlvm_i64 params_handle);
hlvm_i64 Group2011Proc(hlvm_i64 params_handle);
hlvm_i64 NonStudentsProc(hlvm_i64 params_handle);
hlvm_i64 ResultsSinkProc(hlvm_i64 params_handle);

static hlvm_i64 bridge_handle_from_ptr(void *ptr) {
    return (hlvm_i64)(intptr_t)ptr;
}

static void *bridge_ptr_from_handle(hlvm_i64 handle) {
    return (void *)(intptr_t)handle;
}

static bool bridge_take_ownership(bridge_owned_kind_t kind, void *ptr) {
    if (ptr == NULL) {
        return false;
    }
    if (g_bridge_owned_count >= BRIDGE_MAX_OWNED) {
        return false;
    }
    g_bridge_owned[g_bridge_owned_count].kind = kind;
    g_bridge_owned[g_bridge_owned_count].ptr = ptr;
    g_bridge_owned_count++;
    return true;
}

static const char *bridge_basename(const char *path) {
    const char *slash;
    const char *backslash;
    if (path == NULL) {
        return "";
    }
    slash = strrchr(path, '/');
    backslash = strrchr(path, '\\');
    if (slash == NULL && backslash == NULL) {
        return path;
    }
    if (slash == NULL) {
        return backslash + 1;
    }
    if (backslash == NULL) {
        return slash + 1;
    }
    return (slash > backslash) ? (slash + 1) : (backslash + 1);
}

static unsigned bridge_tick_us(void) {
    const char *env = getenv("SPO_TASK2_TICK_US");
    if (env != NULL && env[0] != '\0') {
        unsigned parsed = (unsigned)strtoul(env, NULL, 10);
        if (parsed > 0u) {
            return parsed;
        }
    }
    return DEFAULT_TICK_US;
}

static size_t bridge_record_size(bridge_record_kind_t kind) {
    switch (kind) {
    case BRIDGE_REC_TYPE:
        return sizeof(type_row_t);
    case BRIDGE_REC_VEDOMOST:
        return sizeof(vedomost_row_t);
    case BRIDGE_REC_PERSON:
        return sizeof(person_row_t);
    case BRIDGE_REC_STUDY:
        return sizeof(study_row_t);
    case BRIDGE_REC_STUDENT:
        return sizeof(student_row_t);
    case BRIDGE_REC_RESULT:
        return sizeof(result_line_t);
    default:
        return 0u;
    }
}

static bool bridge_ensure_dataset_loaded(void) {
    const char *data_dir;
    if (g_bridge_dataset_loaded) {
        return true;
    }
    data_dir = getenv("SPO_TASK2_DATA_DIR");
    if (data_dir == NULL || data_dir[0] == '\0') {
        data_dir = "data";
    }
    if (!load_dataset(data_dir, &g_bridge_dataset)) {
        fprintf(stderr, "bridge: failed to load dataset from %s\n", data_dir);
        return false;
    }
    g_bridge_dataset_loaded = true;
    return true;
}

static bool bridge_fill_stream_from_rows(byte_stream_t *stream, const void *rows, size_t count) {
    size_t i;
    for (i = 0u; i < count; ++i) {
        const unsigned char *record = (const unsigned char *)rows + i * stream->record_size;
        if (stream_try_write_record(stream, record) != STREAM_OK) {
            return false;
        }
    }
    stream_close_writer(stream);
    return true;
}

static bool bridge_make_parent_dirs(const char *path) {
    char tmp[512];
    size_t i;
    size_t len;
    if (path == NULL || path[0] == '\0') {
        return false;
    }
    copy_text(tmp, sizeof(tmp), path);
    len = strlen(tmp);
    for (i = 1u; i < len; ++i) {
        if (tmp[i] == '/' || tmp[i] == '\\') {
            char saved = tmp[i];
            tmp[i] = '\0';
            if (mkdir(tmp, 0777) != 0 && errno != EEXIST) {
                return false;
            }
            tmp[i] = saved;
        }
    }
    return true;
}

static void bridge_free_pipe(bridge_pipe_t *pipe) {
    if (pipe == NULL) {
        return;
    }
    if (pipe->kind == BRIDGE_PIPE_MEMORY && pipe->stream_ready) {
        stream_destroy(&pipe->stream);
    }
    free(pipe);
}

static void bridge_cleanup_batch(void) {
    while (g_bridge_owned_count > 0u) {
        bridge_owned_item_t item = g_bridge_owned[g_bridge_owned_count - 1u];
        g_bridge_owned_count--;
        if (item.kind == BRIDGE_OWN_PIPE) {
            bridge_free_pipe((bridge_pipe_t *)item.ptr);
        } else {
            free(item.ptr);
        }
    }
}

static bool bridge_prepare_memory_pipe(bridge_pipe_t *pipe, bridge_record_kind_t kind,
                                       const char *name) {
    size_t record_size;
    if (pipe == NULL || pipe->kind != BRIDGE_PIPE_MEMORY) {
        return false;
    }
    if (pipe->stream_ready) {
        return pipe->record_kind == kind;
    }
    record_size = bridge_record_size(kind);
    if (record_size == 0u) {
        return false;
    }
    if (!stream_init(&pipe->stream, name, record_size, DEFAULT_STREAM_CAPACITY)) {
        return false;
    }
    pipe->record_kind = kind;
    pipe->stream_ready = 1;
    return true;
}

static bool bridge_set_runtime_error(const char *message) {
    if (!g_bridge_runtime_ready) {
        runtime_init(&g_bridge_runtime, bridge_tick_us());
        g_bridge_runtime_ready = true;
    }
    g_bridge_runtime.failed = true;
    copy_text(g_bridge_runtime.error_message, sizeof(g_bridge_runtime.error_message), message);
    return false;
}

static bool bridge_int_field(const void *record, bridge_record_kind_t kind, const char *field_name,
                             int *value_out) {
    if (record == NULL || field_name == NULL || value_out == NULL) {
        return false;
    }
    if (kind == BRIDGE_REC_TYPE) {
        const type_row_t *row = (const type_row_t *)record;
        if (strcmp(field_name, "id") == 0) {
            *value_out = row->id;
            return true;
        }
    } else if (kind == BRIDGE_REC_VEDOMOST) {
        const vedomost_row_t *row = (const vedomost_row_t *)record;
        if (strcmp(field_name, "id") == 0) {
            *value_out = row->id;
            return true;
        }
        if (strcmp(field_name, "type_id") == 0) {
            *value_out = row->type_id;
            return true;
        }
        if (strcmp(field_name, "person_id") == 0) {
            *value_out = row->person_id;
            return true;
        }
        if (strcmp(field_name, "mark") == 0) {
            *value_out = row->mark;
            return true;
        }
    } else if (kind == BRIDGE_REC_PERSON) {
        const person_row_t *row = (const person_row_t *)record;
        if (strcmp(field_name, "id") == 0) {
            *value_out = row->id;
            return true;
        }
    } else if (kind == BRIDGE_REC_STUDY) {
        const study_row_t *row = (const study_row_t *)record;
        if (strcmp(field_name, "person_id") == 0) {
            *value_out = row->person_id;
            return true;
        }
        if (strcmp(field_name, "course") == 0) {
            *value_out = row->course;
            return true;
        }
    } else if (kind == BRIDGE_REC_STUDENT) {
        const student_row_t *row = (const student_row_t *)record;
        if (strcmp(field_name, "person_id") == 0) {
            *value_out = row->person_id;
            return true;
        }
    }
    return false;
}

static const char *bridge_text_field(const void *record, bridge_record_kind_t kind,
                                     const char *field_name) {
    if (record == NULL || field_name == NULL) {
        return "";
    }
    if (kind == BRIDGE_REC_VEDOMOST) {
        const vedomost_row_t *row = (const vedomost_row_t *)record;
        if (strcmp(field_name, "date") == 0) {
            return row->date;
        }
    } else if (kind == BRIDGE_REC_PERSON) {
        const person_row_t *row = (const person_row_t *)record;
        if (strcmp(field_name, "surname") == 0) {
            return row->surname;
        }
        if (strcmp(field_name, "name") == 0) {
            return row->name;
        }
        if (strcmp(field_name, "patronymic") == 0) {
            return row->patronymic;
        }
        if (strcmp(field_name, "birthday") == 0) {
            return row->birthday;
        }
    } else if (kind == BRIDGE_REC_STUDY) {
        const study_row_t *row = (const study_row_t *)record;
        if (strcmp(field_name, "nzk") == 0) {
            return row->nzk;
        }
        if (strcmp(field_name, "form") == 0) {
            return row->form;
        }
        if (strcmp(field_name, "direction_code") == 0) {
            return row->direction_code;
        }
        if (strcmp(field_name, "department") == 0) {
            return row->department;
        }
        if (strcmp(field_name, "faculty") == 0) {
            return row->faculty;
        }
    } else if (kind == BRIDGE_REC_STUDENT) {
        const student_row_t *row = (const student_row_t *)record;
        if (strcmp(field_name, "group_name") == 0) {
            return row->group_name;
        }
        if (strcmp(field_name, "start_date") == 0) {
            return row->start_date;
        }
        if (strcmp(field_name, "end_date") == 0) {
            return row->end_date;
        }
        if (strcmp(field_name, "order_number") == 0) {
            return row->order_number;
        }
        if (strcmp(field_name, "order_state") == 0) {
            return row->order_state;
        }
        if (strcmp(field_name, "status") == 0) {
            return row->status;
        }
    }
    return "";
}

static bool bridge_filter_int_predicate(const void *record, const void *ctx_ptr) {
    const bridge_filter_int_ctx_t *ctx = (const bridge_filter_int_ctx_t *)ctx_ptr;
    int field_value = 0;
    if (!bridge_int_field(record, ctx->record_kind, ctx->field_name, &field_value)) {
        return false;
    }
    if (ctx->op == '<') {
        return field_value < ctx->value;
    }
    if (ctx->op == '>') {
        return field_value > ctx->value;
    }
    if (ctx->op == '=') {
        return field_value == ctx->value;
    }
    return false;
}

static bool bridge_filter_text_predicate(const void *record, const void *ctx_ptr) {
    const bridge_filter_text_ctx_t *ctx = (const bridge_filter_text_ctx_t *)ctx_ptr;
    const char *field_value = bridge_text_field(record, ctx->record_kind, ctx->field_name);
    int cmp = strcmp(field_value, ctx->value);
    if (ctx->op == '<') {
        return cmp < 0;
    }
    if (ctx->op == '>') {
        return cmp > 0;
    }
    if (ctx->op == '=') {
        return cmp == 0;
    }
    return false;
}

static void bridge_thread_entry(void *arg) {
    bridge_thread_arg_t *entry = (bridge_thread_arg_t *)arg;
    bridge_proc_fn_t proc = entry->proc;
    hlvm_i64 params = entry->params;
    hlvm_i64 result = 0;
    free(entry);
    if (proc != NULL) {
        result = proc(params);
        if (result != 0) {
            runtime_set_error("bridge worker returned failure");
        }
    }
}

static bool bridge_prepare_output_for_proc(bridge_proc_fn_t proc, hlvm_i64 params_handle) {
    if (proc == CsvSourceProc) {
        bridge_source_params_t *params = (bridge_source_params_t *)bridge_ptr_from_handle(params_handle);
        if (params == NULL || params->input == NULL || params->output == NULL) {
            return false;
        }
        return bridge_prepare_memory_pipe(params->output->pipe, params->input->pipe->record_kind,
                                          "csv_out");
    }
    if (proc == FilterIntFieldProc) {
        bridge_filter_int_params_t *params =
            (bridge_filter_int_params_t *)bridge_ptr_from_handle(params_handle);
        if (params == NULL || params->input_stream == NULL || params->output_stream == NULL) {
            return false;
        }
        params->ctx.record_kind = params->input_stream->pipe->record_kind;
        return bridge_prepare_memory_pipe(params->output_stream->pipe,
                                          params->input_stream->pipe->record_kind,
                                          "filter_int_out");
    }
    if (proc == FilterTextFieldProc) {
        bridge_filter_text_params_t *params =
            (bridge_filter_text_params_t *)bridge_ptr_from_handle(params_handle);
        if (params == NULL || params->input_stream == NULL || params->output_stream == NULL) {
            return false;
        }
        params->ctx.record_kind = params->input_stream->pipe->record_kind;
        return bridge_prepare_memory_pipe(params->output_stream->pipe,
                                          params->input_stream->pipe->record_kind,
                                          "filter_text_out");
    }
    if (proc == JoinTypesVedomostiProc || proc == JoinPeopleStudiesStudentsProc ||
        proc == UniqueBirthdaysProc || proc == PatronymicCountProc || proc == GroupAgeProc ||
        proc == EnrollmentListProc || proc == ExcellentCountProc || proc == Group2011Proc ||
        proc == NonStudentsProc) {
        bridge_pipe_t *output_pipe = NULL;
        if (proc == JoinTypesVedomostiProc) {
            join_types_ved_arg_t *params =
                (join_types_ved_arg_t *)bridge_ptr_from_handle(params_handle);
            output_pipe = (params == NULL) ? NULL : bridge_pipe_from_byte_stream(params->out);
        } else if (proc == JoinPeopleStudiesStudentsProc) {
            join_people_studies_students_arg_t *params =
                (join_people_studies_students_arg_t *)bridge_ptr_from_handle(params_handle);
            output_pipe = (params == NULL) ? NULL : bridge_pipe_from_byte_stream(params->out);
        } else if (proc == UniqueBirthdaysProc) {
            unique_birthdays_arg_t *params =
                (unique_birthdays_arg_t *)bridge_ptr_from_handle(params_handle);
            output_pipe = (params == NULL) ? NULL : bridge_pipe_from_byte_stream(params->out);
        } else if (proc == GroupAgeProc) {
            group_age_arg_t *params = (group_age_arg_t *)bridge_ptr_from_handle(params_handle);
            output_pipe = (params == NULL) ? NULL : bridge_pipe_from_byte_stream(params->out);
        } else if (proc == EnrollmentListProc) {
            enrollment_list_arg_t *params =
                (enrollment_list_arg_t *)bridge_ptr_from_handle(params_handle);
            output_pipe = (params == NULL) ? NULL : bridge_pipe_from_byte_stream(params->out);
        } else {
            bridge_two_input_params_t *params =
                (bridge_two_input_params_t *)bridge_ptr_from_handle(params_handle);
            output_pipe = (params == NULL || params->output == NULL) ? NULL : params->output->pipe;
        }
        return output_pipe != NULL &&
               bridge_prepare_memory_pipe(output_pipe, BRIDGE_REC_RESULT, "result_out");
    }
    if (proc == ResultsSinkProc) {
        bridge_sink_params_t *params = (bridge_sink_params_t *)bridge_ptr_from_handle(params_handle);
        if (params == NULL || params->input == NULL || params->output == NULL ||
            params->input->pipe == NULL || params->output->pipe == NULL) {
            return false;
        }
        if (params->output->pipe->kind != BRIDGE_PIPE_RESULT ||
            params->input->pipe->kind != BRIDGE_PIPE_MEMORY) {
            return false;
        }
        return bridge_prepare_memory_pipe(params->input->pipe, BRIDGE_REC_RESULT, "result_in");
    }
    return true;
}

hlvm_i64 openCsvPipe(const char *file_name) {
    bridge_pipe_t *pipe;
    const char *base;
    const void *rows = NULL;
    size_t count = 0u;
    bridge_record_kind_t record_kind = BRIDGE_REC_UNKNOWN;
    size_t capacity = DEFAULT_STREAM_CAPACITY;

    if (!bridge_ensure_dataset_loaded()) {
        return 0;
    }

    base = bridge_basename(file_name);
    if (strcmp(base, "types_vedomostei.csv") == 0) {
        rows = g_bridge_dataset.types.rows;
        count = g_bridge_dataset.types.count;
        record_kind = BRIDGE_REC_TYPE;
    } else if (strcmp(base, "vedomosti.csv") == 0) {
        rows = g_bridge_dataset.vedomosti.rows;
        count = g_bridge_dataset.vedomosti.count;
        record_kind = BRIDGE_REC_VEDOMOST;
    } else if (strcmp(base, "people.csv") == 0) {
        rows = g_bridge_dataset.people.rows;
        count = g_bridge_dataset.people.count;
        record_kind = BRIDGE_REC_PERSON;
    } else if (strcmp(base, "studies.csv") == 0) {
        rows = g_bridge_dataset.studies.rows;
        count = g_bridge_dataset.studies.count;
        record_kind = BRIDGE_REC_STUDY;
    } else if (strcmp(base, "students.csv") == 0) {
        rows = g_bridge_dataset.students.rows;
        count = g_bridge_dataset.students.count;
        record_kind = BRIDGE_REC_STUDENT;
    } else {
        return 0;
    }

    if (count > capacity) {
        capacity = count;
    }

    pipe = (bridge_pipe_t *)calloc(1, sizeof(*pipe));
    if (pipe == NULL) {
        return 0;
    }
    pipe->kind = BRIDGE_PIPE_MEMORY;
    pipe->record_kind = record_kind;
    pipe->stream_ready = 1;
    if (!stream_init(&pipe->stream, base, bridge_record_size(record_kind), capacity)) {
        free(pipe);
        return 0;
    }
    if (!bridge_fill_stream_from_rows(&pipe->stream, rows, count)) {
        stream_destroy(&pipe->stream);
        free(pipe);
        return 0;
    }
    if (!bridge_take_ownership(BRIDGE_OWN_PIPE, pipe)) {
        bridge_free_pipe(pipe);
        return 0;
    }
    return bridge_handle_from_ptr(pipe);
}

hlvm_i64 openResultPipe(const char *file_name) {
    bridge_pipe_t *pipe = (bridge_pipe_t *)calloc(1, sizeof(*pipe));
    if (pipe == NULL) {
        return 0;
    }
    pipe->kind = BRIDGE_PIPE_RESULT;
    pipe->record_kind = BRIDGE_REC_RESULT;
    copy_text(pipe->path, sizeof(pipe->path), file_name);
    if (!bridge_take_ownership(BRIDGE_OWN_PIPE, pipe)) {
        free(pipe);
        return 0;
    }
    return bridge_handle_from_ptr(pipe);
}

hlvm_i64 openTypesCsvPipe(void) {
    return openCsvPipe("data/types_vedomostei.csv");
}

hlvm_i64 openVedomostiCsvPipe(void) {
    return openCsvPipe("data/vedomosti.csv");
}

hlvm_i64 openPeopleCsvPipe(void) {
    return openCsvPipe("data/people.csv");
}

hlvm_i64 openStudiesCsvPipe(void) {
    return openCsvPipe("data/studies.csv");
}

hlvm_i64 openStudentsCsvPipe(void) {
    return openCsvPipe("data/students.csv");
}

hlvm_i64 open48_1ResultPipe(void) {
    return openResultPipe("output/48_1.txt");
}

hlvm_i64 open48_2ResultPipe(void) {
    return openResultPipe("output/48_2.txt");
}

hlvm_i64 open48_3ResultPipe(void) {
    return openResultPipe("output/48_3.txt");
}

hlvm_i64 open48_4ResultPipe(void) {
    return openResultPipe("output/48_4.txt");
}

hlvm_i64 open48_5ResultPipe(void) {
    return openResultPipe("output/48_5.txt");
}

hlvm_i64 open48_6ResultPipe(void) {
    return openResultPipe("output/48_6.txt");
}

hlvm_i64 open48_7ResultPipe(void) {
    return openResultPipe("output/48_7.txt");
}

hlvm_i64 open78_1ResultPipe(void) {
    return openResultPipe("output/78_1.txt");
}

hlvm_i64 open78_2ResultPipe(void) {
    return openResultPipe("output/78_2.txt");
}

hlvm_i64 open78_3ResultPipe(void) {
    return openResultPipe("output/78_3.txt");
}

hlvm_i64 open78_4ResultPipe(void) {
    return openResultPipe("output/78_4.txt");
}

hlvm_i64 open78_5ResultPipe(void) {
    return openResultPipe("output/78_5.txt");
}

hlvm_i64 open78_6ResultPipe(void) {
    return openResultPipe("output/78_6.txt");
}

hlvm_i64 open78_7ResultPipe(void) {
    return openResultPipe("output/78_7.txt");
}

hlvm_i64 createPipe(void) {
    bridge_pipe_t *pipe = (bridge_pipe_t *)calloc(1, sizeof(*pipe));
    if (pipe == NULL) {
        return 0;
    }
    pipe->kind = BRIDGE_PIPE_MEMORY;
    if (!bridge_take_ownership(BRIDGE_OWN_PIPE, pipe)) {
        free(pipe);
        return 0;
    }
    return bridge_handle_from_ptr(pipe);
}

hlvm_i64 getInputStream(hlvm_i64 pipe_handle) {
    bridge_pipe_t *pipe = (bridge_pipe_t *)bridge_ptr_from_handle(pipe_handle);
    bridge_stream_t *stream = (bridge_stream_t *)calloc(1, sizeof(*stream));
    if (pipe == NULL || stream == NULL) {
        free(stream);
        return 0;
    }
    stream->pipe = pipe;
    if (!bridge_take_ownership(BRIDGE_OWN_STREAM, stream)) {
        free(stream);
        return 0;
    }
    return bridge_handle_from_ptr(stream);
}

hlvm_i64 getOutputStream(hlvm_i64 pipe_handle) {
    bridge_pipe_t *pipe = (bridge_pipe_t *)bridge_ptr_from_handle(pipe_handle);
    bridge_stream_t *stream = (bridge_stream_t *)calloc(1, sizeof(*stream));
    if (pipe == NULL || stream == NULL) {
        free(stream);
        return 0;
    }
    stream->pipe = pipe;
    stream->is_output = 1;
    if (!bridge_take_ownership(BRIDGE_OWN_STREAM, stream)) {
        free(stream);
        return 0;
    }
    return bridge_handle_from_ptr(stream);
}

hlvm_i64 makeSourceParams(hlvm_i64 input, hlvm_i64 output, hlvm_i64 nonblock) {
    bridge_source_params_t *params = (bridge_source_params_t *)calloc(1, sizeof(*params));
    if (params == NULL) {
        return 0;
    }
    params->input = (bridge_stream_t *)bridge_ptr_from_handle(input);
    params->output = (bridge_stream_t *)bridge_ptr_from_handle(output);
    params->nonblock = nonblock != 0;
    params->spin_seed = UINT64_C(0xA100);
    if (!bridge_take_ownership(BRIDGE_OWN_PARAM, params)) {
        free(params);
        return 0;
    }
    return bridge_handle_from_ptr(params);
}

hlvm_i64 makeFilterIntParams(hlvm_i64 input, hlvm_i64 output, hlvm_i64 nonblock,
                             const char *field_name, hlvm_i64 op, hlvm_i64 value) {
    bridge_filter_int_params_t *params =
        (bridge_filter_int_params_t *)calloc(1, sizeof(*params));
    if (params == NULL) {
        return 0;
    }
    params->input_stream = (bridge_stream_t *)bridge_ptr_from_handle(input);
    params->output_stream = (bridge_stream_t *)bridge_ptr_from_handle(output);
    params->base.in = &params->input_stream->pipe->stream;
    params->base.out = &params->output_stream->pipe->stream;
    params->base.nonblock = nonblock != 0;
    params->base.predicate = bridge_filter_int_predicate;
    params->base.predicate_ctx = &params->ctx;
    params->base.spin_seed = UINT64_C(0xA200);
    copy_text(params->ctx.field_name, sizeof(params->ctx.field_name), field_name);
    params->ctx.op = (char)op;
    params->ctx.value = (int)value;
    if (!bridge_take_ownership(BRIDGE_OWN_PARAM, params)) {
        free(params);
        return 0;
    }
    return bridge_handle_from_ptr(params);
}

hlvm_i64 makeFilterTextParams(hlvm_i64 input, hlvm_i64 output, hlvm_i64 nonblock,
                              const char *field_name, hlvm_i64 op, const char *value) {
    bridge_filter_text_params_t *params =
        (bridge_filter_text_params_t *)calloc(1, sizeof(*params));
    if (params == NULL) {
        return 0;
    }
    params->input_stream = (bridge_stream_t *)bridge_ptr_from_handle(input);
    params->output_stream = (bridge_stream_t *)bridge_ptr_from_handle(output);
    params->base.in = &params->input_stream->pipe->stream;
    params->base.out = &params->output_stream->pipe->stream;
    params->base.nonblock = nonblock != 0;
    params->base.predicate = bridge_filter_text_predicate;
    params->base.predicate_ctx = &params->ctx;
    params->base.spin_seed = UINT64_C(0xA300);
    copy_text(params->ctx.field_name, sizeof(params->ctx.field_name), field_name);
    copy_text(params->ctx.value, sizeof(params->ctx.value), value);
    params->ctx.op = (char)op;
    if (!bridge_take_ownership(BRIDGE_OWN_PARAM, params)) {
        free(params);
        return 0;
    }
    return bridge_handle_from_ptr(params);
}

hlvm_i64 makeSinkParams(hlvm_i64 input, hlvm_i64 output, hlvm_i64 nonblock) {
    bridge_sink_params_t *params = (bridge_sink_params_t *)calloc(1, sizeof(*params));
    if (params == NULL) {
        return 0;
    }
    params->input = (bridge_stream_t *)bridge_ptr_from_handle(input);
    params->output = (bridge_stream_t *)bridge_ptr_from_handle(output);
    params->nonblock = nonblock != 0;
    if (!bridge_take_ownership(BRIDGE_OWN_PARAM, params)) {
        free(params);
        return 0;
    }
    return bridge_handle_from_ptr(params);
}

hlvm_i64 makeSingleInputParams(hlvm_i64 input, hlvm_i64 output, hlvm_i64 nonblock) {
    unique_birthdays_arg_t *params = (unique_birthdays_arg_t *)calloc(1, sizeof(*params));
    if (params == NULL) {
        return 0;
    }
    params->people_in = &((bridge_stream_t *)bridge_ptr_from_handle(input))->pipe->stream;
    params->out = &((bridge_stream_t *)bridge_ptr_from_handle(output))->pipe->stream;
    params->nonblock = nonblock != 0;
    if (!bridge_take_ownership(BRIDGE_OWN_PARAM, params)) {
        free(params);
        return 0;
    }
    return bridge_handle_from_ptr(params);
}

hlvm_i64 makeTwoInputParams(hlvm_i64 left, hlvm_i64 right, hlvm_i64 output, hlvm_i64 nonblock) {
    bridge_two_input_params_t *params = (bridge_two_input_params_t *)calloc(1, sizeof(*params));
    if (params == NULL) {
        return 0;
    }
    params->left = (bridge_stream_t *)bridge_ptr_from_handle(left);
    params->right = (bridge_stream_t *)bridge_ptr_from_handle(right);
    params->output = (bridge_stream_t *)bridge_ptr_from_handle(output);
    params->nonblock = nonblock != 0;
    if (!bridge_take_ownership(BRIDGE_OWN_PARAM, params)) {
        free(params);
        return 0;
    }
    return bridge_handle_from_ptr(params);
}

hlvm_i64 makeThreeInputParams(hlvm_i64 first, hlvm_i64 second, hlvm_i64 third, hlvm_i64 output,
                              hlvm_i64 nonblock) {
    (void)first;
    (void)second;
    (void)third;
    (void)output;
    (void)nonblock;
    return 0;
}

hlvm_i64 makeJoinTypesParams(hlvm_i64 types_input, hlvm_i64 veds_input, hlvm_i64 output,
                             hlvm_i64 nonblock, hlvm_i64 output_date) {
    join_types_ved_arg_t *params = (join_types_ved_arg_t *)calloc(1, sizeof(*params));
    if (params == NULL) {
        return 0;
    }
    params->types_in = &((bridge_stream_t *)bridge_ptr_from_handle(types_input))->pipe->stream;
    params->vedomosti_in = &((bridge_stream_t *)bridge_ptr_from_handle(veds_input))->pipe->stream;
    params->out = &((bridge_stream_t *)bridge_ptr_from_handle(output))->pipe->stream;
    params->nonblock = nonblock != 0;
    params->output_date = output_date != 0;
    if (!bridge_take_ownership(BRIDGE_OWN_PARAM, params)) {
        free(params);
        return 0;
    }
    return bridge_handle_from_ptr(params);
}

hlvm_i64 makeJoinPeopleStudiesStudentsParams(hlvm_i64 people_input, hlvm_i64 studies_input,
                                             hlvm_i64 students_input, hlvm_i64 output,
                                             hlvm_i64 nonblock, hlvm_i64 output_nzk) {
    join_people_studies_students_arg_t *params =
        (join_people_studies_students_arg_t *)calloc(1, sizeof(*params));
    if (params == NULL) {
        return 0;
    }
    params->people_in = &((bridge_stream_t *)bridge_ptr_from_handle(people_input))->pipe->stream;
    params->studies_in = &((bridge_stream_t *)bridge_ptr_from_handle(studies_input))->pipe->stream;
    params->students_in =
        &((bridge_stream_t *)bridge_ptr_from_handle(students_input))->pipe->stream;
    params->out = &((bridge_stream_t *)bridge_ptr_from_handle(output))->pipe->stream;
    params->nonblock = nonblock != 0;
    params->output_nzk = output_nzk != 0;
    if (!bridge_take_ownership(BRIDGE_OWN_PARAM, params)) {
        free(params);
        return 0;
    }
    return bridge_handle_from_ptr(params);
}

hlvm_i64 makeGroupAgeParams(hlvm_i64 people_input, hlvm_i64 students_input, hlvm_i64 output,
                            hlvm_i64 nonblock, const char *reference_group,
                            hlvm_i64 less_than_reference) {
    group_age_arg_t *params = (group_age_arg_t *)calloc(1, sizeof(*params));
    if (params == NULL) {
        return 0;
    }
    params->people_in = &((bridge_stream_t *)bridge_ptr_from_handle(people_input))->pipe->stream;
    params->students_in =
        &((bridge_stream_t *)bridge_ptr_from_handle(students_input))->pipe->stream;
    params->out = &((bridge_stream_t *)bridge_ptr_from_handle(output))->pipe->stream;
    params->nonblock = nonblock != 0;
    copy_text(params->reference_group, sizeof(params->reference_group), reference_group);
    params->less_than_reference = less_than_reference != 0;
    if (!bridge_take_ownership(BRIDGE_OWN_PARAM, params)) {
        free(params);
        return 0;
    }
    return bridge_handle_from_ptr(params);
}

hlvm_i64 makeEnrollmentParams(hlvm_i64 people_input, hlvm_i64 studies_input,
                              hlvm_i64 students_input, hlvm_i64 output, hlvm_i64 nonblock,
                              hlvm_i64 expelled_after) {
    enrollment_list_arg_t *params = (enrollment_list_arg_t *)calloc(1, sizeof(*params));
    if (params == NULL) {
        return 0;
    }
    params->people_in = &((bridge_stream_t *)bridge_ptr_from_handle(people_input))->pipe->stream;
    params->studies_in = &((bridge_stream_t *)bridge_ptr_from_handle(studies_input))->pipe->stream;
    params->students_in =
        &((bridge_stream_t *)bridge_ptr_from_handle(students_input))->pipe->stream;
    params->out = &((bridge_stream_t *)bridge_ptr_from_handle(output))->pipe->stream;
    params->nonblock = nonblock != 0;
    params->expelled_after = expelled_after != 0;
    if (!bridge_take_ownership(BRIDGE_OWN_PARAM, params)) {
        free(params);
        return 0;
    }
    return bridge_handle_from_ptr(params);
}

hlvm_i64 make48_1_types_filter_params(hlvm_i64 input, hlvm_i64 output) {
    return makeFilterIntParams(input, output, 0, "id", '>', 2);
}

hlvm_i64 make48_1_veds_filter_params(hlvm_i64 input, hlvm_i64 output) {
    return makeFilterIntParams(input, output, 0, "person_id", '>', 153285);
}

hlvm_i64 make48_1_join_params(hlvm_i64 types_input, hlvm_i64 veds_input, hlvm_i64 output) {
    return makeJoinTypesParams(types_input, veds_input, output, 0, 0);
}

hlvm_i64 make48_2_people_filter_params(hlvm_i64 input, hlvm_i64 output) {
    return makeFilterTextParams(input, output, 0, "patronymic", '<',
                                "\xD0\x92\xD0\xBB\xD0\xB0\xD0\xB4\xD0\xB8\xD0\xBC\xD0\xB8"
                                "\xD1\x80\xD0\xBE\xD0\xB2\xD0\xB8\xD1\x87");
}

hlvm_i64 make48_2_studies_filter_params(hlvm_i64 input, hlvm_i64 output) {
    return makeFilterIntParams(input, output, 0, "person_id", '<', 112514);
}

hlvm_i64 make48_2_students_filter_params(hlvm_i64 input, hlvm_i64 output) {
    return makeFilterTextParams(input, output, 0, "start_date", '<', "2008-09-01");
}

hlvm_i64 make48_2_join_params(hlvm_i64 people_input, hlvm_i64 studies_input,
                              hlvm_i64 students_input, hlvm_i64 output) {
    return makeJoinPeopleStudiesStudentsParams(people_input, studies_input, students_input, output,
                                               0, 0);
}

hlvm_i64 make48_5_group_age_params(hlvm_i64 people_input, hlvm_i64 students_input,
                                   hlvm_i64 output) {
    return makeGroupAgeParams(people_input, students_input, output, 0, "3100", 0);
}

hlvm_i64 make48_6_enrollment_params(hlvm_i64 people_input, hlvm_i64 studies_input,
                                    hlvm_i64 students_input, hlvm_i64 output) {
    return makeEnrollmentParams(people_input, studies_input, students_input, output, 0, 0);
}

hlvm_i64 make78_1_types_filter_params(hlvm_i64 input, hlvm_i64 output) {
    return makeFilterIntParams(input, output, 1, "id", '<', 2);
}

hlvm_i64 make78_1_veds_filter1_params(hlvm_i64 input, hlvm_i64 output) {
    return makeFilterIntParams(input, output, 1, "person_id", '=', 153285);
}

hlvm_i64 make78_1_veds_filter2_params(hlvm_i64 input, hlvm_i64 output) {
    return makeFilterIntParams(input, output, 1, "person_id", '=', 163249);
}

hlvm_i64 make78_1_join_params(hlvm_i64 types_input, hlvm_i64 veds_input, hlvm_i64 output) {
    return makeJoinTypesParams(types_input, veds_input, output, 1, 1);
}

hlvm_i64 make78_2_people_filter_params(hlvm_i64 input, hlvm_i64 output) {
    return makeFilterTextParams(input, output, 1, "name", '>',
                                "\xD0\x90\xD0\xBB\xD0\xB5\xD0\xBA\xD1\x81\xD0\xB0\xD0\xBD"
                                "\xD0\xB4\xD1\x80");
}

hlvm_i64 make78_2_studies_filter_params(hlvm_i64 input, hlvm_i64 output) {
    return makeFilterIntParams(input, output, 1, "person_id", '=', 113409);
}

hlvm_i64 make78_2_join_params(hlvm_i64 people_input, hlvm_i64 studies_input,
                              hlvm_i64 students_input, hlvm_i64 output) {
    return makeJoinPeopleStudiesStudentsParams(people_input, studies_input, students_input, output,
                                               1, 1);
}

hlvm_i64 make78_5_group_age_params(hlvm_i64 people_input, hlvm_i64 students_input,
                                   hlvm_i64 output) {
    return makeGroupAgeParams(people_input, students_input, output, 1, "1100", 1);
}

hlvm_i64 make78_6_enrollment_params(hlvm_i64 people_input, hlvm_i64 studies_input,
                                    hlvm_i64 students_input, hlvm_i64 output) {
    return makeEnrollmentParams(people_input, studies_input, students_input, output, 1, 1);
}

/* Registers a logical worker inside the shared user-space runtime. */
hlvm_i64 createThread(hlvm_i64 proc_id, hlvm_i64 params) {
    bridge_proc_fn_t proc = (bridge_proc_fn_t)(intptr_t)proc_id;
    bridge_thread_arg_t *arg;
    char thread_name[48];
    char error_message[256];

    if (proc == NULL) {
        bridge_set_runtime_error("bridge createThread: null procedure");
        return -1;
    }
    if (!g_bridge_runtime_ready) {
        runtime_init(&g_bridge_runtime, bridge_tick_us());
        g_bridge_runtime_ready = true;
    }
    if (!bridge_prepare_output_for_proc(proc, params)) {
        snprintf(error_message, sizeof(error_message),
                 "bridge createThread: pipe preparation failed for %s (params=%p)",
                 bridge_proc_name(proc), bridge_ptr_from_handle(params));
        bridge_set_runtime_error(error_message);
        if (proc == ResultsSinkProc) {
            bridge_sink_params_t *sink = (bridge_sink_params_t *)bridge_ptr_from_handle(params);
            fprintf(stderr,
                    "bridge debug: sink input=%s output=%s\n",
                    (sink != NULL && sink->input != NULL && sink->input->pipe != NULL)
                        ? bridge_record_kind_name(sink->input->pipe->record_kind)
                        : "null",
                    (sink != NULL && sink->output != NULL && sink->output->pipe != NULL)
                        ? bridge_record_kind_name(sink->output->pipe->record_kind)
                        : "null");
        } else if (proc == CsvSourceProc) {
            bridge_source_params_t *source =
                (bridge_source_params_t *)bridge_ptr_from_handle(params);
            fprintf(stderr,
                    "bridge debug: source input=%s output=%s output_ready=%d\n",
                    (source != NULL && source->input != NULL && source->input->pipe != NULL)
                        ? bridge_record_kind_name(source->input->pipe->record_kind)
                        : "null",
                    (source != NULL && source->output != NULL && source->output->pipe != NULL)
                        ? bridge_record_kind_name(source->output->pipe->record_kind)
                        : "null",
                    (source != NULL && source->output != NULL && source->output->pipe != NULL)
                        ? source->output->pipe->stream_ready
                        : -1);
        }
        return -1;
    }
    arg = (bridge_thread_arg_t *)calloc(1, sizeof(*arg));
    if (arg == NULL) {
        bridge_set_runtime_error("bridge createThread: allocation failed");
        return -1;
    }
    arg->proc = proc;
    arg->params = params;
    snprintf(thread_name, sizeof(thread_name), "src_%p", (void *)(intptr_t)proc_id);
    if (!runtime_create_thread(&g_bridge_runtime, thread_name, bridge_thread_entry, arg)) {
        free(arg);
        return -1;
    }
    return 0;
}

/* Runs the user-space scheduler until every logical worker finishes. */
hlvm_i64 waitAllThreads(void) {
    bool ok = true;
    if (g_bridge_runtime_ready) {
        ok = runtime_run(&g_bridge_runtime);
        if (!ok) {
            fprintf(stderr, "bridge runtime error: %s\n", g_bridge_runtime.error_message);
        }
        runtime_destroy(&g_bridge_runtime);
        g_bridge_runtime_ready = false;
        memset(&g_bridge_runtime, 0, sizeof(g_bridge_runtime));
    }
    bridge_cleanup_batch();
    return ok ? 0 : 1;
}

hlvm_i64 CsvSourceProc(hlvm_i64 params_handle) {
    bridge_source_params_t *params = (bridge_source_params_t *)bridge_ptr_from_handle(params_handle);
    unsigned char buffer[MAX_RECORD_SIZE];
    if (params == NULL || params->input == NULL || params->output == NULL) {
        return 1;
    }
    while (true) {
        bool eof = false;
        if (!read_record_mode(&params->input->pipe->stream, buffer, params->nonblock, &eof)) {
            return 1;
        }
        if (eof) {
            break;
        }
        if (!write_record_mode(&params->output->pipe->stream, buffer, params->nonblock)) {
            return 1;
        }
        worker_spin(params->spin_seed);
    }
    stream_close_writer(&params->output->pipe->stream);
    runtime_wake_waiters(g_runtime);
    return 0;
}

hlvm_i64 FilterIntFieldProc(hlvm_i64 params_handle) {
    bridge_filter_int_params_t *params =
        (bridge_filter_int_params_t *)bridge_ptr_from_handle(params_handle);
    if (params == NULL) {
        return 1;
    }
    filter_thread(&params->base);
    return 0;
}

hlvm_i64 FilterTextFieldProc(hlvm_i64 params_handle) {
    bridge_filter_text_params_t *params =
        (bridge_filter_text_params_t *)bridge_ptr_from_handle(params_handle);
    if (params == NULL) {
        return 1;
    }
    filter_thread(&params->base);
    return 0;
}

hlvm_i64 JoinTypesVedomostiProc(hlvm_i64 params_handle) {
    join_types_ved_thread(bridge_ptr_from_handle(params_handle));
    return 0;
}

hlvm_i64 JoinPeopleStudiesStudentsProc(hlvm_i64 params_handle) {
    join_people_studies_students_thread(bridge_ptr_from_handle(params_handle));
    return 0;
}

hlvm_i64 UniqueBirthdaysProc(hlvm_i64 params_handle) {
    unique_birthdays_thread(bridge_ptr_from_handle(params_handle));
    return 0;
}

hlvm_i64 PatronymicCountProc(hlvm_i64 params_handle) {
    bridge_two_input_params_t *params =
        (bridge_two_input_params_t *)bridge_ptr_from_handle(params_handle);
    patronymic_count_arg_t cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.people_in = &params->left->pipe->stream;
    cfg.studies_in = &params->right->pipe->stream;
    cfg.out = &params->output->pipe->stream;
    cfg.nonblock = params->nonblock;
    patronymic_count_thread(&cfg);
    return 0;
}

hlvm_i64 GroupAgeProc(hlvm_i64 params_handle) {
    group_age_thread(bridge_ptr_from_handle(params_handle));
    return 0;
}

hlvm_i64 EnrollmentListProc(hlvm_i64 params_handle) {
    enrollment_list_thread(bridge_ptr_from_handle(params_handle));
    return 0;
}

hlvm_i64 ExcellentCountProc(hlvm_i64 params_handle) {
    bridge_two_input_params_t *params =
        (bridge_two_input_params_t *)bridge_ptr_from_handle(params_handle);
    excellent_count_arg_t cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.studies_in = &params->left->pipe->stream;
    cfg.vedomosti_in = &params->right->pipe->stream;
    cfg.out = &params->output->pipe->stream;
    cfg.nonblock = params->nonblock;
    excellent_count_thread(&cfg);
    return 0;
}

hlvm_i64 Group2011Proc(hlvm_i64 params_handle) {
    bridge_two_input_params_t *params =
        (bridge_two_input_params_t *)bridge_ptr_from_handle(params_handle);
    group_2011_arg_t cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.studies_in = &params->left->pipe->stream;
    cfg.students_in = &params->right->pipe->stream;
    cfg.out = &params->output->pipe->stream;
    cfg.nonblock = params->nonblock;
    group_2011_thread(&cfg);
    return 0;
}

hlvm_i64 NonStudentsProc(hlvm_i64 params_handle) {
    bridge_two_input_params_t *params =
        (bridge_two_input_params_t *)bridge_ptr_from_handle(params_handle);
    non_students_arg_t cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.people_in = &params->left->pipe->stream;
    cfg.students_in = &params->right->pipe->stream;
    cfg.out = &params->output->pipe->stream;
    cfg.nonblock = params->nonblock;
    non_students_thread(&cfg);
    return 0;
}

hlvm_i64 ResultsSinkProc(hlvm_i64 params_handle) {
    bridge_sink_params_t *params = (bridge_sink_params_t *)bridge_ptr_from_handle(params_handle);
    string_list_t lines = {0};
    result_line_t line;
    FILE *file;
    size_t i;

    if (params == NULL || params->input == NULL || params->output == NULL ||
        params->output->pipe == NULL || params->output->pipe->kind != BRIDGE_PIPE_RESULT) {
        return 1;
    }

    while (true) {
        bool eof = false;
        if (!read_record_mode(&params->input->pipe->stream, &line, params->nonblock, &eof)) {
            string_list_free(&lines);
            return 1;
        }
        if (eof) {
            break;
        }
        if (!string_list_append(&lines, line.text)) {
            string_list_free(&lines);
            return 1;
        }
    }

    string_list_sort(&lines);
    if (!bridge_make_parent_dirs(params->output->pipe->path)) {
        string_list_free(&lines);
        return 1;
    }
    file = fopen(params->output->pipe->path, "w");
    if (file == NULL) {
        string_list_free(&lines);
        return 1;
    }
    for (i = 0u; i < lines.len; ++i) {
        fprintf(file, "%s\n", lines.items[i]);
    }
    fclose(file);
    string_list_free(&lines);
    return 0;
}
