#ifdef _WIN32

#include <stdio.h>

int main(void) {
    fputs("spo_task2_v18 requires POSIX/ucontext runtime. Build and run it on Linux/s390x.\n",
          stderr);
    return 8;
}

#else

#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif
#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 700
#endif

#include <math.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <ucontext.h>

#include "workload.h"

#define STACK_SIZE (64u * 1024u)
#define DEFAULT_TICK_US 1000u
#define MAX_THREADS 32u
#define MAX_WAIT_ITEMS 8u
#define MAX_RECORD_SIZE 512u
#define DEFAULT_STREAM_CAPACITY 16u
#define REFERENCE_YEAR 2025
#define REFERENCE_MONTH 1
#define REFERENCE_DAY 1

typedef struct {
    int id;
    char name[48];
} type_row_t;

typedef struct {
    int id;
    int type_id;
    int person_id;
    char date[11];
    int mark;
} vedomost_row_t;

typedef struct {
    int id;
    char surname[32];
    char name[32];
    char patronymic[32];
    char birthday[11];
} person_row_t;

typedef struct {
    int person_id;
    char nzk[16];
    char form[16];
    char direction_code[16];
    char department[64];
    char faculty[32];
    int course;
} study_row_t;

typedef struct {
    int person_id;
    char group_name[16];
    char start_date[11];
    char end_date[11];
    char order_number[16];
    char order_state[32];
    char status[16];
} student_row_t;

typedef struct {
    char text[256];
} result_line_t;

typedef struct {
    void *items;
    size_t len;
    size_t cap;
    size_t item_size;
} raw_array_t;

typedef struct {
    char **items;
    size_t len;
    size_t cap;
} string_list_t;

typedef struct {
    type_row_t *rows;
    size_t count;
} types_table_t;

typedef struct {
    vedomost_row_t *rows;
    size_t count;
} vedomosti_table_t;

typedef struct {
    person_row_t *rows;
    size_t count;
} people_table_t;

typedef struct {
    study_row_t *rows;
    size_t count;
} studies_table_t;

typedef struct {
    student_row_t *rows;
    size_t count;
} students_table_t;

typedef struct {
    types_table_t types;
    vedomosti_table_t vedomosti;
    people_table_t people;
    studies_table_t studies;
    students_table_t students;
} dataset_t;

typedef enum {
    THREAD_READY = 0,
    THREAD_RUNNING = 1,
    THREAD_BLOCKED = 2,
    THREAD_FINISHED = 3
} thread_state_t;

typedef enum {
    STREAM_OK = 0,
    STREAM_WOULD_BLOCK = 1,
    STREAM_EOF = 2,
    STREAM_CLOSED = 3
} stream_status_t;

struct byte_stream;
struct spo_mutex;

typedef struct wait_item {
    struct byte_stream *stream;
    unsigned flags;
} wait_item_t;

typedef void (*thread_entry_fn)(void *arg);

typedef struct user_thread {
    ucontext_t context;
    unsigned char *stack;
    thread_entry_fn entry;
    void *arg;
    char name[32];
    thread_state_t state;
    wait_item_t wait_items[MAX_WAIT_ITEMS];
    size_t wait_count;
    struct spo_mutex *wait_mutex;
    bool wait_any;
} user_thread_t;

typedef struct runtime_stats {
    size_t context_switches;
    size_t voluntary_yields;
    size_t block_events;
    size_t wake_events;
    size_t group_waits;
    size_t mutex_waits;
} runtime_stats_t;

typedef struct runtime {
    user_thread_t threads[MAX_THREADS];
    size_t thread_count;
    size_t rr_cursor;
    int current;
    ucontext_t scheduler_context;
    struct sigaction old_action;
    struct itimerval old_timer;
    bool action_installed;
    bool timer_installed;
    volatile sig_atomic_t tick_pending;
    unsigned tick_us;
    runtime_stats_t stats;
    bool failed;
    bool deadlock;
    char error_message[128];
} runtime_t;

typedef struct byte_stream {
    char name[32];
    size_t record_size;
    size_t capacity_records;
    size_t count_records;
    size_t head;
    size_t tail;
    bool writer_closed;
    unsigned char *buffer;
} byte_stream_t;

typedef struct spo_mutex {
    char name[32];
    int owner;
} spo_mutex_t;

typedef struct {
    const void *items;
    size_t count;
    size_t item_size;
    byte_stream_t *out;
    bool nonblock;
    uint64_t spin_seed;
} source_arg_t;

typedef bool (*record_predicate_fn)(const void *record, const void *ctx);

typedef struct {
    byte_stream_t *in;
    byte_stream_t *out;
    bool nonblock;
    record_predicate_fn predicate;
    const void *predicate_ctx;
    uint64_t spin_seed;
} filter_arg_t;

typedef struct {
    byte_stream_t *in;
    string_list_t *out;
    bool nonblock;
} sink_arg_t;

typedef struct {
    byte_stream_t *stream;
    raw_array_t *array;
} collect_spec_t;

typedef struct {
    int loops;
    int *shared_value;
    spo_mutex_t *mutex;
} mutex_test_arg_t;

typedef struct {
    int threshold;
} int_threshold_t;

typedef struct {
    const char *text;
} text_threshold_t;

typedef struct {
    int person_id;
} person_match_t;

typedef struct {
    byte_stream_t *types_in;
    byte_stream_t *vedomosti_in;
    byte_stream_t *out;
    bool nonblock;
    bool output_date;
} join_types_ved_arg_t;

typedef struct {
    byte_stream_t *people_in;
    byte_stream_t *studies_in;
    byte_stream_t *students_in;
    byte_stream_t *out;
    bool nonblock;
    bool output_nzk;
} join_people_studies_students_arg_t;

typedef struct {
    byte_stream_t *people_in;
    byte_stream_t *out;
    bool nonblock;
} unique_birthdays_arg_t;

typedef struct {
    byte_stream_t *people_in;
    byte_stream_t *studies_in;
    byte_stream_t *out;
    bool nonblock;
} patronymic_count_arg_t;

typedef struct {
    byte_stream_t *people_in;
    byte_stream_t *students_in;
    byte_stream_t *out;
    bool nonblock;
    char reference_group[16];
    bool less_than_reference;
} group_age_arg_t;

typedef struct {
    byte_stream_t *people_in;
    byte_stream_t *studies_in;
    byte_stream_t *students_in;
    byte_stream_t *out;
    bool nonblock;
    bool expelled_after;
} enrollment_list_arg_t;

typedef struct {
    byte_stream_t *studies_in;
    byte_stream_t *vedomosti_in;
    byte_stream_t *out;
    bool nonblock;
} excellent_count_arg_t;

typedef struct {
    byte_stream_t *studies_in;
    byte_stream_t *students_in;
    byte_stream_t *out;
    bool nonblock;
} group_2011_arg_t;

typedef struct {
    byte_stream_t *people_in;
    byte_stream_t *students_in;
    byte_stream_t *out;
    bool nonblock;
} non_students_arg_t;

static runtime_t *g_runtime = NULL;

static void copy_text(char *dst, size_t dst_size, const char *src) {
    if (dst_size == 0u) {
        return;
    }
    if (src == NULL) {
        dst[0] = '\0';
        return;
    }
    snprintf(dst, dst_size, "%s", src);
}

static void trim_newline(char *line) {
    size_t len = strlen(line);
    while (len > 0u && (line[len - 1u] == '\n' || line[len - 1u] == '\r')) {
        line[len - 1u] = '\0';
        len--;
    }
}

static int split_csv_fields(char *line, char **fields, size_t expected) {
    size_t idx = 0u;
    char *cursor = line;
    if (expected == 0u) {
        return 0;
    }
    fields[idx++] = cursor;
    while (*cursor != '\0' && idx < expected) {
        if (*cursor == ',') {
            *cursor = '\0';
            fields[idx++] = cursor + 1;
        }
        cursor++;
    }
    return (int)idx;
}

static bool raw_array_init(raw_array_t *array, size_t item_size) {
    array->items = NULL;
    array->len = 0u;
    array->cap = 0u;
    array->item_size = item_size;
    return true;
}

static void raw_array_free(raw_array_t *array) {
    free(array->items);
    array->items = NULL;
    array->len = 0u;
    array->cap = 0u;
    array->item_size = 0u;
}

static bool raw_array_append(raw_array_t *array, const void *item) {
    void *new_items;
    size_t new_cap;
    if (array->len == array->cap) {
        new_cap = (array->cap == 0u) ? 8u : array->cap * 2u;
        new_items = realloc(array->items, new_cap * array->item_size);
        if (new_items == NULL) {
            return false;
        }
        array->items = new_items;
        array->cap = new_cap;
    }
    memcpy((unsigned char *)array->items + array->len * array->item_size, item, array->item_size);
    array->len++;
    return true;
}

static const void *raw_array_get(const raw_array_t *array, size_t index) {
    return (const unsigned char *)array->items + index * array->item_size;
}

static bool string_list_append(string_list_t *list, const char *text) {
    char **new_items;
    size_t new_cap;
    char *copy;
    size_t len = strlen(text);
    copy = (char *)malloc(len + 1u);
    if (copy == NULL) {
        return false;
    }
    memcpy(copy, text, len + 1u);
    if (list->len == list->cap) {
        new_cap = (list->cap == 0u) ? 8u : list->cap * 2u;
        new_items = (char **)realloc(list->items, new_cap * sizeof(char *));
        if (new_items == NULL) {
            free(copy);
            return false;
        }
        list->items = new_items;
        list->cap = new_cap;
    }
    list->items[list->len++] = copy;
    return true;
}

static int cmp_string_ptrs(const void *lhs, const void *rhs) {
    const char *const *a = (const char *const *)lhs;
    const char *const *b = (const char *const *)rhs;
    return strcmp(*a, *b);
}

static void string_list_sort(string_list_t *list) {
    if (list->len > 1u) {
        qsort(list->items, list->len, sizeof(char *), cmp_string_ptrs);
    }
}

static void string_list_free(string_list_t *list) {
    size_t i;
    for (i = 0u; i < list->len; ++i) {
        free(list->items[i]);
    }
    free(list->items);
    list->items = NULL;
    list->len = 0u;
    list->cap = 0u;
}

static void runtime_set_error(const char *message) {
    if (g_runtime != NULL && !g_runtime->failed) {
        g_runtime->failed = true;
        copy_text(g_runtime->error_message, sizeof(g_runtime->error_message), message);
    }
}

static void join_path(char *dst, size_t dst_size, const char *dir, const char *name) {
    snprintf(dst, dst_size, "%s/%s", dir, name);
}

static bool load_types_table(const char *path, types_table_t *table) {
    FILE *file = fopen(path, "r");
    char line[256];
    raw_array_t rows;
    if (file == NULL) {
        return false;
    }
    raw_array_init(&rows, sizeof(type_row_t));
    if (fgets(line, sizeof(line), file) == NULL) {
        fclose(file);
        return false;
    }
    while (fgets(line, sizeof(line), file) != NULL) {
        char *fields[2];
        type_row_t row;
        trim_newline(line);
        if (split_csv_fields(line, fields, 2u) != 2) {
            raw_array_free(&rows);
            fclose(file);
            return false;
        }
        row.id = atoi(fields[0]);
        copy_text(row.name, sizeof(row.name), fields[1]);
        if (!raw_array_append(&rows, &row)) {
            raw_array_free(&rows);
            fclose(file);
            return false;
        }
    }
    fclose(file);
    table->rows = (type_row_t *)rows.items;
    table->count = rows.len;
    return true;
}

static bool load_vedomosti_table(const char *path, vedomosti_table_t *table) {
    FILE *file = fopen(path, "r");
    char line[256];
    raw_array_t rows;
    if (file == NULL) {
        return false;
    }
    raw_array_init(&rows, sizeof(vedomost_row_t));
    if (fgets(line, sizeof(line), file) == NULL) {
        fclose(file);
        return false;
    }
    while (fgets(line, sizeof(line), file) != NULL) {
        char *fields[5];
        vedomost_row_t row;
        trim_newline(line);
        if (split_csv_fields(line, fields, 5u) != 5) {
            raw_array_free(&rows);
            fclose(file);
            return false;
        }
        row.id = atoi(fields[0]);
        row.type_id = atoi(fields[1]);
        row.person_id = atoi(fields[2]);
        copy_text(row.date, sizeof(row.date), fields[3]);
        row.mark = atoi(fields[4]);
        if (!raw_array_append(&rows, &row)) {
            raw_array_free(&rows);
            fclose(file);
            return false;
        }
    }
    fclose(file);
    table->rows = (vedomost_row_t *)rows.items;
    table->count = rows.len;
    return true;
}

static bool load_people_table(const char *path, people_table_t *table) {
    FILE *file = fopen(path, "r");
    char line[256];
    raw_array_t rows;
    if (file == NULL) {
        return false;
    }
    raw_array_init(&rows, sizeof(person_row_t));
    if (fgets(line, sizeof(line), file) == NULL) {
        fclose(file);
        return false;
    }
    while (fgets(line, sizeof(line), file) != NULL) {
        char *fields[5];
        person_row_t row;
        trim_newline(line);
        if (split_csv_fields(line, fields, 5u) != 5) {
            raw_array_free(&rows);
            fclose(file);
            return false;
        }
        row.id = atoi(fields[0]);
        copy_text(row.surname, sizeof(row.surname), fields[1]);
        copy_text(row.name, sizeof(row.name), fields[2]);
        copy_text(row.patronymic, sizeof(row.patronymic), fields[3]);
        copy_text(row.birthday, sizeof(row.birthday), fields[4]);
        if (!raw_array_append(&rows, &row)) {
            raw_array_free(&rows);
            fclose(file);
            return false;
        }
    }
    fclose(file);
    table->rows = (person_row_t *)rows.items;
    table->count = rows.len;
    return true;
}

static bool load_studies_table(const char *path, studies_table_t *table) {
    FILE *file = fopen(path, "r");
    char line[512];
    raw_array_t rows;
    if (file == NULL) {
        return false;
    }
    raw_array_init(&rows, sizeof(study_row_t));
    if (fgets(line, sizeof(line), file) == NULL) {
        fclose(file);
        return false;
    }
    while (fgets(line, sizeof(line), file) != NULL) {
        char *fields[7];
        study_row_t row;
        trim_newline(line);
        if (split_csv_fields(line, fields, 7u) != 7) {
            raw_array_free(&rows);
            fclose(file);
            return false;
        }
        row.person_id = atoi(fields[0]);
        copy_text(row.nzk, sizeof(row.nzk), fields[1]);
        copy_text(row.form, sizeof(row.form), fields[2]);
        copy_text(row.direction_code, sizeof(row.direction_code), fields[3]);
        copy_text(row.department, sizeof(row.department), fields[4]);
        copy_text(row.faculty, sizeof(row.faculty), fields[5]);
        row.course = atoi(fields[6]);
        if (!raw_array_append(&rows, &row)) {
            raw_array_free(&rows);
            fclose(file);
            return false;
        }
    }
    fclose(file);
    table->rows = (study_row_t *)rows.items;
    table->count = rows.len;
    return true;
}

static bool load_students_table(const char *path, students_table_t *table) {
    FILE *file = fopen(path, "r");
    char line[512];
    raw_array_t rows;
    if (file == NULL) {
        return false;
    }
    raw_array_init(&rows, sizeof(student_row_t));
    if (fgets(line, sizeof(line), file) == NULL) {
        fclose(file);
        return false;
    }
    while (fgets(line, sizeof(line), file) != NULL) {
        char *fields[7];
        student_row_t row;
        trim_newline(line);
        if (split_csv_fields(line, fields, 7u) != 7) {
            raw_array_free(&rows);
            fclose(file);
            return false;
        }
        row.person_id = atoi(fields[0]);
        copy_text(row.group_name, sizeof(row.group_name), fields[1]);
        copy_text(row.start_date, sizeof(row.start_date), fields[2]);
        copy_text(row.end_date, sizeof(row.end_date), fields[3]);
        copy_text(row.order_number, sizeof(row.order_number), fields[4]);
        copy_text(row.order_state, sizeof(row.order_state), fields[5]);
        copy_text(row.status, sizeof(row.status), fields[6]);
        if (!raw_array_append(&rows, &row)) {
            raw_array_free(&rows);
            fclose(file);
            return false;
        }
    }
    fclose(file);
    table->rows = (student_row_t *)rows.items;
    table->count = rows.len;
    return true;
}

static bool load_dataset(const char *dir, dataset_t *dataset) {
    char path[512];
    memset(dataset, 0, sizeof(*dataset));
    join_path(path, sizeof(path), dir, "types_vedomostei.csv");
    if (!load_types_table(path, &dataset->types)) {
        return false;
    }
    join_path(path, sizeof(path), dir, "vedomosti.csv");
    if (!load_vedomosti_table(path, &dataset->vedomosti)) {
        return false;
    }
    join_path(path, sizeof(path), dir, "people.csv");
    if (!load_people_table(path, &dataset->people)) {
        return false;
    }
    join_path(path, sizeof(path), dir, "studies.csv");
    if (!load_studies_table(path, &dataset->studies)) {
        return false;
    }
    join_path(path, sizeof(path), dir, "students.csv");
    if (!load_students_table(path, &dataset->students)) {
        return false;
    }
    return true;
}

static void free_dataset(dataset_t *dataset) {
    free(dataset->types.rows);
    free(dataset->vedomosti.rows);
    free(dataset->people.rows);
    free(dataset->studies.rows);
    free(dataset->students.rows);
    memset(dataset, 0, sizeof(*dataset));
}

static void runtime_wake_waiters(runtime_t *rt);
static void thread_yield_current(void);
static void thread_block_current(wait_item_t *items, size_t count, spo_mutex_t *mutex,
                                 bool wait_any);

static bool stream_init(byte_stream_t *stream, const char *name, size_t record_size,
                        size_t capacity_records) {
    memset(stream, 0, sizeof(*stream));
    copy_text(stream->name, sizeof(stream->name), name);
    stream->record_size = record_size;
    stream->capacity_records = capacity_records;
    stream->buffer = (unsigned char *)calloc(capacity_records, record_size);
    return stream->buffer != NULL;
}

static void stream_destroy(byte_stream_t *stream) {
    free(stream->buffer);
    memset(stream, 0, sizeof(*stream));
}

static bool stream_can_read(const byte_stream_t *stream) {
    return stream->count_records > 0u || stream->writer_closed;
}

static bool stream_can_write(const byte_stream_t *stream) {
    return !stream->writer_closed && stream->count_records < stream->capacity_records;
}

static stream_status_t stream_try_write_record(byte_stream_t *stream, const void *record) {
    unsigned char *dst;
    if (stream->writer_closed) {
        return STREAM_CLOSED;
    }
    if (stream->count_records >= stream->capacity_records) {
        return STREAM_WOULD_BLOCK;
    }
    dst = stream->buffer + (stream->tail * stream->record_size);
    memcpy(dst, record, stream->record_size);
    stream->tail = (stream->tail + 1u) % stream->capacity_records;
    stream->count_records++;
    return STREAM_OK;
}

static stream_status_t stream_try_read_record(byte_stream_t *stream, void *record) {
    unsigned char *src;
    if (stream->count_records == 0u) {
        return stream->writer_closed ? STREAM_EOF : STREAM_WOULD_BLOCK;
    }
    src = stream->buffer + (stream->head * stream->record_size);
    memcpy(record, src, stream->record_size);
    stream->head = (stream->head + 1u) % stream->capacity_records;
    stream->count_records--;
    return STREAM_OK;
}

static void stream_close_writer(byte_stream_t *stream) {
    stream->writer_closed = true;
}

static void mutex_init(spo_mutex_t *mutex, const char *name) {
    memset(mutex, 0, sizeof(*mutex));
    copy_text(mutex->name, sizeof(mutex->name), name);
    mutex->owner = -1;
}

static void runtime_timer_handler(int signo) {
    (void)signo;
    if (g_runtime != NULL) {
        g_runtime->tick_pending = 1;
    }
}

static void worker_spin(uint64_t seed) {
    volatile uint64_t acc = seed;
    size_t i;
    for (i = 0u; i < 2048u; ++i) {
        acc = spo_asm_add_u64(acc, (uint64_t)i + 1u);
        acc = spo_asm_double_u64(acc ^ UINT64_C(0x9e3779b97f4a7c15));
        acc = spo_asm_mix_u64(acc, acc >> 7u);
        if ((i & 63u) == 0u && g_runtime != NULL && g_runtime->tick_pending) {
            g_runtime->tick_pending = 0;
            thread_yield_current();
        }
    }
    (void)acc;
}

static bool wait_item_ready(const wait_item_t *item) {
    bool want_read = (item->flags & 0x1u) != 0u;
    bool want_write = (item->flags & 0x2u) != 0u;
    if (item->stream == NULL) {
        return false;
    }
    if (want_read && stream_can_read(item->stream)) {
        return true;
    }
    if (want_write && stream_can_write(item->stream)) {
        return true;
    }
    return false;
}

static bool thread_wait_satisfied(const user_thread_t *thread) {
    size_t i;
    if (thread->wait_mutex != NULL) {
        return thread->wait_mutex->owner < 0;
    }
    if (thread->wait_count == 0u) {
        return false;
    }
    if (thread->wait_any) {
        for (i = 0u; i < thread->wait_count; ++i) {
            if (wait_item_ready(&thread->wait_items[i])) {
                return true;
            }
        }
        return false;
    }
    for (i = 0u; i < thread->wait_count; ++i) {
        if (!wait_item_ready(&thread->wait_items[i])) {
            return false;
        }
    }
    return true;
}

static void runtime_wake_waiters(runtime_t *rt) {
    size_t i;
    for (i = 0u; i < rt->thread_count; ++i) {
        user_thread_t *thread = &rt->threads[i];
        if (thread->state == THREAD_BLOCKED && thread_wait_satisfied(thread)) {
            thread->state = THREAD_READY;
            thread->wait_count = 0u;
            thread->wait_mutex = NULL;
            thread->wait_any = false;
            rt->stats.wake_events++;
        }
    }
}

static void thread_exit_current(void) {
    runtime_t *rt = g_runtime;
    int current = rt->current;
    if (current < 0) {
        return;
    }
    rt->threads[(size_t)current].state = THREAD_FINISHED;
    rt->current = -1;
    swapcontext(&rt->threads[(size_t)current].context, &rt->scheduler_context);
}

static void thread_yield_current(void) {
    runtime_t *rt = g_runtime;
    int current = rt->current;
    if (rt == NULL || current < 0) {
        return;
    }
    rt->threads[(size_t)current].state = THREAD_READY;
    rt->stats.voluntary_yields++;
    rt->current = -1;
    swapcontext(&rt->threads[(size_t)current].context, &rt->scheduler_context);
}

static void thread_block_current(wait_item_t *items, size_t count, spo_mutex_t *mutex,
                                 bool wait_any) {
    runtime_t *rt = g_runtime;
    user_thread_t *thread;
    size_t i;
    if (rt == NULL || rt->current < 0) {
        return;
    }
    thread = &rt->threads[(size_t)rt->current];
    thread->state = THREAD_BLOCKED;
    thread->wait_count = count;
    thread->wait_mutex = mutex;
    thread->wait_any = wait_any;
    for (i = 0u; i < count && i < MAX_WAIT_ITEMS; ++i) {
        thread->wait_items[i] = items[i];
    }
    if (count > 1u && wait_any) {
        rt->stats.group_waits++;
    }
    rt->stats.block_events++;
    rt->current = -1;
    swapcontext(&thread->context, &rt->scheduler_context);
}

static void thread_wait_group(wait_item_t *items, size_t count) {
    if (count == 0u) {
        thread_yield_current();
        return;
    }
    thread_block_current(items, count, NULL, true);
}

static bool mutex_lock(spo_mutex_t *mutex) {
    if (g_runtime == NULL || g_runtime->current < 0) {
        return false;
    }
    while (mutex->owner >= 0 && mutex->owner != g_runtime->current) {
        g_runtime->stats.mutex_waits++;
        thread_block_current(NULL, 0u, mutex, false);
        runtime_wake_waiters(g_runtime);
    }
    mutex->owner = g_runtime->current;
    return true;
}

static void mutex_unlock(spo_mutex_t *mutex) {
    if (g_runtime != NULL && mutex->owner == g_runtime->current) {
        mutex->owner = -1;
        runtime_wake_waiters(g_runtime);
    }
}

static bool read_record_mode(byte_stream_t *stream, void *record, bool nonblock, bool *eof) {
    *eof = false;
    for (;;) {
        stream_status_t status = stream_try_read_record(stream, record);
        if (status == STREAM_OK) {
            runtime_wake_waiters(g_runtime);
            return true;
        }
        if (status == STREAM_EOF) {
            *eof = true;
            runtime_wake_waiters(g_runtime);
            return true;
        }
        if (!nonblock) {
            wait_item_t item;
            item.stream = stream;
            item.flags = 0x1u;
            thread_block_current(&item, 1u, NULL, true);
        } else {
            wait_item_t item;
            item.stream = stream;
            item.flags = 0x1u;
            thread_wait_group(&item, 1u);
        }
        runtime_wake_waiters(g_runtime);
    }
}

static bool write_record_mode(byte_stream_t *stream, const void *record, bool nonblock) {
    for (;;) {
        stream_status_t status = stream_try_write_record(stream, record);
        if (status == STREAM_OK) {
            runtime_wake_waiters(g_runtime);
            return true;
        }
        if (status == STREAM_CLOSED) {
            runtime_set_error("write to closed stream");
            return false;
        }
        if (!nonblock) {
            wait_item_t item;
            item.stream = stream;
            item.flags = 0x2u;
            thread_block_current(&item, 1u, NULL, true);
        } else {
            wait_item_t item;
            item.stream = stream;
            item.flags = 0x2u;
            thread_wait_group(&item, 1u);
        }
        runtime_wake_waiters(g_runtime);
    }
}

static bool emit_result_line(byte_stream_t *out, bool nonblock, const char *fmt, ...) {
    result_line_t line;
    va_list args;
    memset(&line, 0, sizeof(line));
    va_start(args, fmt);
    vsnprintf(line.text, sizeof(line.text), fmt, args);
    va_end(args);
    return write_record_mode(out, &line, nonblock);
}

static void thread_trampoline(uintptr_t index) {
    runtime_t *rt = g_runtime;
    user_thread_t *thread = &rt->threads[(size_t)index];
    thread->entry(thread->arg);
    thread_exit_current();
}

static bool runtime_init(runtime_t *rt, unsigned tick_us) {
    memset(rt, 0, sizeof(*rt));
    rt->tick_us = (tick_us == 0u) ? DEFAULT_TICK_US : tick_us;
    rt->current = -1;
    rt->rr_cursor = 0u;
    return true;
}

static bool runtime_create_thread(runtime_t *rt, const char *name, thread_entry_fn entry,
                                  void *arg) {
    user_thread_t *thread;
    size_t index = rt->thread_count;
    if (index >= MAX_THREADS) {
        rt->failed = true;
        copy_text(rt->error_message, sizeof(rt->error_message), "too many threads");
        return false;
    }
    thread = &rt->threads[index];
    memset(thread, 0, sizeof(*thread));
    thread->entry = entry;
    thread->arg = arg;
    thread->state = THREAD_READY;
    copy_text(thread->name, sizeof(thread->name), name);
    thread->stack = (unsigned char *)malloc(STACK_SIZE);
    if (thread->stack == NULL) {
        rt->failed = true;
        copy_text(rt->error_message, sizeof(rt->error_message), "thread stack allocation failed");
        return false;
    }
    if (getcontext(&thread->context) != 0) {
        free(thread->stack);
        thread->stack = NULL;
        rt->failed = true;
        copy_text(rt->error_message, sizeof(rt->error_message), "getcontext failed");
        return false;
    }
    thread->context.uc_link = &rt->scheduler_context;
    thread->context.uc_stack.ss_sp = thread->stack;
    thread->context.uc_stack.ss_size = STACK_SIZE;
    thread->context.uc_stack.ss_flags = 0;
    makecontext(&thread->context, (void (*)(void))thread_trampoline, 1, (uintptr_t)index);
    rt->thread_count++;
    return true;
}

static bool runtime_start_timer(runtime_t *rt) {
    struct sigaction action;
    struct itimerval timer;
    memset(&action, 0, sizeof(action));
    action.sa_handler = runtime_timer_handler;
    sigemptyset(&action.sa_mask);
    if (sigaction(SIGALRM, &action, &rt->old_action) != 0) {
        return false;
    }
    rt->action_installed = true;
    memset(&timer, 0, sizeof(timer));
    timer.it_interval.tv_sec = (time_t)(rt->tick_us / 1000000u);
    timer.it_interval.tv_usec = (suseconds_t)(rt->tick_us % 1000000u);
    timer.it_value = timer.it_interval;
    if (timer.it_value.tv_sec == 0 && timer.it_value.tv_usec == 0) {
        timer.it_value.tv_usec = (suseconds_t)DEFAULT_TICK_US;
        timer.it_interval.tv_usec = (suseconds_t)DEFAULT_TICK_US;
    }
    if (setitimer(ITIMER_REAL, &timer, &rt->old_timer) != 0) {
        sigaction(SIGALRM, &rt->old_action, NULL);
        rt->action_installed = false;
        return false;
    }
    rt->timer_installed = true;
    return true;
}

static void runtime_destroy(runtime_t *rt) {
    size_t i;
    if (rt->timer_installed) {
        setitimer(ITIMER_REAL, &rt->old_timer, NULL);
        rt->timer_installed = false;
    }
    if (rt->action_installed) {
        sigaction(SIGALRM, &rt->old_action, NULL);
        rt->action_installed = false;
    }
    for (i = 0u; i < rt->thread_count; ++i) {
        free(rt->threads[i].stack);
        rt->threads[i].stack = NULL;
    }
    if (g_runtime == rt) {
        g_runtime = NULL;
    }
}

static size_t runtime_live_thread_count(const runtime_t *rt) {
    size_t i;
    size_t count = 0u;
    for (i = 0u; i < rt->thread_count; ++i) {
        if (rt->threads[i].state != THREAD_FINISHED) {
            count++;
        }
    }
    return count;
}

static int runtime_pick_next(runtime_t *rt) {
    size_t offset;
    for (offset = 0u; offset < rt->thread_count; ++offset) {
        size_t idx = (rt->rr_cursor + 1u + offset) % rt->thread_count;
        if (rt->threads[idx].state == THREAD_READY) {
            rt->rr_cursor = idx;
            return (int)idx;
        }
    }
    return -1;
}

static bool runtime_run(runtime_t *rt) {
    g_runtime = rt;
    if (!runtime_start_timer(rt)) {
        runtime_set_error("failed to start SIGALRM timer");
        return false;
    }
    while (!rt->failed && runtime_live_thread_count(rt) > 0u) {
        int next;
        runtime_wake_waiters(rt);
        next = runtime_pick_next(rt);
        if (next < 0) {
            runtime_wake_waiters(rt);
            next = runtime_pick_next(rt);
            if (next < 0) {
                rt->deadlock = true;
                runtime_set_error("deadlock: no runnable threads");
                break;
            }
        }
        rt->current = next;
        rt->threads[(size_t)next].state = THREAD_RUNNING;
        rt->stats.context_switches++;
        if (swapcontext(&rt->scheduler_context, &rt->threads[(size_t)next].context) != 0) {
            runtime_set_error("swapcontext failed");
            rt->failed = true;
            break;
        }
        rt->current = -1;
    }
    return !rt->failed;
}

static int parse_ymd(const char *date, int *year, int *month, int *day) {
    if (date == NULL || date[0] == '\0') {
        return 0;
    }
    return sscanf(date, "%d-%d-%d", year, month, day);
}

static int calculate_age(const char *birthday) {
    int year = 0;
    int month = 0;
    int day = 0;
    int age;
    if (parse_ymd(birthday, &year, &month, &day) != 3) {
        return 0;
    }
    age = REFERENCE_YEAR - year;
    if (month > REFERENCE_MONTH || (month == REFERENCE_MONTH && day > REFERENCE_DAY)) {
        age--;
    }
    return age;
}

static bool date_before(const char *lhs, const char *rhs) {
    return strcmp(lhs, rhs) < 0;
}

static bool date_after(const char *lhs, const char *rhs) {
    return strcmp(lhs, rhs) > 0;
}

static bool date_not_after(const char *lhs, const char *rhs) {
    return strcmp(lhs, rhs) <= 0;
}

static bool date_not_before_or_empty(const char *lhs, const char *rhs) {
    if (lhs[0] == '\0') {
        return true;
    }
    return strcmp(lhs, rhs) >= 0;
}

static const person_row_t *find_person_row(const raw_array_t *people, int id) {
    size_t i;
    for (i = 0u; i < people->len; ++i) {
        const person_row_t *row = (const person_row_t *)raw_array_get(people, i);
        if (row->id == id) {
            return row;
        }
    }
    return NULL;
}

static const study_row_t *find_study_row(const raw_array_t *studies, int person_id) {
    size_t i;
    for (i = 0u; i < studies->len; ++i) {
        const study_row_t *row = (const study_row_t *)raw_array_get(studies, i);
        if (row->person_id == person_id) {
            return row;
        }
    }
    return NULL;
}

static const student_row_t *find_student_row(const raw_array_t *students, int person_id) {
    size_t i;
    for (i = 0u; i < students->len; ++i) {
        const student_row_t *row = (const student_row_t *)raw_array_get(students, i);
        if (row->person_id == person_id) {
            return row;
        }
    }
    return NULL;
}

static const type_row_t *find_type_row(const raw_array_t *types, int id) {
    size_t i;
    for (i = 0u; i < types->len; ++i) {
        const type_row_t *row = (const type_row_t *)raw_array_get(types, i);
        if (row->id == id) {
            return row;
        }
    }
    return NULL;
}

static bool collect_streams_sync(collect_spec_t *specs, size_t count) {
    size_t i;
    unsigned char buffer[MAX_RECORD_SIZE];
    for (i = 0u; i < count; ++i) {
        bool eof = false;
        if (specs[i].stream->record_size > sizeof(buffer)) {
            runtime_set_error("record size exceeds MAX_RECORD_SIZE");
            return false;
        }
        while (true) {
            if (!read_record_mode(specs[i].stream, buffer, false, &eof)) {
                return false;
            }
            if (eof) {
                break;
            }
            if (!raw_array_append(specs[i].array, buffer)) {
                runtime_set_error("collect_streams_sync: append failed");
                return false;
            }
            worker_spin(UINT64_C(0x100) + (uint64_t)i);
        }
    }
    return true;
}

static bool collect_streams_nonblock(collect_spec_t *specs, size_t count) {
    bool done[MAX_WAIT_ITEMS];
    unsigned char buffer[MAX_RECORD_SIZE];
    size_t i;
    memset(done, 0, sizeof(done));
    if (count > MAX_WAIT_ITEMS) {
        runtime_set_error("too many wait items");
        return false;
    }
    for (i = 0u; i < count; ++i) {
        if (specs[i].stream->record_size > sizeof(buffer)) {
            runtime_set_error("record size exceeds MAX_RECORD_SIZE");
            return false;
        }
    }
    while (true) {
        bool progress = false;
        wait_item_t items[MAX_WAIT_ITEMS];
        size_t item_count = 0u;
        for (i = 0u; i < count; ++i) {
            if (done[i]) {
                continue;
            }
            for (;;) {
                stream_status_t status = stream_try_read_record(specs[i].stream, buffer);
                if (status == STREAM_OK) {
                    if (!raw_array_append(specs[i].array, buffer)) {
                        runtime_set_error("collect_streams_nonblock: append failed");
                        return false;
                    }
                    runtime_wake_waiters(g_runtime);
                    progress = true;
                    worker_spin(UINT64_C(0x200) + (uint64_t)i);
                    continue;
                }
                if (status == STREAM_EOF) {
                    done[i] = true;
                }
                break;
            }
            if (!done[i]) {
                items[item_count].stream = specs[i].stream;
                items[item_count].flags = 0x1u;
                item_count++;
            }
        }
        if (item_count == 0u) {
            break;
        }
        if (!progress) {
            thread_wait_group(items, item_count);
            runtime_wake_waiters(g_runtime);
        }
    }
    return true;
}

static bool predicate_type_gt(const void *record, const void *ctx) {
    const type_row_t *row = (const type_row_t *)record;
    const int_threshold_t *arg = (const int_threshold_t *)ctx;
    return row->id > arg->threshold;
}

static bool predicate_type_lt(const void *record, const void *ctx) {
    const type_row_t *row = (const type_row_t *)record;
    const int_threshold_t *arg = (const int_threshold_t *)ctx;
    return row->id < arg->threshold;
}

static bool predicate_person_gt(const void *record, const void *ctx) {
    const vedomost_row_t *row = (const vedomost_row_t *)record;
    const int_threshold_t *arg = (const int_threshold_t *)ctx;
    return row->person_id > arg->threshold;
}

static bool predicate_person_impossible(const void *record, const void *ctx) {
    const vedomost_row_t *row = (const vedomost_row_t *)record;
    const person_match_t *arg = (const person_match_t *)ctx;
    return row->person_id == arg->person_id && row->person_id == 163249;
}

static bool predicate_person_exact(const void *record, const void *ctx) {
    const study_row_t *row = (const study_row_t *)record;
    const person_match_t *arg = (const person_match_t *)ctx;
    return row->person_id == arg->person_id;
}

static bool predicate_study_person_lt(const void *record, const void *ctx) {
    const study_row_t *row = (const study_row_t *)record;
    const int_threshold_t *arg = (const int_threshold_t *)ctx;
    return row->person_id < arg->threshold;
}

static bool predicate_patronymic_lt(const void *record, const void *ctx) {
    const person_row_t *row = (const person_row_t *)record;
    const text_threshold_t *arg = (const text_threshold_t *)ctx;
    return strcmp(row->patronymic, arg->text) < 0;
}

static bool predicate_name_gt(const void *record, const void *ctx) {
    const person_row_t *row = (const person_row_t *)record;
    const text_threshold_t *arg = (const text_threshold_t *)ctx;
    return strcmp(row->name, arg->text) > 0;
}

static bool predicate_student_start_before(const void *record, const void *ctx) {
    const student_row_t *row = (const student_row_t *)record;
    const text_threshold_t *arg = (const text_threshold_t *)ctx;
    return row->start_date[0] != '\0' && date_before(row->start_date, arg->text);
}

static void source_thread(void *arg) {
    source_arg_t *cfg = (source_arg_t *)arg;
    size_t i;
    for (i = 0u; i < cfg->count; ++i) {
        const void *record = (const unsigned char *)cfg->items + i * cfg->item_size;
        if (!write_record_mode(cfg->out, record, cfg->nonblock)) {
            return;
        }
        worker_spin(cfg->spin_seed + (uint64_t)i);
    }
    stream_close_writer(cfg->out);
    runtime_wake_waiters(g_runtime);
}

static void filter_thread(void *arg) {
    filter_arg_t *cfg = (filter_arg_t *)arg;
    unsigned char buffer[MAX_RECORD_SIZE];
    if (cfg->in->record_size > sizeof(buffer)) {
        runtime_set_error("filter_thread: record too large");
        return;
    }
    while (true) {
        bool eof = false;
        if (!read_record_mode(cfg->in, buffer, cfg->nonblock, &eof)) {
            return;
        }
        if (eof) {
            break;
        }
        if (cfg->predicate(buffer, cfg->predicate_ctx)) {
            if (!write_record_mode(cfg->out, buffer, cfg->nonblock)) {
                return;
            }
        }
        worker_spin(cfg->spin_seed);
    }
    stream_close_writer(cfg->out);
    runtime_wake_waiters(g_runtime);
}

static void sink_thread(void *arg) {
    sink_arg_t *cfg = (sink_arg_t *)arg;
    result_line_t line;
    while (true) {
        bool eof = false;
        if (!read_record_mode(cfg->in, &line, cfg->nonblock, &eof)) {
            return;
        }
        if (eof) {
            break;
        }
        if (!string_list_append(cfg->out, line.text)) {
            runtime_set_error("sink_thread: append failed");
            return;
        }
        worker_spin(UINT64_C(0x330));
    }
}

static void join_types_ved_thread(void *arg) {
    join_types_ved_arg_t *cfg = (join_types_ved_arg_t *)arg;
    raw_array_t types;
    raw_array_t veds;
    collect_spec_t specs[2];
    size_t i;
    raw_array_init(&types, sizeof(type_row_t));
    raw_array_init(&veds, sizeof(vedomost_row_t));
    specs[0].stream = cfg->types_in;
    specs[0].array = &types;
    specs[1].stream = cfg->vedomosti_in;
    specs[1].array = &veds;
    if (cfg->nonblock) {
        if (!collect_streams_nonblock(specs, 2u)) {
            goto cleanup;
        }
    } else {
        if (!collect_streams_sync(specs, 2u)) {
            goto cleanup;
        }
    }
    for (i = 0u; i < veds.len; ++i) {
        const vedomost_row_t *ved = (const vedomost_row_t *)raw_array_get(&veds, i);
        const type_row_t *type = find_type_row(&types, ved->type_id);
        if (cfg->output_date) {
            if (type != NULL) {
                if (!emit_result_line(cfg->out, cfg->nonblock, "%d,%s", type->id, ved->date)) {
                    goto cleanup;
                }
            } else {
                if (!emit_result_line(cfg->out, cfg->nonblock, "NULL,%s", ved->date)) {
                    goto cleanup;
                }
            }
        } else {
            if (type != NULL) {
                if (!emit_result_line(cfg->out, cfg->nonblock, "%d,%d", type->id, ved->id)) {
                    goto cleanup;
                }
            } else {
                if (!emit_result_line(cfg->out, cfg->nonblock, "NULL,%d", ved->id)) {
                    goto cleanup;
                }
            }
        }
        worker_spin(UINT64_C(0x401) + (uint64_t)i);
    }
cleanup:
    raw_array_free(&types);
    raw_array_free(&veds);
    stream_close_writer(cfg->out);
    runtime_wake_waiters(g_runtime);
}

static void join_people_studies_students_thread(void *arg) {
    join_people_studies_students_arg_t *cfg = (join_people_studies_students_arg_t *)arg;
    raw_array_t people;
    raw_array_t studies;
    raw_array_t students;
    collect_spec_t specs[3];
    size_t i;
    raw_array_init(&people, sizeof(person_row_t));
    raw_array_init(&studies, sizeof(study_row_t));
    raw_array_init(&students, sizeof(student_row_t));
    specs[0].stream = cfg->people_in;
    specs[0].array = &people;
    specs[1].stream = cfg->studies_in;
    specs[1].array = &studies;
    specs[2].stream = cfg->students_in;
    specs[2].array = &students;
    if (cfg->nonblock) {
        if (!collect_streams_nonblock(specs, 3u)) {
            goto cleanup;
        }
    } else {
        if (!collect_streams_sync(specs, 3u)) {
            goto cleanup;
        }
    }
    for (i = 0u; i < people.len; ++i) {
        const person_row_t *person = (const person_row_t *)raw_array_get(&people, i);
        const study_row_t *study = find_study_row(&studies, person->id);
        const student_row_t *student = find_student_row(&students, person->id);
        if (study == NULL || student == NULL) {
            continue;
        }
        if (cfg->output_nzk) {
            if (!emit_result_line(cfg->out, cfg->nonblock, "%s,%s,%s", person->surname,
                                  study->nzk, student->start_date)) {
                goto cleanup;
            }
        } else {
            if (!emit_result_line(cfg->out, cfg->nonblock, "%s,%d,%s", person->surname,
                                  study->person_id, student->group_name)) {
                goto cleanup;
            }
        }
        worker_spin(UINT64_C(0x500) + (uint64_t)i);
    }
cleanup:
    raw_array_free(&people);
    raw_array_free(&studies);
    raw_array_free(&students);
    stream_close_writer(cfg->out);
    runtime_wake_waiters(g_runtime);
}

static void unique_birthdays_thread(void *arg) {
    unique_birthdays_arg_t *cfg = (unique_birthdays_arg_t *)arg;
    raw_array_t people;
    collect_spec_t spec;
    size_t i;
    size_t unique = 0u;
    raw_array_init(&people, sizeof(person_row_t));
    spec.stream = cfg->people_in;
    spec.array = &people;
    if (cfg->nonblock) {
        if (!collect_streams_nonblock(&spec, 1u)) {
            raw_array_free(&people);
            return;
        }
    } else {
        if (!collect_streams_sync(&spec, 1u)) {
            raw_array_free(&people);
            return;
        }
    }
    for (i = 0u; i < people.len; ++i) {
        const person_row_t *person = (const person_row_t *)raw_array_get(&people, i);
        size_t j;
        bool seen = false;
        for (j = 0u; j < i; ++j) {
            const person_row_t *prev = (const person_row_t *)raw_array_get(&people, j);
            if (strcmp(prev->birthday, person->birthday) == 0) {
                seen = true;
                break;
            }
        }
        if (!seen) {
            unique++;
        }
    }
    emit_result_line(cfg->out, cfg->nonblock, "%zu", unique);
    raw_array_free(&people);
    stream_close_writer(cfg->out);
    runtime_wake_waiters(g_runtime);
}

typedef struct {
    char patronymic[32];
    int count;
} patronymic_bucket_t;

static void patronymic_count_thread(void *arg) {
    patronymic_count_arg_t *cfg = (patronymic_count_arg_t *)arg;
    raw_array_t people;
    raw_array_t studies;
    collect_spec_t specs[2];
    patronymic_bucket_t buckets[32];
    size_t bucket_count = 0u;
    size_t i;
    raw_array_init(&people, sizeof(person_row_t));
    raw_array_init(&studies, sizeof(study_row_t));
    specs[0].stream = cfg->people_in;
    specs[0].array = &people;
    specs[1].stream = cfg->studies_in;
    specs[1].array = &studies;
    if (cfg->nonblock) {
        if (!collect_streams_nonblock(specs, 2u)) {
            goto cleanup;
        }
    } else {
        if (!collect_streams_sync(specs, 2u)) {
            goto cleanup;
        }
    }
    memset(buckets, 0, sizeof(buckets));
    for (i = 0u; i < studies.len; ++i) {
        const study_row_t *study = (const study_row_t *)raw_array_get(&studies, i);
        const person_row_t *person;
        size_t j;
        bool found = false;
        if (strcmp(study->form, "заочная") != 0) {
            continue;
        }
        person = find_person_row(&people, study->person_id);
        if (person == NULL) {
            continue;
        }
        for (j = 0u; j < bucket_count; ++j) {
            if (strcmp(buckets[j].patronymic, person->patronymic) == 0) {
                buckets[j].count++;
                found = true;
                break;
            }
        }
        if (!found && bucket_count < sizeof(buckets) / sizeof(buckets[0])) {
            copy_text(buckets[bucket_count].patronymic, sizeof(buckets[bucket_count].patronymic),
                      person->patronymic);
            buckets[bucket_count].count = 1;
            bucket_count++;
        }
    }
    for (i = 0u; i < bucket_count; ++i) {
        if (buckets[i].count == 10) {
            emit_result_line(cfg->out, cfg->nonblock, "%s,%d", buckets[i].patronymic,
                             buckets[i].count);
        }
    }
cleanup:
    raw_array_free(&people);
    raw_array_free(&studies);
    stream_close_writer(cfg->out);
    runtime_wake_waiters(g_runtime);
}

typedef struct {
    char group_name[16];
    double total_age;
    int count;
} group_age_bucket_t;

static void group_age_thread(void *arg) {
    group_age_arg_t *cfg = (group_age_arg_t *)arg;
    raw_array_t people;
    raw_array_t students;
    collect_spec_t specs[2];
    group_age_bucket_t buckets[32];
    size_t bucket_count = 0u;
    double reference_value = 0.0;
    bool reference_set = false;
    size_t i;
    raw_array_init(&people, sizeof(person_row_t));
    raw_array_init(&students, sizeof(student_row_t));
    specs[0].stream = cfg->people_in;
    specs[0].array = &people;
    specs[1].stream = cfg->students_in;
    specs[1].array = &students;
    if (cfg->nonblock) {
        if (!collect_streams_nonblock(specs, 2u)) {
            goto cleanup;
        }
    } else {
        if (!collect_streams_sync(specs, 2u)) {
            goto cleanup;
        }
    }
    memset(buckets, 0, sizeof(buckets));
    for (i = 0u; i < students.len; ++i) {
        const student_row_t *student = (const student_row_t *)raw_array_get(&students, i);
        const person_row_t *person = find_person_row(&people, student->person_id);
        int age;
        size_t j;
        if (person == NULL) {
            continue;
        }
        age = calculate_age(person->birthday);
        if (strcmp(student->group_name, cfg->reference_group) == 0) {
            if (!reference_set || (double)age < reference_value) {
                reference_value = (double)age;
                reference_set = true;
            }
        }
        for (j = 0u; j < bucket_count; ++j) {
            if (strcmp(buckets[j].group_name, student->group_name) == 0) {
                buckets[j].total_age += (double)age;
                buckets[j].count++;
                break;
            }
        }
        if (j == bucket_count && bucket_count < sizeof(buckets) / sizeof(buckets[0])) {
            copy_text(buckets[bucket_count].group_name, sizeof(buckets[bucket_count].group_name),
                      student->group_name);
            buckets[bucket_count].total_age = (double)age;
            buckets[bucket_count].count = 1;
            bucket_count++;
        }
    }
    if (reference_set) {
        for (i = 0u; i < bucket_count; ++i) {
            double avg = buckets[i].total_age / (double)buckets[i].count;
            bool match = cfg->less_than_reference ? (avg < reference_value)
                                                  : (fabs(avg - reference_value) < 0.0001);
            if (match) {
                emit_result_line(cfg->out, cfg->nonblock, "%s,%.2f", buckets[i].group_name,
                                 avg);
            }
        }
    }
cleanup:
    raw_array_free(&people);
    raw_array_free(&students);
    stream_close_writer(cfg->out);
    runtime_wake_waiters(g_runtime);
}

static void enrollment_list_thread(void *arg) {
    enrollment_list_arg_t *cfg = (enrollment_list_arg_t *)arg;
    raw_array_t people;
    raw_array_t studies;
    raw_array_t students;
    collect_spec_t specs[3];
    size_t i;
    raw_array_init(&people, sizeof(person_row_t));
    raw_array_init(&studies, sizeof(study_row_t));
    raw_array_init(&students, sizeof(student_row_t));
    specs[0].stream = cfg->people_in;
    specs[0].array = &people;
    specs[1].stream = cfg->studies_in;
    specs[1].array = &studies;
    specs[2].stream = cfg->students_in;
    specs[2].array = &students;
    if (cfg->nonblock) {
        if (!collect_streams_nonblock(specs, 3u)) {
            goto cleanup;
        }
    } else {
        if (!collect_streams_sync(specs, 3u)) {
            goto cleanup;
        }
    }
    for (i = 0u; i < students.len; ++i) {
        const student_row_t *student = (const student_row_t *)raw_array_get(&students, i);
        const study_row_t *study = find_study_row(&studies, student->person_id);
        const person_row_t *person = find_person_row(&people, student->person_id);
        if (study == NULL || person == NULL) {
            continue;
        }
        if (strcmp(study->direction_code, "230101") != 0) {
            continue;
        }
        if (cfg->expelled_after) {
            bool correct_form = (strcmp(study->form, "очная") == 0 ||
                                 strcmp(study->form, "заочная") == 0);
            if (!correct_form || strcmp(student->status, "expelled") != 0 ||
                !date_after(student->end_date, "2012-09-01")) {
                continue;
            }
            if (!emit_result_line(cfg->out, cfg->nonblock, "%s,%d,%s,%s,%s,%s",
                                  student->group_name, person->id, person->surname,
                                  person->name, person->patronymic, student->order_number)) {
                goto cleanup;
            }
        } else {
            if (strcmp(study->form, "очная") != 0 || study->course != 1 ||
                !date_before(student->start_date, "2012-09-01")) {
                continue;
            }
            if (!emit_result_line(cfg->out, cfg->nonblock, "%s,%d,%s,%s,%s,%s,%s",
                                  student->group_name, person->id, person->surname,
                                  person->name, person->patronymic, student->order_number,
                                  student->order_state)) {
                goto cleanup;
            }
        }
        worker_spin(UINT64_C(0x700) + (uint64_t)i);
    }
cleanup:
    raw_array_free(&people);
    raw_array_free(&studies);
    raw_array_free(&students);
    stream_close_writer(cfg->out);
    runtime_wake_waiters(g_runtime);
}

static void excellent_count_thread(void *arg) {
    excellent_count_arg_t *cfg = (excellent_count_arg_t *)arg;
    raw_array_t studies;
    raw_array_t veds;
    collect_spec_t specs[2];
    size_t i;
    int count = 0;
    raw_array_init(&studies, sizeof(study_row_t));
    raw_array_init(&veds, sizeof(vedomost_row_t));
    specs[0].stream = cfg->studies_in;
    specs[0].array = &studies;
    specs[1].stream = cfg->vedomosti_in;
    specs[1].array = &veds;
    if (cfg->nonblock) {
        if (!collect_streams_nonblock(specs, 2u)) {
            goto cleanup;
        }
    } else {
        if (!collect_streams_sync(specs, 2u)) {
            goto cleanup;
        }
    }
    for (i = 0u; i < studies.len; ++i) {
        const study_row_t *study = (const study_row_t *)raw_array_get(&studies, i);
        size_t j;
        bool has_marks = false;
        bool all_five = true;
        if (strcmp(study->faculty, "ФКТИУ") != 0) {
            continue;
        }
        for (j = 0u; j < veds.len; ++j) {
            const vedomost_row_t *ved = (const vedomost_row_t *)raw_array_get(&veds, j);
            if (ved->person_id == study->person_id) {
                has_marks = true;
                if (ved->mark != 5) {
                    all_five = false;
                    break;
                }
            }
        }
        if (has_marks && all_five) {
            count++;
        }
    }
    emit_result_line(cfg->out, cfg->nonblock, "%d", count);
cleanup:
    raw_array_free(&studies);
    raw_array_free(&veds);
    stream_close_writer(cfg->out);
    runtime_wake_waiters(g_runtime);
}

typedef struct {
    char group_name[16];
    int count;
} group_count_bucket_t;

static void group_2011_thread(void *arg) {
    group_2011_arg_t *cfg = (group_2011_arg_t *)arg;
    raw_array_t studies;
    raw_array_t students;
    collect_spec_t specs[2];
    group_count_bucket_t buckets[32];
    size_t bucket_count = 0u;
    size_t i;
    raw_array_init(&studies, sizeof(study_row_t));
    raw_array_init(&students, sizeof(student_row_t));
    specs[0].stream = cfg->studies_in;
    specs[0].array = &studies;
    specs[1].stream = cfg->students_in;
    specs[1].array = &students;
    if (cfg->nonblock) {
        if (!collect_streams_nonblock(specs, 2u)) {
            goto cleanup;
        }
    } else {
        if (!collect_streams_sync(specs, 2u)) {
            goto cleanup;
        }
    }
    memset(buckets, 0, sizeof(buckets));
    for (i = 0u; i < students.len; ++i) {
        const student_row_t *student = (const student_row_t *)raw_array_get(&students, i);
        const study_row_t *study = find_study_row(&studies, student->person_id);
        size_t j;
        if (study == NULL) {
            continue;
        }
        if (strcmp(study->department, "Кафедра вычислительной техники") != 0) {
            continue;
        }
        if (!date_not_after(student->start_date, "2011-12-31") ||
            !date_not_before_or_empty(student->end_date, "2011-01-01")) {
            continue;
        }
        for (j = 0u; j < bucket_count; ++j) {
            if (strcmp(buckets[j].group_name, student->group_name) == 0) {
                buckets[j].count++;
                break;
            }
        }
        if (j == bucket_count && bucket_count < sizeof(buckets) / sizeof(buckets[0])) {
            copy_text(buckets[bucket_count].group_name, sizeof(buckets[bucket_count].group_name),
                      student->group_name);
            buckets[bucket_count].count = 1;
            bucket_count++;
        }
    }
    for (i = 0u; i < bucket_count; ++i) {
        if (buckets[i].count < 5) {
            emit_result_line(cfg->out, cfg->nonblock, "%s,%d", buckets[i].group_name,
                             buckets[i].count);
        }
    }
cleanup:
    raw_array_free(&studies);
    raw_array_free(&students);
    stream_close_writer(cfg->out);
    runtime_wake_waiters(g_runtime);
}

static void non_students_thread(void *arg) {
    non_students_arg_t *cfg = (non_students_arg_t *)arg;
    raw_array_t people;
    raw_array_t students;
    collect_spec_t specs[2];
    size_t i;
    raw_array_init(&people, sizeof(person_row_t));
    raw_array_init(&students, sizeof(student_row_t));
    specs[0].stream = cfg->people_in;
    specs[0].array = &people;
    specs[1].stream = cfg->students_in;
    specs[1].array = &students;
    if (cfg->nonblock) {
        if (!collect_streams_nonblock(specs, 2u)) {
            goto cleanup;
        }
    } else {
        if (!collect_streams_sync(specs, 2u)) {
            goto cleanup;
        }
    }
    for (i = 0u; i < people.len; ++i) {
        const person_row_t *person = (const person_row_t *)raw_array_get(&people, i);
        if (find_student_row(&students, person->id) == NULL) {
            emit_result_line(cfg->out, cfg->nonblock, "%d,%s,%s,%s", person->id,
                             person->surname, person->name, person->patronymic);
        }
    }
cleanup:
    raw_array_free(&people);
    raw_array_free(&students);
    stream_close_writer(cfg->out);
    runtime_wake_waiters(g_runtime);
}

static void mutex_test_thread(void *arg) {
    mutex_test_arg_t *cfg = (mutex_test_arg_t *)arg;
    int i;
    for (i = 0; i < cfg->loops; ++i) {
        if (!mutex_lock(cfg->mutex)) {
            runtime_set_error("mutex_lock failed");
            return;
        }
        (*cfg->shared_value)++;
        mutex_unlock(cfg->mutex);
        worker_spin(UINT64_C(0x880) + (uint64_t)i);
    }
}

static bool query_setup_stream(byte_stream_t *stream, const char *name, size_t record_size) {
    return stream_init(stream, name, record_size, DEFAULT_STREAM_CAPACITY);
}

static bool finish_query(runtime_t *rt, string_list_t *out, runtime_stats_t *stats_out) {
    bool ok = runtime_run(rt);
    string_list_sort(out);
    if (stats_out != NULL) {
        *stats_out = rt->stats;
    }
    if (!ok && rt->error_message[0] != '\0') {
        fprintf(stderr, "runtime error: %s\n", rt->error_message);
    }
    runtime_destroy(rt);
    return ok;
}

static bool run_variant48_q1(const dataset_t *dataset, unsigned tick_us, string_list_t *out,
                             runtime_stats_t *stats) {
    runtime_t rt;
    byte_stream_t types_src;
    byte_stream_t types_filtered;
    byte_stream_t veds_src;
    byte_stream_t veds_filtered;
    byte_stream_t result_stream;
    source_arg_t types_source_arg;
    source_arg_t veds_source_arg;
    filter_arg_t types_filter_arg;
    filter_arg_t veds_filter_arg;
    join_types_ved_arg_t join_arg;
    sink_arg_t sink_arg;
    int_threshold_t gt2 = {2};
    int_threshold_t gt153285 = {153285};
    if (!runtime_init(&rt, tick_us)) {
        return false;
    }
    if (!query_setup_stream(&types_src, "types_src", sizeof(type_row_t)) ||
        !query_setup_stream(&types_filtered, "types_filtered", sizeof(type_row_t)) ||
        !query_setup_stream(&veds_src, "veds_src", sizeof(vedomost_row_t)) ||
        !query_setup_stream(&veds_filtered, "veds_filtered", sizeof(vedomost_row_t)) ||
        !query_setup_stream(&result_stream, "q48_1_out", sizeof(result_line_t))) {
        return false;
    }
    types_source_arg.items = dataset->types.rows;
    types_source_arg.count = dataset->types.count;
    types_source_arg.item_size = sizeof(type_row_t);
    types_source_arg.out = &types_src;
    types_source_arg.nonblock = false;
    types_source_arg.spin_seed = UINT64_C(0x1010);
    veds_source_arg.items = dataset->vedomosti.rows;
    veds_source_arg.count = dataset->vedomosti.count;
    veds_source_arg.item_size = sizeof(vedomost_row_t);
    veds_source_arg.out = &veds_src;
    veds_source_arg.nonblock = false;
    veds_source_arg.spin_seed = UINT64_C(0x2020);
    types_filter_arg.in = &types_src;
    types_filter_arg.out = &types_filtered;
    types_filter_arg.nonblock = false;
    types_filter_arg.predicate = predicate_type_gt;
    types_filter_arg.predicate_ctx = &gt2;
    types_filter_arg.spin_seed = UINT64_C(0x3030);
    veds_filter_arg.in = &veds_src;
    veds_filter_arg.out = &veds_filtered;
    veds_filter_arg.nonblock = false;
    veds_filter_arg.predicate = predicate_person_gt;
    veds_filter_arg.predicate_ctx = &gt153285;
    veds_filter_arg.spin_seed = UINT64_C(0x4040);
    join_arg.types_in = &types_filtered;
    join_arg.vedomosti_in = &veds_filtered;
    join_arg.out = &result_stream;
    join_arg.nonblock = false;
    join_arg.output_date = false;
    sink_arg.in = &result_stream;
    sink_arg.out = out;
    sink_arg.nonblock = false;
    runtime_create_thread(&rt, "src_types", source_thread, &types_source_arg);
    runtime_create_thread(&rt, "flt_types", filter_thread, &types_filter_arg);
    runtime_create_thread(&rt, "src_veds", source_thread, &veds_source_arg);
    runtime_create_thread(&rt, "flt_veds", filter_thread, &veds_filter_arg);
    runtime_create_thread(&rt, "join_q48_1", join_types_ved_thread, &join_arg);
    runtime_create_thread(&rt, "sink_q48_1", sink_thread, &sink_arg);
    if (!finish_query(&rt, out, stats)) {
        return false;
    }
    stream_destroy(&types_src);
    stream_destroy(&types_filtered);
    stream_destroy(&veds_src);
    stream_destroy(&veds_filtered);
    stream_destroy(&result_stream);
    return true;
}

static bool run_variant78_q1(const dataset_t *dataset, unsigned tick_us, string_list_t *out,
                             runtime_stats_t *stats) {
    runtime_t rt;
    byte_stream_t types_src;
    byte_stream_t types_filtered;
    byte_stream_t veds_src;
    byte_stream_t veds_filtered;
    byte_stream_t result_stream;
    source_arg_t types_source_arg;
    source_arg_t veds_source_arg;
    filter_arg_t types_filter_arg;
    filter_arg_t veds_filter_arg;
    join_types_ved_arg_t join_arg;
    sink_arg_t sink_arg;
    int_threshold_t lt2 = {2};
    person_match_t impossible = {153285};
    if (!runtime_init(&rt, tick_us)) {
        return false;
    }
    if (!query_setup_stream(&types_src, "types_src78", sizeof(type_row_t)) ||
        !query_setup_stream(&types_filtered, "types_filtered78", sizeof(type_row_t)) ||
        !query_setup_stream(&veds_src, "veds_src78", sizeof(vedomost_row_t)) ||
        !query_setup_stream(&veds_filtered, "veds_filtered78", sizeof(vedomost_row_t)) ||
        !query_setup_stream(&result_stream, "q78_1_out", sizeof(result_line_t))) {
        return false;
    }
    types_source_arg.items = dataset->types.rows;
    types_source_arg.count = dataset->types.count;
    types_source_arg.item_size = sizeof(type_row_t);
    types_source_arg.out = &types_src;
    types_source_arg.nonblock = true;
    types_source_arg.spin_seed = UINT64_C(0x1111);
    veds_source_arg.items = dataset->vedomosti.rows;
    veds_source_arg.count = dataset->vedomosti.count;
    veds_source_arg.item_size = sizeof(vedomost_row_t);
    veds_source_arg.out = &veds_src;
    veds_source_arg.nonblock = true;
    veds_source_arg.spin_seed = UINT64_C(0x2222);
    types_filter_arg.in = &types_src;
    types_filter_arg.out = &types_filtered;
    types_filter_arg.nonblock = true;
    types_filter_arg.predicate = predicate_type_lt;
    types_filter_arg.predicate_ctx = &lt2;
    types_filter_arg.spin_seed = UINT64_C(0x3333);
    veds_filter_arg.in = &veds_src;
    veds_filter_arg.out = &veds_filtered;
    veds_filter_arg.nonblock = true;
    veds_filter_arg.predicate = predicate_person_impossible;
    veds_filter_arg.predicate_ctx = &impossible;
    veds_filter_arg.spin_seed = UINT64_C(0x4444);
    join_arg.types_in = &types_filtered;
    join_arg.vedomosti_in = &veds_filtered;
    join_arg.out = &result_stream;
    join_arg.nonblock = true;
    join_arg.output_date = true;
    sink_arg.in = &result_stream;
    sink_arg.out = out;
    sink_arg.nonblock = true;
    runtime_create_thread(&rt, "src_types78", source_thread, &types_source_arg);
    runtime_create_thread(&rt, "flt_types78", filter_thread, &types_filter_arg);
    runtime_create_thread(&rt, "src_veds78", source_thread, &veds_source_arg);
    runtime_create_thread(&rt, "flt_veds78", filter_thread, &veds_filter_arg);
    runtime_create_thread(&rt, "join_q78_1", join_types_ved_thread, &join_arg);
    runtime_create_thread(&rt, "sink_q78_1", sink_thread, &sink_arg);
    if (!finish_query(&rt, out, stats)) {
        return false;
    }
    stream_destroy(&types_src);
    stream_destroy(&types_filtered);
    stream_destroy(&veds_src);
    stream_destroy(&veds_filtered);
    stream_destroy(&result_stream);
    return true;
}

static bool run_variant48_q2(const dataset_t *dataset, unsigned tick_us, string_list_t *out,
                             runtime_stats_t *stats) {
    runtime_t rt;
    byte_stream_t people_src;
    byte_stream_t people_filtered;
    byte_stream_t studies_src;
    byte_stream_t studies_filtered;
    byte_stream_t students_src;
    byte_stream_t students_filtered;
    byte_stream_t result_stream;
    source_arg_t people_source_arg;
    source_arg_t studies_source_arg;
    source_arg_t students_source_arg;
    filter_arg_t people_filter_arg;
    filter_arg_t studies_filter_arg;
    filter_arg_t students_filter_arg;
    join_people_studies_students_arg_t join_arg;
    sink_arg_t sink_arg;
    text_threshold_t patro = {"Владимирович"};
    text_threshold_t before_2008 = {"2008-09-01"};
    int_threshold_t lt_112514 = {112514};
    if (!runtime_init(&rt, tick_us)) {
        return false;
    }
    if (!query_setup_stream(&people_src, "people_src48_2", sizeof(person_row_t)) ||
        !query_setup_stream(&people_filtered, "people_flt48_2", sizeof(person_row_t)) ||
        !query_setup_stream(&studies_src, "studies_src48_2", sizeof(study_row_t)) ||
        !query_setup_stream(&studies_filtered, "studies_flt48_2", sizeof(study_row_t)) ||
        !query_setup_stream(&students_src, "students_src48_2", sizeof(student_row_t)) ||
        !query_setup_stream(&students_filtered, "students_flt48_2", sizeof(student_row_t)) ||
        !query_setup_stream(&result_stream, "result48_2", sizeof(result_line_t))) {
        return false;
    }
    people_source_arg.items = dataset->people.rows;
    people_source_arg.count = dataset->people.count;
    people_source_arg.item_size = sizeof(person_row_t);
    people_source_arg.out = &people_src;
    people_source_arg.nonblock = false;
    people_source_arg.spin_seed = UINT64_C(0x4810);
    studies_source_arg.items = dataset->studies.rows;
    studies_source_arg.count = dataset->studies.count;
    studies_source_arg.item_size = sizeof(study_row_t);
    studies_source_arg.out = &studies_src;
    studies_source_arg.nonblock = false;
    studies_source_arg.spin_seed = UINT64_C(0x4820);
    students_source_arg.items = dataset->students.rows;
    students_source_arg.count = dataset->students.count;
    students_source_arg.item_size = sizeof(student_row_t);
    students_source_arg.out = &students_src;
    students_source_arg.nonblock = false;
    students_source_arg.spin_seed = UINT64_C(0x4830);
    people_filter_arg.in = &people_src;
    people_filter_arg.out = &people_filtered;
    people_filter_arg.nonblock = false;
    people_filter_arg.predicate = predicate_patronymic_lt;
    people_filter_arg.predicate_ctx = &patro;
    people_filter_arg.spin_seed = UINT64_C(0x4840);
    studies_filter_arg.in = &studies_src;
    studies_filter_arg.out = &studies_filtered;
    studies_filter_arg.nonblock = false;
    studies_filter_arg.predicate = predicate_study_person_lt;
    studies_filter_arg.predicate_ctx = &lt_112514;
    studies_filter_arg.spin_seed = UINT64_C(0x4850);
    students_filter_arg.in = &students_src;
    students_filter_arg.out = &students_filtered;
    students_filter_arg.nonblock = false;
    students_filter_arg.predicate = predicate_student_start_before;
    students_filter_arg.predicate_ctx = &before_2008;
    students_filter_arg.spin_seed = UINT64_C(0x4860);
    join_arg.people_in = &people_filtered;
    join_arg.studies_in = &studies_filtered;
    join_arg.students_in = &students_filtered;
    join_arg.out = &result_stream;
    join_arg.nonblock = false;
    join_arg.output_nzk = false;
    sink_arg.in = &result_stream;
    sink_arg.out = out;
    sink_arg.nonblock = false;
    runtime_create_thread(&rt, "src_people48_2", source_thread, &people_source_arg);
    runtime_create_thread(&rt, "src_studies48_2", source_thread, &studies_source_arg);
    runtime_create_thread(&rt, "src_students48_2", source_thread, &students_source_arg);
    runtime_create_thread(&rt, "flt_people48_2", filter_thread, &people_filter_arg);
    runtime_create_thread(&rt, "flt_studies48_2", filter_thread, &studies_filter_arg);
    runtime_create_thread(&rt, "flt_students48_2", filter_thread, &students_filter_arg);
    runtime_create_thread(&rt, "join48_2", join_people_studies_students_thread, &join_arg);
    runtime_create_thread(&rt, "sink48_2", sink_thread, &sink_arg);
    if (!finish_query(&rt, out, stats)) {
        return false;
    }
    stream_destroy(&people_src);
    stream_destroy(&people_filtered);
    stream_destroy(&studies_src);
    stream_destroy(&studies_filtered);
    stream_destroy(&students_src);
    stream_destroy(&students_filtered);
    stream_destroy(&result_stream);
    return true;
}

static bool run_variant78_q2(const dataset_t *dataset, unsigned tick_us, string_list_t *out,
                             runtime_stats_t *stats) {
    runtime_t rt;
    byte_stream_t people_src;
    byte_stream_t people_filtered;
    byte_stream_t studies_src;
    byte_stream_t studies_filtered;
    byte_stream_t students_src;
    byte_stream_t result_stream;
    source_arg_t people_source_arg;
    source_arg_t studies_source_arg;
    source_arg_t students_source_arg;
    filter_arg_t people_filter_arg;
    filter_arg_t studies_filter_arg;
    join_people_studies_students_arg_t join_arg;
    sink_arg_t sink_arg;
    text_threshold_t name_threshold = {"Александр"};
    person_match_t person_match = {113409};
    if (!runtime_init(&rt, tick_us)) {
        return false;
    }
    if (!query_setup_stream(&people_src, "people_src78_2", sizeof(person_row_t)) ||
        !query_setup_stream(&people_filtered, "people_flt78_2", sizeof(person_row_t)) ||
        !query_setup_stream(&studies_src, "studies_src78_2", sizeof(study_row_t)) ||
        !query_setup_stream(&studies_filtered, "studies_flt78_2", sizeof(study_row_t)) ||
        !query_setup_stream(&students_src, "students_src78_2", sizeof(student_row_t)) ||
        !query_setup_stream(&result_stream, "result78_2", sizeof(result_line_t))) {
        return false;
    }
    people_source_arg.items = dataset->people.rows;
    people_source_arg.count = dataset->people.count;
    people_source_arg.item_size = sizeof(person_row_t);
    people_source_arg.out = &people_src;
    people_source_arg.nonblock = true;
    people_source_arg.spin_seed = UINT64_C(0x7810);
    studies_source_arg.items = dataset->studies.rows;
    studies_source_arg.count = dataset->studies.count;
    studies_source_arg.item_size = sizeof(study_row_t);
    studies_source_arg.out = &studies_src;
    studies_source_arg.nonblock = true;
    studies_source_arg.spin_seed = UINT64_C(0x7820);
    students_source_arg.items = dataset->students.rows;
    students_source_arg.count = dataset->students.count;
    students_source_arg.item_size = sizeof(student_row_t);
    students_source_arg.out = &students_src;
    students_source_arg.nonblock = true;
    students_source_arg.spin_seed = UINT64_C(0x7830);
    people_filter_arg.in = &people_src;
    people_filter_arg.out = &people_filtered;
    people_filter_arg.nonblock = true;
    people_filter_arg.predicate = predicate_name_gt;
    people_filter_arg.predicate_ctx = &name_threshold;
    people_filter_arg.spin_seed = UINT64_C(0x7840);
    studies_filter_arg.in = &studies_src;
    studies_filter_arg.out = &studies_filtered;
    studies_filter_arg.nonblock = true;
    studies_filter_arg.predicate = predicate_person_exact;
    studies_filter_arg.predicate_ctx = &person_match;
    studies_filter_arg.spin_seed = UINT64_C(0x7850);
    join_arg.people_in = &people_filtered;
    join_arg.studies_in = &studies_filtered;
    join_arg.students_in = &students_src;
    join_arg.out = &result_stream;
    join_arg.nonblock = true;
    join_arg.output_nzk = true;
    sink_arg.in = &result_stream;
    sink_arg.out = out;
    sink_arg.nonblock = true;
    runtime_create_thread(&rt, "src_people78_2", source_thread, &people_source_arg);
    runtime_create_thread(&rt, "src_studies78_2", source_thread, &studies_source_arg);
    runtime_create_thread(&rt, "src_students78_2", source_thread, &students_source_arg);
    runtime_create_thread(&rt, "flt_people78_2", filter_thread, &people_filter_arg);
    runtime_create_thread(&rt, "flt_studies78_2", filter_thread, &studies_filter_arg);
    runtime_create_thread(&rt, "join78_2", join_people_studies_students_thread, &join_arg);
    runtime_create_thread(&rt, "sink78_2", sink_thread, &sink_arg);
    if (!finish_query(&rt, out, stats)) {
        return false;
    }
    stream_destroy(&people_src);
    stream_destroy(&people_filtered);
    stream_destroy(&studies_src);
    stream_destroy(&studies_filtered);
    stream_destroy(&students_src);
    stream_destroy(&result_stream);
    return true;
}

static bool run_single_input_query(const void *rows, size_t count, size_t row_size,
                                   unsigned tick_us, bool nonblock, thread_entry_fn worker,
                                   void *worker_arg, string_list_t *out, runtime_stats_t *stats,
                                   const char *src_name, const char *result_name) {
    runtime_t rt;
    byte_stream_t src;
    byte_stream_t result_stream;
    source_arg_t source_arg;
    sink_arg_t sink_arg;
    if (!runtime_init(&rt, tick_us)) {
        return false;
    }
    if (!query_setup_stream(&src, src_name, row_size) ||
        !query_setup_stream(&result_stream, result_name, sizeof(result_line_t))) {
        return false;
    }
    source_arg.items = rows;
    source_arg.count = count;
    source_arg.item_size = row_size;
    source_arg.out = &src;
    source_arg.nonblock = nonblock;
    source_arg.spin_seed = UINT64_C(0x9000);
    ((unique_birthdays_arg_t *)worker_arg)->people_in = &src;
    ((unique_birthdays_arg_t *)worker_arg)->out = &result_stream;
    ((unique_birthdays_arg_t *)worker_arg)->nonblock = nonblock;
    sink_arg.in = &result_stream;
    sink_arg.out = out;
    sink_arg.nonblock = nonblock;
    runtime_create_thread(&rt, "src_single", source_thread, &source_arg);
    runtime_create_thread(&rt, "worker_single", worker, worker_arg);
    runtime_create_thread(&rt, "sink_single", sink_thread, &sink_arg);
    if (!finish_query(&rt, out, stats)) {
        return false;
    }
    stream_destroy(&src);
    stream_destroy(&result_stream);
    return true;
}

static bool run_variant48_q3(const dataset_t *dataset, unsigned tick_us, string_list_t *out,
                             runtime_stats_t *stats) {
    unique_birthdays_arg_t arg;
    memset(&arg, 0, sizeof(arg));
    return run_single_input_query(dataset->people.rows, dataset->people.count, sizeof(person_row_t),
                                  tick_us, false, unique_birthdays_thread, &arg, out, stats,
                                  "people_q48_3", "result_q48_3");
}

static bool run_variant78_q3(const dataset_t *dataset, unsigned tick_us, string_list_t *out,
                             runtime_stats_t *stats) {
    unique_birthdays_arg_t arg;
    memset(&arg, 0, sizeof(arg));
    return run_single_input_query(dataset->people.rows, dataset->people.count, sizeof(person_row_t),
                                  tick_us, true, unique_birthdays_thread, &arg, out, stats,
                                  "people_q78_3", "result_q78_3");
}

static bool run_variant48_q4(const dataset_t *dataset, unsigned tick_us, string_list_t *out,
                             runtime_stats_t *stats) {
    runtime_t rt;
    byte_stream_t people_src;
    byte_stream_t studies_src;
    byte_stream_t result_stream;
    source_arg_t people_source_arg;
    source_arg_t studies_source_arg;
    patronymic_count_arg_t worker_arg;
    sink_arg_t sink_arg;
    if (!runtime_init(&rt, tick_us)) {
        return false;
    }
    if (!query_setup_stream(&people_src, "people_q48_4", sizeof(person_row_t)) ||
        !query_setup_stream(&studies_src, "studies_q48_4", sizeof(study_row_t)) ||
        !query_setup_stream(&result_stream, "result_q48_4", sizeof(result_line_t))) {
        return false;
    }
    people_source_arg.items = dataset->people.rows;
    people_source_arg.count = dataset->people.count;
    people_source_arg.item_size = sizeof(person_row_t);
    people_source_arg.out = &people_src;
    people_source_arg.nonblock = false;
    people_source_arg.spin_seed = UINT64_C(0x9410);
    studies_source_arg.items = dataset->studies.rows;
    studies_source_arg.count = dataset->studies.count;
    studies_source_arg.item_size = sizeof(study_row_t);
    studies_source_arg.out = &studies_src;
    studies_source_arg.nonblock = false;
    studies_source_arg.spin_seed = UINT64_C(0x9420);
    worker_arg.people_in = &people_src;
    worker_arg.studies_in = &studies_src;
    worker_arg.out = &result_stream;
    worker_arg.nonblock = false;
    sink_arg.in = &result_stream;
    sink_arg.out = out;
    sink_arg.nonblock = false;
    runtime_create_thread(&rt, "src_people48_4", source_thread, &people_source_arg);
    runtime_create_thread(&rt, "src_studies48_4", source_thread, &studies_source_arg);
    runtime_create_thread(&rt, "worker48_4", patronymic_count_thread, &worker_arg);
    runtime_create_thread(&rt, "sink48_4", sink_thread, &sink_arg);
    if (!finish_query(&rt, out, stats)) {
        return false;
    }
    stream_destroy(&people_src);
    stream_destroy(&studies_src);
    stream_destroy(&result_stream);
    return true;
}

static bool run_group_age_query(const dataset_t *dataset, unsigned tick_us, bool nonblock,
                                const char *reference_group, bool less_than_reference,
                                string_list_t *out, runtime_stats_t *stats,
                                const char *people_name, const char *students_name,
                                const char *result_name) {
    runtime_t rt;
    byte_stream_t people_src;
    byte_stream_t students_src;
    byte_stream_t result_stream;
    source_arg_t people_source_arg;
    source_arg_t students_source_arg;
    group_age_arg_t worker_arg;
    sink_arg_t sink_arg;
    if (!runtime_init(&rt, tick_us)) {
        return false;
    }
    if (!query_setup_stream(&people_src, people_name, sizeof(person_row_t)) ||
        !query_setup_stream(&students_src, students_name, sizeof(student_row_t)) ||
        !query_setup_stream(&result_stream, result_name, sizeof(result_line_t))) {
        return false;
    }
    people_source_arg.items = dataset->people.rows;
    people_source_arg.count = dataset->people.count;
    people_source_arg.item_size = sizeof(person_row_t);
    people_source_arg.out = &people_src;
    people_source_arg.nonblock = nonblock;
    people_source_arg.spin_seed = UINT64_C(0x9510);
    students_source_arg.items = dataset->students.rows;
    students_source_arg.count = dataset->students.count;
    students_source_arg.item_size = sizeof(student_row_t);
    students_source_arg.out = &students_src;
    students_source_arg.nonblock = nonblock;
    students_source_arg.spin_seed = UINT64_C(0x9520);
    worker_arg.people_in = &people_src;
    worker_arg.students_in = &students_src;
    worker_arg.out = &result_stream;
    worker_arg.nonblock = nonblock;
    copy_text(worker_arg.reference_group, sizeof(worker_arg.reference_group), reference_group);
    worker_arg.less_than_reference = less_than_reference;
    sink_arg.in = &result_stream;
    sink_arg.out = out;
    sink_arg.nonblock = nonblock;
    runtime_create_thread(&rt, "src_people_age", source_thread, &people_source_arg);
    runtime_create_thread(&rt, "src_students_age", source_thread, &students_source_arg);
    runtime_create_thread(&rt, "worker_age", group_age_thread, &worker_arg);
    runtime_create_thread(&rt, "sink_age", sink_thread, &sink_arg);
    if (!finish_query(&rt, out, stats)) {
        return false;
    }
    stream_destroy(&people_src);
    stream_destroy(&students_src);
    stream_destroy(&result_stream);
    return true;
}

static bool run_variant48_q5(const dataset_t *dataset, unsigned tick_us, string_list_t *out,
                             runtime_stats_t *stats) {
    return run_group_age_query(dataset, tick_us, false, "3100", false, out, stats,
                               "people_q48_5", "students_q48_5", "result_q48_5");
}

static bool run_variant78_q5(const dataset_t *dataset, unsigned tick_us, string_list_t *out,
                             runtime_stats_t *stats) {
    return run_group_age_query(dataset, tick_us, true, "1100", true, out, stats,
                               "people_q78_5", "students_q78_5", "result_q78_5");
}

static bool run_enrollment_query(const dataset_t *dataset, unsigned tick_us, bool nonblock,
                                 bool expelled_after, string_list_t *out, runtime_stats_t *stats,
                                 const char *tag) {
    runtime_t rt;
    byte_stream_t people_src;
    byte_stream_t studies_src;
    byte_stream_t students_src;
    byte_stream_t result_stream;
    source_arg_t people_source_arg;
    source_arg_t studies_source_arg;
    source_arg_t students_source_arg;
    enrollment_list_arg_t worker_arg;
    sink_arg_t sink_arg;
    if (!runtime_init(&rt, tick_us)) {
        return false;
    }
    if (!query_setup_stream(&people_src, "people_enroll", sizeof(person_row_t)) ||
        !query_setup_stream(&studies_src, "studies_enroll", sizeof(study_row_t)) ||
        !query_setup_stream(&students_src, "students_enroll", sizeof(student_row_t)) ||
        !query_setup_stream(&result_stream, tag, sizeof(result_line_t))) {
        return false;
    }
    people_source_arg.items = dataset->people.rows;
    people_source_arg.count = dataset->people.count;
    people_source_arg.item_size = sizeof(person_row_t);
    people_source_arg.out = &people_src;
    people_source_arg.nonblock = nonblock;
    people_source_arg.spin_seed = UINT64_C(0x9610);
    studies_source_arg.items = dataset->studies.rows;
    studies_source_arg.count = dataset->studies.count;
    studies_source_arg.item_size = sizeof(study_row_t);
    studies_source_arg.out = &studies_src;
    studies_source_arg.nonblock = nonblock;
    studies_source_arg.spin_seed = UINT64_C(0x9620);
    students_source_arg.items = dataset->students.rows;
    students_source_arg.count = dataset->students.count;
    students_source_arg.item_size = sizeof(student_row_t);
    students_source_arg.out = &students_src;
    students_source_arg.nonblock = nonblock;
    students_source_arg.spin_seed = UINT64_C(0x9630);
    worker_arg.people_in = &people_src;
    worker_arg.studies_in = &studies_src;
    worker_arg.students_in = &students_src;
    worker_arg.out = &result_stream;
    worker_arg.nonblock = nonblock;
    worker_arg.expelled_after = expelled_after;
    sink_arg.in = &result_stream;
    sink_arg.out = out;
    sink_arg.nonblock = nonblock;
    runtime_create_thread(&rt, "src_people_enroll", source_thread, &people_source_arg);
    runtime_create_thread(&rt, "src_studies_enroll", source_thread, &studies_source_arg);
    runtime_create_thread(&rt, "src_students_enroll", source_thread, &students_source_arg);
    runtime_create_thread(&rt, "worker_enroll", enrollment_list_thread, &worker_arg);
    runtime_create_thread(&rt, "sink_enroll", sink_thread, &sink_arg);
    if (!finish_query(&rt, out, stats)) {
        return false;
    }
    stream_destroy(&people_src);
    stream_destroy(&studies_src);
    stream_destroy(&students_src);
    stream_destroy(&result_stream);
    return true;
}

static bool run_variant48_q6(const dataset_t *dataset, unsigned tick_us, string_list_t *out,
                             runtime_stats_t *stats) {
    return run_enrollment_query(dataset, tick_us, false, false, out, stats, "result_q48_6");
}

static bool run_variant78_q6(const dataset_t *dataset, unsigned tick_us, string_list_t *out,
                             runtime_stats_t *stats) {
    return run_enrollment_query(dataset, tick_us, true, true, out, stats, "result_q78_6");
}

static bool run_variant48_q7(const dataset_t *dataset, unsigned tick_us, string_list_t *out,
                             runtime_stats_t *stats) {
    runtime_t rt;
    byte_stream_t studies_src;
    byte_stream_t veds_src;
    byte_stream_t result_stream;
    source_arg_t studies_source_arg;
    source_arg_t veds_source_arg;
    excellent_count_arg_t worker_arg;
    sink_arg_t sink_arg;
    if (!runtime_init(&rt, tick_us)) {
        return false;
    }
    if (!query_setup_stream(&studies_src, "studies_q48_7", sizeof(study_row_t)) ||
        !query_setup_stream(&veds_src, "veds_q48_7", sizeof(vedomost_row_t)) ||
        !query_setup_stream(&result_stream, "result_q48_7", sizeof(result_line_t))) {
        return false;
    }
    studies_source_arg.items = dataset->studies.rows;
    studies_source_arg.count = dataset->studies.count;
    studies_source_arg.item_size = sizeof(study_row_t);
    studies_source_arg.out = &studies_src;
    studies_source_arg.nonblock = false;
    studies_source_arg.spin_seed = UINT64_C(0x9710);
    veds_source_arg.items = dataset->vedomosti.rows;
    veds_source_arg.count = dataset->vedomosti.count;
    veds_source_arg.item_size = sizeof(vedomost_row_t);
    veds_source_arg.out = &veds_src;
    veds_source_arg.nonblock = false;
    veds_source_arg.spin_seed = UINT64_C(0x9720);
    worker_arg.studies_in = &studies_src;
    worker_arg.vedomosti_in = &veds_src;
    worker_arg.out = &result_stream;
    worker_arg.nonblock = false;
    sink_arg.in = &result_stream;
    sink_arg.out = out;
    sink_arg.nonblock = false;
    runtime_create_thread(&rt, "src_studies48_7", source_thread, &studies_source_arg);
    runtime_create_thread(&rt, "src_veds48_7", source_thread, &veds_source_arg);
    runtime_create_thread(&rt, "worker48_7", excellent_count_thread, &worker_arg);
    runtime_create_thread(&rt, "sink48_7", sink_thread, &sink_arg);
    if (!finish_query(&rt, out, stats)) {
        return false;
    }
    stream_destroy(&studies_src);
    stream_destroy(&veds_src);
    stream_destroy(&result_stream);
    return true;
}

static bool run_variant78_q4(const dataset_t *dataset, unsigned tick_us, string_list_t *out,
                             runtime_stats_t *stats) {
    runtime_t rt;
    byte_stream_t studies_src;
    byte_stream_t students_src;
    byte_stream_t result_stream;
    source_arg_t studies_source_arg;
    source_arg_t students_source_arg;
    group_2011_arg_t worker_arg;
    sink_arg_t sink_arg;
    if (!runtime_init(&rt, tick_us)) {
        return false;
    }
    if (!query_setup_stream(&studies_src, "studies_q78_4", sizeof(study_row_t)) ||
        !query_setup_stream(&students_src, "students_q78_4", sizeof(student_row_t)) ||
        !query_setup_stream(&result_stream, "result_q78_4", sizeof(result_line_t))) {
        return false;
    }
    studies_source_arg.items = dataset->studies.rows;
    studies_source_arg.count = dataset->studies.count;
    studies_source_arg.item_size = sizeof(study_row_t);
    studies_source_arg.out = &studies_src;
    studies_source_arg.nonblock = true;
    studies_source_arg.spin_seed = UINT64_C(0x9810);
    students_source_arg.items = dataset->students.rows;
    students_source_arg.count = dataset->students.count;
    students_source_arg.item_size = sizeof(student_row_t);
    students_source_arg.out = &students_src;
    students_source_arg.nonblock = true;
    students_source_arg.spin_seed = UINT64_C(0x9820);
    worker_arg.studies_in = &studies_src;
    worker_arg.students_in = &students_src;
    worker_arg.out = &result_stream;
    worker_arg.nonblock = true;
    sink_arg.in = &result_stream;
    sink_arg.out = out;
    sink_arg.nonblock = true;
    runtime_create_thread(&rt, "src_studies78_4", source_thread, &studies_source_arg);
    runtime_create_thread(&rt, "src_students78_4", source_thread, &students_source_arg);
    runtime_create_thread(&rt, "worker78_4", group_2011_thread, &worker_arg);
    runtime_create_thread(&rt, "sink78_4", sink_thread, &sink_arg);
    if (!finish_query(&rt, out, stats)) {
        return false;
    }
    stream_destroy(&studies_src);
    stream_destroy(&students_src);
    stream_destroy(&result_stream);
    return true;
}

static bool run_variant78_q7(const dataset_t *dataset, unsigned tick_us, string_list_t *out,
                             runtime_stats_t *stats) {
    runtime_t rt;
    byte_stream_t people_src;
    byte_stream_t students_src;
    byte_stream_t result_stream;
    source_arg_t people_source_arg;
    source_arg_t students_source_arg;
    non_students_arg_t worker_arg;
    sink_arg_t sink_arg;
    if (!runtime_init(&rt, tick_us)) {
        return false;
    }
    if (!query_setup_stream(&people_src, "people_q78_7", sizeof(person_row_t)) ||
        !query_setup_stream(&students_src, "students_q78_7", sizeof(student_row_t)) ||
        !query_setup_stream(&result_stream, "result_q78_7", sizeof(result_line_t))) {
        return false;
    }
    people_source_arg.items = dataset->people.rows;
    people_source_arg.count = dataset->people.count;
    people_source_arg.item_size = sizeof(person_row_t);
    people_source_arg.out = &people_src;
    people_source_arg.nonblock = true;
    people_source_arg.spin_seed = UINT64_C(0x9910);
    students_source_arg.items = dataset->students.rows;
    students_source_arg.count = dataset->students.count;
    students_source_arg.item_size = sizeof(student_row_t);
    students_source_arg.out = &students_src;
    students_source_arg.nonblock = true;
    students_source_arg.spin_seed = UINT64_C(0x9920);
    worker_arg.people_in = &people_src;
    worker_arg.students_in = &students_src;
    worker_arg.out = &result_stream;
    worker_arg.nonblock = true;
    sink_arg.in = &result_stream;
    sink_arg.out = out;
    sink_arg.nonblock = true;
    runtime_create_thread(&rt, "src_people78_7", source_thread, &people_source_arg);
    runtime_create_thread(&rt, "src_students78_7", source_thread, &students_source_arg);
    runtime_create_thread(&rt, "worker78_7", non_students_thread, &worker_arg);
    runtime_create_thread(&rt, "sink78_7", sink_thread, &sink_arg);
    if (!finish_query(&rt, out, stats)) {
        return false;
    }
    stream_destroy(&people_src);
    stream_destroy(&students_src);
    stream_destroy(&result_stream);
    return true;
}

static bool run_mutex_self_test(unsigned tick_us) {
    runtime_t rt;
    spo_mutex_t mutex;
    int value = 0;
    mutex_test_arg_t left;
    mutex_test_arg_t right;
    if (!runtime_init(&rt, tick_us)) {
        return false;
    }
    mutex_init(&mutex, "self_test_mutex");
    left.loops = 500;
    left.shared_value = &value;
    left.mutex = &mutex;
    right.loops = 500;
    right.shared_value = &value;
    right.mutex = &mutex;
    if (!runtime_create_thread(&rt, "mutex_left", mutex_test_thread, &left) ||
        !runtime_create_thread(&rt, "mutex_right", mutex_test_thread, &right)) {
        runtime_destroy(&rt);
        return false;
    }
    if (!runtime_run(&rt)) {
        runtime_destroy(&rt);
        return false;
    }
    runtime_destroy(&rt);
    return value == 1000;
}

typedef bool (*query_runner_fn)(const dataset_t *, unsigned, string_list_t *, runtime_stats_t *);

typedef struct {
    const char *label;
    int variant;
    query_runner_fn runner;
    const char *const *expected;
    size_t expected_count;
} query_case_t;

static const char *const EXPECT_48_1[] = {"3,2001", "3,2007", "4,2002", "5,2004", "NULL,2005"};
static const char *const EXPECT_48_2[] = {"Демин,100020,2200"};
static const char *const EXPECT_48_3[] = {"20"};
static const char *const EXPECT_48_4[] = {"Сергеевич,10"};
static const char *const EXPECT_48_5[] = {"2100,19.00"};
static const char *const EXPECT_48_6[] = {
    "1100,153285,Лисин,Павел,Олегович,PR-285,Зачислен",
    "1100,163249,Матвеев,Руслан,Петрович,PR-249,Зачислен",
    "2200,100020,Демин,Артем,Александрович,PR-020,Зачислен"};
static const char *const EXPECT_48_7[] = {"3"};
static const char *const EXPECT_78_1[] = {NULL};
static const char *const EXPECT_78_2[] = {"Зуев,OK409,2011-09-01"};
static const char *const EXPECT_78_3[] = {"20"};
static const char *const EXPECT_78_4[] = {"1100,2", "2100,1", "2200,1", "4100,3"};
static const char *const EXPECT_78_5[] = {"2100,19.00", "2200,18.00", "3100,20.00",
                                           "4100,21.67", "5100,20.00"};
static const char *const EXPECT_78_6[] = {"2100,170000,Новиков,Степан,Андреевич,PR-700"};
static const char *const EXPECT_78_7[] = {"180000,Осипова,Татьяна,Викторовна",
                                           "190000,Романов,Юрий,Владимирович"};

static const query_case_t QUERY_CASES[] = {
    {"48.1", 48, run_variant48_q1, EXPECT_48_1, 5u},
    {"48.2", 48, run_variant48_q2, EXPECT_48_2, 1u},
    {"48.3", 48, run_variant48_q3, EXPECT_48_3, 1u},
    {"48.4", 48, run_variant48_q4, EXPECT_48_4, 1u},
    {"48.5", 48, run_variant48_q5, EXPECT_48_5, 1u},
    {"48.6", 48, run_variant48_q6, EXPECT_48_6, 3u},
    {"48.7", 48, run_variant48_q7, EXPECT_48_7, 1u},
    {"78.1", 78, run_variant78_q1, EXPECT_78_1, 0u},
    {"78.2", 78, run_variant78_q2, EXPECT_78_2, 1u},
    {"78.3", 78, run_variant78_q3, EXPECT_78_3, 1u},
    {"78.4", 78, run_variant78_q4, EXPECT_78_4, 4u},
    {"78.5", 78, run_variant78_q5, EXPECT_78_5, 5u},
    {"78.6", 78, run_variant78_q6, EXPECT_78_6, 1u},
    {"78.7", 78, run_variant78_q7, EXPECT_78_7, 2u},
};

static void print_query_result(const char *label, const string_list_t *lines,
                               const runtime_stats_t *stats) {
    size_t i;
    printf("\n=== %s ===\n", label);
    printf("Context switches: %zu\n", stats->context_switches);
    printf("Voluntary yields: %zu\n", stats->voluntary_yields);
    printf("Blocked waits: %zu\n", stats->block_events);
    printf("Group waits: %zu\n", stats->group_waits);
    printf("Mutex waits: %zu\n", stats->mutex_waits);
    if (lines->len == 0u) {
        puts("(no rows)");
        return;
    }
    for (i = 0u; i < lines->len; ++i) {
        puts(lines->items[i]);
    }
}

static bool compare_expected(const string_list_t *lines, const char *const *expected,
                             size_t expected_count) {
    size_t i;
    if (lines->len != expected_count) {
        return false;
    }
    for (i = 0u; i < expected_count; ++i) {
        if (strcmp(lines->items[i], expected[i]) != 0) {
            return false;
        }
    }
    return true;
}

static bool run_self_test(const dataset_t *dataset, unsigned tick_us) {
    size_t i;
    if (!run_mutex_self_test(tick_us)) {
        puts("FAIL: mutex self-test");
        return false;
    }
    puts("PASS: mutex self-test");
    for (i = 0u; i < sizeof(QUERY_CASES) / sizeof(QUERY_CASES[0]); ++i) {
        string_list_t lines = {0};
        runtime_stats_t stats = {0};
        if (!QUERY_CASES[i].runner(dataset, tick_us, &lines, &stats)) {
            printf("FAIL: query %s runtime error\n", QUERY_CASES[i].label);
            string_list_free(&lines);
            return false;
        }
        if (!compare_expected(&lines, QUERY_CASES[i].expected, QUERY_CASES[i].expected_count)) {
            printf("FAIL: query %s unexpected output\n", QUERY_CASES[i].label);
            print_query_result(QUERY_CASES[i].label, &lines, &stats);
            string_list_free(&lines);
            return false;
        }
        printf("PASS: query %s\n", QUERY_CASES[i].label);
        string_list_free(&lines);
    }
    return true;
}

static void print_usage(const char *program) {
    printf("Usage: %s [--data-dir DIR] [--tick-us N] [--variant 18|48|78|all] [--self-test]\n",
           program);
}

static void print_variant_banner(int variant) {
    if (variant == 48) {
        puts("SPO task 2, variant 18: DBIS selection 48 in synchronous mode.");
    } else if (variant == 78) {
        puts("SPO task 2, variant 18: DBIS selection 78 in nonblocking mode.");
    } else {
        puts("SPO task 2, variant 18: running DBIS selections 48 (synchronous) and 78 (nonblocking).");
    }
}

int main(int argc, char **argv) {
    const char *data_dir = "data";
    unsigned tick_us = DEFAULT_TICK_US;
    int variant = 0;
    bool self_test = false;
    dataset_t dataset;
    size_t i;
    memset(&dataset, 0, sizeof(dataset));
    for (i = 1u; i < (size_t)argc; ++i) {
        if (strcmp(argv[i], "--data-dir") == 0 && i + 1u < (size_t)argc) {
            data_dir = argv[++i];
        } else if (strcmp(argv[i], "--tick-us") == 0 && i + 1u < (size_t)argc) {
            tick_us = (unsigned)strtoul(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--variant") == 0 && i + 1u < (size_t)argc) {
            ++i;
            if (strcmp(argv[i], "48") == 0) {
                variant = 48;
            } else if (strcmp(argv[i], "78") == 0) {
                variant = 78;
            } else if (strcmp(argv[i], "18") == 0 || strcmp(argv[i], "all") == 0) {
                variant = 0;
            } else {
                print_usage(argv[0]);
                return 1;
            }
        } else if (strcmp(argv[i], "--self-test") == 0) {
            self_test = true;
        } else if (strcmp(argv[i], "--help") == 0) {
            print_usage(argv[0]);
            return 0;
        } else {
            print_usage(argv[0]);
            return 1;
        }
    }
    if (!load_dataset(data_dir, &dataset)) {
        fprintf(stderr, "Failed to load dataset from %s\n", data_dir);
        return 2;
    }
    if (self_test) {
        bool ok = run_self_test(&dataset, tick_us);
        free_dataset(&dataset);
        return ok ? 0 : 3;
    }
    print_variant_banner(variant);
    for (i = 0u; i < sizeof(QUERY_CASES) / sizeof(QUERY_CASES[0]); ++i) {
        string_list_t lines = {0};
        runtime_stats_t stats = {0};
        if (variant != 0 && QUERY_CASES[i].variant != variant) {
            continue;
        }
        if (!QUERY_CASES[i].runner(&dataset, tick_us, &lines, &stats)) {
            fprintf(stderr, "Query %s failed\n", QUERY_CASES[i].label);
            string_list_free(&lines);
            free_dataset(&dataset);
            return 4;
        }
        print_query_result(QUERY_CASES[i].label, &lines, &stats);
        string_list_free(&lines);
    }
    free_dataset(&dataset);
    return 0;
}

#endif
