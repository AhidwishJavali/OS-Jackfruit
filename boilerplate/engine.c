#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE          (1024 * 1024)
#define CONTAINER_ID_LEN    32
#define CONTROL_PATH        "/tmp/mini_runtime.sock"
#define LOG_DIR             "logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN   256
#define LOG_CHUNK_SIZE      4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT  (40UL << 20)
#define DEFAULT_HARD_LIMIT  (64UL << 20)

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    int stop_requested;
    char log_path[PATH_MAX];
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

/* per-container producer thread argument */
typedef struct {
    int pipe_read_fd;
    char container_id[CONTAINER_ID_LEN];
    bounded_buffer_t *buffer;
} producer_arg_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

static supervisor_ctx_t *g_ctx = NULL;

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag, const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }
    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }
    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req, int argc,
                                char *argv[], int start_index)
{
    int i;
    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }
        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }
        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }
        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr, "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }
        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }
    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING: return "starting";
    case CONTAINER_RUNNING:  return "running";
    case CONTAINER_STOPPED:  return "stopped";
    case CONTAINER_KILLED:   return "killed";
    case CONTAINER_EXITED:   return "exited";
    default:                 return "unknown";
    }
}

static int bounded_buffer_init(bounded_buffer_t *buf)
{
    int rc;
    memset(buf, 0, sizeof(*buf));

    rc = pthread_mutex_init(&buf->mutex, NULL);
    if (rc != 0)
        return rc;
    rc = pthread_cond_init(&buf->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buf->mutex);
        return rc;
    }
    rc = pthread_cond_init(&buf->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buf->not_empty);
        pthread_mutex_destroy(&buf->mutex);
        return rc;
    }
    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buf)
{
    pthread_cond_destroy(&buf->not_full);
    pthread_cond_destroy(&buf->not_empty);
    pthread_mutex_destroy(&buf->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buf)
{
    pthread_mutex_lock(&buf->mutex);
    buf->shutting_down = 1;
    pthread_cond_broadcast(&buf->not_empty);
    pthread_cond_broadcast(&buf->not_full);
    pthread_mutex_unlock(&buf->mutex);
}

int bounded_buffer_push(bounded_buffer_t *buf, const log_item_t *item)
{
    pthread_mutex_lock(&buf->mutex);

    while (buf->count == LOG_BUFFER_CAPACITY && !buf->shutting_down)
        pthread_cond_wait(&buf->not_full, &buf->mutex);

    if (buf->shutting_down) {
        pthread_mutex_unlock(&buf->mutex);
        return -1;
    }

    buf->items[buf->tail] = *item;
    buf->tail = (buf->tail + 1) % LOG_BUFFER_CAPACITY;
    buf->count++;

    pthread_cond_signal(&buf->not_empty);
    pthread_mutex_unlock(&buf->mutex);
    return 0;
}

int bounded_buffer_pop(bounded_buffer_t *buf, log_item_t *item)
{
    pthread_mutex_lock(&buf->mutex);

    while (buf->count == 0 && !buf->shutting_down)
        pthread_cond_wait(&buf->not_empty, &buf->mutex);

    if (buf->count == 0 && buf->shutting_down) {
        pthread_mutex_unlock(&buf->mutex);
        return -1;
    }

    *item = buf->items[buf->head];
    buf->head = (buf->head + 1) % LOG_BUFFER_CAPACITY;
    buf->count--;

    pthread_cond_signal(&buf->not_full);
    pthread_mutex_unlock(&buf->mutex);
    return 0;
}

/*
 * logging_thread: single consumer thread shared across all containers.
 * Pops log chunks from the bounded buffer and appends them to the
 * correct per-container log file, identified by container_id in the item.
 * Exits once shutdown is signalled and the buffer is fully drained.
 */
void *logging_thread(void *arg)
{
    bounded_buffer_t *buf = (bounded_buffer_t *)arg;
    log_item_t item;

    while (bounded_buffer_pop(buf, &item) == 0) {
        char log_path[PATH_MAX];
        snprintf(log_path, sizeof(log_path), "%s/%s.log",
                 LOG_DIR, item.container_id);

        FILE *f = fopen(log_path, "a");
        if (!f) {
            perror("logging_thread: fopen");
            continue;
        }
        fwrite(item.data, 1, item.length, f);
        fclose(f);
    }

    return NULL;
}

/*
 * producer_thread: one per container. Reads from the pipe connected to
 * the container's stdout/stderr and pushes chunks into the shared
 * bounded buffer. Exits cleanly on EOF (container exited).
 */
static void *producer_thread(void *arg)
{
    producer_arg_t *parg = (producer_arg_t *)arg;
    log_item_t item;
    ssize_t n;

    fprintf(stderr, "[producer] started for container '%s'\n",
            parg->container_id);

    while (1) {
        memset(&item, 0, sizeof(item));
        strncpy(item.container_id, parg->container_id,
                sizeof(item.container_id) - 1);

        n = read(parg->pipe_read_fd, item.data, sizeof(item.data));
        if (n <= 0)
            break;

        item.length = (size_t)n;

        if (bounded_buffer_push(parg->buffer, &item) != 0)
            break;
    }

    fprintf(stderr, "[producer] EOF for container '%s', exiting\n",
            parg->container_id);

    close(parg->pipe_read_fd);
    free(parg);
    return NULL;
}

static void sigchld_handler(int sig)
{
    (void)sig;
    int saved_errno = errno;
    int status;
    pid_t pid;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        if (!g_ctx)
            continue;

        pthread_mutex_lock(&g_ctx->metadata_lock);
        container_record_t *rec = g_ctx->containers;
        while (rec) {
            if (rec->host_pid == pid) {
                if (WIFEXITED(status)) {
                    rec->exit_code   = WEXITSTATUS(status);
                    rec->exit_signal = 0;
                    rec->state       = CONTAINER_EXITED;
                } else if (WIFSIGNALED(status)) {
                    rec->exit_signal = WTERMSIG(status);
                    rec->exit_code   = 128 + rec->exit_signal;
                    if (rec->stop_requested)
                        rec->state = CONTAINER_STOPPED;
                    else if (rec->exit_signal == SIGKILL)
                        rec->state = CONTAINER_KILLED;
                    else
                        rec->state = CONTAINER_STOPPED;
                }
                fprintf(stderr, "[supervisor] container '%s' pid=%d state=%s\n",
                        rec->id, pid, state_to_string(rec->state));
                break;
            }
            rec = rec->next;
        }
        pthread_mutex_unlock(&g_ctx->metadata_lock);
    }

    errno = saved_errno;
}

static void sigterm_handler(int sig)
{
    (void)sig;
    if (g_ctx)
        g_ctx->should_stop = 1;
}

int register_with_monitor(int monitor_fd, const char *container_id,
                          pid_t host_pid, unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid              = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;
    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id,
                            pid_t host_pid)
{
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;
    return 0;
}

int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    if (dup2(cfg->log_write_fd, STDOUT_FILENO) < 0 ||
        dup2(cfg->log_write_fd, STDERR_FILENO) < 0) {
        perror("dup2");
        return 1;
    }
    close(cfg->log_write_fd);

    if (chroot(cfg->rootfs) != 0 || chdir("/") != 0) {
        perror("chroot/chdir");
        return 1;
    }

    if (mount("proc", "/proc", "proc", 0, NULL) != 0)
        perror("mount /proc");

    if (cfg->nice_value != 0) {
        errno = 0;
        if (nice(cfg->nice_value) == -1 && errno != 0)
            perror("nice");
    }

    char *exec_argv[] = { cfg->command, NULL };
    execv(cfg->command, exec_argv);
    perror("execv");
    return 1;
}

static container_record_t *spawn_container(supervisor_ctx_t *ctx,
                                           const control_request_t *req)
{
    int pipe_fds[2];
    char *stack;
    child_config_t *cfg;
    container_record_t *rec;
    char log_path[PATH_MAX];
    pid_t pid;

    if (pipe(pipe_fds) != 0) {
        perror("pipe");
        return NULL;
    }

    stack = malloc(STACK_SIZE);
    if (!stack) {
        perror("malloc");
        close(pipe_fds[0]);
        close(pipe_fds[1]);
        return NULL;
    }

    cfg = malloc(sizeof(child_config_t));
    if (!cfg) {
        free(stack);
        close(pipe_fds[0]);
        close(pipe_fds[1]);
        return NULL;
    }
    memset(cfg, 0, sizeof(*cfg));
    strncpy(cfg->id,      req->container_id, sizeof(cfg->id) - 1);
    strncpy(cfg->rootfs,  req->rootfs,       sizeof(cfg->rootfs) - 1);
    strncpy(cfg->command, req->command,      sizeof(cfg->command) - 1);
    cfg->nice_value   = req->nice_value;
    cfg->log_write_fd = pipe_fds[1];

    char cwd[PATH_MAX];
if (getcwd(cwd, sizeof(cwd)) == NULL)
    strncpy(cwd, ".", sizeof(cwd) - 1);
snprintf(log_path, sizeof(log_path), "%s/%s/%s.log", cwd, LOG_DIR, req->container_id);

    pid = clone(child_fn, stack + STACK_SIZE,
                CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD,
                cfg);

    close(pipe_fds[1]);

    if (pid < 0) {
        perror("clone");
        free(stack);
        free(cfg);
        close(pipe_fds[0]);
        return NULL;
    }

    free(stack);

    rec = malloc(sizeof(container_record_t));
    if (!rec) {
        free(cfg);
        close(pipe_fds[0]);
        kill(pid, SIGKILL);
        return NULL;
    }
    memset(rec, 0, sizeof(*rec));
    strncpy(rec->id, req->container_id, sizeof(rec->id) - 1);
    rec->host_pid         = pid;
    rec->started_at       = time(NULL);
    rec->state            = CONTAINER_RUNNING;
    rec->soft_limit_bytes = req->soft_limit_bytes;
    rec->hard_limit_bytes = req->hard_limit_bytes;
    rec->stop_requested   = 0;
    strncpy(rec->log_path, log_path, sizeof(rec->log_path) - 1);

    if (ctx->monitor_fd >= 0) {
        if (register_with_monitor(ctx->monitor_fd, req->container_id, pid,
                                  req->soft_limit_bytes,
                                  req->hard_limit_bytes) != 0)
            fprintf(stderr, "[supervisor] warning: monitor registration failed for '%s'\n",
                    req->container_id);
    }

    pthread_mutex_lock(&ctx->metadata_lock);
    rec->next       = ctx->containers;
    ctx->containers = rec;
    pthread_mutex_unlock(&ctx->metadata_lock);

    fprintf(stderr, "[supervisor] started container '%s' pid=%d log=%s\n",
            req->container_id, pid, log_path);

    /* spin up a producer thread to drain the container's pipe into the buffer */
    producer_arg_t *parg = malloc(sizeof(producer_arg_t));
    if (!parg) {
        close(pipe_fds[0]);
    } else {
        parg->pipe_read_fd = pipe_fds[0];
        parg->buffer       = &ctx->log_buffer;
        strncpy(parg->container_id, req->container_id,
                sizeof(parg->container_id) - 1);

        pthread_t tid;
        if (pthread_create(&tid, NULL, producer_thread, parg) != 0) {
            perror("pthread_create producer");
            close(pipe_fds[0]);
            free(parg);
        } else {
            pthread_detach(tid);
        }
    }

    free(cfg);
    return rec;
}

static void handle_ps(supervisor_ctx_t *ctx, control_response_t *resp)
{
    char line[256];
    memset(resp, 0, sizeof(*resp));

    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *rec = ctx->containers;

    if (!rec) {
        snprintf(resp->message, sizeof(resp->message), "no containers\n");
        resp->status = 0;
        pthread_mutex_unlock(&ctx->metadata_lock);
        return;
    }

    while (rec) {
        char tsbuf[32];
        struct tm *tm_info = localtime(&rec->started_at);
        strftime(tsbuf, sizeof(tsbuf), "%H:%M:%S", tm_info);

        snprintf(line, sizeof(line),
                 "%-12s  pid=%-6d  state=%-8s  soft=%luMB  hard=%luMB  started=%s\n",
                 rec->id, rec->host_pid, state_to_string(rec->state),
                 rec->soft_limit_bytes >> 20, rec->hard_limit_bytes >> 20,
                 tsbuf);

        strncat(resp->message, line,
                sizeof(resp->message) - strlen(resp->message) - 1);
        rec = rec->next;
    }
    resp->status = 0;
    pthread_mutex_unlock(&ctx->metadata_lock);
}

static void handle_logs(supervisor_ctx_t *ctx, const control_request_t *req,
                        control_response_t *resp)
{
    memset(resp, 0, sizeof(*resp));

    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *rec = ctx->containers;
    while (rec) {
        if (strncmp(rec->id, req->container_id, CONTAINER_ID_LEN) == 0) {
            snprintf(resp->message, sizeof(resp->message), "%s", rec->log_path);
            resp->status = 0;
            pthread_mutex_unlock(&ctx->metadata_lock);
            return;
        }
        rec = rec->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    snprintf(resp->message, sizeof(resp->message),
             "no container '%s'", req->container_id);
    resp->status = -1;
}

static void handle_stop(supervisor_ctx_t *ctx, const control_request_t *req,
                        control_response_t *resp)
{
    memset(resp, 0, sizeof(*resp));

    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *rec = ctx->containers;
    while (rec) {
        if (strncmp(rec->id, req->container_id, CONTAINER_ID_LEN) == 0) {
            if (rec->state == CONTAINER_RUNNING ||
                rec->state == CONTAINER_STARTING) {
                rec->stop_requested = 1;
                kill(rec->host_pid, SIGTERM);
                snprintf(resp->message, sizeof(resp->message),
                         "sent SIGTERM to '%s' pid=%d", rec->id, rec->host_pid);
                resp->status = 0;
            } else {
                snprintf(resp->message, sizeof(resp->message),
                         "container '%s' is not running", rec->id);
                resp->status = -1;
            }
            pthread_mutex_unlock(&ctx->metadata_lock);
            return;
        }
        rec = rec->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    snprintf(resp->message, sizeof(resp->message),
             "no container '%s'", req->container_id);
    resp->status = -1;
}

static void handle_start(supervisor_ctx_t *ctx, const control_request_t *req,
                         control_response_t *resp)
{
    memset(resp, 0, sizeof(*resp));

    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *existing = ctx->containers;
    while (existing) {
        if (strncmp(existing->id, req->container_id, CONTAINER_ID_LEN) == 0 &&
            (existing->state == CONTAINER_RUNNING ||
             existing->state == CONTAINER_STARTING)) {
            snprintf(resp->message, sizeof(resp->message),
                     "container '%s' already running", req->container_id);
            resp->status = -1;
            pthread_mutex_unlock(&ctx->metadata_lock);
            return;
        }
        existing = existing->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    container_record_t *rec = spawn_container(ctx, req);
    if (!rec) {
        snprintf(resp->message, sizeof(resp->message),
                 "failed to start '%s'", req->container_id);
        resp->status = -1;
        return;
    }

    snprintf(resp->message, sizeof(resp->message),
             "started '%s' pid=%d", rec->id, rec->host_pid);
    resp->status = 0;
}

static void supervisor_event_loop(supervisor_ctx_t *ctx)
{
    while (!ctx->should_stop) {
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(ctx->server_fd, &rfds);

        struct timeval tv = { .tv_sec = 1, .tv_usec = 0 };
        int ready = select(ctx->server_fd + 1, &rfds, NULL, NULL, &tv);

        if (ready < 0) {
            if (errno == EINTR)
                continue;
            perror("select");
            break;
        }
        if (ready == 0)
            continue;

        int client_fd = accept(ctx->server_fd, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EINTR)
                continue;
            perror("accept");
            continue;
        }

        control_request_t req;
        control_response_t resp;
        memset(&req,  0, sizeof(req));
        memset(&resp, 0, sizeof(resp));

        ssize_t n = recv(client_fd, &req, sizeof(req), MSG_WAITALL);
        if (n != (ssize_t)sizeof(req)) {
            close(client_fd);
            continue;
        }

        switch (req.kind) {
        case CMD_START:
            handle_start(ctx, &req, &resp);
            break;

        case CMD_RUN: {
            handle_start(ctx, &req, &resp);
            if (resp.status == 0) {
                pid_t target = -1;
                pthread_mutex_lock(&ctx->metadata_lock);
                container_record_t *r = ctx->containers;
                while (r) {
                    if (strncmp(r->id, req.container_id, CONTAINER_ID_LEN) == 0) {
                        target = r->host_pid;
                        break;
                    }
                    r = r->next;
                }
                pthread_mutex_unlock(&ctx->metadata_lock);

                if (target > 0) {
                    int wstatus;
                    waitpid(target, &wstatus, 0);

                    int exit_code = 0;
                    if (WIFEXITED(wstatus))
                        exit_code = WEXITSTATUS(wstatus);
                    else if (WIFSIGNALED(wstatus))
                        exit_code = 128 + WTERMSIG(wstatus);

                    snprintf(resp.message, sizeof(resp.message),
                             "container '%s' exited code=%d",
                             req.container_id, exit_code);
                    resp.status = exit_code;
                }
            }
            break;
        }

        case CMD_PS:
            handle_ps(ctx, &resp);
            break;

        case CMD_LOGS:
            handle_logs(ctx, &req, &resp);
            break;

        case CMD_STOP:
            handle_stop(ctx, &req, &resp);
            break;

        default:
            snprintf(resp.message, sizeof(resp.message), "unknown command");
            resp.status = -1;
            break;
        }

        send(client_fd, &resp, sizeof(resp), 0);
        close(client_fd);
    }
}

static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    struct sigaction sa;
    struct sockaddr_un addr;
    int rc;

    memset(&ctx, 0, sizeof(ctx));
    g_ctx          = &ctx;
    ctx.server_fd  = -1;
    ctx.monitor_fd = -1;

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0)
        fprintf(stderr, "[supervisor] no kernel monitor: %s\n", strerror(errno));

    if (mkdir(LOG_DIR, 0755) != 0 && errno != EEXIST)
        perror("mkdir logs");

    /* start the single consumer logging thread before accepting any connections */
    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("pthread_create logger");
        goto cleanup;
    }

    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("socket");
        goto cleanup;
    }

    unlink(CONTROL_PATH);
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        goto cleanup;
    }

    if (listen(ctx.server_fd, 8) < 0) {
        perror("listen");
        goto cleanup;
    }

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigchld_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa, NULL);

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigterm_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    fprintf(stderr, "[supervisor] started, rootfs=%s socket=%s\n",
            rootfs, CONTROL_PATH);

    supervisor_event_loop(&ctx);

    fprintf(stderr, "[supervisor] shutting down\n");

cleanup:
    pthread_mutex_lock(&ctx.metadata_lock);
    {
        container_record_t *rec = ctx.containers;
        while (rec) {
            if (rec->state == CONTAINER_RUNNING ||
                rec->state == CONTAINER_STARTING) {
                rec->stop_requested = 1;
                kill(rec->host_pid, SIGTERM);
            }
            rec = rec->next;
        }
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    sleep(1);
    while (waitpid(-1, NULL, WNOHANG) > 0)
        ;

    /* shut down the log buffer and wait for the consumer thread to drain */
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    if (ctx.logger_thread)
        pthread_join(ctx.logger_thread, NULL);

    pthread_mutex_lock(&ctx.metadata_lock);
    {
        container_record_t *rec = ctx.containers;
        while (rec) {
            container_record_t *next = rec->next;
            if (ctx.monitor_fd >= 0)
                unregister_from_monitor(ctx.monitor_fd, rec->id, rec->host_pid);
            free(rec);
            rec = next;
        }
        ctx.containers = NULL;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    bounded_buffer_destroy(&ctx.log_buffer);

    if (ctx.monitor_fd >= 0) close(ctx.monitor_fd);
    if (ctx.server_fd  >= 0) close(ctx.server_fd);
    unlink(CONTROL_PATH);

    pthread_mutex_destroy(&ctx.metadata_lock);
    g_ctx = NULL;
    return 0;
}

static int send_control_request(const control_request_t *req)
{
    int fd;
    struct sockaddr_un addr;
    control_response_t resp;

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect: is the supervisor running?");
        close(fd);
        return 1;
    }

    if (send(fd, req, sizeof(*req), 0) != (ssize_t)sizeof(*req)) {
        perror("send");
        close(fd);
        return 1;
    }

    ssize_t n = recv(fd, &resp, sizeof(resp), MSG_WAITALL);
    close(fd);

    if (n != (ssize_t)sizeof(resp)) {
        fprintf(stderr, "incomplete response from supervisor\n");
        return 1;
    }

    if (req->kind == CMD_LOGS && resp.status == 0) {
        FILE *f = fopen(resp.message, "r");
        if (!f) {
            fprintf(stderr, "cannot open log file: %s\n", resp.message);
            return 1;
        }
        char buf[4096];
        size_t nr;
        while ((nr = fread(buf, 1, sizeof(buf), f)) > 0)
            fwrite(buf, 1, nr, stdout);
        fclose(f);
        return 0;
    }

    printf("%s\n", resp.message);
    return resp.status == 0 ? 0 : 1;
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr, "Usage: %s start <id> <rootfs> <cmd> [opts]\n", argv[0]);
        return 1;
    }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs,       argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command,      argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;
    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr, "Usage: %s run <id> <rootfs> <cmd> [opts]\n", argv[0]);
        return 1;
    }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs,       argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command,      argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;
    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0) return cmd_start(argc, argv);
    if (strcmp(argv[1], "run")   == 0) return cmd_run(argc, argv);
    if (strcmp(argv[1], "ps")    == 0) return cmd_ps();
    if (strcmp(argv[1], "logs")  == 0) return cmd_logs(argc, argv);
    if (strcmp(argv[1], "stop")  == 0) return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
