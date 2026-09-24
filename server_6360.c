/*
 * ============================================================================
 * Module:      IE3010 - Network Programming (Year 3, Semester 2)
 * Project:     NetMessenger - Multi-Client Chat & File-Sharing Server
 * Student ID:  IT23646360
 * File Name:   server_6360.c
 * 
 * Personalisation Values (Calculated per Section 2.4):
 *   - Student ID:          IT23646360
 *   - Last 4 digits:       6360
 *   - Middle 4 digits:     6463 (Digits 3-6 of 23646360)
 *   - Listening Port:      12360 (6000 + 6360)
 *   - Node ID Tag:         NID:6463
 *   - Log File:            netmsg_IT23646360.log
 *   - File Storage Path:   ./storage/IT23646360/<sender_username>/<filename>
 * 
 * Concurrency Model:
 *   - POSIX Threads (pthread) multi-threaded architecture with worker thread
 *     per active client.
 *   - Thread-safe synchronization via pthread_mutex_t for user registry,
 *     room registry, and thread-safe logging.
 * ============================================================================
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <ctype.h>

/* --- Personalisation & Configuration Constants --- */
#define DEFAULT_PORT            12360
#define NODE_ID_TAG             "NID:6463"
#define LOG_FILE_NAME           "netmsg_IT23646360.log"
#define STORAGE_BASE_DIR        "./storage/IT23646360"
#define REGISTRATION_NO         "IT23646360"

#define MAX_CLIENTS             64
#define MAX_ROOMS               32
#define MAX_ROOM_MEMBERS        32
#define USERNAME_MAX_LEN        32
#define ROOM_NAME_MAX_LEN       32
#define BUFFER_SIZE             4096
#define MAX_COMMAND_LEN         2048
#define MAX_FILE_SIZE           (50 * 1024 * 1024) /* 50 MB limit */

/* Rate limiting constants (Optional Extension) */
#define RATE_LIMIT_WINDOW_SEC   5
#define RATE_LIMIT_MAX_CMDS     25

/* --- Data Structures --- */

typedef struct {
    int socket_fd;
    struct sockaddr_in address;
    char username[USERNAME_MAX_LEN + 1];
    int is_registered;
    time_t connect_time;
    
    /* Per-client line framing buffer */
    char recv_buf[BUFFER_SIZE * 2];
    int buf_len;

    /* Rate limiting state */
    time_t window_start;
    int cmd_count;
} client_t;

typedef struct {
    char name[ROOM_NAME_MAX_LEN + 1];
    int member_count;
    char members[MAX_ROOM_MEMBERS][USERNAME_MAX_LEN + 1];
} room_t;

/* --- Global State & Synchronization --- */
static client_t *clients[MAX_CLIENTS];
static room_t rooms[MAX_ROOMS];
static int room_count = 0;

static pthread_mutex_t clients_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t rooms_mutex   = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t log_mutex     = PTHREAD_MUTEX_INITIALIZER;

static FILE *log_fp = NULL;

/* --- Function Declarations --- */
static void log_event(const char *category, const char *format, ...);
static void send_raw(int fd, const char *data, size_t len);
static void send_response_ok(client_t *c, const char *msg);
static void send_response_err(client_t *c, const char *code, const char *reason);
static void forward_msg(int fd, const char *format, ...);
static void make_directory_recursive(const char *path);
static int read_exact(int fd, char *buffer, size_t count);

/* Handler declarations */
static void handle_client_disconnect(client_t *client, const char *reason);
static void *client_worker_thread(void *arg);

/* Protocol commands */
static void cmd_register(client_t *c, const char *args);
static void cmd_list(client_t *c);
static void cmd_bcast(client_t *c, const char *args);
static void cmd_pmsg(client_t *c, const char *args);
static void cmd_join(client_t *c, const char *args);
static void cmd_leave(client_t *c, const char *args);
static void cmd_rooms(client_t *c);
static void cmd_rmsg(client_t *c, const char *args);
static void cmd_sendfile(client_t *c, const char *args);
static void cmd_quit(client_t *c);

/* --- Helper Utilities --- */

static void get_timestamp(char *buf, size_t size) {
    time_t now = time(NULL);
    struct tm tm_info;
    localtime_r(&now, &tm_info);
    strftime(buf, size, "%Y-%m-%d %H:%M:%S", &tm_info);
}

static void log_event(const char *category, const char *format, ...) {
    char ts[32];
    get_timestamp(ts, sizeof(ts));

    pthread_mutex_lock(&log_mutex);
    
    va_list args1, args2;
    va_start(args1, format);
    va_copy(args2, args1);

    /* Print to console */
    printf("[%s] [%s] ", ts, category);
    vprintf(format, args1);
    printf("\n");
    fflush(stdout);
    va_end(args1);

    /* Append to personalized log file */
    if (log_fp) {
        fprintf(log_fp, "[%s] [%s] ", ts, category);
        vfprintf(log_fp, format, args2);
        fprintf(log_fp, "\n");
        fflush(log_fp);
    }
    va_end(args2);

    pthread_mutex_unlock(&log_mutex);
}

static void send_raw(int fd, const char *data, size_t len) {
    size_t total_sent = 0;
    while (total_sent < len) {
        ssize_t sent = send(fd, data + total_sent, len - total_sent, 0);
        if (sent <= 0) {
            break;
        }
        total_sent += sent;
    }
}

static void send_response_ok(client_t *c, const char *msg) {
    char line[BUFFER_SIZE];
    /* Every OK ends with space + NODE_ID_TAG + \n */
    if (msg && strlen(msg) > 0) {
        snprintf(line, sizeof(line), "OK %s %s\n", msg, NODE_ID_TAG);
    } else {
        snprintf(line, sizeof(line), "OK %s\n", NODE_ID_TAG);
    }
    send_raw(c->socket_fd, line, strlen(line));
}

static void send_response_err(client_t *c, const char *code, const char *reason) {
    char line[BUFFER_SIZE];
    /* Error responses follow format: ERR <code> <REASON_CODE> NID:6463\n */
    snprintf(line, sizeof(line), "ERR %s %s %s\n", code, reason, NODE_ID_TAG);
    send_raw(c->socket_fd, line, strlen(line));
}

static void forward_msg(int fd, const char *format, ...) {
    char buf[BUFFER_SIZE];
    va_list args;
    va_start(args, format);
    vsnprintf(buf, sizeof(buf), format, args);
    va_end(args);
    send_raw(fd, buf, strlen(buf));
}

static void make_directory_recursive(const char *path) {
    char tmp[512];
    char *p = NULL;
    size_t len;

    snprintf(tmp, sizeof(tmp), "%s", path);
    len = strlen(tmp);
    if (tmp[len - 1] == '/')
        tmp[len - 1] = 0;
    for (p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = 0;
            mkdir(tmp, 0755);
            *p = '/';
        }
    }
    mkdir(tmp, 0755);
}

static int read_exact(int fd, char *buffer, size_t count) {
    size_t total = 0;
    while (total < count) {
        ssize_t n = recv(fd, buffer + total, count - total, 0);
        if (n <= 0) {
            return -1;
        }
        total += n;
    }
    return 0;
}

/* Rate limiter: returns 1 if allowed, 0 if rate limited */
static int check_rate_limit(client_t *c) {
    time_t now = time(NULL);
    if (now - c->window_start >= RATE_LIMIT_WINDOW_SEC) {
        c->window_start = now;
        c->cmd_count = 1;
        return 1;
    }
    c->cmd_count++;
    if (c->cmd_count > RATE_LIMIT_MAX_CMDS) {
        return 0;
    }
    return 1;
}

/* --- State Management --- */

static client_t *find_client_by_name(const char *username) {
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i] && clients[i]->is_registered && strcmp(clients[i]->username, username) == 0) {
            return clients[i];
        }
    }
    return NULL;
}

static room_t *find_room_by_name(const char *room_name) {
    for (int i = 0; i < room_count; i++) {
        if (strcmp(rooms[i].name, room_name) == 0) {
            return &rooms[i];
        }
    }
    return NULL;
}

static room_t *get_or_create_room(const char *room_name) {
    room_t *r = find_room_by_name(room_name);
    if (r) return r;

    if (room_count >= MAX_ROOMS) return NULL;

    r = &rooms[room_count++];
    strncpy(r->name, room_name, ROOM_NAME_MAX_LEN);
    r->name[ROOM_NAME_MAX_LEN] = '\0';
    r->member_count = 0;
    return r;
}

static int is_room_member(const room_t *r, const char *username) {
    for (int i = 0; i < r->member_count; i++) {
        if (strcmp(r->members[i], username) == 0) {
            return 1;
        }
    }
    return 0;
}

static int add_room_member(room_t *r, const char *username) {
    if (is_room_member(r, username)) return 1;
    if (r->member_count >= MAX_ROOM_MEMBERS) return 0;
    strncpy(r->members[r->member_count++], username, USERNAME_MAX_LEN);
    return 1;
}

static int remove_room_member(room_t *r, const char *username) {
    for (int i = 0; i < r->member_count; i++) {
        if (strcmp(r->members[i], username) == 0) {
            for (int j = i; j < r->member_count - 1; j++) {
                strcpy(r->members[j], r->members[j + 1]);
            }
            r->member_count--;
            return 1;
        }
    }
    return 0;
}

static void remove_client_from_all_rooms(const char *username) {
    pthread_mutex_lock(&rooms_mutex);
    for (int i = 0; i < room_count; i++) {
        remove_room_member(&rooms[i], username);
    }
    pthread_mutex_unlock(&rooms_mutex);
}

/* --- Command Implementations --- */

static void cmd_register(client_t *c, const char *args) {
    if (!args || strlen(args) == 0) {
        send_response_err(c, "007", "INVALID_USERNAME");
        return;
    }

    char username[USERNAME_MAX_LEN + 1];
    if (sscanf(args, "%32s", username) != 1) {
        send_response_err(c, "007", "INVALID_USERNAME");
        return;
    }

    /* Validate username alphanumeric */
    for (size_t i = 0; i < strlen(username); i++) {
        if (!isalnum(username[i]) && username[i] != '_' && username[i] != '-') {
            send_response_err(c, "007", "INVALID_CHARACTERS_IN_USERNAME");
            return;
        }
    }

    pthread_mutex_lock(&clients_mutex);
    if (c->is_registered) {
        pthread_mutex_unlock(&clients_mutex);
        send_response_err(c, "006", "ALREADY_REGISTERED");
        return;
    }

    client_t *existing = find_client_by_name(username);
    if (existing) {
        pthread_mutex_unlock(&clients_mutex);
        send_response_err(c, "001", "USERNAME_TAKEN");
        log_event("REGISTRATION_REJECTED", "Username '%s' already taken by fd %d", username, existing->socket_fd);
        return;
    }

    strncpy(c->username, username, USERNAME_MAX_LEN);
    c->username[USERNAME_MAX_LEN] = '\0';
    c->is_registered = 1;

    /* Notify other connected registered clients */
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i] && clients[i]->is_registered && clients[i]->socket_fd != c->socket_fd) {
            forward_msg(clients[i]->socket_fd, "MSG BCAST system [User %s joined NetMessenger]\n", c->username);
        }
    }
    pthread_mutex_unlock(&clients_mutex);

    char ok_payload[128];
    snprintf(ok_payload, sizeof(ok_payload), "REGISTERED %s", c->username);
    send_response_ok(c, ok_payload);

    log_event("REGISTRATION_SUCCESS", "Client fd %d registered as '%s'", c->socket_fd, c->username);
}

static void cmd_list(client_t *c) {
    char user_list[BUFFER_SIZE];
    user_list[0] = '\0';

    pthread_mutex_lock(&clients_mutex);
    int first = 1;
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i] && clients[i]->is_registered) {
            if (!first) {
                strncat(user_list, ",", sizeof(user_list) - strlen(user_list) - 1);
            }
            strncat(user_list, clients[i]->username, sizeof(user_list) - strlen(user_list) - 1);
            first = 0;
        }
    }
    pthread_mutex_unlock(&clients_mutex);

    char payload[BUFFER_SIZE + 64];
    snprintf(payload, sizeof(payload), "USERS %s", user_list);
    send_response_ok(c, payload);
}

static void cmd_bcast(client_t *c, const char *args) {
    if (!args || strlen(args) == 0) {
        send_response_err(c, "007", "EMPTY_MESSAGE");
        return;
    }

    pthread_mutex_lock(&clients_mutex);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i] && clients[i]->is_registered && clients[i]->socket_fd != c->socket_fd) {
            forward_msg(clients[i]->socket_fd, "MSG BCAST %s %s\n", c->username, args);
        }
    }
    pthread_mutex_unlock(&clients_mutex);

    send_response_ok(c, "SENT");
    log_event("BROADCAST", "From '%s': %s", c->username, args);
}

static void cmd_pmsg(client_t *c, const char *args) {
    if (!args || strlen(args) == 0) {
        send_response_err(c, "007", "MISSING_ARGUMENTS");
        return;
    }

    char target[USERNAME_MAX_LEN + 1];
    char message[BUFFER_SIZE];

    int items = sscanf(args, "%32s %[^\n]", target, message);
    if (items < 2) {
        send_response_err(c, "007", "EMPTY_MESSAGE");
        return;
    }

    pthread_mutex_lock(&clients_mutex);
    client_t *dest = find_client_by_name(target);
    if (!dest) {
        pthread_mutex_unlock(&clients_mutex);
        send_response_err(c, "002", "USER_NOT_FOUND");
        return;
    }

    forward_msg(dest->socket_fd, "MSG PRIV %s %s\n", c->username, message);
    pthread_mutex_unlock(&clients_mutex);

    send_response_ok(c, "SENT");
    log_event("PRIVATE_MSG", "From '%s' to '%s': %s", c->username, target, message);
}

static void cmd_join(client_t *c, const char *args) {
    if (!args || strlen(args) == 0) {
        send_response_err(c, "007", "MISSING_ROOM_NAME");
        return;
    }

    char room_name[ROOM_NAME_MAX_LEN + 1];
    if (sscanf(args, "%32s", room_name) != 1) {
        send_response_err(c, "007", "INVALID_ROOM_NAME");
        return;
    }

    pthread_mutex_lock(&rooms_mutex);
    room_t *r = get_or_create_room(room_name);
    if (!r) {
        pthread_mutex_unlock(&rooms_mutex);
        send_response_err(c, "009", "MAX_ROOMS_REACHED");
        return;
    }

    if (!add_room_member(r, c->username)) {
        pthread_mutex_unlock(&rooms_mutex);
        send_response_err(c, "009", "ROOM_FULL");
        return;
    }

    /* Notify existing room members */
    pthread_mutex_lock(&clients_mutex);
    for (int i = 0; i < r->member_count; i++) {
        if (strcmp(r->members[i], c->username) != 0) {
            client_t *member = find_client_by_name(r->members[i]);
            if (member) {
                forward_msg(member->socket_fd, "MSG ROOM %s system [User %s joined the room]\n", room_name, c->username);
            }
        }
    }
    pthread_mutex_unlock(&clients_mutex);
    pthread_mutex_unlock(&rooms_mutex);

    char payload[128];
    snprintf(payload, sizeof(payload), "JOINED %s", room_name);
    send_response_ok(c, payload);
    log_event("ROOM_JOIN", "User '%s' joined room '%s'", c->username, room_name);
}

static void cmd_leave(client_t *c, const char *args) {
    if (!args || strlen(args) == 0) {
        send_response_err(c, "007", "MISSING_ROOM_NAME");
        return;
    }

    char room_name[ROOM_NAME_MAX_LEN + 1];
    if (sscanf(args, "%32s", room_name) != 1) {
        send_response_err(c, "007", "INVALID_ROOM_NAME");
        return;
    }

    pthread_mutex_lock(&rooms_mutex);
    room_t *r = find_room_by_name(room_name);
    if (!r || !is_room_member(r, c->username)) {
        pthread_mutex_unlock(&rooms_mutex);
        send_response_err(c, "003", "ROOM_NOT_FOUND");
        return;
    }

    remove_room_member(r, c->username);

    /* Notify remaining room members */
    pthread_mutex_lock(&clients_mutex);
    for (int i = 0; i < r->member_count; i++) {
        client_t *member = find_client_by_name(r->members[i]);
        if (member) {
            forward_msg(member->socket_fd, "MSG ROOM %s system [User %s left the room]\n", room_name, c->username);
        }
    }
    pthread_mutex_unlock(&clients_mutex);
    pthread_mutex_unlock(&rooms_mutex);

    char payload[128];
    snprintf(payload, sizeof(payload), "LEFT %s", room_name);
    send_response_ok(c, payload);
    log_event("ROOM_LEAVE", "User '%s' left room '%s'", c->username, room_name);
}

static void cmd_rooms(client_t *c) {
    char room_list[BUFFER_SIZE];
    room_list[0] = '\0';

    pthread_mutex_lock(&rooms_mutex);
    int first = 1;
    for (int i = 0; i < room_count; i++) {
        if (!first) {
            strncat(room_list, ",", sizeof(room_list) - strlen(room_list) - 1);
        }
        strncat(room_list, rooms[i].name, sizeof(room_list) - strlen(room_list) - 1);
        first = 0;
    }
    pthread_mutex_unlock(&rooms_mutex);

    char payload[BUFFER_SIZE + 64];
    snprintf(payload, sizeof(payload), "ROOMS %s", room_list);
    send_response_ok(c, payload);
}

static void cmd_rmsg(client_t *c, const char *args) {
    if (!args || strlen(args) == 0) {
        send_response_err(c, "007", "MISSING_ARGUMENTS");
        return;
    }

    char room_name[ROOM_NAME_MAX_LEN + 1];
    char message[BUFFER_SIZE];

    int items = sscanf(args, "%32s %[^\n]", room_name, message);
    if (items < 2) {
        send_response_err(c, "007", "EMPTY_MESSAGE");
        return;
    }

    pthread_mutex_lock(&rooms_mutex);
    room_t *r = find_room_by_name(room_name);
    if (!r || !is_room_member(r, c->username)) {
        pthread_mutex_unlock(&rooms_mutex);
        send_response_err(c, "003", "ROOM_NOT_FOUND");
        return;
    }

    pthread_mutex_lock(&clients_mutex);
    for (int i = 0; i < r->member_count; i++) {
        if (strcmp(r->members[i], c->username) != 0) {
            client_t *member = find_client_by_name(r->members[i]);
            if (member) {
                forward_msg(member->socket_fd, "MSG ROOM %s %s %s\n", room_name, c->username, message);
            }
        }
    }
    pthread_mutex_unlock(&clients_mutex);
    pthread_mutex_unlock(&rooms_mutex);

    send_response_ok(c, "SENT");
    log_event("ROOM_MSG", "From '%s' to room '%s': %s", c->username, room_name, message);
}

static void cmd_sendfile(client_t *c, const char *args) {
    char target[USERNAME_MAX_LEN + 1];
    char filename[256];
    long long filesize = 0;

    int items = sscanf(args, "%32s %255s %lld", target, filename, &filesize);
    if (items != 3 || filesize < 0) {
        send_response_err(c, "007", "INVALID_SENDFILE_ARGUMENTS");
        return;
    }

    if (filesize > MAX_FILE_SIZE) {
        send_response_err(c, "004", "FILE_TOO_LARGE");
        return;
    }

    /* Sanitise filename (prevent path traversal like ../) */
    char *clean_filename = strrchr(filename, '/');
    if (clean_filename) {
        clean_filename++;
    } else {
        clean_filename = filename;
    }

    /* Check target validity before consuming file bytes */
    pthread_mutex_lock(&clients_mutex);
    client_t *dest_user = find_client_by_name(target);
    pthread_mutex_unlock(&clients_mutex);

    pthread_mutex_lock(&rooms_mutex);
    room_t *dest_room = find_room_by_name(target);
    pthread_mutex_unlock(&rooms_mutex);

    if (!dest_user && !dest_room) {
        send_response_err(c, "002", "TARGET_NOT_FOUND");
        return;
    }

    /* Ensure personalized server storage path: ./storage/<regno>/<sender>/<filename> */
    char storage_dir[512];
    snprintf(storage_dir, sizeof(storage_dir), "%s/%s", STORAGE_BASE_DIR, c->username);
    make_directory_recursive(storage_dir);

    char file_save_path[1024];
    snprintf(file_save_path, sizeof(file_save_path), "%s/%s", storage_dir, clean_filename);

    FILE *dest_fp = fopen(file_save_path, "wb");
    if (!dest_fp) {
        log_event("ERROR", "Failed to create file: %s (%s)", file_save_path, strerror(errno));
        send_response_err(c, "010", "SERVER_STORAGE_ERROR");
        return;
    }

    /* Allocate buffer for transferring to recipient clients in real-time or memory */
    char *file_data = NULL;
    if (filesize > 0) {
        file_data = malloc(filesize);
        if (!file_data) {
            fclose(dest_fp);
            send_response_err(c, "010", "OUT_OF_MEMORY");
            return;
        }
    }

    /*
     * SENDFILE framing rule:
     * File bytes immediately follow \n of the SENDFILE command.
     * Some initial bytes may already be present in c->recv_buf!
     */
    long long bytes_collected = 0;
    if (c->buf_len > 0) {
        size_t available = (size_t)c->buf_len;
        if (available > (size_t)filesize) {
            available = (size_t)filesize;
        }
        memcpy(file_data, c->recv_buf, available);
        fwrite(c->recv_buf, 1, available, dest_fp);
        bytes_collected += available;

        /* Shift unconsumed bytes in recv_buf */
        memmove(c->recv_buf, c->recv_buf + available, c->buf_len - available);
        c->buf_len -= available;
    }

    /* Read remaining bytes over socket */
    while (bytes_collected < filesize) {
        size_t needed = filesize - bytes_collected;
        size_t chunk_to_read = needed > BUFFER_SIZE ? BUFFER_SIZE : needed;
        char temp_chunk[BUFFER_SIZE];
        ssize_t n = recv(c->socket_fd, temp_chunk, chunk_to_read, 0);
        if (n <= 0) {
            log_event("ERROR", "Client %s disconnected during SENDFILE transfer", c->username);
            fclose(dest_fp);
            free(file_data);
            return;
        }
        memcpy(file_data + bytes_collected, temp_chunk, n);
        fwrite(temp_chunk, 1, n, dest_fp);
        bytes_collected += n;
    }

    fclose(dest_fp);

    /* Deliver file to recipient client(s) */
    char file_hdr[512];
    snprintf(file_hdr, sizeof(file_hdr), "MSG FILE %s %s %lld\n", c->username, clean_filename, filesize);

    if (dest_user) {
        pthread_mutex_lock(&clients_mutex);
        /* Re-verify dest_user still connected */
        dest_user = find_client_by_name(target);
        if (dest_user) {
            send_raw(dest_user->socket_fd, file_hdr, strlen(file_hdr));
            if (filesize > 0) {
                send_raw(dest_user->socket_fd, file_data, filesize);
            }
        }
        pthread_mutex_unlock(&clients_mutex);
    } else if (dest_room) {
        pthread_mutex_lock(&rooms_mutex);
        pthread_mutex_lock(&clients_mutex);
        for (int i = 0; i < dest_room->member_count; i++) {
            if (strcmp(dest_room->members[i], c->username) != 0) {
                client_t *member = find_client_by_name(dest_room->members[i]);
                if (member) {
                    send_raw(member->socket_fd, file_hdr, strlen(file_hdr));
                    if (filesize > 0) {
                        send_raw(member->socket_fd, file_data, filesize);
                    }
                }
            }
        }
        pthread_mutex_unlock(&clients_mutex);
        pthread_mutex_unlock(&rooms_mutex);
    }

    if (file_data) free(file_data);

    /* Respond OK FILE_RECEIVED <filename> */
    char ok_payload[512];
    snprintf(ok_payload, sizeof(ok_payload), "FILE_RECEIVED %s", clean_filename);
    send_response_ok(c, ok_payload);

    log_event("FILE_TRANSFER", "Saved '%s' (%lld bytes) from '%s' to '%s'. Target: %s",
              clean_filename, filesize, c->username, file_save_path, target);
}

static void cmd_quit(client_t *c) {
    send_response_ok(c, "BYE");
    log_event("QUIT", "Client %s (fd %d) requested QUIT", c->username, c->socket_fd);
    /* Close socket will be handled upon thread termination */
}

/* --- Command Dispatcher --- */

static void process_command(client_t *c, char *cmd_line) {
    /* Strip trailing newline or carriage return */
    size_t len = strlen(cmd_line);
    while (len > 0 && (cmd_line[len - 1] == '\n' || cmd_line[len - 1] == '\r')) {
        cmd_line[len - 1] = '\0';
        len--;
    }
    if (len == 0) return;

    /* Check Rate Limit (Extension) */
    if (!check_rate_limit(c)) {
        send_response_err(c, "008", "RATE_LIMITED");
        log_event("SECURITY", "Rate limit exceeded by %s (fd %d)", c->username, c->socket_fd);
        return;
    }

    /* Extract verb and arguments */
    char verb[64];
    char *args = "";
    char *space = strchr(cmd_line, ' ');
    if (space) {
        *space = '\0';
        strncpy(verb, cmd_line, sizeof(verb) - 1);
        verb[sizeof(verb) - 1] = '\0';
        args = space + 1;
        while (*args == ' ') args++; /* skip leading spaces */
    } else {
        strncpy(verb, cmd_line, sizeof(verb) - 1);
        verb[sizeof(verb) - 1] = '\0';
    }

    /* Registration enforcement: first command MUST be REGISTER (or QUIT) */
    if (!c->is_registered) {
        if (strcasecmp(verb, "REGISTER") == 0) {
            cmd_register(c, args);
        } else if (strcasecmp(verb, "QUIT") == 0) {
            cmd_quit(c);
            close(c->socket_fd);
            c->socket_fd = -1;
        } else {
            send_response_err(c, "005", "REGISTRATION_REQUIRED");
        }
        return;
    }

    /* Registered user commands */
    if (strcasecmp(verb, "REGISTER") == 0) {
        cmd_register(c, args);
    } else if (strcasecmp(verb, "LIST") == 0) {
        cmd_list(c);
    } else if (strcasecmp(verb, "BCAST") == 0) {
        cmd_bcast(c, args);
    } else if (strcasecmp(verb, "PMSG") == 0) {
        cmd_pmsg(c, args);
    } else if (strcasecmp(verb, "JOIN") == 0) {
        cmd_join(c, args);
    } else if (strcasecmp(verb, "LEAVE") == 0) {
        cmd_leave(c, args);
    } else if (strcasecmp(verb, "ROOMS") == 0) {
        cmd_rooms(c);
    } else if (strcasecmp(verb, "RMSG") == 0) {
        cmd_rmsg(c, args);
    } else if (strcasecmp(verb, "SENDFILE") == 0) {
        cmd_sendfile(c, args);
    } else if (strcasecmp(verb, "QUIT") == 0) {
        cmd_quit(c);
        close(c->socket_fd);
        c->socket_fd = -1;
    } else {
        send_response_err(c, "006", "UNKNOWN_COMMAND");
    }
}

/* --- Client Worker Thread --- */

static void handle_client_disconnect(client_t *client, const char *reason) {
    if (!client) return;

    int fd = client->socket_fd;
    char name[USERNAME_MAX_LEN + 1];
    int registered = client->is_registered;
    strncpy(name, client->username, sizeof(name));

    /* Clean up room memberships */
    if (registered) {
        remove_client_from_all_rooms(name);
    }

    /* Remove from client table and close socket */
    pthread_mutex_lock(&clients_mutex);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i] == client) {
            clients[i] = NULL;
            break;
        }
    }

    /* Broadcast departure notification to remaining clients */
    if (registered) {
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (clients[i] && clients[i]->is_registered && clients[i]->socket_fd != fd) {
                forward_msg(clients[i]->socket_fd, "MSG BCAST system [User %s left NetMessenger]\n", name);
            }
        }
    }
    pthread_mutex_unlock(&clients_mutex);

    if (fd >= 0) {
        close(fd);
    }

    log_event("DISCONNECT", "Client '%s' (fd %d) disconnected. Reason: %s",
              registered ? name : "<unregistered>", fd, reason);

    free(client);
}

static void *client_worker_thread(void *arg) {
    client_t *c = (client_t *)arg;
    char raw_read_buf[BUFFER_SIZE];

    c->buf_len = 0;
    c->window_start = time(NULL);
    c->cmd_count = 0;

    log_event("CONNECT", "Accepted new client from %s:%d (assigned fd %d)",
              inet_ntoa(c->address.sin_addr), ntohs(c->address.sin_port), c->socket_fd);

    while (c->socket_fd >= 0) {
        ssize_t bytes_read = recv(c->socket_fd, raw_read_buf, sizeof(raw_read_buf), 0);
        if (bytes_read <= 0) {
            handle_client_disconnect(c, bytes_read == 0 ? "Normal socket close" : strerror(errno));
            pthread_exit(NULL);
        }

        /* Check buffer overflow */
        if (c->buf_len + bytes_read >= (int)sizeof(c->recv_buf)) {
            log_event("WARN", "Buffer overflow attempt from fd %d", c->socket_fd);
            handle_client_disconnect(c, "Protocol buffer overflow");
            pthread_exit(NULL);
        }

        memcpy(c->recv_buf + c->buf_len, raw_read_buf, bytes_read);
        c->buf_len += bytes_read;

        /* Extract and process all complete lines ending in \n */
        while (c->socket_fd >= 0) {
            char *newline_pos = memchr(c->recv_buf, '\n', c->buf_len);
            if (!newline_pos) {
                break; /* Wait for more bytes */
            }

            int line_len = newline_pos - c->recv_buf;
            char cmd_line[MAX_COMMAND_LEN];
            if (line_len >= MAX_COMMAND_LEN) {
                line_len = MAX_COMMAND_LEN - 1;
            }
            memcpy(cmd_line, c->recv_buf, line_len);
            cmd_line[line_len] = '\0';

            /* Shift buffer past the newline */
            int consumed = (newline_pos - c->recv_buf) + 1;
            memmove(c->recv_buf, c->recv_buf + consumed, c->buf_len - consumed);
            c->buf_len -= consumed;

            process_command(c, cmd_line);
        }
    }

    handle_client_disconnect(c, "Session concluded");
    pthread_exit(NULL);
}

/* --- Main Entry Point --- */

int main(int argc, char *argv[]) {
    int port = DEFAULT_PORT;
    if (argc >= 2) {
        port = atoi(argv[1]);
        if (port <= 0 || port > 65535) {
            fprintf(stderr, "Invalid port number. Defaulting to %d\n", DEFAULT_PORT);
            port = DEFAULT_PORT;
        }
    }

    /* Open personalized log file */
    log_fp = fopen(LOG_FILE_NAME, "a");
    if (!log_fp) {
        fprintf(stderr, "Warning: Could not open log file %s: %s\n", LOG_FILE_NAME, strerror(errno));
    }

    log_event("STARTUP", "==================================================");
    log_event("STARTUP", "NetMessenger Server Starting");
    log_event("STARTUP", "Registration No: %s", REGISTRATION_NO);
    log_event("STARTUP", "Assigned Port:   %d", port);
    log_event("STARTUP", "Node ID Tag:     %s", NODE_ID_TAG);
    log_event("STARTUP", "Log File:        %s", LOG_FILE_NAME);
    log_event("STARTUP", "Storage Base:    %s", STORAGE_BASE_DIR);
    log_event("STARTUP", "==================================================");

    /* Ensure storage root directory exists */
    make_directory_recursive(STORAGE_BASE_DIR);

    /* Create TCP socket */
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("socket creation failed");
        exit(EXIT_FAILURE);
    }

    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt SO_REUSEADDR failed");
    }

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port);

    if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind failed");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    if (listen(server_fd, 16) < 0) {
        perror("listen failed");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    log_event("LISTENING", "Server successfully bound and listening on 0.0.0.0:%d", port);

    while (1) {
        struct sockaddr_in client_addr;
        socklen_t addr_len = sizeof(client_addr);
        int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &addr_len);
        if (client_fd < 0) {
            if (errno == EINTR) continue;
            perror("accept failed");
            continue;
        }

        /* Register in global clients table */
        pthread_mutex_lock(&clients_mutex);
        int slot = -1;
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (clients[i] == NULL) {
                slot = i;
                break;
            }
        }

        if (slot == -1) {
            pthread_mutex_unlock(&clients_mutex);
            log_event("REJECT", "Server capacity full (%d clients). Rejecting fd %d", MAX_CLIENTS, client_fd);
            const char *full_msg = "ERR 011 SERVER_FULL NID:6463\n";
            send_raw(client_fd, full_msg, strlen(full_msg));
            close(client_fd);
            continue;
        }

        client_t *new_client = (client_t *)calloc(1, sizeof(client_t));
        new_client->socket_fd = client_fd;
        new_client->address = client_addr;
        new_client->connect_time = time(NULL);
        new_client->is_registered = 0;
        clients[slot] = new_client;
        pthread_mutex_unlock(&clients_mutex);

        /* Create worker thread */
        pthread_t tid;
        if (pthread_create(&tid, NULL, client_worker_thread, new_client) != 0) {
            perror("pthread_create failed");
            pthread_mutex_lock(&clients_mutex);
            clients[slot] = NULL;
            pthread_mutex_unlock(&clients_mutex);
            free(new_client);
            close(client_fd);
        } else {
            pthread_detach(tid);
        }
    }

    close(server_fd);
    if (log_fp) fclose(log_fp);
    return 0;
}
