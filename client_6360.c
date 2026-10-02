/*
 * ============================================================================
 * Module:      IE3010 - Network Programming (Year 3, Semester 2)
 * Project:     NetMessenger - Interactive TCP/IP Client
 * Student ID:  IT23646360
 * File Name:   client_6360.c
 * 
 * Personalisation Values:
 *   - Listening Port: 12360 (6000 + 6360)
 *   - Node ID Tag:    NID:6463
 *   - Student ID:     IT23646360
 * ============================================================================
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <libgen.h>

#define DEFAULT_SERVER_PORT 12360
#define BUFFER_SIZE         4096
#define DOWNLOAD_DIR        "./downloads"

static int sock_fd = -1;
static volatile int is_running = 1;
static char my_username[64] = {0};

/* --- Utility Functions --- */

static void print_banner(void) {
    printf("\n==========================================================\n");
    printf("   NetMessenger Interactive Client - Port 12360 (NID:6463)  \n");
    printf("   Student ID: IT23646360                                   \n");
    printf("==========================================================\n");
    printf("Available Commands:\n");
    printf("  /register <username>          Register your unique username\n");
    printf("  /list                         List active online users\n");
    printf("  /bcast <message>              Broadcast a public message\n");
    printf("  /pmsg <username> <message>    Send a private message\n");
    printf("  /join <room>                  Join or create a chat room\n");
    printf("  /leave <room>                 Leave a chat room\n");
    printf("  /rooms                        List all active chat rooms\n");
    printf("  /rmsg <room> <message>        Send message to room members\n");
    printf("  /sendfile <target> <filepath> Send a file to user or room\n");
    printf("  /quit                         Disconnect and exit\n");
    printf("  /help                         Display this help menu\n");
    printf("==========================================================\n\n");
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

static void send_raw_bytes(int fd, const char *data, size_t len) {
    size_t total_sent = 0;
    while (total_sent < len) {
        ssize_t sent = send(fd, data + total_sent, len - total_sent, 0);
        if (sent <= 0) break;
        total_sent += sent;
    }
}

/* File Sender Routine */
static void send_file(const char *target, const char *filepath) {
    FILE *fp = fopen(filepath, "rb");
    if (!fp) {
        printf("[!] Failed to open local file '%s': %s\n", filepath, strerror(errno));
        return;
    }

    struct stat st;
    if (stat(filepath, &st) != 0) {
        printf("[!] Failed to stat file: %s\n", strerror(errno));
        fclose(fp);
        return;
    }

    long long filesize = (long long)st.st_size;

    /* Extract pure filename without directory path */
    const char *filename = strrchr(filepath, '/');
    if (!filename) {
        filename = strrchr(filepath, '\\');
    }
    if (filename) filename++;
    else filename = filepath;

    /* Send protocol command line: SENDFILE <target> <filename> <filesize>\n */
    char cmd_hdr[512];
    snprintf(cmd_hdr, sizeof(cmd_hdr), "SENDFILE %s %s %lld\n", target, filename, filesize);
    send_raw_bytes(sock_fd, cmd_hdr, strlen(cmd_hdr));

    /* Immediately stream raw file bytes */
    char file_chunk[BUFFER_SIZE];
    size_t bytes_read = 0;
    long long total_sent = 0;

    printf("[*] Uploading '%s' (%lld bytes) to '%s'...\n", filename, filesize, target);
    while ((bytes_read = fread(file_chunk, 1, sizeof(file_chunk), fp)) > 0) {
        send_raw_bytes(sock_fd, file_chunk, bytes_read);
        total_sent += bytes_read;
    }
    fclose(fp);
    printf("[*] Finished streaming %lld file bytes.\n", total_sent);
}

/* --- Background Receiver Thread --- */

static void *receiver_thread_func(void *arg) {
    (void)arg;
    char buffer[BUFFER_SIZE * 4];
    int buf_len = 0;

    make_directory_recursive(DOWNLOAD_DIR);

    while (is_running) {
        char chunk[BUFFER_SIZE];
        ssize_t n = recv(sock_fd, chunk, sizeof(chunk), 0);
        if (n <= 0) {
            if (is_running) {
                printf("\n[Server disconnected or connection lost]\n");
                is_running = 0;
            }
            break;
        }

        if (buf_len + n >= (int)sizeof(buffer)) {
            buf_len = 0; /* Reset on overflow */
        }
        memcpy(buffer + buf_len, chunk, n);
        buf_len += n;

        /* Process complete lines */
        while (1) {
            char *newline_pos = memchr(buffer, '\n', buf_len);
            if (!newline_pos) break;

            int line_len = newline_pos - buffer;
            char line[BUFFER_SIZE];
            if (line_len >= BUFFER_SIZE) line_len = BUFFER_SIZE - 1;
            memcpy(line, buffer, line_len);
            line[line_len] = '\0';

            /* Shift buffer */
            int consumed = (newline_pos - buffer) + 1;
            memmove(buffer, buffer + consumed, buf_len - consumed);
            buf_len -= consumed;

            /* Check if this line is an incoming file transfer */
            char sender[64], filename[256];
            long long filesize = 0;
            if (sscanf(line, "MSG FILE %63s %255s %lld", sender, filename, &filesize) == 3) {
                printf("\n==================================================\n");
                printf("[INCOMING FILE] From: %s | File: %s (%lld bytes)\n", sender, filename, filesize);
                printf("==================================================\n");

                char dest_path[512];
                snprintf(dest_path, sizeof(dest_path), "%s/%s", DOWNLOAD_DIR, filename);
                FILE *dfp = fopen(dest_path, "wb");
                if (!dfp) {
                    printf("[!] Failed to save incoming file to %s\n", dest_path);
                }

                long long received_file_bytes = 0;

                /* Check what's already buffered */
                if (buf_len > 0) {
                    size_t avail = (size_t)buf_len;
                    if (avail > (size_t)filesize) avail = (size_t)filesize;
                    if (dfp) fwrite(buffer, 1, avail, dfp);
                    received_file_bytes += avail;

                    memmove(buffer, buffer + avail, buf_len - avail);
                    buf_len -= avail;
                }

                /* Read remaining file bytes */
                while (received_file_bytes < filesize) {
                    size_t needed = filesize - received_file_bytes;
                    size_t to_read = needed > BUFFER_SIZE ? BUFFER_SIZE : needed;
                    char temp[BUFFER_SIZE];
                    ssize_t r = recv(sock_fd, temp, to_read, 0);
                    if (r <= 0) break;
                    if (dfp) fwrite(temp, 1, r, dfp);
                    received_file_bytes += r;
                }

                if (dfp) {
                    fclose(dfp);
                    printf("[SUCCESS] File saved successfully to: %s\n", dest_path);
                }
                printf("> ");
                fflush(stdout);
                continue;
            }

            /* Regular message formatting */
            if (strncmp(line, "OK REGISTERED", 13) == 0) {
                sscanf(line, "OK REGISTERED %63s", my_username);
                printf("[SERVER] %s\n", line);
            } else if (strncmp(line, "MSG BCAST", 9) == 0) {
                char from[64], msg[BUFFER_SIZE];
                if (sscanf(line, "MSG BCAST %63s %[^\n]", from, msg) == 2) {
                    printf("\n[BROADCAST] <%s>: %s\n", from, msg);
                } else {
                    printf("\n%s\n", line);
                }
            } else if (strncmp(line, "MSG PRIV", 8) == 0) {
                char from[64], msg[BUFFER_SIZE];
                if (sscanf(line, "MSG PRIV %63s %[^\n]", from, msg) == 2) {
                    printf("\n[PRIVATE] <%s>: %s\n", from, msg);
                } else {
                    printf("\n%s\n", line);
                }
            } else if (strncmp(line, "MSG ROOM", 8) == 0) {
                char room[64], from[64], msg[BUFFER_SIZE];
                if (sscanf(line, "MSG ROOM %63s %63s %[^\n]", room, from, msg) == 3) {
                    printf("\n[ROOM #%s] <%s>: %s\n", room, from, msg);
                } else {
                    printf("\n%s\n", line);
                }
            } else if (strncmp(line, "OK", 2) == 0) {
                printf("[SERVER RESPONSE] %s\n", line);
            } else if (strncmp(line, "ERR", 3) == 0) {
                printf("[ERROR] %s\n", line);
            } else {
                printf("[SERVER] %s\n", line);
            }

            printf("> ");
            fflush(stdout);
        }
    }
    return NULL;
}

/* --- Main Input Loop --- */

int main(int argc, char *argv[]) {
    const char *server_ip = "127.0.0.1";
    int server_port = DEFAULT_SERVER_PORT;

    if (argc >= 2) server_ip = argv[1];
    if (argc >= 3) server_port = atoi(argv[2]);

    printf("[*] Connecting to NetMessenger server at %s:%d...\n", server_ip, server_port);

    sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        perror("socket error");
        exit(EXIT_FAILURE);
    }

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(server_port);

    if (inet_pton(AF_INET, server_ip, &serv_addr.sin_addr) <= 0) {
        perror("Invalid server address");
        close(sock_fd);
        exit(EXIT_FAILURE);
    }

    if (connect(sock_fd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("Connection to server failed");
        close(sock_fd);
        exit(EXIT_FAILURE);
    }

    printf("[+] Connected successfully!\n");
    print_banner();

    pthread_t recv_thread;
    if (pthread_create(&recv_thread, NULL, receiver_thread_func, NULL) != 0) {
        perror("pthread_create failed");
        close(sock_fd);
        exit(EXIT_FAILURE);
    }

    char input_line[BUFFER_SIZE];
    while (is_running) {
        printf("> ");
        fflush(stdout);

        if (!fgets(input_line, sizeof(input_line), stdin)) {
            break;
        }

        /* Strip trailing newline */
        size_t len = strlen(input_line);
        while (len > 0 && (input_line[len - 1] == '\n' || input_line[len - 1] == '\r')) {
            input_line[len - 1] = '\0';
            len--;
        }

        if (len == 0) continue;

        /* Parse slash commands or allow direct raw protocol strings */
        if (input_line[0] == '/') {
            char cmd[64], arg1[256], arg2[BUFFER_SIZE];
            cmd[0] = arg1[0] = arg2[0] = '\0';
            sscanf(input_line, "%63s %255s %[^\n]", cmd, arg1, arg2);

            if (strcmp(cmd, "/help") == 0) {
                print_banner();
            } else if (strcmp(cmd, "/register") == 0) {
                if (strlen(arg1) == 0) {
                    printf("Usage: /register <username>\n");
                    continue;
                }
                char out[256];
                snprintf(out, sizeof(out), "REGISTER %s\n", arg1);
                send_raw_bytes(sock_fd, out, strlen(out));
            } else if (strcmp(cmd, "/list") == 0) {
                send_raw_bytes(sock_fd, "LIST\n", 5);
            } else if (strcmp(cmd, "/bcast") == 0) {
                char *msg = input_line + 6;
                while (*msg == ' ') msg++;
                if (strlen(msg) == 0) {
                    printf("Usage: /bcast <message>\n");
                    continue;
                }
                char out[BUFFER_SIZE];
                snprintf(out, sizeof(out), "BCAST %s\n", msg);
                send_raw_bytes(sock_fd, out, strlen(out));
            } else if (strcmp(cmd, "/pmsg") == 0) {
                if (strlen(arg1) == 0 || strlen(arg2) == 0) {
                    printf("Usage: /pmsg <username> <message>\n");
                    continue;
                }
                char out[BUFFER_SIZE];
                snprintf(out, sizeof(out), "PMSG %s %s\n", arg1, arg2);
                send_raw_bytes(sock_fd, out, strlen(out));
            } else if (strcmp(cmd, "/join") == 0) {
                if (strlen(arg1) == 0) {
                    printf("Usage: /join <room>\n");
                    continue;
                }
                char out[256];
                snprintf(out, sizeof(out), "JOIN %s\n", arg1);
                send_raw_bytes(sock_fd, out, strlen(out));
            } else if (strcmp(cmd, "/leave") == 0) {
                if (strlen(arg1) == 0) {
                    printf("Usage: /leave <room>\n");
                    continue;
                }
                char out[256];
                snprintf(out, sizeof(out), "LEAVE %s\n", arg1);
                send_raw_bytes(sock_fd, out, strlen(out));
            } else if (strcmp(cmd, "/rooms") == 0) {
                send_raw_bytes(sock_fd, "ROOMS\n", 6);
            } else if (strcmp(cmd, "/rmsg") == 0) {
                if (strlen(arg1) == 0 || strlen(arg2) == 0) {
                    printf("Usage: /rmsg <room> <message>\n");
                    continue;
                }
                char out[BUFFER_SIZE];
                snprintf(out, sizeof(out), "RMSG %s %s\n", arg1, arg2);
                send_raw_bytes(sock_fd, out, strlen(out));
            } else if (strcmp(cmd, "/sendfile") == 0) {
                if (strlen(arg1) == 0 || strlen(arg2) == 0) {
                    printf("Usage: /sendfile <target_user_or_room> <filepath>\n");
                    continue;
                }
                send_file(arg1, arg2);
            } else if (strcmp(cmd, "/quit") == 0) {
                send_raw_bytes(sock_fd, "QUIT\n", 5);
                usleep(100000);
                is_running = 0;
                break;
            } else {
                printf("[!] Unknown command: %s. Type /help for usage.\n", cmd);
            }
        } else {
            /* Raw protocol command sent directly with \n appended */
            char out[BUFFER_SIZE];
            snprintf(out, sizeof(out), "%s\n", input_line);
            send_raw_bytes(sock_fd, out, strlen(out));
        }
    }

    is_running = 0;
    close(sock_fd);
    pthread_join(recv_thread, NULL);
    printf("\nNetMessenger client terminated cleanly.\n");
    return 0;
}
