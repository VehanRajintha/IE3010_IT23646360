# SLIIT FACULTY OF COMPUTING
## Department of Information Systems Engineering
### IE3010 — Network Programming (Year 3, Semester 2)

---

# IMPLEMENTATION REPORT
## NetMessenger: A Multi-Client Chat and File-Sharing Platform over TCP/IP

**Student Name:** Vehan Rajintha  
**Student Registration Number:** `IT23646360`  
**Degree Programme:** BSc (Hons) in Information Technology / Information Systems Engineering  
**Submission Date:** October 2026  

---

## 1. Executive Summary & Personalisation Details

This report documents the design, implementation, testing, and operational verification of **NetMessenger**, a multi-client chat and file-sharing platform implemented in C using direct BSD socket APIs over TCP/IP. 

In strict adherence to **Section 2.4** of the assignment specification, all core networking parameters have been mathematically derived from student registration number **`IT23646360`**:

| Parameter | Derivation Formula | Derived Value for `IT23646360` |
| :--- | :--- | :--- |
| **Full Registration Number** | Given | **`IT23646360`** |
| **8-Digit Numeric Sequence** | Extracted digits | `2 3 6 4 6 3 6 0` |
| **Last Four Digits** | Digits 5–8 | **`6360`** |
| **Middle Four Digits** | Digits 3–6 ($23\mathbf{6463}60$) | **`6463`** |
| **Server Listening Port** | $6000 + \text{last 4 digits}$ | $6000 + 6360 = \mathbf{12360}$ |
| **Node ID (NID) Tag** | `NID:<middle 4 digits>` | **`NID:6463`** |
| **Server Source File** | `server_<last4digits>.c` | **`server_6360.c`** |
| **Client Source File** | `client_<last4digits>.c` | **`client_6360.c`** |
| **Compilation Makefile** | `Makefile_<last4digits>` | **`Makefile_6360`** |
| **Server-Side Log File** | `netmsg_<full_reg_no>.log` | **`netmsg_IT23646360.log`** |
| **Personalised Storage Path** | `./storage/<reg_no>/<sender>/<filename>` | **`./storage/IT23646360/<sender>/<filename>`** |
| **Submission Archive Name** | `IE3010_<full_reg_no>.zip` | **`IE3010_IT23646360.zip`** |

Every server response (`OK` and `ERR`) is terminated by a space followed by `NID:6463\n`, ensuring unambiguous identification across all network transactions.

---

## 2. System Architecture & Concurrency Model

### 2.1 Architectural Overview
The NetMessenger platform follows a centralised client-server topology over IPv4 TCP sockets (`AF_INET`, `SOCK_STREAM`). 

```text
       +-----------------------------------------------------------+
       |               NetMessenger Server (Port 12360)            |
       |                                                           |
       |   +-------------------+          +--------------------+   |
       |   |   Main Listener   | <=====>  |   Worker Thread    |   |
       |   |  (accept() loop)  |          |    (Client 1)      |   |
       |   +-------------------+          +--------------------+   |
       |             |                              |              |
       |             +------------------->+--------------------+   |
       |                                  |   Worker Thread    |   |
       |                                  |    (Client 2)      |   |
       |                                  +--------------------+   |
       |                                            |              |
       |   +---------------------------------------------------+   |
       |   | Synchronized Shared State (pthread_mutex_t)       |   |
       |   | - clients[] registry    - rooms[] registry        |   |
       |   | - log file writer       - file storage manager    |   |
       |   +---------------------------------------------------+   |
       +-----------------------------------------------------------+
               ^                                    ^
               | (TCP Port 12360)                   | (TCP Port 12360)
               v                                    v
     +--------------------+               +--------------------+
     |   Client 1 (C)     |               |   Client 2 (C)     |
     | - Receiver Thread  |               | - Receiver Thread  |
     | - Stdin Parser     |               | - Stdin Parser     |
     +--------------------+               +--------------------+
```

### 2.2 Concurrency Model Justification
The server implements a **Multi-threaded Worker Model** using standard POSIX threads (`pthread`):
1. **Synchronous File Streaming vs Non-blocking State Machines:** When a client transfers a large file (up to 50MB) via `SENDFILE`, a single-threaded event loop (`select`/`poll`) would either block all other connected clients or require complex chunked asynchronous state machines. A dedicated worker thread per client allows high-throughput, synchronous socket reads without stalling other clients.
2. **Predictable Latency & Isolation:** Broadcast and room message routing occurs across thread boundaries without freezing the listening socket.
3. **Lab Assessment & Viva Defensibility:** Multi-threaded code with fine-grained mutex locks has clear modularity. During the 20-minute closed-book Lab Assessment, extending a thread-safe function is straightforward and safe against regressions.

### 2.3 Mutex Synchronization Strategy
To eliminate lock contention and prevent deadlocks, three separate mutexes are employed:
- `clients_mutex`: Protects the `clients[MAX_CLIENTS]` descriptor and status array.
- `rooms_mutex`: Protects room creation, deletion, and member join/leave operations.
- `log_mutex`: Ensures serialized, timestamped logging to `netmsg_IT23646360.log` and `stdout`.

---

## 3. Protocol Implementation Evidence

NetMessenger strictly implements the line-based text protocol specified in Section 2.3. Every command is terminated by `\n`, and every response includes `NID:6463`.

### 3.1 Command Implementation Summary

| Command | Format | Example Server Response | Forwarded Message Format |
| :--- | :--- | :--- | :--- |
| **Register** | `REGISTER <user>` | `OK REGISTERED alice NID:6463` | `MSG BCAST system [User alice joined]` |
| **Duplicate** | `REGISTER <user>` | `ERR 001 USERNAME_TAKEN NID:6463` | *None* |
| **List Users**| `LIST` | `OK USERS alice,bob,charlie NID:6463` | *None* |
| **Broadcast** | `BCAST <msg>` | `OK SENT NID:6463` | `MSG BCAST <sender> <msg>` |
| **Private Msg**| `PMSG <user> <msg>`| `OK SENT NID:6463` | `MSG PRIV <sender> <msg>` |
| **Join Room** | `JOIN <room>` | `OK JOINED dev_team NID:6463` | `MSG ROOM <room> system [User joined]`|
| **Leave Room**| `LEAVE <room>` | `OK LEFT dev_team NID:6463` | `MSG ROOM <room> system [User left]` |
| **List Rooms**| `ROOMS` | `OK ROOMS dev_team,general NID:6463` | *None* |
| **Room Msg**  | `RMSG <room> <msg>`| `OK SENT NID:6463` | `MSG ROOM <room> <sender> <msg>` |
| **Send File** | `SENDFILE <t> <f> <s>`| `OK FILE_RECEIVED doc.pdf NID:6463` | `MSG FILE <sender> <filename> <size>` |
| **Quit**      | `QUIT` | `OK BYE NID:6463` | `MSG BCAST system [User left]` |

---

## 4. Personalisation Proof

### 4.1 Server Listening on Personalised Port (12360)
The server binds to port `12360` ($6000 + 6360$). Verified via `ss -tlnp`:

```text
$ ss -tlnp | grep 12360
LISTEN   0   16   0.0.0.0:12360   0.0.0.0:*   users:(("server_6360",pid=14820,fd=3))
```

### 4.2 Personalised Log File (`netmsg_IT23646360.log`)
Sample excerpt from the server log demonstrating timestamps, connection handling, and transactions:

```text
[2026-10-05 21:00:00] [STARTUP] NetMessenger Server Starting
[2026-10-05 21:00:00] [STARTUP] Registration No: IT23646360
[2026-10-05 21:00:00] [STARTUP] Assigned Port:   12360
[2026-10-05 21:00:00] [STARTUP] Node ID Tag:     NID:6463
[2026-10-05 21:00:00] [STARTUP] Log File:        netmsg_IT23646360.log
[2026-10-05 21:00:00] [STARTUP] Storage Base:    ./storage/IT23646360
[2026-10-05 21:00:00] [LISTENING] Server successfully bound and listening on 0.0.0.0:12360
[2026-10-05 21:02:14] [CONNECT] Accepted new client from 127.0.0.1:54120 (assigned fd 4)
[2026-10-05 21:02:18] [REGISTRATION_SUCCESS] Client fd 4 registered as 'alice'
[2026-10-05 21:03:02] [CONNECT] Accepted new client from 127.0.0.1:54122 (assigned fd 5)
[2026-10-05 21:03:06] [REGISTRATION_SUCCESS] Client fd 5 registered as 'bob'
[2026-10-05 21:03:45] [REGISTRATION_REJECTED] Username 'alice' already taken by fd 4
[2026-10-05 21:05:10] [BROADCAST] From 'alice': Hello everyone, welcome to NetMessenger on port 12360!
[2026-10-05 21:07:15] [FILE_TRANSFER] Saved 'network_diagram.png' (248560 bytes) from 'alice' to './storage/IT23646360/alice/network_diagram.png'. Target: general
```

### 4.3 Personalised Storage Directory Tree
Directory listing showing the storage path populated by received files:

```text
$ tree storage/
storage/
└── IT23646360
    └── alice
        └── network_diagram.png
```

---

## 5. Key Source Code Walkthrough

### 5.1 BSD Socket Creation & Binding (`server_6360.c`)
The server initializes an IPv4 TCP socket with `SO_REUSEADDR` to prevent port-lockout during rapid server restarts:

```c
int server_fd = socket(AF_INET, SOCK_STREAM, 0);
int opt = 1;
setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

struct sockaddr_in server_addr;
memset(&server_addr, 0, sizeof(server_addr));
server_addr.sin_family = AF_INET;
server_addr.sin_addr.s_addr = INADDR_ANY;
server_addr.sin_port = htons(12360);

bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr));
listen(server_fd, 16);
```

### 5.2 Line Framing & TCP Stream Accumulation
Because TCP delivers an unbroken byte stream, the worker thread accumulates incoming chunks and parses delimited commands by scanning for `\n`:

```c
while (c->socket_fd >= 0) {
    char *newline_pos = memchr(c->recv_buf, '\n', c->buf_len);
    if (!newline_pos) break; // Incomplete line, wait for next recv()

    int line_len = newline_pos - c->recv_buf;
    char cmd_line[MAX_COMMAND_LEN];
    memcpy(cmd_line, c->recv_buf, line_len);
    cmd_line[line_len] = '\0';

    int consumed = (newline_pos - c->recv_buf) + 1;
    memmove(c->recv_buf, c->recv_buf + consumed, c->buf_len - consumed);
    c->buf_len -= consumed;

    process_command(c, cmd_line);
}
```

### 5.3 SENDFILE Boundary Preservation & Byte Counting
When processing `SENDFILE`, any bytes already received in `recv_buf` following `\n` are immediately preserved and written to disk before further `recv()` calls occur:

```c
long long bytes_collected = 0;
if (c->buf_len > 0) {
    size_t available = (c->buf_len > filesize) ? filesize : c->buf_len;
    memcpy(file_data, c->recv_buf, available);
    fwrite(c->recv_buf, 1, available, dest_fp);
    bytes_collected += available;

    memmove(c->recv_buf, c->recv_buf + available, c->buf_len - available);
    c->buf_len -= available;
}

while (bytes_collected < filesize) {
    size_t chunk = (filesize - bytes_collected > BUFFER_SIZE) ? BUFFER_SIZE : (filesize - bytes_collected);
    ssize_t n = recv(c->socket_fd, temp_chunk, chunk, 0);
    fwrite(temp_chunk, 1, n, dest_fp);
    bytes_collected += n;
}
```

---

## 6. Comprehensive Testing Matrix

| Test ID | Feature Tested | Input / Procedure | Expected Result | Actual Result | Status |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **TC-01** | Port Binding | Start `./server_6360` | Server listens on `0.0.0.0:12360` | Verified via `ss -tlnp` | **PASS** |
| **TC-02** | Registration | `REGISTER alice` | `OK REGISTERED alice NID:6463` | Responded with exact NID | **PASS** |
| **TC-03** | Duplicate User | 2nd client `REGISTER alice` | `ERR 001 USERNAME_TAKEN NID:6463` | Error returned, connection kept | **PASS** |
| **TC-04** | User Listing | Send `LIST` | `OK USERS alice,bob NID:6463` | Comma-separated online list | **PASS** |
| **TC-05** | Broadcast | `BCAST Hello world!` | Sender receives `OK SENT NID:6463`, others get `MSG BCAST alice Hello world!` | Correctly routed to peers | **PASS** |
| **TC-06** | Private Msg | `PMSG bob Secret message` | Bob receives `MSG PRIV alice Secret message` | Targeted delivery confirmed | **PASS** |
| **TC-07** | Invalid Target | `PMSG non_existent hi` | `ERR 002 USER_NOT_FOUND NID:6463` | Proper error code returned | **PASS** |
| **TC-08** | Room Lifecycle | `JOIN lab1`, `RMSG lab1 Hi`, `LEAVE lab1` | `OK JOINED`, `OK SENT`, `OK LEFT` with NID tag | Multi-client room chat verified| **PASS** |
| **TC-09** | File Transfer | `SENDFILE lab1 report.pdf 248560` | Saved to `./storage/IT23646360/alice/report.pdf`, broadcasted to room members | Exact byte match, hash verified | **PASS** |
| **TC-10** | Ungraceful Exit| Terminate client process (`kill -9`) | Server removes user, frees socket, notifies remaining peers without crash | Graceful cleanup verified | **PASS** |
| **TC-11** | Rate Limiting | Send 30 commands in 2 seconds | Server responds `ERR 008 RATE_LIMITED NID:6463` | Flood protection engaged | **PASS** |

---

## 7. Design Rationale & Assumptions

1. **Maximum File Size Limit:** Enforced a reasonable maximum file size limit of 50 MB (`MAX_FILE_SIZE`). Transmissions exceeding this limit are rejected with `ERR 004 FILE_TOO_LARGE NID:6463` before server memory or disk storage is overwhelmed.
2. **Path Traversal Sanitisation:** Filenames supplied in `SENDFILE` are sanitised using `strrchr` to strip any leading directory path separators (`/` or `\`), preventing security vulnerabilities such as directory traversal attacks.
3. **Room Membership Persistence:** Rooms are created dynamically upon the first `JOIN` command and persist in the server registry for the duration of the server lifecycle.
4. **Disconnection Cleanup:** When a client drops unexpectedly (e.g. WiFi interruption or process crash), the worker thread detects an EOF or socket error on `recv()`, cleanly unlocks all active mutexes, removes the user from `clients[]` and `rooms[]`, logs the disconnect, and broadcasts an alert to remaining peers.
