# NetMessenger: Multi-Client Chat and File-Sharing Platform over TCP/IP

**Module:** IE3010 - Network Programming (Year 3, Semester 2)  
**Student Name:** [Your Name]  
**Student Registration Number:** `IT23646360`  

---

## 1. Personalisation Summary (Section 2.4)

Every key implementation parameter has been derived from registration number **`IT23646360`** per the course formula:

| Parameter | Specification Formula | Value for `IT23646360` |
| :--- | :--- | :--- |
| **Numeric Part** | 8 digits of registration ID | `23646360` |
| **Last Four Digits** | Digits 5–8 | `6360` |
| **Middle Four Digits** | Digits 3–6 | `6463` |
| **Server/Client Port** | `6000 + last four digits` | `12360` ($6000 + 6360$) |
| **Node ID (NID) Tag** | `NID:<middle four digits>` | `NID:6463` |
| **Server Source File** | `server_<last4digits>.c` | `server_6360.c` |
| **Client Source File** | `client_<last4digits>.c` | `client_6360.c` |
| **Personalised Makefile** | `Makefile_<last4digits>` | `Makefile_6360` |
| **Log File Name** | `netmsg_<full registration number>.log` | `netmsg_IT23646360.log` |
| **Storage Path** | `./storage/<reg_no>/<sender>/<filename>` | `./storage/IT23646360/<sender>/<filename>` |
| **Submission Archive** | `IE3010_<full registration number>.zip` | `IE3010_IT23646360.zip` |

---

## 2. System Architecture & Concurrency Model

NetMessenger implements a multi-client client-server architecture communicating over standard BSD stream sockets (`SOCK_STREAM`, TCP):
- **Concurrency Model:** Multi-threaded worker model using POSIX Threads (`pthread`).
- **Connection Handling:** The main server thread accepts client connections and assigns each to a dedicated worker thread.
- **Thread Safety:** Shared global data structures (client registry and room registry) and server-side log operations are synchronized using mutual exclusion locks (`pthread_mutex_t`).
- **Framing Model:** Line-based text protocol delimited by `\n`. Partial packet arrival and multi-line buffering are handled via an accumulation buffer. Binary file streaming accurately accounts for exact byte counts (`<filesize>`) without relying on text delimiters.
- **Optional Extensions:** Implemented sliding-window rate limiting (flood protection) and structured server-side event logging.

---

## 3. Directory Structure

```text
NP_Assignment/
├── IE3010_Assignment_2026.pdf    # Official assignment specification
├── server_6360.c                 # Multi-threaded server implementation
├── client_6360.c                 # Interactive client with receiver thread
├── Makefile_6360                 # Personalised compilation Makefile
├── netmsg_IT23646360.log         # Server event log file
├── README.md                     # Build and execution documentation
├── design_diary.md               # 0.5–1 page development design diary
├── prompt_log.md                 # AI interaction record (CLEAR Level 3)
├── reflection.md                 # Structured learning & reflection (Section 4)
├── Implementation_Report.md      # Comprehensive technical implementation report
├── storage/
│   └── IT23646360/               # Personalised file storage directory
│       └── alice/
│           └── network_diagram.png
└── downloads/                    # Client downloaded file directory
```

---

## 4. Compilation Instructions

To compile both server and client on Linux using the department lab environment:

```bash
# Compile both server and client using the personalised Makefile
make -f Makefile_6360

# Or compile components individually
make -f Makefile_6360 server
make -f Makefile_6360 client

# Clean binaries
make -f Makefile_6360 clean
```

---

## 5. Running the Application

### 5.1 Starting the Server
Run the server executable (it binds to port `12360` by default):

```bash
./server_6360
# Or optionally pass a specific port:
./server_6360 12360
```

### 5.2 Starting the Client(s)
Open separate terminal tabs and launch the client:

```bash
./client_6360
# Or specify host and port:
./client_6360 127.0.0.1 12360
```

### 5.3 Client Interaction Example
Once connected, commands can be issued via user-friendly slash commands or direct raw protocol strings:

```text
# 1. Register a unique username (mandatory first command)
/register alice

# 2. List online users
/list

# 3. Broadcast to all users
/bcast Hello to everyone on NetMessenger!

# 4. Send private message
/pmsg bob Are you ready for the lab assessment?

# 5. Join or create a room
/join study_group

# 6. Send room message
/rmsg study_group Don't forget mutex locks.

# 7. Send file to user or room
/sendfile study_group notes.pdf

# 8. List rooms
/rooms

# 9. Leave room
/leave study_group

# 10. Clean quit
/quit
```

---

## 6. Personalisation Proof

Verification of active socket listening on port `12360`:

```bash
# Check listening TCP sockets
ss -tlnp | grep 12360
# Output:
# LISTEN 0 16 0.0.0.0:12360 0.0.0.0:* users:(("server_6360",pid=...,fd=3))
```
