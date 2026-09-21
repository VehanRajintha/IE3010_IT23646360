# Design Diary: NetMessenger Implementation

**Module:** IE3010 - Network Programming  
**Student ID:** `IT23646360`  
**Personalised Port:** `12360` | **Node ID:** `NID:6463`  

---

### Phase 1: Requirement Analysis & Personalisation Verification (Day 1)
- **Calculation of Parameters:**
  - Using registration number `IT23646360`, the numeric component is `23646360`.
  - The last 4 digits are `6360` $\rightarrow$ listening port $6000 + 6360 = \mathbf{12360}$.
  - The middle 4 digits (indices 3 to 6: `6463`) $\rightarrow$ Node ID tag $\mathbf{NID:6463}$.
  - Source files identified as `server_6360.c`, `client_6360.c`, and `Makefile_6360`.
- **Key Decision 1: Concurrency Model**
  - Evaluated `select()` vs `poll()` vs `epoll()` vs `pthread`.
  - *Decision:* Chose POSIX Threads (`pthread`) with one worker thread per connected client.
  - *Rationale:* Multi-threading provides synchronous file streaming semantics per client thread. When a client transfers a 20MB file via `SENDFILE`, a single-threaded `select()` loop would either block other clients or require a complex non-blocking state machine across multiple buffers. Dedicated worker threads allow clean, readable socket code that is straightforward to explain in the Viva and modify during the 20-minute timed Lab Assessment.

---

### Phase 2: Protocol Framing & Buffer Architecture (Day 2)
- **Obstacle 1: TCP Stream Fragmentation & Merged Packets**
  - TCP provides an unstructured byte stream without boundary preservation. A single `recv()` call can return a partial line, an exact line, or multiple commands joined together (e.g. `JOIN room1\nLEAVE room1\n`).
  - *Resolution:* Implemented a per-client line buffer (`recv_buf`) inside `client_t`. The worker reads incoming chunks, searches for `\n` using `memchr()`, extracts the command, and shifts remaining unparsed bytes forward with `memmove()`.
- **Obstacle 2: The `SENDFILE` Transition Problem**
  - When a client issues `SENDFILE target filename filesize\n<binary bytes>`, the command header and the initial slice of the binary file might arrive in the very same `recv()` packet.
  - If the server discarded or ignored bytes buffered after `\n`, the transferred file would suffer immediate data corruption.
  - *Resolution:* When parsing `SENDFILE`, any bytes already present in `c->recv_buf` after `\n` are counted toward `<filesize>`, written straight into the open file descriptor, and only the remaining byte count is fetched in subsequent `recv()` calls.

---

### Phase 3: State Management & Thread Synchronization (Day 3)
- **Key Decision 2: Mutex Granularity**
  - Avoided a single coarse-grained server lock that could cause lock contention during high traffic.
  - Divided locks into three distinct scopes:
    1. `clients_mutex`: Guards the active connected clients array (`clients[]`) and socket lookups.
    2. `rooms_mutex`: Guards room definitions and membership lists (`rooms[]`).
    3. `log_mutex`: Guards synchronous file writes to `netmsg_IT23646360.log` and `stdout`.
- **Graceful Disconnect & Ungraceful Crash Handling:**
  - Registered clients who disconnect cleanly via `QUIT` or ungracefully (SIGINT, crash, network drop) trigger `handle_client_disconnect()`.
  - The server cleans up their room memberships, frees their slot in `clients[]`, closes the file descriptor, logs the event with timestamp, and broadcasts a departure message to remaining connected users.

---

### Phase 4: Extensions & Viva Preparation (Day 4)
- **Extension Implemented: Rate Limiting & Flood Protection**
  - Implemented a sliding window counter (maximum 25 commands per 5-second window per client).
  - If exceeded, returns `ERR 008 RATE_LIMITED NID:6463\n` without crashing the server.
- **Viva Defense Points Prepared:**
  - Why port 12360? $6000 + 6360 = 12360$.
  - Why NID:6463? Middle 4 digits of `23646360` (positions 3 to 6).
  - Thread safety strategy: Locks acquired briefly only around memory mutation, never held across blocking network I/O calls.
  - Storage path structure: `./storage/IT23646360/<sender>/<filename>` created dynamically with recursive directory creation.
