# AI Interaction Record & Prompt Log (CLEAR Level 3)

**Module:** IE3010 - Network Programming (Year 3, Semester 2)  
**Student ID:** `IT23646360`  
**Assessment Component:** Part 1 - Take-Home Implementation  

---

## Overview
In compliance with Section 3 of the IE3010 Assignment specification (CLEAR Framework Level 3: AI Collaboration), this document logs all substantive AI interactions during the design, development, and debugging of the NetMessenger application. All AI outputs were critically evaluated, reviewed line-by-line, tested against edge cases, and adapted to conform strictly to the personalized requirements of registration number **`IT23646360`**.

---

## Log Entries

### Entry 1: Personalisation & Port Calculation Verification
- **Tool Used:** Gemini AI Assistant (Antigravity)
- **Prompt:**
  > "Please extract and verify the personalisation formulas from Section 2.4 of the assignment for my student registration ID IT23646360, specifically port, NID tag, source filenames, log file, and storage path."
- **AI Output Summary:**
  - Numeric part: `23646360`
  - Last 4 digits: `6360` $\rightarrow$ Port: $6000 + 6360 = 12360$
  - Middle 4 digits (3rd to 6th digits): `6463` $\rightarrow$ `NID:6463`
  - Source files: `server_6360.c`, `client_6360.c`, `Makefile_6360`
  - Storage path: `./storage/IT23646360/<sender_username>/<filename>`
- **Evaluation & Modification:**
  - Manually validated the 8-digit sequence `2 3 6 4 6 3 6 0`:
    - Index 1: 2
    - Index 2: 3
    - Index 3: 6
    - Index 4: 4
    - Index 5: 6
    - Index 6: 3
    - Index 7: 6
    - Index 8: 0
  - Confirmed middle digits (3rd to 6th) are exactly `6463` and last 4 digits are `6360`. Accepted without alteration.

---

### Entry 2: BSD Socket Framing & Buffer Handling
- **Tool Used:** Gemini AI Assistant (Antigravity)
- **Prompt:**
  > "How should the BSD socket server in C handle partial line reads, multiple commands packed into a single recv buffer, and the boundary condition between a SENDFILE text header and raw binary file bytes?"
- **AI Output Summary:**
  - Proposed an accumulation buffer per client structure.
  - Advised using `memchr(buf, '\n', len)` to parse lines and `memmove()` to shift consumed bytes.
  - For `SENDFILE`, suggested copying leftover bytes in the accumulation buffer immediately into the file stream before making subsequent raw `recv()` calls.
- **Evaluation & Modification:**
  - Tested the logic against packet fragmentation scenarios. Added bounds checking (`c->buf_len + bytes_read >= sizeof(c->recv_buf)`) to prevent buffer overflow attacks and added sanity checks on `<filesize>` (`MAX_FILE_SIZE = 50MB`).

---

### Entry 3: Thread Synchronization & Mutex Hierarchy
- **Tool Used:** Gemini AI Assistant (Antigravity)
- **Prompt:**
  > "Review the concurrency model for the server. Should we use one global mutex or separate mutexes for clients, rooms, and logging? What are the deadlock risks when broadcasting messages?"
- **AI Output Summary:**
  - Recommended three granular mutexes: `clients_mutex`, `rooms_mutex`, and `log_mutex`.
  - Warned that holding `clients_mutex` while performing blocking socket `send()` calls can stall other worker threads if a client is slow.
  - Advised minimizing the critical section to copying socket descriptors or active state, and keeping file I/O operations outside mutex locks.
- **Evaluation & Modification:**
  - Implemented the three granular mutexes. Ensured that disk writes in `SENDFILE` never hold `clients_mutex` or `rooms_mutex`, completely preventing thread starvation and deadlocks.

---

### Entry 4: Client Interactive Interface & Download Handling
- **Tool Used:** Gemini AI Assistant (Antigravity)
- **Prompt:**
  > "Design an interactive C client for NetMessenger that supports both convenient slash commands (/register, /bcast, /sendfile) and direct protocol commands, while running a background pthread to listen for incoming messages and binary file transfers."
- **AI Output Summary:**
  - Provided a skeleton with a dedicated receiver thread and a command parser loop on `stdin`.
- **Evaluation & Modification:**
  - Rewrote the command parser to handle paths with spaces, sanitise downloaded filenames using `strrchr` (preventing directory traversal attacks), and format incoming broadcast/private/room messages with distinct visual prefixes (`[BROADCAST]`, `[PRIVATE]`, `[ROOM]`).
