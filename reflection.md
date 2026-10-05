# Structured Reflection on Learning & AI Use

**Module:** IE3010 - Network Programming (Year 3, Semester 2)  
**Student ID:** `IT23646360`  
**Word Count:** 418 words  

---

### 1. Which AI tools, if any, did you use, and at which stages of the work?
Throughout Part 1, I utilized Gemini AI (via Antigravity IDE) across three distinct stages: initial architecture exploration, edge-case analysis of BSD socket framing, and review of thread synchronization strategies. In the planning phase, I used AI to double-check my arithmetic derivation for the personalised port (12360) and Node ID (`NID:6463`). During implementation, AI served as an interactive code reviewer to assess how TCP stream fragmentation could corrupt the `SENDFILE` protocol boundary. Finally, during testing, I used it to generate a checklist of boundary test cases, including sudden client disconnects and duplicate username registrations.

### 2. What did the AI do well? Where did it get things wrong or mislead you?
The AI excelled at providing boilerplate structures for POSIX threads and explaining low-level socket options like `SO_REUSEADDR`. It was particularly helpful in identifying race conditions that arise when iterating over shared client descriptor arrays while new connections are concurrently arriving. 

However, the AI initially misled me regarding socket buffer management. In its first suggestion for handling `SENDFILE`, it assumed that `recv()` would cleanly stop at the newline character following the command header, leaving the raw file bytes untouched in the kernel buffer. In reality, because TCP is an unstructured continuous byte stream, any `recv()` call that pulls the text header may also pull the first few hundred bytes of binary file data. Relying on the initial AI proposal caused file corruption on binary payloads.

### 3. What did you change, add, or reject from any AI output, and why?
I fundamentally redesigned the buffer ingestion mechanism. Instead of performing separate `recv()` calls for headers and file payloads, I implemented an accumulation buffer with pointer tracking. When a `SENDFILE` command is identified, any trailing bytes already present in the client’s buffer are immediately written to the destination file before issuing further `recv()` calls. Furthermore, I rejected a single global mutex suggested by the AI because it would have forced worker threads to block on disk writes during file transfers. I replaced it with three fine-grained mutexes (`clients_mutex`, `rooms_mutex`, and `log_mutex`).

### 4. What did you learn about your own understanding of network programming through completing this assignment?
Completing this assignment solidified my understanding of the fundamental difference between packet-oriented protocols and stream-oriented TCP sockets. I learned that an application layer protocol must take complete responsibility for message framing, delimiter parsing, and flow control. Designing thread-safe multi-client state management taught me the critical importance of keeping critical sections minimal to prevent thread starvation. Most importantly, personally constructing every byte parsing routine gave me the deep confidence needed to explain and modify this codebase live under exam conditions.
