# Reliable Transport over UDP

**Author:** Vlad Parau

A reliable, connection-oriented transport protocol built on top of UDP in C++, modeled after TCP. It provides a three-way handshake, a sliding window sized from the Bandwidth-Delay Product (BDP), Selective Repeat ARQ for out-of-order delivery, and receiver-driven flow control. Multiple concurrent connections are handled with dedicated worker threads.

---

## Project Structure

```text
.
├── lib/
│   ├── include/
│   │   ├── lib.h         # Sender/Receiver class interfaces and per-connection state
│   │   ├── protocol.h    # Segment types and the control-header layout
│   │   └── utils.h       # Helper declarations
│   ├── libcommon.cpp     # Shared logic used by both endpoints
│   ├── librecv.cpp       # Receiver class implementation
│   └── libsend.cpp       # Sender class implementation
├── Makefile
└── README.md
```

> This is the transport **library**: the sender and receiver logic that the test harness links against.

---

## Protocol Overview

The protocol is a reliable transport mechanism over UDP, similar to TCP, built from three main components.

### Three-Way Handshake
Before any data is transferred, the client and server establish a connection in three steps:
1. The client sends **SYN**.
2. The server replies with **SYN-ACK**, whose payload carries the **dedicated port** it will listen on for this connection's data.
3. The client confirms with **ACK**.

After the handshake, all data flows over the dedicated port opened by the server, **not** over the fixed listening port `8032`. Both sides retransmit unacknowledged handshake packets every **200 ms**.

### Sliding Window with Selective Repeat ARQ
The sender may transmit multiple segments without waiting for an ACK on each, up to the window limit. The window is computed from the **Bandwidth-Delay Product**: `bandwidth × RTT / segment_size`. On a timer expiry, the sender retransmits all unacknowledged segments in the window. The receiver accepts **out-of-order** segments, buffers and reorders them before delivering to the application, and sends an ACK for every segment received.

### Flow Control
In each ACK, the receiver advertises how many more segments it can accept (`recv_window`). The sender never exceeds this value, which prevents overrunning the receiver's buffer.

Sequence numbers are **16-bit (0–65535)** with controlled overflow via `% 65536`.

---

## Segment Types (`protocol.h`)

| Type | Value | Purpose |
|---|---|---|
| `TYPE_SYN` | `0x01` | First packet from the client to initiate a connection |
| `TYPE_SYN_ACK` | `0x02` | Server reply; payload carries the dedicated data port |
| `TYPE_ACK` | `0x03` | Client confirmation that completes the three-way handshake |
| `TYPE_DATA` | `0x04` | Data segments sent from sender to receiver |
| `TYPE_CTRL` | `0x05` | ACK packets sent from receiver back to sender during transfer |
| `SERVER_PORT` | `8032` | Fixed port on which the server listens for connection requests |

`poli_tcp_synack` is the control header followed by `data_port` — the dedicated port the server sends during the handshake.

---

## Per-Connection State (`lib.h`)

**Sender:**
- `max_window_seq` — max number of in-flight unacknowledged segments; derived from the BDP.
- `recv_window_peer` — receive window advertised by the receiver in the last ACK; used for flow control.
- `send_base` — sequence number of the oldest unacknowledged segment; the start of the sliding window.
- `next_seq` — sequence number of the next segment to send.
- `send_buf` — queue where `send_data` places bytes from the application; `sender_handler` drains and packetizes them.
- `window_segs` — copies of sent-but-unacknowledged segments, indexed by sequence number; used for retransmission on timeout.

**Receiver:**
- `expected_seq` — next in-order sequence number expected; everything before it has already been delivered.
- `recv_buf` — queue of in-order bytes ready to be delivered via `recv_data`.
- `recv_window` — out-of-order segments held temporarily until the sequence gap is filled (Selective Repeat).
- `data_ready` — condition variable `recv_data` waits on when `recv_buf` is empty; signaled by `receiver_handler` when new data arrives.

---

## Key Functions

### Sender (`libsend.cpp`)
| Function | Description |
|---|---|
| `init_sender()` | Saves link parameters and starts the `sender_handler` thread |
| `setup_connection()` | Runs the three-way handshake with retry, computes the window from BDP, registers the connection; returns `conn_id` |
| `send_data()` | Copies data into `send_buf` under a mutex; does not send over UDP directly |
| `pump_window()` | Drains `send_buf` and sends segments while the window allows; keeps copies in `window_segs` for retransmission |
| `sender_handler()` | Waits for ACKs and timeouts; retransmits `window_segs` on timeout, advances `send_base` on ACK, calls `pump_window` each iteration for all connections |

### Receiver (`librecv.cpp`)
| Function | Description |
|---|---|
| `init_receiver()` | Creates the listening socket on port `8032` and starts the `receiver_handler` thread |
| `wait4connect()` | Runs the server-side handshake with retry; uses `MSG_PEEK` to handle a lost final ACK; returns `conn_id` |
| `recv_data()` | Copies data from `recv_buf` into the application buffer; blocks on `pthread_cond_wait` when no data is available |
| `receiver_handler()` | Receives segments, stores them in `recv_window`, delivers them in order into `recv_buf`, signals `data_ready`, and sends ACKs |

---

## Notes

- Concurrency is handled with POSIX threads: a dedicated handler thread per endpoint, with mutexes and condition variables guarding shared connection state.
- The sender decouples the application-facing `send_data()` from the wire: data is buffered and the handler thread decides when to transmit based on window and flow-control limits.
- Retransmission is timer-driven; unacknowledged segments are kept in `window_segs` until acknowledged.
