# Wattknot N2K stream — producer contract

The contract between a software N2K producer and a Wattknot hub, as this
plugin implements it. It is an extract of the hub's own contract document,
which also covers Wattknot's gateway hardware and is kept with the hub; what
is here is everything a producer like this one needs, and nothing a producer
does not. Section numbers follow the full document so the plugin's comments
cite the same ones. The hub parses against the contract, not against any
producer's source.

A producer **taps the bus and forwards it**. It decodes nothing, originates
nothing, and never becomes a bus node.

---

## 2. The two byte classes on the wire

The stream interleaves exactly two kinds of bytes:

1. **Actisense binary frames**, carrying the NMEA 2000 data, serialised by
   the producer to the canboat-compatible binary format the hub's decoder
   (canboatjs) parses. The producer does not decode, modify or originate
   these; it is a pure tap.
2. **Telemetry lines** — emitted by the producer itself, in the gaps
   *between* frames (§4).

The hub demultiplexes the two before handing the frames to its decoder.

**Requirements on the frames:**

- **Complete messages only.** Each frame carries one complete N2K message
  (command `0x93`, N2K message received). Fast-packet messages are
  reassembled **by the producer**, never sent as individual CAN frames: the
  hub's decoder switches mode on its first complete message and cannot then
  mix complete and single-frame input, so a stream must be uniformly
  complete. A partial fast-packet (a gap, a missing first frame, a sequence
  change, a frame out of order — CAN delivers one sender's frames in order,
  so out of order means lost) is discarded, never sent. Reassembly is keyed
  per sender address and PGN. **Which PGNs are fast-packet is not decidable
  from the bytes**: the reference is the canboat PGN database (the
  `@canboat/pgns` package), the same table the hub's decoder is built on. A
  PGN is fast-packet if **any** definition of it in the table has
  `Type: "Fast"`, **or** it lies in a range NMEA 2000 defines as fast-packet
  regardless of table: **126720** and **130816–131071** (the proprietary
  fast-packet ranges), which cover manufacturer PGNs the table may not list.
  Any other PGN absent from the table is single-frame.
- **Exactly the bytes sent.** The data length is the length the sender
  declared, never padded to a PGN definition's length. Real devices send
  short messages: an AIS unit sending 129810 at 34 bytes against a 35-byte
  definition. The trailing field is then *absent*, which the hub reports as
  null rather than inventing a value from padding.
- **Timestamp (informative).** The frame's 4-byte timestamp carries
  milliseconds since the producer started. The hub does not use it: values
  are stamped on arrival.

### 2.1 Actisense framing (reference, not owned by this contract)

| Byte | Value | Role |
|---|---|---|
| DLE / Escape | `0x10` | marker byte |
| STX | `0x02` | start of text (always preceded by DLE) |
| ETX | `0x03` | end of text (always preceded by DLE) |

A frame is `DLE STX <cmd> <len> <data…> <checksum> DLE ETX`. **Every** `0x10`
between `DLE STX` and `DLE ETX` is **doubled** (`0x10 0x10`): in the command,
the length, the payload and the checksum alike. Consequently a lone `0x10`
never appears mid-frame: **`0x10` is a fully controlled byte.**

For command `0x93` the payload is
`prio | pgn (3 bytes, LSB first) | dst | src | timestamp (4 bytes, LE) | dlen | data…`,
`len` is `11 + dlen`, and the checksum makes `cmd + len + payload + checksum`
sum to zero modulo 256.

### 2.2 The demux rule

`0x10` is the controlled byte, so the two classes are separable without
ambiguity:

- A frame **starts only** with `0x10 0x02` and **ends only** with `0x10 0x03`.
- Telemetry lines contain **no `0x10` byte** (they are printable ASCII, §3)
  and appear **only between frames.**

This is a structural distinction, not a content heuristic — it cannot
collide. Every producer emits the same single muxed stream, so the hub has
one demuxer and one decoder whatever is upstream.

---

## 3. Telemetry line format

```
$WKT,<key>=<value>,<key>=<value>,…<LF>
```

- **Sentinel:** the literal `$WKT` (Wattknot Telemetry).
- **Encoding:** printable ASCII only. **No byte may be `0x10`** (what
  guarantees the demux in §2.2).
- **Separator:** comma between fields. **Field:** `key=value`; keys are the
  short identifiers of §5, values are decimal ASCII integers — `kind`
  included, which is an integer code, not a word. A non-integer value makes
  the hub drop the whole line.
- **Terminator:** a single line-feed `\n` (`0x0A`).
- **No checksum.** The line is advisory telemetry, not bus data; a corrupted
  line is dropped and the next arrives in ≤ 1 s (§4).

A line MUST begin with `v=`, and in v4 `kind=` MUST follow as the second
field. A software producer is **kind 2**:

```
$WKT,v=4,kind=2,up=259,rx=134,txd=0,txl=11459,fw=100
```

---

## 4. Cadence and liveness

- The producer emits **one telemetry line per second (1 Hz)**, on a timer,
  **unconditionally** — independent of whether the bus is carrying traffic.
- Emission happens between frames, never mid-frame: each frame and each line
  is written whole.

The unconditional heartbeat resolves "producer dead" from "bus quiet":

| Hub observes | Meaning |
|---|---|
| Telemetry arriving, `rx` = 0 | Bus is quiet; producer healthy |
| Telemetry arriving, `rx` > 0 | Normal operation |
| No telemetry for **15 s** | Producer is dead / link down |

Routine cellular stalls are longer than a few seconds, so the network
transport's window is 15 s of heartbeat silence.

---

## 5. Field definitions (kind 2)

| Key | Name | Type | Unit | Meaning / failure it exposes |
|---|---|---|---|---|
| `v`    | schema version | const | — | Spec version this line conforms to: `4`. Always first. |
| `kind` | producer kind | enum | — | `2`, a software producer. Always second. |
| `up`   | uptime | counter | s | Whole seconds since the producer instance started (a plugin's `start`, so a settings save or a `reboot` command both restart it). **Resets to ~0 ⇒ the producer restarted.** Measured on a monotonic clock, never the wall clock, which a host may correct from GPS. |
| `rx`   | frames this interval | gauge (per-interval) | frames | CAN frames received off the bus since the previous line (≈ the last second); read as ≈ frames/s. **Not** a lifetime total. Counts CAN frames, not reassembled messages. |
| `txd`  | forward drops | counter | units | Writes (frames or lines) dropped **because the link back-pressured**. Since start, resets with `up`. A rising `txd` means the link is stalling. |
| `txl`  | link-down drops | counter | units | Writes dropped because there was no link to send on (not connected, or reconnecting). Since start, resets with `up`. Separates "my connection was gone" from "my connection was slow". |
| `fw`   | producer version | const | — | The producer's version as an integer: `major × 10000 + minor × 100 + patch` (`0.2.0` → `200`). The hub displays it and compares nothing. |

### 5.1 Mandatory field set

Every field is mandatory; the hub rejects an incomplete line. Unknown keys
are ignored (§8).

| Version | Kind | Mandatory keys (after `v`) |
|---|---|---|
| v4 | 2 | `kind up rx txd txl fw` |

### 5.2 Semantic types

- **counter:** monotonically increasing; `up`, `txd` and `txl` count since
  this start and reset to ~0 on restart (§5.3). The hub may difference
  successive samples.
- **gauge (per-interval):** a count over the interval since the previous
  line, reset to 0 each line (`rx`). Use the value directly as a rate; do
  **not** sum successive samples — the producer keeps no lifetime count, and
  a dropped line simply costs one rate sample.
- **enum:** a code from a fixed set, constant for the run (`kind`).
- **const:** fixed for the line's lifetime (`v`, `fw`).

### 5.3 Restart semantics (hub obligation)

`up` resets on restart. The hub MUST treat `up` going **backwards** as a
restart — restarting its baseline rather than emitting a spurious negative
or huge value. `rx` needs no such handling: it is a fresh per-interval count
every line.

---

## 6.1 Network transport

The stream travels over the network only under these conditions:

- **The producer dials the hub. Outbound only; the producer opens no
  listening port, ever.**
- **Authenticated:** the producer presents the per-hub producer token. An
  unauthenticated connection carries nothing.
- **Encrypted whenever the path leaves the boat's network:** `wss://` to the
  hub's cloud address. On the boat's own network, plain `ws://` with the
  token is permitted.
- **Carriage:** WebSocket binary messages whose concatenated payloads are the
  byte stream of §2–§5. Message boundaries carry no meaning; the hub's
  demuxer reassembles across them exactly as across serial reads. A producer
  may therefore batch: this plugin sends the writes of each 50 ms window as
  one message, because a message per frame at 230 frames/s is 230 TLS
  records a second over a satellite uplink. Fifty milliseconds of staleness
  sits well inside the one second "drop, don't delay" accepts. Commands (§7)
  come back as WebSocket messages carrying the same `<token>\n` lines.
- **Drop, don't delay:** the producer MUST NOT queue stream bytes while the
  link is backed up or down. It drops the write and counts it (`txd` when
  backed up, `txl` when down), so the hub only ever receives fresh data. The
  hub stamps values on arrival, so a delayed frame would arrive looking
  fresh.
- **Producer configuration** is the hub's address plus the token. The same
  producer build reaches a hub on the boat's network or in the cloud.

### The endpoint

| | |
|---|---|
| Path | `/api/n2k/stream`, on the hub's one HTTP origin |
| Cloud address | `wss://<boat id>.api.wattknot.app/api/n2k/stream` — the boat id is the identity, the token the proof |
| Boat-network address | `ws://<hub host>:<port>/api/n2k/stream` (a boat hub serves on port 80) |
| Handshake | A standard WebSocket upgrade over HTTP/1.1 |
| Credential | `Authorization: Bearer <token>` request header. **Never in the URL**: intermediaries log URLs. |
| Stream | Producer → hub as **binary** messages; any chunking |
| Commands | Hub → producer as **text** messages, one `<token>\n` line each (§7) |
| Liveness | 15 s of heartbeat silence ⇒ the hub marks the producer dead (§4) |

**Refusals.** The hub answers the upgrade with an HTTP status instead of
`101` when it will not take the stream, and the cloud address returns the
same statuses for its own reasons. A producer shows the status to its
operator and retries with a back-off, since a refusal rarely clears on its
own:

| Status | Meaning |
|---|---|
| `401` | Bad or missing token. |
| `404` | Path not served: the hub is not on the network transport; or, at the cloud address, an unknown boat id or a path that is not the stream. |
| `503` | The hub's N2K ingest is stopped; or, at the cloud address, the boat is offline. |

**One producer per hub, newest wins.** A new accepted connection replaces
the current one and the hub closes the old socket. A producer whose socket
closes simply redials; a producer that is closed again within seconds of
each connect is sharing a token with another producer, which is a
misconfiguration, not a fault to work around.

**On connect** the hub sends `refresh` (§7.2); a listen-only producer
ignores it.

**Dead links: the producer detects them itself.** After `refresh` the hub
sends nothing, so nothing from the hub will tell a producer its link has
died. A half-open TCP connection (a cellular or satellite NAT dropping its
mapping) looks open to the producer until the kernel gives up, minutes
later, while the hub marked it dead at 15 s. A producer therefore keeps its
own liveness check — WebSocket pings, which the hub answers and the cloud
address passes through — and terminates and redials a socket that stops
answering. This plugin pings every 10 s and gives up when a ping is
unanswered by the time the next is due, so a dead link is found in 10–20 s.
It also abandons a handshake unanswered after 10 s.

**Token:** one per hub, handed to the operator with the address, fixed until
deliberately rotated. A rotation is a new value on both sides and a redial.

**Open:** compression (permessage-deflate is proposed, not negotiated today).

---

## 7. Hub → producer commands

Until this section the stream is unidirectional. Commands add a **thin
reverse channel** on the same link — the hub writes short text commands back
to the producer. This is the only inbound class; the producer never reads
bus data *from* the hub.

### 7.1 Command format

```
<command>\n
```

- A command is a lowercase ASCII token terminated by a single line-feed
  `\n`. A trailing `\r` is tolerated and ignored.
- The producer reads bytes into a line buffer until `\n`, then matches the
  token.
- **Unknown lines are ignored** — silently dropped, no error, no action. A
  corrupted line simply fails to match and nothing happens.

### 7.2 Commands (kind 2)

| Token | Meaning |
|---|---|
| `refresh` | **Optional.** Asks the bus's devices to re-announce themselves. A listen-only producer ignores it; a producer never becomes a bus node just to answer it. |
| `reboot` | Restart the **producer** only, **never the host device**. The hub sees `up` reset to ~0 (§5.3). |

### 7.4 Semantics

- A command takes effect **on receipt**; there is no cadence.
- Actions are **on-demand only** — the producer never restarts on its own
  account of a command.
- There is **no acknowledgement and no retry.** If an action appears not to
  have taken, the hub simply sends the command again.

---

## 8. Versioning

- Every telemetry line carries `v=<n>` as its first field.
- **Additive changes** (new keys appended) increment `v`. A reader of an
  older version MUST ignore unknown keys; a reader of a newer version MUST
  NOT treat a missing key as zero — it is *absent*.
- **Breaking changes** (renamed or removed keys, changed units or types, an
  added checksum) increment `v`.
- Kinds have their own field sets (§5.1). Adding a kind is additive and does
  not increment `v`; a hub that does not know a kind drops its lines.
- The Actisense data class (§2.1) is **not** versioned by this field — it is
  the external canboat format and evolves on its own terms.
