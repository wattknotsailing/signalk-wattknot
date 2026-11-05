'use strict'

// Frames in, bytes out: the producer half of Wattknot's N2K stream contract
// (protocol/n2k-stream.md, kind 2). Pure — no I/O and no timers. The caller
// feeds one CAN frame per frame() call and calls heartbeat() once a second;
// every byte leaves through the injected `send`, and all time comes from the
// injected `clock`, so a test drives both deterministically.

// Actisense framing bytes (§2.1).
const DLE = 0x10
const STX = 0x02
const ETX = 0x03

// Actisense command "N2K message received" — the only command the contract
// accepts (§2 "Complete messages only").
const N2K_MSG_RECEIVED = 0x93

// prio, 3 PGN bytes, dst, src, 4 timestamp bytes, dlen (§2.1: `len` is 11 + dlen).
const HEADER_LENGTH = 11

// Fast-packet framing is NMEA 2000's, not Wattknot's: a 5-bit frame counter
// allows 32 frames, the first carrying 6 data bytes and each later one 7. A
// declared length beyond that can never complete, so it is discarded on sight.
const FAST_MAX_LENGTH = 6 + 31 * 7

// A fast packet's frames follow each other on the bus: one 8-byte extended
// frame is about 0.5 ms at 250 kbit/s, so even a 32-frame message is under
// 20 ms of bus time, and a sender interleaving other traffic stretches that to
// tens of milliseconds, not seconds. A message whose next frame has not come
// within 2 s is not coming — the sender lost a frame we never saw, went
// quiet, or restarted — so it is expired, measured from its latest frame.
// Without a bound it would be held until that sender happened to send the
// same PGN again, and a stray frame of a later message carrying the same
// sequence id and the awaited index could complete it with another
// message's bytes. 2 s is generous on purpose: it must never expire a
// message still arriving.
const PARTIAL_MS = 2000

// A PGN is 18 bits of the 29-bit CAN identifier; anything larger is not one.
const MAX_PGN = 0x3ffff

// A CAN frame carries at most 8 data bytes.
const CAN_MAX_LENGTH = 8

// canboatjs writes each payload byte as two hex digits.
const HEX_BYTE = /^[0-9a-f]{2}$/i

// The Actisense timestamp is 4 bytes, so milliseconds since start wrap after
// ~49.7 days. The hub ignores it (§2 "Timestamp (informative)"); wrapping keeps
// the field well-formed on a long-running server instead of overflowing.
const TIMESTAMP_RANGE = 2 ** 32

// Writes that arrive within this window go to the link as ONE WebSocket
// message (§6.1 "Carriage": message boundaries carry no meaning, the hub's
// demuxer reassembles across them). At 230 frames/s a message per frame is
// 230 TLS records a second, each carrying ~25 bytes of payload under ~100 of
// overhead, and a Starlink uplink — a beam switch every ~15 s, a brief stall
// each — backed the socket up past the busy cap about once a second (seen
// 2026-10-27: ~1% of frames dropped). One message per 50 ms is a tenth of the
// records, and 50 ms of staleness is well inside the one second the busy cap
// already accepts (connection.js). The cap is checked when the batch is sent,
// so a batch dropped there counts every write in it (§6.1 "Drop, don't delay":
// each write either went or is counted).
const BATCH_MS = 50

/**
 * @param {object} deps
 * @param {(pgn: number) => boolean} deps.isFast  whether a PGN is a fast packet
 *   (§2: the canboat PGN table decides; injected so this module stays free of it)
 * @param {(buffer: Buffer) => 'sent' | 'busy' | 'down'} deps.send  the one
 *   outbound edge; 'busy' = dropped under back-pressure, 'down' = dropped with
 *   no link (§6.1 "Drop, don't delay")
 * @param {{ now: () => number }} deps.clock  milliseconds, monotonic
 * @param {number} deps.fw  the producer build as an integer (§5 `fw`)
 * @param {{ setTimeout, clearTimeout }} deps.timers  for the batch window (BATCH_MS)
 */
function createStream ({ isFast, send, clock, fw, timers }) {
  // §3: a non-integer value makes the hub drop the whole line, and with it the
  // producer's liveness — so a bad `fw` fails loudly here, once, at start.
  if (!Number.isSafeInteger(fw) || fw < 0) {
    throw new TypeError(`fw must be a non-negative integer, got ${fw}`)
  }

  const start = clock.now()

  // §5: rx is per-interval (reset every line); txd and txl count since start.
  let rx = 0
  let txd = 0
  let txl = 0

  // Fast packets in progress, keyed per sender and PGN: `${src}:${pgn}`, each
  // stamped `at` its latest frame (PARTIAL_MS).
  const partials = new Map()

  function elapsed () {
    return Math.floor(clock.now() - start)
  }

  // The batch in progress: the writes of the current BATCH_MS window, sent as
  // one message when the window closes. Never more than one window deep — the
  // timer is the only thing that empties it, so nothing here queues beyond it.
  let pending = []
  let batchTimer = null

  // §6.1: never queue (beyond the window), never retry — every write in the
  // batch either went or is counted when the batch meets the link.
  function write (buffer) {
    pending.push(buffer)
    // The first write of a window arms it. Keyed on the batch, not the timer
    // handle, so a timer that fires before setTimeout returns (a test's instant
    // one) cannot leave a stale handle that stops the next window arming.
    if (pending.length === 1) batchTimer = timers.setTimeout(flush, BATCH_MS)
  }

  function flush () {
    batchTimer = null
    const writes = pending.length
    const batch = writes === 1 ? pending[0] : Buffer.concat(pending)
    pending = []
    const result = send(batch)
    if (result === 'busy') txd += writes
    else if (result === 'down') txl += writes
  }

  // The plugin stopping: a window still open is abandoned, not sent — the link
  // is going too, and a late flush would count against a run that is over.
  function stop () {
    if (batchTimer !== null) timers.clearTimeout(batchTimer)
    batchTimer = null
    pending = []
  }

  function frame (event) {
    const can = parse(event)
    if (can === null) return
    // §5 `rx`: CAN frames, not reassembled messages — so counted before reassembly.
    rx++
    if (!isFast(can.pgn)) {
      write(encode(can, can.data, elapsed()))
      return
    }
    const message = reassemble(can)
    if (message !== null) write(encode(can, message, elapsed()))
  }

  // §2 "Complete messages only": returns the message bytes once the last frame
  // arrives, otherwise null. Strictly in order — an out-of-order frame is
  // indistinguishable from a gap, and a gap discards the message, never sends it.
  function reassemble ({ pgn, src, data }) {
    const key = `${src}:${pgn}`
    const sequence = data[0] >> 5
    const index = data[0] & 0x1f
    const now = clock.now()

    if (index === 0) {
      // A first frame always starts afresh; whatever was in progress for this
      // sender and PGN is now unfinishable (§2: a sequence change).
      partials.delete(key)
      if (data.length < 2) return null
      const length = data[1]
      if (length > FAST_MAX_LENGTH) return null
      return progress(key, { sequence, next: 1, length, bytes: data.slice(2), at: now }, data.length)
    }

    const part = partials.get(key)
    if (part === undefined || expired(part, now) || part.sequence !== sequence || part.next !== index) {
      // No first frame, one too old to continue (PARTIAL_MS), a sequence
      // change, or a gap (§2).
      partials.delete(key)
      return null
    }
    part.bytes.push(...data.slice(1))
    part.next++
    part.at = now
    return progress(key, part, data.length)
  }

  function expired (part, now) {
    return now - part.at > PARTIAL_MS
  }

  // Forgets every expired partial. reassemble() already refuses to continue
  // one, so this only bounds what is held: a sender that never sends the PGN
  // again would otherwise keep its last partial for the life of the stream.
  // Run once a second from heartbeat(), over a map of at most one entry per
  // sender and fast PGN.
  function sweep () {
    const now = clock.now()
    for (const [key, part] of partials) {
      if (expired(part, now)) partials.delete(key)
    }
  }

  function progress (key, part, frameLength) {
    if (part.bytes.length >= part.length) {
      partials.delete(key)
      // §2 "Exactly the bytes sent": the declared length, never the padding
      // that fills out the last CAN frame.
      return part.bytes.slice(0, part.length)
    }
    if (frameLength < CAN_MAX_LENGTH) {
      // A short frame that does not finish the message leaves the next frame's
      // bytes at the wrong offset: a gap, so the message is discarded.
      partials.delete(key)
      return null
    }
    partials.set(key, part)
    return null
  }

  // §3–§5.1: the v4 kind 2 set, in the order the contract lists it, integers only.
  function heartbeat () {
    const up = Math.floor(elapsed() / 1000)
    const line = `$WKT,v=4,kind=2,up=${up},rx=${rx},txd=${txd},txl=${txl},fw=${fw}\n`
    // §5.2: rx resets every line, sent or not — a dropped line costs one sample.
    rx = 0
    write(Buffer.from(line, 'ascii'))
    sweep()
  }

  function counters () {
    return { rx, txd, txl }
  }

  return { frame, heartbeat, counters, stop }
}

// One canboatjs:rawoutput event to { prio, pgn, dst, src, data: number[] }, or
// null when it is not a well-formed CAN frame. A malformed event is dropped
// silently and is not counted in rx: nothing partial or invented is ever sent
// (§2), and a frame we cannot read is not a frame we received.
function parse (event) {
  const id = event && event.pgn
  if (!id || typeof id !== 'object') return null
  const { prio, pgn, dst, src } = id
  if (!inRange(prio, 7) || !inRange(pgn, MAX_PGN) || !inRange(dst, 0xff) || !inRange(src, 0xff)) {
    return null
  }
  const { length, data } = event
  if (!inRange(length, CAN_MAX_LENGTH) || length === 0) return null
  if (!Array.isArray(data) || data.length !== length) return null
  if (!data.every((hex) => typeof hex === 'string' && HEX_BYTE.test(hex))) return null
  return { prio, pgn, dst, src, data: data.map((hex) => parseInt(hex, 16)) }
}

function inRange (value, max) {
  return Number.isInteger(value) && value >= 0 && value <= max
}

// §2.1: DLE STX 0x93 len prio pgn(3, LSB first) dst src timestamp(4, LE) dlen
// data checksum DLE ETX, where the checksum makes cmd + len + payload + checksum
// sum to zero modulo 256, and every 0x10 between STX and ETX is doubled so that
// 0x10 stays a fully controlled byte (§2.2's demux depends on it).
function encode ({ prio, pgn, dst, src }, data, ms) {
  const timestamp = ms % TIMESTAMP_RANGE
  const body = [
    N2K_MSG_RECEIVED,
    HEADER_LENGTH + data.length,
    prio,
    pgn & 0xff, (pgn >> 8) & 0xff, pgn >> 16,
    dst,
    src,
    timestamp & 0xff, (timestamp >>> 8) & 0xff, (timestamp >>> 16) & 0xff, timestamp >>> 24,
    data.length,
    ...data
  ]
  const sum = body.reduce((total, byte) => total + byte, 0)
  body.push((256 - (sum % 256)) % 256)

  const out = [DLE, STX]
  for (const byte of body) {
    out.push(byte)
    if (byte === DLE) out.push(DLE)
  }
  out.push(DLE, ETX)
  return Buffer.from(out)
}

// §2: NMEA 2000 defines these PGNs as fast-packet by range, whatever the table
// lists — and the manufacturer PGNs that live here are the ones the table most
// often lacks. Sending one as single frames would hand the hub the mixed stream
// §2 forbids.
const PROPRIETARY_FAST_SINGLE = 126720
const PROPRIETARY_FAST_FROM = 130816
const PROPRIETARY_FAST_TO = 131071

/**
 * The fast-packet rule of §2 built from the canboat PGN table: a PGN is fast if
 * any definition of it has Type 'Fast', or if it lies in a proprietary
 * fast-packet range. Takes the definitions array (`getPGNs()` from
 * `@canboat/pgns`) rather than the package, so this module needs no dependency
 * and a test can hand it a table of its own.
 *
 * @param {Array<{ PGN: number, Type: string }>} definitions
 * @returns {(pgn: number) => boolean}
 */
function createIsFast (definitions) {
  const fast = new Set(definitions.filter((d) => d.Type === 'Fast').map((d) => d.PGN))
  return (pgn) => fast.has(pgn) ||
    pgn === PROPRIETARY_FAST_SINGLE ||
    (pgn >= PROPRIETARY_FAST_FROM && pgn <= PROPRIETARY_FAST_TO)
}

module.exports = { createStream, createIsFast, BATCH_MS }
