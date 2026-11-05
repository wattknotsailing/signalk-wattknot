'use strict'

// Every expected byte string here was built by hand from the contract's byte
// layout (protocol/n2k-stream.md §2.1), with the checksum arithmetic written
// beside it — never by running the code under test.
//
// 0x93 payload layout (§2.1):
//   prio | pgn (3 bytes, LSB first) | dst | src | timestamp (4 bytes, LE) | dlen | data…
//   len = 11 + dlen; checksum makes cmd + len + payload + checksum ≡ 0 (mod 256)
//   frame = 10 02 <cmd len payload checksum, each 0x10 doubled> 10 03

const { createStream, createIsFast, BATCH_MS } = require('./stream')

// The only fast-packet PGN in these tests: 129029 = 0x01F805 → bytes 05 f8 01.
const FAST_PGN = 129029

// A canboatjs:rawoutput event from plain bytes. canId is left out: the stream
// reads the decoded prio/pgn/src/dst, never the raw identifier.
function raw ({ pgn, src, dst = 255, prio = 2 }, bytes) {
  return {
    pgn: { prio, src, pgn, dst },
    length: bytes.length,
    data: bytes.map((b) => b.toString(16).padStart(2, '0'))
  }
}

// A stream wired to a recording sink and a hand-driven clock. `results` scripts
// what the sink answers, one per write, then 'sent' for every write after.
function setup ({ start = 0, fw = 1, results = [] } = {}) {
  const clock = { t: start, now: () => clock.t }
  const sent = []
  const send = (buffer) => {
    sent.push(buffer)
    return results.length > 0 ? results.shift() : 'sent'
  }
  // Instant timers: the batch window closes as soon as it opens, so each
  // write reaches the sink on its own, as these tests were written. The
  // batching itself is tested below with a timer the test holds.
  const timers = { setTimeout: (fn) => { fn(); return 1 }, clearTimeout: () => {} }
  const stream = createStream({ isFast: (pgn) => pgn === FAST_PGN, send, clock, fw, timers })
  return { stream, sent, clock }
}

const hex = (buffer) => [...buffer].map((b) => b.toString(16).padStart(2, '0')).join(' ')
const text = (buffer) => buffer.toString('ascii')

// Base single-frame header used by the escaping vectors:
//   prio 2, pgn 127250 = 0x01F112 → 12 f1 01, dst ff, src 01, timestamp 0.
// Its byte sum before dlen and data:
//   0x93 + len + 02 + 12 + f1 + 01 + ff + 01 + 00 00 00 00
//   = 147 + len + 2 + 18 + 241 + 1 + 255 + 1 = 665 + len
const BASE = { pgn: 127250, src: 1 }

describe('createStream', () => {
  test('refuses an fw that the hub would reject as a non-integer value (§3)', () => {
    const deps = { isFast: () => false, send: () => 'sent', clock: { now: () => 0 } }
    expect(() => createStream({ ...deps, fw: '1.1.0' })).toThrow(TypeError)
    expect(() => createStream({ ...deps, fw: 1.5 })).toThrow(TypeError)
    expect(() => createStream({ ...deps, fw: -1 })).toThrow(TypeError)
    expect(() => createStream({ ...deps, fw: 0 })).not.toThrow()
  })
})

describe('Actisense 0x93 encoding (§2.1)', () => {
  test('exact byte layout for a single-frame PGN, from the verbatim rawoutput event', () => {
    // Created at t=5000, frame at t=5258 → timestamp 258 = 0x00000102 → 02 01 00 00.
    const { stream, sent, clock } = setup({ start: 5000 })
    clock.t = 5258
    stream.frame({
      pgn: { canId: 435295269, prio: 6, src: 37, pgn: 127508, dst: 255 },
      length: 8,
      data: ['00', '2e', '05', '00', '00', 'ff', 'ff', '84']
    })
    // len = 11 + 8 = 19 = 0x13; pgn 127508 = 0x01F214 → 14 f2 01; src 37 = 0x25.
    // sum = 0x93 147 + 0x13 19 + 06 + 0x14 20 + 0xf2 242 + 01 + 0xff 255 + 0x25 37
    //     + 02 + 01 + 00 + 00 + 08 + (00 + 0x2e 46 + 05 + 00 + 00 + 255 + 255 + 0x84 132)
    //     = 738 + 693 = 1431; 1431 mod 256 = 151; checksum = 256 − 151 = 105 = 0x69.
    expect(sent).toHaveLength(1)
    expect(hex(sent[0])).toBe(
      '10 02 93 13 06 14 f2 01 ff 25 02 01 00 00 08 00 2e 05 00 00 ff ff 84 69 10 03'
    )
  })

  test('checksum makes cmd + len + payload + checksum sum to zero modulo 256', () => {
    const { stream, sent } = setup()
    stream.frame(raw({ pgn: 127508, src: 37, prio: 6 }, [0x00, 0x2e, 0x05, 0x00, 0x00, 0xff, 0xff, 0x84]))
    // Strip 10 02 … 10 03; no byte in this frame is 0x10, so nothing is doubled.
    const inner = [...sent[0]].slice(2, -2)
    expect(inner.reduce((a, b) => a + b, 0) % 256).toBe(0)
  })

  test('a frame shorter than 8 bytes is carried at its real length, never padded', () => {
    const { stream, sent } = setup()
    stream.frame(raw(BASE, [0x01, 0x02, 0x03]))
    // len = 11 + 3 = 14 = 0x0e; sum = 665 + 14 + dlen 3 + (1 + 2 + 3) = 688;
    // 688 mod 256 = 176; checksum = 80 = 0x50.
    expect(hex(sent[0])).toBe('10 02 93 0e 02 12 f1 01 ff 01 00 00 00 00 03 01 02 03 50 10 03')
  })

  test('timestamp is milliseconds since the stream was created, little-endian', () => {
    // 16909060 ms = 0x01020304 → 04 03 02 01.
    const { stream, sent, clock } = setup({ start: 1000 })
    clock.t = 1000 + 0x01020304
    stream.frame(raw(BASE, [0x00]))
    // len 0x0c; sum = 665 + 12 + (4 + 3 + 2 + 1) + dlen 1 + 0 = 688; mod 256 = 176; checksum 0x50.
    expect(hex(sent[0])).toBe('10 02 93 0c 02 12 f1 01 ff 01 04 03 02 01 01 00 50 10 03')
  })

  test('timestamp wraps at 2^32 ms rather than overflowing its 4 bytes', () => {
    const { stream, sent, clock } = setup()
    clock.t = 2 ** 32 + 5
    stream.frame(raw(BASE, [0x00]))
    // timestamp 5 → 05 00 00 00; sum = 665 + 12 + 5 + 1 + 0 = 683; mod 256 = 171; checksum 85 = 0x55.
    expect(hex(sent[0])).toBe('10 02 93 0c 02 12 f1 01 ff 01 05 00 00 00 01 00 55 10 03')
  })

  describe('DLE (0x10) is doubled at every position it can occur', () => {
    test('in the data', () => {
      const { stream, sent } = setup()
      stream.frame(raw(BASE, [0x10]))
      // len 0x0c; sum = 665 + 12 + 1 + 16 = 694; mod 256 = 182; checksum 74 = 0x4a.
      expect(hex(sent[0])).toBe('10 02 93 0c 02 12 f1 01 ff 01 00 00 00 00 01 10 10 4a 10 03')
    })

    test('in the length byte', () => {
      const { stream, sent } = setup()
      // dlen 5 → len = 11 + 5 = 16 = 0x10.
      stream.frame(raw(BASE, [0x01, 0x02, 0x03, 0x04, 0x05]))
      // sum = 665 + 16 + 5 + 15 = 701; mod 256 = 189; checksum 67 = 0x43.
      expect(hex(sent[0])).toBe(
        '10 02 93 10 10 02 12 f1 01 ff 01 00 00 00 00 05 01 02 03 04 05 43 10 03'
      )
    })

    test('in the checksum, right before DLE ETX', () => {
      const { stream, sent } = setup()
      // For checksum 0x10 the body must sum to 240 mod 256. Without data the sum
      // is 665 + 12 + 1 = 678 ≡ 166, so the data byte is 240 − 166 = 74 = 0x4a.
      stream.frame(raw(BASE, [0x4a]))
      expect(hex(sent[0])).toBe('10 02 93 0c 02 12 f1 01 ff 01 00 00 00 00 01 4a 10 10 10 03')
    })

    test('in the timestamp', () => {
      const { stream, sent, clock } = setup()
      clock.t = 16 // → 10 00 00 00
      stream.frame(raw(BASE, [0x00]))
      // sum = 665 + 12 + 16 + 1 + 0 = 694; mod 256 = 182; checksum 0x4a.
      expect(hex(sent[0])).toBe('10 02 93 0c 02 12 f1 01 ff 01 10 10 00 00 00 01 00 4a 10 03')
    })

    test('in the PGN', () => {
      const { stream, sent } = setup()
      // 126992 = 0x01F010 → 10 f0 01.
      stream.frame(raw({ pgn: 126992, src: 1 }, [0x00]))
      // sum = 147 + 12 + 2 + 16 + 240 + 1 + 255 + 1 + 0 + 1 + 0 = 675; mod 256 = 163; checksum 93 = 0x5d.
      expect(hex(sent[0])).toBe('10 02 93 0c 02 10 10 f0 01 ff 01 00 00 00 00 01 00 5d 10 03')
    })

    test('in the destination and source', () => {
      const { stream, sent } = setup()
      // ISO Request 59904 = 0x00EA00 → 00 ea 00, addressed dst 0x10 from src 0x10,
      // asking for 126996 = 0x01F014 → 14 f0 01.
      stream.frame(raw({ pgn: 59904, src: 0x10, dst: 0x10, prio: 6 }, [0x14, 0xf0, 0x01]))
      // len = 14 = 0x0e; sum = 147 + 14 + 6 + 0 + 234 + 0 + 16 + 16 + 0 + 3 + (20 + 240 + 1) = 697;
      // mod 256 = 185; checksum 71 = 0x47.
      expect(hex(sent[0])).toBe(
        '10 02 93 0e 06 00 ea 00 10 10 10 10 00 00 00 00 03 14 f0 01 47 10 03'
      )
    })
  })
})

describe('fast-packet reassembly (§2 "Complete messages only")', () => {
  // One 19-byte message (data 0x21…0x33) from src 7, prio 3, in three CAN frames.
  // Counter byte = sequence id << 5 | frame index. The last frame carries 6 data
  // bytes and one 0xff of padding that must not be sent.
  function message (sequence, src = 7) {
    const c = sequence << 5
    const id = { pgn: FAST_PGN, src, prio: 3 }
    return [
      raw(id, [c | 0, 19, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26]),
      raw(id, [c | 1, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d]),
      raw(id, [c | 2, 0x2e, 0x2f, 0x30, 0x31, 0x32, 0x33, 0xff])
    ]
  }
  // len = 11 + 19 = 30 = 0x1e; dlen 19 = 0x13.
  // sum = 147 + 30 + 3 + 5 + 248 + 1 + 255 + src + 0 + 19 + (0x21 + … + 0x33 = (33 + 51) × 19 / 2 = 798)
  //     = 1506 + src. src 7 → 1513, mod 256 = 233, checksum 23 = 0x17.
  //                   src 8 → 1514, mod 256 = 234, checksum 22 = 0x16.
  const DATA = '21 22 23 24 25 26 27 28 29 2a 2b 2c 2d 2e 2f 30 31 32 33'
  const FROM_7 = `10 02 93 1e 03 05 f8 01 ff 07 00 00 00 00 13 ${DATA} 17 10 03`
  const FROM_8 = `10 02 93 1e 03 05 f8 01 ff 08 00 00 00 00 13 ${DATA} 16 10 03`

  test('a message split across frames in order is sent once, at its declared length', () => {
    const { stream, sent } = setup()
    const [f0, f1, f2] = message(2)
    stream.frame(f0)
    stream.frame(f1)
    expect(sent).toHaveLength(0)
    stream.frame(f2)
    expect(sent.map(hex)).toEqual([FROM_7])
  })

  test('a message that fits its first frame is sent from that frame alone', () => {
    const { stream, sent } = setup()
    stream.frame(raw({ pgn: FAST_PGN, src: 7, prio: 3 }, [0x60, 3, 0x21, 0x22, 0x23, 0xff, 0xff, 0xff]))
    // len 14 = 0x0e; sum = 147 + 14 + 3 + 5 + 248 + 1 + 255 + 7 + 3 + (33 + 34 + 35) = 785;
    // mod 256 = 17; checksum 239 = 0xef.
    expect(sent.map(hex)).toEqual(['10 02 93 0e 03 05 f8 01 ff 07 00 00 00 00 03 21 22 23 ef 10 03'])
  })

  test('frames out of order are discarded, and the next whole message still goes', () => {
    const { stream, sent } = setup()
    const [f0, f1, f2] = message(2)
    stream.frame(f0)
    stream.frame(f2)
    stream.frame(f1)
    expect(sent).toHaveLength(0)
    message(3).forEach(stream.frame)
    expect(sent.map(hex)).toEqual([FROM_7])
  })

  test('a missing middle frame discards the message', () => {
    const { stream, sent } = setup()
    const [f0, , f2] = message(2)
    stream.frame(f0)
    stream.frame(f2)
    expect(sent).toHaveLength(0)
  })

  test('the first frame missing discards the message', () => {
    const { stream, sent } = setup()
    const [, f1, f2] = message(2)
    stream.frame(f1)
    stream.frame(f2)
    expect(sent).toHaveLength(0)
  })

  test('two sources interleaving the same PGN are reassembled independently', () => {
    const { stream, sent } = setup()
    const a = message(2, 7)
    const b = message(2, 8)
    stream.frame(a[0])
    stream.frame(b[0])
    stream.frame(a[1])
    stream.frame(b[1])
    stream.frame(a[2])
    expect(sent.map(hex)).toEqual([FROM_7])
    stream.frame(b[2])
    expect(sent.map(hex)).toEqual([FROM_7, FROM_8])
  })

  test('a sequence id change mid-message discards the message', () => {
    const { stream, sent } = setup()
    const [f0, f1] = message(2)
    const [, , g2] = message(3)
    stream.frame(f0)
    stream.frame(f1)
    stream.frame(g2)
    expect(sent).toHaveLength(0)
  })

  test('a new first frame mid-message abandons the old one and completes the new one', () => {
    const { stream, sent } = setup()
    const [f0, f1] = message(2)
    stream.frame(f0)
    stream.frame(f1)
    message(3).forEach(stream.frame)
    expect(sent.map(hex)).toEqual([FROM_7])
  })

  test('a single-frame PGN is never held, even between frames of a fast packet', () => {
    const { stream, sent } = setup()
    const [f0, f1, f2] = message(2)
    stream.frame(f0)
    // Its first two bytes look like a fast-packet first frame from the same
    // source; it is not one, because the table says its PGN is single-frame.
    stream.frame(raw({ pgn: 127250, src: 7 }, [0x40, 0x13, 0, 0, 0, 0, 0, 0]))
    expect(sent).toHaveLength(1)
    stream.frame(f1)
    stream.frame(f2)
    expect(hex(sent[1])).toBe(FROM_7)
  })

  test('frames beyond the declared length are neither appended nor sent again', () => {
    const { stream, sent } = setup()
    message(2).forEach(stream.frame)
    // A fourth frame of the same sequence: the message completed at frame 2.
    stream.frame(raw({ pgn: FAST_PGN, src: 7, prio: 3 }, [0x43, 1, 2, 3, 4, 5, 6, 7]))
    expect(sent.map(hex)).toEqual([FROM_7])
  })

  test('a declared length beyond what 32 frames can carry (6 + 31 × 7 = 223) is discarded', () => {
    const { stream, sent } = setup()
    const id = { pgn: FAST_PGN, src: 7, prio: 3 }
    stream.frame(raw(id, [0x00, 224, 1, 2, 3, 4, 5, 6]))
    for (let i = 1; i <= 31; i++) stream.frame(raw(id, [i, 1, 2, 3, 4, 5, 6, 7]))
    expect(sent).toHaveLength(0)
  })

  test('a short middle frame is a gap and discards the message', () => {
    const { stream, sent } = setup()
    const [f0, , f2] = message(2)
    stream.frame(f0)
    stream.frame(raw({ pgn: FAST_PGN, src: 7, prio: 3 }, [0x41, 0x27, 0x28, 0x29]))
    stream.frame(f2)
    expect(sent).toHaveLength(0)
  })

  test('a first frame with no length byte is discarded', () => {
    const { stream, sent } = setup()
    const [, f1, f2] = message(2)
    stream.frame(raw({ pgn: FAST_PGN, src: 7, prio: 3 }, [0x40]))
    stream.frame(f1)
    stream.frame(f2)
    expect(sent).toHaveLength(0)
  })

  describe('a message left unfinished expires 2 s after its latest frame', () => {
    test('a partial with no frame for over 2 s is dropped: its late frames never send it', () => {
      const { stream, sent, clock } = setup()
      const [f0, f1, f2] = message(2)
      stream.frame(f0)
      clock.t = 2001
      stream.frame(f1)
      stream.frame(f2)
      expect(sent).toHaveLength(0)
    })

    test('frames each within 2 s of the last complete the message, however long it takes in all', () => {
      const { stream, sent, clock } = setup()
      const [f0, f1, f2] = message(2)
      stream.frame(f0)
      clock.t = 2000
      stream.frame(f1)
      clock.t = 4000
      stream.frame(f2)
      // FROM_7 framed at 4000 ms = 0x0fa0 → a0 0f 00 00; sum = 1513 + 0xa0 160
      // + 0x0f 15 = 1688; mod 256 = 152; checksum 104 = 0x68.
      expect(sent.map(hex)).toEqual([`10 02 93 1e 03 05 f8 01 ff 07 a0 0f 00 00 13 ${DATA} 68 10 03`])
    })

    test('the heartbeat sweeps out expired partials and keeps the live ones', () => {
      const { stream, sent, clock } = setup()
      const a = message(2, 7)
      const b = message(2, 8)
      stream.frame(a[0])
      clock.t = 1500
      stream.frame(b[0])
      // At 2500 ms src 7's partial is 2.5 s old and goes; src 8's is 1 s old.
      clock.t = 2500
      stream.heartbeat()
      expect(sent.map(text)).toEqual([`$WKT,v=4,kind=2,up=2,rx=2,txd=0,txl=0,fw=1\n`])
      a.slice(1).forEach(stream.frame)
      b.slice(1).forEach(stream.frame)
      // FROM_8 framed at 2500 ms = 0x09c4 → c4 09 00 00; sum = 1514 + 0xc4 196
      // + 0x09 9 = 1719; mod 256 = 183; checksum 73 = 0x49.
      expect(sent.slice(1).map(hex)).toEqual([`10 02 93 1e 03 05 f8 01 ff 08 c4 09 00 00 13 ${DATA} 49 10 03`])
    })
  })

  test('a malformed event mid-message does not disturb the message in progress', () => {
    const { stream, sent } = setup()
    const [f0, f1, f2] = message(2)
    stream.frame(f0)
    stream.frame({ ...f1, data: ['zz'] })
    stream.frame(f1)
    stream.frame(f2)
    expect(sent.map(hex)).toEqual([FROM_7])
  })
})

describe('heartbeat line (§3–§5.1, v4 kind 2)', () => {
  test('exact line text for a fresh stream', () => {
    const { stream, sent } = setup({ start: 1000, fw: 1 })
    stream.heartbeat()
    expect(sent.map(text)).toEqual(['$WKT,v=4,kind=2,up=0,rx=0,txd=0,txl=0,fw=1\n'])
  })

  test('fields are v, kind, up, rx, txd, txl, fw in that order, every value an integer', () => {
    const { stream, sent } = setup({ fw: 120 })
    stream.heartbeat()
    const line = text(sent[0])
    expect(line.startsWith('$WKT,')).toBe(true)
    expect(line.endsWith('\n')).toBe(true)
    const fields = line.slice('$WKT,'.length, -1).split(',').map((f) => f.split('='))
    expect(fields.map(([key]) => key)).toEqual(['v', 'kind', 'up', 'rx', 'txd', 'txl', 'fw'])
    for (const [, value] of fields) expect(value).toMatch(/^[0-9]+$/)
  })

  test('the line is printable ASCII and never contains 0x10 (§2.2 demux)', () => {
    const { stream, sent } = setup()
    stream.heartbeat()
    for (const byte of sent[0].subarray(0, -1)) {
      expect(byte).toBeGreaterThanOrEqual(0x20)
      expect(byte).toBeLessThanOrEqual(0x7e)
    }
    expect(sent[0][sent[0].length - 1]).toBe(0x0a)
  })

  test('rx counts CAN frames since the previous line and resets every line', () => {
    const { stream, sent } = setup()
    stream.frame(raw(BASE, [0x00]))
    stream.frame(raw(BASE, [0x00]))
    stream.frame(raw(BASE, [0x00]))
    stream.heartbeat()
    stream.heartbeat()
    expect(sent.slice(3).map(text)).toEqual([
      '$WKT,v=4,kind=2,up=0,rx=3,txd=0,txl=0,fw=1\n',
      '$WKT,v=4,kind=2,up=0,rx=0,txd=0,txl=0,fw=1\n'
    ])
  })

  test('rx counts CAN frames, not reassembled messages, including frames later discarded', () => {
    const { stream, sent } = setup()
    const id = { pgn: FAST_PGN, src: 7, prio: 3 }
    stream.frame(raw(id, [0x40, 19, 0, 0, 0, 0, 0, 0]))
    stream.frame(raw(id, [0x41, 0, 0, 0, 0, 0, 0, 0]))
    stream.frame(raw(id, [0x43, 0, 0, 0, 0, 0, 0, 0])) // a gap: the message is discarded
    stream.heartbeat()
    expect(sent.map(text)).toEqual(['$WKT,v=4,kind=2,up=0,rx=3,txd=0,txl=0,fw=1\n'])
  })

  test('up is whole seconds since creation, from the clock', () => {
    const { stream, sent, clock } = setup({ start: 1000 })
    clock.t = 1000 + 1999
    stream.heartbeat()
    clock.t = 1000 + 2000
    stream.heartbeat()
    clock.t = 1000 + 259000
    stream.heartbeat()
    expect(sent.map(text)).toEqual([
      '$WKT,v=4,kind=2,up=1,rx=0,txd=0,txl=0,fw=1\n',
      '$WKT,v=4,kind=2,up=2,rx=0,txd=0,txl=0,fw=1\n',
      '$WKT,v=4,kind=2,up=259,rx=0,txd=0,txl=0,fw=1\n'
    ])
  })

  test('a silent bus still gets a line every call, with rx=0 and counts unchanged (§4)', () => {
    const { stream, sent, clock } = setup()
    stream.heartbeat()
    clock.t = 1000
    stream.heartbeat()
    clock.t = 2000
    stream.heartbeat()
    expect(sent.map(text)).toEqual([
      '$WKT,v=4,kind=2,up=0,rx=0,txd=0,txl=0,fw=1\n',
      '$WKT,v=4,kind=2,up=1,rx=0,txd=0,txl=0,fw=1\n',
      '$WKT,v=4,kind=2,up=2,rx=0,txd=0,txl=0,fw=1\n'
    ])
  })
})

describe('drop counting (§6.1 "Drop, don\'t delay")', () => {
  test("a frame answered 'busy' counts to txd", () => {
    const { stream, sent } = setup({ results: ['busy'] })
    stream.frame(raw(BASE, [0x00]))
    stream.heartbeat()
    expect(text(sent[1])).toBe('$WKT,v=4,kind=2,up=0,rx=1,txd=1,txl=0,fw=1\n')
  })

  test("a frame answered 'down' counts to txl", () => {
    const { stream, sent } = setup({ results: ['down'] })
    stream.frame(raw(BASE, [0x00]))
    stream.heartbeat()
    expect(text(sent[1])).toBe('$WKT,v=4,kind=2,up=0,rx=1,txd=0,txl=1,fw=1\n')
  })

  test("a frame answered 'sent' counts to neither", () => {
    const { stream, sent } = setup({ results: ['sent'] })
    stream.frame(raw(BASE, [0x00]))
    stream.heartbeat()
    expect(text(sent[1])).toBe('$WKT,v=4,kind=2,up=0,rx=1,txd=0,txl=0,fw=1\n')
  })

  test('a dropped line counts like a frame, and shows in the next line', () => {
    const { stream, sent } = setup({ results: ['busy', 'down', 'sent'] })
    stream.heartbeat()
    stream.heartbeat()
    stream.heartbeat()
    stream.heartbeat()
    expect(sent.map(text)).toEqual([
      '$WKT,v=4,kind=2,up=0,rx=0,txd=0,txl=0,fw=1\n',
      '$WKT,v=4,kind=2,up=0,rx=0,txd=1,txl=0,fw=1\n',
      '$WKT,v=4,kind=2,up=0,rx=0,txd=1,txl=1,fw=1\n',
      '$WKT,v=4,kind=2,up=0,rx=0,txd=1,txl=1,fw=1\n'
    ])
  })

  test('a dropped write is never retried or queued: one send per write, nothing more', () => {
    const { stream, sent } = setup({ results: ['busy', 'down'] })
    stream.frame(raw(BASE, [0x01]))
    stream.frame(raw(BASE, [0x02]))
    stream.frame(raw(BASE, [0x03]))
    expect(sent).toHaveLength(3)
    // The third send carries only the third frame: data byte 03.
    // sum = 665 + 12 + 1 + 3 = 681; mod 256 = 169; checksum 87 = 0x57.
    expect(hex(sent[2])).toBe('10 02 93 0c 02 12 f1 01 ff 01 00 00 00 00 01 03 57 10 03')
  })

  test('counters() reports rx since the last line and txd/txl since start', () => {
    const { stream } = setup({ results: ['busy', 'down', 'sent'] })
    stream.frame(raw(BASE, [0x00]))
    stream.frame(raw(BASE, [0x00]))
    expect(stream.counters()).toEqual({ rx: 2, txd: 1, txl: 1 })
    stream.heartbeat()
    expect(stream.counters()).toEqual({ rx: 0, txd: 1, txl: 1 })
  })
})

describe('malformed input is dropped silently and not counted in rx', () => {
  const good = raw(BASE, [0x01, 0x02])
  const cases = [
    ['no event', null],
    ['no pgn object', { length: 2, data: ['01', '02'] }],
    ['pgn not an object', { ...good, pgn: 127250 }],
    ['non-integer PGN', { ...good, pgn: { ...good.pgn, pgn: 127250.5 } }],
    ['PGN as a string', { ...good, pgn: { ...good.pgn, pgn: '127250' } }],
    ['PGN beyond 18 bits', { ...good, pgn: { ...good.pgn, pgn: 0x40000 } }],
    ['priority beyond 3 bits', { ...good, pgn: { ...good.pgn, prio: 8 } }],
    ['negative source', { ...good, pgn: { ...good.pgn, src: -1 } }],
    ['source beyond a byte', { ...good, pgn: { ...good.pgn, src: 256 } }],
    ['destination missing', { ...good, pgn: { ...good.pgn, dst: undefined } }],
    ['missing data', { pgn: good.pgn, length: 2 }],
    ['data not an array', { ...good, data: '0102' }],
    ['length disagreeing with data', { ...good, length: 3 }],
    ['length missing', { pgn: good.pgn, data: good.data }],
    ['length beyond a CAN frame', { ...good, length: 9, data: ['01', '02', '03', '04', '05', '06', '07', '08', '09'] }],
    ['zero length', { ...good, length: 0, data: [] }],
    ['odd hex: one digit', { ...good, data: ['1', '02'] }],
    ['odd hex: three digits', { ...good, data: ['001', '02'] }],
    ['odd hex: not hex', { ...good, data: ['zz', '02'] }],
    ['hex as a number', { ...good, data: [12, '02'] }]
  ]

  test.each(cases)('%s', (_name, event) => {
    const { stream, sent } = setup()
    stream.frame(event)
    expect(sent).toHaveLength(0)
    expect(stream.counters().rx).toBe(0)
  })

  test('uppercase hex is read the same as lowercase', () => {
    const { stream, sent } = setup()
    stream.frame({ ...raw(BASE, [0x4a]), data: ['4A'] })
    // The checksum-0x10 vector from the escaping tests.
    expect(hex(sent[0])).toBe('10 02 93 0c 02 12 f1 01 ff 01 00 00 00 00 01 4a 10 10 10 03')
  })
})

describe('the fast-packet rule (§2): the canboat table plus the proprietary ranges', () => {
  // A table in the shape of @canboat/pgns getPGNs(): one PGN may have several
  // definitions, and only some of them may be Fast.
  const table = [
    { PGN: 127250, Type: 'Single' },
    { PGN: 129029, Type: 'Fast' },
    { PGN: 130323, Type: 'Single' },
    { PGN: 130323, Type: 'Fast' },
    { PGN: 59904, Type: 'ISO' }
  ]
  const isFast = createIsFast(table)

  test('a PGN whose only definition is Single is not fast', () => {
    expect(isFast(127250)).toBe(false)
  })

  test('a PGN whose definition is Fast is fast', () => {
    expect(isFast(129029)).toBe(true)
  })

  test('a PGN is fast if ANY of its definitions is Fast', () => {
    expect(isFast(130323)).toBe(true)
  })

  test('an ISO PGN is not fast', () => {
    expect(isFast(59904)).toBe(false)
  })

  test('a PGN absent from the table and outside the ranges is single-frame', () => {
    expect(isFast(65280)).toBe(false) // proprietary single-frame range
    expect(isFast(130306)).toBe(false)
  })

  test('126720, the addressable proprietary fast-packet PGN, is fast without a table entry', () => {
    expect(isFast(126720)).toBe(true)
  })

  test('the proprietary fast-packet range 130816–131071 is fast at both ends and inside', () => {
    expect(isFast(130816)).toBe(true)
    expect(isFast(130900)).toBe(true)
    expect(isFast(131071)).toBe(true)
  })

  test('the range has hard edges', () => {
    expect(isFast(130815)).toBe(false)
    expect(isFast(131072)).toBe(false)
  })

  test('an empty table leaves only the ranges', () => {
    const bare = createIsFast([])
    expect(bare(129029)).toBe(false)
    expect(bare(130816)).toBe(true)
  })
})

// §6.1 "Carriage": writes within one BATCH_MS window go as one message, and a
// batch the link refuses counts every write in it.
describe('batching (one message per window)', () => {
  function held ({ results = [] } = {}) {
    const clock = { t: 0, now: () => clock.t }
    const sent = []
    const send = (buffer) => { sent.push(buffer); return results.length > 0 ? results.shift() : 'sent' }
    const timeouts = []
    const timers = {
      setTimeout: (fn, ms) => { timeouts.push({ fn, ms }); return timeouts.length },
      clearTimeout: (id) => { timeouts[id - 1] = null }
    }
    const fire = () => { const t = timeouts.find(Boolean); timeouts[timeouts.indexOf(t)] = null; t.fn() }
    const stream = createStream({ isFast: (pgn) => pgn === FAST_PGN, send, clock, fw: 1, timers })
    return { stream, sent, timeouts, fire }
  }

  test('frames within the window leave as one message, in order, when it closes', () => {
    const { stream, sent, timeouts, fire } = held()
    stream.frame(raw({ pgn: 127250, src: 20 }, [1, 2, 3, 4, 5, 6, 7, 8]))
    stream.frame(raw({ pgn: 127251, src: 20 }, [8, 7, 6, 5, 4, 3, 2, 1]))
    expect(sent).toHaveLength(0)
    expect(timeouts.filter(Boolean)).toHaveLength(1)
    expect(timeouts[0].ms).toBe(BATCH_MS)
    fire()
    expect(sent).toHaveLength(1)
    // The same bytes two single sends would have produced, back to back.
    const single = setup()
    single.stream.frame(raw({ pgn: 127250, src: 20 }, [1, 2, 3, 4, 5, 6, 7, 8]))
    single.stream.frame(raw({ pgn: 127251, src: 20 }, [8, 7, 6, 5, 4, 3, 2, 1]))
    expect(hex(sent[0])).toBe(hex(Buffer.concat(single.sent)))
    // The next write opens a new window.
    stream.frame(raw({ pgn: 127250, src: 20 }, [1, 2, 3, 4, 5, 6, 7, 8]))
    expect(timeouts.filter(Boolean)).toHaveLength(1)
    expect(sent).toHaveLength(1)
  })

  test('a lone write goes as itself, not a copy', () => {
    const { stream, sent, fire } = held()
    stream.heartbeat()
    fire()
    expect(sent).toHaveLength(1)
    expect(sent[0].toString('ascii')).toMatch(/^\$WKT,v=4,kind=2,/)
  })

  test('a batch the link refuses counts every write in it, by the link\'s reason', () => {
    const busy = held({ results: ['busy'] })
    for (let i = 0; i < 3; i++) busy.stream.frame(raw({ pgn: 127250, src: 20 }, [1, 2, 3, 4, 5, 6, 7, 8]))
    busy.fire()
    expect(busy.stream.counters()).toMatchObject({ txd: 3, txl: 0 })

    const down = held({ results: ['down'] })
    down.stream.heartbeat()
    down.stream.frame(raw({ pgn: 127250, src: 20 }, [1, 2, 3, 4, 5, 6, 7, 8]))
    down.fire()
    expect(down.stream.counters()).toMatchObject({ txd: 0, txl: 2 })
  })

  test('stop() abandons an open window: nothing sent, nothing counted, the timer cleared', () => {
    const { stream, sent, timeouts } = held()
    stream.frame(raw({ pgn: 127250, src: 20 }, [1, 2, 3, 4, 5, 6, 7, 8]))
    stream.stop()
    expect(timeouts.filter(Boolean)).toHaveLength(0)
    expect(sent).toHaveLength(0)
    expect(stream.counters()).toMatchObject({ txd: 0, txl: 0 })
    stream.stop()   // harmless twice, and with no window open
  })
})
