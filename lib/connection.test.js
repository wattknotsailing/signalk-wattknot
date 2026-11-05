'use strict'

// The carrier against a real `ws` server on loopback (README "Testing"): the
// server checks the path, the Authorization header and the binary type of
// what arrives, and answers refusals with a raw HTTP status. Only the redial
// timer is faked — injected, so ws's own internals keep their real timers —
// and one stub socket stands in where a real one cannot be made to back up on
// demand.

const http = require('http')
const { WebSocket, WebSocketServer } = require('ws')
const { createConnection, addressFor, credentialFrom, MQTT_PATH } = require('./connection')

const TOKEN = 'producer-token-123'
const PATH = '/api/n2k/stream'

// The busy cap and redial delay chosen in connection.js, restated so a change
// to either shows up here as a decision, not a silent drift.
const BUSY_CAP_BYTES = 4096
const REDIAL_MS = 5000

// The keepalive interval chosen in connection.js, restated like the two above.
const KEEPALIVE_MS = 10000

// A hand-driven stand-in for setTimeout/clearTimeout: nothing fires until the
// test says so, and what is pending can be inspected. The connection keeps two
// kinds of timer, told apart here by their delay: redials and keepalives.
function fakeTimers () {
  const pending = new Map()
  let nextId = 1
  const ofDelay = (ms) => [...pending.entries()].filter(([, timer]) => timer.ms === ms)
  return {
    pending,
    redials: () => ofDelay(REDIAL_MS).map(([, timer]) => timer),
    keepalives: () => ofDelay(KEEPALIVE_MS).map(([, timer]) => timer),
    setTimeout (fn, ms) {
      const id = nextId++
      pending.set(id, { fn, ms })
      return id
    },
    clearTimeout (id) {
      pending.delete(id)
    },
    // Fires every pending timer of that delay.
    fire (ms) {
      const due = ofDelay(ms)
      for (const [id] of due) pending.delete(id)
      for (const [, { fn }] of due) fn()
    }
  }
}

// Polls with real timers until `check` holds; fails the test after 2 s.
async function until (check) {
  const deadline = Date.now() + 2000
  while (!check()) {
    if (Date.now() > deadline) throw new Error('timed out waiting')
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
}

// Lets in-flight socket events land, for asserting that something did NOT happen.
const settle = () => new Promise((resolve) => setTimeout(resolve, 50))

const servers = []
const connections = []

// A hub on 127.0.0.1, port 0. `refuse` answers every upgrade with that HTTP
// status instead of 101. Each accepted connection is recorded with its
// request headers and every message it received.
async function startHub ({ refuse = null } = {}) {
  const server = http.createServer()
  const wss = new WebSocketServer({ noServer: true })
  const hub = { server, wss, upgrades: 0, accepted: [] }

  server.on('upgrade', (request, socket, head) => {
    hub.upgrades++
    if (refuse !== null) {
      socket.end(`HTTP/1.1 ${refuse} Refused\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`)
      return
    }
    wss.handleUpgrade(request, socket, head, (ws) => {
      const record = { ws, url: request.url, headers: request.headers, messages: [], closed: false }
      ws.on('message', (data, isBinary) => record.messages.push({ data, isBinary }))
      ws.on('close', () => { record.closed = true })
      hub.accepted.push(record)
    })
  })

  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve))
  hub.address = addressFor({ localAddress: `127.0.0.1:${server.address().port}` })
  servers.push(hub)
  return hub
}

// A connection under test with recording callbacks and fake redial timers.
function connect (address, { Socket = WebSocket } = {}) {
  const timers = fakeTimers()
  const statuses = []
  const refused = [] // the `refused` flag that came with each status, in order
  const connected = [] // and the `connected` flag
  const commands = []
  const conn = createConnection({
    address,
    token: TOKEN,
    WebSocket: Socket,
    timers,
    onStatus: (text, detail) => {
      statuses.push(text)
      refused.push(detail.refused)
      connected.push(detail.connected)
    },
    onCommand: (line) => commands.push(line)
  })
  connections.push(conn)
  return { conn, timers, statuses, refused, connected, commands }
}

// A port nothing listens on: bound, then released.
async function closedPort () {
  const server = http.createServer()
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve))
  const { port } = server.address()
  await new Promise((resolve) => server.close(resolve))
  return port
}

afterEach(async () => {
  for (const conn of connections.splice(0)) conn.stop()
  for (const hub of servers.splice(0)) {
    for (const { ws } of hub.accepted) ws.terminate()
    hub.wss.close()
    await new Promise((resolve) => hub.server.close(resolve))
  }
})

describe('addressFor (README "Settings", §6.1 "The endpoint")', () => {
  test('builds the cloud address from a boat ID', () => {
    expect(addressFor({ boatId: 'a1b2c3' })).toBe('wss://a1b2c3.api.wattknot.app/api/n2k/stream')
  })

  test('builds a local address from host:port', () => {
    expect(addressFor({ boatId: 'a1b2c3', localAddress: 'wattknot.local:8080' }))
      .toBe('ws://wattknot.local:8080/api/n2k/stream')
    expect(addressFor({ localAddress: '192.168.1.20:80' })).toBe('ws://192.168.1.20:80/api/n2k/stream')
    expect(addressFor({ localAddress: '[fe80::1]:8080' })).toBe('ws://[fe80::1]:8080/api/n2k/stream')
  })

  test('the MQTT door is the same host with its own path, cloud and local alike', () => {
    expect(MQTT_PATH).toBe('/api/mqtt')
    expect(addressFor({ boatId: 'a1b2c3' }, MQTT_PATH)).toBe('wss://a1b2c3.api.wattknot.app/api/mqtt')
    expect(addressFor({ localAddress: '192.168.0.153:8080' }, MQTT_PATH)).toBe('ws://192.168.0.153:8080/api/mqtt')
  })

  test('a local host without a port takes ws:// default 80, where a boat hub serves', () => {
    expect(addressFor({ localAddress: 'wattknot.local' })).toBe('ws://wattknot.local/api/n2k/stream')
  })

  test('an empty or blank local address means cloud', () => {
    expect(addressFor({ boatId: 'a1b2c3', localAddress: '' })).toBe('wss://a1b2c3.api.wattknot.app/api/n2k/stream')
    expect(addressFor({ boatId: 'a1b2c3', localAddress: '   ' })).toBe('wss://a1b2c3.api.wattknot.app/api/n2k/stream')
  })

  test('trims both fields', () => {
    expect(addressFor({ boatId: '  a1b2c3\n' })).toBe('wss://a1b2c3.api.wattknot.app/api/n2k/stream')
    expect(addressFor({ localAddress: ' wattknot.local:80 \t' })).toBe('ws://wattknot.local:80/api/n2k/stream')
  })

  test('a local address makes the boat ID unnecessary', () => {
    expect(addressFor({ boatId: '', localAddress: 'wattknot.local:80' })).toBe('ws://wattknot.local:80/api/n2k/stream')
  })

  test('refuses a missing boat ID when no local address is set', () => {
    expect(() => addressFor({})).toThrow('Boat ID is required')
    expect(() => addressFor()).toThrow('Boat ID is required')
    expect(() => addressFor({ boatId: '  ', localAddress: ' ' })).toThrow('Boat ID is required')
  })

  test('refuses a boat ID that is a hostname or URL rather than the device id alone', () => {
    for (const boatId of [
      'a1b2c3.api.wattknot.app',
      'wss://a1b2c3.api.wattknot.app/api/n2k/stream',
      'a1 b2',
      '-a1b2',
      'a1b2-',
      'a'.repeat(64)
    ]) {
      expect(() => addressFor({ boatId })).toThrow('Boat ID must be')
    }
    expect(addressFor({ boatId: 'a'.repeat(63) })).toBe(`wss://${'a'.repeat(63)}.api.wattknot.app/api/n2k/stream`)
  })

  test('refuses a local address with a scheme, path, credentials, whitespace or a bad port', () => {
    for (const localAddress of [
      'ws://wattknot.local:80',
      'wss://wattknot.local',
      'http://wattknot.local',
      'wattknot.local:80/api/n2k/stream',
      'wattknot.local/',
      'user:pass@wattknot.local',
      'watt knot.local',
      'wattknot.local:',
      'wattknot.local:abc',
      'wattknot.local:0',
      'wattknot.local:65536',
      'wattknot.local:123456',
      'fe80::1'
    ]) {
      expect(() => addressFor({ boatId: 'a1b2c3', localAddress })).toThrow('Local hub address must be host or host:port')
    }
    expect(addressFor({ localAddress: 'wattknot.local:65535' })).toBe('ws://wattknot.local:65535/api/n2k/stream')
  })
})

describe('createConnection token (§6.1 "Credential")', () => {
  const deps = { address: 'ws://127.0.0.1:1/api/n2k/stream', WebSocket, timers: fakeTimers(), onStatus () {}, onCommand () {} }

  test('refuses a missing, blank or spaced token', () => {
    expect(() => createConnection({ ...deps })).toThrow('Token is required')
    expect(() => createConnection({ ...deps, token: '   ' })).toThrow('Token is required')
    expect(() => createConnection({ ...deps, token: 'two words' })).toThrow('Token is required')
    expect(() => createConnection({ ...deps, token: 'line\nbreak' })).toThrow('Token is required')
  })
})

describe('credentialFrom (§6.1 "Credential"): the token check both feeds share', () => {
  test('returns the token as sent: trimmed', () => {
    expect(credentialFrom('  producer-token-123 ')).toBe('producer-token-123')
  })

  test('refuses a missing, blank or spaced token, in the words createConnection uses', () => {
    for (const token of [undefined, '', '   ', 'two words', 'line\nbreak']) {
      expect(() => credentialFrom(token)).toThrow('Token is required, and must contain no spaces')
    }
  })
})

describe('connect', () => {
  test('dials the stream path with the token as a Bearer header, never in the URL', async () => {
    const hub = await startHub()
    const { conn, statuses } = connect(hub.address)
    conn.start()
    await until(() => statuses.includes('connected'))

    expect(hub.accepted).toHaveLength(1)
    const [record] = hub.accepted
    expect(record.url).toBe(PATH)
    expect(record.headers.authorization).toBe(`Bearer ${TOKEN}`)
    expect(statuses).toEqual(['connecting', 'connected'])
  })

  test('offers no compression: §6.1 leaves permessage-deflate open, not negotiated', async () => {
    const hub = await startHub()
    const { conn } = connect(hub.address)
    conn.start()
    await until(() => hub.accepted.length === 1)
    expect(hub.accepted[0].headers['sec-websocket-extensions']).toBeUndefined()
  })

  test('trims the token before sending it', async () => {
    const hub = await startHub()
    const statuses = []
    const conn = createConnection({
      address: hub.address,
      token: `  ${TOKEN}\n`,
      WebSocket,
      timers: fakeTimers(),
      onStatus: (text) => statuses.push(text),
      onCommand () {}
    })
    connections.push(conn)
    conn.start()
    await until(() => hub.accepted.length === 1)
    expect(hub.accepted[0].headers.authorization).toBe(`Bearer ${TOKEN}`)
  })

  test('a send arrives at the hub as one binary message with exactly those bytes', async () => {
    const hub = await startHub()
    const { conn, statuses } = connect(hub.address)
    conn.start()
    await until(() => statuses.includes('connected'))

    const bytes = Buffer.from([0x10, 0x02, 0x93, 0x00, 0xff, 0x10, 0x10, 0x10, 0x03])
    expect(conn.send(bytes)).toBe('sent')
    await until(() => hub.accepted[0].messages.length === 1)

    const [message] = hub.accepted[0].messages
    expect(message.isBinary).toBe(true)
    expect(Buffer.compare(message.data, bytes)).toBe(0)
  })
})

describe('refusals (§6.1 "Refusals")', () => {
  test.each([
    [401, 'refused: HTTP 401 (bad or missing token)'],
    [404, 'refused: HTTP 404 (not served: wrong boat ID or address, or the hub is not on the network transport)'],
    [503, 'refused: HTTP 503 (boat offline, or the hub\'s N2K ingest is stopped)'],
    [500, 'refused: HTTP 500']
  ])('HTTP %i is shown to the operator and a redial is scheduled', async (status, text) => {
    const hub = await startHub({ refuse: status })
    const { conn, statuses, timers } = connect(hub.address)
    conn.start()
    await until(() => statuses.length === 2)

    expect(statuses).toEqual(['connecting', `${text}; redialling in 5 s`])
    expect([...timers.pending.values()].map(({ ms }) => ms)).toEqual([REDIAL_MS])

    // The aborted handshake's late error and close change nothing.
    await settle()
    expect(statuses).toHaveLength(2)
    expect(timers.pending.size).toBe(1)
  })

  test('the redial after a refusal dials again', async () => {
    const hub = await startHub({ refuse: 401 })
    const { conn, statuses, timers } = connect(hub.address)
    conn.start()
    await until(() => statuses.length === 2)
    timers.fire(REDIAL_MS)
    await until(() => statuses.length === 4)

    expect(hub.upgrades).toBe(2)
    expect(statuses[2]).toBe('connecting')
    expect(statuses[3]).toMatch(/^refused: HTTP 401/)
  })

  test('stop() cancels the redial a refusal scheduled', async () => {
    const hub = await startHub({ refuse: 503 })
    const { conn, statuses, timers } = connect(hub.address)
    conn.start()
    await until(() => statuses.length === 2)
    conn.stop()

    expect(timers.pending.size).toBe(0)
    await settle()
    expect(hub.upgrades).toBe(1)
  })
})

describe('drop rule (§6.1 "Drop, don\'t delay")', () => {
  test('down before start, and while the handshake is still in progress', async () => {
    const hub = await startHub()
    const { conn, statuses } = connect(hub.address)
    expect(conn.send(Buffer.from('x'))).toBe('down')
    conn.start()
    expect(conn.send(Buffer.from('x'))).toBe('down')
    await until(() => statuses.includes('connected'))
    expect(conn.send(Buffer.from('x'))).toBe('sent')
  })

  test('down after the hub closes, through the redial wait', async () => {
    const hub = await startHub()
    const { conn, statuses, timers } = connect(hub.address)
    conn.start()
    await until(() => hub.accepted.length === 1 && statuses.length === 2)

    hub.accepted[0].ws.close(1000)
    await until(() => timers.redials().length === 1)
    expect(conn.send(Buffer.from('x'))).toBe('down')
  })

  test('nothing dropped while down is sent after the reconnect', async () => {
    const hub = await startHub()
    const { conn, statuses, timers } = connect(hub.address)
    conn.start()
    await until(() => hub.accepted.length === 1 && statuses.length === 2)
    hub.accepted[0].ws.close(1000)
    await until(() => timers.redials().length === 1)

    expect(conn.send(Buffer.from('stale'))).toBe('down')
    timers.fire(REDIAL_MS)
    await until(() => hub.accepted.length === 2 && statuses.at(-1) === 'connected')
    expect(conn.send(Buffer.from('fresh'))).toBe('sent')
    await until(() => hub.accepted[1].messages.length === 1)
    await settle()

    expect(hub.accepted[1].messages.map(({ data }) => data.toString())).toEqual(['fresh'])
  })

  // A stub socket here: a real one cannot be made to hold a chosen
  // bufferedAmount on demand. Everything else in this file uses a real socket.
  test('busy above the cap, sent at it, and a busy write is not sent', () => {
    const sockets = []
    class StubSocket {
      static OPEN = 1
      constructor () {
        this.readyState = StubSocket.OPEN
        this.bufferedAmount = 0
        this.sent = []
        sockets.push(this)
      }

      on () {}
      send (buffer) { this.sent.push(buffer) }
      close () {}
    }
    const { conn } = connect('ws://stub/api/n2k/stream', { Socket: StubSocket })
    conn.start()
    const [socket] = sockets

    socket.bufferedAmount = BUSY_CAP_BYTES + 1
    expect(conn.send(Buffer.from('late'))).toBe('busy')
    socket.bufferedAmount = BUSY_CAP_BYTES
    expect(conn.send(Buffer.from('ok'))).toBe('sent')

    expect(socket.sent.map(String)).toEqual(['ok'])
  })
})

describe('redial (§6.1 "newest wins": a producer whose socket closes simply redials)', () => {
  test('after the hub closes the connection, a fixed delay, then a new connection', async () => {
    const hub = await startHub()
    const { conn, statuses, timers } = connect(hub.address)
    conn.start()
    await until(() => hub.accepted.length === 1 && statuses.length === 2)

    hub.accepted[0].ws.close(1000)
    await until(() => timers.redials().length === 1)
    expect(statuses.at(-1)).toBe('connection closed (code 1000); redialling in 5 s')
    expect([...timers.pending.values()][0].ms).toBe(REDIAL_MS)

    timers.fire(REDIAL_MS)
    await until(() => hub.accepted.length === 2 && statuses.at(-1) === 'connected')
    expect(hub.accepted[0].ws).not.toBe(hub.accepted[1].ws)
    // Connected again: no redial pending, only the new socket's keepalive.
    expect(timers.redials()).toHaveLength(0)
    expect(timers.keepalives()).toHaveLength(1)
  })

  test('after a connection error, a fixed delay, then a dial again', async () => {
    const address = addressFor({ localAddress: `127.0.0.1:${await closedPort()}` })
    const { conn, statuses, timers } = connect(address)
    conn.start()
    await until(() => timers.redials().length === 1)

    expect(statuses[1]).toMatch(/^connection failed: connect ECONNREFUSED 127\.0\.0\.1:\d+; redialling in 5 s$/)
    expect([...timers.pending.values()][0].ms).toBe(REDIAL_MS)
    // The error's following close changes nothing.
    await settle()
    expect(statuses).toHaveLength(2)

    timers.fire(REDIAL_MS)
    expect(statuses[2]).toBe('connecting')
    await until(() => timers.redials().length === 1)
  })
})

describe('commands (§7.1)', () => {
  async function connected () {
    const hub = await startHub()
    const harness = connect(hub.address)
    harness.conn.start()
    await until(() => hub.accepted.length === 1 && harness.statuses.length === 2)
    return { hub, ...harness, hubSocket: hub.accepted[0].ws }
  }

  test('one line per message', async () => {
    const { hubSocket, commands } = await connected()
    hubSocket.send('refresh\n')
    hubSocket.send('reboot\n')
    await until(() => commands.length === 2)
    expect(commands).toEqual(['refresh', 'reboot'])
  })

  test('two lines in one message', async () => {
    const { hubSocket, commands } = await connected()
    hubSocket.send('refresh\nreboot\n')
    await until(() => commands.length === 2)
    expect(commands).toEqual(['refresh', 'reboot'])
  })

  test('a line split across two messages is delivered once, whole', async () => {
    const { hubSocket, commands } = await connected()
    hubSocket.send('reb')
    await settle()
    expect(commands).toEqual([])
    hubSocket.send('oot\n')
    await until(() => commands.length === 1)
    expect(commands).toEqual(['reboot'])
  })

  test('\\r\\n is tolerated and the \\r stripped', async () => {
    const { hubSocket, commands } = await connected()
    hubSocket.send('refresh\r\nreboot\r\n')
    await until(() => commands.length === 2)
    expect(commands).toEqual(['refresh', 'reboot'])
  })

  test('an empty line is not delivered: it can carry no token', async () => {
    const { hubSocket, commands } = await connected()
    hubSocket.send('\n\r\n')
    hubSocket.send('refresh\n')
    await until(() => commands.length === 1)
    await settle()
    expect(commands).toEqual(['refresh'])
  })

  test('unknown lines are delivered as they are: their meaning is the caller\'s', async () => {
    const { hubSocket, commands } = await connected()
    hubSocket.send('FLY TO THE MOON\n')
    await until(() => commands.length === 1)
    expect(commands).toEqual(['FLY TO THE MOON'])
  })

  test('a binary message from the hub is ignored', async () => {
    const { hubSocket, commands } = await connected()
    hubSocket.send(Buffer.from('reboot\n'), { binary: true })
    hubSocket.send('refresh\n')
    await until(() => commands.length === 1)
    await settle()
    expect(commands).toEqual(['refresh'])
  })

  test('a partial line from a dropped connection never prefixes the next one\'s', async () => {
    const { hub, hubSocket, commands, timers, statuses, conn } = await connected()
    hubSocket.send('reb')
    await settle()
    hubSocket.close(1000)
    await until(() => timers.redials().length === 1)
    timers.fire(REDIAL_MS)
    await until(() => hub.accepted.length === 2 && statuses.at(-1) === 'connected')

    hub.accepted[1].ws.send('oot\nrefresh\n')
    await until(() => commands.length === 2)
    expect(commands).toEqual(['oot', 'refresh'])
    conn.stop()
  })
})

describe('lifecycle', () => {
  test('stop() while open closes the socket normally and goes quiet', async () => {
    const hub = await startHub()
    const { conn, statuses, timers } = connect(hub.address)
    conn.start()
    await until(() => hub.accepted.length === 1 && statuses.length === 2)

    conn.stop()
    expect(conn.send(Buffer.from('x'))).toBe('down')
    await until(() => hub.accepted[0].closed)
    await settle()

    expect(statuses).toHaveLength(2)
    expect(timers.pending.size).toBe(0)
  })

  test('stop() while the handshake is in progress leaves no redial', async () => {
    const hub = await startHub()
    const { conn, statuses, timers } = connect(hub.address)
    conn.start()
    conn.stop()
    await settle()

    expect(statuses).toEqual(['connecting'])
    expect(timers.pending.size).toBe(0)
  })

  test('stop() during the redial wait cancels the redial', async () => {
    const hub = await startHub()
    const { conn, statuses, timers } = connect(hub.address)
    conn.start()
    await until(() => hub.accepted.length === 1 && statuses.length === 2)
    hub.accepted[0].ws.close(1000)
    await until(() => timers.redials().length === 1)

    conn.stop()
    expect(timers.pending.size).toBe(0)
    await settle()
    expect(hub.upgrades).toBe(1)
  })

  test('stop() before start, and stop() twice, are harmless', async () => {
    const hub = await startHub()
    const { conn, statuses } = connect(hub.address)
    conn.stop()
    conn.start()
    await until(() => statuses.length === 2)
    conn.stop()
    conn.stop()
    await settle()
    expect(statuses).toHaveLength(2)
  })

  test('start() twice dials once', async () => {
    const hub = await startHub()
    const { conn, statuses } = connect(hub.address)
    conn.start()
    conn.start()
    await until(() => statuses.length === 2)
    await settle()
    expect(hub.upgrades).toBe(1)
  })

  test('start() right after stop() gives exactly one new socket and no redial from the old', async () => {
    const hub = await startHub()
    const { conn, statuses, timers } = connect(hub.address)
    conn.start()
    await until(() => hub.accepted.length === 1 && statuses.length === 2)

    conn.stop()
    conn.start()
    await until(() => hub.accepted.length === 2 && hub.accepted[0].closed)
    await until(() => statuses.at(-1) === 'connected')
    await settle()

    expect(hub.upgrades).toBe(2)
    // No redial from the old socket; one keepalive, the new socket's.
    expect(timers.redials()).toHaveLength(0)
    expect(timers.keepalives()).toHaveLength(1)
    expect(statuses).toEqual([
      'connecting', 'connected',
      'connecting', 'connected'
    ])
    expect(hub.accepted[1].closed).toBe(false)
  })

  // A stub socket here: a real one cannot be made to deliver its events on
  // cue after stop(). Firing every handler of the stopped socket by hand
  // proves none of them reaches the callbacks or schedules a redial.
  test('no handler of a stopped socket fires a status, a command or a redial', () => {
    const sockets = []
    class StubSocket {
      static OPEN = 1
      constructor () {
        this.handlers = {}
        this.readyState = 0
        sockets.push(this)
      }

      on (event, handler) { this.handlers[event] = handler }
      close () {}
      terminate () {}
    }
    const { conn, statuses, commands, timers } = connect('ws://stub/api/n2k/stream', { Socket: StubSocket })
    conn.start()
    conn.stop()
    conn.start()
    expect(sockets).toHaveLength(2)
    const before = [...statuses]

    const { handlers } = sockets[0]
    handlers.open()
    handlers.message(Buffer.from('reboot\n'), false)
    handlers['unexpected-response']({}, { statusCode: 401 })
    handlers.error(new Error('late'))
    handlers.close(1006)

    expect(statuses).toEqual(before)
    expect(commands).toEqual([])
    expect(timers.pending.size).toBe(0)
  })
})

describe('refusals are flagged, other states are not (§6.1 "Refusals")', () => {
  test('a refusal carries refused: true; connecting, connected and a close carry false', async () => {
    const refusing = await startHub({ refuse: 401 })
    const a = connect(refusing.address)
    a.conn.start()
    await until(() => a.statuses.length === 2)
    expect(a.refused).toEqual([false, true])

    const hub = await startHub()
    const b = connect(hub.address)
    b.conn.start()
    await until(() => b.statuses.length === 2)
    hub.accepted[0].ws.close(1000)
    await until(() => b.statuses.length === 3)
    expect(b.refused).toEqual([false, false, false])
    // Only the link coming up is flagged connected: the caller's cue to start
    // counting drops that mean something.
    expect(a.connected).toEqual([false, false])
    expect(b.connected).toEqual([false, true, false])
  })
})

describe('dead-link detection (§6.1 "Dead links")', () => {
  test('passes ws a handshake timeout, so an unanswered upgrade is an error and a redial', () => {
    const options = []
    class SpySocket {
      static OPEN = 1
      constructor (url, opts) { options.push(opts); this.readyState = 0 }
      on () {}
      close () {}
    }
    const { conn } = connect('ws://stub/api/n2k/stream', { Socket: SpySocket })
    conn.start()
    expect(options[0].handshakeTimeout).toBe(10000)
  })

  test('once connected, a ping every interval, answered by the hub, and the link stays up', async () => {
    const hub = await startHub()
    const pings = []
    const { conn, statuses, timers } = connect(hub.address)
    conn.start()
    await until(() => hub.accepted.length === 1 && statuses.length === 2)
    hub.accepted[0].ws.on('ping', () => pings.push(Date.now()))

    expect(timers.keepalives()).toHaveLength(1)
    timers.fire(KEEPALIVE_MS)
    await until(() => pings.length === 1)
    // The pong has to land before the next ping is due, or the link reads dead.
    await settle()
    timers.fire(KEEPALIVE_MS)
    await until(() => pings.length === 2)
    await settle()

    expect(statuses).toHaveLength(2) // still just connecting, connected
    expect(hub.accepted[0].closed).toBe(false)
    expect(timers.keepalives()).toHaveLength(1)
    expect(timers.redials()).toHaveLength(0)
  })

  // A stub socket: a real one cannot be made to swallow a ping on cue. Its
  // ping() does nothing, which is exactly what a half-open connection does.
  test('no pong by the next ping: the socket is terminated, reported, and a redial scheduled', () => {
    const sockets = []
    class DeadSocket {
      static OPEN = 1
      constructor () {
        this.handlers = {}
        this.readyState = DeadSocket.OPEN
        this.pings = 0
        this.terminated = 0
        sockets.push(this)
      }

      on (event, handler) { this.handlers[event] = handler }
      ping () { this.pings++ }
      terminate () { this.terminated++ }
      close () {}
    }
    const { conn, statuses, refused, timers } = connect('ws://stub/api/n2k/stream', { Socket: DeadSocket })
    conn.start()
    const [socket] = sockets
    socket.handlers.open()

    timers.fire(KEEPALIVE_MS) // ping 1, unanswered
    expect(socket.pings).toBe(1)
    expect(socket.terminated).toBe(0)
    timers.fire(KEEPALIVE_MS) // ping 1 still unanswered: dead
    expect(socket.terminated).toBe(1)
    expect(socket.pings).toBe(1)
    expect(statuses.at(-1)).toBe('link dead: no reply to ping in 10 s; redialling in 5 s')
    expect(refused.at(-1)).toBe(false)
    expect(timers.redials()).toHaveLength(1)
    expect(timers.keepalives()).toHaveLength(0)
    expect(conn.send(Buffer.from('x'))).toBe('down')
  })

  test('a pong resets the wait: pings keep flowing while they are answered', () => {
    const sockets = []
    class StubSocket {
      static OPEN = 1
      constructor () {
        this.handlers = {}
        this.readyState = StubSocket.OPEN
        this.pings = 0
        this.terminated = 0
        sockets.push(this)
      }

      on (event, handler) { this.handlers[event] = handler }
      ping () { this.pings++; this.handlers.pong() }
      terminate () { this.terminated++ }
      close () {}
    }
    const { conn, timers } = connect('ws://stub/api/n2k/stream', { Socket: StubSocket })
    conn.start()
    const [socket] = sockets
    socket.handlers.open()

    for (let i = 0; i < 5; i++) timers.fire(KEEPALIVE_MS)
    expect(socket.pings).toBe(5)
    expect(socket.terminated).toBe(0)
    expect(timers.keepalives()).toHaveLength(1)
  })

  test('stop() clears the keepalive, and a stopped socket\'s late pong or keepalive changes nothing', () => {
    const sockets = []
    class StubSocket {
      static OPEN = 1
      constructor () {
        this.handlers = {}
        this.readyState = StubSocket.OPEN
        this.pings = 0
        sockets.push(this)
      }

      on (event, handler) { this.handlers[event] = handler }
      ping () { this.pings++ }
      terminate () {}
      close () {}
    }
    const { conn, statuses, timers } = connect('ws://stub/api/n2k/stream', { Socket: StubSocket })
    conn.start()
    const [socket] = sockets
    socket.handlers.open()
    expect(timers.keepalives()).toHaveLength(1)

    conn.stop()
    expect(timers.pending.size).toBe(0)
    const before = [...statuses]
    socket.handlers.pong()
    expect(statuses).toEqual(before)
  })

  test('a keepalive that fires for a socket no longer current does nothing', () => {
    // The timer is cleared on drop and stop, so this is the belt to that
    // brace: the callback itself checks the socket. Reached by firing a timer
    // the fake kept after the connection let go of its handle.
    const sockets = []
    class StubSocket {
      static OPEN = 1
      constructor () {
        this.handlers = {}
        this.readyState = StubSocket.OPEN
        this.pings = 0
        sockets.push(this)
      }

      on (event, handler) { this.handlers[event] = handler }
      ping () { this.pings++ }
      terminate () {}
      close () {}
    }
    const { conn, timers } = connect('ws://stub/api/n2k/stream', { Socket: StubSocket })
    conn.start()
    sockets[0].handlers.open()
    const [keepalive] = timers.keepalives()
    conn.stop()
    conn.start()
    sockets[1].handlers.open()

    keepalive.fn() // the first socket's keepalive, fired by hand after stop() cleared it
    expect(sockets[0].pings).toBe(0)
    expect(sockets[1].pings).toBe(0)
    expect(timers.keepalives()).toHaveLength(1)
  })
})
