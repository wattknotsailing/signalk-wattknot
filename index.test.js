'use strict'

// The Signal K glue against a fake `app` (README "Testing": on,
// removeListener, setPluginStatus, setPluginError) and a real `ws` server on
// loopback, with the real stream and connection behind it. The heartbeat and
// redial timers and the clock are injected and driven by hand, except in the
// one test that runs the production wiring as Signal K would.

const http = require('http')
const { EventEmitter } = require('events')
const { WebSocket, WebSocketServer } = require('ws')
const { getPGNs } = require('@canboat/pgns')
const plugin = require('./index')
const { BATCH_MS } = require('./lib/stream')
const { createPlugin, fwFrom } = plugin
const pkg = require('./package.json')

const TOKEN = 'producer-token-123'
const RAW = 'canboatjs:rawoutput'

// The status-line form chosen in index.js, restated so a change shows up here
// as a decision.
// Before the link first comes up there is no dropped part (txd/txl null).
const status = (text, rx, txd = null, txl = null, victron = 'off') =>
  `${text} · Victron: ${victron} · ${rx} frames/s` + (txd === null ? '' : ` · dropped ${txd} slow link, ${txl} no link`)

// A Signal K app as the plugin sees it: an emitter for the tap, and the two
// reporting calls recorded in order.
function fakeApp () {
  const emitter = new EventEmitter()
  const app = {
    statuses: [],
    errors: [],
    attached: [],
    detached: [],
    on (event, fn) {
      app.attached.push({ event, fn })
      emitter.on(event, fn)
    },
    removeListener (event, fn) {
      app.detached.push({ event, fn })
      emitter.removeListener(event, fn)
    },
    setPluginStatus (text) { app.statuses.push(text) },
    setPluginError (text) { app.errors.push(text) },
    // Signal K's plugin logging: debug to the server's debug log, error to its
    // error log. Recorded so a test can assert what the owner would find there.
    logged: [],
    debug (text) { app.logged.push(['debug', text]) },
    error (text) { app.logged.push(['error', text]) },
    emit: (event, data) => emitter.emit(event, data),
    listeners: (event) => emitter.listenerCount(event)
  }
  return app
}

// Hand-driven timers: nothing fires until the test says so. Timeouts are the
// connection's redials; intervals are the plugin's heartbeat.
function fakeTimers () {
  let nextId = 1
  const timers = {
    timeouts: new Map(),
    intervals: new Map(),
    intervalsSet: 0,
    intervalsCleared: 0,
    setTimeout (fn, ms) {
      // The stream's batch window (BATCH_MS) runs on a real timer, so bytes
      // reach the hub as the tests await them; the plugin's own long timers
      // (redial, retry) stay held for the test to fire.
      if (ms === BATCH_MS) return setTimeout(fn, 0)
      const id = nextId++
      timers.timeouts.set(id, { fn, ms })
      return id
    },
    clearTimeout (id) { timers.timeouts.delete(id) },
    setInterval (fn, ms) {
      const id = nextId++
      timers.intervalsSet++
      timers.intervals.set(id, { fn, ms })
      return id
    },
    clearInterval (id) {
      timers.intervalsCleared++
      timers.intervals.delete(id)
    },
    // One heartbeat: every live interval fires once.
    tick () {
      for (const { fn } of [...timers.intervals.values()]) fn()
    }
  }
  return timers
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

const hex = (buffer) => [...buffer].map((b) => b.toString(16).padStart(2, '0')).join(' ')

const servers = []
const plugins = []

// A hub on 127.0.0.1, port 0, recording each accepted connection with its
// request and the bytes it received. `refuse` answers every upgrade with that
// HTTP status instead of 101.
async function startHub ({ refuse = null } = {}) {
  const server = http.createServer()
  const wss = new WebSocketServer({ noServer: true })
  const hub = { server, wss, accepted: [] }
  server.on('upgrade', (request, socket, head) => {
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
  hub.localAddress = `127.0.0.1:${server.address().port}`
  hub.url = `ws://${hub.localAddress}/api/n2k/stream`
  servers.push(hub)
  return hub
}

// Every byte one accepted connection has received, as one buffer.
const received = (record) => Buffer.concat(record.messages.map((m) => m.data))

// A plugin with recording app, hand-driven timers and clock, and the real
// ws client and canboat table unless a test swaps one.
function setup ({ WebSocket: Socket = WebSocket } = {}) {
  const app = fakeApp()
  const timers = fakeTimers()
  const clock = { t: 0, reads: 0, now: () => { clock.reads++; return clock.t } }
  const instance = createPlugin(app, { WebSocket: Socket, timers, clock, getPGNs })
  plugins.push(instance)
  return { app, timers, clock, instance }
}

// A plugin started against a hub and connected.
async function connected () {
  const hub = await startHub()
  const env = setup()
  env.instance.start({ token: TOKEN, localAddress: hub.localAddress })
  await until(() => env.app.statuses.includes('N2K: connected'))
  return { hub, ...env }
}

afterEach(async () => {
  for (const instance of plugins.splice(0)) instance.stop()
  for (const hub of servers.splice(0)) {
    for (const { ws } of hub.accepted) ws.terminate()
    hub.wss.close()
    await new Promise((resolve) => hub.server.close(resolve))
  }
})

// The single-frame vector from lib/stream.test.js, verbatim: Battery Status
// 127508 from src 37, framed at 258 ms after the stream was created.
const BATTERY = {
  pgn: { canId: 435295269, prio: 6, src: 37, pgn: 127508, dst: 255 },
  length: 8,
  data: ['00', '2e', '05', '00', '00', 'ff', 'ff', '84']
}
const BATTERY_FRAME = '10 02 93 13 06 14 f2 01 ff 25 02 01 00 00 08 00 2e 05 00 00 ff ff 84 69 10 03'

describe('plugin shape (Signal K plugin API, README "Settings")', () => {
  test('id is the package name; name and description are set', () => {
    const instance = plugin(fakeApp())
    expect(instance.id).toBe('signalk-wattknot')
    expect(instance.id).toBe(pkg.name)
    expect(typeof instance.name).toBe('string')
    expect(instance.name).not.toBe('')
    expect(typeof instance.description).toBe('string')
    expect(instance.description).not.toBe('')
    expect(typeof instance.start).toBe('function')
    expect(typeof instance.stop).toBe('function')
  })

  test('the settings are the local hub address, boat ID and token, one switch per feed, both on, and "Allow switches", off', () => {
    const { schema } = plugin(fakeApp())
    expect(schema.type).toBe('object')
    // Row order on the page: the two addresses, the token, then the switches.
    expect(Object.keys(schema.properties)).toEqual(['localAddress', 'boatId', 'token', 'sendN2k', 'sendVictron', 'allowSwitches'])
    for (const field of Object.values(schema.properties)) {
      expect(field.title).not.toBe('')
      expect(field.description).not.toBe('')
    }
    for (const key of ['localAddress', 'boatId', 'token']) expect(schema.properties[key].type).toBe('string')
    expect(schema.properties.sendN2k).toMatchObject({ type: 'boolean', title: 'Send NMEA 2000 data', default: true })
    expect(schema.properties.sendVictron).toMatchObject({ type: 'boolean', title: 'Send Victron data', default: true })
    expect(schema.properties.allowSwitches).toMatchObject({ type: 'boolean', title: 'Allow switches', default: false })
    // The owner's yes covers exactly what lib/victron.js WRITABLE admits, and
    // the words say so.
    expect(schema.properties.allowSwitches.description).toBe(
      'Let your hub switch this GX\'s relay outputs and set your MultiPlus or Quattro\'s mode, and nothing else. Off by default.'
    )
    // The boat ID is not required by the form: a local address makes it unnecessary.
    expect(schema.required).toEqual(['token'])
  })

  test('the uiSchema hides only the local hub address; the token stays plain text, the rest is the schema as written', () => {
    expect(plugin(fakeApp()).uiSchema).toEqual({ localAddress: { 'ui:widget': 'hidden' } })
  })
})

describe('the production wiring', () => {
  test('real ws, timers, clock and table: dials with the token and heartbeats on a real 1 s timer', async () => {
    const hub = await startHub()
    const app = fakeApp()
    const instance = plugin(app)
    plugins.push(instance)
    instance.start({ token: TOKEN, localAddress: hub.localAddress })
    await until(() => hub.accepted.length === 1)
    expect(hub.accepted[0].headers.authorization).toBe(`Bearer ${TOKEN}`)
    await until(() => received(hub.accepted[0]).toString('ascii').includes('\n'))
    expect(received(hub.accepted[0]).toString('ascii'))
      .toMatch(new RegExp(`^\\$WKT,v=4,kind=2,up=[01],rx=0,txd=0,txl=0,fw=${fwFrom(pkg.version)}\\n$`))
    instance.stop()
    expect(app.listeners(RAW)).toBe(0)
  })
})

describe('fw (§5, v4 formula)', () => {
  test('major × 10000 + minor × 100 + patch', () => {
    expect(fwFrom('0.1.0')).toBe(100)
    expect(fwFrom('1.2.3')).toBe(10203)
    expect(fwFrom('1.1.0')).toBe(10100)
    expect(fwFrom('0.0.0')).toBe(0)
    expect(fwFrom('12.99.99')).toBe(129999)
  })

  test('a prerelease or build suffix reads as its release', () => {
    expect(fwFrom('0.2.0-beta.1')).toBe(200)
    expect(fwFrom('1.2.3+build.7')).toBe(10203)
  })

  test('refuses a minor or patch above 99, which would collide with another build', () => {
    expect(() => fwFrom('1.100.0')).toThrow(RangeError)
    expect(() => fwFrom('1.0.100')).toThrow(RangeError)
  })

  test('refuses what is not a semver version', () => {
    for (const version of ['1.2', 'v1.2.3', '1.2.3.4', '', 'a.b.c']) {
      expect(() => fwFrom(version)).toThrow(RangeError)
    }
  })

  test('the stream is given this package\'s version', async () => {
    const { hub, timers } = await connected()
    timers.tick()
    await until(() => hub.accepted[0].messages.length === 1)
    expect(received(hub.accepted[0]).toString('ascii')).toMatch(new RegExp(`,fw=${fwFrom(pkg.version)}\\n$`))
  })
})

describe('settings errors', () => {
  // Counts constructions, so "dials nothing" is checked rather than assumed.
  function counting () {
    const counter = { made: 0 }
    counter.Socket = class extends WebSocket {
      constructor (...args) {
        counter.made++
        super(...args)
      }
    }
    return counter
  }

  test('no boat ID and no local address: the owner is told, nothing dials, no tap, no timer', () => {
    const counter = counting()
    const { app, timers, instance } = setup({ WebSocket: counter.Socket })
    instance.start({ token: TOKEN })
    expect(app.errors).toEqual(['Boat ID is required when no local hub address is set'])
    expect(counter.made).toBe(0)
    expect(app.attached).toEqual([])
    expect(timers.intervalsSet).toBe(0)
    expect(app.statuses).toEqual([])
  })

  test('a bad token: the owner is told, nothing dials, no tap, no timer', () => {
    const counter = counting()
    const { app, timers, instance } = setup({ WebSocket: counter.Socket })
    instance.start({ boatId: 'a1b2c3', token: 'two words' })
    instance.start({ boatId: 'a1b2c3' })
    expect(app.errors).toEqual([
      'Token is required, and must contain no spaces',
      'Token is required, and must contain no spaces'
    ])
    expect(counter.made).toBe(0)
    expect(app.attached).toEqual([])
    expect(timers.intervalsSet).toBe(0)
  })

  test('a malformed boat ID or local address is reported as connection.js words it', () => {
    const { app, instance } = setup()
    instance.start({ boatId: 'a1b2c3.api.wattknot.app', token: TOKEN })
    instance.start({ localAddress: 'ws://wattknot.local', token: TOKEN })
    expect(app.errors).toEqual([
      'Boat ID must be the hub\'s device id alone: letters, digits and hyphens (got "a1b2c3.api.wattknot.app")',
      'Local hub address must be host or host:port, with no scheme or path (got "ws://wattknot.local")'
    ])
    expect(app.attached).toEqual([])
  })

  test('start() with no settings at all is a settings error, not a crash', () => {
    const { app, instance } = setup()
    instance.start(undefined)
    expect(app.errors).toEqual(['Boat ID is required when no local hub address is set'])
  })

  test('a failed start leaves nothing for stop() to undo', () => {
    const { app, timers, instance } = setup()
    instance.start({})
    instance.stop()
    expect(app.detached).toEqual([])
    expect(timers.intervalsCleared).toBe(0)
  })
})

describe('end to end on loopback', () => {
  test('the upgrade arrives on the stream path with the token as a Bearer header', async () => {
    const { hub } = await connected()
    expect(hub.accepted).toHaveLength(1)
    expect(hub.accepted[0].url).toBe('/api/n2k/stream')
    expect(hub.accepted[0].headers.authorization).toBe(`Bearer ${TOKEN}`)
  })

  test('one single-frame rawoutput event arrives as exactly its Actisense frame (§2.1)', async () => {
    const { hub, app, clock } = await connected()
    // The stream was created at t = 0; the vector was framed 258 ms later.
    clock.t = 258
    app.emit(RAW, BATTERY)
    await until(() => hub.accepted[0].messages.length === 1)
    expect(hub.accepted[0].messages[0].isBinary).toBe(true)
    expect(hex(received(hub.accepted[0]))).toBe(BATTERY_FRAME)
  })

  test('a fast packet is reassembled by the real canboat table before it is sent (§2)', async () => {
    const { hub, app } = await connected()
    // The 19-byte 129029 message from lib/stream.test.js: src 7, prio 3, three frames.
    const id = { prio: 3, src: 7, pgn: 129029, dst: 255 }
    const frames = [
      ['40', '13', '21', '22', '23', '24', '25', '26'],
      ['41', '27', '28', '29', '2a', '2b', '2c', '2d'],
      ['42', '2e', '2f', '30', '31', '32', '33', 'ff']
    ]
    for (const data of frames) app.emit(RAW, { pgn: id, length: 8, data })
    await until(() => hub.accepted[0].messages.length === 1)
    await settle()
    expect(hub.accepted[0].messages).toHaveLength(1)
    expect(hex(received(hub.accepted[0]))).toBe(
      '10 02 93 1e 03 05 f8 01 ff 07 00 00 00 00 13 ' +
      '21 22 23 24 25 26 27 28 29 2a 2b 2c 2d 2e 2f 30 31 32 33 17 10 03'
    )
  })

  test('a heartbeat tick writes the v4 kind 2 line with this package\'s fw (§3–§5)', async () => {
    const { hub, app, timers, clock } = await connected()
    app.emit(RAW, BATTERY)
    await until(() => hub.accepted[0].messages.length === 1)
    clock.t = 3000
    timers.tick()
    await until(() => hub.accepted[0].messages.length === 2)
    expect(hub.accepted[0].messages[1].isBinary).toBe(true)
    expect(hub.accepted[0].messages[1].data.toString('ascii'))
      .toBe(`$WKT,v=4,kind=2,up=3,rx=1,txd=0,txl=0,fw=${fwFrom(pkg.version)}\n`)
  })

  test('the heartbeat timer runs at 1 Hz (§4)', async () => {
    const { timers } = await connected()
    expect([...timers.intervals.values()].map((i) => i.ms)).toEqual([1000])
  })
})

describe('status line', () => {
  test('the connection\'s text is shown as it changes', async () => {
    const { hub, app } = await connected()
    expect(app.statuses).toEqual(['N2K: connecting', 'N2K: connected'])
    expect(app.errors).toEqual([])
  })

  test('the counters are appended at each heartbeat, in the pinned form', async () => {
    const { hub, app, timers } = await connected()
    app.emit(RAW, BATTERY)
    app.emit(RAW, BATTERY)
    timers.tick()
    expect(app.statuses.at(-1)).toBe(status('N2K: connected', 2, 0, 0))
    expect(app.statuses.at(-1))
      .toBe('N2K: connected · Victron: off · 2 frames/s · dropped 0 slow link, 0 no link')
  })

  test('the counters are what the heartbeat line tells the hub: rx per second, drops since start', async () => {
    const hub = await startHub()
    const { app, timers, instance } = setup()
    instance.start({ token: TOKEN, localAddress: hub.localAddress })
    // Still connecting: every write is dropped as link-down (txl).
    const connecting = 'N2K: connecting'
    app.emit(RAW, BATTERY)
    app.emit(RAW, BATTERY)
    app.emit(RAW, BATTERY)
    timers.tick()
    expect(app.statuses.at(-1)).toBe(status(connecting, 3))
    // The first line itself was dropped too, and rx restarted from 0.
    timers.tick()
    expect(app.statuses.at(-1)).toBe(status(connecting, 0))
  })

  test('the latest connection text carries the counters after a change', async () => {
    const { hub, app, timers } = await connected()
    hub.accepted[0].ws.close()
    await until(() => app.statuses.some((s) => s.startsWith('N2K: connection closed')))
    timers.tick()
    expect(app.statuses.at(-1)).toBe(status('N2K: connection closed (code 1005); redialling in 5 s', 0, 0, 0))
    // A frame now has no link to go on, nor had the heartbeat line the tick
    // above wrote: both counted when their batch window closes, because the
    // link HAD come up.
    app.emit(RAW, BATTERY)
    await settle()
    timers.tick()
    expect(app.statuses.at(-1)).toBe(status('N2K: connection closed (code 1005); redialling in 5 s', 1, 0, 2))
  })
})

describe('commands from the hub (§7)', () => {
  test('refresh is ignored: this producer is listen-only', async () => {
    const { hub, app, timers } = await connected()
    const before = app.statuses.length
    hub.accepted[0].ws.send('refresh\n')
    await settle()
    expect(hub.accepted).toHaveLength(1)
    expect(hub.accepted[0].closed).toBe(false)
    expect(app.detached).toEqual([])
    expect(timers.intervalsCleared).toBe(0)
    expect(app.statuses).toHaveLength(before)
  })

  test('reboot restarts the N2K feed: the old socket closes, a new one dials, one tap, the same timer', async () => {
    const { hub, app, timers } = await connected()
    const firstTap = app.attached[0].fn
    hub.accepted[0].ws.send('reboot\n')
    await until(() => hub.accepted.length === 2)
    await until(() => hub.accepted[0].closed)
    expect(hub.accepted[1].headers.authorization).toBe(`Bearer ${TOKEN}`)
    expect(app.detached).toEqual([{ event: RAW, fn: firstTap }])
    expect(app.attached).toHaveLength(2)
    expect(app.listeners(RAW)).toBe(1)
    expect(timers.intervalsSet).toBe(1)
    expect(timers.intervalsCleared).toBe(0)
    expect(timers.intervals.size).toBe(1)
  })

  test('after a reboot the status is the new link\'s: no dropped counts until it first comes up', async () => {
    const { hub, app, timers } = await connected()
    timers.tick()
    expect(app.statuses.at(-1)).toBe(status('N2K: connected', 0, 0, 0))
    // The hub stops answering upgrades, so the new link stays dialling; the
    // held sockets are destroyed at the end, or the server could not close.
    const held = []
    hub.server.removeAllListeners('upgrade')
    hub.server.on('upgrade', (request, socket) => held.push(socket))
    hub.accepted[0].ws.send('reboot\n')
    await until(() => app.statuses.filter((s) => s === 'N2K: connecting').length === 2)
    timers.tick()
    expect(app.statuses.at(-1)).toBe(status('N2K: connecting', 0))
    await until(() => held.length === 1)
    for (const socket of held) socket.destroy()
  })

  test('reboot resets up to ~0: a new stream, a new start (§5.3)', async () => {
    const { hub, app, timers, clock } = await connected()
    clock.t = 7000
    hub.accepted[0].ws.send('reboot\n')
    await until(() => app.statuses.filter((s) => s === 'N2K: connected').length === 2)
    clock.t = 9000
    timers.tick()
    await until(() => hub.accepted[1].messages.length === 1)
    expect(hub.accepted[1].messages[0].data.toString('ascii')).toMatch(/^\$WKT,v=4,kind=2,up=2,/)
  })

  test('the lines after a reboot in the same message reach nothing: they were for the old instance', async () => {
    const { hub, app, timers } = await connected()
    hub.accepted[0].ws.send('reboot\nreboot\n')
    await until(() => hub.accepted.length === 2)
    await settle()
    expect(hub.accepted).toHaveLength(2)
    expect(app.attached).toHaveLength(2)
    expect(timers.intervalsSet).toBe(1)
  })

  test('a frame through the old tap after a reboot reaches nothing: it was for the old stream', async () => {
    const { hub, app, clock } = await connected()
    const oldTap = app.attached[0].fn
    hub.accepted[0].ws.send('reboot\n')
    await until(() => app.statuses.filter((s) => s === 'N2K: connected').length === 2)
    const reads = clock.reads
    oldTap(BATTERY)
    await settle()
    expect(clock.reads).toBe(reads)
    expect(hub.accepted[1].messages).toEqual([])
  })

  test('an unknown line is ignored (§7.1)', async () => {
    const { hub, app, timers } = await connected()
    hub.accepted[0].ws.send('REBOOT\nreboot now\nshutdown\n')
    await settle()
    expect(hub.accepted).toHaveLength(1)
    expect(app.detached).toEqual([])
    expect(timers.intervalsCleared).toBe(0)
  })
})

describe('the tap runs inside canboatjs\'s emit', () => {
  // An event whose every read throws: what a frame that breaks the stream
  // looks like from the emit's side.
  const throwing = (message) => ({ get pgn () { throw new Error(message) } })

  test('a frame that throws never throws into the emit: logged once at error, and the frames after it still go', async () => {
    const { hub, app, timers } = await connected()
    for (let i = 0; i < 3; i++) expect(() => app.emit(RAW, throwing('bad event'))).not.toThrow()
    expect(app.logged).toEqual([['error', 'N2K tap: a CAN frame threw: bad event']])

    // Frames pass again: the streak ends with its count, and the frame counts and goes.
    app.emit(RAW, BATTERY)
    expect(app.logged).toEqual([
      ['error', 'N2K tap: a CAN frame threw: bad event'],
      ['debug', 'N2K tap: "bad event" 3 time(s) in a row']
    ])
    timers.tick()
    expect(app.statuses.at(-1)).toBe(status('N2K: connected', 1, 0, 0))
    // The frame and the heartbeat line after it, one batch: the line says rx=1.
    await until(() => received(hub.accepted[0]).toString('latin1').includes('\n'))
    expect(received(hub.accepted[0]).toString('latin1')).toMatch(/\$WKT,v=4,kind=2,up=0,rx=1,/)
  })

  test('a different error is new information: at error again, the streak before it closed with its count', async () => {
    const { app } = await connected()
    app.emit(RAW, throwing('one'))
    app.emit(RAW, throwing('one'))
    app.emit(RAW, throwing('two'))
    expect(app.logged).toEqual([
      ['error', 'N2K tap: a CAN frame threw: one'],
      ['debug', 'N2K tap: "one" 2 time(s) in a row'],
      ['error', 'N2K tap: a CAN frame threw: two']
    ])
  })
})

describe('lifecycle', () => {
  test('stop() detaches the same tap it attached, clears the timer and closes the socket', async () => {
    const { hub, app, timers, instance } = await connected()
    const [{ event, fn }] = app.attached
    expect(event).toBe(RAW)
    instance.stop()
    expect(app.detached).toEqual([{ event: RAW, fn }])
    expect(app.listeners(RAW)).toBe(0)
    expect(timers.intervalsCleared).toBe(1)
    expect(timers.intervals.size).toBe(0)
    await until(() => hub.accepted[0].closed)
  })

  test('a frame or a tick after stop() reaches nothing, even through a reference held from before', async () => {
    const { hub, app, timers, clock, instance } = await connected()
    const tap = app.attached[0].fn
    const [{ fn: heartbeat }] = [...timers.intervals.values()]
    const statuses = app.statuses.length
    instance.stop()
    const reads = clock.reads
    app.emit(RAW, BATTERY)
    tap(BATTERY)
    heartbeat()
    await settle()
    // The stream's clock is read on every frame it encodes; untouched means
    // the stream was never reached.
    expect(clock.reads).toBe(reads)
    expect(app.statuses).toHaveLength(statuses)
    expect(hub.accepted[0].messages).toEqual([])
  })

  test('start → stop → start attaches exactly one tap and runs exactly one timer', async () => {
    const hub = await startHub()
    const { app, timers, instance } = setup()
    const options = { token: TOKEN, localAddress: hub.localAddress }
    instance.start(options)
    instance.stop()
    instance.start(options)
    expect(app.attached).toHaveLength(2)
    expect(app.detached).toEqual([{ event: RAW, fn: app.attached[0].fn }])
    expect(app.listeners(RAW)).toBe(1)
    expect(timers.intervals.size).toBe(1)
    await until(() => app.statuses.at(-1) === 'N2K: connected')
    await settle()
    // The first start's socket was stopped before it opened; only the second is live.
    expect(hub.accepted.filter((r) => !r.closed)).toHaveLength(1)
  })

  test('stop() before start() and stop() twice are harmless', async () => {
    const { app, timers, instance } = setup()
    instance.stop()
    expect(app.detached).toEqual([])
    const hub = await startHub()
    instance.start({ token: TOKEN, localAddress: hub.localAddress })
    instance.stop()
    instance.stop()
    expect(app.detached).toHaveLength(1)
    expect(timers.intervalsCleared).toBe(1)
  })
})

describe('refusals are the plugin error, not status (§6.1 "Refusals")', () => {
  test('a refused handshake is reported as the plugin error, stays red through the heartbeat, and holds still', async () => {
    const hub = await startHub({ refuse: 401 })
    const { app, timers, instance } = setup()
    instance.start({ token: TOKEN, localAddress: hub.localAddress })
    await until(() => app.errors.length === 1)

    const refusedText = 'N2K: refused: HTTP 401 (bad or missing token); redialling in 5 s'
    expect(app.statuses).toEqual(['N2K: connecting'])
    expect(app.errors).toEqual([refusedText])

    // The heartbeat keeps the owner's view red while refused, and nothing is
    // written as plain status. The line carries no counters: Signal K keeps
    // every error text that differs from the last as a "Last error" beneath
    // the current one, so a count that ticked every second put two changing
    // red lines on the page. Frames arriving meanwhile change nothing.
    timers.tick()
    expect(app.errors.at(-1)).toBe(`${refusedText} · Victron: off`)
    app.emit(RAW, BATTERY)
    timers.tick()
    expect(app.errors.at(-1)).toBe(`${refusedText} · Victron: off`)
    expect(app.errors).toHaveLength(3)
    expect(app.statuses).toHaveLength(1)
  })

  test('a good connection after a refusal is status again', async () => {
    const refusing = await startHub({ refuse: 401 })
    const { app, timers, instance } = setup()
    instance.start({ token: TOKEN, localAddress: refusing.localAddress })
    await until(() => app.errors.length === 1)

    // The owner fixes the token and saves: Signal K stops and starts us.
    const hub = await startHub()
    instance.stop()
    instance.start({ token: TOKEN, localAddress: hub.localAddress })
    await until(() => app.statuses.includes('N2K: connected'))
    timers.tick()
    expect(app.statuses.at(-1)).toBe(status('N2K: connected', 0, 0, 0))
    expect(app.errors).toHaveLength(1)
  })
})

// The Victron feed wired beside the stream (protocol/mqtt-producer.md). D-Bus
// is a fake in the shape lib/venus.test.js proved against the spike; the
// broker is a real aedes in-process, with a second real client as the hub;
// connectMqtt is injected so the test can point the feed's client at that
// broker over TCP while asserting the URL and options production would use.
describe('the Victron feed', () => {
  const net = require('net')
  const mqtt = require('mqtt')
  const PORTAL = '102c6bf7d60f'
  const BUS_ITEM = 'com.victronenergy.BusItem'
  const DBUS = 'org.freedesktop.DBus'
  const sig = (type) => ({ type, child: [] })
  const variant = (type, value) => [[sig(type)], [value]]
  const reply = (value) => (msg, cb) => process.nextTick(cb, null, value)

  function fakeDbus (overrides = {}) {
    const table = {
      [`${DBUS} AddMatch`]: reply(undefined),
      [`${DBUS} ListNames`]: reply(['com.victronenergy.system', 'com.victronenergy.battery.ttyUSB0', 'org.bluez']),
      [`${DBUS} GetNameOwner`]: (msg, cb) => process.nextTick(cb, null, msg.body[0] === 'com.victronenergy.system' ? ':1.10' : ':1.11'),
      'com.victronenergy.system GetValue': reply(variant('s', PORTAL)),
      'com.victronenergy.system GetItems': reply([['/Serial', [['Value', variant('s', PORTAL)]]]]),
      'com.victronenergy.battery.ttyUSB0 GetItems': reply([
        ['/DeviceInstance', [['Value', variant('i', 279)]]],
        ['/Soc', [['Value', variant('d', 87.5)]]]
      ]),
      ...overrides
    }
    const connection = new EventEmitter()
    connection.ended = 0
    connection.end = () => { connection.ended++ }
    connection.setValueShapes = () => connection
    const bus = { connection, invoke (msg, cb) { table[`${msg.destination} ${msg.member}`](msg, cb) } }
    let made = 0
    return { dbus: { systemBus () { made++; return bus } }, connection, made: () => made }
  }

  let aedes, brokerServer, brokerPort, hubClient, hubMessages, connects

  beforeEach(async () => {
    aedes = require('aedes')()
    brokerServer = net.createServer(aedes.handle)
    await new Promise((resolve) => brokerServer.listen(0, '127.0.0.1', resolve))
    brokerPort = brokerServer.address().port
    hubMessages = []
    hubClient = mqtt.connect(`mqtt://127.0.0.1:${brokerPort}`)
    hubClient.on('message', (topic, payload, packet) => hubMessages.push({ topic, payload: payload.toString(), retain: packet.retain }))
    await new Promise((resolve) => hubClient.on('connect', resolve))
    await new Promise((resolve) => hubClient.subscribe('N/#', resolve))
    connects = []
  })

  afterEach(async () => {
    for (const instance of plugins.splice(0)) instance.stop()
    hubClient.end(true)
    await new Promise((resolve) => aedes.close(resolve))
    await new Promise((resolve) => brokerServer.close(resolve))
    for (const hub of servers.splice(0)) {
      for (const { ws } of hub.accepted) ws.terminate()
      hub.wss.close()
      await new Promise((resolve) => hub.server.close(resolve))
    }
  })

  // The feed's client is pointed at the test broker over TCP, whatever URL
  // index.js built; the URL and options are recorded for assertion.
  const connectMqtt = (url, options) => {
    connects.push({ url, options })
    return mqtt.connect(`mqtt://127.0.0.1:${brokerPort}`, { reconnectPeriod: 200 })
  }

  // A plugin with the D-Bus fake and the test broker behind it. `settings`
  // replaces the default ones (this hub's address and the token) when given.
  function victronPlugin ({ dbusFake = fakeDbus(), wallClock = { now: () => 1_792_785_000_000 } } = {}) {
    const app = fakeApp()
    const timers = fakeTimers()
    const clock = { now: () => 0 }
    const instance = createPlugin(app, { WebSocket, timers, clock, getPGNs, dbus: dbusFake.dbus, connectMqtt, wallClock })
    plugins.push(instance)
    return { app, timers, instance, dbusFake }
  }

  async function startWithVictron ({ dbusFake, wallClock, settings = {} } = {}) {
    const hub = await startHub()
    const env = victronPlugin({ dbusFake, wallClock })
    env.instance.start({ token: TOKEN, localAddress: hub.localAddress, ...settings })
    return { hub, ...env }
  }

  const announce = (filters, sourceId = 'victron') => new Promise((resolve) =>
    hubClient.publish(`wattknot/hub/subscriptions/${sourceId}`, JSON.stringify(filters), { retain: true }, resolve))

  test('without a D-Bus module there is no feed: status says off and nothing is dialled', async () => {
    const hub = await startHub()
    const { app, timers, instance } = setup()
    instance.start({ token: TOKEN, localAddress: hub.localAddress })
    timers.tick()
    expect(app.statuses.at(-1)).toMatch(/ · Victron: off · /)
    expect(connects).toEqual([])
  })

  test('dials the MQTT door at the same host and token as the stream, with the contract\'s client options', async () => {
    const { hub } = await startWithVictron()
    await until(() => connects.length === 1)
    expect(connects[0].url).toBe(`ws://${hub.localAddress}/api/mqtt`)
    expect(connects[0].options).toEqual({
      wsOptions: { headers: { Authorization: `Bearer ${TOKEN}` } },
      keepalive: 30,
      reconnectPeriod: 5000,
      clean: true,
      resubscribe: false,
      clientId: `signalk-wattknot-${PORTAL}`
    })
  })

  test('end to end: the Serial retained on connect, then the full publish once the hub announces', async () => {
    const { app, timers } = await startWithVictron()
    await until(() => hubMessages.length === 1)
    expect(hubMessages[0]).toMatchObject({ topic: `N/${PORTAL}/system/0/Serial`, payload: `{"value":"${PORTAL}"}` })
    // Retention shows to a LATE subscriber (MQTT 3.1.1 §3.3.1.3: a live delivery
    // to an existing subscription carries retain 0): the wizard's identify mode.
    const late = mqtt.connect(`mqtt://127.0.0.1:${brokerPort}`)
    const seen = await new Promise((resolve) => {
      late.on('message', (topic, payload, packet) => resolve({ topic, payload: payload.toString(), retain: packet.retain }))
      late.on('connect', () => late.subscribe('N/+/system/0/Serial'))
    })
    late.end(true)
    expect(seen).toEqual({ topic: `N/${PORTAL}/system/0/Serial`, payload: `{"value":"${PORTAL}"}`, retain: true })

    await announce(['#'])
    await until(() => hubMessages.some((m) => m.topic === `N/${PORTAL}/full_publish_completed`))
    const topics = hubMessages.slice(1).map((m) => m.topic)
    expect(topics).toEqual([
      `N/${PORTAL}/battery/279/DeviceInstance`,
      `N/${PORTAL}/battery/279/Soc`,
      `N/${PORTAL}/system/0/Serial`,
      `N/${PORTAL}/full_publish_completed`
    ])
    expect(hubMessages.find((m) => m.topic === `N/${PORTAL}/battery/279/Soc`).payload).toBe('{"value":87.5}')
    expect(hubMessages.at(-1).payload).toBe('{"value":1792785000}')
    expect(hubMessages.slice(1).every((m) => m.retain === false)).toBe(true)

    timers.tick()
    expect(app.statuses.at(-1)).toMatch(/ · Victron: connected · /)
  })

  test('the feed dials only once the initial read is complete, so its full publish is whole', async () => {
    // The battery's GetItems answers when the test says: the slow service on
    // a loaded GX. Until it does, no client is dialled — a feed connected at
    // portal-id time paid its full publish from a half-read cache and the
    // hub confirmed a partial boat (protocol §3, 2026-10-26).
    let release = null
    const dbusFake = fakeDbus({
      'com.victronenergy.battery.ttyUSB0 GetItems': (msg, cb) => { release = () => cb(null, [
        ['/DeviceInstance', [['Value', variant('i', 279)]]],
        ['/Soc', [['Value', variant('d', 87.5)]]]
      ]) }
    })
    const { app, timers } = await startWithVictron({ dbusFake })
    await until(() => release !== null)
    await settle()
    expect(connects).toEqual([])
    expect(hubMessages).toEqual([])
    timers.tick()
    expect(app.statuses.at(-1)).toMatch(/ · Victron: starting · /)

    release()
    await until(() => connects.length === 1)
    await announce(['#'])
    await until(() => hubMessages.some((m) => m.topic === `N/${PORTAL}/full_publish_completed`))
    expect(hubMessages.map((m) => m.topic)).toEqual([
      `N/${PORTAL}/system/0/Serial`,
      `N/${PORTAL}/battery/279/DeviceInstance`,
      `N/${PORTAL}/battery/279/Soc`,
      `N/${PORTAL}/system/0/Serial`,
      `N/${PORTAL}/full_publish_completed`
    ])
  })

  test('a D-Bus change is rendered and published; a service leaving is forgotten', async () => {
    const { dbusFake } = await startWithVictron()
    await announce(['#'])
    await until(() => hubMessages.some((m) => m.topic === `N/${PORTAL}/full_publish_completed`))
    const before = hubMessages.length

    dbusFake.connection.emit('message', {
      type: 4, sender: ':1.11', interface: BUS_ITEM, member: 'ItemsChanged', path: '/',
      body: [[['/Soc', [['Value', variant('d', 86)]]]]]
    })
    await until(() => hubMessages.length === before + 1)
    expect(hubMessages.at(-1)).toMatchObject({ topic: `N/${PORTAL}/battery/279/Soc`, payload: '{"value":86}', retain: false })

    // A service the renderer never saw leaving is nothing to forget; then the
    // battery leaves and its topics go with it.
    for (const [name, owner] of [['com.victronenergy.solarcharger.ttyUSB1', ':1.19'], ['com.victronenergy.battery.ttyUSB0', ':1.11']]) {
      dbusFake.connection.emit('message', {
        type: 4, sender: DBUS, interface: DBUS, member: 'NameOwnerChanged', path: '/org/freedesktop/DBus',
        body: [name, owner, '']
      })
    }
    await settle()
    hubMessages.length = 0
    await new Promise((resolve) => hubClient.publish(`R/${PORTAL}/keepalive`, '', resolve))
    await until(() => hubMessages.some((m) => m.topic === `N/${PORTAL}/full_publish_completed`))
    expect(hubMessages.map((m) => m.topic)).toEqual([`N/${PORTAL}/system/0/Serial`, `N/${PORTAL}/full_publish_completed`])
  })

  test('D-Bus failing at start: the owner is told, the error logged, and it is tried again from scratch after 30 s', async () => {
    const dbusFake = fakeDbus({ 'com.victronenergy.system GetValue': (msg, cb) => process.nextTick(cb, new Error('no such service')) })
    const { app, timers } = await startWithVictron({ dbusFake })
    await until(() => app.logged.some(([level]) => level === 'error'))
    timers.tick()
    expect(app.statuses.at(-1)).toMatch(/ · Victron: no such service; retrying in 30 s · /)
    expect(app.logged).toContainEqual(['error', 'Victron feed: no such service'])
    expect([...timers.timeouts.values()].map((t) => t.ms)).toContain(30000)
    expect(dbusFake.connection.ended).toBe(1)
    expect(connects).toEqual([])

    // The retry: a fresh venus, a fresh attempt.
    for (const [id, t] of [...timers.timeouts.entries()]) if (t.ms === 30000) { timers.timeouts.delete(id); t.fn() }
    expect(dbusFake.made()).toBe(2)
  })

  test('a failure that repeats is logged at error once, then at debug; recovery is logged with the count', async () => {
    const state = { failing: true }
    const dbusFake = fakeDbus({
      'com.victronenergy.system GetValue': (msg, cb) => state.failing
        ? process.nextTick(cb, new Error('no such service'))
        : process.nextTick(cb, null, variant('s', PORTAL))
    })
    const { app, timers } = await startWithVictron({ dbusFake })
    const retry = () => { for (const [id, t] of [...timers.timeouts.entries()]) if (t.ms === 30000) { timers.timeouts.delete(id); t.fn() } }
    const errors = () => app.logged.filter(([level, text]) => level === 'error' && text.startsWith('Victron feed:'))
    const debugs = () => app.logged.filter(([level, text]) => level === 'debug' && text.startsWith('Victron feed:'))

    await until(() => errors().length === 1)
    retry()
    await until(() => debugs().length === 1)
    retry()
    await until(() => debugs().length === 2)
    expect(errors()).toEqual([['error', 'Victron feed: no such service']])
    expect(debugs()).toEqual([
      ['debug', 'Victron feed: no such service (attempt 2)'],
      ['debug', 'Victron feed: no such service (attempt 3)']
    ])
    timers.tick()
    expect(app.statuses.at(-1)).toMatch(/ · Victron: no such service; retrying in 30 s · /)

    // The host grows a Venus D-Bus: the streak ends with its count.
    state.failing = false
    retry()
    await until(() => debugs().length === 3)
    expect(debugs().at(-1)).toEqual(['debug', 'Victron feed: recovered after 3 failed attempt(s)'])
    await until(() => Object.keys(aedes.clients).length === 2)

    // A later failure, with a different reason, is new information: error again.
    dbusFake.connection.emit('error', new Error('bus gone'))
    await until(() => errors().length === 2)
    expect(errors().at(-1)).toEqual(['error', 'Victron feed: D-Bus bus gone'])
  })

  test('D-Bus failing later tears the feed down once, even when the failure also rejects start()', async () => {
    const { app, timers, dbusFake } = await startWithVictron()
    await until(() => Object.keys(aedes.clients).length === 2) // the hub and the feed
    dbusFake.connection.emit('error', new Error('bus gone'))
    await until(() => Object.keys(aedes.clients).length === 1)
    await settle()
    expect(app.logged.filter(([level, text]) => level === 'error' && text.startsWith('Victron feed:'))).toHaveLength(1)
    expect([...timers.timeouts.values()].filter((t) => t.ms === 30000)).toHaveLength(1)
    timers.tick()
    expect(app.statuses.at(-1)).toMatch(/ · Victron: D-Bus bus gone; retrying in 30 s · /)
  })

  test('stop() ends the feed\'s client and the bus, and clears a pending retry', async () => {
    const { instance, timers, dbusFake } = await startWithVictron()
    await until(() => Object.keys(aedes.clients).length === 2)
    instance.stop()
    await until(() => Object.keys(aedes.clients).length === 1)
    expect(dbusFake.connection.ended).toBe(1)

    const failing = fakeDbus({ 'com.victronenergy.system GetValue': (msg, cb) => process.nextTick(cb, new Error('x')) })
    const second = await startWithVictron({ dbusFake: failing })
    await until(() => [...second.timers.timeouts.values()].some((t) => t.ms === 30000))
    second.instance.stop()
    expect([...second.timers.timeouts.values()].filter((t) => t.ms === 30000)).toHaveLength(0)
  })

  test('a bus with no Victron service is a calm state: its own status, debug only, looked at again every 5 min', async () => {
    const dbusFake = fakeDbus({
      'com.victronenergy.system GetValue': (msg, cb) => process.nextTick(cb, new Error('org.freedesktop.DBus.Error.ServiceUnknown')),
      [`${DBUS} ListNames`]: reply([DBUS, 'org.bluez'])
    })
    const { app, timers } = await startWithVictron({ dbusFake })
    const victronLines = () => app.logged.filter(([, text]) => text.startsWith('Victron feed:'))
    await until(() => victronLines().length === 1)
    expect(victronLines()).toEqual([['debug', 'Victron feed: no GX on this bus; looking again every 5 min']])
    timers.tick()
    expect(app.statuses.at(-1)).toMatch(/ · Victron: no GX on this bus · /)
    expect([...timers.timeouts.values()].map((t) => t.ms)).toContain(300000)
    expect([...timers.timeouts.values()].map((t) => t.ms)).not.toContain(30000)
    expect(connects).toEqual([])

    // Five minutes on, a fresh look; the same answer is a debug line with the attempt.
    for (const [id, t] of [...timers.timeouts.entries()]) if (t.ms === 300000) { timers.timeouts.delete(id); t.fn() }
    expect(dbusFake.made()).toBe(2)
    await until(() => victronLines().length === 2)
    expect(victronLines().at(-1)).toEqual(['debug', 'Victron feed: no GX on this bus (attempt 2)'])
    expect(app.logged.filter(([level]) => level === 'error')).toEqual([])
  })

  test('a reboot restarts the N2K feed only: the Victron feed keeps its D-Bus and its MQTT session', async () => {
    const { hub, app, timers, dbusFake } = await startWithVictron()
    await until(() => Object.keys(aedes.clients).length === 2 && app.statuses.includes('N2K: connected'))
    // The broker's sessions: the hub's and the feed's, by client id.
    const sessions = Object.keys(aedes.clients).sort()

    hub.accepted[0].ws.send('reboot\n')
    await until(() => hub.accepted.length === 2 && app.statuses.filter((s) => s === 'N2K: connected').length === 2)
    await settle()
    expect(dbusFake.made()).toBe(1)
    expect(dbusFake.connection.ended).toBe(0)
    expect(connects).toHaveLength(1)
    expect(Object.keys(aedes.clients).sort()).toEqual(sessions)
    timers.tick()
    expect(app.statuses.at(-1)).toMatch(/^N2K: connected · Victron: connected · /)
  })

  test('a failure arriving after stop() changes nothing', async () => {
    let release
    const held = fakeDbus({ 'com.victronenergy.system GetValue': (msg, cb) => { release = () => cb(new Error('late')) } })
    const { app, instance, timers } = await startWithVictron({ dbusFake: held })
    await until(() => typeof release === 'function')
    instance.stop()
    release()
    await settle()
    expect(app.logged.filter(([level]) => level === 'error')).toEqual([])
    expect([...timers.timeouts.values()].filter((t) => t.ms === 30000)).toHaveLength(0)
  })

  describe('"Allow switches" (README "Settings"; §2 "W/ writes", §4.2)', () => {
    const SOC_W = `W/${PORTAL}/battery/279/Soc`
    const WRITABLE = `wattknot/producer/${PORTAL}/writable`
    const flagSeen = () => new Promise((resolve) => {
      const late = mqtt.connect(`mqtt://127.0.0.1:${brokerPort}`)
      late.on('message', (topic, payload, packet) => { late.end(true); resolve({ topic, payload: payload.toString(), retain: packet.retain }) })
      late.on('connect', () => late.subscribe('wattknot/producer/#'))
    })
    const publish = (topic, text) => new Promise((resolve) => hubClient.publish(topic, text, resolve))

    test('off, or missing: the flag says false, a write is refused in the error log and no SetValue is made', async () => {
      const sets = []
      const dbusFake = fakeDbus({ 'com.victronenergy.battery.ttyUSB0 SetValue': (msg, cb) => { sets.push(msg); cb(null, 0) } })
      const { app } = await startWithVictron({ dbusFake })
      await until(() => connects.length === 1)
      expect(await flagSeen()).toEqual({ topic: WRITABLE, payload: 'false', retain: true })

      await publish(SOC_W, '{"value":5}')
      await until(() => app.logged.some(([level, text]) => level === 'error' && text.startsWith('refused write')))
      expect(app.logged.filter(([, text]) => text.startsWith('refused write'))).toEqual([['error', `refused write ${SOC_W}: writes are not allowed`]])
      await settle()
      expect(sets).toEqual([])
    })

    // A relay output on the GX (lib/victron.js WRITABLE): the system service
    // gains one, and its SetValue is answered as `setValue` says.
    const RELAY_W = `W/${PORTAL}/system/0/SwitchableOutput/0/State`
    const withRelay = (setValue) => fakeDbus({
      'com.victronenergy.system GetItems': reply([
        ['/Serial', [['Value', variant('s', PORTAL)]]],
        ['/SwitchableOutput/0/State', [['Value', variant('i', 0)]]]
      ]),
      'com.victronenergy.system SetValue': setValue
    })
    const refusals = (app) => app.logged.filter(([, text]) => text.startsWith('refused write'))

    test('on: the flag says true and a write is one SetValue, typed as the item was read, logged at debug with its topic only', async () => {
      const sets = []
      const dbusFake = withRelay((msg, cb) => { sets.push(msg); process.nextTick(cb, null, 0) })
      const { app } = await startWithVictron({ dbusFake, settings: { allowSwitches: true } })
      await until(() => connects.length === 1)
      expect(await flagSeen()).toEqual({ topic: WRITABLE, payload: 'true', retain: true })

      await publish(RELAY_W, '{"value":1}')
      await until(() => sets.length === 1)
      expect(sets[0]).toEqual({
        destination: 'com.victronenergy.system', path: '/SwitchableOutput/0/State', interface: BUS_ITEM, member: 'SetValue',
        signature: 'v', body: [[[sig('i')], [1]]]
      })
      await until(() => app.logged.some(([, text]) => text.startsWith('wrote')))
      // No payload, as feed.js logs none: the line is the topic.
      expect(app.logged.filter(([, text]) => text.startsWith('wrote'))).toEqual([['debug', `wrote ${RELAY_W}`]])
      expect(app.errors).toEqual([])
    })

    test('on: a write the renderer refuses is the error log with the reason, and no SetValue', async () => {
      const sets = []
      const dbusFake = withRelay((msg, cb) => { sets.push(msg); cb(null, 0) })
      const { app } = await startWithVictron({ dbusFake, settings: { allowSwitches: true } })
      await until(() => connects.length === 1)

      await publish(SOC_W, '{"value":5}')
      await publish(`W/${PORTAL}/system/0/SwitchableOutput/9/State`, '{"value":1}')
      await publish(RELAY_W, '{"value":"on"}')
      await until(() => refusals(app).filter(([level]) => level === 'error').length === 3)
      expect(refusals(app).filter(([level]) => level === 'error')).toEqual([
        ['error', `refused write ${SOC_W}: not a switch`],
        ['error', `refused write W/${PORTAL}/system/0/SwitchableOutput/9/State: unknown topic`],
        ['error', `refused write ${RELAY_W}: value must be a whole number`]
      ])
      await settle()
      expect(sets).toEqual([])
    })

    test('on: a refusal that repeats is at error once, then at debug; the streak ends with its count', async () => {
      const sets = []
      const dbusFake = withRelay((msg, cb) => { sets.push(msg); process.nextTick(cb, null, 0) })
      const { app } = await startWithVictron({ dbusFake, settings: { allowSwitches: true } })
      await until(() => connects.length === 1)

      for (let i = 0; i < 3; i++) await publish(SOC_W, '{"value":5}')
      await until(() => refusals(app).length === 3)
      expect(refusals(app)).toEqual([
        ['error', `refused write ${SOC_W}: not a switch`],
        ['debug', `refused write ${SOC_W}: not a switch (refusal 2)`],
        ['debug', `refused write ${SOC_W}: not a switch (refusal 3)`]
      ])

      // A different reason is new information, at error, after the old streak's count.
      await publish(RELAY_W, '{"value":"on"}')
      await until(() => refusals(app).length === 5)
      expect(refusals(app).slice(3)).toEqual([
        ['debug', 'refused write: "not a switch" 3 time(s) in a row'],
        ['error', `refused write ${RELAY_W}: value must be a whole number`]
      ])

      // An accepted write ends the streak with its count.
      await publish(RELAY_W, '{"value":1}')
      await until(() => sets.length === 1)
      expect(refusals(app).slice(5)).toEqual([['debug', 'refused write: "value must be a whole number" 1 time(s) in a row']])
    })

    test('on: the GX refusing the set is the error log with the code', async () => {
      const dbusFake = withRelay((msg, cb) => process.nextTick(cb, null, -1))
      const { app } = await startWithVictron({ dbusFake, settings: { allowSwitches: true } })
      await until(() => connects.length === 1)

      await publish(RELAY_W, '{"value":1}')
      await until(() => app.logged.some(([, text]) => text.startsWith('write ')))
      expect(app.logged.filter(([, text]) => text.startsWith('write '))).toEqual([['error', `write ${RELAY_W} failed: SetValue returned -1`]])
    })
  })

  describe('switching a feed off (README "Settings")', () => {
    const N2K_OFF = 'N2K: off · Victron: '

    test('settings saved before the switches existed run both feeds, as they did', async () => {
      const { hub, app } = await startWithVictron()
      await until(() => hub.accepted.length === 1 && connects.length === 1)
      await until(() => app.statuses.includes('N2K: connected'))
    })

    test('N2K off: no stream socket, no tap, no heartbeat; the feed runs and the status shows both feeds each second', async () => {
      const { hub, app, timers, dbusFake } = await startWithVictron({ settings: { sendN2k: false } })
      expect(app.statuses).toEqual([`${N2K_OFF}starting`])
      expect(app.attached).toEqual([])
      await until(() => connects.length === 1)
      await until(() => {
        timers.tick()
        return app.statuses.at(-1) === `${N2K_OFF}connected`
      })
      await settle()
      expect(hub.accepted).toEqual([])
      expect(timers.intervalsSet).toBe(1)
      expect(dbusFake.made()).toBe(1)
      expect(app.errors).toEqual([])
    })

    test('N2K off: a D-Bus failure is still told, on the next second', async () => {
      const dbusFake = fakeDbus({ 'com.victronenergy.system GetValue': (msg, cb) => process.nextTick(cb, new Error('no such service')) })
      const { app, timers } = await startWithVictron({ dbusFake, settings: { sendN2k: false } })
      await until(() => app.logged.some(([level]) => level === 'error'))
      timers.tick()
      expect(app.statuses.at(-1)).toBe(`${N2K_OFF}no such service; retrying in 30 s`)
    })

    test('Victron off: D-Bus is never opened and the MQTT door never dialled; the stream runs as before', async () => {
      const { hub, app, timers, dbusFake } = await startWithVictron({ settings: { sendVictron: false } })
      await until(() => app.statuses.includes('N2K: connected'))
      app.emit(RAW, BATTERY)
      timers.tick()
      expect(app.statuses.at(-1)).toBe(status('N2K: connected', 1, 0, 0))
      await settle()
      expect(dbusFake.made()).toBe(0)
      expect(connects).toEqual([])
      expect(hub.accepted).toHaveLength(1)
    })

    test('both off: nothing is opened, no timer, the status says so once, and no setting is checked', async () => {
      const { app, timers, instance, dbusFake } = victronPlugin()
      instance.start({ sendN2k: false, sendVictron: false })
      await settle()
      expect(app.errors).toEqual([])
      expect(app.statuses).toEqual(['N2K: off · Victron: off'])
      expect(app.attached).toEqual([])
      expect(timers.intervalsSet).toBe(0)
      expect(dbusFake.made()).toBe(0)
      expect(connects).toEqual([])
      instance.stop()
      expect(app.detached).toEqual([])
      expect(timers.intervalsCleared).toBe(0)
    })

    test('N2K off still checks the settings the feed dials with: the owner is told, and nothing is opened', async () => {
      const { app, timers, instance, dbusFake } = victronPlugin()
      instance.start({ sendN2k: false, token: TOKEN })
      instance.start({ sendN2k: false, boatId: 'a1b2c3', token: 'two words' })
      await settle()
      expect(app.errors).toEqual([
        'Boat ID is required when no local hub address is set',
        'Token is required, and must contain no spaces'
      ])
      expect(app.statuses).toEqual([])
      expect(timers.intervalsSet).toBe(0)
      expect(dbusFake.made()).toBe(0)
      expect(connects).toEqual([])
    })

    test('N2K off: stop() ends the feed and the status timer, and detaches nothing', async () => {
      const { app, timers, instance, dbusFake } = await startWithVictron({ settings: { sendN2k: false } })
      await until(() => Object.keys(aedes.clients).length === 2)
      const [{ fn: tick }] = [...timers.intervals.values()]
      instance.stop()
      await until(() => Object.keys(aedes.clients).length === 1)
      expect(dbusFake.connection.ended).toBe(1)
      expect(timers.intervalsCleared).toBe(1)
      expect(app.detached).toEqual([])
      // A tick through a reference held from before stop() reaches nothing.
      const before = app.statuses.length
      tick()
      expect(app.statuses).toHaveLength(before)
    })
  })

  test('the production wiring carries the real D-Bus module, the MQTT client factory and the wall clock', () => {
    const instance = plugin(fakeApp())
    expect(instance.name).toBe('Wattknot Plugin')
    expect(instance.description).toMatch(/Victron/)
  })
})
