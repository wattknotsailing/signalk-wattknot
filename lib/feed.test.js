'use strict'

// The feed against a real MQTT broker (aedes) on loopback, with a second real
// mqtt.js client playing the hub: it announces its subscriptions, sends R/ and
// W/ requests, and subscribes to N/# to see what the feed publishes. Every
// expected topic, payload and order here is written by hand from the contract
// (protocol/mqtt-producer.md), never read back from the code under test.

const net = require('net')
const fs = require('fs')
const path = require('path')
const Aedes = require('aedes')
const mqtt = require('mqtt')
const { createFeed } = require('./feed')

const PORTAL = 'c0619ab1d2e3'
const ANNOUNCE = 'wattknot/hub/subscriptions/'
const SERIAL = `N/${PORTAL}/system/0/Serial`
const SERIAL_PAYLOAD = `{"value":"${PORTAL}"}`
const COMPLETED = `N/${PORTAL}/full_publish_completed`
const KEEPALIVE = `R/${PORTAL}/keepalive`
const SOC = `N/${PORTAL}/battery/279/Soc`
const VOLTAGE = `N/${PORTAL}/battery/279/Dc/0/Voltage`
const SOLAR = `N/${PORTAL}/solarcharger/290/Yield/Power`

// 2025-10-24T10:00:00.999Z in ms: full_publish_completed carries whole unix
// seconds, so the 999 ms must be floored away, not rounded up.
const NOW_MS = 1761300000999
const NOW_S = 1761300000

// MQTT's wildcard rules (§4), written here so the test does not depend on
// lib/victron.js: `+` is one level, `#` is this level and everything below.
// In the feed's shape, a matcher compiled per filter set; `compiled` records
// each set it was built for.
const compiled = []
function createMatcher (filters) {
  compiled.push(filters)
  return {
    matches: (topic) => {
      const levels = topic.split('/')
      return filters.some((filter) => {
        const parts = filter.split('/')
        for (let i = 0; i < parts.length; i++) {
          if (parts[i] === '#') return true
          if (i >= levels.length) return false
          if (parts[i] !== '+' && parts[i] !== levels[i]) return false
        }
        return parts.length === levels.length
      })
    }
  }
}

// Polls with real timers until `check` (sync or async) holds; fails after 2 s.
async function until (check) {
  const deadline = Date.now() + 2000
  while (!(await check())) {
    if (Date.now() > deadline) throw new Error('timed out waiting')
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
}

// Lets in-flight packets land, for asserting that something did NOT happen.
const settle = () => new Promise((resolve) => setTimeout(resolve, 100))

const brokers = []
const hubs = []
const feeds = []

// A real aedes broker on 127.0.0.1, port 0.
async function startBroker () {
  const aedes = Aedes()
  const server = net.createServer(aedes.handle)
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve))
  brokers.push({ aedes, server })
  return {
    url: `mqtt://127.0.0.1:${server.address().port}`,
    clients: () => Object.keys(aedes.clients),
    // A link dropping, from the broker's side.
    kick: (clientId) => aedes.clients[clientId].close()
  }
}

// The hub: subscribes to N/# and records every message with its retain flag.
async function startHub (broker, clientId = 'hub') {
  const client = mqtt.connect(broker.url, { clientId, reconnectPeriod: 0 })
  hubs.push(client)
  const received = []
  client.on('message', (topic, payload, packet) => {
    received.push({ topic, payload: payload.toString(), retain: packet.retain })
  })
  await client.subscribeAsync('N/#')
  return {
    client,
    received,
    topics: () => received.map((m) => m.topic),
    completions: () => received.filter((m) => m.topic === COMPLETED).length,
    // §4: retained, as the hub publishes them.
    announce: (sourceId, filters) =>
      client.publishAsync(ANNOUNCE + sourceId, JSON.stringify(filters), { retain: true, qos: 1 }),
    publish: (topic, payload, options = {}) => client.publishAsync(topic, payload, { qos: 1, ...options })
  }
}

function startFeed (broker, { clientId = 'feed', reconnectPeriod = 50, autoStart = true, onWrite = null } = {}) {
  const statuses = []
  const warnings = []
  const debugs = []
  const made = []
  const feed = createFeed({
    connect: () => {
      const client = mqtt.connect(broker.url, { clientId, reconnectPeriod })
      made.push(client)
      return client
    },
    portalId: PORTAL,
    logger: { warn: (text) => warnings.push(text), debug: (text) => debugs.push(text) },
    clock: { now: () => NOW_MS },
    createMatcher,
    onStatus: (text) => statuses.push(text),
    onWrite
  })
  feeds.push(feed)
  if (autoStart) feed.start()
  return { feed, statuses, warnings, debugs, made }
}

// Both ends ready: the feed's subscriptions are in place (the broker handled
// them before the feed's Serial publish, which the hub has now seen).
async function ready (broker, options) {
  const hub = await startHub(broker)
  const run = startFeed(broker, options)
  await until(() => hub.topics().includes(SERIAL))
  return { hub, ...run }
}

// A late subscriber: what it receives on subscribing is exactly what the
// broker holds retained under N/#.
async function retainedUnder (broker) {
  const late = await startHub(broker, 'late')
  await settle()
  return late.received
}

// Waits until the feed holds exactly this filter set.
const filtersBecome = (feed, expected) =>
  until(() => JSON.stringify(feed.filters()) === JSON.stringify(expected))

// The connection's first announcement, and the unasked full publish it
// triggers (§3) landed at the hub, so a test can mark what follows.
async function firstAnnouncement (hub, feed, filters) {
  await hub.announce('bau', filters)
  await filtersBecome(feed, [...filters].sort())
  await settle()
}

let broker

beforeEach(async () => {
  broker = await startBroker()
})

afterEach(async () => {
  for (const feed of feeds.splice(0)) feed.stop()
  for (const hub of hubs.splice(0)) hub.end(true)
  for (const { aedes, server } of brokers.splice(0)) {
    await new Promise((resolve) => aedes.close(resolve))
    await new Promise((resolve) => server.close(resolve))
  }
})

describe('construction', () => {
  test('refuses a portal id that is not one topic level without wildcards', () => {
    const deps = { connect: () => null, logger: { warn () {} }, clock: { now: () => 0 }, createMatcher }
    for (const portalId of ['', 'a/b', 'c0+', '#', undefined, 42]) {
      expect(() => createFeed({ ...deps, portalId })).toThrow('portalId must be one topic level')
    }
    expect(() => createFeed({ ...deps, portalId: PORTAL })).not.toThrow()
  })

  test('onStatus is optional', async () => {
    const feed = createFeed({
      connect: () => mqtt.connect(broker.url, { clientId: 'quiet' }),
      portalId: PORTAL,
      logger: { warn () {} },
      clock: { now: () => NOW_MS },
      createMatcher
    })
    feeds.push(feed)
    feed.start()
    await until(() => feed.isConnected())
  })

  test('hard-codes no address and no client library: the caller connects (§1)', () => {
    const source = fs.readFileSync(path.join(__dirname, 'feed.js'), 'utf8')
    expect(source).not.toMatch(/\b(mqtts?|wss?|tcp):\/\//)
    expect(source).not.toMatch(/require\(['"]mqtt['"]\)/)
  })
})

describe('on connect', () => {
  test('reports connecting then connected', async () => {
    const { feed, statuses } = await ready(broker)
    expect(feed.isConnected()).toBe(true)
    expect(statuses).toEqual(['connecting', 'connected'])
  })

  test('subscribes to the announcements, R/ and W/ (§2, §4)', async () => {
    const { hub, feed, warnings } = await ready(broker)

    await firstAnnouncement(hub, feed, [SOC])

    const mark = hub.received.length
    await hub.publish(`R/${PORTAL}/system/0/Serial`, '')
    await hub.announce('bau', ['#'])
    await filtersBecome(feed, ['#'])
    await hub.publish(KEEPALIVE, '')
    await until(() => hub.topics().includes(COMPLETED))
    // The mirror read was filtered out (Serial not announced); widening to
    // `#` made the Serial eligible, and the full publish carried it again.
    expect(hub.topics().slice(mark)).toEqual([SERIAL, SERIAL, COMPLETED])

    await hub.publish(`W/${PORTAL}/settings/0/Settings/CGwacs/AcPowerSetPoint`, '{"value":100}')
    await until(() => warnings.length === 1)
  })

  test('publishes the Serial retained, with the portal id as its value (§2)', async () => {
    const { hub } = await ready(broker)
    expect(hub.received.find((m) => m.topic === SERIAL).payload).toBe(SERIAL_PAYLOAD)

    const late = await retainedUnder(broker)
    expect(late).toEqual([{ topic: SERIAL, payload: SERIAL_PAYLOAD, retain: true }])
  })

  test('the Serial is counted as published', async () => {
    const { feed } = await ready(broker)
    expect(feed.counters()).toEqual({ published: 1, dropped: 0 })
  })
})

describe('the full publish after the feed connects (§3)', () => {
  test('fires once, unasked, on the first announcement after connect: filtered, sorted, completed', async () => {
    const { hub, feed } = await ready(broker)
    feed.set(SOLAR, '{"value":412}')
    feed.set(SOC, '{"value":80}')
    feed.set(VOLTAGE, '{"value":13.2}')
    const mark = hub.received.length

    await hub.announce('bau', ['#'])
    await until(() => hub.completions() === 1)
    expect(hub.received.slice(mark)).toEqual([
      { topic: VOLTAGE, payload: '{"value":13.2}', retain: false },
      { topic: SOC, payload: '{"value":80}', retain: false },
      { topic: SOLAR, payload: '{"value":412}', retain: false },
      { topic: SERIAL, payload: SERIAL_PAYLOAD, retain: false },
      { topic: COMPLETED, payload: `{"value":${NOW_S}}`, retain: false }
    ])
  })

  test('goes through the filter, the completion included', async () => {
    const { hub, feed } = await ready(broker)
    feed.set(SOLAR, '{"value":412}')
    feed.set(SOC, '{"value":80}')
    const mark = hub.received.length

    await firstAnnouncement(hub, feed, [`N/${PORTAL}/battery/#`])
    expect(hub.topics().slice(mark)).toEqual([SOC])
  })

  test('does not fire again on a later announcement in the same connection', async () => {
    const { hub, feed } = await ready(broker)
    feed.set(SOC, '{"value":80}')
    await firstAnnouncement(hub, feed, ['#'])
    expect(hub.completions()).toBe(1)
    const mark = hub.received.length

    await hub.announce('discovery', ['#'])
    await hub.announce('bau', [SOC])
    await filtersBecome(feed, ['#', SOC])
    await settle()
    expect(hub.received.length).toBe(mark)
  })

  test('an empty first announcement still uses it up: the next one widens, with no completion', async () => {
    const { hub, feed } = await ready(broker)
    feed.set(SOC, '{"value":80}')
    await firstAnnouncement(hub, feed, [])
    expect(hub.topics()).toEqual([SERIAL])
    await hub.announce('bau', ['#'])
    await filtersBecome(feed, ['#'])
    await settle()
    expect(hub.topics()).toEqual([SERIAL, SOC, SERIAL])
  })

  test('fires again after a reconnect', async () => {
    const { hub, feed, statuses } = await ready(broker)
    feed.set(SOC, '{"value":80}')
    await firstAnnouncement(hub, feed, ['#'])
    expect(hub.completions()).toBe(1)
    const mark = hub.received.length

    broker.kick('feed')
    await until(() => statuses.filter((s) => s === 'connected').length === 2)
    // The retained announcement is the reconnected session's first.
    await until(() => hub.completions() === 2)
    // The Serial on connect, then the full publish.
    expect(hub.topics().slice(mark)).toEqual([SERIAL, SOC, SERIAL, COMPLETED])
  })

  test('does not fire while no announcement arrives', async () => {
    const { hub, feed } = await ready(broker)
    feed.set(SOC, '{"value":80}')
    await hub.publish(`R/${PORTAL}/battery/279/Soc`, '')
    await settle()
    expect(hub.topics()).toEqual([SERIAL])
    expect(feed.counters()).toEqual({ published: 1, dropped: 1 })
  })
})

describe('a later announcement in the same connection (§3)', () => {
  // Three stored values; the first announcement admits only the Soc, so its
  // full publish sends [SOC] and no completion.
  async function afterFirst () {
    const run = await ready(broker)
    run.feed.set(SOLAR, '{"value":412}')
    run.feed.set(SOC, '{"value":80}')
    run.feed.set(VOLTAGE, '{"value":13.2}')
    const first = run.hub.received.length
    await run.hub.announce('victron', [SOC])
    await filtersBecome(run.feed, [SOC])
    await settle()
    expect(run.hub.topics().slice(first)).toEqual([SOC])
    return run
  }

  test('a second source widening the union publishes only the newly eligible topics, sorted, with no completion and no retain', async () => {
    const { hub, feed } = await afterFirst()
    const mark = hub.received.length

    await hub.announce('devices', [`N/${PORTAL}/solarcharger/#`, `N/${PORTAL}/battery/279/Dc/#`, SOC])
    await filtersBecome(feed, [`N/${PORTAL}/battery/279/Dc/#`, SOC, `N/${PORTAL}/solarcharger/#`])
    await settle()
    expect(hub.received.slice(mark)).toEqual([
      { topic: VOLTAGE, payload: '{"value":13.2}', retain: false },
      { topic: SOLAR, payload: '{"value":412}', retain: false }
    ])

    const late = await retainedUnder(broker)
    expect(late).toEqual([{ topic: SERIAL, payload: SERIAL_PAYLOAD, retain: true }])
  })

  test('an announcement that leaves the union unchanged publishes nothing', async () => {
    const { hub, feed } = await afterFirst()
    const mark = hub.received.length

    await hub.announce('victron', [SOC]) // the same again
    await hub.announce('devices', [SOC]) // already in the union
    await filtersBecome(feed, [SOC])
    await settle()
    expect(hub.received.length).toBe(mark)
  })

  test('an announcement that narrows the union publishes nothing; widening again sends what returns', async () => {
    const { hub, feed } = await afterFirst()
    await hub.announce('devices', [`N/${PORTAL}/battery/#`])
    await filtersBecome(feed, [`N/${PORTAL}/battery/#`, SOC])
    await settle()
    const mark = hub.received.length

    await hub.announce('devices', [])
    await filtersBecome(feed, [SOC])
    await hub.announce('victron', [])
    await filtersBecome(feed, [])
    await settle()
    expect(hub.received.length).toBe(mark)

    await hub.announce('devices', [`N/${PORTAL}/battery/279/+`])
    await filtersBecome(feed, [`N/${PORTAL}/battery/279/+`])
    await settle()
    expect(hub.topics().slice(mark)).toEqual([SOC])
  })

  test('a widening that admits no stored topic publishes nothing', async () => {
    const { hub, feed } = await afterFirst()
    const mark = hub.received.length
    await hub.announce('devices', [`N/${PORTAL}/vebus/#`])
    await filtersBecome(feed, [SOC, `N/${PORTAL}/vebus/#`])
    await settle()
    expect(hub.received.length).toBe(mark)
  })
})

describe('publishing a change (§4)', () => {
  test('before any announcement a set publishes nothing and is counted dropped', async () => {
    const { hub, feed } = await ready(broker)
    feed.set(SOC, '{"value":80}')
    await settle()
    expect(hub.topics()).toEqual([SERIAL])
    expect(feed.filters()).toEqual([])
    expect(feed.counters()).toEqual({ published: 1, dropped: 1 })
  })

  test('a matching set publishes exactly its payload; a non-matching one is stored and counted dropped', async () => {
    const { hub, feed } = await ready(broker)
    await firstAnnouncement(hub, feed, [SOC])

    feed.set(VOLTAGE, '{"value":13.2}')
    feed.set(SOC, '{"value":80.5}')
    await until(() => hub.topics().includes(SOC))
    await settle()
    expect(hub.received.slice(1)).toEqual([{ topic: SOC, payload: '{"value":80.5}', retain: false }])
    expect(feed.counters()).toEqual({ published: 2, dropped: 1 })

    // Stored, not lost: a `#` full publish carries it.
    await hub.announce('bau', ['#'])
    await filtersBecome(feed, ['#'])
    await hub.publish(KEEPALIVE, '')
    await until(() => hub.topics().includes(COMPLETED))
    expect(hub.received.find((m) => m.topic === VOLTAGE).payload).toBe('{"value":13.2}')
  })

  test('a set with the stored payload is no change and publishes nothing', async () => {
    const { hub, feed } = await ready(broker)
    await firstAnnouncement(hub, feed, ['#'])
    const mark = hub.received.length
    const before = feed.counters().published

    feed.set(SOC, '{"value":80}')
    feed.set(SOC, '{"value":80}')
    feed.set(SOC, '{"value":81}')
    await until(() => hub.received.length === mark + 2)
    await settle()
    expect(hub.received.slice(mark).map((m) => m.payload)).toEqual(['{"value":80}', '{"value":81}'])
    expect(feed.counters()).toEqual({ published: before + 2, dropped: 0 })
  })

  test('the renderer reporting the Serial the feed already holds is no change', async () => {
    const { hub, feed } = await ready(broker)
    await firstAnnouncement(hub, feed, ['#'])
    const mark = hub.received.length
    feed.set(SERIAL, SERIAL_PAYLOAD)
    await settle()
    expect(hub.received.length).toBe(mark)
  })

  test('a change while not connected is stored only: nothing published, nothing dropped', async () => {
    const { feed } = startFeed(broker, { autoStart: false })
    feed.set(SOC, '{"value":80}')
    expect(feed.isConnected()).toBe(false)
    expect(feed.counters()).toEqual({ published: 0, dropped: 0 })

    // Sent after connecting, by the full publish the first announcement brings.
    const hub = await startHub(broker)
    feed.start()
    await until(() => hub.topics().includes(SERIAL))
    await hub.announce('bau', ['#'])
    await until(() => hub.topics().includes(COMPLETED))
    expect(hub.received.find((m) => m.topic === SOC).payload).toBe('{"value":80}')
  })
})

describe('the subscription announcement (§4)', () => {
  test('`#` in any source publishes everything', async () => {
    const { hub, feed } = await ready(broker)
    await firstAnnouncement(hub, feed, [SOC])
    await hub.announce('discovery', ['#'])
    await filtersBecome(feed, ['#', SOC])
    await settle() // `#` widened the union to the Serial, which it sends
    const mark = hub.received.length

    feed.set(SOLAR, '{"value":412}')
    feed.set(`N/${PORTAL}/vecan/can0/Link/Something`, '{"value":null}')
    await until(() => hub.received.length === mark + 2)
    expect(feed.counters().dropped).toBe(0)
  })

  test('the effective set is the union across source ids', async () => {
    const { hub, feed } = await ready(broker)
    await hub.announce('victron', [`N/${PORTAL}/battery/#`])
    await hub.announce('energy', [`N/${PORTAL}/solarcharger/+/Yield/Power`, `N/${PORTAL}/battery/#`])
    await filtersBecome(feed, [`N/${PORTAL}/battery/#`, `N/${PORTAL}/solarcharger/+/Yield/Power`])

    feed.set(SOC, '{"value":80}')
    feed.set(SOLAR, '{"value":412}')
    feed.set(`N/${PORTAL}/solarcharger/290/Yield/User`, '{"value":3.1}')
    await until(() => hub.received.length === 3)
    await settle()
    expect(hub.topics().slice(1)).toEqual([SOC, SOLAR])
    expect(feed.counters().dropped).toBe(1)
  })

  test('an empty array from one source removes its filters and leaves the others', async () => {
    const { hub, feed } = await ready(broker)
    await hub.announce('victron', [SOC])
    await hub.announce('energy', [SOLAR])
    await filtersBecome(feed, [SOC, SOLAR])

    await hub.announce('victron', [])
    await filtersBecome(feed, [SOLAR])

    feed.set(SOC, '{"value":80}')
    feed.set(SOLAR, '{"value":412}')
    await until(() => hub.topics().includes(SOLAR))
    await settle()
    expect(hub.topics()).toEqual([SERIAL, SOLAR])
  })

  test('an unparseable announcement is logged and treated as empty', async () => {
    const { hub, feed, warnings } = await ready(broker)
    await hub.announce('energy', [SOLAR])
    for (const [payload, n] of [['not json', 1], ['{"filters":["#"]}', 2], ['["#",7]', 3], ['null', 4]]) {
      await hub.announce('victron', [SOC])
      await filtersBecome(feed, [SOC, SOLAR])
      await hub.publish(ANNOUNCE + 'victron', payload, { retain: true })
      await filtersBecome(feed, [SOLAR])
      expect(warnings).toHaveLength(n)
      expect(warnings[n - 1]).toBe(`ignoring unreadable subscription announcement from "victron": ${payload}`)
    }
  })

  test('the union is compiled once per announcement, never per topic it is asked about', async () => {
    const { hub, feed } = await ready(broker)
    feed.set(SOC, '{"value":80}')
    feed.set(VOLTAGE, '{"value":13.2}')
    feed.set(SOLAR, '{"value":412}')
    const before = compiled.length

    // Each announcement asks about every stored topic, twice over for the
    // second (what it newly admits); each still compiles one matcher.
    await firstAnnouncement(hub, feed, [SOC])
    await hub.announce('energy', [SOLAR])
    await filtersBecome(feed, [SOC, SOLAR])
    expect(compiled.slice(before)).toEqual([[SOC], [SOC, SOLAR]])
  })

  test('a zero-length announcement (a cleared retained message) clears the source without a warning', async () => {
    const { hub, feed, warnings } = await ready(broker)
    await hub.announce('victron', [SOC])
    await hub.announce('energy', [SOLAR])
    await filtersBecome(feed, [SOC, SOLAR])

    await hub.publish(ANNOUNCE + 'victron', '', { retain: true })
    await filtersBecome(feed, [SOLAR])
    expect(warnings).toEqual([])
  })
})

describe('R/ requests (§2)', () => {
  // Discrete values, set in an unsorted order so the full publish has to sort.
  function seed (feed) {
    feed.set(SOLAR, '{"value":412}')
    feed.set(SOC, '{"value":80}')
    feed.set(VOLTAGE, '{"value":13.2}')
  }

  test('a keepalive with suppress-republish publishes nothing', async () => {
    const { hub, feed } = await ready(broker)
    seed(feed)
    await firstAnnouncement(hub, feed, ['#'])
    const mark = hub.received.length

    await hub.publish(KEEPALIVE, '{"keepalive-options": ["suppress-republish"]}')
    // A fence: a mirror read sent after the keepalive is answered after it.
    await hub.publish(`R/${PORTAL}/battery/279/Soc`, '')
    await until(() => hub.received.length > mark)
    await settle()
    expect(hub.topics().slice(mark)).toEqual([SOC])
  })

  test('an empty keepalive publishes every matching value in sorted order, then full_publish_completed, none retained', async () => {
    const { hub, feed } = await ready(broker)
    seed(feed)
    await firstAnnouncement(hub, feed, ['#'])
    const mark = hub.received.length

    await hub.publish(KEEPALIVE, '')
    await until(() => hub.completions() === 2)
    expect(hub.received.slice(mark)).toEqual([
      { topic: VOLTAGE, payload: '{"value":13.2}', retain: false },
      { topic: SOC, payload: '{"value":80}', retain: false },
      { topic: SOLAR, payload: '{"value":412}', retain: false },
      { topic: SERIAL, payload: SERIAL_PAYLOAD, retain: false },
      { topic: COMPLETED, payload: `{"value":${NOW_S}}`, retain: false }
    ])

    // Retained only as the Serial was on connect: nothing else is held.
    const late = await retainedUnder(broker)
    expect(late).toEqual([{ topic: SERIAL, payload: SERIAL_PAYLOAD, retain: true }])
  })

  test('any other keepalive payload is a full publish', async () => {
    const { hub, feed } = await ready(broker)
    await firstAnnouncement(hub, feed, ['#'])
    const base = hub.completions()
    const payloads = ['{"keepalive-options": ["other"]}', '{"keepalive-options": "suppress-republish"}', 'null', '1']
    for (const [i, payload] of payloads.entries()) {
      await hub.publish(KEEPALIVE, payload)
      await until(() => hub.completions() === base + i + 1)
    }
  })

  test('a full publish carries only what the hub subscribed to, the completion included', async () => {
    const { hub, feed } = await ready(broker)
    seed(feed)
    await firstAnnouncement(hub, feed, [`N/${PORTAL}/battery/+/Soc`])
    const mark = hub.received.length

    await hub.publish(KEEPALIVE, '')
    await until(() => hub.received.length > mark)
    await settle()
    expect(hub.topics().slice(mark)).toEqual([SOC])
  })

  test('a one-topic read republishes that stored value', async () => {
    const { hub, feed } = await ready(broker)
    seed(feed)
    await firstAnnouncement(hub, feed, ['#'])
    const mark = hub.received.length

    await hub.publish(`R/${PORTAL}/battery/279/Soc`, '')
    await until(() => hub.received.length > mark)
    await settle()
    expect(hub.received.slice(mark)).toEqual([{ topic: SOC, payload: '{"value":80}', retain: false }])
  })

  test('a read for a topic with no stored value, or the bare portal, publishes nothing', async () => {
    const { hub, feed } = await ready(broker)
    seed(feed)
    await firstAnnouncement(hub, feed, ['#'])
    const mark = hub.received.length

    await hub.publish(`R/${PORTAL}/battery/279/Nope`, '')
    await hub.publish(`R/${PORTAL}`, '')
    await hub.publish(`R/${PORTAL}/battery/279/Soc`, '') // the fence
    await until(() => hub.received.length > mark)
    await settle()
    expect(hub.topics().slice(mark)).toEqual([SOC])
  })

  test('a read for a stored topic the hub did not subscribe to publishes nothing', async () => {
    const { hub, feed } = await ready(broker)
    seed(feed)
    await firstAnnouncement(hub, feed, [SOC])
    const mark = hub.received.length

    await hub.publish(`R/${PORTAL}/battery/279/Dc/0/Voltage`, '')
    await hub.publish(`R/${PORTAL}/battery/279/Soc`, '') // the fence
    await until(() => hub.received.length > mark)
    await settle()
    expect(hub.topics().slice(mark)).toEqual([SOC])
  })
})

describe('W/ writes (§2) and the writable flag (§4.2)', () => {
  const WRITABLE = `wattknot/producer/${PORTAL}/writable`

  // What a hub subscribing late finds retained under the producer's own topic.
  async function retainedFlag () {
    const late = await startHub(broker, 'late-flag')
    await late.client.subscribeAsync('wattknot/producer/#')
    await settle()
    return late.received.filter((m) => m.topic.startsWith('wattknot/producer/'))
  }

  test('without onWrite a write is refused: logged at warn without its payload, nothing changed or published', async () => {
    const { hub, feed, warnings } = await ready(broker)
    feed.set(SOC, '{"value":80}')
    await firstAnnouncement(hub, feed, ['#'])
    const mark = hub.received.length
    const before = feed.counters()

    await hub.publish(`W/${PORTAL}/battery/279/Soc`, '{"value":5}')
    await until(() => warnings.length === 1)
    expect(warnings[0]).toBe(`refused write W/${PORTAL}/battery/279/Soc: writes are not allowed`)
    await settle()
    expect(hub.received.length).toBe(mark)
    expect(feed.counters()).toEqual(before)

    await hub.publish(`R/${PORTAL}/battery/279/Soc`, '')
    await until(() => hub.received.length > mark)
    expect(hub.received[mark].payload).toBe('{"value":80}')
  })

  test('without onWrite, refusals are logged on change: the first at warn, every later one at debug with the count', async () => {
    const { hub, warnings, debugs } = await ready(broker)
    await hub.publish(`W/${PORTAL}/battery/279/Soc`, '{"value":5}')
    await hub.publish(`W/${PORTAL}/vebus/276/Mode`, '{"value":4}')
    await hub.publish(`W/${PORTAL}/battery/279/Soc`, '{"value":6}')
    await until(() => debugs.length === 2)
    expect(warnings).toEqual([`refused write W/${PORTAL}/battery/279/Soc: writes are not allowed`])
    expect(debugs).toEqual([
      `refused write W/${PORTAL}/vebus/276/Mode: writes are not allowed (refusal 2)`,
      `refused write W/${PORTAL}/battery/279/Soc: writes are not allowed (refusal 3)`
    ])
  })

  test('without onWrite the flag says false, retained, so a hub shows its controls read-only', async () => {
    await ready(broker)
    expect(await retainedFlag()).toEqual([{ topic: WRITABLE, payload: 'false', retain: true }])
  })

  test('with onWrite the flag says true, and a write is handed over with its topic and payload, unlogged', async () => {
    const writes = []
    const { hub, feed, warnings } = await ready(broker, { onWrite: (topic, text) => writes.push([topic, text]) })
    expect(await retainedFlag()).toEqual([{ topic: WRITABLE, payload: 'true', retain: true }])
    await firstAnnouncement(hub, feed, ['#'])
    const mark = hub.received.length

    await hub.publish(`W/${PORTAL}/vebus/276/Mode`, '{"value":4}')
    await until(() => writes.length === 1)
    expect(writes).toEqual([[`W/${PORTAL}/vebus/276/Mode`, '{"value":4}']])
    await settle()
    expect(warnings).toEqual([])
    // The set is the caller's; the feed itself publishes nothing for a write.
    expect(hub.received.length).toBe(mark)
  })

  test('the flag is re-sent on every connect, after the Serial', async () => {
    const { hub, feed, made } = await ready(broker)
    await hub.client.subscribeAsync('wattknot/producer/#')
    await settle() // the retained flag from the first connect lands on subscribe
    const mark = hub.received.length
    broker.kick('feed')
    await until(() => hub.received.length >= mark + 2)
    expect(hub.received.slice(mark).map((m) => [m.topic, m.payload])).toEqual([[SERIAL, SERIAL_PAYLOAD], [WRITABLE, 'false']])
    expect(feed.isConnected()).toBe(true)
    expect(made).toHaveLength(1)
  })
})

describe('forgetting values', () => {
  test('remove and removeWhere forget topics, so a full publish and a read omit them', async () => {
    const { hub, feed } = await ready(broker)
    const other = `N/${PORTAL}/battery/2790/Soc` // shares a prefix only without the slash
    feed.set(SOC, '{"value":80}')
    feed.set(VOLTAGE, '{"value":13.2}')
    feed.set(SOLAR, '{"value":412}')
    feed.set(other, '{"value":55}')

    feed.remove(SOLAR)
    feed.remove(`N/${PORTAL}/never/0/Set`) // forgetting what was never held is harmless
    feed.removeWhere(`N/${PORTAL}/battery/279/`)

    // The full publish the first announcement brings already omits them…
    const first = hub.received.length
    await hub.announce('bau', ['#'])
    await until(() => hub.completions() === 1)
    expect(hub.topics().slice(first)).toEqual([other, SERIAL, COMPLETED])

    // …and so do a read and a requested full publish.
    const mark = hub.received.length
    await hub.publish(`R/${PORTAL}/battery/279/Soc`, '')
    await hub.publish(KEEPALIVE, '')
    await until(() => hub.completions() === 2)
    expect(hub.topics().slice(mark)).toEqual([other, SERIAL, COMPLETED])
  })

  test('a removed topic set again is a change', async () => {
    const { hub, feed } = await ready(broker)
    await firstAnnouncement(hub, feed, ['#'])
    const mark = hub.received.length
    feed.set(SOC, '{"value":80}')
    feed.remove(SOC)
    feed.set(SOC, '{"value":80}')
    await until(() => hub.received.length === mark + 2)
  })
})

describe('the link', () => {
  test('a dropped connection reports offline, reconnects, re-subscribes and re-publishes the Serial retained (§3)', async () => {
    const { hub, feed, statuses } = await ready(broker, { reconnectPeriod: 300 })
    await firstAnnouncement(hub, feed, ['#'])
    await hub.announce('gone', [SOC])
    await filtersBecome(feed, ['#', SOC])
    // Clear the retained Serial so only a re-publish can put it back.
    await hub.publish(SERIAL, '', { retain: true })
    expect(await retainedUnder(broker)).toEqual([])

    expect(broker.clients().sort()).toEqual(['feed', 'hub', 'late'])
    broker.kick('feed')
    await until(() => statuses.includes('offline'))
    expect(feed.isConnected()).toBe(false)
    // While the feed is away the hub clears one source: the retained
    // announcement disappears, so only a rebuilt filter set can notice.
    await hub.publish(ANNOUNCE + 'gone', '', { retain: true })

    await until(() => statuses.filter((s) => s === 'connected').length === 2)
    expect(statuses).toEqual(['connecting', 'connected', 'offline', 'reconnecting', 'connected'])
    await filtersBecome(feed, ['#'])

    const late = await startHub(broker, 'late2')
    await until(() => late.received.length === 1)
    expect(late.received).toEqual([{ topic: SERIAL, payload: SERIAL_PAYLOAD, retain: true }])
  })

  test('a client error is reported as status', async () => {
    const { feed, statuses } = startFeed({ url: 'mqtt://127.0.0.1:1' }, { reconnectPeriod: 0 })
    await until(() => statuses.some((s) => s.startsWith('error: ')))
    expect(statuses[0]).toBe('connecting')
    expect(statuses.find((s) => s.startsWith('error: '))).toMatch(/^error: connect ECONNREFUSED 127\.0\.0\.1:1$/)
    expect(feed.isConnected()).toBe(false)
  })
})

describe('lifecycle', () => {
  test('start twice is one client', async () => {
    const { feed, made } = startFeed(broker)
    feed.start()
    await until(() => feed.isConnected())
    await settle()
    expect(made).toHaveLength(1)
    expect(broker.clients()).toEqual(['feed'])
  })

  test('stop ends the client, and its late events reach nothing', async () => {
    const { feed, statuses, warnings, made } = startFeed(broker)
    await until(() => feed.isConnected())
    feed.stop()
    expect(feed.isConnected()).toBe(false)
    await until(() => broker.clients().length === 0)
    expect(made[0].disconnecting).toBe(true)

    const seen = [...statuses]
    const before = feed.counters()
    const client = made[0]
    client.emit('connect', {})
    client.emit('message', KEEPALIVE, Buffer.from(''), {})
    client.emit('message', `W/${PORTAL}/x`, Buffer.from(''), {})
    client.emit('message', ANNOUNCE + 'bau', Buffer.from('["#"]'), {})
    client.emit('offline')
    client.emit('reconnect')
    client.emit('error', new Error('late'))
    expect(statuses).toEqual(seen)
    expect(warnings).toEqual([])
    expect(feed.filters()).toEqual([])
    expect(feed.counters()).toEqual(before)
  })

  test('stop before start, and stop twice, are harmless', () => {
    const { feed, made } = startFeed(broker, { autoStart: false })
    feed.stop()
    feed.start()
    feed.stop()
    feed.stop()
    expect(made).toHaveLength(1)
  })

  test('start after stop makes a new client', async () => {
    const { feed, made, statuses } = startFeed(broker)
    await until(() => feed.isConnected())
    feed.stop()
    feed.start()
    await until(() => feed.isConnected())
    expect(made).toHaveLength(2)
    expect(statuses).toEqual(['connecting', 'connected', 'connecting', 'connected'])
  })
})
