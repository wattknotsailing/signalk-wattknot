'use strict'

// D-Bus access against a fake dbus-native: systemBus() hands back a bus whose
// invoke() answers from a table the test fills, keyed `destination member`,
// and whose connection is an EventEmitter the test emits signals and
// failures on. Answers arrive on the next tick, as a real reply would, unless
// a test holds one to choose the moment itself.

const { EventEmitter } = require('events')
const { createVenus, NO_GX } = require('./venus')

const DBUS = 'org.freedesktop.DBus'
const BUS_ITEM = 'com.victronenergy.BusItem'
const PORTAL_ID = '102c6bf7d60f'

// A signature tree node as dbus-native's classic shapes carry it.
const sig = (type) => ({ type, child: [] })
const variant = (type, value) => [[sig(type)], [value]]

// The three match rules venus.js subscribes, restated so a change to any of
// them shows up here as a decision.
const RULES = [
  `type='signal',interface='${BUS_ITEM}',member='ItemsChanged'`,
  `type='signal',interface='${BUS_ITEM}',member='PropertiesChanged'`,
  `type='signal',sender='${DBUS}',interface='${DBUS}',member='NameOwnerChanged'`
]

// The classic value shapes venus.js asks for, restated like the rules.
const CLASSIC = { plainValues: false, variants: 'tree', returnBigInt: false }

// GetItems bodies, a{sa{sv}}: [[path, [[key, variant], …]], …].
const ITEMS = {
  'com.victronenergy.system': [
    ['/Serial', [['Value', variant('s', PORTAL_ID)], ['Text', variant('s', PORTAL_ID)]]]
  ],
  'com.victronenergy.battery.ttyUSB0': [
    ['/DeviceInstance', [['Value', variant('i', 279)], ['Text', variant('s', '279')]]],
    ['/Soc', [['Value', variant('d', 87.5)], ['Text', variant('s', '87.5%')]]]
  ],
  'com.victronenergy.settings': [
    ['/Settings/System/TimeZone', [['Value', variant('s', 'Europe/London')], ['Text', variant('s', 'Europe/London')]]]
  ]
}

const OWNERS = {
  'com.victronenergy.system': ':1.10',
  'com.victronenergy.battery.ttyUSB0': ':1.11',
  'com.victronenergy.settings': ':1.12',
  'com.victronenergy.vecan.can0': ':1.13'
}

// Table answers: each is (msg, cb) => void.
const reply = (value) => (msg, cb) => process.nextTick(cb, null, value)
const failWith = (message) => (msg, cb) => process.nextTick(cb, new Error(message))

function defaultTable () {
  return {
    [`${DBUS} AddMatch`]: reply(undefined),
    [`${DBUS} ListNames`]: reply([
      DBUS,
      ':1.10',
      'com.victronenergy.system',
      'org.bluez',
      'com.victronenergy.battery.ttyUSB0',
      'com.victronenergy.settings'
    ]),
    [`${DBUS} GetNameOwner`]: (msg, cb) => process.nextTick(cb, null, OWNERS[msg.body[0]]),
    'com.victronenergy.system GetValue': reply(variant('s', PORTAL_ID)),
    'com.victronenergy.system GetItems': reply(ITEMS['com.victronenergy.system']),
    'com.victronenergy.battery.ttyUSB0 GetItems': reply(ITEMS['com.victronenergy.battery.ttyUSB0']),
    'com.victronenergy.settings GetItems': reply(ITEMS['com.victronenergy.settings']),
    'com.victronenergy.vecan.can0 GetItems': reply([['/Mode', [['Value', variant('i', 1)]]]])
  }
}

// A fake dbus-native module. `calls` records every invoke() and the shapes
// set, in order; `held` keeps answers a test chose to release itself.
function fakeDbus (table = defaultTable()) {
  const calls = []
  const held = []
  const connection = new EventEmitter()
  connection.ended = 0
  connection.end = () => { connection.ended++ }
  connection.setValueShapes = (shapes) => {
    calls.push({ setValueShapes: shapes })
    return connection
  }
  const bus = {
    connection,
    invoke (msg, cb) {
      calls.push(msg)
      const answer = table[`${msg.destination} ${msg.member}`]
      if (answer === 'hold') held.push({ msg, cb })
      else answer(msg, cb)
    }
  }
  let made = 0
  const dbus = {
    systemBus () {
      made++
      return bus
    }
  }
  return { dbus, bus, connection, calls, held, table, made: () => made }
}

// Hand-driven timers: the retry of a failed read fires when a test says so.
function fakeTimers () {
  let next = 1
  const timers = {
    timeouts: new Map(),
    setTimeout (fn, ms) {
      const id = next++
      timers.timeouts.set(id, { fn, ms })
      return id
    },
    clearTimeout (id) { timers.timeouts.delete(id) },
    // Fires every pending timeout once.
    fire () {
      for (const [id, { fn }] of [...timers.timeouts.entries()]) {
        timers.timeouts.delete(id)
        fn()
      }
    }
  }
  return timers
}

// A venus under test with recording callbacks.
function setup (table) {
  const fake = fakeDbus(table)
  const items = []
  const gone = []
  const errors = []
  const logger = { debug: jest.fn(), error: jest.fn() }
  const timers = fakeTimers()
  const venus = createVenus({
    dbus: fake.dbus,
    timers,
    logger,
    onItems: (busName, body, source) => items.push({ busName, body, source }),
    onServiceGone: (busName) => gone.push(busName),
    onError: (err) => errors.push(err)
  })
  return { ...fake, venus, items, gone, errors, logger, timers }
}

// Lets the fake's next-tick answers and the awaits behind them land.
const settle = () => new Promise((resolve) => setImmediate(resolve))

const signal = (fields) => ({ type: 4, ...fields })
const itemsChanged = (sender, body) => signal({ sender, interface: BUS_ITEM, member: 'ItemsChanged', path: '/', body: [body] })
const ownerChanged = (name, oldOwner, newOwner) =>
  signal({ sender: DBUS, interface: DBUS, member: 'NameOwnerChanged', path: '/org/freedesktop/DBus', body: [name, oldOwner, newOwner] })

const SOC_CHANGE = [['/Soc', [['Value', variant('d', 88)], ['Text', variant('s', '88%')]]]]

describe('start()', () => {
  test('onPortalId is told the portal id before any item is reported, so the caller can build its renderer first', async () => {
    const t = setup()
    const order = []
    const venus = createVenus({
      dbus: t.dbus, logger: t.logger,
      onPortalId: (id) => order.push(['portalId', id]),
      onItems: (busName) => order.push(['items', busName]),
      onServiceGone: () => {}, onError: () => {}
    })
    await venus.start()
    expect(order[0]).toEqual(['portalId', PORTAL_ID])
    expect(order.slice(1).every(([kind]) => kind === 'items')).toBe(true)
    expect(order.length).toBe(4)
    venus.stop()
  })

  test('reads the portal id, every Victron service once, and subscribes the three signals', async () => {
    const t = setup()
    const result = await t.venus.start()

    expect(result).toEqual({ portalId: PORTAL_ID, services: 3 })
    expect(t.made()).toBe(1)

    const methodCalls = t.calls.filter((c) => c.member !== undefined)
    expect(methodCalls[0]).toEqual({ destination: 'com.victronenergy.system', path: '/Serial', interface: BUS_ITEM, member: 'GetValue' })

    const matches = methodCalls.filter((c) => c.member === 'AddMatch')
    expect(matches).toEqual(RULES.map((rule) => ({
      destination: DBUS, path: '/org/freedesktop/DBus', interface: DBUS, member: 'AddMatch', signature: 's', body: [rule]
    })))

    expect(methodCalls.find((c) => c.member === 'ListNames'))
      .toEqual({ destination: DBUS, path: '/org/freedesktop/DBus', interface: DBUS, member: 'ListNames' })

    // Only com.victronenergy.* names, each with GetNameOwner then GetItems on '/'.
    const perService = methodCalls.filter((c) => c.member === 'GetNameOwner' || c.member === 'GetItems')
    expect(perService.map((c) => [c.member, c.member === 'GetNameOwner' ? c.body[0] : c.destination])).toEqual([
      ['GetNameOwner', 'com.victronenergy.system'],
      ['GetItems', 'com.victronenergy.system'],
      ['GetNameOwner', 'com.victronenergy.battery.ttyUSB0'],
      ['GetItems', 'com.victronenergy.battery.ttyUSB0'],
      ['GetNameOwner', 'com.victronenergy.settings'],
      ['GetItems', 'com.victronenergy.settings']
    ])
    expect(perService[0]).toEqual({ destination: DBUS, path: '/org/freedesktop/DBus', interface: DBUS, member: 'GetNameOwner', signature: 's', body: ['com.victronenergy.system'] })
    expect(perService[1]).toEqual({ destination: 'com.victronenergy.system', path: '/', interface: BUS_ITEM, member: 'GetItems' })

    expect(t.items).toEqual([
      { busName: 'com.victronenergy.system', body: ITEMS['com.victronenergy.system'], source: 'GetItems' },
      { busName: 'com.victronenergy.battery.ttyUSB0', body: ITEMS['com.victronenergy.battery.ttyUSB0'], source: 'GetItems' },
      { busName: 'com.victronenergy.settings', body: ITEMS['com.victronenergy.settings'], source: 'GetItems' }
    ])
    expect(t.logger.error).not.toHaveBeenCalled()
    expect(t.logger.debug).toHaveBeenCalledWith('D-Bus: 3 Victron services')
    expect(t.logger.debug).toHaveBeenCalledWith('D-Bus: com.victronenergy.battery.ttyUSB0 read, 2 paths')
    expect(t.errors).toEqual([])
    expect(t.connection.ended).toBe(0)
  })

  test('asks for the classic value shapes before the first call, so every variant keeps its signature', async () => {
    const t = setup()
    await t.venus.start()
    expect(t.calls[0]).toEqual({ setValueShapes: CLASSIC })
  })

  test('subscribes before listing, so a service appearing mid-read is not missed', async () => {
    const t = setup()
    await t.venus.start()
    const members = t.calls.map((c) => c.member)
    expect(members.lastIndexOf('AddMatch')).toBeLessThan(members.indexOf('ListNames'))
  })

  test('a service whose GetItems fails is logged and skipped; the rest are read', async () => {
    const table = defaultTable()
    table['com.victronenergy.battery.ttyUSB0 GetItems'] = failWith('org.freedesktop.DBus.Error.NoReply')
    const t = setup(table)

    expect(await t.venus.start()).toEqual({ portalId: PORTAL_ID, services: 2 })
    expect(t.items.map((i) => i.busName)).toEqual(['com.victronenergy.system', 'com.victronenergy.settings'])
    expect(t.logger.error).toHaveBeenCalledWith(
      'D-Bus: com.victronenergy.battery.ttyUSB0 not read: org.freedesktop.DBus.Error.NoReply; retrying every 60 s')

    // Skipped entirely: its changes are not reported without its snapshot.
    t.connection.emit('message', itemsChanged(':1.11', SOC_CHANGE))
    expect(t.items).toHaveLength(2)
  })

  test('a service whose GetNameOwner fails is logged and skipped', async () => {
    const table = defaultTable()
    table[`${DBUS} GetNameOwner`] = (msg, cb) => msg.body[0] === 'com.victronenergy.settings'
      ? process.nextTick(cb, new Error('NameHasNoOwner'))
      : process.nextTick(cb, null, OWNERS[msg.body[0]])
    const t = setup(table)

    expect(await t.venus.start()).toEqual({ portalId: PORTAL_ID, services: 2 })
    expect(t.calls.some((c) => c.destination === 'com.victronenergy.settings')).toBe(false)
    expect(t.logger.error).toHaveBeenCalledWith('D-Bus: com.victronenergy.settings not read: NameHasNoOwner; retrying every 60 s')
  })

  describe('a failed read is retried', () => {
    // GetItems on the battery (and, asked for, the settings) fails until the
    // test says otherwise.
    function flakyTable ({ settingsToo = false } = {}) {
      const table = defaultTable()
      const state = { failing: true }
      const flaky = (name) => (msg, cb) => state.failing
        ? process.nextTick(cb, new Error('NoReply'))
        : process.nextTick(cb, null, ITEMS[name])
      table['com.victronenergy.battery.ttyUSB0 GetItems'] = flaky('com.victronenergy.battery.ttyUSB0')
      if (settingsToo) table['com.victronenergy.settings GetItems'] = flaky('com.victronenergy.settings')
      return { table, state }
    }

    test('after 60 s, all failures on one timer; once read, a service is followed like the rest', async () => {
      const { table, state } = flakyTable({ settingsToo: true })
      const t = setup(table)
      expect(await t.venus.start()).toEqual({ portalId: PORTAL_ID, services: 1 })
      expect([...t.timers.timeouts.values()].map((x) => x.ms)).toEqual([60000])
      t.items.length = 0

      state.failing = false
      t.timers.fire()
      await settle()
      await settle()
      await settle()
      expect(t.items.map((i) => i.busName)).toEqual(['com.victronenergy.battery.ttyUSB0', 'com.victronenergy.settings'])
      expect(t.logger.debug).toHaveBeenCalledWith('D-Bus: com.victronenergy.battery.ttyUSB0 read on retry')
      expect(t.logger.debug).toHaveBeenCalledWith('D-Bus: com.victronenergy.settings read on retry')
      // Read, so no further retry is owed.
      expect(t.timers.timeouts.size).toBe(0)

      t.connection.emit('message', itemsChanged(':1.11', SOC_CHANGE))
      expect(t.items).toHaveLength(3)
    })

    test('a failure that repeats is logged at error once, then at debug', async () => {
      const { table } = flakyTable()
      const t = setup(table)
      await t.venus.start()

      for (let round = 0; round < 3; round++) {
        t.timers.fire()
        await settle()
        await settle()
      }
      expect(t.logger.error).toHaveBeenCalledTimes(1)
      expect(t.logger.debug.mock.calls.filter(([text]) => text.startsWith('D-Bus: com.victronenergy.battery.ttyUSB0 still not read'))).toHaveLength(3)
      // Still owed: the next round is scheduled.
      expect([...t.timers.timeouts.values()].map((x) => x.ms)).toEqual([60000])
    })

    test('a service appearing whose read fails is retried the same way', async () => {
      const table = defaultTable()
      table['com.victronenergy.vecan.can0 GetItems'] = failWith('ServiceUnknown')
      const t = setup(table)
      await t.venus.start()
      expect(t.timers.timeouts.size).toBe(0)

      t.connection.emit('message', ownerChanged('com.victronenergy.vecan.can0', '', ':1.13'))
      await settle()
      await settle()
      expect(t.logger.error).toHaveBeenCalledWith('D-Bus: com.victronenergy.vecan.can0 not read: ServiceUnknown; retrying every 60 s')
      expect([...t.timers.timeouts.values()].map((x) => x.ms)).toEqual([60000])
    })

    test('a failed service that leaves the bus is no longer retried', async () => {
      const { table } = flakyTable()
      const t = setup(table)
      await t.venus.start()
      const reads = () => t.calls.filter((c) => c.destination === 'com.victronenergy.battery.ttyUSB0' && c.member === 'GetItems').length
      expect(reads()).toBe(1)

      t.connection.emit('message', ownerChanged('com.victronenergy.battery.ttyUSB0', ':1.11', ''))
      t.timers.fire()
      await settle()
      await settle()
      expect(reads()).toBe(1)
      expect(t.timers.timeouts.size).toBe(0)
    })

    test('stop() cancels the retry, and a round in progress reads nothing more', async () => {
      const { table } = flakyTable()
      const t = setup(table)
      await t.venus.start()
      expect(t.timers.timeouts.size).toBe(1)
      t.venus.stop()
      expect(t.timers.timeouts.size).toBe(0)

      // A round already running when stop() lands: the read in flight is
      // refused and the loop goes no further.
      const second = setup(flakyTable({ settingsToo: true }).table)
      await second.venus.start()
      second.calls.length = 0
      second.timers.fire()
      second.venus.stop()
      await settle()
      await settle()
      expect(second.calls.map((c) => c.destination)).toEqual([DBUS])
      expect(second.logger.debug.mock.calls.filter(([text]) => text.includes('still not read'))).toHaveLength(0)
    })
  })

  test('a GetItems reply that is not an a{sa{sv}} is logged and skipped', async () => {
    const table = defaultTable()
    table['com.victronenergy.settings GetItems'] = reply({ '/Foo': {} })
    const t = setup(table)

    expect(await t.venus.start()).toEqual({ portalId: PORTAL_ID, services: 2 })
    expect(t.logger.error).toHaveBeenCalledWith('D-Bus: com.victronenergy.settings not read: GetItems reply is not an a{sa{sv}}; retrying every 60 s')
  })

  test('an onItems that throws is logged and does not stop the read', async () => {
    const t = setup()
    const onItems = jest.fn(() => { throw new Error('renderer bug') })
    const venus = createVenus({ dbus: t.dbus, logger: t.logger, onItems, onServiceGone: jest.fn(), onError: jest.fn() })

    expect(await venus.start()).toEqual({ portalId: PORTAL_ID, services: 3 })
    expect(onItems).toHaveBeenCalledTimes(3)
    expect(t.logger.error).toHaveBeenCalledWith('D-Bus: onItems for com.victronenergy.system threw: renderer bug')
  })

  test('no Victron services at all reads none', async () => {
    const table = defaultTable()
    table[`${DBUS} ListNames`] = reply([DBUS, 'org.bluez'])
    const t = setup(table)
    expect(await t.venus.start()).toEqual({ portalId: PORTAL_ID, services: 0 })
    expect(t.items).toEqual([])
  })

  test.each([
    ['GetValue fails', 'com.victronenergy.system GetValue', failWith('ServiceUnknown'), 'ServiceUnknown'],
    ['the portal id is a Venus invalid', 'com.victronenergy.system GetValue', reply([[sig('a'), sig('i')], [[]]]), 'com.victronenergy.system /Serial is not a portal id'],
    ['the portal id is empty', 'com.victronenergy.system GetValue', reply(variant('s', '')), 'com.victronenergy.system /Serial is not a portal id'],
    ['the portal id is not a variant', 'com.victronenergy.system GetValue', reply(PORTAL_ID), 'com.victronenergy.system /Serial is not a portal id'],
    ['the portal id variant has no value list', 'com.victronenergy.system GetValue', reply([[sig('s')], PORTAL_ID]), 'com.victronenergy.system /Serial is not a portal id'],
    ['AddMatch fails', `${DBUS} AddMatch`, failWith('AccessDenied'), 'AccessDenied'],
    ['ListNames fails', `${DBUS} ListNames`, failWith('NoReply'), 'NoReply']
  ])('rejects with the connection ended when %s', async (_, key, answer, message) => {
    const table = defaultTable()
    table[key] = answer
    const t = setup(table)

    await expect(t.venus.start()).rejects.toThrow(message)
    expect(t.connection.ended).toBe(1)
    expect(t.items).toEqual([])
    // A failed start is the caller's rejection, not a connection failure.
    t.connection.emit('end')
    expect(t.errors).toEqual([])
  })

  describe('a bus with no GX on it (NO_GX)', () => {
    const NOT_A_GX = [DBUS, ':1.10', 'org.bluez', 'org.freedesktop.NetworkManager']

    test('no portal id and no Victron service at all: rejects with code NO_GX, the connection ended', async () => {
      const table = defaultTable()
      table['com.victronenergy.system GetValue'] = failWith('org.freedesktop.DBus.Error.ServiceUnknown')
      table[`${DBUS} ListNames`] = reply(NOT_A_GX)
      const t = setup(table)

      const err = await t.venus.start().catch((e) => e)
      expect(err.message).toBe('no GX on this bus')
      expect(err.code).toBe(NO_GX)
      expect(NO_GX).toBe('NO_GX')
      expect(t.connection.ended).toBe(1)
      expect(t.items).toEqual([])
      expect(t.logger.error).not.toHaveBeenCalled()
      // Asked in this order: the portal id, then why there is none.
      expect(t.calls.filter((c) => c.member !== undefined).map((c) => c.member)).toEqual(['GetValue', 'ListNames'])
    })

    test('no portal id but Victron services present is the original failure, not NO_GX', async () => {
      const table = defaultTable()
      table['com.victronenergy.system GetValue'] = failWith('NoReply')
      const t = setup(table)
      const err = await t.venus.start().catch((e) => e)
      expect(err.message).toBe('NoReply')
      expect(err.code).toBeUndefined()
    })

    test('no portal id and a bus that cannot say what it carries is the original failure', async () => {
      for (const answer of [failWith('AccessDenied'), reply('not a list')]) {
        const table = defaultTable()
        table['com.victronenergy.system GetValue'] = failWith('ServiceUnknown')
        table[`${DBUS} ListNames`] = answer
        const t = setup(table)
        const err = await t.venus.start().catch((e) => e)
        expect(err.message).toBe('ServiceUnknown')
        expect(err.code).toBeUndefined()
      }
    })

    test('on a GX nothing more is asked: the bus is listed once, after the portal id', async () => {
      const t = setup()
      await t.venus.start()
      expect(t.calls.filter((c) => c.member === 'ListNames')).toHaveLength(1)
    })
  })

  test('runs once per instance', async () => {
    const t = setup()
    await t.venus.start()
    await expect(t.venus.start()).rejects.toThrow('D-Bus access runs once per instance')
    expect(t.made()).toBe(1)
  })

  test('cannot start after stop()', async () => {
    const t = setup()
    t.venus.stop()
    await expect(t.venus.start()).rejects.toThrow('D-Bus access runs once per instance')
    expect(t.made()).toBe(0)
  })

  test('stop() mid-read rejects start, reports nothing more and logs no failures', async () => {
    const table = defaultTable()
    table['com.victronenergy.battery.ttyUSB0 GetItems'] = 'hold'
    const t = setup(table)
    const started = t.venus.start()
    await settle()
    await settle()
    expect(t.held).toHaveLength(1)

    t.venus.stop()
    // The real library fails a pending call when the connection closes.
    t.held[0].cb(new Error('connection closed'))

    await expect(started).rejects.toThrow('D-Bus access stopped')
    expect(t.items.map((i) => i.busName)).toEqual(['com.victronenergy.system'])
    // settings was never asked for: nothing goes out on a stopped instance.
    expect(t.calls.some((c) => c.destination === 'com.victronenergy.settings')).toBe(false)
    expect(t.logger.error).not.toHaveBeenCalled()
    expect(t.connection.ended).toBe(1)
  })

  test('a reply landing after stop() is not delivered', async () => {
    const table = defaultTable()
    table['com.victronenergy.battery.ttyUSB0 GetItems'] = 'hold'
    const t = setup(table)
    const started = t.venus.start()
    await settle()
    await settle()

    t.venus.stop()
    t.held[0].cb(null, ITEMS['com.victronenergy.battery.ttyUSB0'])

    await expect(started).rejects.toThrow('D-Bus access stopped')
    expect(t.items.map((i) => i.busName)).toEqual(['com.victronenergy.system'])
  })

  test('a change sent straight after the snapshot is reported after it, not before or never', async () => {
    const table = defaultTable()
    table['com.victronenergy.battery.ttyUSB0 GetItems'] = 'hold'
    const t = setup(table)
    const started = t.venus.start()
    await settle()
    await settle()

    // Before the reply: the service is not yet entered, so the change is
    // dropped; D-Bus ordering puts it inside the snapshot that follows.
    t.connection.emit('message', itemsChanged(':1.11', SOC_CHANGE))
    expect(t.items.map((i) => i.busName)).toEqual(['com.victronenergy.system'])

    // dbus-native emits every message of one read synchronously: the reply,
    // then the change, with no await in between.
    t.held[0].cb(null, ITEMS['com.victronenergy.battery.ttyUSB0'])
    t.connection.emit('message', itemsChanged(':1.11', SOC_CHANGE))

    expect(t.items.slice(1)).toEqual([
      { busName: 'com.victronenergy.battery.ttyUSB0', body: ITEMS['com.victronenergy.battery.ttyUSB0'], source: 'GetItems' },
      { busName: 'com.victronenergy.battery.ttyUSB0', body: SOC_CHANGE, source: 'ItemsChanged' }
    ])
    await started
  })
})

describe('signals', () => {
  async function started (table) {
    const t = setup(table)
    await t.venus.start()
    t.items.length = 0
    t.logger.debug.mockClear()
    return t
  }

  test('ItemsChanged from a known service is reported with its body', async () => {
    const t = await started()
    t.connection.emit('message', itemsChanged(':1.11', SOC_CHANGE))
    expect(t.items).toEqual([{ busName: 'com.victronenergy.battery.ttyUSB0', body: SOC_CHANGE, source: 'ItemsChanged' }])
  })

  test('ItemsChanged from an unknown sender is ignored', async () => {
    const t = await started()
    t.connection.emit('message', itemsChanged(':1.99', SOC_CHANGE))
    expect(t.items).toEqual([])
    expect(t.logger.error).not.toHaveBeenCalled()
  })

  test('PropertiesChanged is reported in the GetItems shape, for its own path', async () => {
    const t = await started()
    const props = [['Value', variant('d', 13.2)], ['Text', variant('s', '13.2V')]]
    t.connection.emit('message', signal({ sender: ':1.11', interface: BUS_ITEM, member: 'PropertiesChanged', path: '/Dc/0/Voltage', body: [props] }))
    expect(t.items).toEqual([{ busName: 'com.victronenergy.battery.ttyUSB0', body: [['/Dc/0/Voltage', props]], source: 'PropertiesChanged' }])
  })

  test('another BusItem member, or another interface, from a known service is ignored', async () => {
    const t = await started()
    t.connection.emit('message', signal({ sender: ':1.11', interface: BUS_ITEM, member: 'SomethingElse', path: '/', body: [[]] }))
    t.connection.emit('message', signal({ sender: ':1.11', interface: 'org.freedesktop.DBus.Properties', member: 'PropertiesChanged', path: '/', body: [[]] }))
    expect(t.items).toEqual([])
  })

  test('non-signal messages are ignored', async () => {
    const t = await started()
    for (const type of [1, 2, 3]) {
      t.connection.emit('message', { type, sender: ':1.11', interface: BUS_ITEM, member: 'ItemsChanged', path: '/', body: [SOC_CHANGE] })
    }
    expect(t.items).toEqual([])
  })

  test.each([
    ['ItemsChanged with no body', itemsChanged(':1.11', SOC_CHANGE), (m) => { delete m.body }],
    ['ItemsChanged with a body that is not an array', itemsChanged(':1.11', SOC_CHANGE), (m) => { m.body = 'x' }],
    ['ItemsChanged whose argument is not an array', itemsChanged(':1.11', SOC_CHANGE), (m) => { m.body = [{}] }],
    ['PropertiesChanged whose argument is not an array', signal({ sender: ':1.11', interface: BUS_ITEM, member: 'PropertiesChanged', path: '/Soc' }), (m) => { m.body = [null] }],
    ['NameOwnerChanged with no body', ownerChanged('com.victronenergy.tank.x', '', ':1.50'), (m) => { delete m.body }],
    ['NameOwnerChanged with a field that is not a string', ownerChanged('com.victronenergy.tank.x', '', ':1.50'), (m) => { m.body = ['com.victronenergy.tank.x', null, ':1.50'] }]
  ])('a malformed signal is logged and dropped, never thrown: %s', async (_, msg, spoil) => {
    const t = await started()
    spoil(msg)
    expect(() => t.connection.emit('message', msg)).not.toThrow()
    expect(t.items).toEqual([])
    expect(t.logger.error).toHaveBeenCalledWith(expect.stringMatching(new RegExp(`^D-Bus: ${msg.member} from ${msg.sender} dropped: body is not`)))
    // The next good one still lands.
    t.connection.emit('message', itemsChanged(':1.11', SOC_CHANGE))
    expect(t.items).toHaveLength(1)
  })

  test('an onItems that throws on a signal is logged, never thrown into the bus', async () => {
    const t = setup()
    const onItems = jest.fn()
    const venus = createVenus({ dbus: t.dbus, logger: t.logger, onItems, onServiceGone: jest.fn(), onError: jest.fn() })
    await venus.start()
    onItems.mockImplementation(() => { throw new Error('renderer bug') })

    expect(() => t.connection.emit('message', itemsChanged(':1.11', SOC_CHANGE))).not.toThrow()
    expect(t.logger.error).toHaveBeenCalledWith('D-Bus: onItems for com.victronenergy.battery.ttyUSB0 threw: renderer bug')
  })

  describe('NameOwnerChanged', () => {
    test('a Victron service appearing is read and reported, then followed', async () => {
      const t = await started()
      const before = t.calls.length
      t.connection.emit('message', ownerChanged('com.victronenergy.vecan.can0', '', ':1.13'))
      await settle()
      await settle()

      expect(t.calls.slice(before).map((c) => c.member)).toEqual(['GetNameOwner', 'GetItems'])
      expect(t.items).toEqual([{ busName: 'com.victronenergy.vecan.can0', body: [['/Mode', [['Value', variant('i', 1)]]]], source: 'GetItems' }])
      expect(t.logger.debug).toHaveBeenCalledWith('D-Bus: com.victronenergy.vecan.can0 appeared')

      t.connection.emit('message', itemsChanged(':1.13', SOC_CHANGE))
      expect(t.items[1]).toEqual({ busName: 'com.victronenergy.vecan.can0', body: SOC_CHANGE, source: 'ItemsChanged' })
    })

    test('a service appearing whose read fails is logged, not thrown', async () => {
      const table = defaultTable()
      table['com.victronenergy.vecan.can0 GetItems'] = failWith('ServiceUnknown')
      const t = await started(table)
      t.connection.emit('message', ownerChanged('com.victronenergy.vecan.can0', '', ':1.13'))
      await settle()
      await settle()

      expect(t.items).toEqual([])
      expect(t.logger.error).toHaveBeenCalledWith('D-Bus: com.victronenergy.vecan.can0 not read: ServiceUnknown; retrying every 60 s')
    })

    test('a Victron service disappearing is reported gone, and its old unique name is no longer believed', async () => {
      const t = await started()
      t.connection.emit('message', ownerChanged('com.victronenergy.battery.ttyUSB0', ':1.11', ''))

      expect(t.gone).toEqual(['com.victronenergy.battery.ttyUSB0'])
      expect(t.logger.debug).toHaveBeenCalledWith('D-Bus: com.victronenergy.battery.ttyUSB0 gone')
      t.connection.emit('message', itemsChanged(':1.11', SOC_CHANGE))
      expect(t.items).toEqual([])
      // The others still are.
      t.connection.emit('message', itemsChanged(':1.12', SOC_CHANGE))
      expect(t.items).toHaveLength(1)
    })

    test('a service handed to a new owner drops the old and re-reads under the new', async () => {
      const table = defaultTable()
      const t = await started(table)
      table[`${DBUS} GetNameOwner`] = reply(':1.77')
      t.connection.emit('message', ownerChanged('com.victronenergy.battery.ttyUSB0', ':1.11', ':1.77'))
      await settle()
      await settle()

      expect(t.gone).toEqual([])
      expect(t.items).toEqual([{ busName: 'com.victronenergy.battery.ttyUSB0', body: ITEMS['com.victronenergy.battery.ttyUSB0'], source: 'GetItems' }])
      t.connection.emit('message', itemsChanged(':1.11', SOC_CHANGE))
      expect(t.items).toHaveLength(1)
      t.connection.emit('message', itemsChanged(':1.77', SOC_CHANGE))
      expect(t.items).toHaveLength(2)
    })

    test('an onServiceGone that throws is logged, never thrown into the bus', async () => {
      const t = setup()
      const onServiceGone = jest.fn(() => { throw new Error('feed bug') })
      const venus = createVenus({ dbus: t.dbus, logger: t.logger, onItems: jest.fn(), onServiceGone, onError: jest.fn() })
      await venus.start()

      expect(() => t.connection.emit('message', ownerChanged('com.victronenergy.settings', ':1.12', ''))).not.toThrow()
      expect(t.logger.error).toHaveBeenCalledWith('D-Bus: onServiceGone for com.victronenergy.settings threw: feed bug')
    })

    test('a non-Victron name is ignored', async () => {
      const t = await started()
      const before = t.calls.length
      t.connection.emit('message', ownerChanged('org.bluez', '', ':1.40'))
      t.connection.emit('message', ownerChanged(':1.40', '', ':1.40'))
      t.connection.emit('message', ownerChanged('org.bluez', ':1.40', ''))
      await settle()

      expect(t.calls.length).toBe(before)
      expect(t.items).toEqual([])
      expect(t.gone).toEqual([])
    })

    test('one not sent by the bus daemon is not believed', async () => {
      const t = await started()
      const forged = ownerChanged('com.victronenergy.battery.ttyUSB0', ':1.11', '')
      forged.sender = ':1.66'
      t.connection.emit('message', forged)
      expect(t.gone).toEqual([])
    })
  })
})

describe('the connection failing, and stop()', () => {
  async function started () {
    const t = setup()
    await t.venus.start()
    t.items.length = 0
    return t
  }

  test('an error is reported once and stops the instance', async () => {
    const t = await started()
    const failure = new Error('read ECONNRESET')
    t.connection.emit('error', failure)
    t.connection.emit('close', failure)
    t.connection.emit('error', new Error('again'))

    expect(t.errors).toEqual([failure])
    expect(t.connection.ended).toBe(1)
    t.connection.emit('message', itemsChanged(':1.11', SOC_CHANGE))
    t.connection.emit('message', ownerChanged('com.victronenergy.settings', ':1.12', ''))
    expect(t.items).toEqual([])
    expect(t.gone).toEqual([])
  })

  test('the connection ending is reported once', async () => {
    const t = await started()
    t.connection.emit('end')
    t.connection.emit('close')
    expect(t.errors).toHaveLength(1)
    expect(t.errors[0].message).toBe('D-Bus connection ended')
    expect(t.connection.ended).toBe(1)
  })

  test('the connection closing without an end is reported', async () => {
    const t = await started()
    t.connection.emit('close')
    expect(t.errors.map((e) => e.message)).toEqual(['D-Bus connection closed'])
  })

  test('a connection failure during start() is reported and start() rejects', async () => {
    const table = defaultTable()
    table[`${DBUS} ListNames`] = 'hold'
    const t = setup(table)
    const started = t.venus.start()
    await settle()
    await settle()

    const failure = new Error('EPIPE')
    t.connection.emit('error', failure)
    t.held[0].cb(new Error('connection closed'))

    await expect(started).rejects.toThrow('D-Bus access stopped')
    expect(t.errors).toEqual([failure])
    expect(t.connection.ended).toBe(1)
  })

  test('stop() ends the connection once, is idempotent, and makes every later event inert', async () => {
    const t = await started()
    t.venus.stop()
    t.venus.stop()
    expect(t.connection.ended).toBe(1)

    // The listeners stay attached: a late 'error' has somewhere to go.
    expect(t.connection.listenerCount('error')).toBe(1)
    t.connection.emit('error', new Error('late'))
    t.connection.emit('end')
    t.connection.emit('close')
    t.connection.emit('message', itemsChanged(':1.11', SOC_CHANGE))
    t.connection.emit('message', ownerChanged('com.victronenergy.vecan.can0', '', ':1.13'))
    t.connection.emit('message', ownerChanged('com.victronenergy.settings', ':1.12', ''))
    await settle()

    expect(t.errors).toEqual([])
    expect(t.items).toEqual([])
    expect(t.gone).toEqual([])
  })

  test('stop() before start() is safe and touches no bus', () => {
    const t = setup()
    t.venus.stop()
    expect(t.made()).toBe(0)
    expect(t.connection.ended).toBe(0)
  })
})

describe('set() — one SetValue per write the hub is allowed (§2 "W/ writes")', () => {
  const VEBUS = 'com.victronenergy.battery.ttyUSB0'
  const MODE = [[{ type: 'i', child: [] }], [4]]

  async function started (table) {
    const t = setup(table)
    await t.venus.start()
    t.calls.length = 0
    return t
  }

  test('sends the variant as given, signature v, to the service and path, and resolves on 0', async () => {
    const t = await started({ ...defaultTable(), [`${VEBUS} SetValue`]: reply(0) })
    await expect(t.venus.set(VEBUS, '/Mode', MODE)).resolves.toBeUndefined()
    expect(t.calls).toEqual([{ destination: VEBUS, path: '/Mode', interface: BUS_ITEM, member: 'SetValue', signature: 'v', body: [MODE] }])
  })

  test('a non-zero answer is the service refusing: rejects with the code', async () => {
    const t = await started({ ...defaultTable(), [`${VEBUS} SetValue`]: reply(-1) })
    await expect(t.venus.set(VEBUS, '/Mode', MODE)).rejects.toThrow('SetValue returned -1')
  })

  test('a D-Bus error rejects with it', async () => {
    const t = await started({ ...defaultTable(), [`${VEBUS} SetValue`]: (msg, cb) => process.nextTick(cb, new Error('no such path')) })
    await expect(t.venus.set(VEBUS, '/Mode', MODE)).rejects.toThrow('no such path')
  })

  test('after stop() a set is refused without touching the bus', async () => {
    const t = await started({ ...defaultTable(), [`${VEBUS} SetValue`]: reply(0) })
    t.venus.stop()
    await expect(t.venus.set(VEBUS, '/Mode', MODE)).rejects.toThrow('D-Bus access stopped')
    expect(t.calls).toEqual([])
  })
})
