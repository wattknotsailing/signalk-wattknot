'use strict'

// Every expected topic and payload here is written out by hand from the rules
// in protocol/mqtt-producer.md §2 and §4 (and MQTT 3.1.1 §4.7 for the
// matcher) — never produced by running the code under test.

const {
  serviceType,
  instanceFor,
  topicFor,
  isSkipped,
  serialTopic,
  fullPublishCompletedTopic,
  decodeVariant,
  itemsToObject,
  payloadFor,
  createMatcher,
  WRITABLE
} = require('./victron')

const { createRenderer } = require('./victron')

// One filter against one topic, through the compiled matcher the feed uses:
// the MQTT 3.1.1 rules below are pinned on the same path production takes.
const matches = (filter, topic) => createMatcher([filter]).matches(topic)

const PORTAL = '102c6bf7d60f'

// dbus-native's classic shapes (a connection opened with plainValues: false):
// a signature node is { type, child }, a variant is [[node], [value]].
const node = (type, ...child) => ({ type, child })
const variant = (sig, value) => [[sig], [value]]

const D = node('d')
const I = node('i')
const S = node('s')
const B = node('b')
const V = node('v')
const AI = node('a', I)
const ASV = node('a', node('{', S, V))

describe('service type and instance (§2)', () => {
  test('a battery with a /DeviceInstance uses it', () => {
    const busName = 'com.victronenergy.battery.ttyUSB0'
    expect(serviceType(busName)).toBe('battery')
    expect(instanceFor(busName, 279)).toBe(279)
    expect(topicFor({ portalId: PORTAL, busName, instance: 279, path: '/Dc/0/Voltage' }))
      .toBe('N/102c6bf7d60f/battery/279/Dc/0/Voltage')
  })

  test('system, with no suffix and no /DeviceInstance, is instance 0', () => {
    const busName = 'com.victronenergy.system'
    expect(serviceType(busName)).toBe('system')
    expect(instanceFor(busName, undefined)).toBe(0)
    expect(topicFor({ portalId: PORTAL, busName, instance: 0, path: '/Serial' }))
      .toBe('N/102c6bf7d60f/system/0/Serial')
  })

  test('vecan with no /DeviceInstance takes its name suffix', () => {
    const busName = 'com.victronenergy.vecan.can0'
    expect(serviceType(busName)).toBe('vecan')
    expect(instanceFor(busName, undefined)).toBe('can0')
    expect(topicFor({ portalId: PORTAL, busName, instance: 'can0', path: '/Alarms/SameUniqueNameUsed' }))
      .toBe('N/102c6bf7d60f/vecan/can0/Alarms/SameUniqueNameUsed')
  })

  test('only vecan uses the suffix: modbusclient.tcp is instance 0', () => {
    const busName = 'com.victronenergy.modbusclient.tcp'
    expect(serviceType(busName)).toBe('modbusclient')
    expect(instanceFor(busName, undefined)).toBe(0)
  })

  test('a /DeviceInstance wins over the vecan suffix', () => {
    expect(instanceFor('com.victronenergy.vecan.can0', 7)).toBe(7)
  })

  test('vecan with no suffix falls back to 0', () => {
    expect(instanceFor('com.victronenergy.vecan', undefined)).toBe(0)
  })

  test('a suffix with dots in it is kept whole', () => {
    expect(serviceType('com.victronenergy.vecan.can0.x')).toBe('vecan')
    expect(instanceFor('com.victronenergy.vecan.can0.x', undefined)).toBe('can0.x')
  })

  test('a /DeviceInstance that is not an integer (an invalid, a string) counts as absent', () => {
    expect(instanceFor('com.victronenergy.battery.ttyUSB0', null)).toBe(0)
    expect(instanceFor('com.victronenergy.battery.ttyUSB0', '279')).toBe(0)
    expect(instanceFor('com.victronenergy.vecan.can1', null)).toBe('can1')
  })

  test('a name that is not Victron\'s has no type and no topic', () => {
    for (const busName of ['org.freedesktop.DBus', ':1.42', 'com.victronenergy', 'com.victronenergy.', 'com.victronenergy..x']) {
      expect(serviceType(busName)).toBeNull()
      expect(topicFor({ portalId: PORTAL, busName, instance: 0, path: '/Serial' })).toBeNull()
    }
    expect(instanceFor('org.freedesktop.DBus', undefined)).toBe(0)
  })
})

describe('topics (§2)', () => {
  test('a path with or without its leading slash makes the same topic', () => {
    const item = { portalId: PORTAL, busName: 'com.victronenergy.solarcharger.ttyUSB1', instance: 290 }
    expect(topicFor({ ...item, path: '/Yield/Power' })).toBe('N/102c6bf7d60f/solarcharger/290/Yield/Power')
    expect(topicFor({ ...item, path: 'Yield/Power' })).toBe('N/102c6bf7d60f/solarcharger/290/Yield/Power')
  })

  test('Serial and full_publish_completed', () => {
    expect(serialTopic(PORTAL)).toBe('N/102c6bf7d60f/system/0/Serial')
    expect(fullPublishCompletedTopic(PORTAL)).toBe('N/102c6bf7d60f/full_publish_completed')
  })
})

describe('skip list (§2)', () => {
  test('system ServiceMapping is skipped, with or without the leading slash', () => {
    expect(isSkipped('/ServiceMapping/com_victronenergy_battery_279', 'com.victronenergy.system')).toBe(true)
    expect(isSkipped('ServiceMapping/com_victronenergy_vebus_276', 'com.victronenergy.system')).toBe(true)
  })

  test('near misses of ServiceMapping are not: another level name, the bare level, another service', () => {
    expect(isSkipped('/ServiceMappings/x', 'com.victronenergy.system')).toBe(false)
    expect(isSkipped('/ServiceMapping', 'com.victronenergy.system')).toBe(false)
    expect(isSkipped('/ServiceMapping/x', 'com.victronenergy.settings')).toBe(false)
  })

  test('vebus Interfaces/Mk2/Tunnel is skipped on any vebus service', () => {
    expect(isSkipped('/Interfaces/Mk2/Tunnel', 'com.victronenergy.vebus.ttyS4')).toBe(true)
    expect(isSkipped('Interfaces/Mk2/Tunnel', 'com.victronenergy.vebus.ttyUSB0')).toBe(true)
  })

  test('near misses of the tunnel are not: a sibling, a child, another service', () => {
    expect(isSkipped('/Interfaces/Mk2/Version', 'com.victronenergy.vebus.ttyS4')).toBe(false)
    expect(isSkipped('/Interfaces/Mk2/Tunnel/x', 'com.victronenergy.vebus.ttyS4')).toBe(false)
    expect(isSkipped('/Interfaces/Mk2/Tunnel', 'com.victronenergy.system')).toBe(false)
  })

  test('an ordinary path, and a name that is not Victron\'s, are not skipped', () => {
    expect(isSkipped('/Dc/0/Voltage', 'com.victronenergy.battery.ttyUSB0')).toBe(false)
    expect(isSkipped('/ServiceMapping/x', 'org.freedesktop.DBus')).toBe(false)
  })
})

describe('decoding by signature (§2)', () => {
  test('basic types come through as they are', () => {
    expect(decodeVariant(variant(D, 12.8))).toBe(12.8)
    expect(decodeVariant(variant(I, -3))).toBe(-3)
    expect(decodeVariant(variant(S, '12.80V'))).toBe('12.80V')
    expect(decodeVariant(variant(B, true))).toBe(true)
  })

  test('a dict becomes an object, written with its keys sorted', () => {
    const value = decodeVariant(variant(ASV, [
      ['state', variant(I, 2)],
      ['active', variant(B, true)],
      ['name', variant(S, 'Lynx')]
    ]))
    expect(value).toEqual({ active: true, name: 'Lynx', state: 2 })
    expect(payloadFor(value, '/Info')).toBe('{"value":{"active":true,"name":"Lynx","state":2}}')
  })

  test('a list of dicts (aa{sv}, system /Batteries) becomes a list of sorted objects', () => {
    const battery = [
      ['voltage', variant(D, 12.8)],
      ['soc', variant(D, 85)],
      ['id', variant(S, 'com.victronenergy.battery.ttyUSB0')],
      ['instance', variant(I, 279)],
      ['active_battery_service', variant(B, true)]
    ]
    const value = decodeVariant(variant(node('a', ASV), [battery]))
    expect(payloadFor(value, '/Batteries')).toBe(
      '{"value":[{"active_battery_service":true,"id":"com.victronenergy.battery.ttyUSB0",' +
      '"instance":279,"soc":85,"voltage":12.8}]}'
    )
  })

  test('a struct becomes an array, each field by its own type', () => {
    const sig = node('(', S, I, node('a', node('y')))
    expect(decodeVariant(variant(sig, ['x', 5, Buffer.from([1, 2])]))).toEqual(['x', 5, [1, 2]])
  })

  test('a byte array (a Buffer) becomes an array of numbers', () => {
    expect(decodeVariant(variant(node('a', node('y')), Buffer.from([0x00, 0x10, 0xff])))).toEqual([0, 16, 255])
    expect(payloadFor(decodeVariant(variant(node('a', node('y')), Buffer.from([0x00, 0x10, 0xff]))), '/Raw'))
      .toBe('{"value":[0,16,255]}')
  })

  test('an empty ai is a Venus invalid and becomes null', () => {
    expect(decodeVariant(variant(AI, []))).toBeNull()
  })

  test('a non-empty ai, and an empty array of another type, are lists', () => {
    expect(decodeVariant(variant(AI, [1, 2]))).toEqual([1, 2])
    expect(decodeVariant(variant(node('a', S), []))).toEqual([])
  })

  test('an empty ai in a typed position (an element of an aai) is a real empty list', () => {
    expect(decodeVariant(variant(node('a', AI), [[1], []]))).toEqual([[1], []])
  })

  test('a variant nested in a dict value is unwrapped, and an invalid inside one is null', () => {
    const value = decodeVariant(variant(ASV, [
      ['outer', variant(V, variant(D, 1.5))],
      ['gone', variant(AI, [])]
    ]))
    expect(payloadFor(value, '/X')).toBe('{"value":{"gone":null,"outer":1.5}}')
  })

  test('undefined and null become null', () => {
    expect(decodeVariant(undefined)).toBeNull()
    expect(decodeVariant(null)).toBeNull()
  })

  test('a 64-bit integer (a BigInt from dbus-native) is written exactly', () => {
    const value = decodeVariant(variant(node('x'), 9007199254740993n))
    expect(payloadFor(value, '/Counter')).toBe('{"value":9007199254740993}')
  })
})

describe('itemsToObject (GetItems / ItemsChanged, a{sa{sv}})', () => {
  test('keys each item by path with its fields decoded', () => {
    const body = [
      ['/Dc/0/Voltage', [['Value', variant(D, 12.8)], ['Text', variant(S, '12.80V')]]],
      ['/Soc', [['Value', variant(AI, [])], ['Text', variant(S, '---')]]]
    ]
    expect(itemsToObject(body)).toEqual({
      '/Dc/0/Voltage': { Value: 12.8, Text: '12.80V' },
      '/Soc': { Value: null, Text: '---' }
    })
  })

  test('an empty body is an empty object', () => {
    expect(itemsToObject([])).toEqual({})
  })
})

describe('payloads (§2)', () => {
  test('a number, with no path given', () => {
    expect(payloadFor(12.8)).toBe('{"value":12.8}')
  })

  test('a string', () => {
    expect(payloadFor('12.80V', '/Dc/0/Voltage')).toBe('{"value":"12.80V"}')
  })

  test('null and undefined', () => {
    expect(payloadFor(null, '/Soc')).toBe('{"value":null}')
    expect(payloadFor(undefined, '/Soc')).toBe('{"value":null}')
  })

  test('a nested object, sorted at every level', () => {
    expect(payloadFor({ b: [{ z: 1, a: 2 }], a: { y: 'q', x: null } }, '/N'))
      .toBe('{"value":{"a":{"x":null,"y":"q"},"b":[{"a":2,"z":1}]}}')
  })

  test('keys sort as strings, as the broker writes them: "10" before "9"', () => {
    expect(payloadFor({ 9: 'b', 10: 'a', x: 'c' }, '/N')).toBe('{"value":{"10":"a","9":"b","x":"c"}}')
  })

  test('a key that needs escaping is escaped', () => {
    expect(payloadFor({ 'a"b': 1 }, '/N')).toBe('{"value":{"a\\"b":1}}')
  })

  test('a path ending Password, Pincode or Passphrase is masked, whatever the value', () => {
    expect(payloadFor('hunter2', '/Settings/Services/AccessPointPassword')).toBe('{"value":"******"}')
    expect(payloadFor(123456, '/Settings/Ble/Service/Pincode')).toBe('{"value":"******"}')
    expect(payloadFor(null, '/Settings/Network/Wifi/Passphrase')).toBe('{"value":"******"}')
  })

  test('a path merely containing Password mid-way is not masked', () => {
    expect(payloadFor(1, '/Settings/Password/Enabled')).toBe('{"value":1}')
    expect(payloadFor(1, '/Settings/PasswordSet')).toBe('{"value":1}')
  })

  test('the match is case-sensitive, as the contract names the suffixes', () => {
    expect(payloadFor('x', '/Settings/password')).toBe('{"value":"x"}')
  })
})

describe('matches (§4, MQTT 3.1.1 §4.7)', () => {
  test('# matches every topic, of any depth', () => {
    expect(matches('#', 'a')).toBe(true)
    expect(matches('#', 'a/b')).toBe(true)
    expect(matches('#', 'N/102c6bf7d60f/battery/279/Soc')).toBe(true)
  })

  test('an empty topic is not a topic (§4.7.3), so even # does not match it', () => {
    expect(matches('#', '')).toBe(false)
  })

  test('a/# matches a itself and anything below, but not b/a', () => {
    expect(matches('a/#', 'a')).toBe(true)
    expect(matches('a/#', 'a/b/c')).toBe(true)
    expect(matches('a/#', 'b/a')).toBe(false)
  })

  test('a/+ matches exactly one level below a', () => {
    expect(matches('a/+', 'a/b')).toBe(true)
    expect(matches('a/+', 'a/b/c')).toBe(false)
    expect(matches('a/+', 'a')).toBe(false)
  })

  test('+ matches an empty level', () => {
    expect(matches('a/+', 'a/')).toBe(true)
    expect(matches('+/+', '/a')).toBe(true)
    expect(matches('+', '/a')).toBe(false)
  })

  test('+/+ matches two levels', () => {
    expect(matches('+/+', 'a/b')).toBe(true)
  })

  test('a mixed filter: N/+/system/0/Serial', () => {
    expect(matches('N/+/system/0/Serial', 'N/102c6bf7d60f/system/0/Serial')).toBe(true)
    expect(matches('N/+/system/0/Serial', 'N/102c6bf7d60f/system/1/Serial')).toBe(false)
  })

  test('levels compare literally', () => {
    expect(matches('N/102c6bf7d60f/battery/279/Soc', 'N/102c6bf7d60f/battery/279/Soc')).toBe(true)
    expect(matches('N/102c6bf7d60f/battery/279/Soc', 'N/102c6bf7d60f/battery/279/soc')).toBe(false)
    expect(matches('N/102c6bf7d60f/battery/279/Soc', 'N/102c6bf7d60f/battery/279/Soc/x')).toBe(false)
    expect(matches('N/102c6bf7d60f/battery/279', 'N/102c6bf7d60f/battery/279/Soc')).toBe(false)
  })

  test('a $ topic is not matched by a filter starting with a wildcard (§4.7.2)', () => {
    expect(matches('#', '$SYS/x')).toBe(false)
    expect(matches('+/x', '$SYS/x')).toBe(false)
    expect(matches('$SYS/#', '$SYS/x')).toBe(true)
    expect(matches('$SYS/+', '$SYS/x')).toBe(true)
  })

  test('an invalid filter matches nothing, and is not an error', () => {
    expect(matches('a/#/b', 'a/x/b')).toBe(false)
    expect(matches('a+', 'a+')).toBe(false)
    expect(matches('a#', 'a#')).toBe(false)
    expect(matches('a/b#', 'a/b')).toBe(false)
    expect(matches('', 'a')).toBe(false)
    expect(matches(7, '7')).toBe(false)
    expect(matches(null, 'a')).toBe(false)
  })
})

describe('createMatcher (§4: the union of the announcements, compiled once)', () => {
  test('no filters match nothing', () => {
    expect(createMatcher([]).matches('N/102c6bf7d60f/battery/279/Soc')).toBe(false)
  })

  test('one matching filter among others is enough, exact or wildcard', () => {
    expect(createMatcher(['x', '#']).matches('N/102c6bf7d60f/battery/279/Soc')).toBe(true)
    expect(createMatcher(['x', 'N/+/battery/279/Soc']).matches('N/102c6bf7d60f/battery/279/Soc')).toBe(true)
    expect(createMatcher(['x', 'N/102c6bf7d60f/battery/279/Soc']).matches('N/102c6bf7d60f/battery/279/Soc')).toBe(true)
  })

  test('none matching is false', () => {
    expect(createMatcher(['x', 'N/+/battery/280/Soc']).matches('N/102c6bf7d60f/battery/279/Soc')).toBe(false)
  })

  test('exact and wildcard filters in one set each match as they would alone', () => {
    const m = createMatcher([
      'N/102c6bf7d60f/battery/279/Soc', // exact
      'N/+/system/0/Serial', // one-level wildcard
      'N/102c6bf7d60f/solarcharger/#', // multi-level, which matches its parent too
      '$SYS/broker' // exact, on a $ topic
    ])
    // The exact filter: the identical topic only.
    expect(m.matches('N/102c6bf7d60f/battery/279/Soc')).toBe(true)
    expect(m.matches('N/102c6bf7d60f/battery/279/soc')).toBe(false)
    expect(m.matches('N/102c6bf7d60f/battery/279/Soc/x')).toBe(false)
    expect(m.matches('N/102c6bf7d60f/battery/279')).toBe(false)
    // The + filter: any portal, nothing else varies.
    expect(m.matches('N/102c6bf7d60f/system/0/Serial')).toBe(true)
    expect(m.matches('N/c0619ab1d2e3/system/0/Serial')).toBe(true)
    expect(m.matches('N/102c6bf7d60f/system/1/Serial')).toBe(false)
    // The # filter: the parent and everything below.
    expect(m.matches('N/102c6bf7d60f/solarcharger')).toBe(true)
    expect(m.matches('N/102c6bf7d60f/solarcharger/290/Yield/Power')).toBe(true)
    expect(m.matches('N/102c6bf7d60f/solarchargers/290')).toBe(false)
    // A $ topic matches an exact filter naming it, and no wildcard-led one.
    expect(m.matches('$SYS/broker')).toBe(true)
    expect(createMatcher(['#', '+/broker']).matches('$SYS/broker')).toBe(false)
    // An empty topic is not a topic (§4.7.3), even against #.
    expect(m.matches('')).toBe(false)
    expect(createMatcher(['#']).matches('')).toBe(false)
  })

  test('an invalid filter in the set is ignored, not thrown: the rest still match', () => {
    const filters = ['a/#/b', 'a+', 'a/b#', '', 7, null, 'N/+/battery/279/Soc', 'x/y']
    expect(() => createMatcher(filters)).not.toThrow()
    const m = createMatcher(filters)
    expect(m.matches('N/102c6bf7d60f/battery/279/Soc')).toBe(true)
    expect(m.matches('x/y')).toBe(true)
    expect(m.matches('a/x/b')).toBe(false)
    expect(m.matches('a+')).toBe(false)
    expect(m.matches('a/b')).toBe(false)
    expect(m.matches('7')).toBe(false)
  })
})

describe('createRenderer: bodies in, (topic, payload) pairs out, instances remembered', () => {
  const BATTERY = 'com.victronenergy.battery.ttyUSB0'
  const snapshot = [
    ['/DeviceInstance', [['Value', variant(I, 279)], ['Text', variant(S, '279')]]],
    ['/Dc/0/Voltage', [['Value', variant(D, 12.8)], ['Text', variant(S, '12.80V')]]],
    ['/Soc', [['Value', variant(D, 91)]]]
  ]

  test('a snapshot renders every item under the instance it carries', () => {
    const r = createRenderer({ portalId: PORTAL })
    expect(r.items(BATTERY, snapshot)).toEqual([
      [`N/${PORTAL}/battery/279/DeviceInstance`, '{"value":279}'],
      [`N/${PORTAL}/battery/279/Dc/0/Voltage`, '{"value":12.8}'],
      [`N/${PORTAL}/battery/279/Soc`, '{"value":91}']
    ])
  })

  test('a later change without /DeviceInstance keeps the instance the snapshot set', () => {
    const r = createRenderer({ portalId: PORTAL })
    r.items(BATTERY, snapshot)
    expect(r.items(BATTERY, [['/Soc', [['Value', variant(D, 90.5)]]]]))
      .toEqual([[`N/${PORTAL}/battery/279/Soc`, '{"value":90.5}']])
  })

  test('a service with no /DeviceInstance takes the default instance rule: system 0, vecan its suffix', () => {
    const r = createRenderer({ portalId: PORTAL })
    expect(r.items('com.victronenergy.system', [['/Serial', [['Value', variant(S, PORTAL)]]]]))
      .toEqual([[`N/${PORTAL}/system/0/Serial`, `{"value":"${PORTAL}"}`]])
    expect(r.items('com.victronenergy.vecan.can0', [['/Link/Soc', [['Value', variant(node('a', I), [])]]]]))
      .toEqual([[`N/${PORTAL}/vecan/can0/Link/Soc`, '{"value":null}']])
  })

  test('skipped paths and entries without a Value are left out; secrets are masked', () => {
    const r = createRenderer({ portalId: PORTAL })
    expect(r.items('com.victronenergy.system', [
      ['/ServiceMapping/com_victronenergy_battery_279', [['Value', variant(S, 'x')]]],
      ['/Dc/Battery/Soc', [['Text', variant(S, '91%')]]],
      ['/Dc/Battery/Voltage', [['Value', variant(D, 12.8)]]]
    ])).toEqual([[`N/${PORTAL}/system/0/Dc/Battery/Voltage`, '{"value":12.8}']])
    expect(r.items('com.victronenergy.settings', [['/Settings/Ble/Service/Pincode', [['Value', variant(S, '000000')]]]]))
      .toEqual([[`N/${PORTAL}/settings/0/Settings/Ble/Service/Pincode`, '{"value":"******"}']])
  })

  test('a non-Victron bus name renders nothing', () => {
    const r = createRenderer({ portalId: PORTAL })
    expect(r.items('org.freedesktop.DBus', [['/x', [['Value', variant(I, 1)]]]])).toEqual([])
  })

  test('gone() gives the service\'s topic prefix once and forgets its instance', () => {
    const r = createRenderer({ portalId: PORTAL })
    r.items(BATTERY, snapshot)
    expect(r.gone(BATTERY)).toBe(`N/${PORTAL}/battery/279/`)
    expect(r.gone(BATTERY)).toBeNull()
    // Seen again after leaving: a fresh snapshot sets the instance anew.
    expect(r.items(BATTERY, [['/DeviceInstance', [['Value', variant(I, 280)]]]]))
      .toEqual([[`N/${PORTAL}/battery/280/DeviceInstance`, '{"value":280}']])
  })

  test('gone() for a name never seen, or not Victron\'s, is null', () => {
    const r = createRenderer({ portalId: PORTAL })
    expect(r.gone(BATTERY)).toBeNull()
    r.items('org.freedesktop.DBus', [['/x', [['Value', variant(I, 1)]]]])
    expect(r.gone('org.freedesktop.DBus')).toBeNull()
  })
})

describe('createRenderer.writeFor: a W/ topic and payload back to the D-Bus set (§2 "W/ writes")', () => {
  const VEBUS = 'com.victronenergy.vebus.ttyO1'
  const SYSTEM = 'com.victronenergy.system'
  const SETTINGS = 'com.victronenergy.settings'
  const U = node('u')
  const X = node('x')
  const vebus = [
    ['/DeviceInstance', [['Value', variant(I, 276)]]],
    ['/Mode', [['Value', variant(I, 3)], ['Text', variant(S, 'On')]]],
    ['/Ac/ActiveIn/CurrentLimit', [['Value', variant(D, 16)]]]
  ]
  // The GX's relay outputs. A real State is an integer; the others here carry
  // every other signature, because a write is checked against the type the
  // item itself was read with, whatever it is.
  const system = [
    ['/SwitchableOutput/0/State', [['Value', variant(I, 0)]]],
    ['/SwitchableOutput/1/State', [['Value', variant(U, 0)]]],
    ['/SwitchableOutput/2/State', [['Value', variant(X, 0)]]],
    ['/SwitchableOutput/3/State', [['Value', variant(D, 0)]]],
    ['/SwitchableOutput/4/State', [['Value', variant(B, false)]]],
    ['/SwitchableOutput/5/State', [['Value', variant(S, 'off')]]],
    ['/SwitchableOutput/6/State', [['Value', variant(AI, [])]]],
    ['/SwitchableOutput/7/State', [['Value', variant(ASV, [])]]],
    ['/SwitchableOutput/8/State', [['Value', undefined]]],
    ['/SwitchableOutput/0/Name', [['Value', variant(S, 'Deck light')]]],
    ['/Relay/0/State', [['Value', variant(I, 0)]]]
  ]
  const settings = [
    ['/Settings/CGwacs/AcPowerSetPoint', [['Value', variant(D, 50)]]]
  ]
  const W = (path) => `W/${PORTAL}/vebus/276${path}`
  const OUT = (n) => `W/${PORTAL}/system/0/SwitchableOutput/${n}/State`
  const ready = () => {
    const r = createRenderer({ portalId: PORTAL })
    r.items(VEBUS, vebus)
    r.items(SYSTEM, system)
    r.items(SETTINGS, settings)
    return r
  }

  test('the switches are the GX\'s relay outputs and the MultiPlus/Quattro mode, and nothing else', () => {
    // Restated so that widening the list — a contract change — shows up here
    // as a decision.
    expect(WRITABLE.map(({ type, path }) => [type, path.source])).toEqual([
      ['system', '^SwitchableOutput\\/\\d+\\/State$'],
      ['vebus', '^Mode$']
    ])
  })

  test('a write to a switch is the service, the path and the value in the item\'s own signature', () => {
    const r = ready()
    expect(r.writeFor(W('/Mode'), '{"value":4}')).toEqual({
      busName: VEBUS, path: '/Mode', variant: [[I], [4]]
    })
    expect(r.writeFor(OUT(0), '{"value":1}')).toEqual({
      busName: SYSTEM, path: '/SwitchableOutput/0/State', variant: [[I], [1]]
    })
  })

  test('every scalar type, with the value as JSON gave it', () => {
    const r = ready()
    expect(r.writeFor(OUT(0), '{"value":-5}').variant).toEqual([[I], [-5]])
    expect(r.writeFor(OUT(1), '{"value":0}').variant).toEqual([[U], [0]])
    expect(r.writeFor(OUT(2), '{"value":-5}').variant).toEqual([[X], [-5]])
    expect(r.writeFor(OUT(3), '{"value":12.5}').variant).toEqual([[D], [12.5]])
    expect(r.writeFor(OUT(3), '{"value":12}').variant).toEqual([[D], [12]])
    expect(r.writeFor(OUT(4), '{"value":false}').variant).toEqual([[B], [false]])
    expect(r.writeFor(OUT(5), '{"value":"on"}').variant).toEqual([[S], ['on']])
  })

  test('anything that is not a switch is refused as such, published or not', () => {
    const r = ready()
    const notASwitch = { refused: 'not a switch' }
    // Published items the hub could name: a setting (the reason for the
    // list), the Multi's other items, a relay output's other paths, and the
    // legacy relay form the hub's catalogue does not write.
    expect(r.writeFor(`W/${PORTAL}/settings/0/Settings/CGwacs/AcPowerSetPoint`, '{"value":100}')).toEqual(notASwitch)
    expect(r.writeFor(W('/Ac/ActiveIn/CurrentLimit'), '{"value":10}')).toEqual(notASwitch)
    expect(r.writeFor(`W/${PORTAL}/system/0/SwitchableOutput/0/Name`, '{"value":"x"}')).toEqual(notASwitch)
    expect(r.writeFor(`W/${PORTAL}/system/0/Relay/0/State`, '{"value":1}')).toEqual(notASwitch)
    // Near misses on the two shapes: the path held exactly, the type with it.
    expect(r.writeFor(W('/Mode/Extra'), '{"value":4}')).toEqual(notASwitch)
    expect(r.writeFor(W('/mode'), '{"value":4}')).toEqual(notASwitch)
    expect(r.writeFor(`W/${PORTAL}/system/0/SwitchableOutput/x/State`, '{"value":1}')).toEqual(notASwitch)
    expect(r.writeFor(`W/${PORTAL}/system/0/SwitchableOutput//State`, '{"value":1}')).toEqual(notASwitch)
    expect(r.writeFor(`W/${PORTAL}/system/0/SwitchableOutput/0/State/x`, '{"value":1}')).toEqual(notASwitch)
    expect(r.writeFor(`W/${PORTAL}/vebus/276/SwitchableOutput/0/State`, '{"value":1}')).toEqual(notASwitch)
    expect(r.writeFor(`W/${PORTAL}/system/0/Mode`, '{"value":4}')).toEqual(notASwitch)
    expect(r.writeFor(`W/${PORTAL}/vebus`, '{"value":4}')).toEqual(notASwitch)
  })

  test('the switch check comes first, so a refusal never says whether the path exists', () => {
    const r = ready()
    expect(r.writeFor(`W/${PORTAL}/settings/0/Settings/CGwacs/AcPowerSetPoint`, '{"value":1}'))
      .toEqual(r.writeFor(`W/${PORTAL}/settings/0/Settings/CGwacs/NoSuchPath`, '{"value":1}'))
  })

  test('a switch never published is refused as unknown: another boat, another output, a service that has left', () => {
    const r = ready()
    expect(r.writeFor(`W/other/vebus/276/Mode`, '{"value":4}')).toEqual({ refused: 'unknown topic' })
    expect(r.writeFor(OUT(9), '{"value":1}')).toEqual({ refused: 'unknown topic' })
    expect(r.writeFor(`N/${PORTAL}/vebus/276/Mode`, '{"value":4}')).toEqual({ refused: 'unknown topic' })
    r.gone(VEBUS)
    expect(r.writeFor(W('/Mode'), '{"value":4}')).toEqual({ refused: 'unknown topic' })
  })

  test('a payload that is not {"value": <scalar>} is refused', () => {
    const r = ready()
    for (const text of ['', 'not json', '4', '[4]', 'null', '{}', '{"value":null}', '{"value":[1]}', '{"value":{"a":1}}', '{"on":true}']) {
      expect(r.writeFor(W('/Mode'), text)).toEqual({ refused: 'payload is not {"value": <scalar>}' })
    }
  })

  test('a value that does not fit the item\'s type is refused, saying what would', () => {
    const r = ready()
    expect(r.writeFor(W('/Mode'), '{"value":2.5}')).toEqual({ refused: 'value must be a whole number' })
    expect(r.writeFor(W('/Mode'), '{"value":"3"}')).toEqual({ refused: 'value must be a whole number' })
    expect(r.writeFor(OUT(1), '{"value":-1}')).toEqual({ refused: 'value must be a whole number, zero or more' })
    expect(r.writeFor(OUT(3), '{"value":"16"}')).toEqual({ refused: 'value must be a number' })
    expect(r.writeFor(OUT(4), '{"value":1}')).toEqual({ refused: 'value must be a boolean' })
    expect(r.writeFor(OUT(5), '{"value":1}')).toEqual({ refused: 'value must be a string' })
  })

  test('an item that is not a scalar, or has no typed value, is refused', () => {
    const r = ready()
    expect(r.writeFor(OUT(6), '{"value":50}')).toEqual({ refused: 'value must be a scalar (the item is "a")' })
    expect(r.writeFor(OUT(7), '{"value":1}')).toEqual({ refused: 'value must be a scalar (the item is "a")' })
    expect(r.writeFor(OUT(8), '{"value":1}')).toEqual({ refused: 'the item has no typed value' })
  })

  test('a later change updates the type a write is checked against', () => {
    const r = ready()
    r.items(VEBUS, [['/Mode', [['Value', variant(D, 3)]]]])
    expect(r.writeFor(W('/Mode'), '{"value":2.5}')).toEqual({ busName: VEBUS, path: '/Mode', variant: [[D], [2.5]] })
  })
})
