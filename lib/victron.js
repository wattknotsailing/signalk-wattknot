'use strict'

// Victron's own topic model, rendered from D-Bus: what the GX's broker plugin
// (dbus-flashmq) publishes, reproduced so the hub cannot tell a producer from
// the GX's broker (protocol/mqtt-producer.md §2). The same rules run backwards
// for a write: a `W/` topic and payload from the hub become the D-Bus service,
// path and typed value to set (§2, "W/ writes"). And the MQTT wildcard
// matcher that decides what the hub has asked for (§4.1). Pure — no I/O, no
// dependencies: lib/venus.js hands in what D-Bus said, lib/feed.js publishes
// what comes out.
//
//   com.victronenergy.battery.ttyUSB0  +  /DeviceInstance 279  +  /Dc/0/Voltage
//     → topic   N/<portal id>/battery/279/Dc/0/Voltage
//     → payload {"value":12.8}

// §2: every Victron service is com.victronenergy.<type>[.<suffix>].
const PREFIX = 'com.victronenergy.'

// §2: these paths render masked, whatever their value. "Ending" is read as a
// plain string suffix, so `/Settings/WifiPassword` is masked and
// `/Settings/Password/Enabled` is not.
const SECRET_PATH = /(?:Password|Pincode|Passphrase)$/
const MASK = '******'

// 'com.victronenergy.vecan.can0' → { type: 'vecan', suffix: 'can0' }; null for
// a name that is not Victron's, including a unique name (':1.42'). The suffix
// is everything after the type, dots included.
function parseName (busName) {
  if (!busName.startsWith(PREFIX)) return null
  const rest = busName.slice(PREFIX.length)
  const dot = rest.indexOf('.')
  const type = dot === -1 ? rest : rest.slice(0, dot)
  if (type === '') return null
  return { type, suffix: dot === -1 ? '' : rest.slice(dot + 1) }
}

/**
 * §2: the service type is the third dotted segment of the bus name.
 *
 * @param {string} busName  e.g. 'com.victronenergy.battery.ttyUSB0'
 * @returns {string | null}  'battery', or null when the name is not Victron's
 */
function serviceType (busName) {
  const parts = parseName(busName)
  return parts === null ? null : parts.type
}

/**
 * §2: the device instance in the topic. `/DeviceInstance` if the service has
 * one; otherwise `vecan` uses its name suffix and every other service uses 0.
 * A `/DeviceInstance` that is not an integer (an invalid, rendered null) counts
 * as absent: a topic level of `null` would name no device.
 *
 * @param {string} busName
 * @param {*} deviceInstance  the decoded `/DeviceInstance` value, or undefined
 *   when the service has none
 * @returns {number | string}
 */
function instanceFor (busName, deviceInstance) {
  if (Number.isInteger(deviceInstance)) return deviceInstance
  const parts = parseName(busName)
  if (parts !== null && parts.type === 'vecan' && parts.suffix !== '') return parts.suffix
  return 0
}

// A D-Bus object path as topic levels: the leading slash is the root, not an
// empty first level.
function stripSlash (path) {
  return path.startsWith('/') ? path.slice(1) : path
}

/**
 * §2: `N/<portal id>/<service type>/<device instance>/<D-Bus path>`.
 *
 * @param {{ portalId: string, busName: string, instance: number | string, path: string }} item
 * @returns {string | null}  null when the bus name is not Victron's
 */
function topicFor ({ portalId, busName, instance, path }) {
  const type = serviceType(busName)
  if (type === null) return null
  return `N/${portalId}/${type}/${instance}/${stripSlash(path)}`
}

/**
 * §2: the broker never publishes `system/0/ServiceMapping/…` or
 * `vebus/<any instance>/Interfaces/Mk2/Tunnel`, so neither does a producer.
 * The first is a subtree; the second is one item, exactly as the contract
 * names it.
 *
 * @param {string} path  the D-Bus path, with or without its leading slash
 * @param {string} busName
 * @returns {boolean}
 */
function isSkipped (path, busName) {
  const type = serviceType(busName)
  const levels = stripSlash(path)
  if (type === 'system') return levels.startsWith('ServiceMapping/')
  if (type === 'vebus') return levels === 'Interfaces/Mk2/Tunnel'
  return false
}

/** §2: the GX's `Serial` topic, the one topic published retained. */
function serialTopic (portalId) {
  return `N/${portalId}/system/0/Serial`
}

/** §2: the topic that ends a full publish, which the hub's discovery waits for. */
function fullPublishCompletedTopic (portalId) {
  return `N/${portalId}/full_publish_completed`
}

// ---------------------------------------------------------------------------
// Values (§2 "Values are rendered by D-Bus signature").
//
// These walk dbus-native's classic shapes, which a connection opened with
// `plainValues: false` hands back: the default plain shapes drop the
// signature, and with it the one fact that tells an invalid from an empty
// list. In those shapes a variant is `[[signatureNode], [value]]`, a node is
// `{ type, child: [] }`, a dict is an array of `[key, value]` pairs, a struct
// an array, and `ay` a Buffer.

/**
 * One variant to plain JSON-able data. A Venus "invalid" is a variant holding
 * an empty `ai` — what Victron's Python D-Bus library sends for None — so it
 * is recognised here, at the variant, and renders as null (§2). An empty `ai`
 * in a typed position, such as an element of an `aai`, is a real empty list.
 *
 * @param {*} variant  as dbus-native delivers it; undefined or null when there
 *   is no value at all, which renders as null
 * @returns {*}
 */
function decodeVariant (variant) {
  if (variant === undefined || variant === null) return null
  const [[node], [value]] = variant
  if (node.type === 'a' && node.child[0].type === 'i' && value.length === 0) return null
  return decode(node, value)
}

function decode (node, value) {
  switch (node.type) {
    case 'v':
      return decodeVariant(value)
    case 'a':
      return decodeArray(node.child[0], value)
    case '(':
      return value.map((field, i) => decode(node.child[i], field))
    default:
      return value
  }
}

function decodeArray (element, value) {
  if (element.type === '{') {
    // Key order is left to the serializer (payloadFor), the only place it
    // can be kept: a JavaScript object puts integer-like keys first whatever
    // order they were inserted in.
    const valueNode = element.child[1]
    return Object.fromEntries(value.map(([key, entry]) => [key, decode(valueNode, entry)]))
  }
  // §2: byte arrays as number arrays. Array.from reads a Buffer and a plain
  // number array alike.
  if (element.type === 'y') return Array.from(value)
  return value.map((entry) => decode(element, entry))
}

/**
 * A `GetItems` reply or an `ItemsChanged` signal body (`a{sa{sv}}`) to one
 * object keyed by path, every variant decoded.
 *
 *   [['/Dc/0/Voltage', [['Value', v], ['Text', v]]], …]
 *     → { '/Dc/0/Voltage': { Value: 12.8, Text: '12.80V' }, … }
 *
 * @param {Array<[string, Array<[string, *]>]>} body
 * @returns {Object<string, Object<string, *>>}
 */
function itemsToObject (body) {
  return Object.fromEntries(body.map(([path, fields]) => [
    path,
    Object.fromEntries(fields.map(([key, variant]) => [key, decodeVariant(variant)]))
  ]))
}

/**
 * §2: the payload `{"value": <json>}`, byte for byte as the broker writes it:
 * object keys sorted, a missing value as null, a secret path masked.
 *
 * @param {*} value  a decoded value (decodeVariant / itemsToObject)
 * @param {string} [path]  the D-Bus path, for masking
 * @returns {string}
 */
function payloadFor (value, path = '') {
  return `{"value":${SECRET_PATH.test(path) ? `"${MASK}"` : json(value)}}`
}

// JSON.stringify, except that object keys are written sorted (§2: the broker
// sorts them, in plain string order, so "10" comes before "9" — which a
// JavaScript object cannot hold, hence sorting here and not when decoding)
// and a 64-bit integer, which dbus-native delivers as a BigInt that
// JSON.stringify refuses, is written exactly.
function json (value) {
  if (Array.isArray(value)) return `[${value.map(json).join(',')}]`
  if (typeof value === 'bigint') return value.toString()
  if (value !== null && typeof value === 'object') {
    const keys = Object.keys(value).sort()
    return `{${keys.map((key) => `${JSON.stringify(key)}:${json(value[key])}`).join(',')}}`
  }
  return JSON.stringify(value ?? null)
}

// ---------------------------------------------------------------------------
// Writes (§2 "W/ writes"): the hub's topic and payload back to D-Bus.
//
// A write names a topic the renderer has published, so the service, path and
// value type are already known here — the type from the variant the value
// arrived in, which is what the item itself is declared as. A value is sent
// back in dbus-native's own reader shape, [[node], [value]], which its writer
// accepts as is, so the signature the GX gave is the signature the GX gets.

// The only items a write may set, by service type and topic path (the D-Bus
// path without its leading slash): the GX's relay outputs, and the
// MultiPlus/Quattro mode. This mirrors what the hub's switch catalogue
// writes, and nothing more: every item the plugin publishes is a topic the
// hub could otherwise name, settings among them (charge voltages, ESS), and
// the owner's "Allow switches" is a yes to switching, not to those. Widening
// this list is a contract change (§2 "W/ writes"), made there first.
const WRITABLE = [
  { type: 'system', path: /^SwitchableOutput\/\d+\/State$/ },
  { type: 'vebus', path: /^Mode$/ }
]

// Whether a `W/<portal id>/<type>/<instance>/<path>` topic names an item on
// WRITABLE. Read from the topic alone, before anything published is looked
// at, so a refused path never reveals whether it exists.
function isSwitch (topic) {
  const levels = topic.split('/')
  const type = levels[2]
  const path = levels.slice(4).join('/')
  return WRITABLE.some((item) => item.type === type && item.path.test(path))
}

// The D-Bus types a write may set: the scalars. A dictionary, a list or a
// struct is not a switch, and composing one from JSON would guess at a shape
// the hub never meant to send.
const SIGNED = new Set(['n', 'i', 'x'])
const UNSIGNED = new Set(['y', 'q', 'u', 't'])

// Why `value` cannot be written as `type`, or null when it can. Integers must
// be whole and unsigned ones non-negative; everything else is a plain type
// match. dbus-native would otherwise truncate, wrap or throw inside its own
// marshaller, with a message about neither the topic nor the value.
function misfit (type, value) {
  if (type === 'b') return typeof value === 'boolean' ? null : 'a boolean'
  if (type === 's') return typeof value === 'string' ? null : 'a string'
  if (type === 'd') return typeof value === 'number' ? null : 'a number'
  if (SIGNED.has(type)) return Number.isInteger(value) ? null : 'a whole number'
  if (UNSIGNED.has(type)) return Number.isInteger(value) && value >= 0 ? null : 'a whole number, zero or more'
  return `a scalar (the item is "${type}")`
}

// The `Value` variant's signature node for each path in an a{sa{sv}} body;
// paths without a Value are left out.
function valueNodes (body) {
  const nodes = new Map()
  for (const [path, fields] of body) {
    const value = fields.find(([key]) => key === 'Value')
    if (value !== undefined && Array.isArray(value[1])) nodes.set(path, value[1][0][0])
  }
  return nodes
}

// The payload's value, or undefined when the payload is not `{"value": …}`
// holding one non-null scalar.
function writtenValue (text) {
  let parsed
  try {
    parsed = JSON.parse(text)
  } catch {
    return undefined
  }
  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) return undefined
  const value = parsed.value
  return ['number', 'string', 'boolean'].includes(typeof value) ? value : undefined
}

// ---------------------------------------------------------------------------
// Topic filters (§4.1: "MQTT's own wildcard rules"), as MQTT 3.1.1 §4.7 defines
// them.

// §4.7.1: `+` and `#` must each fill a whole level, and `#` must be the last
// one. A filter that breaks this is not a filter, so it matches nothing; the
// hub would never announce one, and an error here would stop a publish over
// someone else's typo.
function validFilter (levels) {
  return levels.every((level, i) => {
    if (level === '#') return i === levels.length - 1
    return level === '+' || (!level.includes('+') && !level.includes('#'))
  })
}

// One valid wildcard filter, already split, against a topic's levels.
function matchLevels (want, topic, have) {
  // §4.7.2: a filter starting with a wildcard does not match a `$` topic.
  if (topic.startsWith('$') && (want[0] === '+' || want[0] === '#')) return false
  for (let i = 0; i < want.length; i++) {
    // `#` matches the parent level too, so `a/#` matches `a` (§4.7.1.2).
    if (want[i] === '#') return true
    if (i === have.length) return false
    if (want[i] !== '+' && want[i] !== have[i]) return false
  }
  return want.length === have.length
}

/**
 * §4: a topic is published when any announced filter matches it, by MQTT
 * 3.1.1 §4.7. Compiled once per filter set, because the feed asks for every
 * stored topic each time the hub announces (twice over, to find what an
 * announcement newly admits), and the hub announces once per subscribe with
 * no coalescing — on a hub restart, a burst of them. A filter with no
 * wildcard matches only the identical topic, so those go in a Set and cost
 * one lookup; the wildcard filters are split and validated here, once, not
 * per topic. Invalid filters, non-strings and empty strings (§4.7.3: both
 * must be at least one character) are left out, so they match nothing and
 * break nothing.
 *
 * @param {string[]} filters  the union of the hub's announcements
 * @returns {{ matches: (topic: string) => boolean }}
 */
function createMatcher (filters) {
  const exact = new Set()
  const wildcards = []
  for (const filter of filters) {
    if (typeof filter !== 'string' || filter === '') continue
    const want = filter.split('/')
    if (!validFilter(want)) continue
    if (want.includes('+') || want.includes('#')) wildcards.push(want)
    else exact.add(filter)
  }

  function matches (topic) {
    if (topic === '') return false
    if (exact.has(topic)) return true
    if (wildcards.length === 0) return false
    const have = topic.split('/')
    return wildcards.some((want) => matchLevels(want, topic, have))
  }

  return { matches }
}

// ---------------------------------------------------------------------------
// The renderer: D-Bus bodies in, (topic, payload) pairs out, remembering each
// service's instance. Stateful but still free of I/O — lib/venus.js hands in
// bodies, lib/feed.js takes the pairs.

/**
 * @param {{ portalId: string }} options
 * @returns {{ items: Function, gone: Function, writeFor: Function }}
 */
function createRenderer ({ portalId }) {
  // busName → instance. A service's GetItems snapshot carries /DeviceInstance
  // when it has one, and lib/venus.js delivers that snapshot before any
  // change, so the instance is known before it is needed. A partial
  // ItemsChanged without /DeviceInstance keeps the instance already known.
  const instances = new Map()

  // N/ topic → where a write to it goes: the service, the D-Bus path and the
  // signature node its value arrived with (§2 "W/ writes"). Every published
  // topic has an entry, so a write to a topic never published is refused as
  // unknown — the one way to know a path exists is to have read it.
  const targets = new Map()

  const writePrefix = `W/${portalId}/`

  /**
   * One a{sa{sv}} body (a snapshot or a change) to the pairs it publishes.
   * Skipped paths and entries without a Value are left out.
   *
   * @param {string} busName
   * @param {Array} body
   * @returns {Array<[string, string]>}  [topic, payload] pairs
   */
  function items (busName, body) {
    const decoded = itemsToObject(body)
    const nodes = valueNodes(body)
    if (Object.prototype.hasOwnProperty.call(decoded, '/DeviceInstance')) {
      instances.set(busName, instanceFor(busName, decoded['/DeviceInstance'].Value))
    } else if (!instances.has(busName)) {
      instances.set(busName, instanceFor(busName, undefined))
    }
    const instance = instances.get(busName)
    const out = []
    for (const [path, item] of Object.entries(decoded)) {
      if (!Object.prototype.hasOwnProperty.call(item, 'Value') || isSkipped(path, busName)) continue
      const topic = topicFor({ portalId, busName, instance, path })
      if (topic === null) continue
      out.push([topic, payloadFor(item.Value, path)])
      targets.set(topic, { busName, path, node: nodes.get(path) })
    }
    return out
  }

  /**
   * A service has left the bus: the topic prefix every one of its topics
   * shares, for the feed to forget them; null if it was never seen.
   *
   * @param {string} busName
   * @returns {string | null}
   */
  function gone (busName) {
    if (!instances.has(busName)) return null
    const instance = instances.get(busName)
    instances.delete(busName)
    const topic = topicFor({ portalId, busName, instance, path: '/' })
    if (topic === null) return null
    for (const known of targets.keys()) {
      if (known.startsWith(topic)) targets.delete(known)
    }
    return topic
  }

  /**
   * §2 "W/ writes": a write from the hub to the D-Bus set it asks for, or
   * the reason it is refused. The topic must name a switch (WRITABLE) and be
   * one this renderer has published, the payload `{"value": <scalar>}`, and
   * the value must fit the item's own type.
   *
   * @param {string} topic  `W/<portal id>/<type>/<instance>/<path>`
   * @param {string} text  the payload
   * @returns {{ busName: string, path: string, variant: Array } | { refused: string }}
   */
  function writeFor (topic, text) {
    if (!isSwitch(topic)) return { refused: 'not a switch' }
    const target = topic.startsWith(writePrefix) ? targets.get(`N/${portalId}/${topic.slice(writePrefix.length)}`) : undefined
    if (target === undefined) return { refused: 'unknown topic' }
    // Published with no variant at all (a Value of nothing renders null): no
    // type to set with.
    if (target.node === undefined) return { refused: 'the item has no typed value' }
    const value = writtenValue(text)
    if (value === undefined) return { refused: 'payload is not {"value": <scalar>}' }
    const why = misfit(target.node.type, value)
    if (why !== null) return { refused: `value must be ${why}` }
    return { busName: target.busName, path: target.path, variant: [[target.node], [value]] }
  }

  return { items, gone, writeFor }
}

module.exports = {
  createRenderer,
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
}
