'use strict'

// D-Bus access on a Venus OS GX: the raw material for the Victron feed
// (protocol/mqtt-producer.md §2). It enumerates the com.victronenergy.*
// services, bulk-reads each with GetItems, follows ItemsChanged /
// PropertiesChanged and services coming and going, and reports every body
// as D-Bus delivered it to the caller's callbacks. It renders nothing and
// knows no MQTT. `dbus-native` is injected: index.js passes the real module,
// a test a fake.
//
// It follows, and sets only on request: one SetValue per write the hub is
// allowed to make (§2 "W/ writes", decided in index.js), and no per-path
// GetValue, because the feed answers a one-topic R/ from the current values
// these signals keep it holding.
//
// Measured from inside Signal K's process on a Cerbo GX (Venus OS 3.81): 20
// services, GetItems on all, ~600 ItemsChanged a minute, and Signal K's user
// permitted on the system bus.

const BUS_ITEM = 'com.victronenergy.BusItem'
const VICTRON = 'com.victronenergy.'
const DBUS = 'org.freedesktop.DBus'
const DBUS_PATH = '/org/freedesktop/DBus'

// §2: the portal id is com.victronenergy.system /Serial.
const SYSTEM = 'com.victronenergy.system'

// D-Bus message type 4 is a signal (D-Bus specification, "Message Types").
const SIGNAL = 4

// The renderer needs each value's D-Bus signature (§2: values are rendered by
// signature, and a Venus "invalid" is an empty `ai`, rendered `null`, which
// only the signature tells apart from an empty list of anything else).
// dbus-native 0.14+ by default unwraps variants and drops the signature,
// turns string-keyed dicts into objects, and reads 64-bit integers as BigInt,
// which JSON cannot carry. These are the library's classic shapes, the ones
// the rendering was proven byte-identical with on a real GX: a variant as
// `[signatureTree, [value]]`, a dict as an array of pairs, 64-bit as a number.
// systemBus() takes no options, so they are set on the connection before the
// first message is read.
const VALUE_SHAPES = { plainValues: false, variants: 'tree', returnBigInt: false }

// The three signals followed. ItemsChanged carries a{sa{sv}} for the whole
// service, PropertiesChanged an a{sv} for the one path it is sent on, and
// NameOwnerChanged services appearing and disappearing.
const MATCH_RULES = [
  `type='signal',interface='${BUS_ITEM}',member='ItemsChanged'`,
  `type='signal',interface='${BUS_ITEM}',member='PropertiesChanged'`,
  `type='signal',sender='${DBUS}',interface='${DBUS}',member='NameOwnerChanged'`
]

// A service is followed only once its snapshot has been read (see readService),
// so a read that fails — a service still starting when Signal K asked, or one
// that did not answer within dbus-native's 25 s call deadline — would otherwise
// leave that service silently absent until it or Signal K restarted. Failed
// reads are tried again, together, on this cadence until each succeeds or the
// service leaves the bus. Logged on change, not on attempt: the first failure
// at error, repeats at debug, since a service that will never answer (one
// without GetItems) would otherwise fill the owner's log.
const RETRY_MS = 60000

// The `code` of start()'s rejection when the bus answers but carries no
// com.victronenergy.* service at all: a Signal K server that is not a GX (a
// Raspberry Pi), where the Victron feed has nothing to read. Told apart from a
// failure so the caller can treat it as the calm state it is.
const NO_GX = 'NO_GX'

/**
 * @param {object} deps
 * @param {typeof import('dbus-native')} deps.dbus  the dbus-native module
 * @param {{ setTimeout: Function, clearTimeout: Function }} deps.timers
 * @param {{ debug: Function, error: Function }} deps.logger
 * @param {(busName: string, body: Array, source: 'GetItems' | 'ItemsChanged' | 'PropertiesChanged') => void} deps.onItems
 *   an a{sa{sv}} body, `[[path, [[key, variant], …]], …]`. 'GetItems' is the
 *   service's whole state; the other two are changes.
 * @param {(busName: string) => void} deps.onServiceGone
 * @param {(err: Error) => void} deps.onError  the bus connection failed or
 *   ended; called once, after which this instance is stopped
 */
function createVenus ({ dbus, timers, logger, onItems, onServiceGone, onError, onPortalId = () => {} }) {
  let bus = null
  let stopped = false
  // Signals carry the sender's unique name (':1.42'); this maps it back to
  // the well-known service name. A service is entered only once its GetItems
  // snapshot is delivered, so nothing is ever reported for a service before
  // its whole state (and with it /DeviceInstance, which the topic needs).
  const owners = new Map()
  // Services whose read failed and are owed another try (RETRY_MS above), and
  // the one timer that retries them all.
  const failed = new Set()
  let retryTimer = null

  // The callbacks run inside dbus-native's read loop, where a throw does not
  // stay local: with no 'handlerError' listener the library rethrows it on
  // the next tick and Signal K's process goes down. A failing callback is
  // the caller's bug, so it is logged and the bus carries on.
  function deliver (label, callback, ...args) {
    try {
      callback(...args)
    } catch (err) {
      logger.error(`D-Bus: ${label} for ${args[0]} threw: ${err.message}`)
    }
  }

  // One method call. `onReply` runs inside the reply's own dispatch, before
  // dbus-native emits whatever message came in after it; what it returns
  // resolves the call. A reply landing after stop() is refused, so a stopped
  // instance reports nothing.
  function call (msg, onReply = (reply) => reply) {
    return new Promise((resolve, reject) => {
      if (stopped) return reject(new Error('D-Bus access stopped'))
      bus.invoke(msg, (err, reply) => {
        if (stopped) return reject(new Error('D-Bus access stopped'))
        if (err) return reject(err)
        try {
          resolve(onReply(reply))
        } catch (replyError) {
          reject(replyError)
        }
      })
    })
  }

  function daemon (member, body) {
    return {
      destination: DBUS,
      path: DBUS_PATH,
      interface: DBUS,
      member,
      ...(body === undefined ? {} : { signature: 's', body })
    }
  }

  // GetNameOwner, then GetItems on '/'. The snapshot is delivered and the
  // owner entered synchronously in the reply's dispatch: dbus-native emits
  // every message buffered in one read before an `await` would resume, so a
  // change sent straight after the reply would otherwise be reported ahead
  // of the older snapshot, or dropped. A signal that arrives before the reply
  // is dropped safely: D-Bus keeps one sender's messages in order, so the
  // snapshot already holds it. One service failing is logged, skipped and
  // retried (RETRY_MS); the rest are unaffected. Never rejects: answers
  // whether it was read.
  async function readService (name) {
    try {
      const owner = await call(daemon('GetNameOwner', [name]))
      await call({ destination: name, path: '/', interface: BUS_ITEM, member: 'GetItems' }, (body) => {
        if (!Array.isArray(body)) throw new Error('GetItems reply is not an a{sa{sv}}')
        owners.set(owner, name)
        logger.debug(`D-Bus: ${name} read, ${body.length} paths`)
        deliver('onItems', onItems, name, body, 'GetItems')
      })
      if (failed.delete(name)) logger.debug(`D-Bus: ${name} read on retry`)
      return true
    } catch (err) {
      if (stopped) return false
      if (failed.has(name)) {
        logger.debug(`D-Bus: ${name} still not read: ${err.message}`)
      } else {
        logger.error(`D-Bus: ${name} not read: ${err.message}; retrying every ${RETRY_MS / 1000} s`)
        failed.add(name)
      }
      scheduleRetry()
      return false
    }
  }

  // Called only from a failure that was not after stop(), so `failed` is never
  // empty here and the instance is live.
  function scheduleRetry () {
    if (retryTimer !== null) return
    retryTimer = timers.setTimeout(retryFailed, RETRY_MS)
  }

  // One at a time, like start(). A read that fails again schedules the next
  // round itself, so the rounds stay RETRY_MS apart.
  async function retryFailed () {
    retryTimer = null
    for (const name of [...failed]) {
      if (stopped) return
      await readService(name)
    }
  }

  function onMessage (msg) {
    if (stopped || msg.type !== SIGNAL) return
    try {
      dispatch(msg)
    } catch (err) {
      // A malformed body is one signal lost, not a reason to stop.
      logger.error(`D-Bus: ${msg.member} from ${msg.sender} dropped: ${err.message}`)
    }
  }

  function dispatch ({ sender, interface: iface, member, path, body }) {
    // Only the bus daemon may send as org.freedesktop.DBus, so a
    // NameOwnerChanged from anyone else is not believed.
    if (iface === DBUS && member === 'NameOwnerChanged' && sender === DBUS) {
      ownerChanged(body)
      return
    }
    if (iface !== BUS_ITEM) return
    const name = owners.get(sender)
    // Not (yet) a service with a delivered snapshot; see readService.
    if (name === undefined) return
    if (member === 'ItemsChanged') {
      deliver('onItems', onItems, name, firstArray(body), 'ItemsChanged')
    } else if (member === 'PropertiesChanged') {
      // An a{sv} for the signal's own path: the same shape as one GetItems
      // entry, so the caller has one shape to handle.
      deliver('onItems', onItems, name, [[path, firstArray(body)]], 'PropertiesChanged')
    }
  }

  function ownerChanged (body) {
    const [name, oldOwner, newOwner] = Array.isArray(body) ? body : []
    if (![name, oldOwner, newOwner].every((field) => typeof field === 'string')) {
      throw new Error('body is not (sss)')
    }
    if (!name.startsWith(VICTRON)) return
    // The old unique name is dead whether the service left or was replaced;
    // unique names are never reused, so nothing more is believed from it.
    if (oldOwner !== '') owners.delete(oldOwner)
    if (newOwner === '') {
      // Gone, so no longer owed a retry; if it comes back it is read afresh.
      failed.delete(name)
      logger.debug(`D-Bus: ${name} gone`)
      deliver('onServiceGone', onServiceGone, name)
    } else {
      logger.debug(`D-Bus: ${name} appeared`)
      readService(name)
    }
  }

  function firstArray (body) {
    if (!Array.isArray(body) || !Array.isArray(body[0])) throw new Error('body is not an array')
    return body[0]
  }

  // One run per instance. Ends the connection and makes every later
  // event inert; idempotent, and safe before start(). The connection's
  // listeners stay attached, so a late 'error' still has one (an unhandled
  // 'error' would take the process down).
  function stop () {
    if (stopped) return
    stopped = true
    owners.clear()
    failed.clear()
    if (retryTimer !== null) {
      timers.clearTimeout(retryTimer)
      retryTimer = null
    }
    if (bus !== null) bus.connection.end()
  }

  function fail (err) {
    if (stopped) return
    stop()
    onError(err)
  }

  /**
   * Connects, reads the portal id, subscribes, then reads every Victron
   * service. Rejects, with the connection ended, if the bus or the portal id
   * cannot be had — with `code` NO_GX when the bus carries no Victron service
   * at all — and one service failing is logged and does not. onItems fires
   * for each service as it is read, before this resolves.
   *
   * @returns {Promise<{ portalId: string, services: number }>}  services read
   */
  async function start () {
    if (bus !== null || stopped) throw new Error('D-Bus access runs once per instance')
    bus = dbus.systemBus()
    bus.connection.setValueShapes(VALUE_SHAPES)
    bus.connection.on('message', onMessage)
    bus.connection.on('error', fail)
    bus.connection.on('end', () => fail(new Error('D-Bus connection ended')))
    bus.connection.on('close', () => fail(new Error('D-Bus connection closed')))

    try {
      const portalId = await portalIdOrWhy()
      // Told before any item: every topic needs the portal id (§2), so the
      // caller builds its renderer and feed here, and the first onItems below
      // already has somewhere to go.
      onPortalId(portalId)

      // Subscribed before the services are listed, so one appearing while
      // they are read is not missed; a service read twice is harmless.
      await Promise.all(MATCH_RULES.map((rule) => call(daemon('AddMatch', [rule]))))

      const names = (await call(daemon('ListNames'))).filter((name) => name.startsWith(VICTRON))
      logger.debug(`D-Bus: ${names.length} Victron services`)
      let services = 0
      // One at a time: twenty GetItems at once would land
      // on the GX's single bus daemon together for no gain at start.
      for (const name of names) {
        if (await readService(name)) services++
      }
      if (stopped) throw new Error('D-Bus access stopped')
      return { portalId, services }
    } catch (err) {
      stop()
      throw err
    }
  }

  // §2: the portal id, or why there is none. On a GX the system service
  // answers and nothing more is asked. When it does not, the bus is asked
  // whether it carries any Victron service at all: none means this host is
  // not a GX (NO_GX), anything else — Victron services present, or the bus
  // unable to say — is the original failure. Asked only on failure, so a GX
  // starts exactly as it did.
  async function portalIdOrWhy () {
    try {
      return await call({ destination: SYSTEM, path: '/Serial', interface: BUS_ITEM, member: 'GetValue' }, serial)
    } catch (err) {
      if (await withoutVictron()) {
        const none = new Error('no GX on this bus')
        none.code = NO_GX
        throw none
      }
      throw err
    }
  }

  // True only when ListNames answers with a list holding no Victron service.
  async function withoutVictron () {
    try {
      const names = await call(daemon('ListNames'))
      return Array.isArray(names) && !names.some((name) => name.startsWith(VICTRON))
    } catch {
      return false
    }
  }

  /**
   * §2 "W/ writes": set one item's value. Victron's BusItem SetValue takes a
   * variant and answers an integer, 0 for done; anything else is the service
   * refusing (a read-only path, a value it will not take), reported as a
   * rejection with the code so the caller can log it. The new value is not
   * reported back here: the service announces it on ItemsChanged like any
   * other change, and the feed publishes it from there.
   *
   * @param {string} busName  e.g. 'com.victronenergy.vebus.ttyO1'
   * @param {string} path  e.g. '/Mode'
   * @param {Array} variant  dbus-native's reader shape, [[node], [value]]
   * @returns {Promise<void>}
   */
  function set (busName, path, variant) {
    return call({ destination: busName, path, interface: BUS_ITEM, member: 'SetValue', signature: 'v', body: [variant] }, (code) => {
      if (code !== 0) throw new Error(`SetValue returned ${code}`)
    })
  }

  return { start, stop, set }
}

// /Serial's variant holds the portal id as a string. Without it no topic can
// be built (§2), so anything else fails start().
function serial (variant) {
  const value = Array.isArray(variant) && Array.isArray(variant[1]) ? variant[1][0] : undefined
  if (typeof value !== 'string' || value === '') {
    throw new Error(`${SYSTEM} /Serial is not a portal id`)
  }
  return value
}

module.exports = { createVenus, NO_GX }
