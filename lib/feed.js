'use strict'

// The MQTT client half of the Victron feed, per Wattknot's MQTT producer
// contract (protocol/mqtt-producer.md). It is handed topics and payloads
// already rendered (lib/victron.js) and owns only the MQTT behaviour the
// contract asks of a producer: hold the current value per topic, publish a
// change when the hub subscribed to its topic (§4.1), answer `R/` as the GX's
// broker does (§2), retain the Serial and the writable flag and nothing else
// (§2, §4.2), hand a `W/` to the caller when writes are allowed and refuse it
// when they are not (§2), and send current state whole once after each
// connect of its own (§3). It knows nothing about D-Bus and builds no
// address: the caller's `connect` returns a client already pointed at the
// door (§1) with its credential, and the caller's `onWrite` does the set.

// §4.1: the hub announces each source's subscriptions here, one retained
// message per source id.
const ANNOUNCEMENTS = 'wattknot/hub/subscriptions/'

// §4.2: the producer tells the hub whether it will act on writes, retained,
// on every connect. Absent means writable to the hub, so the flag is
// published whichever way the owner's switch is set.
const WRITABLE = (portalId) => `wattknot/producer/${portalId}/writable`

// §2: a liveness poke carries this option and wants no reply.
const SUPPRESS_REPUBLISH = 'suppress-republish'

// The portal id is a topic level of every topic and of the R/ and W/
// subscriptions below, so a separator or a wildcard in it would subscribe to
// other boats' requests or publish under a topic the hub never maps.
const PORTAL_ID = /^[^/+#]+$/

/**
 * @param {object} deps
 * @param {() => import('mqtt').MqttClient} deps.connect  returns a client
 *   already configured for the door (§1); called once per start()
 * @param {string} deps.portalId  the GX's VRM portal id, the topic prefix (§2)
 * @param {{ warn: (text: string) => void, debug: (text: string) => void }} deps.logger
 * @param {{ now: () => number }} deps.clock  wall-clock milliseconds since the
 *   Unix epoch, for full_publish_completed's unix seconds (§2) — not a
 *   monotonic clock
 * @param {(filters: string[]) => { matches: (topic: string) => boolean }} deps.createMatcher
 *   MQTT's wildcard rules (§4.1), compiled for one filter set
 * @param {(text: string) => void} [deps.onStatus]  connecting, connected,
 *   offline, reconnecting, or `error: <message>`
 * @param {((topic: string, text: string) => void) | null} [deps.onWrite]  a
 *   `W/` write from the hub, topic and payload, when the owner allows writes
 *   (§2); null, the default, refuses every write and says so in the writable
 *   flag (§4.2). One value decides both, so they cannot disagree.
 */
function createFeed ({ connect, portalId, logger, clock, createMatcher, onStatus = () => {}, onWrite = null }) {
  if (typeof portalId !== 'string' || !PORTAL_ID.test(portalId)) {
    throw new Error(`portalId must be one topic level with no wildcards (got "${portalId}")`)
  }

  const serialTopic = `N/${portalId}/system/0/Serial`
  const writableTopic = WRITABLE(portalId)
  const writable = onWrite !== null
  const completedTopic = `N/${portalId}/full_publish_completed`
  const readPrefix = `R/${portalId}`
  const keepaliveTopic = `${readPrefix}/keepalive`

  // The current rendered payload per topic. The Serial is seeded from the
  // portal id it is defined by (§2: the portal id IS system/0/Serial), so a
  // full publish carries it even before the renderer reports it; the
  // renderer's own identical value is then no change.
  const values = new Map([[serialTopic, JSON.stringify({ value: portalId })]])

  // §4: each source's announced filters, by source id; the effective set is
  // their union, recomputed whenever one changes, and compiled into the
  // matcher every publish asks (see union()).
  const sources = new Map()
  let filters = []
  let matcher = createMatcher(filters)
  let everything = false

  // Writes refused while writes are not allowed, for the log (see the W/
  // handler below).
  let refusals = 0

  // §3: after the producer (re)connects nothing on the hub side asks for a
  // full publish, so one is owed per connect and paid on the first
  // announcement, the first moment the feed knows what the hub wants.
  let fullPublishOwed = false

  const counters = { published: 0, dropped: 0 }

  // The one live client, or null. Every handler checks that its own client is
  // still this one, so a stopped client's late events are inert.
  let client = null

  function isConnected () {
    return client !== null && client.connected
  }

  // §4: publish a topic only if it matches the union; `#` means everything.
  function allowed (topic) {
    return everything || matcher.matches(topic)
  }

  // The effective set changed: compiled here, once, rather than per topic
  // asked (lib/victron.js createMatcher says why that matters).
  function union (next) {
    filters = next
    matcher = createMatcher(filters)
    everything = filters.includes('#')
  }

  // §2: QoS 0, as the GX's broker publishes, and never retained except where
  // the caller says so (the Serial on connect).
  function send (topic, payload, retain = false) {
    client.publish(topic, payload, { qos: 0, retain })
    counters.published++
  }

  function set (topic, payload) {
    if (values.get(topic) === payload) return
    values.set(topic, payload)
    // Offline, a change is only stored: §3 has a full publish follow every
    // reconnect (the feed's own, or the hub's request when the hub is the one
    // that reconnected), which sends current state whole, so nothing is queued.
    if (!isConnected()) return
    if (allowed(topic)) send(topic, payload)
    else counters.dropped++
  }

  function remove (topic) {
    values.delete(topic)
  }

  // A service leaving the bus takes all its topics with it.
  function removeWhere (prefix) {
    for (const topic of values.keys()) {
      if (topic.startsWith(prefix)) values.delete(topic)
    }
  }

  // §4: an announcement's payload is a JSON array of filters. A zero-length
  // payload is MQTT's own clearing of a retained message, so it clears the
  // source quietly; anything else that is not an array of strings is logged
  // and, like an empty array, clears it, since a filter set we cannot read
  // must not keep an old one alive.
  function announced (sourceId, text) {
    // What the hub could see before this announcement; not needed when the
    // connection's full publish is still owed, since that sends everything.
    const before = fullPublishOwed ? null : new Set(eligible())
    let parsed = []
    if (text !== '') {
      try {
        parsed = JSON.parse(text)
      } catch {
        parsed = null
      }
      if (!Array.isArray(parsed) || !parsed.every((filter) => typeof filter === 'string')) {
        logger.warn(`ignoring unreadable subscription announcement from "${sourceId}": ${text}`)
        parsed = []
      }
    }
    if (parsed.length === 0) sources.delete(sourceId)
    else sources.set(sourceId, parsed)
    union([...new Set([...sources.values()].flat())].sort())
    // §3: the unasked full publish, once per connect. Any announcement pays
    // it, an empty or unreadable one included: the contract keys it to the
    // first announcement's arrival, not to its content.
    if (fullPublishOwed) {
      fullPublishOwed = false
      fullPublish()
      return
    }
    // §3: the hub's sources announce separately, so a later announcement
    // that widens the union sends the stored topics it newly made eligible —
    // current values only, no completion marker. One that narrows the union
    // or leaves it as it was makes nothing newly eligible and sends nothing.
    for (const topic of eligible()) {
      if (!before.has(topic)) send(topic, values.get(topic))
    }
  }

  // The stored topics the hub's union admits, in a stable (sorted) order.
  function eligible () {
    return [...values.keys()].sort().filter(allowed)
  }

  // §2: every current value the hub subscribed to, in a stable (sorted) order,
  // then full_publish_completed, which is filtered like everything else (§4:
  // "dumps included"; a hub that waits for it subscribes to it, as on a GX).
  function fullPublish () {
    for (const topic of eligible()) send(topic, values.get(topic))
    if (allowed(completedTopic)) {
      send(completedTopic, JSON.stringify({ value: Math.floor(clock.now() / 1000) }))
    }
  }

  function suppressed (text) {
    try {
      const options = JSON.parse(text)['keepalive-options']
      return Array.isArray(options) && options.includes(SUPPRESS_REPUBLISH)
    } catch {
      return false
    }
  }

  // §2: `R/<portal>/keepalive` is a poke (suppressed) or a full publish (any
  // other payload, the empty one included); any other `R/<portal>/<path>` is
  // a mirror read of `N/<portal>/<path>`, answered from the stored value,
  // which is current by construction. A path we hold no value for gets no
  // reply, and a mirror read is filtered like any other publish (§4).
  function read (topic, text) {
    if (topic === keepaliveTopic) {
      if (!suppressed(text)) fullPublish()
      return
    }
    const wanted = `N/${portalId}${topic.slice(readPrefix.length)}`
    if (values.has(wanted) && allowed(wanted)) send(wanted, values.get(wanted))
  }

  function start () {
    if (client !== null) return
    const own = connect()
    client = own
    onStatus('connecting')

    // Each handler is inert once `own` is no longer the live client. The
    // listeners are never removed: mqtt.js emitting a late 'error' with no
    // listener attached would throw.
    own.on('connect', () => {
      if (own !== client) return
      // The announcements are rebuilt from what this session's subscription
      // delivers (they are retained, §4), so a source the hub cleared while
      // we were away cannot leave its old filters behind.
      sources.clear()
      union([])
      fullPublishOwed = true
      // Subscribed on every connect rather than left to the library's
      // resubscribe option, which is the caller's to set.
      own.subscribe([`${ANNOUNCEMENTS}+`, `${readPrefix}/#`, `W/${portalId}/#`])
      // §2: the hub's setup wizard identifies a GX by reading the Serial on
      // subscribe, so it is retained, and re-sent on every connect regardless
      // of the announcement (§4.1). §4.2: the writable flag follows it, the
      // same way, so a hub subscribing at any time learns both.
      send(serialTopic, values.get(serialTopic), true)
      // Not through send(): the counters count Victron values, and this is
      // a convention of ours.
      own.publish(writableTopic, JSON.stringify(writable), { qos: 0, retain: true })
      onStatus('connected')
    })

    own.on('message', (topic, payload) => {
      if (own !== client) return
      const text = payload.toString('utf8')
      if (topic.startsWith(ANNOUNCEMENTS)) {
        announced(topic.slice(ANNOUNCEMENTS.length), text)
      } else if (topic.startsWith('W/')) {
        // §2: the caller does the set when the owner allows writes; refused
        // otherwise. A refusal does not log the payload: a write may carry a
        // setting's new value, a secret among them. Logged on change, not on
        // every write, as index.js logs its own refusals: a hub that keeps
        // sending writes this producer has said it will refuse would
        // otherwise fill the owner's error log with one line. There is only
        // one reason here, fixed for the feed's life, so the first is the
        // warning and every later one is debug with the running count.
        if (writable) {
          onWrite(topic, text)
        } else {
          refusals++
          if (refusals === 1) logger.warn(`refused write ${topic}: writes are not allowed`)
          else logger.debug(`refused write ${topic}: writes are not allowed (refusal ${refusals})`)
        }
      } else {
        read(topic, text)
      }
    })

    own.on('offline', () => {
      if (own === client) onStatus('offline')
    })
    own.on('reconnect', () => {
      if (own === client) onStatus('reconnecting')
    })
    own.on('error', (error) => {
      if (own === client) onStatus(`error: ${error.message}`)
    })
  }

  function stop () {
    if (client === null) return
    const own = client
    client = null
    own.end(true)
  }

  return {
    start,
    stop,
    set,
    remove,
    removeWhere,
    isConnected,
    // The hub's effective filter set (§4), sorted: empty until the first
    // announcement of this connection.
    filters: () => [...filters],
    counters: () => ({ ...counters })
  }
}

module.exports = { createFeed }
