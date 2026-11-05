'use strict'

// Signal K glue: the plugin Signal K loads, wiring two feeds to one hub.
//
//   N2K: the server's raw CAN tap → lib/stream.js → lib/connection.js, per
//        Wattknot's N2K stream contract (protocol/n2k-stream.md, kind 2).
//   Victron: lib/venus.js (D-Bus) → lib/victron.js (rendering) → lib/feed.js
//        (an ordinary MQTT client of the hub's own broker), per the MQTT
//        producer contract (protocol/mqtt-producer.md).
//
// Everything about either wire lives in lib/; this file owns the settings
// page, start/stop, the heartbeat timer, the hub's commands (§7), the status
// line, and keeping the two feeds independent: a failure in one never touches
// the other, and the owner can switch either off.

const timers = require('timers')
const { performance } = require('perf_hooks')
const WebSocket = require('ws')
const dbus = require('dbus-native')
const mqtt = require('mqtt')
const { getPGNs } = require('@canboat/pgns')
const { createStream, createIsFast } = require('./lib/stream')
const { createConnection, addressFor, credentialFrom, MQTT_PATH } = require('./lib/connection')
const { createVenus, NO_GX } = require('./lib/venus')
const { createRenderer, createMatcher } = require('./lib/victron')
const { createFeed } = require('./lib/feed')
const { name: PACKAGE_NAME, version: PACKAGE_VERSION } = require('./package.json')

// The Victron feed retries from scratch after D-Bus fails — the bus daemon
// restarting, or a host with no system bus the plugin can open. On such a host
// the same failure repeats forever, so it is logged on change, not on attempt:
// the first at error, repeats at debug, and a recovery at debug with the
// count. The status line carries the state throughout. The retry itself is a
// failed socket connect every 30 s, which costs nothing, so it is not backed
// off.
const VICTRON_RETRY_MS = 30000

// A bus that answers but carries no Victron service at all (lib/venus.js
// NO_GX) is a Signal K server that is not a GX — a Raspberry Pi — and that is
// not a fault but what the host is, so it is shown as a calm state of its own
// (`Victron: no GX on this bus`), logged at debug, never at error, and looked
// at again only every 5 minutes. Not never: a GX whose Venus services start
// after Signal K does is still found without a restart, and five minutes is
// short against a boat's day and long against a log that would otherwise
// gain a line every 30 s for nothing.
const NO_GX_RETRY_MS = 300000

// mqtt.js options for the feed's client (protocol/mqtt-producer.md §1, §3):
// the token as a WebSocket request header, never in the URL; the library's
// own keepalive and reconnect are the liveness mechanism; a clean session,
// because the hub asks for a full publish after any gap; and no library
// resubscribe, because the feed subscribes on every connect itself (so one
// SUBSCRIBE per reconnect, not two).
const MQTT_KEEPALIVE_S = 30
const MQTT_RECONNECT_MS = 5000

// The canboatjs provider emits one event per CAN frame, before decode
// (README "What it does" 1).
const RAW_FRAMES = 'canboatjs:rawoutput'

// §4: one telemetry line per second, on a timer, unconditionally.
const HEARTBEAT_MS = 1000

// §5 `fw`: the leading major.minor.patch of a semver version. A prerelease or
// build suffix is allowed and ignored, so `0.2.0-beta.1` reads as `0.2.0`.
const SEMVER = /^(\d+)\.(\d+)\.(\d+)(?:[-+].*)?$/

/**
 * §5 `fw` from v4: `major × 10000 + minor × 100 + patch`. Minor and patch
 * each get two decimal digits, so a value above 99 would carry into the next
 * part and two builds would share an integer — the collision v4 replaced
 * digit concatenation to end. Refused rather than reported ambiguously.
 *
 * @param {string} version  a semver version, e.g. this package's
 * @returns {number}
 */
function fwFrom (version) {
  const match = SEMVER.exec(version)
  if (match === null) throw new RangeError(`not a semver version: "${version}"`)
  const [major, minor, patch] = match.slice(1).map(Number)
  if (minor > 99 || patch > 99) {
    throw new RangeError(`minor and patch must be 0–99 for §5 fw, got "${version}"`)
  }
  return major * 10000 + minor * 100 + patch
}

// Computed at load, so a version the formula cannot carry fails the test
// suite before release, not the plugin on a boat.
const FW = fwFrom(PACKAGE_VERSION)

/**
 * Logging on change, not on every occurrence, for a fault that can repeat
 * faster than anyone reads it: a tap that throws on every CAN frame, a hub
 * sending the same refused write. The shape is the Victron feed's own (fail()
 * in createPlugin): the first occurrence of a reason at its site's level,
 * repeats at debug or not at all, and the streak's count once it ends — on a
 * different reason, or when the site says the fault has stopped. This only
 * counts; each site words its own lines.
 *
 * @param {object} lines
 * @param {(text: string) => void} lines.first  a streak begins
 * @param {(text: string, count: number) => void} lines.repeat  the same
 *   reason again; `count` includes this one
 * @param {(reason: string, count: number) => void} lines.ended  the streak is
 *   over, after `count` occurrences
 * @returns {{ note: (reason: string, text: string) => void, end: () => void }}
 */
function createStreak ({ first, repeat, ended }) {
  let streak = null

  function end () {
    if (streak === null) return
    const { reason, count } = streak
    streak = null
    ended(reason, count)
  }

  function note (reason, text) {
    if (streak !== null && streak.reason === reason) {
      streak.count++
      repeat(text, streak.count)
      return
    }
    end()
    streak = { reason, count: 1 }
    first(text)
  }

  return { note, end }
}

// Shown in Signal K's plugin configuration page, for the boat owner (README
// "Settings"), in this order: the two addresses, of which one is filled, the
// token, then one switch per feed. Only the token is always required: the boat
// ID is required only when no local hub address is set, which a schema cannot
// say simply, so addressFor() decides it at start and the message reaches the
// owner as the plugin error. The token is shown in plain text: the owner
// pastes it once and needs to see what was pasted. The feed switches default
// on, and one missing from saved settings reads as on (see start), so an
// upgrade from before they existed runs both feeds as it did. "Allow
// switches" is the other way round — off, and missing reads as off — because
// it lets the hub operate the boat's equipment (protocol/mqtt-producer.md §2
// "W/ writes"): the owner says yes once, here, where Signal K's own login
// stands between the setting and the internet. Its words name exactly what
// lib/victron.js WRITABLE admits, so the owner is told the whole of what the
// yes allows.
const schema = {
  type: 'object',
  required: ['token'],
  properties: {
    localAddress: {
      type: 'string',
      title: 'Local hub address',
      description: 'Only for a Wattknot hub on this boat\'s own network: its address as host or host:port, ' +
        'for example wattknot.local or 192.168.1.20:8080. Leave empty to connect to your hub over the internet.'
    },
    boatId: {
      type: 'string',
      title: 'Boat ID',
      description: 'The boat ID that came with your Wattknot hub\'s token. Used to reach the hub over the internet.'
    },
    token: {
      type: 'string',
      title: 'Token',
      description: 'The token that came with your Wattknot hub. It is what lets this server send ' +
        'your boat\'s data to your hub.'
    },
    sendN2k: {
      type: 'boolean',
      title: 'Send NMEA 2000 data',
      description: 'Send this boat\'s NMEA 2000 network to your hub.',
      default: true
    },
    sendVictron: {
      type: 'boolean',
      title: 'Send Victron data',
      description: 'Send this Victron GX device\'s system data to your hub: batteries, solar, inverter and the rest.',
      default: true
    },
    allowSwitches: {
      type: 'boolean',
      title: 'Allow switches',
      description: 'Let your hub switch this GX\'s relay outputs and set your MultiPlus or Quattro\'s mode, ' +
        'and nothing else. Off by default.',
      default: false
    }
  }
}

/**
 * The plugin, with its outside world injected. Signal K uses the default
 * export below, which hands this the real one.
 *
 * @param {object} app  the Signal K server's plugin API
 * @param {object} deps
 * @param {typeof import('ws')} deps.WebSocket
 * @param {{ setTimeout, clearTimeout, setInterval, clearInterval }} deps.timers
 * @param {{ now: () => number }} deps.clock  milliseconds, monotonic
 * @param {() => Array<{ PGN: number, Type: string }>} deps.getPGNs  the canboat table
 */
function createPlugin (app, { WebSocket, timers, clock, getPGNs, dbus, connectMqtt, wallClock }) {
  // The running instance, or null. Each start() makes a new record and every
  // handler it creates checks that its record is still this one, so a stopped
  // instance's handlers are inert even when called through a reference taken
  // before stop(): an emit already in progress (EventEmitter calls the
  // listeners it held when the emit began), or a late tick. A `reboot`
  // replaces the N2K feed inside a run, so its handlers check their own
  // stream and connection as well (startN2k). Detaching the tap and clearing
  // the timer remain the primary mechanism; this makes a straggler harmless.
  let current = null

  // A refused handshake (§6.1 "Refusals") is a setting the owner must fix — a
  // wrong token or boat ID — so it is shown as the plugin's error, in red,
  // like a settings error at start. Every other connection state is status.
  function report (run, text) {
    if (run.refused) app.setPluginError(text)
    else app.setPluginStatus(text)
  }

  function start (options) {
    const settings = options || {}
    // Each feed runs unless its switch is off; a switch missing from the
    // settings is on (see schema). The Victron feed also needs a D-Bus module:
    // without one (a test, a host without one) there is no feed.
    const sendN2k = settings.sendN2k !== false
    const sendVictron = settings.sendVictron !== false && Boolean(dbus)
    // text: the N2K link's state, 'off' while it is switched off.
    // victronFailure: the current streak of identical Victron failures, or
    // null while the feed is healthy (see VICTRON_RETRY_MS).
    // base: the drop counters when the link first came up, or null before it.
    // Frames dropped while a plugin is still dialling after a start are the
    // restart's price, not the link's health, so the status counts from here;
    // the heartbeat's own counters stay since-start, as §5 defines them.
    // refusals: the hub's refused writes, logged on change (see write()).
    const run = {
      text: sendN2k ? '' : 'off',
      refused: false,
      victronText: 'off',
      victron: null,
      victronFailure: null,
      refusals: createStreak({
        first: (text) => app.error(text),
        repeat: (text, count) => app.debug(`${text} (refusal ${count})`),
        ended: (reason, count) => app.debug(`refused write: "${reason}" ${count} time(s) in a row`)
      }),
      base: null,
      stream: null,
      timer: null
    }
    // Both feeds dial the same host with the same token, so either one on
    // needs both settings good. A settings error: say what is wrong and open
    // nothing. The owner fixes the field and saves, and Signal K starts us
    // again. With both feeds off nothing is dialled, so nothing is checked.
    let token = null
    if (sendN2k || sendVictron) {
      try {
        addressFor(settings)
        token = credentialFrom(settings.token)
      } catch (error) {
        app.setPluginError(error.message)
        return
      }
    }

    current = run
    if (sendN2k) startN2k(run, settings, token)
    // The Victron feed, beside the stream and independent of it.
    if (sendVictron) startVictron(run, settings, token)
    if (sendN2k || sendVictron) run.timer = timers.setInterval(() => tick(run), HEARTBEAT_MS)
    // With N2K on, its connection reports from the first dial; otherwise the
    // owner sees which feeds are off from the start, not a second later.
    if (!sendN2k) report(run, statusLine(run, null))
  }

  // Once a second: the N2K heartbeat (§4) when that feed is on, and the status
  // line either way, which is how a Victron state change reaches the owner.
  function tick (run) {
    if (run !== current) return
    let counters = null
    if (run.stream !== null) {
      // Read before heartbeat(): this is exactly what the line about to be
      // written tells the hub (rx resets as it is written, §5.2), so the
      // owner's status and the hub's view of this producer agree.
      counters = run.stream.counters()
      run.stream.heartbeat()
    }
    report(run, statusLine(run, counters))
  }

  function stop () {
    if (current === null) return
    const run = current
    current = null
    if (run.timer !== null) timers.clearInterval(run.timer)
    if (run.stream !== null) stopN2k(run)
    if (run.victron) stopVictron(run.victron)
  }

  // The N2K feed (protocol/n2k-stream.md, kind 2): the raw CAN tap → the
  // stream → the connection. The settings are already checked, so
  // createConnection cannot refuse them here. Run at start and again on a
  // `reboot` (obey), each time a new link: nothing counted, no state yet.
  //
  // Every handler checks that its own stream and connection are still the
  // run's, not only that the run is current, because a `reboot` replaces them
  // inside a run that carries on: the rest of the message the reboot came in
  // (connection.js delivers every line of it) and an emit already in flight
  // reach the old instance's handlers, and those must be inert.
  function startN2k (run, settings, token) {
    run.text = ''
    run.refused = false
    run.base = null
    const connection = createConnection({
      address: addressFor(settings),
      token,
      WebSocket,
      timers,
      onStatus: (text, { refused, connected }) => {
        run.text = text
        run.refused = refused
        if (connected && run.base === null) run.base = run.stream.counters()
        report(run, `N2K: ${text}`)
      },
      onCommand: (line) => {
        if (run === current && run.connection === connection) obey(line, run, settings, token)
      }
    })
    const stream = createStream({
      isFast: createIsFast(getPGNs()),
      send: connection.send,
      clock,
      fw: FW,
      timers
    })
    // The tap runs inside canboatjs's emit, so a throw from it would not stay
    // ours: it would unwind into Signal K's N2K pipeline and break it for
    // every plugin on the server. A frame that throws is caught and the tap
    // carries on with the next. Logged on change (createStreak): the first
    // of a reason at error, its repeats not at all — at frame rate even a
    // debug line per frame is a storm of its own — and the count at debug
    // once frames pass again or the reason changes.
    const tapErrors = createStreak({
      first: (text) => app.error(`N2K tap: a CAN frame threw: ${text}`),
      repeat: () => {},
      ended: (reason, count) => app.debug(`N2K tap: "${reason}" ${count} time(s) in a row`)
    })
    run.stream = stream
    run.connection = connection
    run.tap = (event) => {
      if (run !== current || run.stream !== stream) return
      try {
        stream.frame(event)
      } catch (err) {
        tapErrors.note(err.message, err.message)
        return
      }
      tapErrors.end()
    }
    app.on(RAW_FRAMES, run.tap)
    connection.start()
  }

  function stopN2k (run) {
    app.removeListener(RAW_FRAMES, run.tap)
    run.stream.stop()
    run.connection.stop()
  }

  // The Victron feed (protocol/mqtt-producer.md): D-Bus → renderer → an
  // ordinary MQTT client of the hub's broker. The portal id arrives first
  // (venus.onPortalId), so the renderer and the feed exist before the first
  // item and the feed's cache fills as the services are read. The feed
  // CONNECTS only once venus.start() resolves — the initial read complete —
  // because the full publish it owes on the hub's first announcement (§3) is
  // what the hub takes as discovery complete: connected at portal-id time it
  // paid that debt from a half-read cache, sent full_publish_completed, and
  // the hub's wizard confirmed a thirteen-device boat with three
  // (2026-10-26). D-Bus failing, at start or later, tears the feed down and
  // tries again from scratch after VICTRON_RETRY_MS (NO_GX_RETRY_MS on a bus
  // with no Victron service); the stream never notices.
  function startVictron (run, options, token) {
    const victron = { venus: null, feed: null, renderer: null, retry: null }
    run.victron = victron
    run.victronText = 'starting'
    const address = addressFor(options, MQTT_PATH)
    // venus.js guarantees its callbacks: the portal id before any item, and
    // nothing at all after stop(). So the callbacks below need no run guard;
    // only a failure can arrive late (start() rejecting after a stop), and
    // the retry timer is cleared by stopVictron.
    // `noGx`: the bus has no Victron service (NO_GX_RETRY_MS) — the same
    // streak and the same retry from scratch, but a calm state rather than a
    // fault: its own status text, never an error line, a long back-off.
    function fail (reason, noGx = false) {
      if (run !== current || victron.retry !== null) return
      const retryMs = noGx ? NO_GX_RETRY_MS : VICTRON_RETRY_MS
      run.victronText = noGx ? reason : `${reason}; retrying in ${retryMs / 1000} s`
      if (run.victronFailure !== null && run.victronFailure.reason === reason) {
        run.victronFailure.count++
        app.debug(`Victron feed: ${reason} (attempt ${run.victronFailure.count})`)
      } else {
        run.victronFailure = { reason, count: 1 }
        if (noGx) app.debug(`Victron feed: ${reason}; looking again every ${NO_GX_RETRY_MS / 60000} min`)
        else app.error(`Victron feed: ${reason}`)
      }
      stopVictron(victron)
      victron.retry = timers.setTimeout(() => startVictron(run, options, token), retryMs)
    }

    // start() resolving ends a failure streak: the next failure, whatever its
    // reason, is new information and is logged at error again. No run guard:
    // venus.js rejects a start() that was stopped, so this is only ever the
    // current run's.
    function recovered () {
      if (run.victronFailure === null) return
      app.debug(`Victron feed: recovered after ${run.victronFailure.count} failed attempt(s)`)
      run.victronFailure = null
    }

    // §2 "W/ writes": the hub's write becomes one D-Bus set, when the owner's
    // "Allow switches" is on. The renderer decides whether it is a switch,
    // where it goes and whether the value fits. A refusal is the owner's
    // error log (it means a hub sent what the contract says it should not),
    // logged on change (run.refusals): the first of a reason at error,
    // repeats at debug, and the count once a write is accepted or the reason
    // changes, so a hub repeating a bad write cannot flood the log. An
    // accepted write is the debug log with its topic only: like feed.js, this
    // plugin never logs a W/ payload, so the rule holds whatever the
    // allow-list admits. The GX refusing the set is an error with the code.
    // The new state is not reported here: the device announces it on D-Bus
    // and the feed publishes it like any other change.
    function write (topic, text) {
      const target = victron.renderer.writeFor(topic, text)
      if (target.refused !== undefined) {
        run.refusals.note(target.refused, `refused write ${topic}: ${target.refused}`)
        return
      }
      run.refusals.end()
      victron.venus.set(target.busName, target.path, target.variant).then(
        () => app.debug(`wrote ${topic}`),
        (err) => app.error(`write ${topic} failed: ${err.message}`)
      )
    }

    victron.venus = createVenus({
      dbus,
      timers,
      logger: { debug: app.debug, error: app.error },
      onPortalId: (portalId) => {
        victron.renderer = createRenderer({ portalId })
        victron.feed = createFeed({
          connect: () => connectMqtt(address, {
            wsOptions: { headers: { Authorization: `Bearer ${token}` } },
            keepalive: MQTT_KEEPALIVE_S,
            reconnectPeriod: MQTT_RECONNECT_MS,
            clean: true,
            resubscribe: false,
            clientId: `${PACKAGE_NAME}-${portalId}`
          }),
          portalId,
          logger: { warn: app.error, debug: app.debug },
          clock: wallClock,
          createMatcher,
          onStatus: (text) => { run.victronText = text },
          // Missing reads as off (see schema): a write needs a yes.
          onWrite: options.allowSwitches === true ? write : null
        })
      },
      onItems: (busName, body) => {
        for (const [topic, payload] of victron.renderer.items(busName, body)) victron.feed.set(topic, payload)
      },
      onServiceGone: (busName) => {
        const prefix = victron.renderer.gone(busName)
        if (prefix !== null) victron.feed.removeWhere(prefix)
      },
      onError: (err) => fail(`D-Bus ${err.message}`)
    })
    // Resolved = every service read: now the feed dials, with a whole cache
    // behind its first full publish. No stopped-feed guard: a stop makes
    // venus.start() reject, and nothing runs between its resolution and here.
    victron.venus.start().then(() => {
      recovered()
      victron.feed.start()
    }, (err) => fail(err.message, err.code === NO_GX))
  }

  // Idempotent: a failure may arrive both as onError and as start() rejecting.
  function stopVictron (victron) {
    if (victron.retry !== null) {
      timers.clearTimeout(victron.retry)
      victron.retry = null
    }
    if (victron.feed !== null) {
      victron.feed.stop()
      victron.feed = null
    }
    victron.venus.stop()
  }

  // §7.2, kind 2. `reboot` restarts the producer process only, never the
  // host. It arrives on the N2K stream and the producer it names is that
  // stream's, so it restarts the N2K feed alone: the tap, the stream and the
  // connection stopped and started again with the same settings, a new
  // stream resetting `up` to ~0 as the hub expects (§5.3). The Victron feed
  // is another producer on another door, and is left exactly as it was:
  // restarting it too would re-read D-Bus and drop its MQTT session, costing
  // the hub a full publish it never asked for. The heartbeat timer carries
  // on and finds the new stream. `refresh` is optional and this producer is
  // listen-only, so it is ignored, as is any line it does not know (§7.1).
  function obey (line, run, settings, token) {
    if (line !== 'reboot') return
    stopN2k(run)
    startN2k(run, settings, token)
  }

  return {
    id: PACKAGE_NAME,
    // "Wattknot Plugin", not "Wattknot": what the owner sees in Signal K's plugin
    // list is this plugin, not the product it feeds. The package stays
    // signalk-wattknot, the store's convention for every plugin.
    name: 'Wattknot Plugin',
    description: 'Streams this boat\'s NMEA 2000 bus and Victron system to your Wattknot hub',
    schema,
    // The local hub address is hidden from the settings page for now: every
    // boat the plugin is installed on today reaches its hub through the cloud,
    // and a third field asking about a network the owner does not have only
    // confuses. The setting, its code and its tests stay; delete this entry to
    // show it again (a dev hub on the boat's own network).
    uiSchema: { localAddress: { 'ui:widget': 'hidden' } },
    start,
    stop
  }
}

// What the owner reads on Signal K's plugin page: each link's state, named
// and nothing else (the address is in the settings, a scroll away; a link
// switched off reads `off`), then, while the N2K feed runs, the last second's
// CAN frames and the writes dropped, by cause (§5 rx, txd, txl), counted from
// the moment the link first came up — before that there is no link to drop
// from, and the number would only say how long the dial took. `counters` is
// null when the N2K feed is off. One line with separators: Signal K's plugin
// page collapses line breaks (seen 2026-10-27).
//
// While the hub refuses the handshake there are no counters either: the line
// is the plugin's error (report()), and Signal K files every error text that
// differs from the last as a "Last error" shown in red beneath the current
// one — counters that tick every second made two red lines that both
// changed every second (seen 2026-11-06). A refused link has nothing to
// count against, so the error holds still until something changes.
function statusLine ({ text, victronText, base, refused }, counters) {
  const links = `N2K: ${text} · Victron: ${victronText}`
  if (counters === null || refused) return links
  const { rx, txd, txl } = counters
  const dropped = base === null ? '' : ` · dropped ${txd - base.txd} slow link, ${txl - base.txl} no link`
  return `${links} · ${rx} frames/s${dropped}`
}

// The production wiring. `performance` is the stream's clock because `up`
// (§5) must never go backwards, and the wall clock can: a GX corrects it from
// GPS. The feed takes the wall clock on purpose: full_publish_completed
// carries unix seconds (mqtt-producer.md §2).
const PRODUCTION = {
  WebSocket, timers, clock: performance, getPGNs,
  dbus, connectMqtt: mqtt.connect, wallClock: Date
}

/** The plugin as Signal K loads it: one argument, the server's app. */
function plugin (app) {
  return createPlugin(app, PRODUCTION)
}

module.exports = plugin
module.exports.createPlugin = createPlugin
module.exports.fwFrom = fwFrom
