'use strict'

// The carrier: one WebSocket to a Wattknot hub, per the network transport of
// protocol/n2k-stream.md (§6.1) and its command back-channel (§7). It knows
// nothing about N2K — bytes go out, command lines come back — so a later feed
// can reuse it unchanged. All time comes from the injected `timers`, and the
// WebSocket class is injected, so a test drives both.

// §6.1 "The endpoint": one path on the hub's one HTTP origin.
const PATH = '/api/n2k/stream'

// §6.1 "The endpoint": the cloud address is built from the hub's device id.
const CLOUD_DOMAIN = 'api.wattknot.app'

// The device id becomes one DNS label of the cloud host, so it is held to a
// label's shape: letters, digits and inner hyphens, at most 63 characters. A
// pasted hostname or URL fails here rather than as a DNS error on every dial.
const DNS_LABEL = /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/i

// A local hub address is `host` or `host:port`: a hostname, an IPv4 address or
// a bracketed IPv6 address. A scheme, a path, credentials or whitespace are
// refused, because the plugin supplies the scheme and path itself (§6.1) and
// silently stripping a pasted `wss://` would dial in clear what the operator
// meant to encrypt.
const LOCAL_ADDRESS = /^(\[[0-9a-f:.]+\]|[a-z0-9.-]+)(?::(\d{1,5}))?$/i

// The token travels in an HTTP header (§6.1 "Credential"), so it must be
// visible ASCII: a space or control character would split or corrupt the
// header line. The contract does not define the token's alphabet beyond that.
const TOKEN = /^[\x21-\x7e]+$/

// §6.1 "Drop, don't delay": the hub stamps values on arrival, so every byte
// waiting in our send buffer is a byte that will arrive claiming to be fresh.
// The stream runs at roughly 3.5 KB/s, so 4 KiB is about one second of it —
// the heartbeat's own cadence (§4), and the most staleness we accept before
// writes are dropped and counted as `txd`. On a healthy link this buffer stays
// near empty (writes pass straight to the kernel), so the cap only bites once
// the link is already backing up. `bufferedAmount` cannot see the kernel's own
// socket buffer below it, so the real delay can exceed this second.
const BUSY_CAP_BYTES = 4096

// §6.1 asks for a back-off on refusal ("a refusal rarely clears on its own")
// and a redial on every close. One fixed 5 s serves both: short against the
// hub's 15 s liveness window (§4), so a dropped link is usually back before
// the hub marks the producer dead, yet slow enough that two producers sharing
// a token (§6.1 "newest wins") flap at a visible, harmless pace rather than in
// a tight loop, and a wrong token costs the relay one request per 5 s.
const REDIAL_MS = 5000

// §6.1 "Dead links": the hub tells a producer nothing about the link — after
// `refresh` it sends nothing at all — and a half-open TCP connection (a
// cellular or Starlink NAT dropping its mapping) stays OPEN here until the
// kernel gives up, minutes later, every write meanwhile counted as busy rather
// than down. So the producer asks: a WebSocket ping every KEEPALIVE_MS, and if
// the previous ping has had no pong by the time the next is due, the link is
// dead — terminate and redial. Detection takes between one and two intervals,
// comparable to the hub's own 15 s window (§4). The ws server answers pings by
// itself and the relay pipes them, so the hub needs nothing.
const KEEPALIVE_MS = 10000

// A TCP connect that is never answered with an upgrade (a middlebox accepting
// and then stalling) would otherwise leave the producer on "connecting"
// forever: ws aborts the handshake after this and reports an error, which
// redials like any other failure.
const HANDSHAKE_TIMEOUT_MS = 10000

// §6.1 "Refusals": what each status means, in the operator's terms. The same
// status can come from the hub or, on the cloud address, from the relay, and
// nothing on the wire says which, so each meaning names both.
const REFUSALS = {
  401: 'bad or missing token',
  404: 'not served: wrong boat ID or address, or the hub is not on the network transport',
  503: 'boat offline, or the hub\'s N2K ingest is stopped'
}

// The hub's second door, for the Victron feed (protocol/mqtt-producer.md §1):
// same host and token, this path.
const MQTT_PATH = '/api/mqtt'

/**
 * The hub's stream URL from the plugin's settings. A non-empty local hub
 * address wins and the boat ID is then not needed; otherwise the boat ID
 * builds the cloud address. Throws an Error with an operator-facing message
 * when the settings cannot make an address. `path` picks the door: the N2K
 * stream by default, or the MQTT door.
 *
 * @param {{ boatId?: string, localAddress?: string }} settings
 * @param {string} [path]
 * @returns {string}
 */
function addressFor ({ boatId = '', localAddress = '' } = {}, path = PATH) {
  const local = localAddress.trim()
  if (local !== '') {
    const match = LOCAL_ADDRESS.exec(local)
    const port = match && match[2] !== undefined ? Number(match[2]) : null
    if (match === null || port === 0 || port > 65535) {
      throw new Error(`Local hub address must be host or host:port, with no scheme or path (got "${local}")`)
    }
    // §6.1: plain ws:// is permitted on the boat's own network. No port means
    // ws://'s default of 80, which is where a boat hub serves.
    return `ws://${local}${path}`
  }

  const id = boatId.trim()
  if (id === '') {
    throw new Error('Boat ID is required when no local hub address is set')
  }
  if (!DNS_LABEL.test(id)) {
    throw new Error(`Boat ID must be the hub's device id alone: letters, digits and hyphens (got "${id}")`)
  }
  // §6.1: wss:// whenever the path leaves the boat's network.
  return `wss://${id}.${CLOUD_DOMAIN}${path}`
}

/**
 * The token as it is sent: the setting trimmed and held to TOKEN. Throws an
 * Error with an operator-facing message otherwise. Both feeds send the same
 * token, and either may run without the other, so each checks it here.
 *
 * @param {string} [token]  the setting as the owner entered it
 * @returns {string}
 */
function credentialFrom (token) {
  const credential = (token || '').trim()
  if (!TOKEN.test(credential)) {
    // §6.1: an unauthenticated connection carries nothing, so there is no
    // point dialling without one.
    throw new Error('Token is required, and must contain no spaces')
  }
  return credential
}

/**
 * @param {object} deps
 * @param {string} deps.address  from addressFor()
 * @param {string} deps.token  the hub's producer token (§6.1 "Credential")
 * @param {typeof import('ws')} deps.WebSocket  the `ws` client class
 * @param {{ setTimeout: Function, clearTimeout: Function }} deps.timers
 * @param {(text: string, detail: { refused: boolean, connected: boolean }) => void} deps.onStatus
 *   operator-facing state; `refused` marks a refused handshake (§6.1), `connected`
 *   the one status that is the link coming up
 * @param {(line: string) => void} deps.onCommand  one §7 line, terminator stripped
 */
function createConnection ({ address, token, WebSocket, timers, onStatus, onCommand }) {
  const credential = credentialFrom(token)

  let running = false
  // The one live socket, or null. Every handler checks that its own socket is
  // still this one, which is what makes a stopped or dropped socket's late
  // events inert: no status, no command, no second redial.
  let socket = null
  let redialTimer = null
  let keepaliveTimer = null

  // Every state change reaches the caller through one callback; `refused`
  // marks the one kind that is a setting the owner must fix (§6.1 "Refusals")
  // rather than a passing state of the link, so the caller can show it as an
  // error instead of status.
  function status (text, refused = false, connected = false) {
    onStatus(text, { refused, connected })
  }

  function clearKeepalive () {
    if (keepaliveTimer !== null) {
      timers.clearTimeout(keepaliveTimer)
      keepaliveTimer = null
    }
  }

  function start () {
    if (running) return
    running = true
    dial()
  }

  function stop () {
    running = false
    if (redialTimer !== null) {
      timers.clearTimeout(redialTimer)
      redialTimer = null
    }
    clearKeepalive()
    if (socket !== null) {
      const old = socket
      socket = null
      // A normal closure. The listeners stay attached (and inert): removing
      // them would leave ws's late 'error' with no listener, which throws.
      old.close(1000)
    }
  }

  // §6.1 "Drop, don't delay": never queue, never retry. The answer tells the
  // caller which drop counter to bump.
  function send (buffer) {
    if (socket === null || socket.readyState !== WebSocket.OPEN) return 'down'
    if (socket.bufferedAmount > BUSY_CAP_BYTES) return 'busy'
    // §6.1 "Stream": binary messages; message boundaries carry no meaning.
    socket.send(buffer, { binary: true })
    return 'sent'
  }

  function dial () {
    redialTimer = null
    status('connecting')
    const ws = new WebSocket(address, {
      // §6.1 "Credential": a request header, never the URL.
      headers: { Authorization: `Bearer ${credential}` },
      // ws offers permessage-deflate unless told not to; §6.1 leaves
      // compression open and not negotiated today, so it is never offered.
      perMessageDeflate: false,
      handshakeTimeout: HANDSHAKE_TIMEOUT_MS
    })
    socket = ws
    // §7.1: bytes are read into a line buffer until \n. Held per socket, so a
    // partial line from a dropped connection never prefixes the next one's.
    let pending = ''
    // Whether the last ping on this socket is still unanswered.
    let awaitingPong = false

    function keepalive () {
      keepaliveTimer = null
      if (ws !== socket) return
      if (awaitingPong) {
        drop(ws, `link dead: no reply to ping in ${KEEPALIVE_MS / 1000} s`)
        return
      }
      awaitingPong = true
      ws.ping()
      keepaliveTimer = timers.setTimeout(keepalive, KEEPALIVE_MS)
    }

    ws.on('open', () => {
      if (ws !== socket) return
      status('connected', false, true)
      keepaliveTimer = timers.setTimeout(keepalive, KEEPALIVE_MS)
    })

    ws.on('pong', () => {
      if (ws === socket) awaitingPong = false
    })

    ws.on('message', (data, isBinary) => {
      // §6.1 "Commands": text messages only; the producer never reads any
      // other data from the hub (§7), so a binary message is ignored.
      if (ws !== socket || isBinary) return
      pending += data.toString('utf8')
      const lines = pending.split('\n')
      pending = lines.pop()
      for (const line of lines) {
        // §7.1: a trailing \r is tolerated and ignored. An empty line can
        // carry no token, so it is not delivered.
        const command = line.endsWith('\r') ? line.slice(0, -1) : line
        if (command !== '') onCommand(command)
      }
    })

    // §6.1 "Refusals": the handshake was answered with a status, not 101.
    // With this listener attached ws leaves the request to us, so drop()
    // aborts it.
    ws.on('unexpected-response', (request, response) => {
      drop(ws, refusal(response.statusCode), true)
    })

    // ws emits 'error' and then 'close' for one failure; whichever arrives
    // first drops the socket, and the second finds it already inert.
    ws.on('error', (error) => {
      drop(ws, `connection failed: ${error.message}`)
    })

    ws.on('close', (code) => {
      drop(ws, `connection closed (code ${code})`)
    })
  }

  // §6.1: a producer whose socket closes simply redials, after a fixed back-off.
  function drop (ws, reason, refused = false) {
    if (ws !== socket) return
    socket = null
    clearKeepalive()
    ws.terminate()
    status(`${reason}; redialling in ${REDIAL_MS / 1000} s`, refused)
    redialTimer = timers.setTimeout(dial, REDIAL_MS)
  }

  return { start, stop, send }
}

function refusal (status) {
  const meaning = REFUSALS[status]
  return meaning === undefined
    ? `refused: HTTP ${status}`
    : `refused: HTTP ${status} (${meaning})`
}

module.exports = { createConnection, addressFor, credentialFrom, MQTT_PATH }
