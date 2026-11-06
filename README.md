# Wattknot plugin for Signal K

Package `signalk-wattknot` — the store's naming convention for every plugin;
the name an owner sees in Signal K is **Wattknot Plugin**, because what they
are looking at is this plugin, not the product it feeds.

A Signal K server plugin that streams a boat's NMEA 2000 bus to a Wattknot
hub. It is the N2K producer for Wattknot's cloud tier: a boat with a Victron
GX (or any Signal K server with a CAN connection) and no Wattknot gateway on
board.

Both feeds were accepted on a Cerbo GX against a hub through the cloud. Six source files, each with its tests
beside it, at 100% coverage. "Installing" below is for an owner; the rest of
this README is the design and the developer documentation.

## Installing

You need a Signal K server with the boat's NMEA 2000 bus connected to it — on
a Victron GX that is Venus OS Large with Signal K enabled and a CAN
connection configured — and a Wattknot hub reachable through the cloud. The
hub's **Boat ID** and **Token** come from Wattknot when the hub is set up.

The server must run on **Node 20 or newer** (`engines` in `package.json`;
Venus OS Large 3.85 ships Node 24, and `node -v` on the GX tells you). From
Node 20 a dial tries the hub's IPv6 and IPv4 addresses together, so a boat
whose internet is IPv6-only — some Starlink installations are — connects;
Node 18 dials one address and, when that is the unreachable one, waits out
its timeout on every attempt. The first beta boat was exactly that
(2026-11-05). Signal K's own current server needs Node 22 in any case.

1. In Signal K, open **Appstore**, search for *Wattknot*, and install
   **signalk-wattknot**. Restart the server if it asks.
2. Open **Server → Plugin Config → Wattknot Plugin**. Paste the Boat ID and
   the Token, and submit. The plugin starts on its own.
3. The plugin's status line should read `N2K: connected · Victron:
   connected` within a few seconds, and the hub shows the boat's signals.

Leave **Allow switches** off unless you want Wattknot to operate your
inverter, charger or relays from its own switch controls; everything else is
read-only. Updates arrive the way every Signal K plugin's do: the Appstore
shows *update available* and you click it. Nothing is ever pushed to the
boat.

## Stance

This plugin is written as a third party would write it: against Wattknot's
published contracts only, with no knowledge of hub internals. The N2K
contract is one document, [`docs/n2k-stream.md`](docs/n2k-stream.md): §2 the
frames, §3–§5 the heartbeat, §6.1 the endpoint, §7 the commands. The Victron
feed's is [`docs/mqtt-producer.md`](docs/mqtt-producer.md). Both live in the
hub's repository; the copies here are the version this plugin implements. If
building the plugin needs a fact that is not in them, the contract gets
amended; the plugin never reaches past it.

It is a separate repo and a separate release train for the same reason: it
runs on someone else's platform, on their update cadence, through their app
store.

## What it does

1. **Taps raw CAN frames** from the server's canboatjs provider
   (`app.on('canboatjs:rawoutput')`): one event per frame, before decode,
   shaped `{ pgn: { canId, prio, src, dst, pgn }, length, data: ['00', '2e', …] }`
   with the payload as hex strings. This shape is Signal K's, not part of
   Wattknot's contract. Which CAN interface it comes from belongs to Signal
   K's connection settings, not to us.
2. **Reassembles fast packets** into complete messages. The contract requires
   complete messages only (§2); a message that cannot be completed is
   dropped, nothing partial is ever sent. Which PGNs are fast packets comes
   from the canboat PGN table (below).
3. **Encodes each complete message as an Actisense 0x93 frame** (§2.1): DLE
   STX, 0x93, length, priority, PGN, destination, source, a timestamp the
   hub ignores, data, checksum, DLE ETX, with 0x10 doubled inside the body.
   Exactly the bytes the bus carried, nothing re-derived.
4. **Interleaves a kind 2 heartbeat** once a second, unconditionally (§3–§5):
   `$WKT,v=4,kind=2,up=…,rx=…,txd=…,txl=…,fw=…`. `rx` is CAN frames seen
   since the previous line, `txd` writes dropped under back-pressure, `txl`
   writes dropped with no link, `fw` this package's version as
   `major × 10000 + minor × 100 + patch` (§5).
5. **Dials the hub** over a WebSocket with `Authorization: Bearer <token>`
   (§6.1) and writes each frame and each line as one binary message. Message
   boundaries carry no meaning to the hub.
6. **Drops, never delays** (§6.1). While the socket is backed up or down the
   write is dropped and counted. No queue, no replay: the hub stamps on
   arrival, so a late frame would lie about its age.
7. **Redials** on any close, after a fixed 5 s. A refusal (HTTP 401, 404,
   503) is shown as the plugin's error, in red, since it is a setting the
   owner must fix, not a passing state of the link. **Detects a dead link
   itself**: a WebSocket ping every 10 s, and a ping unanswered by the time
   the next is due means the socket is terminated and redialled, so a
   half-open connection costs 10–20 s rather than the minutes TCP would take
   to notice. A handshake unanswered for 10 s is abandoned the same way.
8. **Reads commands back** (§7): `reboot` restarts the N2K feed (stop then
   start with the same settings, so `up` resets; never the GX, and not the
   Victron feed, which the command has nothing to do with); `refresh` is
   ignored, because this producer is listen-only and never becomes a bus
   node. Unknown lines are ignored.

## Settings

Six settings; five on Signal K's plugin configuration page, the local hub
address hidden for now (a `uiSchema` entry in `index.js`: every boat the
plugin is installed on today reaches its hub through the cloud, and a field
about a network the owner does not have only confuses). The setting, its
code and its tests stay; delete that entry to show it again.

| Field | Required | Meaning |
|---|---|---|
| Local hub address | no | `host` or `host:port` of a hub on the boat's own network, port 80 when omitted. When set, the plugin dials `ws://<address>/api/n2k/stream` instead of the cloud. A pasted scheme or path is refused, not stripped: silently turning `wss://` into `ws://` would dial in clear. Empty means cloud. |
| Boat ID | unless a local address is set | The hub's device id, held to the shape of one DNS label. The plugin builds `wss://<boat id>.api.wattknot.app/api/n2k/stream` from it. |
| Token | yes | The hub's producer token, in plain text. Sent as a bearer header, never in the URL. |
| Send NMEA 2000 data | no, on by default | Off: the N2K feed never dials, taps nothing and writes no heartbeat. |
| Send Victron data | no, on by default | Off: the Victron feed never opens D-Bus or dials the MQTT door. |
| Allow switches | no, **off** by default | On: the hub may switch the GX's relay outputs and set a MultiPlus or Quattro's mode, and nothing else — a `W/` write to one of those two paths becomes one D-Bus `SetValue`; any other path is refused. Off: every write is refused and logged, and the hub is told so it shows those controls read-only. |

Rows in that order. A choice-of-target control that showed only one address
was tried and rejected; the two plain fields stay, with the rule enforced at
start.

**The switches (0.1.6).** Each feed can be switched off on its own, because
a boat may get one of them from elsewhere: N2K from another producer on the
same hub, which this plugin's stream would otherwise knock off ("newest
wins", §6.1), or a Signal K server that is not a GX, where there is no
Victron system to read. A switch missing from saved settings reads as on, so
an upgrade from 0.1.5 or earlier runs both feeds as it did. Either feed on
needs the address and token good, since both dial the same host with the
same token; with both off nothing is dialled, nothing is checked, and the
status line says `N2K: off · Victron: off`. With N2K off the status line
has no frame counters, and the hub's Connectivity panel shows the N2K
producer as down, which is true: there is none.

**Allow switches.** The hub writes a switch the same way to a cloud hub's
broker as to the GX's own: `W/<portal>/<type>/<instance>/<path>` with
`{"value": …}` (`docs/mqtt-producer.md` §2 "W/ writes"). With this on,
the plugin sets that value over D-Bus on the service and path it reads the
topic from, typed as the item was read, and the device's new state comes
back through the feed like any other change; there is no reply, as the GX's
broker gives none. **Only two paths can be written**, the ones the hub's
switches use: the GX's relay outputs
(`system/0/SwitchableOutput/<n>/State`) and a MultiPlus or Quattro's mode
(`vebus/<instance>/Mode`). The list is a constant in `lib/victron.js`
and widening it is a contract change, not a tweak: everything else the GX
exposes — charge voltages, ESS, network settings — stays out of reach
however the hub is configured. It refuses — logged in Signal K's error log,
nothing done — a write with the setting off, to any other path, to a topic
it has never published, with a payload that is not `{"value": <scalar>}`,
or with a value that does not fit the item's type. An accepted write is
logged at debug with its topic (never the value), and the GX refusing a set
is an error with its code. Off by
default, and missing from saved settings reads as off: the hub's token
alone never operates the boat's equipment; the owner says yes here, behind
Signal K's own login. The plugin tells the hub which way it is set with one
retained topic, `wattknot/producer/<portal>/writable`, `true` or `false`,
on every connect (§4.2); a hub that reads `false` refuses its own switch
commands for this source and shows the switches as state, not buttons.
Saving the setting restarts the plugin, so the next connect carries the new
value.

Nothing else: no reconnect tuning, no PGN filter. Where the user obtains the
boat ID and token is the hub's job and is documented there. Saving the
settings restarts the plugin, which the hub sees as `up` returning to zero,
the same as a `reboot` command.

## Structure

The N2K stream is three source files, each with a test file beside it (the
Victron feed's three are in their own section below):

| File | Owns |
|---|---|
| `lib/stream.js` | Frames in, bytes out. Fast-packet reassembly, Actisense encoding, the heartbeat line and its counters, and the fast-packet rule built from the canboat table plus the proprietary ranges. Pure: no I/O, a `clock` injected for `up`. Written from the contract alone and checked byte for byte against a reference capture (39,455 messages identical). Writes within one 50 ms window leave as ONE WebSocket message (`BATCH_MS`, §6.1 "Carriage": boundaries carry no meaning), a tenth of the TLS records a message-per-frame cost on a Starlink uplink, and 50 ms of staleness against the one second the busy cap accepts; a batch the link refuses counts every write in it. A fast packet whose next frame has not come within 2 s (`PARTIAL_MS`) is expired, measured from its latest frame on the injected clock, so a lost message is never completed with a later message's bytes and the partials map stays bounded. |
| `lib/connection.js` | The carrier. Builds the address from settings, dials with the token, writes bytes with the drop rule, reports `busy` / `down` back so the stream can count, redials, delivers command lines. Knows nothing about N2K: a later Victron feed reuses it as is. |
| `index.js` | Signal K glue only. Plugin id, name, settings schema, `start`/`stop`, wiring the tap into the stream and the stream into the connection, the hub's commands, and the status line — `N2K: <state> · Victron: <state> · N frames/s · dropped N slow link, N no link`, each link its state and nothing else (the address is in the settings; a link switched off reads `off`, and with N2K off there are no counters); the dropped counts run from the moment the link first came up (frames dropped while still dialling after a start are the restart's price, not the link's health) while the heartbeat's own counters stay since-start as §5 defines them; one line, because Signal K's plugin page collapses line breaks. Each `start` makes a run record and every handler checks it is still the current one, so a stopped instance's stragglers are inert: the rest of a message after a `reboot`, an emit already in flight. The CAN tap runs inside the server's own emit, so a frame that throws is caught — the first logged at error, the streak's count at debug when it ends — and never reaches Signal K's N2K pipeline. |

## Dependencies

Four at runtime, declared in `package.json`, never borrowed from the server's
own `node_modules` (which would break on a server upgrade). All four are already in Signal K's own tree on a GX, so nothing new
lands on a boat.

- **`ws`**, the N2K stream's WebSocket client. Node's built-in client cannot
  send an `Authorization` header, and the contract puts the token in the
  header.
- **The canboat PGN table** (`@canboat/pgns`, `getPGNs()`), used for one
  question only: is this PGN a fast packet. Not the canboatjs decoder;
  nothing here decodes.
- **`dbus-native`**, the Victron feed's D-Bus client, pure JavaScript. Opened
  in its classic value shapes (`plainValues: false`): the default shapes drop
  each value's signature, which is the one fact that tells a Venus "invalid"
  from an empty list.
- **`mqtt`**, the Victron feed's MQTT client, over WebSocket with the token as
  a request header.

Dev dependencies: Jest, and `aedes` 0.51 as the in-process broker the feed is
tested against (the CommonJS line; 1.x is ESM-only and Jest cannot load it).

## Testing

Jest, 100% lines, branches, functions and statements, enforced in the config,
run before every commit. The same bar as the hub and relay.

- **From the contract**: the Actisense worked example and checksum, DLE
  doubling at every position, heartbeat line shape and the v4 kind 2
  mandatory set, address building for cloud and local.
- **Reassembly boundaries**: a message split across frames, frames out of
  order (a loss, so the message is discarded), a missing frame, two sources
  interleaving the same PGN, a single-frame PGN that must never be held, a
  proprietary-range PGN the table does not list.
- **The connection state machine**: refused with each status, link down,
  redial, hub closing (newest wins), drops counted to `txd` versus `txl`,
  `reboot` received, `refresh` and junk ignored, stop during a redial.
- **A real `ws` server in the integration test**, on loopback, not a mock.
- **A small CAN fixture**: a handful of frames picked from the dock capture,
  heading, wind, depth and one fast-packet PGN. Not the whole capture, which
  carries neighbours' AIS and the marina's position.
- **A fake `app`**: `on`, `removeListener`, `setPluginStatus`,
  `setPluginError`, `debug`, `error`, the six calls the plugin makes.
- **The Victron feed**: the renderer's rules each pinned with a hand-built
  expected string, including the shapes seen on a real GX; the
  D-Bus reader against a fake bus with a table of answers; the MQTT client
  against a real `aedes` broker in-process with a second real client as the
  hub, which announces subscriptions, asks for full publishes and mirror
  reads, and checks retention as a late subscriber; the wiring end to end
  from a fake D-Bus signal to a message at the hub.

What a third party cannot do is check decode correctness against hub code.
They look at the hub's signals page; so do we.

## Release

Published to npm as `signalk-wattknot` with the `signalk-node-server-plugin`
keyword, which is all Signal K's app store is: a search of the npm registry
for that keyword. It is the only install path that needs no SSH and gives
owners updates — Signal K shows "update available" and the owner clicks it;
we never push to a boat.

- **Ownership.** The `wattknot` npm organisation, two-factor enforced.
  Whoever holds it can put code on owners' GX units.
- **Source.** Public, under Apache-2.0 (`LICENSE`). A tarball is readable by
  anyone, so a closed repository would hide nothing and lose the provenance
  link the store shows. The licence withholds the Wattknot name: fork the
  code, not the brand.
- **Mechanics.** A `v*` tag publishes, through
  `.github/workflows/publish.yml`: the test gate must be green, the tag must
  equal `package.json`'s version, and the publish goes through npm's trusted
  publishing (OIDC from this repository, no long-lived token anywhere,
  provenance attached by npm). The first version alone was published from a
  laptop, because a trusted publisher is configured in a package's own
  settings and the package had to exist first.

To release: bump `version`, commit, `git tag v<version>`, push the tag.
`npm pack --dry-run` shows what the tarball will carry — the `files` entry
keeps the tests and the Jest config out of it.

Acceptance, in two steps on the Cerbo:

1. Install from a local tarball, point the local hub address at the dev hub,
   confirm the hub shows the same signals as the gateway did.
2. Publish, install from the Signal K app store, point at a hub through the
   cloud. Only this step proves the customer path.

## The Victron feed

The second feed, against Wattknot's second contract,
`docs/mqtt-producer.md`. The plugin is an **ordinary MQTT client of the
hub's own broker**: it reads the GX over D-Bus, renders Victron's own topic
model (what the GX's broker plugin publishes), and connects with the standard
MQTT client library over WebSocket to the same address and token as the N2K
stream, path `/api/mqtt`. No GX setting is changed: the GX's broker is off
unless the owner enables what Victron now calls insecure, which is why the
feed does not use it.

| File | Owns |
|---|---|
| `lib/venus.js` | D-Bus access with `dbus-native`: enumerate `com.victronenergy.*`, bulk-read each with `GetItems`, follow `ItemsChanged` / `PropertiesChanged` and services coming and going. A service whose read fails is retried every 60 s until it reads or leaves the bus, logged once at error and then at debug, so a service still starting when Signal K asked is not silently absent until a restart. No `GetValue` beyond the portal id; one `SetValue` per write the owner allows (`set`), which resolves on the service's 0 and rejects with its code otherwise. Reports each body to a callback; renders nothing. |
| `lib/victron.js` | Pure. The rendering rules (topic from bus name, instance and path; value by D-Bus signature with sorted keys; `null` for invalid; secrets masked; the skip list), the same rules backwards for a write (`writeFor`: a `W/` topic and payload to the service, path and value in the item's own signature, or the reason it is refused — `not a switch` first, from the topic alone, for anything outside `WRITABLE`, the relay outputs and the Multi's mode), and the MQTT wildcard matcher, compiled once per announced filter set (`createMatcher`: exact topics in a Set, wildcard filters pre-split), because the feed re-checks every stored topic on every announcement and a hub restart sends a burst of them. |
| `lib/feed.js` | The client: connects with `mqtt`, holds the current value per topic, publishes a change only when its topic matches the hub's announced subscriptions (`wattknot/hub/subscriptions/+`, union), answers `R/` as the GX's broker does (keepalive with suppress: nothing; empty: full publish ending in `full_publish_completed`; one topic: republish it), publishes `system/0/Serial` and the writable flag retained and nothing else retained, hands a `W/` to the caller when writes are allowed and refuses it when not. |
| `index.js` | Starts and stops the feed beside the N2K stream; a failure in one never touches the other; the status line shows both. D-Bus failing is retried from scratch every 30 s, logged on change, not on attempt (first at error, repeats at debug, recovery at debug with the count). A bus that answers but carries no Victron service — a Raspberry Pi running Signal K — is its own calm state, `Victron: no GX on this bus`, at debug only and looked at again every 5 minutes. Owns the write: the renderer's answer to the D-Bus set, and the log lines (the first refusal of a reason at error and its repeats at debug with the count when the streak ends, so a hub sending bad writes cannot flood the log; written at debug with the topic, never the value; failed at error). |

The feed uses the boat ID or local address, and the token, that the N2K
stream has; its own switch, "Send Victron data", turns it off (see
"Settings").

What the plugin copies from the GX's broker, and why, is in the contract.
What it deliberately does not do: publish anything on a timer, retain
anything but `Serial` and the writable flag, or write to the GX unless the
owner has said so ("Allow switches").

## Not in the first release

- **Compression.** permessage-deflate is proposed in the contract and open.
- **Local address discovery.** The user types the address.

## Footprint

Measured 2026-10-25 on a Cerbo GX (Venus OS 3.81, two cores) at ~120 CAN
frames/s with both feeds running, three 120 s samples of Signal K's process
from `/proc`, the plugin toggled from Signal K's plugin page between them:
Signal K at 41–43% of one core and 187–197 MB with the plugin on, 30% and
169 MB with it off. **The plugin costs about 12 points of one core and
20–28 MB.** The split between the two feeds was not measured.

The sample, for next time (BusyBox shell; `pidof` because `pgrep -f` matches
the SSH shell itself):

    ssh root@venus.local 'for p in $(pidof node); do grep -q signalk-server /proc/$p/cmdline && P=$p; done; echo "pid $P, cores $(nproc)"; grep VmRSS /proc/$P/status; cat /proc/loadavg; p1=$(awk "{print \$14+\$15}" /proc/$P/stat); read -r _ u1 n1 s1 i1 rest < /proc/stat; sleep 120; p2=$(awk "{print \$14+\$15}" /proc/$P/stat); read -r _ u2 n2 s2 i2 rest < /proc/stat; grep VmRSS /proc/$P/status; cat /proc/loadavg; echo "signalk cpu: $(( (p2-p1) * 100 / 100 / 120 )) % of one core"; echo "box busy: $(( ( (u2+n2+s2) - (u1+n1+s1) ) * 100 / ( (u2+n2+s2+i2) - (u1+n1+s1+i1) ) )) % of all cores"'

Read the plugin's status line before and after: its `dropped` counters restart
with the plugin, so a run after re-enabling is a clean window on how often
the link backs up.

## Licence

Apache-2.0; the text is in `LICENSE`.
