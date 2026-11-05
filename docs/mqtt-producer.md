# Wattknot MQTT producer — contract

The contract between an MQTT producer and a Wattknot hub, as this plugin's
Victron feed implements it. Companion to [n2k-stream.md](n2k-stream.md), the
other producer door. It is an extract of the hub's own contract document,
kept with the hub; section numbers follow it so the plugin's comments cite
the same ones.

A producer is an **ordinary MQTT client of the hub's own broker**, reached
through one authenticated WebSocket. Nothing here is a protocol of
Wattknot's except two small conventions, marked as such in §4. Everything
else is MQTT 3.1.1 and, for Victron data, Victron's own topic model.

---

## 1. The door

| | |
|---|---|
| Path | `/api/mqtt`, on the hub's one HTTP origin |
| Cloud address | `wss://<boat id>.api.wattknot.app/api/mqtt` — the boat id is the identity, the token the proof |
| Boat-network address | `ws://<hub host>:<port>/api/mqtt` (a boat hub serves on port 80) |
| Handshake | a standard WebSocket upgrade over HTTP/1.1, **binary** frames |
| Credential | `Authorization: Bearer <token>` request header, the same token as the N2K stream (n2k-stream.md §6.1). **Never in the URL.** |
| Behind the door | the hub's own MQTT broker, to which the WebSocket's bytes are piped unchanged |

The producer therefore speaks **MQTT 3.1.1 over WebSocket**, exactly as a
client of any broker's WebSocket listener would. A standard client library
with a WebSocket transport and a way to set request headers is all it needs.
The hub's refusals are the N2K door's: `401` bad or missing token, `404` no
door (no producer token configured on this hub) or, at the cloud address,
an unknown boat, `503` the hub is stopping or the boat is offline.

Several producers may connect; each is its own pipe to the broker, and the
broker's MQTT keepalive is what notices a dead one.

## 2. What the hub expects on the broker

The hub's Victron pipeline is the same as on a boat whose hub talks to the
GX's broker directly. A Victron producer therefore reproduces **what
Victron's broker plugin (dbus-flashmq) publishes**, so the hub cannot tell
the difference:

- **Topics** `N/<portal id>/<service type>/<device instance>/<D-Bus path>`,
  payload `{"value": <json>}`. The portal id is the GX's VRM portal id
  (`com.victronenergy.system /Serial`). Service type is the third dotted
  segment of the D-Bus name (`com.victronenergy.battery.ttyUSB0` →
  `battery`). Device instance is `/DeviceInstance` if the service has one;
  otherwise `vecan` uses its name suffix (`vecan/can0`) and all others use
  `0`. Values are rendered by D-Bus signature: dictionaries as objects with
  **sorted keys**, arrays and structs as arrays, byte arrays as number
  arrays, a Venus "invalid" (an empty `ai`) as `null`. Paths ending
  `Password`, `Pincode` or `Passphrase` render as `"******"`. The broker
  skips `system/0/ServiceMapping/*` and `vebus/*/Interfaces/Mk2/Tunnel`; so
  does a producer.
- **`N/<portal id>/system/0/Serial` is published retained**, as the GX's
  broker does: the hub identifies a GX by reading it on subscribe.
- **No other Victron publish is retained.** The hub stamps values on
  arrival; a retained value from an hour ago would arrive looking fresh.
  (The one other retained topic is the producer's own writable flag, §4.2.)
- **`R/` requests are answered**, as the GX's broker answers them:
  - `R/<portal id>/keepalive` with payload
    `{"keepalive-options": ["suppress-republish"]}` — a liveness poke, no
    reply;
  - `R/<portal id>/keepalive` with an empty payload — a **full publish**:
    every current value, ending with `N/<portal id>/full_publish_completed`
    (payload `{"value": <unix seconds>}`), which the hub waits for;
  - `R/<portal id>/<service>/<instance>/<path>` — republish that one value
    now.
- **`W/` writes are a D-Bus set, behind a switch on the boat.** The hub
  writes exactly what it writes to the GX's broker — topic
  `W/<portal id>/<service type>/<device instance>/<D-Bus path>`, payload
  `{"value": <json>}` — and a producer whose owner has allowed writes sets
  that value on the service and path it reads the topic from, typed by the
  item's own D-Bus signature. No reply, as the GX's broker gives none: the
  hub sees the device's new state come back as an ordinary `N/` publish, or
  not. A producer **refuses** a write, logging it and doing nothing, when
  writes are not allowed, when the topic is not one of the hub's switches
  (the GX's relay outputs, `system/0/SwitchableOutput/<n>/State`, and a
  MultiPlus or Quattro's `vebus/<instance>/Mode` — the only paths the hub
  ever writes), when the topic is one it has never read, when the payload
  is not `{"value": …}` with a non-null scalar, or when the value does not
  fit the item's type. The switch is **off by default**, so a hub token
  alone can never operate a boat's equipment. The producer tells the hub
  whether writes are allowed (§4.2), and a hub that is told they are not
  shows its controls read-only rather than sending writes that would be
  refused.

A producer publishes a value **on change**, when the GX reports it, not on a
timer. The hub's keepalive cadence (30 s) is for the GX's broker, which
would otherwise go quiet; a producer that is already event-driven simply
treats it as above.

## 3. Liveness

The producer's MQTT client keepalive (the library's own ping) is what
detects a dead link at both ends: the broker drops a client whose keepalive
lapses, and the client reconnects. The producer needs no cache of what the
hub missed: current state is re-sent whole, by a full publish (§2), in both
reconnect cases:

- **the hub reconnects** to its broker: it requests a full publish itself;
- **the producer reconnects** to the broker: the hub's connection was never
  interrupted, so nothing asks. The producer therefore performs a full
  publish unasked, **once**, as soon as the first subscription announcement
  (§4.1) has arrived after a connect, filtered by the union at that moment
  and ending with `full_publish_completed`. **A producer connects only once
  its initial read of the GX is complete**, so that publish is whole: the
  hub takes `full_publish_completed` as its view of the boat being
  complete. The hub has more than one source and each announces separately,
  so a **later announcement that widens the union publishes the topics it
  newly made eligible** (their current values, no completion marker).
  Together these are the one time a producer publishes without being asked.

## 4. Two conventions of Wattknot's

Both are application conventions on top of MQTT, **not canonical MQTT**, and
both are removable: a producer that ignores the first publishes everything;
a hub that never sees the second treats the producer as writable, which is
how a hub talking to the GX's broker directly behaves.

### 4.1 The subscription announcement

The GX's broker delivers a client only what it subscribed to: in business as
usual the hub subscribes its mapped topics; while it is discovering a boat
it subscribes `#`. A producer must publish **only what the hub subscribed
to**, dumps included. With the broker on the hub, MQTT gives a publisher no
way to learn a consumer's subscriptions, so the hub announces them:

| | |
|---|---|
| Topic | `wattknot/hub/subscriptions/<source id>` |
| Payload | a JSON array of MQTT topic filters, sorted, e.g. `["#","N/102c6bf7d60f/battery/279/Soc"]` |
| Retained | yes; and re-published on every hub connect |
| When | every time the source subscribes or unsubscribes |

A producer subscribes to `wattknot/hub/subscriptions/+`, takes the **union**
of every array it holds, and publishes a topic only if it matches one of the
filters by MQTT's own wildcard rules (`+`, `#`). An announcement containing
`#` means publish everything. Until the first announcement arrives, publish
nothing but the retained `Serial`, which is published regardless.

### 4.2 The writable flag

Whether the producer will act on `W/` writes (§2) is the boat owner's
decision, taken on the boat, and the hub has to know it or it offers
controls that do nothing. So the producer says:

| | |
|---|---|
| Topic | `wattknot/producer/<portal id>/writable` |
| Payload | `true` or `false` (JSON) |
| Retained | yes; and re-published on every connect of the producer's |
| When | on every connect, after the `Serial`; the owner changing the switch restarts the producer, so the next connect carries the new value |

**Absent means writable**: the GX's own broker never publishes it, and a hub
connected to that broker writes as it always has. `false` makes the hub
show that source's switches as state, not buttons, and give the buttons
back the moment the flag turns.

## 5. Summary

| Thing | Value |
|---|---|
| Door | `/api/mqtt`, WebSocket binary, `Authorization: Bearer`, the N2K door's token |
| Inside | MQTT 3.1.1 to the hub's own broker, standard client |
| Victron topics | dbus-flashmq's: `N/<portal>/<type>/<instance>/<path>` `{"value":…}` |
| Retained | `N/<portal>/system/0/Serial` and `wattknot/producer/<portal>/writable`, nothing else |
| `R/` | keepalive with suppress → nothing · empty → full publish + `full_publish_completed` · one topic → republish it |
| `W/` | the boat's switch on → D-Bus set on a switch path, typed by the item's signature, no reply · otherwise, or a path that is not a switch, or a value that does not fit → refused and logged |
| Writable flag | `wattknot/producer/<portal>/writable` `true`/`false`, retained, every connect · absent = writable |
| Filter | union of `wattknot/hub/subscriptions/+`, MQTT wildcard rules, `#` = all |
| Liveness | the client library's MQTT keepalive; full publish on every reconnect |
