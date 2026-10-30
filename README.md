# ironflock-go

The Go SDK of the [IronFlock](https://www.ironflock.com) IoT DevOps platform — the Go counterpart of
[ironflock-py](https://github.com/IronFlock/sdk-py) and
[ironflock-js](https://github.com/IronFlock/sdk-js), with the same functionality.

## About

With this library you can publish data from your apps on your IoT edge hardware to the fleet data storage of
the IronFlock platform, read it back, react to it in realtime, call functions on other devices, read other
apps' shared data, and store files. When the library is used on a device registered in IronFlock, it uses the
private messaging realm of the device's fleet automatically, so the data collection is always private to the
app user's fleet.

## Requirements

- Go 1.27.2 or newer

## Installation

```shell
go get github.com/ironflock/sdk-go
```

### WAMP library

The SDK speaks WAMP through IronFlock's fork of [nexus](https://github.com/IronFlock/nexus)
(module `github.com/ironflock/nexus/v3`, branch `main`) — the code
the [IronFlock router](https://github.com/IronFlock/router) is built on. It is an ordinary
dependency: `go get` pulls the fork release this module pins (v3.4.2), and no `replace` is needed in your module.
`go install` and `go run` with a version (`pkg@latest`) work too.

## Usage

```go
package main

import (
	"context"
	"log"
	"time"

	ironflock "github.com/ironflock/sdk-go"
)

func main() {
	// Reads the identity and credentials the device agent injects into the app container.
	ifl, err := ironflock.New()
	if err != nil {
		log.Fatal(err)
	}

	// Run connects, runs main, and shuts down on return, SIGINT or SIGTERM.
	err = ifl.Run(context.Background(), func(ctx context.Context) error {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			row := ironflock.Row{"tsp": time.Now(), "temperature": 22.5}
			if err := ifl.PublishToTable(ctx, "sensordata", row); err != nil {
				log.Print(err)
			}
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
			}
		}
	})
	if err != nil {
		log.Fatal(err)
	}
}
```

More in [examples/](examples).

Every table has a mandatory `tsp` column, the row's timestamp: give every row you write one (a `time.Time` is
sent as RFC 3339 in UTC). The data backend refuses a row without it, and the write fails — see `AppendToTable` below.

Every network operation takes a `context.Context` and is safe to call from many goroutines. `Start` (and
`Run`) block until the app's realm is joined. Operations issued before that — by a goroutine started before
`Run`, say — wait for `Start` and then for the connection, within the time they wait for a connection anyway
(see [Connection reliability](#connection-reliability)); `Stop` ends that wait at once with `wamp.ErrStopped`.

### Arguments and keyword arguments

WAMP messages carry positional arguments and keyword arguments. Methods that send a payload (`Publish`,
`PublishToTable`, `AppendToTable`, `Call`, `CallDeviceFunction`) take positional arguments variadically; a
`Kwargs` value among them is sent as keyword arguments, like `sql.Named` in `database/sql`:

```go
ifl.PublishToTable(ctx, "sensordata", ironflock.Row{"tsp": now, "temperature": 22.5}) // args [{"tsp": "…", "temperature": 22.5}]
ifl.Call(ctx, "com.example.proc", 1, 2, ironflock.Kwargs{"scale": 10})                // args [1, 2], kwargs {"scale": 10}
```

A table write's row is its first positional argument: the data backend reads the columns from there, and from
the keyword arguments only where the table's data template maps a column to them (`path: kwargs.<key>`).

Values can be maps, slices and scalars, or structs. They are converted by the rules of `encoding/json` as of
Go 1.27 — struct fields named by their `json` tags, with `omitempty`, `omitzero`, `-`, `,string`, `,embed` and
embedded structs; map keys that are strings, numbers, or values with a `MarshalText` or `AppendText` method (a
map whose keys are no strings, such as `bool` or struct keys, can be sent only empty); and `MarshalJSONTo`,
`MarshalJSON`, `AppendText` and `MarshalText` methods used where `encoding/json` uses them (also on a nil
pointer in a field of type `json.Marshaler`, `encoding.TextAppender` or `encoding.TextMarshaler`; where the
method would panic, null is sent) — so the platform receives the shape Python and JavaScript apps send. Four
things differ from `encoding/json`, the first three as in the Python SDK:

- Floats stay floats: `2.0` is not sent as the integer `2`, and NaN and ±Inf are sent as they are.
- `[]byte` is sent as binary (msgpack bin, like Python `bytes`), not as a base64 string.
- Every `time.Time` becomes an RFC 3339 string in UTC (`"2026-01-02T03:04:05.123Z"`) wherever it is: a value
  or a map key, behind a pointer, in an interface, or embedded in a struct (which `encoding/json` encodes as
  that time). Map keys naming the same instant become one entry.
- A struct with an unexported embedded struct named by its tag (``inner `json:"x"` ``) whose marshal or
  unmarshal method, or `IsZero` with `omitzero`, cannot be called on it is refused with an error naming the
  field: `encoding/json` leaves such a field out, silently dropping its data.

As with `encoding/json`, a nil slice or map — `Row(nil)` included — is sent as null; use `[]any{}` or
`map[string]any{}` for an empty list or object.

```go
type Reading struct {
	Temperature float64   `json:"temperature"`
	Tsp         time.Time `json:"tsp"`
}
ifl.PublishToTable(ctx, "sensordata", Reading{Temperature: 22.5, Tsp: time.Now()})
```

Received payloads use `map[string]any`, `[]any`, `string`, `int64`, `float64`, `bool`, `[]byte` and `nil`
(`uint64` for integers above `math.MaxInt64`). A number arrives as `int64` when its sender encoded it as an
integer. The data backend does so for every whole number, so in table rows a numeric column holding 22 reads
as `int64(22)` and 22.5 as `float64`; timestamp columns, `tsp` included, arrive as epoch milliseconds.
`ironflock.Decode[T]`, `ironflock.DecodeRows[T]` and `Result.Decode` convert payloads into your own types
through `encoding/json`, so a `float64` field takes either kind of number. NaN and ±Inf decode as JSON null
does: a `float64` field keeps 0, a `*float64` field stays nil.

## Options

`ironflock.New` reads the environment the IronFlock device agent injects into every app container. Options
override it:

| Option | Environment variable | Description |
|--------|----------------------|-------------|
| `WithSerialNumber(s)` | `DEVICE_SERIAL_NUMBER` | Device serial number, sent as `DEVICE_SERIAL_NUMBER` in the metadata of publications and table writes. Required — `New` fails without it. It does not change the identity the connection authenticates with while the agent injects a per-app credential; use `WithCredentials` for that |
| `WithDeviceKey(k)` | `DEVICE_KEY` | Device key |
| `WithDeviceName(n)` | `DEVICE_NAME` | Device name |
| `WithAppName(n)` | `APP_NAME` | App name |
| `WithSwarmKey(k)` | `SWARM_KEY` | Swarm (fleet) key |
| `WithAppKey(k)` | `APP_KEY` | App key |
| `WithEnv(e)` | `ENV` | `PROD` (in any case) selects the production realm; anything else, unset included, the development realm |
| `WithURL(u)` | `DEVICE_ENDPOINT_URL` | Router WebSocket URL, e.g. `wss://cbw.ironflock.com/ws-ua-usr`. Without it the URL comes from `DEVICE_ENDPOINT_URL` (path replaced by `/ws-ua-usr`), then from the studio URL; with neither set it is `wss://cbw.ironflock.com/ws-ua-usr` |
| `WithReswarmURL(u)` | `RESWARM_URL` | Studio URL used to look up the router of a known IronFlock deployment. When the router URL comes from it, an unknown studio URL makes `Start` fail (`cannot resolve WebSocket URI…`): for another deployment, set `DEVICE_ENDPOINT_URL` or use `WithURL` |
| `WithCredentials(id, secret)` | `APP_AUTH_ID`, `APP_AUTH_SECRET` | WAMP-CRA credential. By default the per-app credential the agent injects is used (read from `/data/env` first, so a rotated credential is picked up on the next reconnect), falling back to the device serial number — the legacy credential, which [cross-app access](#cross-app-data-access) does not accept. `New` fails with `ErrInvalidArgument` when only one of the two is given |
| `WithReconnectWindow(d)` | | How long a table operation waits for the connection — after a platform restart, or before `Start` — and retries while the data backend comes back, before it fails (default 60 s, `DefaultReconnectWindow`). The table writes (`PublishToTable` included: it appends) and the reads are such operations, and so is the catch-up read of a table subscription. `Publish`, `Subscribe`, `SubscribeToTable` and `RegisterDeviceFunction` wait as long for the connection, and are not retried. 0 turns the window off: these operations then behave exactly like all other operations — 10 s for the connection, before `Start` as well, and no retries. A negative `d` makes `New` fail with `ErrInvalidArgument`. See [Connection reliability](#connection-reliability) |
| `WithLogger(l)` | | `*slog.Logger` for connection lifecycle logs (default `slog.Default()`) |

Missing `DEVICE_KEY`, `APP_NAME`, `SWARM_KEY` or `APP_KEY` are logged as a warning; the operations that need
them fail with an error wrapping `ironflock.ErrMissingConfig`. Without `SWARM_KEY` or `APP_KEY`, `Start` fails
at once: the app's realm could never be joined.

Only the per-app credential and the remote-access ports are read live, from `/data/env` (or the directory
`IRONFLOCK_ENV_DIR` names); every other value comes from the environment the app started with. The agent keeps
`/data/env` up to date while the app runs. Older agents make it readable by root only: in a container there that
runs as another user the SDK sees the values the container started with — no rotated credential, no changed
remote-access port — and logs a warning once.

## API Reference

### Lifecycle

| Method | Description |
|--------|-------------|
| `New(opts ...Option) (*IronFlock, error)` | Creates an instance from the environment and options; does not connect |
| `Start(ctx) error` | Connects and blocks until the app's realm is joined. A realm that does not exist yet (the data backend is still being provisioned) is waited for. A `Start` that failed (`ctx` ended first, say) may be called again. On a started or starting instance it fails with `ErrAlreadyStarted`, after `Stop` with `wamp.ErrStopped` |
| `Stop(ctx) error` | Closes every consumed-app connection and the connection itself, within `ctx`. Every operation still waiting — for `Start`, for the connection, or between the retries of a table call — fails at once with `wamp.ErrStopped`. Idempotent and final: operations issued afterwards fail with `wamp.ErrStopped` too, and `Run` returns once `main` does |
| `Run(ctx, main func(ctx) error) error` | `Start`, run `main` (or wait), `Stop` (within 10 s). `main`'s context is cancelled when `ctx` ends, on SIGINT or SIGTERM, and by `Stop` from anywhere; once shutdown has begun, a second signal terminates the process. Returns `main`'s error, or the `Start` error. On a started instance it fails with `ErrAlreadyStarted` and leaves it running |
| `IsConnected() bool` | Whether the connection is currently established |
| `Files() *filestore.FileStore` | The app's managed object storage — see [Managed File Storage](#managed-file-storage) |
| `Connection() *wamp.Connection` | The underlying connection, for advanced use — see [Advanced usage](#advanced-usage) |
| `Stage()` | The stage of the app's realm: `StageDevelopment` (`"DEV"`) or `StageProduction` (`"PROD"`). The cross-app API writes stages in lower case (`"dev"`, `"prod"`; `Stage.Lower()`) |
| `SerialNumber()`, `DeviceKey()`, `DeviceName()`, `AppName()`, `SwarmKey()`, `AppKey()` | The resolved identity |

### `Publish(ctx, topic, args ...any) error`

Publishes an event to a topic. The publish is acknowledged: a refusal by the router is returned as an error.
It waits up to the reconnect window (60 s by default, see `WithReconnectWindow`) for a connection, so a publish
rides out a short outage, and is then sent once, never retried. The router acknowledges an event nobody
subscribes to: subscribers that are down meanwhile miss it. The device metadata `DEVICE_SERIAL_NUMBER`, `DEVICE_KEY` and `DEVICE_NAME` is added to the keyword arguments
(your own keys win). `Publish` takes no publish options: a `wamp.PublishOptions` value among the arguments is
sent as a positional argument. `ifl.Connection().Publish` takes them (see [Advanced usage](#advanced-usage)).

```go
err := ifl.Publish(ctx, "com.myapp.mytopic", map[string]any{"temperature": 20})
```

### `PublishToTable(ctx, table, args ...any) error`

Writes a row to a fleet table. It is `AppendToTable` without the insert outcome: it calls the table's append
procedure `append.<SWARM_KEY>.<APP_KEY>.<table>`, with the same arguments, so it rides out a platform restart —
it waits up to the reconnect window for the connection and retries while the data backend does not serve the
table yet — and it returns an error when the data backend refuses the row (the message begins
`Publish to table '<table>'`). Read rows back with `GetHistory` or `SubscribeToTable`.

```go
err := ifl.PublishToTable(ctx, "sensordata", ironflock.Row{"tsp": time.Now(), "temperature": 22.5, "humidity": 60})
```

The row needs a `tsp`; the refusals are those of `AppendToTable`.

> ironflock-go 1.9.1 and earlier published the row to the topic `<SWARM_KEY>.<APP_KEY>.<table>` instead: a row
> published while the data backend restarted was lost, and a refused row was dropped without an error. Now such
> a row fails — a row without `tsp`, or a table missing from the data template (after the reconnect window).

On a table with an entity key (`maintainLatestFlagFor` in the data template), a row may carry just the entity key,
`tsp` and the columns that change: a column the row leaves out keeps the entity's previous value, and a `nil` value
(in an `ironflock.Row`, or a `nil` pointer field) is sent as null and clears the column. A struct field tagged
`omitempty` or `omitzero` is left out when it is empty, so it keeps the old value instead of clearing it. This
holds for all four table write methods. On a table without an entity key, a left-out column is stored as `NULL`.

The four table write methods take a table name of letters, digits, `-` and `_`, at most 100 characters; any
other name fails with `ErrInvalidArgument` before anything is sent.

### `AppendToTable(ctx, table, args ...any) (*Result, error)`

Appends a row by calling the table's append procedure `append.<SWARM_KEY>.<APP_KEY>.<table>` and returns the
insert outcome. A refused row is an error. `PublishToTable` does the same. A row without `tsp` is refused with `wamp.error.runtime_error` and
no reason, which the error message then points out; the typed refusals are listed under
[Data backend refusals](#data-backend-refusals) — `URIStorageFull`, for one, is temporary: append the row later.

### `PublishRowsToTable(ctx, table, rows any, kwargs ...Kwargs) error`

Writes **many rows in a single message** (bulk insert). The platform inserts the batch atomically
(all-or-nothing). It is `AppendRowsToTable` without the outcome: it calls the bulk procedure
`appendBulk.<SWARM_KEY>.<APP_KEY>.<table>`, rides out a platform restart as `PublishToTable` does, and returns an
error when the data backend refuses the batch (the message begins `Bulk publish of <n> row(s) to table
'<table>'`). (ironflock-go 1.9.1 and earlier published the batch to the topic
`bulk.<SWARM_KEY>.<APP_KEY>.<table>`, where a refused batch was dropped without an error.) `rows` is a non-empty
slice of rows (`[]Row`, `[]map[string]any`, or a slice of structs), each encoded exactly as `PublishToTable`
encodes a row and carrying its own `tsp`. (A row is taken as a value, as by
`PublishToTable`: a `MarshalJSON`, `MarshalText` or other marshal method with a pointer receiver is used for
`[]*T` only.)

```go
err := ifl.PublishRowsToTable(ctx, "sensordata", []ironflock.Row{
	{"tsp": "2024-01-15T10:30:00.000Z", "temperature": 22.5},
	{"tsp": "2024-01-15T10:30:01.000Z", "temperature": 22.7},
})
```

### `AppendRowsToTable(ctx, table, rows any, kwargs ...Kwargs) (*Result, error)`

Appends many rows in a single call to `appendBulk.<SWARM_KEY>.<APP_KEY>.<table>` and returns the outcome, e.g.
`{"success": true, "count": 2}`. If any row is invalid the entire batch is rejected, with the refusals of
`AppendToTable`. `PublishRowsToTable` does the same.

### `ReportError(ctx, errOrMsg any, opts ...ReportErrorOptions) (*Result, error)`

Reports an application error into the fleet's `error-logs` table (`ironflock.ErrorLogsTable`), stamped
`source: "app"`, a level and a timestamp. It is queryable with `GetHistory`, streamed on
`transformed.error-logs`, usable in board templates, and does not fire the platform's system-error toast. An
`error` is recorded with `%+v`, so errors that carry a stack trace include it. The levels are `LevelError` (the
default), `LevelWarning`, `LevelInfo` and `LevelDebug`, the types the board's toast widget shows. `LevelWarn`,
and the level `"warn"` that ironflock-go 1.9.1 and earlier sent, go out as `"warning"`.
`ReportErrorOptions.Tsp` sets the timestamp, a string (default: now, RFC 3339 in UTC with milliseconds, as in
`"2026-01-02T03:04:05.123Z"`). The row is appended as `AppendToTable` appends it, riding out a platform restart,
and the insert outcome returned. `ReportErrorOptions.Append` is kept for compatibility: the row is appended
either way, and it only picks which operation the error message names (`Append to table 'error-logs'` with it,
`Publish to table 'error-logs'` without). ironflock-go 1.9.1 and earlier published the row without `Append`: a
row reported while the data backend restarted was lost.

```go
ifl.ReportError(ctx, "Sensor read timed out", ironflock.ReportErrorOptions{Level: ironflock.LevelWarning})
ifl.ReportError(ctx, err)
res, err := ifl.ReportError(ctx, "Calibration failed", ironflock.ReportErrorOptions{
	UserMessage: "Please recalibrate", // operator-facing text (default: the message)
})
```

### `Subscribe(ctx, topic, handler, opts ...SubscribeOptions) (*Subscription, error)`

Subscribes to a topic. The subscription is restored automatically after every reconnect. It waits up to the
reconnect window (see `WithReconnectWindow`) for a connection, so a short outage does not make it fail, and is
not retried.

```go
sub, err := ifl.Subscribe(ctx, "com.myapp.mytopic", func(ev *ironflock.Event) {
	log.Println(ev.Args, ev.Kwargs)
})
defer ifl.Unsubscribe(ctx, sub)
```

Each subscription delivers its events in order, one at a time, on its own goroutine, so a handler may call any
other method, including `Call`. A slow handler delays only its own subscription. Subscriptions that must not
run their handlers concurrently share a delivery group: pass the same `wamp.NewDeliveryGroup()` as
`SubscribeOptions.Group` to each, and they deliver one event at a time across all of them, in the order the
events arrive. `Unsubscribe` takes effect at once: the handler is called for no further event, and it does not
wait for a connection. Use your own topic names — see [URIs an app may use](#uris-an-app-may-use).
`SubscribeOptions{Match: "prefix"}` subscribes to a prefix.

`sub` is a handle (see [Handles](#handles)): `sub.Unsubscribe(ctx)` removes it as `ifl.Unsubscribe(ctx, sub)`
does, but returns a refusal as the unwrapped `*wamp.Error`, which `ifl.Unsubscribe` wraps in an
`*OperationError`. `sub.Topic()` and `sub.Match()` return the topic and the match policy (`""` for exact), and
`sub.Active()` reports whether the subscription is live at the router — false while the connection is down,
after a failed restore until a retry succeeds, and after `Unsubscribe`. Every `Subscribe` returns a handle of
its own, also on a topic subscribed already.

### `SubscribeToTable(ctx, table, handler, opts ...SubscribeOptions) (*TableSubscription, error)`

Subscribes to the stored rows of a table: the data backend's realtime feed `transformed.<table>` and its bulk
counterpart `transformed.bulk.<table>`. Each event carries one row as stored — values typed to the
data-template columns, secret columns masked — in `ev.Args[0]` (`ev.Row()`). Rows of a bulk insert arrive as
one event per row, so handler code stays the same. The handler is called one event at a time, in the order the
events arrive on both feeds: it never runs concurrently with itself. The returned `*TableSubscription` is a
handle (see [Handles](#handles)): its `Unsubscribe`, or `ifl.Unsubscribe(ctx, ts)`, removes both subscriptions —
no handler call starts after it — `ts.Table()` names the table, and `ts.Active()` reports whether both feeds are
live at the router. Its fields `Rows` and `Bulk` are the two `*Subscription`s; remove them only through the
handle's `Unsubscribe`, which also ends the catch-up below.

**Rows stored while the connection was down arrive after the reconnect.** The router does not replay what was
published while the connection was down, so after every reconnect the SDK reads the rows stored meanwhile and
delivers them to your handler, oldest first, before the rows that arrive live: every row whose `tsp` is later
than the newest row your handler got. Before the first row, the start is the newest row stored when you
subscribed: the SDK reads it right after subscribing, and when that read fails — or is answered only after the
connection dropped and came back, when the newest row may be one stored while it was down — it starts at the
time of subscribing (by the device's clock); a table that was empty then is read from its start. A
`SubscribeToTable` issued while there is no connection — before the first join, or during a reconnect — waits
for it (up to the reconnect window, as `Subscribe` does) and subscribes then, so the rows stored before are not
caught up on. A row written
meanwhile with an older `tsp` (a device sending readings it buffered, say) is not caught up. One catch-up
delivers at most the newest 10000 rows, and logs a warning when it read that many. The catch-up read waits for
the data backend like any history read, up to the reconnect window, and the live rows wait for it; when it
fails, the rows stored meanwhile are not delivered, and a warning is logged. Each caught-up row reaches the
handler as an `Event` with `Topic` (`transformed.<table>`) and `Args` only (no `Kwargs` or `Details`), in turn
on the subscription's `DeliveryGroup` (the one in `SubscribeOptions.Group`, if set); a panic is recovered and
logged while the catch-up goes on. Warnings go to the logger of `WithLogger`.

A transform (a data template's SQL view) has no rows of its own: on every tick of its schedule the data
backend publishes the whole view — at most 3000 rows — as one event. Read it with `ev.Rows()`; `ev.Row()` is
nil for it. A transform needs no catch-up: its next tick after a reconnect publishes the whole view again.

```go
ts, err := ifl.SubscribeToTable(ctx, "sensordata", func(ev *ironflock.Event) {
	row := ev.Row()
	log.Println(row["temperature"])
})
```

### `GetHistory(ctx, table, q *TableQueryParams) ([]Row, error)`

Reads rows of a table or transform (`history.transformed.<table>`): the newest rows that match, in ascending
`tsp` order. `nil` reads the 10 newest rows. A result too large for one message of the data backend (8 MiB)
arrives in chunks, which `GetHistory` reassembles in memory into the rows of a single read; `ctx` must cover
the whole transfer.

```go
rows, err := ifl.GetHistory(ctx, "sensordata", &ironflock.TableQueryParams{
	Limit:     500,
	TimeRange: ironflock.Between(start, end),
	FilterAnd: []ironflock.Filter{
		ironflock.Where("temperature", ">", 20),
		ironflock.Where("humidity", "<=", 80),
	},
	Columns: []string{"temperature", "humidity"},
})

// Current value(s) only: the data backend derives the latest row per entity
// (the table's maintainLatestFlagFor columns; without one, the most recent row).
current, err := ifl.GetHistory(ctx, "sensordata", &ironflock.TableQueryParams{
	Limit:     100,
	FilterAnd: []ironflock.Filter{ironflock.Latest()},
})

// OR groups: entries of FilterAnd are AND-ed; a group combines its own entries.
live, err := ifl.GetHistory(ctx, "parts", &ironflock.TableQueryParams{
	Limit:     100,
	FilterAnd: []ironflock.Filter{ironflock.Or(ironflock.IsNull("deleted"), ironflock.Where("deleted", "=", false))},
})
```

| Field | Description |
|-------|-------------|
| `Limit` | Maximum number of rows, 1–10000 (`ironflock.MaxQueryLimit`; 1–3000 for a transform) (required) |
| `Offset` | Rows to skip |
| `TimeRange` | `&TimeRange{Start, End}`: the rows with `Start <= tsp < End`. Each bound a `time.Time`, an ISO 8601 string, an epoch-milliseconds number, or `nil` for an open end (`Between`, `Since`, `Until` build one from `time.Time`). Strings and times cannot be mixed with numbers |
| `FilterAnd` | AND-ed filters: `Where(column, operator, value)`, `IsNull`, `IsNotNull`, the `Latest()` marker, `Or(...)`/`And(...)` groups |
| `Columns` | Columns to return (`tsp`, `device_key` and `authid` are always included); `nil` for all |
| `ColumnPaths` | Prunes `json` columns: JSON paths in the notation filters use (`"json_data.a.b"`). A `json` column listed in `Columns` comes back holding only its paths (fleetdb v1.0.52 and later) |

Table and secret reads currently use both bounds truncated to the whole second, and ignore a bound of exactly
1970-01-01T00:00:00Z (epoch 0); series reads use them to the millisecond. A `time.Time` bound is sent as
RFC 3339 in UTC, a number as epoch milliseconds (NaN and ±Inf are refused). A string bound is sent exactly as
given, and the data backend reads it with JavaScript's `Date`, so the SDK accepts the extended ISO 8601 forms
`Date` reads correctly: a date (`2026`, `2026-07`, `2026-07-01`), or a date and a time of day
(`2026-07-01T12:30`, `2026-07-01T12:30:15`, `2026-07-01T12:30:15.250`; `T`, `t` or a space between them) with
an optional `Z` or UTC offset (`+02:00` or `+0200`). A time without either is read as UTC. Other strings —
basic format (`20260701T123000Z`), hours without minutes, offsets of hours only, days a month does not have
(`2026-02-30`), a space before the time in the years 0–99 (`Date` reads `0001-01-01 00:00:00` as 2001) — fail
with `ErrInvalidArgument` before anything is sent.

Operators (`ironflock.SQLOperators()` returns them): `=`, `!=`, `<>`, `>`, `<`, `>=`, `<=`, `LIKE`, `ILIKE`,
`NOT LIKE`, `NOT ILIKE`, `IN`, `NOT IN`, `IS NULL`, `IS NOT NULL`, in any case: the SDK sends them in this
form, the only one the data backend accepts (an unknown operator is sent as given, with a warning). A
comparison takes a string, number or bool — a value that encodes as a string, such as a `uuid.UUID`, is one.
`IN`/`NOT IN` take a slice of those (or a comma-joined string); a nil or empty slice is the empty set: `IN`
matches no row, `NOT IN` every row. `IS NULL` and `IS NOT NULL` take no value and are the only way to ask about
NULL. Columns your data template declares `secret: true` come back as `ironflock.SecretPlaceholder`; a filter on
one is refused (`URISecretColumn`).

A transform (a data template's SQL view) applies `Limit`, `Offset` and `FilterAnd` only: it ignores
`TimeRange`, `Columns`, `ColumnPaths` and `Latest()`, and drops a filter on a column the view does not declare
(it has no implicit `tsp`, `device_key` or `authid`), which widens the result. Its rows come in the view's own
order, reversed. Bound a transform's time range in its SQL.

`ironflock.DecodeRows[T](rows)` converts rows into a slice of your own struct type (decode `tsp` into an
`int64`: it arrives as epoch milliseconds).

### `GetSeriesHistory(ctx, table, q SeriesQueryParams) ([]Row, error)`

Reads a table's history **down-sampled into time buckets** — columns aggregated per bucket — ideal for charts
over long ranges (`history.transformed.series.<table>`; tables only, not transforms; data backends from
fleetdb v1.0.58 on).

```go
now := time.Now()
avg := ironflock.SeriesMetric{Ref: "temperature", Method: ironflock.MethodAvg} // AVG, SUM, COUNT, MIN, MAX, FIRST, LAST
series, err := ifl.GetSeriesHistory(ctx, "sensordata", ironflock.SeriesQueryParams{
	Metrics:   []ironflock.SeriesMetric{avg, {Ref: "humidity", Method: ironflock.MethodMax}},
	Bucket:    time.Hour,                                      // 0: the range divided into Limit buckets
	Limit:     24,                                             // the bucket budget, 1–10000
	TimeRange: ironflock.Between(now.Add(-24*time.Hour), now), // required, with a start
	GroupBy:   []string{"device_key"},
})
for _, row := range series {
	log.Println(row["tsp"], row["device_key"], row[avg.Column()]) // avg.Column() is "AVG:temperature"
}
```

Each row is one bucket — of one group, with `GroupBy`: `tsp` is the bucket's start in epoch milliseconds, each
`GroupBy` column comes back under its own name, and each metric under `SeriesMetric.Column()`,
`"<METHOD>:<ref>"`. The rows ascend by `tsp`; a bucket without rows is absent. A `Ref` can also be a JSON path
into a `json` column (`"json_data.temp"`). AVG and SUM take numeric columns, MIN and MAX strings and
timestamps too; FIRST and LAST give the value of the bucket's earliest and latest row; COUNT counts the values
that are not null (of `tsp`: the rows).

`Limit` counts buckets, not rows. Without a `Bucket` the time range is divided into `Limit` buckets. With one
(at least 1 s, in whole milliseconds), the data backend widens the buckets to a multiple of it when the range
holds more than `Limit` of them — and an open end reaches up to the data backend's now, a little after yours:
`Since(now.Add(-24*time.Hour))` holds 25 hourly buckets, so `Limit: 24` gets two-hour ones. Bound the end, or
allow a bucket more. `FilterAnd` takes predicates and groups; the `Latest()` marker is not supported here. The
data backend refuses what it cannot answer with `URIInvalidTimeRange`, `URIInvalidMetric`, `URIInvalidGroupBy`
and `URISeriesTooManyGroups` (more than 50,000 rows).

### `Call(ctx, topic, args ...any) (*Result, error)`

Calls a procedure by its full WAMP URI. A `CallOptions` value among the arguments configures the call: a
`Timeout` for the callee, `DiscloseMe`, `OnProgress`, which receives the callee's progressive results in
order (without it, a progressive result fails the call), and `Extra`, further WAMP CALL options sent as they
are (`receive_progress` there without `OnProgress` fails the call before it is sent). When `ctx` ends, `Call`
returns at once with an error wrapping `ctx.Err()`, and the router is told to cancel the call in the
background. An app cannot register names like `com.myapp.proc`, so this reaches procedures that platform
services register on the app's realm; to call a function another device of your app registered, use
`CallDeviceFunction`.

### `CallDeviceFunction(ctx, deviceKey int, topic, args ...any) (*Result, error)`

Calls a function registered by another device of this app with `RegisterDeviceFunction`. The full URI is
`<SWARM_KEY>.<deviceKey>.<APP_KEY>.<STAGE>.<topic>`, with `STAGE` `DEV` or `PROD`. `CallFunction` is its
deprecated former name.

```go
res, err := ifl.CallDeviceFunction(ctx, 42, "com.myapp.add", 1, 2)
sum := res.Value() // int64(3)
```

### `RegisterDeviceFunction(ctx, topic, handler, opts ...RegisterOptions) (*Registration, error)`

Registers a function other devices of this app — and dashboard widget actions — can call, as
`<SWARM_KEY>.<DEVICE_KEY>.<APP_KEY>.<STAGE>.<topic>`. The registration is restored after every reconnect. It
waits up to the reconnect window for a connection, as `Subscribe` does, and is not retried. `Register` is an
alias; `RegisterFunction` is the deprecated former name.

```go
_, err := ifl.RegisterDeviceFunction(ctx, "com.myapp.add", func(ctx context.Context, inv *ironflock.Invocation) (any, error) {
	if len(inv.Args) != 2 {
		return nil, errors.New("add takes two numbers")
	}
	a, ok1 := inv.Args[0].(int64)
	b, ok2 := inv.Args[1].(int64)
	if !ok1 || !ok2 {
		return nil, fmt.Errorf("add takes two integers, got %T and %T", inv.Args[0], inv.Args[1])
	}
	return a + b, nil
})
```

The returned value becomes the result (`*Result` for several values or keyword results), converted like any
payload (see [Arguments and keyword arguments](#arguments-and-keyword-arguments)); a returned `*WampError` is
sent with its URI and converted payload, any other error as `wamp.error.runtime_error` with its text. A result
that cannot be converted (a channel, a function) is answered with `wamp.error.runtime_error` and logged. When
the router discloses the caller, `inv.Details["caller_authid"]` is the calling device's serial number, whichever
credential it used.

`RegisterOptions` sets `Match` (`"prefix"` or `"wildcard"`), `Invoke` (the router accepts only single
registrations: leave it empty), `ForceReregister` (default true: the registration takes the procedure over from
a previous session of the app, after a crash, say) and `Extra`, further WAMP REGISTER options sent as they are.
A procedure can be registered once per connection: registering it again before it is unregistered fails with
`wamp.error.procedure_already_exists`. `reg` is a handle (see [Handles](#handles)): `ifl.Unregister(ctx, reg)`,
or `reg.Unregister(ctx)`, removes the registration at once, without waiting for a connection: the handler is
called no more, and it is not restored again. (`reg.Unregister` returns a refusal as the unwrapped
`*wamp.Error`; `ifl.Unregister` wraps it in an `*OperationError`.) `reg.Procedure()` returns the full URI,
and `reg.Active()` reports whether the registration is live at the router — false while the connection is
down, after a failed restore until a retry succeeds, once another session has taken the procedure over, and
after `Unregister`.

### Handles

`Subscribe`, `SubscribeToTable` and `RegisterDeviceFunction` return a handle on what they made:

| Handle | Returned by | Methods |
|--------|-------------|---------|
| `*Subscription` | `Subscribe` | `Unsubscribe(ctx) error`, `Topic() string`, `Match() string`, `Active() bool` |
| `*TableSubscription` | `SubscribeToTable`, also of a consumed app (`ConsumedApp.SubscribeToTable`) | `Unsubscribe(ctx) error`, `Table() string`, `Active() bool`; fields `Rows` and `Bulk`, the `*Subscription`s of its two feeds |
| `*Registration` | `RegisterDeviceFunction` and its aliases `Register` and `RegisterFunction` | `Unregister(ctx) error`, `Procedure() string`, `Active() bool` |

`ifl.Unsubscribe(ctx, handle)` takes a `*Subscription` or a `*TableSubscription`, of the app's own tables or of
a consumed app's (both satisfy `ironflock.Unsubscriber`), and `ifl.Unregister(ctx, reg)` takes a
`*Registration`. Each removes the handle as the handle's own method does, except for the error: the `IronFlock`
methods wrap a refusal in an `*OperationError`, while `Subscription.Unsubscribe` and `Registration.Unregister`
return the unwrapped `*wamp.Error` (see below). A nil handle is a no-op. The rules:

- **At once.** The handler is called for no further event — not for one still queued, not for the remaining
  rows of a bulk event, not for the rows a catch-up would deliver — though a call already under way is not
  waited for, so a handler may remove its own subscription. What was removed is never restored after a
  reconnect.
- **No wait for the connection.** While the connection is down, the subscription or registration is removed
  locally and the call returns at once: the router dropped the session's subscriptions and registrations with
  the session. While connected, the call returns once the router has answered; a refusal is returned — as an
  `*OperationError` from the `IronFlock` methods and `TableSubscription.Unsubscribe`, as a `*wamp.Error` from
  `Subscription.Unsubscribe` and `Registration.Unregister` — and the local removal stands.
- **Idempotent.** A second call is a no-op, and so is a call after `Stop` (or after a consumed app's `Close`).
- **Independent.** Two handles on one topic or table are independent: removing one leaves the other's handler
  subscribed, and the router's subscription of a topic ends with its last handler. A `TableSubscription`'s
  `Unsubscribe` removes both of its feeds and ends its catch-up.
- **`Active()`** is true while the subscription or registration is live at the router: false while the
  connection is down, after a failed restore until a retry succeeds, and after `Unsubscribe` or `Unregister`. A
  `TableSubscription` is active while both of its feeds are.

```go
ts, err := ifl.SubscribeToTable(ctx, "sensordata", handler)
if err != nil {
	return err
}
log.Printf("following %s (live: %v)", ts.Table(), ts.Active())
// ...
if err := ifl.Unsubscribe(ctx, ts); err != nil { // or ts.Unsubscribe(ctx)
	log.Print(err)
}
```

### `SetDeviceLocation(ctx, long, lat float64) (*Result, error)`

Asks the platform to update the device's location. **Not available at the moment:** no platform service
answers it on the app's realm yet, so it fails with `wamp.error.no_such_procedure`.

### `GetRemoteAccessURLForPort(port int, protocol, remotePortEnvironment string) (string, bool)`

The public URL under which a port declared in `port-template.yml` is reachable once its tunnel is active,
e.g. `https://<device_key>-<app_name>-8080.app.ironflock.com` (the app name in lower case; the label of an
`https` port starts with `secure-`). `protocol` is `http` (default when empty), `https`, `tcp` or `udp`. On a
device of an instance (`INSTANCE_KEY` set) an http(s) URL is the route through the cloud tunnel edge,
`https://i<INSTANCE_KEY>-<label>.<CLOUD_TUNNEL_DOMAIN>`. The result is `false` when no URL can be composed: an
unknown protocol, no device key or app name, a tcp/udp port without a public port, or a label over 63
characters or with a dot (an app name with a dot cannot be tunneled). The port variables are read live from
`/data/env`, so call it again rather than caching the result; `TUNNEL_DOMAIN`, `CLOUD_TUNNEL_DOMAIN` and
`INSTANCE_KEY` come from the environment the app started with.

A tcp/udp URL carries the public port the tunnel assigned, which the device agent announces as
`REMOTE_PORT_FOR_<port>` (agent 0.19.8 and later); a non-empty `remotePortEnvironment` names the variable to read
instead — the template's own `remote_port_environment`, which older agents need. On an instance device the
internet-facing port, the variable's `_CLOUD` companion, is preferred while the agent announces it; once it is
gone (cloud forwarding turned off), the instance-local URL is returned again.

The agent tunnels the ports of PROD installs only: in a DEV container the http(s) URL is that of the PROD
install of the same app, and tcp/udp give none. On an appliance installed without a domain (plain mode) the
agent sets the tunnel domain to `localhost`. The platform does not set `CLOUD_TUNNEL_DOMAIN`: an instance's
URLs use `app.ironflock.com` unless the app sets that variable itself.

## Secret Columns

A data-template column declared `secret: true` is encrypted by the data backend before it is stored. No read
path hands the plaintext back by accident: `GetHistory`, `SubscribeToTable` and cross-app reads return
`ironflock.SecretPlaceholder` (`"__secret__"`) for a non-null value. The two procedures below are callable
only by your app's own containers. The data backend registers them only for tables that declare a secret
column: on any other table the call is retried as a procedure not registered yet, and fails with
`wamp.error.no_such_procedure` once the reconnect window is up. The error names the causes, the most likely
first: the table declares no secret column (`secret: true` in the data-template), the table is not declared,
or the app's data backend predates secret columns.

### `RevealSecrets(ctx, table, q *TableQueryParams) ([]Row, error)`

Reads rows with the secret columns decrypted, newest first (unlike `GetHistory`). `nil` reads the 10 most
recent rows; `Limit` must be 1–100 (`ironflock.MaxSecretLimit`). Select rows by their non-secret columns —
values are encrypted with a random IV, so the data backend refuses a filter on a secret column
(`URISecretColumn`). It answers at most 30 reveals a minute per app credential on a device (`URIRateLimited`):
reveal a secret once and keep it, rather than on every use.

```go
rows, err := ifl.RevealSecrets(ctx, "credentials", &ironflock.TableQueryParams{
	Limit: 1, FilterAnd: []ironflock.Filter{ironflock.Latest()},
})
```

### `VerifySecret(ctx, table, column, candidate string, q *TableQueryParams) (*SecretVerifyResult, error)`

Checks a candidate against a stored secret without reading it back; the comparison runs in constant time
inside the data backend. `nil` checks the most recent row; `Limit` must be 1–100, as for `RevealSecrets`.
`Match` is true when any selected row matches; an unexpected response never reads as a match. The data backend
answers at most 120 checks a minute per app credential on a device (`URIRateLimited`) — for a check on every
request, cache a positive outcome briefly — and refuses a column that is not a secret column
(`URINotASecretColumn`).

```go
res, err := ifl.VerifySecret(ctx, "credentials", "api_key", receivedKey, nil)
if err == nil && res.Match { /* ... */ }
```

## Cross-App Data Access

Read another app's fleet data, in the same project and fleet. Your app must declare the provider app in its
data-template `consumes:` section (or hold the wildcard grant, `consumes: [{app: "*"}]`), and the project user
must grant access. Access is **read-only**. The connection needs the per-app credential the device agent
injects (`APP_AUTH_ID`, `APP_AUTH_SECRET`). The platform refuses the legacy device credential on another app's
realm (`NOT_AUTHORIZED`, with a message that says so) — unless the provider app runs on the same device, whose
realm it then joins with that app's own full rights rather than read-only.

```yaml
# .ironflock/data-template.yml of your app
consumes:
  - app: weather-app
```

```go
weather, err := ifl.ConnectToApp(ctx, "weather-app")
var cerr *ironflock.CrossAppAccessError
if errors.As(err, &cerr) {
	log.Fatal(cerr.Code) // e.g. NO_GRANT
}

for _, t := range weather.Tables {
	log.Println(t.Tablename)
}
rows, err := weather.GetHistory(ctx, "forecasts", &ironflock.TableQueryParams{Limit: 100})
_, err = weather.SubscribeToTable(ctx, "forecasts", func(ev *ironflock.Event) { log.Println(ev.Row()) })
```

| Method | Description |
|--------|-------------|
| `ConnectToApp(ctx, app, opts ...ConnectToAppOptions) (*ConsumedApp, error)` | Opens (or returns the cached) read-only handle. `Stage` selects `"dev"`/`"prod"` (default: this app's stage); `OnError` is called when an open handle is later denied for good (e.g. the grant was revoked) |
| `ListConsumableApps(ctx) ([]ConsumedAppInfo, error)` | Lists every non-private provider and its catalog without connecting (wildcard consumers) |
| `ConnectToAllApps(ctx, opts ...ConnectToAllAppsOptions) ([]*ConsumedApp, error)` | Opens every provider with a data backend for the stage (wildcard consumers). Failures go to `OnError` unless `StopOnError` is set |
| `(*ConsumedApp).GetHistory`, `GetSeriesHistory`, `SubscribeToTable` | As for your own tables (series: tables only). `SubscribeToTable` delivers bulk-inserted rows one at a time and, after a reconnect, the rows stored while the connection was down, exactly like `SubscribeToTable` on your own app |
| `(*ConsumedApp).IsConnected()`, `Connection()` | Whether the connection to the provider is open; the underlying `*wamp.Connection` |
| `(*ConsumedApp).Close(ctx)` | Closes the handle; `Stop` closes all of them |

`ConsumedApp.Tables` and `ConsumedApp.Transforms` are what the provider shares on the handle's stage: `TableInfo`
values with `Tablename`, `Description` and `Columns`, the column definitions of the provider's data template
(`{"id": "temperature", "dataType": "numeric"}`, say). `ListConsumableApps` returns a `ConsumedAppInfo` per
provider — `App`, `ProviderAppKey` and `Stages` — whose `Catalog("dev")` or `Catalog("prod")` is the
`*StageCatalog` (`Tables`, `Transforms`) of that stage, nil where the provider has no data backend;
`ConnectToAllApps` skips those.

Handles are cached per app and stage, and concurrent opens share one attempt; `ctx` bounds only the caller's
wait. An attempt that cannot join the provider's realm within 50 s fails with an error wrapping
`wamp.ErrNotConnected`. App names match in any case; `ConsumedApp.App` keeps the spelling of the call that
opened the handle. `ConsumedApp.Stage` is the stage in lower case (`"dev"` or `"prod"`), unlike
`IronFlock.Stage()`. `Stop` aborts opens still in flight: they fail with an error wrapping `wamp.ErrStopped`.
Errors are `*CrossAppAccessError` with a `Code`: `NO_GRANT`, `PROVIDER_NOT_INSTALLED`, `UNKNOWN_APP`,
`PRIVATE_TABLE` (not in the provider's shared catalog), `SECRET_COLUMN` (a query filters on or selects a column
the provider marks secret — including inside filter groups), `NOT_AUTHORIZED` (the provider's realm refuses
the connection, a read or a table subscription) — the constants `CodeNoGrant`, `CodeProviderNotInstalled`,
`CodeUnknownApp`, `CodePrivateTable`, `CodeSecretColumn` and `CodeNotAuthorized`.

A denial while `ConnectToApp` waits is returned at once. Once a handle is open, a denial counts as final only
when it persists — 3 refusals or more since the connection was last up (the router closing the session for an
auth reason counts as one, as does each refused reconnect), the first at least 60 s ago — because the platform
refuses the same way while it briefly cannot verify access. Until then the handle keeps reconnecting, and
recovers if a reconnect succeeds. Once the denial is final, `OnError` is called and the handle is evicted: the
next `ConnectToApp` opens a fresh connection.

## Managed File Storage

Every app data backend gets private object storage alongside its tables.

```go
files := ifl.Files()

// Store an object and get a permanent URL back in the same call.
info, err := files.Put(ctx, "part-1.jpg", jpeg, filestore.ContentType("image/jpeg"))

// That URL is safe to put in a table column — a dashboard widget can render it.
err = ifl.PublishToTable(ctx, "inspections", ironflock.Row{"tsp": time.Now(), "part_id": "1", "photo_url": info.URL})

data, err := files.Get(ctx, "part-1.jpg")
for obj, err := range files.Iter(ctx, filestore.Prefix("2026/")) {
	if err != nil {
		break
	}
	log.Println(obj.Key, obj.Size)
}
```

| Method | Description |
|--------|-------------|
| `Put(ctx, key, data, opts...)` / `PutReader(ctx, key, r, size, opts...)` / `PutFile(ctx, key, path, opts...)` | Store bytes, a stream, or a local file. Returns `*ObjectInfo` with a permanent `URL` |
| `Get(ctx, key, opts...)` / `GetTo(ctx, key, w, opts...)` / `GetToFile(ctx, key, path, opts...)` | Read into memory, a writer, or a file |
| `List(ctx, opts...)` / `Iter(ctx, opts...)` | One page of a folder-style listing / every object under a prefix, in subfolders too (an `iter.Seq2`). A page holds up to `Limit` entries (default 200, `filestore.DefaultListLimit`; the server caps it at `Catalog.ListMaxLimit`) |
| `Stat(ctx, key, opts...)` / `Exists(ctx, key, opts...)` | Metadata without transferring |
| `Delete` / `Copy(ctx, key, to, opts...)` / `Move` | `Move` is copy-then-delete and **not atomic** |
| `URL(ctx, key, opts...)` / `CloudURL` | Permanent authenticated URL (`""` where the deployment has no HTTP edge); `CloudURL` through an appliance's cloud files tunnel (`""` while it is off) |
| `ShareURL(ctx, key, opts...)` | Expiring bearer link (default 15 min) — hand it to a person, do not store it |
| `UploadURL(ctx, key, opts...)` | Expiring presigned upload target (default 1 h, `filestore.DefaultUploadTTL`) — a bearer write capability, see below |
| `Usage(ctx, opts...)` | Bytes stored, quota and free bytes (`-1` when unlimited); `filestore.Detail()` adds a per-namespace breakdown |
| `Catalog(ctx)` / `RefreshCatalog(ctx)` / `Namespaces(ctx)` | Namespaces and server limits, cached (`RefreshCatalog` re-fetches) |

Options: `filestore.Namespace(n)`, `ContentType`, `ToNamespace`, `Version`, `TTL`, `Size`, `Prefix`,
`Delimiter`, `Limit`, `Cursor`, `Detail`. Without `Namespace` (or with `""`) a call uses the namespace
`"default"` (`filestore.DefaultNamespace`), which exists only while the app's data template declares no
namespaces of its own, or declares one named `default`.

`List` returns the file service's folder view: the objects whose keys continue the prefix without a `/`, and
the folders below in `Prefixes`; `filestore.Delimiter("")` lists flat. The file service (fleetfiles v0.2.0)
refuses a prefix that ends in `/`, so `List` cannot open a folder by name — `Iter` can: `Iter(ctx,
filestore.Prefix("2026/"))` yields every object under that folder, in subfolders too, and nothing else.
The file service is the authority on keys: a malformed key or prefix (a leading or trailing `/`, an empty, `.`
or `..` segment, characters such as `\ : * ? " < > |`, over 900 bytes) is refused — with `INVALID_KEY` by a
service that classifies key errors, with `INTERNAL` by fleetfiles v0.2.0.

`Files()` does no I/O and may be called before `Start`. A file call waits up to 10 s for the connection (before
`Start` as well) and is not retried: unlike table operations, file calls do not use the reconnect window, so
during a platform restart a call can fail with `NOT_AVAILABLE` until the file service has registered again —
as it does whenever it binds the data backend anew. Such a call ran nowhere, so an app may repeat it.

Objects up to the server's `InlineMaxBytes` (6 MiB) travel through the router; larger ones go directly to the
object store over HTTPS through presigned URLs (`PutReader`, `PutFile`, `GetTo` and `GetToFile` stream, so a
multi-gigabyte object never has to fit in memory; a namespace takes objects up to 100 MiB unless the data
template's `maxObjectBytes` raises its cap, to at most 5 GiB). The direct path needs the device to reach the
object store host; HTTP(S) proxies from the environment are honoured. Where the deployment cannot presign (an
appliance, by default) or the object store is out of reach, reads fall back to ranges over the router
(fleetfiles v0.1.33 and later); an object replaced during such a read fails it with `INTERNAL` ("object changed
while it was read"). Writes have no such fallback: where the deployment cannot presign, a `Put` over the inline
limit fails with `TOO_LARGE`. `Stop` closes the idle connections of the store's HTTP client
(`FileStore.CloseIdleConnections` does that for a store you create with `filestore.New(caller, nil)`).

`GetToFile` reaches `path` as `open(2)` (and Python's `open(path, "wb")`) do, symbolic links included: the file
they lead to is written (created when the last link dangles), and the links stay. It downloads into a temporary
file next to that file and renames it into place once complete, so a failed download never leaves a partial
file and keeps the previous one. The replacement gets, as far as the process may, the old file's owner and
group, and then its permission bits (it is created for its owner only, so it is never more permissive than the
old file); set-ID and sticky bits, ACLs and extended attributes are not carried over, and other hard links to
the old file keep the old content. Its directory must take a new file and
a rename — not on a read-only or pseudo file system such as `/proc` or `/sys`; write such a target with `GetTo`
into `os.OpenFile(path, os.O_WRONLY|os.O_TRUNC, 0)`. A FIFO or a device is written in place as the object
streams in (a FIFO nobody reads blocks the call: `ctx` cannot interrupt it), and a file that is a mount point
(a single file bind-mounted into the container) is overwritten once the download is complete.

The permanent URLs are composed from the cached catalog; `CloudURL` re-reads it when it is older than 30 s, as
the user can switch the cloud tunnel at any time. The file edge answers the URL of an object in a namespace
declared `share: none` with 404: read such objects with `Get`. An `UploadURL` target is checked against the
namespace's content types and size cap and the quota when it is minted, not when it is used (the URL is
signed for the host only): anyone holding it can write any content to its key until it expires.

A failure of the file service or of a direct transfer is a `*filestore.Error` with a stable `Code` — branch on
it, never on `Reason` (a cancelled context, local file errors and router-level WAMP errors other than the ones
below come back as they are): `NOT_AUTHORIZED`, `NO_SUCH_NAMESPACE`, `NO_SUCH_OBJECT`, `TOO_LARGE`,
`OBJECT_TOO_LARGE`, `QUOTA_EXCEEDED`, `CONTENT_TYPE_NOT_ALLOWED`, `INVALID_KEY` (a malformed key or prefix),
`INVALID_RANGE` (a range outside the object, which the SDK never asks for), `NOT_SUPPORTED`, `NOT_AVAILABLE` (no
file service on this deployment, or not yet after a restart), `PRESIGN_UNREACHABLE` (object store not reachable
directly — a proxy?), `CLOCK_SKEW` (device clock too far off; check NTP), `INTERNAL`. A newer server may add
codes; they pass through as-is. The codes are the constants `filestore.Code…` (`filestore.CodeNoSuchObject`,
say), `filestore.ErrorCodes()` lists those this release knows, and
`errors.Is(err, &filestore.Error{Code: filestore.CodeNoSuchObject})` matches any `*filestore.Error` with that
code.

## URIs an app may use

Your app joins its own data realm, `realm-<SWARM_KEY>-<APP_KEY>-<dev|prod>`, with the router role `app`. The
router checks every publish, subscribe, call and register there against that role's allow-list; the SDK's own
methods stay inside it. It matters when you pass your own URIs to `Publish`, `Subscribe`, `Call` or the
connection. A refused action fails with `wamp.error.not_authorized` (`ironflock.WampURI(err)`).

| URI on your own realm | Allowed | SDK method |
|-----------------------|---------|------------|
| Your own names, e.g. `com.myapp.status` | publish, subscribe (prefix subscriptions too), call; never register | `Publish`, `Subscribe`, `Call` |
| `<SWARM_KEY>.<APP_KEY>.<table>`, `bulk.<SWARM_KEY>.<APP_KEY>.<table>` | publish only (never subscribe) | – (the table write topics; the SDK appends instead, see `PublishToTable`) |
| `transformed.<table>`, `transformed.bulk.<table>` | subscribe only | `SubscribeToTable` |
| `databackend.errors.…`, `files.events…` | subscribe only | – (the data backend's error and file events) |
| `<SWARM_KEY>.<DEVICE_KEY>.<APP_KEY>.<STAGE>.<name>` | register (single), call, publish | `RegisterDeviceFunction`, `CallDeviceFunction` |
| `append.…`, `appendBulk.…`, `history.transformed.…`, `secret.reveal.…`, `secret.verify.…`, `files.read.…`, `files.write.…` | call (the SDK calls them) | table writes (`PublishToTable` and `ReportError` included), history, secret and file APIs |
| `sys.appaccess.list`, `sys.appaccess.resolve` | call | `ListConsumableApps`, `ConnectToApp`, `ConnectToAllApps` |

**Identity-checked names.** Names that start with a digit — the table write topics and the device-function
URIs — also pass the platform's identity check, which binds them to your realm's swarm key. A registration is
accepted only for a function URI of your own device with your realm's swarm key, app key and stage
(`<SWARM_KEY>.<DEVICE_KEY>.<APP_KEY>.<STAGE>.<name>`); a prefix or wildcard registration only when it fixes those
four segments. Publish and call need only your realm's swarm key: a row published to a table topic with another
app key is accepted, but reaches no data backend and is lost. Apps cannot subscribe to these names. Let the SDK
build these URIs (`RegisterDeviceFunction`, `CallDeviceFunction`), and write rows with the table methods.

**Reserved names.** The router refuses `sys.` apart from the two cross-app procedures above, and `wamp.` apart
from `wamp.session.get` (the JavaScript SDK's heartbeat). The other names in the table belong to the platform:
do not start your own names with a digit, `bulk.`, `transformed.`, `databackend.errors.`, `files.`, `append.`,
`appendBulk.`, `history.` or `secret.`. Every other first segment is your app's own topic space. Nobody
subscribes to raw table topics — the data backend also takes rows published there (boards' forms write there,
and ironflock-go 1.9.1 and earlier's `PublishToTable` and `PublishRowsToTable` did): they carry the row as sent,
including the plaintext of secret columns — use `SubscribeToTable`.

## Connection reliability

The connection reconnects on its own. When the socket drops, the SDK retries until the router is back
(1 s backoff growing to 2 s), then restores every subscription and every registered device function — you
never re-subscribe. Table subscriptions also deliver the rows stored while the connection was down (see
`SubscribeToTable`); events on your own topics are not stored, so those published meanwhile are lost. If one
cannot be restored, the rest still are, and the failed one is retried while the session lasts (after 1 s,
doubling up to 30 s) and after every reconnect: the router accepted it before, so its refusal is a passing
one, such as the platform's identity check failing closed for a moment.

A router that is reachable but has no realm for the app yet (the data backend is still being provisioned) is
retried every couple of seconds, so the app comes up the moment the realm does. A realm still missing after
a minute is most likely never going to appear, so the SDK slows to one attempt every two minutes until it
does.

A socket that dies *silently* — an idle NAT or proxy that drops the connection without telling either side —
is caught by WebSocket keepalive: the SDK pings every 20 s and drops a link whose pong is missing for two
intervals, then reconnects. This covers subscribe-only apps and cross-app connections, which never write.

Table operations (`PublishToTable`, `AppendToTable`, the bulk variants, `ReportError`, the history, series and
secret reads, a consumed app's history and series reads, and the catch-up read of a table subscription) ride
out a platform restart: they wait up to the reconnect window (default 60 s, `WithReconnectWindow`) for the
connection to come back, and retry while the data backend has not registered its procedures again yet (after
0.5 s, doubling up to 5 s) — so a write or read of a table the data template does not declare (or a misspelled
name) fails with `wamp.error.no_such_procedure` only once the window is up. The table publishes
(`PublishToTable`, `PublishRowsToTable`, `ReportError`) are such calls: they append, so a row written while the
data backend restarts is not lost. (ironflock-go 1.9.1 and earlier published them to the table topics, sent
once: a row published while the data backend restarted was lost.)

`Publish`, `Subscribe`, `SubscribeToTable` (a consumed app's too) and `RegisterDeviceFunction` (with its
aliases) wait for the connection within the reconnect window too, so a short outage does not make them fail;
they are not retried. A subscription or registration made is restored after every later reconnect.

Every other operation — `Call`, `CallDeviceFunction`, `SetDeviceLocation`, cross-app discovery and opens, and
file calls — waits up to 10 s for a connection and is not retried. An operation issued before `Start` waits for
`Start` within the same time (the reconnect window, or 10 s). When the time is up, the operation fails with an
error wrapping `wamp.ErrNotConnected`, whose message names the wait in seconds.

`WithReconnectWindow(0)` turns the window off: the operations above then behave exactly like all other
operations — they wait 10 s for a connection, before `Start` as well, and are not retried, so a catch-up right
after a platform restart gets no retry either. A negative window makes `New` fail with
`ErrInvalidArgument`. `Stop` ends every wait at once — for `Start`, for the connection, between the retries of
a table call — with an error wrapping `wamp.ErrStopped`. `Unsubscribe` and `Unregister` never wait for a
connection (see [Handles](#handles)).

## Errors

| Error | Test with | Meaning |
|-------|-----------|---------|
| `ironflock.ErrInvalidArgument` | `errors.Is` | Invalid parameters (the Python SDK's `ValueError`), reported before anything is sent — also an invalid option, such as a negative `WithReconnectWindow`, which `New` refuses |
| `ironflock.ErrMissingConfig` | `errors.Is` | Identity the operation needs is missing: the serial number (`New`), `SWARM_KEY` or `APP_KEY` (`Start`, table operations), a device key (device functions) |
| `ironflock.ErrAlreadyStarted` | `errors.Is` | `Start` or `Run` on an instance that is started or starting |
| `wamp.ErrNotConnected` | `errors.Is` | No connection within the operation's wait (see [Connection reliability](#connection-reliability)) |
| `wamp.ErrStopped` | `errors.Is` | Stopped for good: an operation, `Start` or `ConnectToApp` after `Stop` or interrupted by it, or a consumed app after `Close` or a revoked grant |
| `wamp.ErrNotConfigured` | `errors.Is` | `ifl.Connection()` used before `Start` configured it (the `IronFlock` methods wait for `Start` instead) |
| `*ironflock.OperationError` | `errors.As` | A failed operation; the message names the operation and the cause, which it wraps |
| `*wamp.Error` (`ironflock.WampError`) | `errors.As`, `ironflock.WampURI(err)` | The router or callee refused; carries the URI and payload |
| `*ironflock.CrossAppAccessError` | `errors.As` | Cross-app access was declined or misused (`Code`) |
| `*filestore.Error` (`ironflock.FileStoreError`) | `errors.As` | A file operation failed (`Code`) |

`errors.Is` and `errors.As` see through `*OperationError`: `errors.Is(err, wamp.ErrStopped)` holds for a
`PublishToTable` after `Stop`.

### Data backend refusals

An operation the data backend refuses fails with an `*OperationError` around a `*WampError` that carries one of
these URIs (and the reason as its first argument). Branch on `ironflock.WampURI(err)`; the SDK retries none of
them. Every table write reports them, `PublishToTable` included (ironflock-go 1.9.1 and earlier dropped a
published row the data backend refused without an error).

| Constant | Refused |
|----------|---------|
| `URIRateLimited` | `RevealSecrets` beyond 30, `VerifySecret` beyond 120 calls a minute per app credential on a device |
| `URIResultTooLarge` | A read with a single row over the 8 MiB message budget, or a result over the 1 GiB guard (other large results arrive in chunks, which the SDK reassembles); its kwargs name the largest columns |
| `URIInvalidLimit` | A read of a transform with a `Limit` over 3000 |
| `URIInvalidTimeRange`, `URIInvalidMetric`, `URIInvalidGroupBy`, `URISeriesTooManyGroups` | Series queries the table cannot answer (see `GetSeriesHistory`) |
| `URISecretColumn`, `URINotASecretColumn` | A filter on a secret column; `VerifySecret` on a column that is not secret (a table without secret columns has no secret procedures: see [Secret Columns](#secret-columns)) |
| `URIStorageFull` | A table write while the appliance's disk is nearly full — temporary: write the rows later |
| `URIStorageOverusage` | A table write beyond the account's storage allowance |
| `URIEntityKeyConflict` | A row whose entity key (`maintainLatestFlagFor`) and `tsp` another row has already, in the batch or stored: give every row its own `tsp` |
| `URISecretSentinelUnresolvable`, `URISecretCiphertextRejected` | A write that keeps a secret's previous value without an entity key to find it by; a write of an already encrypted value |

## Advanced usage

`ifl.Connection()` is the underlying `*wamp.Connection`, a self-healing WAMP session with the methods
`Call`, `Publish`, `Subscribe`, `Register` and friends; it can also be used on its own (see
[examples/connection](examples/connection)). Unlike the `IronFlock` methods, its operations do not wait for
`Start`: until `Start` has configured the connection they fail with `wamp.ErrNotConfigured`. They wait for a
session as long as their window says (`Call`'s retry window, `Publish`'s wait window; `Subscribe` and
`Register` wait the default 10 s, `SubscribeWithin` and `RegisterWithin` as long as you pass). Its `Register`
sends a handler's values as given, so return JSON-like values (`nil`, booleans, numbers, strings, `[]byte`,
`[]any`, `map[string]any`); `RegisterDeviceFunction` converts them for you. `wamp.IsNotServedYet(err)` tells
the refusals of a call whose procedure is not registered (yet): such a call never ran, so it can be repeated.
The rules in [URIs an app may use](#uris-an-app-may-use) apply to it as well.

## Differences from the Python and JavaScript SDKs

The Go SDK follows the Python and JavaScript SDKs and puts the same messages on the wire. Where they differ,
it takes the more robust variant; Go-specific choices:

- Every operation takes a `context.Context`; durations are `time.Duration`.
- Arguments are variadic with a `Kwargs` marker instead of `*args, **kwargs` / `(args, kwargs)`.
- Operations issued before `Start` wait for it within their window, as the Python and JavaScript SDKs do from
  1.10.0 (Python 1.9.1 fails them at once). `Stop` is final, while the Python and JavaScript SDKs can be
  started again after `stop()`.
- The handles — `Subscription`, `TableSubscription` (which removes both the row and the bulk subscription),
  `Registration` — and `Unsubscribe`/`Unregister` behave as in the Python and JavaScript SDKs from 1.10.0;
  `IronFlock.Unsubscribe` takes either subscription handle as an `Unsubscriber`. (The 1.9.1 SDKs return
  autobahn's objects, and their `subscribe_to_table`/`subscribeToTable` only the row feed's subscription.)
- `VerifySecret` returns `{Match, Checked}` (as JavaScript); Python returns only the boolean.
- Filter groups (`Or`/`And`) are supported (as JavaScript), and the cross-app secret-column guard also looks
  inside them.
- Liveness uses WebSocket pings (as Python); the JavaScript SDK uses a WAMP heartbeat because browsers have no
  ping API.
- There are no retained-event options (`get_retained`, `retain`): the router does not keep retained
  events. Raw WAMP options still pass through the `Extra` of `SubscribeOptions`, `RegisterOptions` and
  `CallOptions`, and of `wamp.PublishOptions`, which only `ifl.Connection().Publish` takes.
- `GetSeriesHistory` takes a method per metric (`SeriesMetric`), the series query of fleetdb v1.0.58 and later.
  The Python and JavaScript SDKs send the same query since 1.9.1 and also accept, deprecated, column names with
  one top-level method.
- Where the 1.9.1 Python and JavaScript SDKs predate the current platform, Go follows the platform: tcp/udp
  remote-access URLs read `REMOTE_PORT_FOR_<port>`, a failed restore is retried while the session lasts, and a
  consumed app's auth denial counts only once it persists.

## Development

```shell
just test               # unit tests with the race detector (go test -race -count=1 ./...)
just check              # go vet, golangci-lint, cross-builds for the device platforms, go mod tidy -diff
just coverage           # unit test coverage summary
just test-integration   # end-to-end suites against the router (ROUTER_IMAGE, or ROUTER_BIN and ROUTER_REF_CONFIG)
```

The unit tests need nothing external: they run against an in-process nexus router. The end-to-end tests run
against the [router](https://github.com/IronFlock/router) and a fake IronFlock platform,
call across to the Python SDK, and compare the wire payloads with the Python SDK's run of the same scenario.
They live in [integration/](integration), whose README covers the harness, the environment variables and CI;
the [Justfile](Justfile) lists every task.

## License

MIT
