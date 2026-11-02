"""Model of fleetdb-service for the integration harness.

Pinned to fleetdb-service v1.4.0 (commit d6f5386) and, for cross-app
discovery, RESWARM reswarm-backend v1.4.1 (backend/api/appaccess/appaccess.ts).
Every rule below names the source it mirrors; when fleetdb changes, re-read
those places and update this file and the vendored contract tables in
contracts/ (see test_fake_platform.py).

Pure Python, standard library only: fake_platform.py binds it to WAMP, and
test_fake_platform.py checks it without a router.

What it models, per declared table of a data template:

  procedures   append.<swarm>.<app>.<t>, appendBulk.<swarm>.<app>.<t>,
               history.transformed.<t>, history.transformed.series.<t>,
               secret.reveal.<t> / secret.verify.<t> (tables with a secret
               column only), history.transformed.<transform>,
               transform.refresh.<transform>, the same four for the system
               table error-logs, sys.appaccess.resolve/list
  topics       <swarm>.<app>.<t> and bulk.<swarm>.<app>.<t> (writes; the
               rows are republished on transformed.<t> / transformed.bulk.<t>)
  requests     typia.assert of TableQueryArgs, SeriesQueryArgs,
               SecretRevealArgs, SecretVerifyArgs, AppAccessResolveArgs
               (types/rpc.ts): a refusal is wamp.error.runtime_error with the
               TypeGuardError as its only argument, as autobahn-js sends it
  writes       transformRow: the template's column paths, device_key from
               DEVICE_KEY, authid from the publisher, a mandatory tsp (append:
               runtime_error [{}]; publish: dropped), secret sentinels,
               entity-key carry-over and conflicts, bulk all-or-nothing, the
               storage refusals (test hook)
  reads        newest `limit` rows after `offset` returned ascending, tsp as
               epoch ms, SELECT * with device_key and authid, `columns` /
               `columnPaths` projection, [start, end) truncated to whole
               seconds, the `latest` marker (DISTINCT ON the entity key),
               filter groups with SQL's three-valued logic (filter-core's
               evaluator), secret redaction, the chunked progressive envelope
               when the caller set receive_progress, result_too_large without
  series       time_bucket over [start, end) aligned to the start, the bucket
               budget (limit, bucketMs widening), "<METHOD>:<ref>" columns,
               groupBy columns verbatim, empty buckets absent, the typed
               refusals of validateSeriesColumns and invalid_time_range
  transforms   limit <= 3000 (invalid_limit), Limit/Offset/FilterAnd only
  refresh      transform.refresh.<transform> (caller role app only, [] or
               [{}]): one transform tick on demand — the view's rows (at most
               3000, reversed, secrets redacted) published on
               transformed.<transform> as the cron tick publishes them, and
               the result {success, materialized, refreshed, skipped,
               published, rows}

Deviations (none of them reaches a wire shape an SDK can rely on):
  - rows live in memory; a value Postgres would reject (an unparseable tsp, a
    projected column that does not exist, a non-numeric json path under a
    numeric aggregate) fails with wamp.error.runtime_error and a small
    pg-like error object, not the full one node-postgres serializes
  - timestamps parse as ISO 8601 (with or without "T", zone or UTC); V8's and
    Postgres' other legacy formats are not accepted
  - LIKE on numeric/timestamp/json values matches nothing (filter-core's
    evaluator does the same; Postgres compares their ::TEXT)
  - continuous aggregates, the cron schedule of transforms (only the
    on-demand tick is modeled, and it always refreshes and always publishes:
    no write-generation skip, overlap guard, refresh pacing or subscriber
    gate), alarms, the subscriber gate and the realtime size guard are not
    modeled
"""

import datetime
import decimal
import json
import math
import re
import threading
import time

FLEETDB_VERSION = "fleetdb-service v1.4.0 (d6f5386)"

# utils.ts: CHUNK_BYTES = floor(min(ROUTER_MAX_FRAME_BYTES, 16 MiB) / 2);
# MAX_RESULT_BYTES (FLEETDB_MAX_RESULT_BYTES default) is the runaway guard.
LEGACY_FRAME_FLOOR = 16 * 1024 * 1024
CHUNK_BYTES = LEGACY_FRAME_FLOOR // 2
MAX_RESULT_BYTES = 1024 * 1024 * 1024
SERIES_ROW_CAP = 50_000  # sql-generator.ts
TRANSFORM_LIMIT_CAP = 3000  # DataBackend.registerSelectTransform

# secrets.ts
SECRET_PREFIX = "ifsec:1:"
SECRET_PLACEHOLDER = "__secret__"
SECRET_MASK_UI = "•" * 8

# DataBackend.rateLimitSecretRpc: calls per minute per (operation, caller authid).
SECRET_RATE_LIMITS = {"reveal": 30, "verify": 120}

# storageGuard.ts
REASON_STORAGE_FULL = "STORAGE_FULL"
ERROR_URI_STORAGE_FULL = "sys.dataservice.error.storage_full"
ERROR_URI_STORAGE_OVERUSAGE = "sys.dataservice.error.storage_overusage"

DATA_TYPES = ("string", "numeric", "bigint", "timestamp", "boolean", "json")
OPERATORS = ("=", ">", "<", ">=", "<=", "!=", "<>", "LIKE", "ILIKE", "NOT LIKE", "NOT ILIKE",
             "IN", "NOT IN", "IS NULL", "IS NOT NULL")
DOWNSAMPLE_METHODS = ("AVG", "SUM", "COUNT", "MIN", "MAX", "FIRST", "LAST")


class WampError(Exception):
    """A WAMP error a handler answers with: error URI, Arguments, ArgumentsKw."""

    def __init__(self, error, args=None, kwargs=None):
        super().__init__(error)
        self.error = error
        self.wamp_args = list(args or [])
        self.wamp_kwargs = dict(kwargs or {})


def runtime_error(err_object):
    """What autobahn-js sends for a non-autobahn.Error thrown by a handler:
    wamp.error.runtime_error with Arguments [err], and fleetdb's realm
    serializer is JSON.stringify, which keeps only an Error's own enumerable
    fields: {} for a plain Error."""
    return WampError("wamp.error.runtime_error", [err_object])


def pg_error(code):
    """The enumerable fields of a node-postgres DatabaseError that matter
    (the real one also carries length, position, file, line, routine)."""
    return runtime_error({"name": "error", "severity": "ERROR", "code": code})


# ----------------------------------------------------------- JavaScript values
#
# fleetdb is JavaScript: these mirror Number(), String(), parseFloat() and
# parseInt() for the scalar values a WAMP payload can carry.

_JS_DECIMAL = re.compile(r"^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$")
_JS_FLOAT_PREFIX = re.compile(r"^[+-]?(Infinity|(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?)")
_JS_INT_PREFIX = re.compile(r"^[+-]?\d+")
NAN = float("nan")


def is_number(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def js_number(v):
    """Number(v)."""
    if v is None:
        return 0
    if isinstance(v, bool):
        return 1 if v else 0
    if is_number(v):
        return v
    if isinstance(v, str):
        s = v.strip()
        if s == "":
            return 0
        if _JS_DECIMAL.match(s):
            return float(s)
        if s in ("Infinity", "+Infinity"):
            return math.inf
        if s == "-Infinity":
            return -math.inf
        for prefix, base in (("0x", 16), ("0X", 16), ("0o", 8), ("0O", 8), ("0b", 2), ("0B", 2)):
            if s.startswith(prefix):
                try:
                    return int(s[2:], base)
                except ValueError:
                    return NAN
        return NAN
    if isinstance(v, list):
        return js_number(js_string(v))
    return NAN


def _js_float_string(x):
    """Number.prototype.toString(): the shortest round-trip digits (Python's
    repr and V8 agree on them), laid out per ECMA-262 Number::toString."""
    if math.isnan(x):
        return "NaN"
    if math.isinf(x):
        return "Infinity" if x > 0 else "-Infinity"
    if x == 0:
        return "0"
    sign = "-" if x < 0 else ""
    t = decimal.Decimal(repr(abs(x))).normalize().as_tuple()
    digits = "".join(str(d) for d in t.digits)
    k = len(digits)
    n = t.exponent + k
    if k <= n <= 21:
        return sign + digits + "0" * (n - k)
    if 0 < n <= 21:
        return sign + digits[:n] + "." + digits[n:]
    if -6 < n <= 0:
        return sign + "0." + "0" * (-n) + digits
    e = n - 1
    exp = ("+" if e >= 0 else "-") + str(abs(e))
    return sign + digits[0] + ("." + digits[1:] if k > 1 else "") + "e" + exp


def js_string(v):
    """String(v) for the values a payload carries."""
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        return _js_float_string(v)
    if isinstance(v, str):
        return v
    if isinstance(v, list):
        return ",".join("" if x is None else js_string(x) for x in v)
    if isinstance(v, dict):
        return "[object Object]"
    if isinstance(v, (bytes, bytearray)):
        return ",".join(str(b) for b in v)
    return str(v)


def js_parse_float(v):
    s = js_string(v).lstrip()
    m = _JS_FLOAT_PREFIX.match(s)
    if not m:
        return NAN
    text = m.group(0)
    if text.endswith("Infinity"):
        return -math.inf if text.startswith("-") else math.inf
    return float(text)


def js_parse_int(v):
    s = js_string(v).lstrip()
    m = _JS_INT_PREFIX.match(s)
    return int(m.group(0)) if m else NAN


def wire_value(v):
    """A value the way it reaches an SDK from fleetdb: JSON.stringify on the
    fleetdb session, re-encoded by the router, so an integral JavaScript
    number arrives as an integer and NaN/Infinity as null."""
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
        if v == int(v) and abs(v) < 1e21:
            return int(v)
        return v
    if isinstance(v, dict):
        return {k: wire_value(x) for k, x in v.items()}
    if isinstance(v, list):
        return [wire_value(x) for x in v]
    return v


# --------------------------------------------------------------- timestamps

_ISO = re.compile(
    r"^\s*([+-]\d{6}|\d{4})(?:-(\d{2})(?:-(\d{2}))?)?"
    r"(?:[T ](\d{2}):(\d{2})(?::(\d{2})(?:[.,](\d+))?)?)?"
    r"\s*(Z|z|[+-]\d{2}(?::?\d{2})?)?\s*$")
_DAYS_IN_MONTH = (31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)


def _days_from_civil(y, m, d):
    """Days since 1970-01-01 of a proleptic Gregorian date (any year)."""
    y -= m <= 2
    era = (y if y >= 0 else y - 399) // 400
    yoe = y - era * 400
    doy = (153 * (m + (-3 if m > 2 else 9)) + 2) // 5 + d - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    return era * 146097 + doe - 719468


def parse_iso_ms(text):
    """Epoch ms of an ISO 8601 date-time (a date alone is UTC midnight, a
    date-time without a zone is UTC: fleetdb and its Postgres run in UTC), or
    None when the text is not one."""
    m = _ISO.match(text) if isinstance(text, str) else None
    if not m:
        return None
    year, month, day, hh, mi, ss, frac, zone = m.groups()
    y, mo, d = int(year), int(month or 1), int(day or 1)
    h, mn, sec = int(hh or 0), int(mi or 0), int(ss or 0)
    leap = y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)
    if not 1 <= mo <= 12 or not 1 <= d <= _DAYS_IN_MONTH[mo - 1] + (1 if mo == 2 and leap else 0):
        return None
    if h > 23 or mn > 59 or sec > 59:
        return None
    ms = (_days_from_civil(y, mo, d) * 86400 + h * 3600 + mn * 60 + sec) * 1000
    if frac:
        ms += int((frac + "000")[:3])
    if zone and zone not in ("Z", "z"):
        sign = 1 if zone[0] == "+" else -1
        digits = zone[1:].replace(":", "")
        offset = int(digits[:2]) * 60 + (int(digits[2:4]) if len(digits) >= 4 else 0)
        ms -= sign * offset * 60000
    return ms


def js_date_ms(v):
    """new Date(v).getTime() for a number or an ISO string; NaN otherwise."""
    if is_number(v):
        return v
    if isinstance(v, str):
        ms = parse_iso_ms(v)
        return NAN if ms is None else ms
    return NAN


def iso_ms(ms):
    return datetime.datetime.fromtimestamp(ms / 1000, tz=datetime.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.") + "%03dZ" % (int(ms) % 1000)


# ------------------------------------------------- typia.assert (types/rpc.ts)
#
# A port of the validators typia 9.7.2 generates for fleetdb's RPC argument
# types: the same checks in the same order, so the first failure has the same
# path and "expected" text. contracts/typia.json holds the verdicts of the real
# compiled validators for a corpus of payloads; test_fake_platform.py checks
# this port against it.

UNDEFINED = object()


class TypeGuard(Exception):
    def __init__(self, path, expected, value):
        super().__init__(path)
        self.path, self.expected, self.value = path, expected, value

    def wire(self):
        """JSON.stringify(TypeGuardError): own enumerable fields only."""
        out = {"method": "typia.assert", "path": self.path, "expected": self.expected}
        if self.value is UNDEFINED:
            out["description"] = ("The value at this path is `undefined`.\n\n"
                                  "Please fill the `%s` typed value next time." % self.expected)
        else:
            out["value"] = self.value
        return out


def _prop(obj, key):
    if isinstance(obj, dict) and key in obj:
        return obj[key]
    return UNDEFINED


def _is_object(v):  # typeof v === "object" && v !== null (arrays included)
    return isinstance(v, (dict, list))


def _is_uint32(v):
    return is_number(v) and not (isinstance(v, float) and (math.isnan(v) or math.isinf(v))) \
        and math.floor(v) == v and 0 <= v <= 4294967295


def _fail(path, expected, value):
    raise TypeGuard(path, expected, value)


_OPS_EXPECTED = '("!=" | "<" | "<=" | "<>" | "=" | ">" | ">=" | "ILIKE" | "IN" | "IS NOT NULL" | "IS NULL" | "LIKE" | "NOT ILIKE" | "NOT IN" | "NOT LIKE")'
_OPS_EXPECTED_UNDEF = _OPS_EXPECTED[:-1] + " | undefined)"
_FILTER_NODE = "(SQLFilterGroup | SQLFilterLatestMarker | SQLFilterPredicate)"
_PRED_VALUE = "(Array<string | number | boolean> | boolean | number | string | undefined)"
_METHODS_EXPECTED = '("AVG" | "COUNT" | "FIRST" | "LAST" | "MAX" | "MIN" | "SUM")'


def _is_scalar(v):
    return isinstance(v, (str, bool)) or is_number(v)


def _check_time_range(v, path):
    if len(v) == 2 and all(x is None or isinstance(x, str) for x in v):
        return
    if len(v) == 2 and all(x is None or is_number(x) for x in v):
        return
    _fail(path, "(ISOTimeRange | TimeRange)", v)


def _check_filter_node(node, path):
    latest = _prop(node, "latest")
    combinator = _prop(node, "combinator")
    if latest is True:
        column = _prop(node, "column")
        if not (column is UNDEFINED or isinstance(column, str)):
            _fail(path + ".column", "(string | undefined)", column)
        op = _prop(node, "operator")
        if not (op is UNDEFINED or (isinstance(op, str) and op in OPERATORS)):
            _fail(path + ".operator", _OPS_EXPECTED_UNDEF, op)
        value = _prop(node, "value")
        if not (value is UNDEFINED or _is_scalar(value)):
            _fail(path + ".value", "(boolean | number | string | undefined)", value)
        data_type = _prop(node, "dataType")
        if not (data_type is UNDEFINED or isinstance(data_type, str)):
            _fail(path + ".dataType", "(string | undefined)", data_type)
        return
    if combinator in ("AND", "OR"):
        filters = _prop(node, "filters")
        if not isinstance(filters, list):
            _fail(path + ".filters", "Array<SQLFilterAnd>", filters)
        for i, child in enumerate(filters):
            if not _is_object(child):
                _fail("%s.filters[%d]" % (path, i), _FILTER_NODE, child)
            _check_filter_node(child, "%s.filters[%d]" % (path, i))
        if not (latest is UNDEFINED or latest is False):
            _fail(path + ".latest", "(false | undefined)", latest)
        for key in ("column", "operator"):
            v = _prop(node, key)
            if v is not UNDEFINED:
                _fail(path + "." + key, "undefined", v)
        return
    column = _prop(node, "column")
    if not isinstance(column, str):
        _fail(path + ".column", "string", column)
    op = _prop(node, "operator")
    if not (isinstance(op, str) and op in OPERATORS):
        _fail(path + ".operator", _OPS_EXPECTED, op)
    value = _prop(node, "value")
    if value is None:
        _fail(path + ".value", _PRED_VALUE, value)
    if not (value is UNDEFINED or _is_scalar(value)):
        if not isinstance(value, list):
            _fail(path + ".value", _PRED_VALUE, value)
        for i, elem in enumerate(value):
            if not _is_scalar(elem):
                _fail("%s.value[%d]" % (path, i), "(boolean | number | string)", elem)
    data_type = _prop(node, "dataType")
    if not (data_type is UNDEFINED or isinstance(data_type, str)):
        _fail(path + ".dataType", "(string | undefined)", data_type)
    if not (latest is UNDEFINED or latest is False):
        _fail(path + ".latest", "(false | undefined)", latest)
    for key in ("combinator", "filters"):
        v = _prop(node, key)
        if v is not UNDEFINED:
            _fail(path + "." + key, "undefined", v)


def _check_filter_and(obj, path):
    v = _prop(obj, "filterAnd")
    if v is None or v is UNDEFINED:
        return
    if not isinstance(v, list):
        _fail(path + ".filterAnd", "(Array<SQLFilterAnd> | null | undefined)", v)
    for i, node in enumerate(v):
        if not _is_object(node):
            _fail("%s.filterAnd[%d]" % (path, i), _FILTER_NODE, node)
        _check_filter_node(node, "%s.filterAnd[%d]" % (path, i))


def _check_string_list(obj, key, path):
    v = _prop(obj, key)
    if v is None or v is UNDEFINED:
        return
    if not isinstance(v, list):
        _fail(path + "." + key, "(Array<string> | null | undefined)", v)
    for i, elem in enumerate(v):
        if not isinstance(elem, str):
            _fail("%s.%s[%d]" % (path, key, i), "string", elem)


def _check_offset(obj, path):
    v = _prop(obj, "offset")
    if v is UNDEFINED:
        return
    if is_number(v):
        if not _is_uint32(v):
            _fail(path + ".offset", 'number & Type<"uint32">', v)
        return
    _fail(path + ".offset", '((number & Type<"uint32">) | undefined)', v)


def _check_limit(obj, path, minimum, maximum):
    v = _prop(obj, "limit")
    parts = ['Type<"uint32">'] + (["Minimum<%d>" % minimum] if minimum is not None else []) + ["Maximum<%d>" % maximum]
    if not is_number(v):
        _fail(path + ".limit", "(number & %s)" % " & ".join(parts), v)
    if not _is_uint32(v):
        _fail(path + ".limit", 'number & Type<"uint32">', v)
    if minimum is not None and not minimum <= v:
        _fail(path + ".limit", "number & Minimum<%d>" % minimum, v)
    if not v <= maximum:
        _fail(path + ".limit", "number & Maximum<%d>" % maximum, v)


def _check_optional_time_range(obj, path):
    v = _prop(obj, "timeRange")
    if v is None or v is UNDEFINED:
        return
    if not isinstance(v, list):
        _fail(path + ".timeRange", "(ISOTimeRange | TimeRange | null | undefined)", v)
    _check_time_range(v, path + ".timeRange")


def _check_table_params(obj, path):
    _check_limit(obj, path, None, 10000)
    _check_offset(obj, path)
    _check_optional_time_range(obj, path)
    _check_filter_and(obj, path)
    _check_string_list(obj, "columns", path)
    _check_string_list(obj, "columnPaths", path)


def _check_secret_selector(obj, path):
    # Omit<TableQueryParams, "limit"> & {limit}: typia lists the remaining
    # properties starting with `columns`, then the new limit.
    _check_string_list(obj, "columns", path)
    _check_offset(obj, path)
    _check_optional_time_range(obj, path)
    _check_filter_and(obj, path)
    _check_string_list(obj, "columnPaths", path)
    _check_limit(obj, path, 1, 100)


def _check_verify_params(obj, path):
    _check_secret_selector(obj, path)
    column = _prop(obj, "column")
    if not isinstance(column, str):
        _fail(path + ".column", "(string & MinLength<1>)", column)
    if column == "":
        _fail(path + ".column", "string & MinLength<1>", column)
    candidate = _prop(obj, "candidate")
    if not isinstance(candidate, str):
        _fail(path + ".candidate", "string", candidate)


def _check_series_params(obj, path):
    metrics = _prop(obj, "metrics")
    if not isinstance(metrics, list):
        _fail(path + ".metrics", "(Array<SeriesMetric> & MinItems<1>)", metrics)
    if len(metrics) < 1:
        _fail(path + ".metrics", "Array<> & MinItems<1>", metrics)
    for i, m in enumerate(metrics):
        p = "%s.metrics[%d]" % (path, i)
        if not _is_object(m):
            _fail(p, "SeriesMetric", m)
        ref = _prop(m, "ref")
        if not isinstance(ref, str):
            _fail(p + ".ref", "string", ref)
        method = _prop(m, "method")
        if not (isinstance(method, str) and method in DOWNSAMPLE_METHODS):
            _fail(p + ".method", _METHODS_EXPECTED, method)
    _check_limit(obj, path, 1, 10000)
    tr = _prop(obj, "timeRange")
    if tr is not None:
        if not isinstance(tr, list):
            _fail(path + ".timeRange", "(ISOTimeRange | TimeRange | null)", tr)
        _check_time_range(tr, path + ".timeRange")
    bucket = _prop(obj, "bucketMs")
    if not (bucket is None or bucket is UNDEFINED):
        if not is_number(bucket):
            _fail(path + ".bucketMs", "((number & Minimum<1000>) | null | undefined)", bucket)
        if not 1000 <= bucket:
            _fail(path + ".bucketMs", "number & Minimum<1000>", bucket)
    _check_string_list(obj, "groupBy", path)
    _check_filter_and(obj, path)


def _check_resolve_params(obj, path):
    app = _prop(obj, "app")
    if not isinstance(app, str):
        _fail(path + ".app", "string", app)


_ARG_TYPES = {
    # kind: (Args name, tuple name, element name, element check)
    "table": ("TableQueryArgs", "[TableQueryParams]", "TableQueryParams", _check_table_params),
    "series": ("SeriesQueryArgs", "[SeriesQueryParams]", "SeriesQueryParams", _check_series_params),
    "reveal": ("SecretRevealArgs", "[SecretQuerySelector]", "SecretQuerySelector", _check_secret_selector),
    "verify": ("SecretVerifyArgs", "[SecretVerifyParams]", "SecretVerifyParams", _check_verify_params),
    "resolve": ("AppAccessResolveArgs", "[__type]", "__type", _check_resolve_params),
}


def typia_check(kind, args):
    """None when typia.assert<...Args>(args) accepts args, else the
    TypeGuardError as JSON (what goes on the wire)."""
    name, tuple_name, elem_name, check = _ARG_TYPES[kind]
    try:
        if not isinstance(args, list):
            _fail("$input", name, args)
        if len(args) != 1:
            _fail("$input", tuple_name, args)
        if not _is_object(args[0]):
            _fail("$input[0]", elem_name, args[0])
        check(args[0], "$input[0]")
    except TypeGuard as e:
        return e.wire()
    return None


def typia_assert(kind, args):
    err = typia_check(kind, args)
    if err is not None:
        raise runtime_error(err)
    return args[0]


# --------------------------------------------- filter-core (src/filter-core)

_IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def parse_column_path(column):
    """path.ts parseColumnPath: (base, [keys])."""
    m = _IDENT.match(column)
    if not m:
        return column, []
    base, path, i = m.group(0), [], m.end()
    while i < len(column):
        ch = column[i]
        if ch == ".":
            i += 1
            start = i
            while i < len(column) and column[i] not in ".[":
                i += 1
            key = column[start:i]
            if key == "":
                return column, []
            path.append(key)
            continue
        if ch == "[":
            close = column.find("]", i)
            if close == -1:
                return column, []
            key = column[i + 1:close].strip()
            if len(key) >= 2 and key[0] in "'\"" and key[-1] == key[0]:
                key = key[1:-1]
            if key == "":
                return column, []
            path.append(key)
            i = close + 1
            continue
        return column, []
    return base, path


def resolve_row_value(row, column):
    base, path = parse_column_path(column)
    value = row.get(base) if isinstance(row, dict) else None
    for key in path:
        if value is None:
            return None
        if isinstance(value, dict):
            value = value.get(key)
        elif isinstance(value, list) and re.fullmatch(r"0|[1-9][0-9]*", key) and int(key) < len(value):
            value = value[int(key)]
        else:
            return None
    return value


_NUMERIC_RE = re.compile(r"^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$")
_INTEGER_RE = re.compile(r"^[+-]?\d+$")
_TRUE_WORDS = {"t", "tr", "tru", "true", "y", "ye", "yes", "on", "1"}
_FALSE_WORDS = {"f", "fa", "fal", "fals", "false", "n", "no", "of", "off", "0"}
_HAS_ZONE_RE = re.compile(r"(Z|[+-]\d{2}(:?\d{2})?)$", re.I)
_DMY_RE = re.compile(r"^(\d{1,2})[./](\d{1,2})[./](\d{4})([T ].*)?$")


def _coerce_timestamp(value):
    if is_number(value):
        return ("timestamp", value) if math.isfinite(value) else None
    if not isinstance(value, str):
        return None
    text = value.strip()
    if text == "":
        return None
    if _INTEGER_RE.match(text):
        return ("timestamp", int(text))
    dmy = _DMY_RE.match(text)
    if dmy:
        d, m, y, rest = dmy.groups()
        return _coerce_timestamp("%s-%s-%s%s" % (y, m.zfill(2), d.zfill(2), (rest or "").replace(" ", "T", 1)))
    normalized = text if _HAS_ZONE_RE.search(text) else text.replace(" ", "T", 1) + "Z"
    ms = parse_iso_ms(normalized)
    return None if ms is None else ("timestamp", ms)


def coerce(value, data_type):
    """coerce.ts: (kind, comparable) or None when the value cannot be read
    as that type (the predicate is then unknown)."""
    if value is None:
        return None
    if data_type == "string":
        if isinstance(value, (dict, list)):
            return None
        return ("string", js_string(value))
    if data_type in ("numeric", "bigint"):
        if isinstance(value, (bool, dict, list)):
            return None
        text = js_string(value).strip()
        if not (_INTEGER_RE if data_type == "bigint" else _NUMERIC_RE).match(text):
            return None
        num = float(text)
        if not math.isfinite(num):
            return None
        return ("number", int(text) if _INTEGER_RE.match(text) else num)
    if data_type == "boolean":
        if isinstance(value, bool):
            return ("boolean", value)
        if is_number(value):
            return ("boolean", value == 1) if value in (0, 1) else None
        if not isinstance(value, str):
            return None
        word = value.strip().lower()
        if word in _TRUE_WORDS:
            return ("boolean", True)
        if word in _FALSE_WORDS:
            return ("boolean", False)
        return None
    if data_type == "timestamp":
        return _coerce_timestamp(value)
    if data_type == "json":
        if isinstance(value, str):
            try:
                return ("json", json.loads(value))
            except ValueError:
                return None
        return ("json", value)
    return None


def infer_leaf_data_type(value):
    if isinstance(value, bool):
        return "boolean"
    if is_number(value):
        return "numeric"
    if isinstance(value, (dict, list)):
        return "json"
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("{") or text.startswith("["):
            try:
                json.loads(text)
                return "json"
            except ValueError:
                pass
        if text == "":
            return "string"
        if text.lower() in ("true", "false"):
            return "boolean"
        if _NUMERIC_RE.match(text):
            return "numeric"
    return "string"


def render_data_type(predicate):
    """render.ts resolveRenderType without a column map, as the SQL
    generators call it: the dataType addFilterColumnDataTypes stamped (the
    declared type, numeric for device_key, string for authid, the inferred
    leaf type for a json path), TEXT when there is none."""
    stamped = predicate.get("dataType")
    return stamped if isinstance(stamped, str) and stamped in DATA_TYPES else "string"


def set_elements(value):
    if isinstance(value, list):
        return value
    if value is None:
        return []
    return js_string(value).split(",")


def compile_like(pattern, case_insensitive):
    """like.ts: an anchored regex, or None for a trailing escape."""
    out, i = [], 0
    while i < len(pattern):
        ch = pattern[i]
        if ch == "\\":
            if i + 1 >= len(pattern):
                return None
            out.append(re.escape(pattern[i + 1]))
            i += 2
            continue
        out.append("[\\s\\S]*" if ch == "%" else "[\\s\\S]" if ch == "_" else re.escape(ch))
        i += 1
    return re.compile("^" + "".join(out) + "$", re.I if case_insensitive else 0)


def _compare(a, b):
    if a is None or b is None or a[0] != b[0] or a[0] == "json":
        return None
    x, y = a[1], b[1]
    return 0 if x == y else (-1 if x < y else 1)


def _equals(a, b):
    if a is None or b is None or a[0] != b[0]:
        return None
    if a[0] == "json":
        return a[1] == b[1]
    return _compare(a, b) == 0


def _and3(parts):
    unknown = False
    for p in parts:
        if p == "false":
            return "false"
        unknown |= p == "unknown"
    return "unknown" if unknown else "true"


def _or3(parts):
    unknown = False
    for p in parts:
        if p == "true":
            return "true"
        unknown |= p == "unknown"
    return "unknown" if unknown else "false"


def _not3(t):
    return {"true": "false", "false": "true"}.get(t, "unknown")


def is_group(node):
    return isinstance(node, dict) and isinstance(node.get("combinator"), str) and isinstance(node.get("filters"), list)


def is_latest_marker(node):
    return isinstance(node, dict) and node.get("latest") is True


def _evaluate_predicate(pred, row):
    data_type = render_data_type(pred)
    raw = resolve_row_value(row, pred["column"])
    op = pred.get("operator")
    if op in ("IS NULL", "IS NOT NULL"):
        return "true" if (op == "IS NULL") == (raw is None) else "false"
    if op in ("IN", "NOT IN") and not set_elements(pred.get("value")):
        return "false" if op == "IN" else "true"
    if raw is None:
        return "unknown"
    left = coerce(raw, data_type)
    if left is None:
        return "unknown"
    if op in ("IN", "NOT IN"):
        hits = []
        for element in set_elements(pred.get("value")):
            right = coerce(element, data_type)
            eq = _equals(left, right) if right is not None else None
            hits.append("unknown" if eq is None else ("true" if eq else "false"))
        hit = _or3(hits)
        return hit if op == "IN" else _not3(hit)
    if op in ("LIKE", "ILIKE", "NOT LIKE", "NOT ILIKE"):
        if data_type == "string" and left[0] == "string":
            text = left[1]
        elif data_type == "bigint" and left[0] == "number":
            text = js_string(left[1])
        elif data_type == "boolean" and left[0] == "boolean":
            text = "true" if left[1] else "false"
        else:
            return "unknown"
        pattern = coerce(pred.get("value"), "string")
        if pattern is None:
            return "unknown"
        regex = compile_like(pattern[1], op in ("ILIKE", "NOT ILIKE"))
        if regex is None:
            return "unknown"
        matched = regex.match(text) is not None
        return "true" if matched != op.startswith("NOT") else "false"
    right = coerce(pred.get("value"), data_type)
    if right is None:
        return "unknown"
    if op in ("=", "!=", "<>"):
        eq = _equals(left, right)
        if eq is None:
            return "unknown"
        return "true" if (op == "=") == eq else "false"
    c = _compare(left, right)
    if c is None:
        return "unknown"
    return "true" if {">": c > 0, ">=": c >= 0, "<": c < 0, "<=": c <= 0}.get(op, False) else "false"


def evaluate_node(node, row):
    """evaluate.ts evaluateNode over a sanitized (type-stamped) node; the
    evaluator and the SQL renderer select the same rows (fleetdb pins that
    in tests/filterConformance.test.ts)."""
    if is_latest_marker(node):
        return "true"
    if is_group(node):
        parts = [evaluate_node(child, row) for child in node["filters"]]
        return _or3(parts) if node["combinator"] == "OR" else _and3(parts)
    if isinstance(node, dict) and isinstance(node.get("column"), str):
        return _evaluate_predicate(node, row)
    return "true"


def filter_rows(rows, filters):
    """The rows a WHERE over `filters` keeps: only "true" survives."""
    return [r for r in rows if _and3(evaluate_node(n, r) for n in (filters or [])) == "true"]


def is_constraint(node):
    """sql-generator.ts isConstraint."""
    return node.get("latest") is not True and node.get("column") != "latest_flag" and \
        (is_group(node) or bool(node.get("column")))


def collect_leaf_columns(node):
    if is_latest_marker(node):
        return []
    if is_group(node):
        return [c for child in node["filters"] for c in collect_leaf_columns(child)]
    if isinstance(node.get("column"), str):
        return [node["column"]]
    return []


# --------------------------------------------------------------- templates

class Column:
    def __init__(self, d):
        self.raw = dict(d)
        self.id = d["id"]
        self.data_type = d["dataType"]
        self.path = d.get("path") or ""
        self.secret = d.get("secret") is True


class TableDef:
    """A data-template table, or a transform when sql/rows are given."""

    def __init__(self, d, transform=False):
        self.raw = dict(d)
        self.tablename = d["tablename"]
        self.description = d.get("description", "")
        self.columns = [Column(c) for c in d.get("columns", [])]
        self.entity_key = list(d.get("maintainLatestFlagFor") or [])
        self.private = d.get("private") is True
        self.transform = transform
        # Harness-only: the chunk budget of this table's reads (default:
        # fleetdb's CHUNK_BYTES), so a test can see the chunked envelope with
        # a few small rows.
        self.chunk_bytes = d.get("x-chunk-bytes")
        # Harness-only: the rows a transform's view yields, in view order.
        self.view_rows = list(d.get("x-rows") or [])
        # A transform declared materialize: true is a materialized view,
        # which a tick refreshes before reading it.
        self.materialize = d.get("materialize") is True
        if not transform and not any(c.id == "tsp" for c in self.columns):
            raise ValueError("table %s: a data template table needs a tsp column" % self.tablename)

    def column(self, cid):
        for c in self.columns:
            if c.id == cid:
                return c
        return None

    def secret_ids(self):
        return [c.id for c in self.columns if c.secret]

    def catalog_entry(self):
        """RESWARM appaccess.ts catalogEntry (JSON drops an absent field)."""
        return {k: self.raw[k] for k in ("tablename", "description", "columns") if k in self.raw}


# ErrorService.ts errorLogTableDefinition; error-logs is created WITH an
# `id SERIAL` column (withSerialId) and is private for cross-app readers.
ERROR_LOGS = {
    "tablename": "error-logs",
    "columns": [
        {"id": "tsp", "dataType": "timestamp", "name": "", "description": "", "path": ""},
        {"id": "msg", "dataType": "string", "name": "", "description": "", "path": ""},
        {"id": "user_message", "dataType": "string", "name": "", "description": "", "path": ""},
        {"id": "source", "dataType": "string", "name": "", "description": "", "path": ""},
        {"id": "level", "dataType": "string", "name": "", "description": "", "path": ""},
    ],
}


# --------------------------------------------------------------- lodash.get

def _lodash_path(path):
    """lodash's stringToPath: "args[0].a", "kwargs['b c']" -> keys."""
    keys = []
    for m in re.finditer(r"[^.[\]]+|\[(?:(['\"])(.*?)\1|([^\]]*))\]", path):
        if m.group(0).startswith("["):
            keys.append(m.group(2) if m.group(1) else m.group(3))
        else:
            keys.append(m.group(0))
    return keys


def lodash_get(data, path):
    value = data
    for key in _lodash_path(path):
        if isinstance(value, dict):
            if key not in value:
                return UNDEFINED
            value = value[key]
        elif isinstance(value, list):
            if not re.fullmatch(r"\d+", key) or int(key) >= len(value):
                return UNDEFINED
            value = value[int(key)]
        else:
            return UNDEFINED
    return value


# ------------------------------------------------------------- the backend

class Backend:
    """One app databackend (one per-app data realm): its template, its rows
    and the procedures fleetdb registers for it.

    publish(topic, args, exclude_authrole) is called for every realtime
    republish; it must not block. now() returns epoch seconds. fixtures
    ({table: [rows]}) are stored at every reset, like rows the writer
    ((kwargs, details) of a publish) wrote; the insert hook writes as the
    same writer.
    """

    def __init__(self, swarm, app, stage, template, platform, publish, now=time.time, fixtures=None,
                 writer=None):
        self.swarm, self.app, self.stage = swarm, app, stage.upper()
        self.platform = platform
        self.publish = publish
        self.now = now
        self.lock = threading.RLock()
        self.tables = {}
        for t in template.get("tables", []):
            d = TableDef(t)
            self.tables[d.tablename] = d
        self.error_logs = TableDef(ERROR_LOGS)
        self.transforms = {t["tablename"]: TableDef(t, transform=True) for t in template.get("transforms", [])}
        self.fixtures = fixtures or {}
        self.writer = writer or ({}, {})
        self.reset()

    # ------------------------------------------------------------ state

    def reset(self):
        with self.lock:
            self.rows = {name: [] for name in list(self.tables) + ["error-logs"]}
            self.seq = 0
            self.error_log_id = 0
            self.chunk_override = {}
            self.refusal = None
            self.secret_calls = {}
            for table, rows in self.fixtures.items():
                self.insert(table, rows)

    def table_def(self, name):
        return self.error_logs if name == "error-logs" else self.tables.get(name)

    def readable_defs(self):
        return list(self.tables.values()) + [self.error_logs]

    # ------------------------------------------------------- registration

    def procedures(self):
        """(uri, handler) in fleetdb's registration order (DataBackend.register)."""
        s, a = self.swarm, self.app
        out = []
        for t in self.readable_defs():
            n = t.tablename
            out += [("history.transformed." + n, "history"),
                    ("history.transformed.series." + n, "series"),
                    ("append.%d.%d.%s" % (s, a, n), "append"),
                    ("appendBulk.%d.%d.%s" % (s, a, n), "append_bulk")]
            if t.secret_ids():
                out += [("secret.reveal." + n, "reveal"), ("secret.verify." + n, "verify")]
        for n in self.transforms:
            out.append(("history.transformed." + n, "transform"))
            out.append(("transform.refresh." + n, "refresh"))
        out += [("sys.appaccess.resolve", "resolve"), ("sys.appaccess.list", "list")]
        return out

    def write_topics(self):
        """(topic, table, bulk) fleetdb subscribes (subscribeTableWrites)."""
        out = []
        for t in self.readable_defs():
            out.append(("%d.%d.%s" % (self.swarm, self.app, t.tablename), t.tablename, False))
            out.append(("bulk.%d.%d.%s" % (self.swarm, self.app, t.tablename), t.tablename, True))
        return out

    def call(self, uri, args, kwargs, details):
        """Answers one registered procedure; raises WampError."""
        for proc, handler in self.procedures():
            if proc == uri:
                return getattr(self, "_rpc_" + handler)(uri, list(args), dict(kwargs or {}), details or {})
        raise WampError("wamp.error.no_such_procedure", ["no callee registered for procedure <%s>" % uri])

    def event(self, topic, args, kwargs, details):
        """One publish on a write topic. Errors are swallowed, as fleetdb's
        subscription handlers log them and drop the row."""
        for t, table, bulk in self.write_topics():
            if t == topic:
                try:
                    if bulk:
                        self._insert_bulk(self.table_def(table), list(args), dict(kwargs or {}), details or {})
                    else:
                        self._insert_row(self.table_def(table), list(args), dict(kwargs or {}), details or {})
                except (WampError, _StorageRefused):
                    pass  # logged by fleetdb, the row is dropped
                return

    # ----------------------------------------------------------- writes

    def _check_allowance(self):
        if self.refusal:
            raise _StorageRefused(self.refusal)

    def _transform_row(self, table, args, kwargs, details):
        """DataBackend.transformRow."""
        data = {"args": args, "kwargs": kwargs, "details": details}
        res = {}
        for c in table.columns:
            v = lodash_get(data, c.path or "args[0]." + c.id)
            if v is not UNDEFINED:
                res[c.id] = v
        raw_device_key = kwargs.get("DEVICE_KEY") if kwargs.get("DEVICE_KEY") is not None else (
            args[0].get("DEVICE_KEY") if args and isinstance(args[0], dict) else None)
        device_key = NAN if raw_device_key is None else js_number(raw_device_key)
        res["device_key"] = None if (isinstance(device_key, float) and math.isnan(device_key)) else device_key
        # controlAuthz.ts resolvePublisherAuthid: publisher_authid ?? caller_authid.
        authid = details.get("publisher_authid")
        res["authid"] = authid if authid is not None else details.get("caller_authid")
        for c in table.columns:
            if not c.secret:
                continue
            value = res.get(c.id)
            if value is None:
                continue
            text = js_string(value)
            if text in (SECRET_PLACEHOLDER, SECRET_MASK_UI):
                if not self._can_carry_over(table, res):
                    raise WampError("sys.dataservice.error.secret_sentinel_unresolvable", [
                        "Secret column '%s' of '%s' received the keep-previous sentinel, but there is no "
                        "entity identity to resolve it (the table declares no maintainLatestFlagFor, or an "
                        "entity-key column is absent/null in this payload). Send the real value, null to "
                        "clear, or omit the field." % (c.id, table.tablename)])
                del res[c.id]  # kept: inherited from the entity's latest row
                continue
            if text.startswith(SECRET_PREFIX):
                raise WampError("sys.dataservice.error.secret_ciphertext_rejected", [
                    "Secret column '%s' of '%s' received an already-encrypted '%s…' value. Ciphertext "
                    "cannot be written back — send the plaintext, null to clear, or omit the field to keep "
                    "the current value." % (c.id, table.tablename, SECRET_PREFIX)])
            res[c.id] = text
        if "tsp" not in res:
            # A plain Error ("'tsp' missing in publish payload <t>"): on the
            # append path autobahn-js answers runtime_error [{}].
            raise runtime_error({})
        return res

    def _can_carry_over(self, table, res):
        if not table.entity_key:
            return False
        return all(k == "device_key" or res.get(k) is not None for k in table.entity_key)

    def _needs_carry_over(self, table, res):
        return self._can_carry_over(table, res) and any(c.id not in res for c in table.columns)

    def _stored_value(self, column, raw):
        """sql-generator.ts formatColumnValueSQL, then the node-postgres
        type parser reading the stored value back."""
        if raw is None:
            return None
        dt = column.data_type
        if dt == "boolean":
            if raw == "true" or raw is True or (is_number(raw) and raw == 1):
                return True
            if raw == "false" or raw is False or (is_number(raw) and raw == 0):
                return False
            return None
        if dt == "bigint":
            if isinstance(raw, bool) or not (isinstance(raw, str) or is_number(raw)):
                return None
            text = js_string(raw).strip()
            return int(text) if re.fullmatch(r"-?\d+", text) else None
        if dt == "numeric":
            n = js_number(raw)
            if isinstance(n, float) and not math.isfinite(n):
                return None
            return js_parse_float(js_string(n))
        if dt == "json" or isinstance(raw, (dict, list)):
            if dt != "json":
                raise pg_error("42804")  # datatype_mismatch: jsonb into a text/timestamp column
            return json.loads(json.dumps(raw))
        if dt == "timestamp":
            if is_number(raw):
                if not math.isfinite(raw):
                    raise pg_error("22008")  # datetime_field_overflow
                return int(math.floor(raw))  # to_timestamp(ms / 1000), read back as ms
            if isinstance(raw, str) and raw.strip() != "":
                ms = parse_iso_ms(raw)
                if ms is None:
                    raise pg_error("22007")  # invalid_datetime_format
                return ms
            return None
        return js_string(raw)

    def _store(self, table, res):
        """The stored row (every physical column) of a transformed row, as
        the INSERT writes it and node-postgres reads it back; raises where
        the INSERT would fail."""
        row = {}
        if table is self.error_logs:
            row["id"] = None  # SERIAL, assigned once the row is accepted
        for c in table.columns:
            row[c.id] = self._stored_value(c, res[c.id]) if c.id in res else None
        dk = res.get("device_key")
        # device_key bigint NOT NULL DEFAULT -1; formatRowValuesTuple sends -1 for null.
        row["device_key"] = -1 if dk is None else (int(dk) if float(dk).is_integer() else dk)
        row["authid"] = res.get("authid")
        return row

    def _previous_entity_row(self, table, row, extra=()):
        candidates = [r for r in list(self.rows[table.tablename]) + list(extra)
                      if all(r.get(k) == row.get(k) for k in table.entity_key)]
        if not candidates:
            return None
        return max(candidates, key=lambda r: (r["tsp"], r["_seq"]))

    def _check_entity_conflict(self, table, row, extra=()):
        """The UNIQUE (entity key..., tsp) index of an entity table."""
        if not table.entity_key:
            return
        for r in list(self.rows[table.tablename]) + list(extra):
            if r["tsp"] == row["tsp"] and all(r.get(k) == row.get(k) for k in table.entity_key):
                raise WampError("sys.dataservice.error.entity_key_conflict", [
                    "Write to '%s' rejected: two rows carry the same (%s, tsp). This table declares an "
                    "entity key, so one entity can hold only one row per timestamp \u2014 otherwise 'latest' "
                    "reads of it would be ambiguous. Give each row of the batch its own tsp, or send one row "
                    "per entity." % (table.tablename, ", ".join(table.entity_key))])

    def _build(self, table, res, extra=()):
        """(stored row, republish base) of one transformed row. An entity
        row with absent columns goes through the merge INSERT: the absent
        columns are inherited from the entity's latest row and the dense
        stored row is what fleetdb republishes; otherwise it republishes the
        transformed row itself (only the columns the payload carried)."""
        row = self._store(table, res)
        merged = self._needs_carry_over(table, res)
        if merged:
            previous = self._previous_entity_row(table, row, extra)
            if previous is not None:
                for c in table.columns:
                    if c.id not in res:
                        row[c.id] = previous.get(c.id)
        if row.get("tsp") is None:
            raise pg_error("23502")  # not_null_violation: tsp
        self._check_entity_conflict(table, row, extra)
        self.seq += 1
        row["_seq"] = self.seq
        base = {k: v for k, v in row.items() if not k.startswith("_")} if merged else dict(res)
        return row, base

    def _accept(self, table, rows):
        for row in rows:
            if table is self.error_logs:
                self.error_log_id += 1
                row["id"] = self.error_log_id
        self.rows[table.tablename].extend(rows)

    def _republish(self, table, base):
        """The row processAndInsertRow / processAndInsertBulk publishes:
        typed per template column (coerceToDataType), secrets masked."""
        out = dict(base)
        for c in table.columns:
            if c.id in out:
                out[c.id] = _coerce_to_data_type(out[c.id], c.data_type)
        for sid in table.secret_ids():
            if out.get(sid) is not None:
                out[sid] = SECRET_PLACEHOLDER
        return wire_value(out)

    def _publish_options(self, table):
        """readPublishOptions: private-for-readers definitions exclude app_reader."""
        return ["app_reader"] if (table.private or table is self.error_logs) else None

    def _insert_row(self, table, args, kwargs, details):
        """processAndInsertRow."""
        with self.lock:
            self._check_allowance()
            res = self._transform_row(table, args, kwargs, details)
            row, base = self._build(table, res)
            self._accept(table, [row])
            event = self._republish(table, base)
        self.publish("transformed." + table.tablename, [event], self._publish_options(table))

    def _insert_bulk(self, table, args, kwargs, details):
        """processAndInsertBulk: every row transformed first, all-or-nothing."""
        with self.lock:
            self._check_allowance()
            rows = args[0] if args else None
            if not isinstance(rows, list) or not rows:
                # A plain Error ("bulk insert for <t> expects args[0] to be a
                # non-empty array of rows"): runtime_error [{}].
                raise runtime_error({})
            transformed = [self._transform_row(table, [r], kwargs, details) for r in rows]
            built = []
            for res in transformed:
                built.append(self._build(table, res, [row for row, _ in built]))
            self._accept(table, [row for row, _ in built])
            events = [self._republish(table, base) for _, base in built]
        self.publish("transformed.bulk." + table.tablename, [events], self._publish_options(table))
        return len(built)

    def _rpc_append(self, uri, args, kwargs, details):
        table = self.table_def(uri.split(".", 3)[3])
        try:
            self._insert_row(table, args, kwargs, details)
        except _StorageRefused as e:
            raise e.wamp_error()
        return {"success": True}

    def _rpc_append_bulk(self, uri, args, kwargs, details):
        table = self.table_def(uri.split(".", 3)[3])
        try:
            count = self._insert_bulk(table, args, kwargs, details)
        except _StorageRefused as e:
            raise e.wamp_error()
        return {"success": True, "count": count}

    # ------------------------------------------------------------ reads

    def _assert_readable(self, table, details):
        if (table.private or table is self.error_logs) and details.get("caller_authrole") == "app_reader":
            raise WampError("wamp.error.not_authorized", ["'%s' is private and not readable cross-app" % table.tablename])

    def _sanitize_filters(self, nodes, table, implicit_system_columns=True):
        """addFilterColumnDataTypes + sanitizeFilterNodes."""
        if not isinstance(nodes, list):
            return nodes
        self._assert_no_mixed(nodes)
        return self._sanitize_nodes(nodes, table, implicit_system_columns)

    def _assert_no_mixed(self, nodes):
        for node in nodes or []:
            if not isinstance(node, dict):
                continue
            has_group = node.get("combinator") is not None or node.get("filters") is not None
            has_leaf = node.get("column") is not None or node.get("operator") is not None
            if has_group and has_leaf:
                raise WampError("sys.dataservice.error.invalid_filter", [
                    "A filter entry is both a group and a predicate. Send either {combinator, filters} or "
                    "{column, operator, value}, never both."])
            if has_group and isinstance(node.get("filters"), list):
                self._assert_no_mixed(node["filters"])

    def _sanitize_nodes(self, nodes, table, implicit):
        kept = []
        for node in nodes:
            if is_group(node):
                children = self._sanitize_nodes(node.get("filters") or [], table, implicit)
                if children:
                    kept.append({**node, "filters": children})
                continue
            if node.get("latest") is True:
                kept.append(node)
                continue
            column = node.get("column")
            if not column:
                continue
            node = dict(node)
            if node.get("dataType") is not None and node.get("dataType") not in DATA_TYPES:
                node["dataType"] = None
            if implicit and column in ("latest_flag", "device_key", "authid"):
                if column == "latest_flag":
                    node["dataType"] = "boolean"
                    kept.append(node)
                elif column == "device_key":
                    node["dataType"] = "numeric"
                    if self._valid_filter_value(node):
                        kept.append(node)
                else:
                    node["dataType"] = "string"
                    kept.append(node)
                continue
            base, path = parse_column_path(column)
            col = table.column(base)
            if col is None:
                continue
            if col.secret:
                raise WampError("sys.dataservice.error.secret_column", [
                    "Column '%s' is encrypted and cannot be used in a filter. Use the SDK's "
                    "verifySecret()/verify_secret() to test a value instead." % base])
            if path and col.data_type == "json":
                if not node.get("dataType") or node.get("dataType") == "json":
                    node["dataType"] = infer_leaf_data_type(node.get("value"))
            else:
                node["dataType"] = col.data_type
            if self._valid_filter_value(node):
                kept.append(node)
        return kept

    def _valid_filter_value(self, node):
        op = node.get("operator")
        if op in ("LIKE", "ILIKE", "NOT LIKE", "NOT ILIKE", "IS NULL", "IS NOT NULL"):
            return True
        if node.get("dataType") not in ("numeric", "bigint"):
            return True
        parts = set_elements(node.get("value")) if op in ("IN", "NOT IN") else [node.get("value")]
        return all(coerce(p, node["dataType"]) is not None for p in parts)

    def _sanitize_column_paths(self, query, table):
        paths = query.get("columnPaths")
        if not paths:
            return None
        whole, kept = set(), []
        for ref in paths:
            if not ref or ref.strip() == "":
                continue
            base, path = parse_column_path(ref)
            if not path:
                whole.add(base)
                continue
            col = table.column(base)
            if col is None or col.secret or col.data_type != "json":
                continue
            if base not in (query.get("columns") or []):
                continue
            kept.append(ref)
        result = [r for r in kept if parse_column_path(r)[0] not in whole]
        return result or None

    def _select(self, table, limit, offset, time_range, filters, columns, column_paths):
        """generateSelectTableSQL over the stored rows: newest first."""
        filters = filters or []
        want_latest = any(f.get("latest") is True for f in filters) or any(
            f.get("column") == "latest_flag" and f.get("value") in (True, "true", 1, "1") for f in filters)
        predicates = [f for f in filters if is_constraint(f)]
        rows = list(self.rows[table.tablename])

        def in_time(r):
            start = time_range[0] if time_range else None
            end = time_range[1] if time_range else None
            if _truthy(start) and not r["tsp"] >= _sql_seconds(start) * 1000:
                return False
            if _truthy(end) and not r["tsp"] < _sql_seconds(end) * 1000:
                return False
            return True

        newest_first = lambda rs: sorted(rs, key=lambda r: (r["tsp"], r["_seq"]), reverse=True)
        if want_latest and table.entity_key:
            keyset = set(table.entity_key)
            inside = [n for n in predicates if collect_leaf_columns(n) and all(c in keyset for c in collect_leaf_columns(n))]
            outside = [n for n in predicates if collect_leaf_columns(n) and not all(c in keyset for c in collect_leaf_columns(n))]
            candidates = [r for r in rows if all(r.get(k) is not None for k in table.entity_key)]
            candidates = filter_rows(candidates, inside)
            latest = {}
            for r in newest_first(candidates):
                latest.setdefault(tuple(json.dumps(r.get(k), sort_keys=True) for k in table.entity_key), r)
            selected = [r for r in latest.values() if in_time(r)]
            selected = newest_first(filter_rows(selected, outside))
            selected = selected[offset:offset + limit]
        elif want_latest:
            selected = newest_first(filter_rows([r for r in rows if in_time(r)], predicates))[:1]
        else:
            selected = newest_first(filter_rows([r for r in rows if in_time(r)], predicates))
            selected = selected[offset:offset + limit]
        return [self._project(table, r, columns, column_paths, want_latest) for r in selected]

    def _project(self, table, row, columns, column_paths, want_latest):
        projected = [c for c in (columns or []) if c and c.strip() != "" and c != "latest_flag"]
        physical = ([("id")] if table is self.error_logs else []) + [c.id for c in table.columns] + ["device_key", "authid"]
        if not projected:
            return {k: row.get(k) for k in physical}
        wanted = list(dict.fromkeys(projected + ["tsp", "device_key", "authid"] +
                                    ([k for k in table.entity_key if k] if want_latest else [])))
        out = {}
        trees = _path_trees(column_paths or [])
        for c in wanted:
            if c not in physical:
                raise pg_error("42703")  # undefined_column
            v = row.get(c)
            if c in trees and not (want_latest and c in table.entity_key):
                v = _prune(v, trees[c])
            out[c] = v
        return out

    def _chunk_budget(self, table):
        if table.tablename in self.chunk_override:
            return self.chunk_override[table.tablename]
        return table.chunk_bytes or CHUNK_BYTES

    def _respond_rows(self, table, details, rows, secret_ids):
        """DataBackend.respondRows over rows that arrive newest first:

          - they fit one chunk: the plain array, reversed into ascending order
          - larger, the caller set receive_progress: each chunk goes out as a
            progressive result [chunkIndex, rows] (rows still newest first)
            as soon as it is full, then the final result
            {chunked: true, chunkCount, totalRows, order: "asc"}: concatenate
            the chunks and reverse to get the single-shot answer
          - larger, no receive_progress: result_too_large

        A single row over the chunk budget, or a total over the runaway
        guard, is result_too_large either way, even after chunks went out.
        """
        progress = details.get("progress")
        budget = self._chunk_budget(table)
        for r in rows:
            for sid in secret_ids:
                if r.get(sid) is not None:
                    r[sid] = SECRET_PLACEHOLDER
        key_overhead = None
        pending = total_bytes = total_rows = chunk_count = 0
        column_bytes = {}
        buffered = []

        def too_large(limit_bytes, detail):
            top = sorted(column_bytes.items(), key=lambda kv: -kv[1])[:3]
            top = [{"name": n, "bytes": b, "pct": int(round(b / max(total_bytes, 1) * 100))} for n, b in top]
            culprit = (" Column '%s' accounts for %d%%." % (top[0]["name"], top[0]["pct"])) if top else ""
            return WampError("sys.dataservice.error.result_too_large", [
                "This query returned %d MB (%d rows).%s Narrow the time range, lower the limit, or exclude "
                "that column." % (int(round(total_bytes / (1024 * 1024))), total_rows, culprit)],
                {"bytes": total_bytes, "rows": total_rows, "tablename": table.tablename,
                 "limitBytes": limit_bytes, "topColumns": top, "detail": detail})

        for row in rows:
            row = wire_value(row)
            if key_overhead is None:
                key_overhead = sum(len(k) + 4 for k in row)
            size = estimate_row_bytes(row, key_overhead, column_bytes)
            total_bytes += size
            total_rows += 1
            if size > budget:
                raise too_large(budget, "single row exceeds the chunk budget")
            if total_bytes > MAX_RESULT_BYTES:
                raise too_large(MAX_RESULT_BYTES, "result exceeds the runaway guard")
            if pending + size > budget:
                if progress is None:
                    raise too_large(budget, "caller cannot receive progressive results")
                progress(chunk_count, buffered)
                chunk_count += 1
                buffered, pending = [], 0
            buffered.append(row)
            pending += size
        if chunk_count == 0:
            return list(reversed(buffered))
        progress(chunk_count, buffered)
        chunk_count += 1
        return {"chunked": True, "chunkCount": chunk_count, "totalRows": total_rows, "order": "asc"}

    def _rpc_history(self, uri, args, kwargs, details):
        """registerSelectTable."""
        table = self.table_def(uri[len("history.transformed."):])
        self._assert_readable(table, details)
        query = dict(typia_assert("table", args))
        tr = query.get("timeRange")
        time_range = [js_date_ms(tr[0]) if tr[0] is not None else None,
                      js_date_ms(tr[1]) if tr[1] is not None else None] if tr else None
        with self.lock:
            filters = self._sanitize_filters(query.get("filterAnd"), table)
            paths = self._sanitize_column_paths(query, table)
            rows = self._select(table, query["limit"], query.get("offset") or 0, time_range,
                                filters, query.get("columns"), paths)
        return self._respond_rows(table, details, rows, table.secret_ids())

    def _rpc_transform(self, uri, args, kwargs, details):
        """registerSelectTransform: queryView, Limit/Offset/FilterAnd only."""
        transform = self.transforms[uri[len("history.transformed."):]]
        self._assert_readable(transform, details)
        query = typia_assert("table", args)
        if query["limit"] > TRANSFORM_LIMIT_CAP:
            raise WampError("sys.dataservice.error.invalid_limit",
                            ["limit must not exceed %d (got %s)" % (TRANSFORM_LIMIT_CAP, js_string(query["limit"]))])
        filters = self._sanitize_filters(query.get("filterAnd"), transform, implicit_system_columns=False)
        predicates = [f for f in (filters or []) if is_constraint(f)]
        rows = filter_rows([dict(r) for r in transform.view_rows], predicates)
        offset = query.get("offset") or 0
        rows = rows[offset:offset + query["limit"]]
        return self._respond_rows(transform, details, rows, transform.secret_ids())

    def _rpc_refresh(self, uri, args, kwargs, details):
        """registerRefreshTransform: one transform tick on demand, the body
        of the cron tick. App containers only (the caller gate on
        details.caller_authrole, as for the secret procedures); the arguments
        are ignored, so [] and [{}] (what the SDKs send) are the same call.
        The tick: REFRESH MATERIALIZED VIEW when the transform is
        materialized (always run here: the write-generation skip, the overlap
        guard and the refresh semaphore are not modeled), then the view — at
        most 3000 rows, reversed, secrets redacted — published on
        transformed.<t> with the read publish options, as the cron tick
        publishes it (always: the subscriber gate is not modeled). The result
        is the wire contract's {success, materialized, refreshed, skipped,
        published, rows}."""
        transform = self.transforms[uri[len("transform.refresh."):]]
        role = details.get("caller_authrole")
        if role != "app":
            raise WampError("wamp.error.not_authorized", [
                "transform.refresh is callable only by the app's own containers (caller role '%s')" % (role or "unknown")])
        rows = [dict(r) for r in transform.view_rows[:TRANSFORM_LIMIT_CAP]]
        rows.reverse()
        for r in rows:
            for sid in transform.secret_ids():
                if r.get(sid) is not None:
                    r[sid] = SECRET_PLACEHOLDER
        rows = [wire_value(r) for r in rows]
        self.publish("transformed." + transform.tablename, [rows], self._publish_options(transform))
        return {"success": True, "materialized": transform.materialize, "refreshed": transform.materialize,
                "skipped": False, "published": True, "rows": len(rows)}

    # ----------------------------------------------------------- series

    def _validate_series_columns(self, query, table):
        """validateSeriesColumns."""
        def data_type_of(base):
            if base == "device_key":
                return "bigint"
            if base == "authid":
                return "string"
            c = table.column(base)
            return c.data_type if c else None

        def invalid(uri, reason):
            return WampError("sys.dataservice.error." + uri, [reason])

        secret = set(table.secret_ids())
        group_set = list(dict.fromkeys(query.get("groupBy") or []))
        for m in query["metrics"]:
            ref, method = m["ref"], m["method"]
            base, path = parse_column_path(ref)
            if len(("%s:%s" % (method, ref)).encode()) > 63:
                raise invalid("invalid_metric", "metric ref '%s' is too long — '%s:%s' must fit 63 bytes" % (ref, method, ref))
            if base == "tsp":
                if method != "COUNT" or path:
                    raise invalid("invalid_metric", "'tsp' is the bucket column; only COUNT(tsp) (row count) is allowed")
                continue
            dt = data_type_of(base)
            if dt is None:
                raise invalid("invalid_metric", "metric ref '%s' is not a column of this table" % ref)
            if base in secret:
                raise invalid("invalid_metric", "'%s' is an encrypted column and cannot be used as a metric — "
                              "encrypted values are randomized per row, so aggregating or grouping on them is "
                              "meaningless" % ref)
            if path and dt != "json":
                raise invalid("invalid_metric", "metric ref '%s': '%s' is not a json column" % (ref, base))
            if not path:
                if method in ("AVG", "SUM") and dt not in ("numeric", "bigint"):
                    raise invalid("invalid_metric", "%s requires a numeric column, '%s' is %s" % (method, ref, dt))
                if method in ("MIN", "MAX") and dt not in ("numeric", "bigint", "string", "timestamp"):
                    raise invalid("invalid_metric", "%s is not defined on %s column '%s'" % (method, dt, ref))
            if ref in group_set:
                raise invalid("invalid_group_by", "'%s' cannot be both a metric and a groupBy column" % ref)
        for g in group_set:
            base, path = parse_column_path(g)
            if len(g.encode()) > 63:
                raise invalid("invalid_group_by", "groupBy ref '%s' is too long — must fit 63 bytes" % g)
            if base == "tsp":
                raise invalid("invalid_group_by", "'tsp' is the bucket column, not a groupBy column")
            dt = data_type_of(base)
            if dt is None:
                raise invalid("invalid_group_by", "groupBy ref '%s' is not a column of this table" % g)
            if base in secret:
                raise invalid("invalid_group_by", "'%s' is an encrypted column and cannot be used as a groupBy "
                              "column — encrypted values are randomized per row, so aggregating or grouping "
                              "on them is meaningless" % g)
            if path and dt != "json":
                raise invalid("invalid_group_by", "groupBy ref '%s': '%s' is not a json column" % (g, base))

    def _rpc_series(self, uri, args, kwargs, details):
        """registerSelectSeries."""
        table = self.table_def(uri[len("history.transformed.series."):])
        self._assert_readable(table, details)
        query = dict(typia_assert("series", args))
        tr = query.get("timeRange")
        time_range = [js_date_ms(tr[0]) if tr[0] is not None else None,
                      js_date_ms(tr[1]) if tr[1] is not None else None] if tr is not None else None
        start = time_range[0] if time_range else None
        if time_range is None or start is None or not math.isfinite(start):
            raise WampError("sys.dataservice.error.invalid_time_range", ["series queries require a timeRange with a start bound"])
        end = time_range[1]
        if end is not None and (not math.isfinite(end) or end <= start):
            raise WampError("sys.dataservice.error.invalid_time_range", ["timeRange end must be after start"])
        self._validate_series_columns(query, table)
        with self.lock:
            filters = self._sanitize_filters(query.get("filterAnd"), table)
            rows = self._series_rows(table, query, start, end, filters)
        if len(rows) > SERIES_ROW_CAP:
            raise WampError("sys.dataservice.error.series_too_many_groups", [
                "series result exceeds %d rows — reduce the bucket count, narrow the time range, or group "
                "by a lower-cardinality column" % SERIES_ROW_CAP], {"tablename": table.tablename, "cap": SERIES_ROW_CAP})
        return self._respond_rows(table, details, rows, table.secret_ids())

    def _series_rows(self, table, query, start, end, filters):
        """generateSelectSeriesSQL: ORDER BY bucket DESC, groups ASC."""
        now_ms = self.now() * 1000
        end_ms = end if end is not None else now_ms
        limit = query["limit"]
        bucket_ms = query.get("bucketMs")
        origin_us = round(start * 1000)
        if bucket_ms is not None:
            requested = math.ceil((end_ms - start) / bucket_ms)
            width_us = round(bucket_ms * max(1, math.ceil(requested / limit)) * 1000)
        else:
            width_us = round((end_ms - start) * 1000 / limit)
        width_us = max(width_us, 1)
        predicates = [f for f in (filters or []) if is_constraint(f)]
        rows = [r for r in self.rows[table.tablename] if start <= r["tsp"] < end_ms]
        rows = filter_rows(rows, predicates)
        groups = list(dict.fromkeys(query.get("groupBy") or []))
        metrics = []
        for m in query["metrics"]:
            alias = "%s:%s" % (m["method"], m["ref"])
            if alias not in [a for a, _, _ in metrics]:
                metrics.append((alias, m["ref"], m["method"]))
        buckets = {}
        for r in sorted(rows, key=lambda r: (r["tsp"], r["_seq"])):
            tsp_us = r["tsp"] * 1000
            bucket_us = origin_us + ((tsp_us - origin_us) // width_us) * width_us
            gvals = tuple(_series_value(r, g) for g in groups)
            key = (bucket_us, json.dumps(gvals, sort_keys=True, default=str))
            buckets.setdefault(key, (bucket_us, gvals, []))[2].append(r)
        out = []
        for bucket_us, gvals, members in buckets.values():
            row = {"tsp": int(bucket_us // 1000)}
            for g, v in zip(groups, gvals):
                row[g] = v
            for alias, ref, method in metrics:
                row[alias] = _aggregate(method, ref, members, table)
            out.append(row)

        def group_key(row):
            return tuple((row[g] is None, _sort_value(row[g])) for g in groups)
        out.sort(key=group_key)
        out.sort(key=lambda r: r["tsp"], reverse=True)
        return out

    # ---------------------------------------------------------- secrets

    def _assert_secret_caller(self, details, operation):
        role = details.get("caller_authrole")
        if role != "app":
            raise WampError("wamp.error.not_authorized", [
                "secret.%s is callable only by the app's own containers (caller role '%s')" % (operation, role or "unknown")])

    def _rate_limit(self, details, operation):
        key = "%s|%s" % (operation, details.get("caller_authid") or "unknown")
        now = self.now()
        start, count = self.secret_calls.get(key, (None, 0))
        if start is None or now - start >= 60:
            self.secret_calls[key] = (now, 1)
            return
        count += 1
        self.secret_calls[key] = (start, count)
        if count > SECRET_RATE_LIMITS[operation]:
            raise WampError("sys.dataservice.error.rate_limited", [
                "secret.%s is limited to %d calls per minute" % (operation, SECRET_RATE_LIMITS[operation])])

    def _secret_rows(self, table, query, projection):
        filters = self._sanitize_filters(query.get("filterAnd"), table)
        paths = self._sanitize_column_paths(query, table)
        tr = query.get("timeRange")
        time_range = [js_date_ms(tr[0]) if tr[0] is not None else None,
                      js_date_ms(tr[1]) if tr[1] is not None else None] if tr else None
        # selectSecretRows calls selectTable directly: no respondRows, so the
        # rows stay newest first and nothing is redacted or chunked.
        return self._select(table, query["limit"], query.get("offset") or 0, time_range, filters, projection, paths)

    def _rpc_reveal(self, uri, args, kwargs, details):
        table = self.table_def(uri[len("secret.reveal."):])
        self._assert_readable(table, details)
        self._assert_secret_caller(details, "reveal")
        query = typia_assert("reveal", args)
        secret_ids = table.secret_ids()
        requested = [c for c in (query.get("columns") or []) if c and c.strip() != ""]
        projection = list(dict.fromkeys(requested + secret_ids)) if requested else None
        with self.lock:
            self._rate_limit(details, "reveal")
            rows = self._secret_rows(table, query, projection)
        return [wire_value(r) for r in rows]

    def _rpc_verify(self, uri, args, kwargs, details):
        table = self.table_def(uri[len("secret.verify."):])
        self._assert_readable(table, details)
        self._assert_secret_caller(details, "verify")
        query = typia_assert("verify", args)
        column = table.column(query["column"])
        if column is None or not column.secret:
            raise WampError("sys.dataservice.error.not_a_secret_column",
                            ["'%s' is not a secret column of '%s'" % (query["column"], table.tablename)])
        with self.lock:
            self._rate_limit(details, "verify")
            rows = self._secret_rows(table, query, [column.id, "tsp"])
        hits = sum(1 for r in rows if isinstance(r.get(column.id), str) and r[column.id] == query["candidate"])
        return {"match": hits > 0, "checked": len(rows)}

    # -------------------------------------------------------- appaccess

    def _rpc_resolve(self, uri, args, kwargs, details):
        """sys.appaccess.resolve: fleetdb's proxy to RESWARM's resolve. The
        consumer is the realm's app; the answer echoes `app` as the caller
        spelled it ({...resolved, app}); RESWARM's errors pass through."""
        app = typia_assert("resolve", args)["app"]
        resolved = self._upstream(lambda: self.platform.appaccess_resolve(self.swarm, self.app, app))
        return {**resolved, "app": app}

    def _rpc_list(self, uri, args, kwargs, details):
        """sys.appaccess.list: RESWARM's answer as is."""
        return self._upstream(lambda: self.platform.appaccess_list(self.swarm, self.app))

    @staticmethod
    def _upstream(fn):
        try:
            return fn()
        except WampError:
            raise
        except Exception as e:  # an autobahn.Error from RESWARM: URI and its message
            if isinstance(getattr(e, "error", None), str):
                raise WampError(e.error, [getattr(e, "message", str(e))]) from e
            raise

    def catalog(self):
        """The non-private catalog of this databackend (RESWARM
        nonPrivateCatalog over the installed template)."""
        return {"tables": [t.catalog_entry() for t in self.tables.values() if not t.private],
                "transforms": [t.catalog_entry() for t in self.transforms.values() if not t.private]}

    # ------------------------------------------------------- test hooks

    def insert(self, table, rows):
        """Stores rows through the write path (tsp required) as the writer
        would publish them, without republishing; returns how many."""
        t = self.table_def(table)
        if t is None:
            raise WampError("test.error.no_such_table", [table])
        if not isinstance(rows, list):
            raise WampError("test.error.invalid_argument", ["rows must be a list"])
        kwargs, details = self.writer
        with self.lock:
            built = []
            for r in rows:
                res = self._transform_row(t, [r], dict(kwargs), dict(details))
                built.append(self._build(t, res, built)[0])
            self._accept(t, built)
        return len(built)

    def set_chunk_bytes(self, table, n):
        if self.table_def(table) is None and table not in self.transforms:
            raise WampError("test.error.no_such_table", [table])
        with self.lock:
            if n is None:
                self.chunk_override.pop(table, None)
            else:
                self.chunk_override[table] = int(n)

    def refuse_writes(self, reason):
        with self.lock:
            self.refusal = reason or None


class _StorageRefused(Exception):
    def __init__(self, reason):
        super().__init__(reason)
        self.reason = reason

    def wamp_error(self):
        """DataBackend.asWriteRefusal."""
        if self.reason == REASON_STORAGE_FULL:
            return WampError(ERROR_URI_STORAGE_FULL, [
                "appliance storage full — new rows are refused until space is freed: harness storage guard"])
        return WampError(ERROR_URI_STORAGE_OVERUSAGE, [
            "storage allowance exceeded (%s) — new rows are refused" % self.reason])


def _truthy(v):
    return v is not None and v != 0 and not (isinstance(v, float) and math.isnan(v))


def _sql_seconds(ms):
    """`to_timestamp(<ms> / 1000)`: an integer ms value divides as SQL
    integers (whole seconds, truncated toward zero); a fractional one does
    not."""
    if isinstance(ms, int) or (isinstance(ms, float) and ms.is_integer() and abs(ms) < 2 ** 53):
        ms = int(ms)
        return int(ms / 1000) if ms < 0 else ms // 1000
    return ms / 1000


def _sort_value(v):
    if v is None:
        return (0, "")
    if isinstance(v, bool):
        return (1, int(v))
    if is_number(v):
        return (2, v)
    if isinstance(v, str):
        return (3, v)
    return (4, json.dumps(v, sort_keys=True))


def _coerce_to_data_type(value, data_type):
    """DataBackend.ts coerceToDataType (the republish typing)."""
    if value is None:
        return None
    if data_type == "numeric":
        n = js_parse_float(value)
        return None if math.isnan(n) else n
    if data_type == "bigint":
        n = js_parse_int(value)
        return None if isinstance(n, float) else n
    if data_type == "timestamp":
        t = js_date_ms(value)
        return None if isinstance(t, float) and math.isnan(t) else t
    if data_type == "boolean":
        if value is True or value == "true" or (is_number(value) and value == 1):
            return True
        if value is False or value == "false" or (is_number(value) and value == 0):
            return False
        return None
    if data_type == "json":
        if isinstance(value, str):
            try:
                return json.loads(value)
            except ValueError:
                return value
        return value
    return js_string(value)


def _jsonb_text(v):
    """`->>`: a json leaf as text (SQL NULL for JSON null)."""
    if v is None:
        return None
    if isinstance(v, str):
        return v
    if isinstance(v, bool):
        return "true" if v else "false"
    if is_number(v):
        return js_string(v)
    return json.dumps(v, separators=(", ", ": "), ensure_ascii=False)


def _series_value(row, ref):
    """columnToSQL: a plain column as stored, a json path through `->>` (text)."""
    base, path = parse_column_path(ref)
    if not path:
        return row.get(base)
    return _jsonb_text(resolve_row_value(row, ref))


def _aggregate(method, ref, rows, table):
    """One "<METHOD>:<ref>" cell of a bucket (SERIES_FN; json paths are
    cast ::NUMERIC for AVG/SUM/MIN/MAX)."""
    base, path = parse_column_path(ref)
    values = [_series_value(r, ref) for r in rows]
    if path and method in ("AVG", "SUM", "MIN", "MAX"):
        cast = []
        for v in values:
            if v is None:
                cast.append(None)
                continue
            c = coerce(v, "numeric")
            if c is None:
                raise pg_error("22P02")  # invalid_text_representation
            cast.append(c[1])
        values = cast
    present = [v for v in values if v is not None]
    if method == "COUNT":
        return len(rows) if base == "tsp" else len(present)
    if method in ("FIRST", "LAST"):
        ordered = sorted(rows, key=lambda r: (r["tsp"], r["_seq"]))
        return _series_value(ordered[0] if method == "FIRST" else ordered[-1], ref)
    if not present:
        return None
    if method == "AVG":
        return float(sum(present)) / len(present)
    if method == "SUM":
        return sum(present)
    return min(present) if method == "MIN" else max(present)


def estimate_row_bytes(row, key_overhead, per_column=None):
    """utils.ts estimateRowBytes."""
    total = key_overhead + 2
    for key, value in row.items():
        if isinstance(value, str):
            size = len(value.encode()) + 2
        elif isinstance(value, bool) or is_number(value):
            size = 8
        elif value is None:
            size = 4
        else:
            size = len(json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode())
        total += size
        if per_column is not None:
            per_column[key] = per_column.get(key, 0) + size
    return total


MAX_KEYS_PER_LEVEL = 50


def _path_trees(refs):
    """sql-generator.ts buildColumnPathTrees: {base: {key: subtree | None}};
    a bare ref wins over paths, a shorter terminal absorbs deeper refs."""
    trees, whole = {}, set()
    for ref in refs:
        base, path = parse_column_path(ref)
        if not path:
            whole.add(base)
            trees.pop(base, None)
            continue
        if base in whole:
            continue
        node = trees.setdefault(base, {})
        for i, key in enumerate(path):
            if key in node and node[key] is None:
                break
            if i == len(path) - 1:
                node[key] = None
                break
            node = node.setdefault(key, {})
    return trees


def _prune(value, tree):
    """prunedJsonColumnSQL: NULL stays NULL, a missing key comes back as
    JSON null, more than 50 keys on a level returns the whole column."""
    def too_wide(node):
        return len(node) == 0 or len(node) > MAX_KEYS_PER_LEVEL or any(
            sub is not None and too_wide(sub) for sub in node.values())

    def render(node, v):
        out = {}
        for key, sub in node.items():
            child = v.get(key) if isinstance(v, dict) else None
            out[key] = child if sub is None else render(sub, child)
        return out

    if value is None:
        return None
    if too_wide(tree):
        return value
    return render(tree, value)
