"""Offline self-test of the fake platform (standard library only, no router):

    python3 -I integration/test_fake_platform.py [-v]

Checks the models against the verdicts the real services gave for the same
inputs (contracts/*.json, see contracts/README.md), the per-app credential
fixture against the device agent's derivation, and the fleetdb and fleetfiles
behaviors the integration suite and the SDK tests rely on. CI runs it in the
harness job; run it after changing a fake_*.py file or regenerating a table.
"""

import base64
import json
import os
import sys
import time
import unittest
import urllib.parse

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)  # `python -I` keeps the script's directory off sys.path

import fake_auth  # noqa: E402
import fake_fleetdb as fdb  # noqa: E402
import fake_fleetfiles as ffs  # noqa: E402
import fake_platform  # noqa: E402  (its harness data; autobahn is not needed)


def contract(name):
    with open(os.path.join(HERE, "contracts", name), encoding="utf-8") as f:
        return json.load(f)


SERIAL = fake_auth.SERIAL
APP_SESSION = {"caller_authid": SERIAL, "caller_authrole": "app"}
READER_SESSION = {"caller_authid": SERIAL, "caller_authrole": "app_reader"}
DEVICE_KWARGS = {"DEVICE_KEY": "42", "DEVICE_NAME": "interop-dev", "DEVICE_SERIAL_NUMBER": SERIAL}


class ContractTables(unittest.TestCase):
    """The models answer exactly what the real services answered."""

    def test_typia_validators(self):
        for case in contract("fleetdb_typia.json")["cases"]:
            with self.subTest(case["name"]):
                self.assertEqual(fdb.typia_check(case["type"], case["args"]), case["error"])

    def test_normalize_key(self):
        for case in contract("fleetfiles_keys.json")["cases"]:
            with self.subTest(repr(case["key"])[:60]):
                try:
                    got = {"normalized": ffs.normalize_key(case["key"])}
                except ffs.FileError as e:
                    self.assertEqual(e.code, ffs.CODE_INTERNAL)
                    got = {"error": e.reason}
                want = {k: case[k] for k in ("normalized", "error") if k in case}
                self.assertEqual(got, want)

    def test_authorize(self):
        db = fake_auth.PlatformDB()
        for case in contract("ironflock_auth_authorize.json")["cases"]:
            with self.subTest("%s %s %s" % (case["session"]["authrole"], case["action"], case["uri"])):
                got, _ = fake_auth.authorize(db, case["session"], case["uri"], case["action"], case["options"])
                self.assertEqual(got, case["verdict"])

    def test_authenticate_userapp(self):
        for state in contract("userapp_authenticate.json")["states"]:
            db = fake_auth.PlatformDB()
            db.set_grants(state["grants"])
            for e in state["epochs"]:
                db.epochs[(fake_auth.DEVICE_KEY, e["app"], e["stage"])] = (e["epoch"], time.time() - e["rotated_seconds_ago"])
            if not state["app_cred_key"]:
                db.app_cred_keys.clear()
            for case in state["cases"]:
                with self.subTest("%s: %s %s %s" % (state["name"], case["realm"], case["authid"][:20], case["legacy_mode"])):
                    self.assertEqual(db.authenticate_userapp(case["realm"], case["authid"], case["legacy_mode"]),
                                     case["verdict"])


class CredentialFixture(unittest.TestCase):
    """perapp_env/ holds the credential the device agent would write."""

    def read(self, name):
        with open(os.path.join(HERE, "router", "perapp_env", name), "rb") as f:
            return f.read()

    def test_fixture_is_the_agent_derivation(self):
        authid, secret = fake_auth.app_credential(SERIAL, fake_auth.OWN_APP, "DEV", 1)
        self.assertEqual(self.read("APP_AUTH_ID.txt"), authid.encode())  # no trailing newline, as the agent writes
        self.assertEqual(self.read("APP_AUTH_SECRET.txt"), secret.encode())
        self.assertEqual(authid, "app-26-dev-e1@" + SERIAL)

    def test_static_mode_user_matches_fixture(self):
        with open(os.path.join(HERE, "router", "config.template.yaml"), encoding="utf-8") as f:
            template = f.read()
        line = '{ authid: "%s", role: app, secret: "%s" }' % (self.read("APP_AUTH_ID.txt").decode(),
                                                           self.read("APP_AUTH_SECRET.txt").decode())
        self.assertIn(line, template)

    def test_admission(self):
        db = fake_auth.PlatformDB()
        authid = self.read("APP_AUTH_ID.txt").decode()
        own, _ = fake_auth.userapp_authenticate(db, "realm-2-26-dev", authid)
        self.assertEqual((own["success"], own["role"], own["authid"]), (True, "app", SERIAL))
        self.assertEqual(own["secret"], self.read("APP_AUTH_SECRET.txt").decode())
        reader, _ = fake_auth.userapp_authenticate(db, "realm-2-77-dev", authid)
        self.assertEqual((reader["role"], reader["authid"]), ("app_reader", SERIAL))
        legacy, _ = fake_auth.userapp_authenticate(db, "realm-2-77-dev", SERIAL)
        self.assertEqual(legacy, {"success": False})  # legacy_foreign
        old, verdict = fake_auth.userapp_authenticate(db, "realm-2-26-dev", "app-26-per-app")
        self.assertEqual((old, verdict["reason"]), ({"success": False}, "unknown_device"))

    def test_rotation(self):
        db = fake_auth.PlatformDB()
        new = db.rotate()
        self.assertEqual(new["epoch"], 2)
        ok, _ = fake_auth.userapp_authenticate(db, "realm-2-26-dev", new["authid"])
        self.assertEqual(ok["secret"], new["secret"])
        grace, _ = fake_auth.userapp_authenticate(db, "realm-2-26-dev", "app-26-dev-e1@" + SERIAL)
        self.assertTrue(grace["success"])
        db.reset_credentials()
        db.rotate(grace_expired=True)
        stale, verdict = fake_auth.userapp_authenticate(db, "realm-2-26-dev", "app-26-dev-e1@" + SERIAL)
        self.assertEqual((stale, verdict["reason"]), ({"success": False}, "stale_epoch"))


class JavaScriptValues(unittest.TestCase):
    def test_number_to_string(self):
        for value, want in [(100.0, "100"), (22.5, "22.5"), (1e21, "1e+21"), (1e-7, "1e-7"), (1.5e-6, "0.0000015"),
                            (0.1 + 0.2, "0.30000000000000004"), (-0.5, "-0.5"), (123456789012345680000.0, "123456789012345680000"),
                            (2.5e-300, "2.5e-300"), (1.7976931348623157e308, "1.7976931348623157e+308")]:
            self.assertEqual(fdb.js_string(value), want)

    def test_iso_dates(self):
        self.assertEqual(fdb.parse_iso_ms("2026-01-01T00:00:00Z"), 1767225600000)
        self.assertEqual(fdb.parse_iso_ms("2026-01-01"), 1767225600000)
        self.assertEqual(fdb.parse_iso_ms("2026-01-01T01:00:00.123+01:00"), 1767225600123)
        self.assertEqual(fdb.parse_iso_ms("1969-12-31T23:59:59Z"), -1000)
        self.assertEqual(fdb.parse_iso_ms("+010000-01-01T00:00:00Z"), 253402300800000)
        for bad in ("2026-02-30", "2026-13-01", "20260101", "yesterday", "2026-01-01T25:00:00Z"):
            self.assertIsNone(fdb.parse_iso_ms(bad), bad)


def backend(template=None, fixtures=None):
    events = []
    b = fdb.Backend(2, 26, "DEV", template or fake_platform.OWN_TEMPLATE, fake_auth.PlatformDB(),
                    lambda topic, args, exclude=None: events.append((topic, args, exclude)),
                    fixtures=fake_platform.OWN_FIXTURES if fixtures is None else fixtures,
                    writer=fake_platform.DEVICE_WRITER)
    return b, events


def call(b, uri, args, kwargs=None, details=APP_SESSION):
    try:
        return b.call(uri, args, kwargs or {}, dict(details))
    except fdb.WampError as e:
        return ("error", e.error, e.wamp_args, e.wamp_kwargs)


class Fleetdb(unittest.TestCase):
    def test_old_series_shape_is_refused(self):
        b, _ = backend()
        got = call(b, "history.transformed.series.sensordata", [{
            "metrics": ["temperature"], "method": "AVG", "limit": 100,
            "timeRange": ["2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z"], "groupBy": ["device_key"]}])
        self.assertEqual(got, ("error", "wamp.error.runtime_error", [{
            "method": "typia.assert", "path": "$input[0].metrics[0]", "expected": "SeriesMetric",
            "value": "temperature"}], {}))

    def test_series_answers_aliased_bucket_columns(self):
        b, _ = backend()
        b.insert("sensordata", [{"tsp": 1767225600000 + i * 1000, "temperature": t}
                                for i, t in enumerate([1.0, 3.0, 10.0, 20.0])])
        got = call(b, "history.transformed.series.sensordata", [{
            "metrics": [{"ref": "temperature", "method": "AVG"}, {"ref": "tsp", "method": "COUNT"},
                        {"ref": "temperature", "method": "AVG"}],
            "limit": 2, "timeRange": [1767225600000, 1767225604000], "groupBy": ["device_key"]}])
        self.assertEqual(got, [{"tsp": 1767225600000, "device_key": 42, "AVG:temperature": 2, "COUNT:tsp": 2},
                               {"tsp": 1767225602000, "device_key": 42, "AVG:temperature": 15, "COUNT:tsp": 2}])
        widened = call(b, "history.transformed.series.sensordata", [{
            "metrics": [{"ref": "temperature", "method": "MAX"}], "limit": 2, "bucketMs": 1000,
            "timeRange": [1767225600000, 1767225604000]}])
        self.assertEqual([r["tsp"] for r in widened], [1767225600000, 1767225602000])  # 4 buckets widened to 2

    def test_series_refusals(self):
        b, _ = backend()
        base = {"metrics": [{"ref": "temperature", "method": "AVG"}], "limit": 10, "timeRange": [1767225600000, None]}
        cases = [({"timeRange": None}, "sys.dataservice.error.invalid_time_range"),
                 ({"timeRange": [1767225600000, 1767225600000]}, "sys.dataservice.error.invalid_time_range"),
                 ({"metrics": [{"ref": "source", "method": "AVG"}]}, "sys.dataservice.error.invalid_metric"),
                 ({"metrics": [{"ref": "nope", "method": "MAX"}]}, "sys.dataservice.error.invalid_metric"),
                 ({"metrics": [{"ref": "tsp", "method": "MAX"}]}, "sys.dataservice.error.invalid_metric"),
                 ({"groupBy": ["tsp"]}, "sys.dataservice.error.invalid_group_by"),
                 ({"groupBy": ["temperature"]}, "sys.dataservice.error.invalid_group_by"),
                 ({"bucketMs": 999}, "wamp.error.runtime_error")]
        for change, uri in cases:
            with self.subTest(change):
                self.assertEqual(call(b, "history.transformed.series.sensordata", [dict(base, **change)])[1], uri)

    def test_requests_are_validated(self):
        b, _ = backend()
        lower = call(b, "history.transformed.sensordata", [{"limit": 1, "filterAnd": [
            {"column": "source", "operator": "like", "value": "%x%"}]}])
        self.assertEqual((lower[1], lower[2][0]["path"]), ("wamp.error.runtime_error", "$input[0].filterAnd[0].operator"))
        self.assertEqual(call(b, "history.transformed.sensordata", [{"limit": 10001}])[2][0]["expected"],
                         "number & Maximum<10000>")
        self.assertEqual(call(b, "secret.reveal.credentials", [{"limit": 101}])[2][0]["expected"],
                         "number & Maximum<100>")
        mixed = call(b, "history.transformed.sensordata", [{"limit": 1, "filterAnd": [
            {"combinator": "OR", "filters": [], "column": "x"}]}])
        self.assertEqual(mixed[1], "wamp.error.runtime_error")

    def test_append_needs_tsp(self):
        b, events = backend()
        self.assertEqual(call(b, "append.2.26.sensordata", [{"temperature": 23}], DEVICE_KWARGS),
                         ("error", "wamp.error.runtime_error", [{}], {}))
        self.assertEqual(call(b, "appendBulk.2.26.sensordata", [[{"tsp": 1767225600000}, {"temperature": 1}]],
                              DEVICE_KWARGS)[1:3], ("wamp.error.runtime_error", [{}]))
        self.assertEqual(call(b, "appendBulk.2.26.sensordata", [[]], DEVICE_KWARGS)[1:3], ("wamp.error.runtime_error", [{}]))
        self.assertEqual(b.rows["sensordata"], [])  # all-or-nothing
        self.assertEqual(events, [])

    def test_publish_without_tsp_is_dropped(self):
        b, events = backend()
        b.event("2.26.sensordata", [{"temperature": 1}], DEVICE_KWARGS, {"publisher_authid": SERIAL})
        b.event("2.26.sensordata", [], dict(DEVICE_KWARGS, temperature=1.5), {"publisher_authid": SERIAL})
        self.assertEqual((b.rows["sensordata"], events), ([], []))

    def test_write_republishes_the_payload_columns(self):
        b, events = backend()
        self.assertEqual(call(b, "append.2.26.sensordata", [{"tsp": "2026-01-01T00:00:02Z", "temperature": "23.5",
                                                               "ignored": 1}], DEVICE_KWARGS, APP_SESSION), {"success": True})
        self.assertEqual(events, [("transformed.sensordata", [{"tsp": 1767225602000, "temperature": 23.5,
                                                               "device_key": 42, "authid": SERIAL}], None)])
        b.event("bulk.2.26.sensordata", [[{"tsp": 1767225603000, "n": "7"}]], DEVICE_KWARGS, {"publisher_authid": SERIAL})
        self.assertEqual(events[-1], ("transformed.bulk.sensordata", [[{"tsp": 1767225603000, "n": 7, "device_key": 42,
                                                                        "authid": SERIAL}]], None))
        call(b, "append.2.26.error-logs", [{"tsp": 1767225600000, "msg": "m"}], DEVICE_KWARGS)
        self.assertEqual(events[-1][2], ["app_reader"])  # error-logs is private for readers

    def test_history_is_ascending_with_epoch_ms(self):
        b, _ = backend()
        b.insert("sensordata", [{"tsp": "2026-01-01T00:00:03Z", "temperature": 3},
                                {"tsp": "2026-01-01T00:00:01Z", "temperature": 1},
                                {"tsp": "2026-01-01T00:00:02Z", "temperature": 2}])
        rows = call(b, "history.transformed.sensordata", [{"limit": 2}])
        self.assertEqual(rows, [
            {"tsp": 1767225602000, "temperature": 2, "n": None, "source": None, "device_key": 42, "authid": SERIAL},
            {"tsp": 1767225603000, "temperature": 3, "n": None, "source": None, "device_key": 42, "authid": SERIAL}])
        page = call(b, "history.transformed.sensordata", [{"limit": 1, "offset": 1, "columns": ["temperature"]}])
        self.assertEqual(page, [{"temperature": 2, "tsp": 1767225602000, "device_key": 42, "authid": SERIAL}])

    def test_time_range_is_half_open_in_whole_seconds(self):
        b, _ = backend()
        b.insert("sensordata", [{"tsp": 1767225600500, "temperature": 1}, {"tsp": 1767225601000, "temperature": 2}])
        got = call(b, "history.transformed.sensordata", [{"limit": 10, "timeRange": [1767225600999, 1767225601000]}])
        self.assertEqual([r["temperature"] for r in got], [1])  # [00:00:00, 00:00:01)
        series = call(b, "history.transformed.series.sensordata", [{"metrics": [{"ref": "tsp", "method": "COUNT"}],
                                                                    "limit": 1, "timeRange": [1767225600999, 1767225602000]}])
        self.assertEqual(series[0]["COUNT:tsp"], 1)  # series bounds are exact

    def test_latest_marker(self):
        b, _ = backend()
        b.insert("sensordata", [{"tsp": 1767225601000, "temperature": 1}, {"tsp": 1767225602000, "temperature": 2}])
        got = call(b, "history.transformed.sensordata", [{"limit": 10, "offset": 5, "filterAnd": [{"latest": True}]}])
        self.assertEqual([r["temperature"] for r in got], [2])  # no entity key: the single latest row
        b.insert("credentials", [{"tsp": 1767225601000, "machine": "m2", "api_key": "a"},
                                 {"tsp": 1767225602000, "machine": "m2", "api_key": "b"}])
        latest = call(b, "history.transformed.credentials", [{"limit": 10, "filterAnd": [{"latest": True}]}])
        self.assertEqual([(r["machine"], r["api_key"]) for r in latest], [("m1", "__secret__"), ("m2", "__secret__")])

    def test_filters(self):
        b, _ = backend()
        b.insert("sensordata", [{"tsp": 1767225600000 + i, "temperature": t, "source": s}
                                for i, (t, s) in enumerate([(1, "a"), (5, None), (9, "b")])])
        q = lambda f: [r["temperature"] for r in call(b, "history.transformed.sensordata", [{"limit": 10, "filterAnd": f}])]
        self.assertEqual(q([{"column": "temperature", "operator": ">", "value": "4"}]), [5, 9])
        self.assertEqual(q([{"column": "source", "operator": "!=", "value": "a"}]), [9])  # NULL is unknown
        self.assertEqual(q([{"column": "source", "operator": "IS NULL"}]), [5])
        self.assertEqual(q([{"column": "temperature", "operator": "IN", "value": []}]), [])
        self.assertEqual(q([{"column": "temperature", "operator": "NOT IN", "value": []}]), [1, 5, 9])
        self.assertEqual(q([{"combinator": "OR", "filters": [{"column": "source", "operator": "LIKE", "value": "b%"},
                                                             {"column": "temperature", "operator": "<", "value": 2}]}]), [1, 9])
        self.assertEqual(q([{"column": "temperature", "operator": "=", "value": "x"}]), [1, 5, 9])  # dropped, widens
        self.assertEqual(q([{"column": "unknown", "operator": "=", "value": 1}]), [1, 5, 9])
        self.assertEqual(call(b, "history.transformed.credentials", [{"limit": 1, "filterAnd": [
            {"column": "api_key", "operator": "=", "value": "x"}]}])[1], "sys.dataservice.error.secret_column")

    def test_secrets(self):
        b, _ = backend()
        b.insert("credentials", [{"tsp": 1767225601000, "machine": "m2", "api_key": "two"}])
        revealed = call(b, "secret.reveal.credentials", [{"limit": 10}])
        self.assertEqual([r["api_key"] for r in revealed], ["two", "right"])  # newest first: no respondRows
        self.assertEqual(call(b, "secret.verify.credentials", [{"limit": 1, "column": "api_key", "candidate": "two"}]),
                         {"match": True, "checked": 1})
        self.assertEqual(call(b, "secret.verify.credentials", [{"limit": 1, "column": "machine", "candidate": "x"}])[1],
                         "sys.dataservice.error.not_a_secret_column")
        self.assertEqual(call(b, "secret.reveal.credentials", [{"limit": 1}], details=READER_SESSION)[1],
                         "wamp.error.not_authorized")
        for _ in range(fdb.SECRET_RATE_LIMITS["reveal"] - 1):
            call(b, "secret.reveal.credentials", [{"limit": 1}])
        self.assertEqual(call(b, "secret.reveal.credentials", [{"limit": 1}])[1], "sys.dataservice.error.rate_limited")

    def test_chunked_results(self):
        b, _ = backend()
        b.insert("chunked", [{"tsp": 1767225600000 + i * 1000, "seq": i, "payload": "x" * 40} for i in range(5)])
        small = call(b, "history.transformed.chunked", [{"limit": 3}])  # ~155 bytes a row, 512 a chunk
        self.assertEqual([r["seq"] for r in small], [2, 3, 4])
        chunks = []
        details = dict(APP_SESSION, progress=lambda index, rows: chunks.append((index, [r["seq"] for r in rows])))
        summary = call(b, "history.transformed.chunked", [{"limit": 10}], details=details)
        self.assertEqual(summary, {"chunked": True, "chunkCount": 2, "totalRows": 5, "order": "asc"})
        self.assertEqual(chunks, [(0, [4, 3, 2]), (1, [1, 0])])  # newest first: concatenate and reverse
        refused = call(b, "history.transformed.chunked", [{"limit": 10}])
        self.assertEqual((refused[1], refused[3]["detail"]),
                         ("sys.dataservice.error.result_too_large", "caller cannot receive progressive results"))
        b.set_chunk_bytes("sensordata", 64)
        b.insert("sensordata", [{"tsp": 1767225600000, "source": "y" * 100}])
        self.assertEqual(call(b, "history.transformed.sensordata", [{"limit": 1}], details=details)[3]["detail"],
                         "single row exceeds the chunk budget")
        b.reset()
        self.assertEqual(b._chunk_budget(b.table_def("sensordata")), fdb.CHUNK_BYTES)

    def test_transforms(self):
        b, _ = backend()
        self.assertEqual(call(b, "history.transformed.sensordata_hourly", [{"limit": 3001}])[1],
                         "sys.dataservice.error.invalid_limit")
        got = call(b, "history.transformed.sensordata_hourly", [{"limit": 2, "offset": 1,
                                                                 "timeRange": [1, 2], "columns": ["tsp"]}])
        self.assertEqual(got, [{"tsp": 1767232800000, "avg_temperature": 22.5},
                               {"tsp": 1767229200000, "avg_temperature": 22}])

    def test_cross_app(self):
        provider, _ = backend(fake_platform.PROVIDER_TEMPLATE, fake_platform.PROVIDER_FIXTURES)
        self.assertEqual(call(provider, "history.transformed.private_stats", [{"limit": 1}], details=READER_SESSION)[1],
                         "wamp.error.not_authorized")
        rows = call(provider, "history.transformed.readings", [{"limit": 2, "filterAnd": [{"latest": True}]}],
                    details=READER_SESSION)
        self.assertEqual(rows, [{"tsp": 1767229200000, "temp": 20, "api_key": "__secret__", "device_key": 42,
                                 "authid": SERIAL}])
        own, _ = backend()
        platform = fake_auth.PlatformDB()
        platform.add_databackend(2, 77, "DEV", provider.catalog)
        own.platform = platform
        resolved = call(own, "sys.appaccess.resolve", [{"app": "Weather"}])
        self.assertEqual((resolved["app"], resolved["provider_app_key"]), ("Weather", 77))  # echoed as sent
        self.assertEqual([t["tablename"] for t in resolved["stages"]["dev"]["tables"]], ["readings"])
        self.assertEqual(call(own, "sys.appaccess.resolve", [{"app": "nogrant"}])[1:3],
                         ("sys.appaccess.error.no_grant", ["no data access grant for provider 'nogrant'"]))
        self.assertEqual(call(own, "sys.appaccess.list", [])[1], "sys.appaccess.error.no_grant")
        platform.set_grants(["weather", "*"])
        self.assertEqual([a["app"] for a in call(own, "sys.appaccess.list", [])], ["weather"])
        self.assertEqual(call(own, "sys.appaccess.resolve", [{"app": "nosuch"}])[1], "sys.appaccess.error.unknown_app")
        self.assertEqual(call(own, "sys.appaccess.resolve", [{"app": 1}])[1], "wamp.error.runtime_error")

    def test_entity_key_conflict_and_storage_refusal(self):
        b, _ = backend()
        self.assertEqual(call(b, "append.2.26.credentials", [{"tsp": "2026-01-01T00:00:00Z", "machine": "m1",
                                                              "api_key": "x"}], DEVICE_KWARGS)[1],
                         "sys.dataservice.error.entity_key_conflict")
        b.refuse_writes(fdb.REASON_STORAGE_FULL)
        self.assertEqual(call(b, "append.2.26.sensordata", [{"tsp": 1}], DEVICE_KWARGS)[1], fdb.ERROR_URI_STORAGE_FULL)
        b.refuse_writes("OVERUSAGE_STORAGE")
        self.assertEqual(call(b, "append.2.26.sensordata", [{"tsp": 1}], DEVICE_KWARGS)[1], fdb.ERROR_URI_STORAGE_OVERUSAGE)
        b.event("2.26.sensordata", [{"tsp": 1}], DEVICE_KWARGS, {})
        self.assertEqual(b.rows["sensordata"], [])

    def test_undeclared_tables_have_no_procedures(self):
        b, _ = backend()
        uris = [u for u, _ in b.procedures()]
        self.assertNotIn("append.2.26.nope", uris)
        self.assertNotIn("secret.reveal.sensordata", uris)  # no secret column
        self.assertIn("secret.reveal.credentials", uris)
        self.assertIn("append.2.26.error-logs", uris)


def file_service():
    events = []
    svc = ffs.FileService(2, 3317, fake_platform.FILE_NAMESPACES, fake_platform.PUBLIC_BASE_URL,
                          lambda: "http://127.0.0.1:1", lambda topic, args, exclude=None: events.append((topic, args)))
    return svc, events


def fcall(svc, uri, a=None, role="app"):
    return svc.call(uri, [a] if a is not None else [], {}, {"caller_authrole": role})


def put(svc, key, data=b"x", ns="default", ct="text/plain"):
    r = fcall(svc, "files.write.put", {"namespace": ns, "key": key, "data": base64.b64encode(data).decode(),
                                       "content_type": ct})
    assert r["success"], r
    return r["payload"]


class Fleetfiles(unittest.TestCase):
    def test_folder_style_list(self):
        svc, _ = file_service()
        for key in ("a/b c.txt", "a/sub/d.txt", "abc.txt"):
            put(svc, key)

        def ls(**a):
            r = fcall(svc, "files.read.list", dict({"namespace": "default"}, **a))
            if not r["success"]:
                return r["code"], r["reason"]
            return [o["key"] for o in r["payload"]["objects"]], r["payload"]["prefixes"]
        # The table the real handlers answered in r3/verify-12 (fleetfiles v0.2.0).
        self.assertEqual(ls(prefix="a/"), ("INTERNAL", "object key must not end with '/'"))
        self.assertEqual(ls(), (["abc.txt"], ["a/"]))
        self.assertEqual(ls(prefix="a"), (["abc.txt"], ["a/"]))
        self.assertEqual(ls(prefix="a/sub"), ([], ["a/sub/"]))
        self.assertEqual(ls(prefix="a", delimiter=""), (["a/b c.txt", "a/sub/d.txt", "abc.txt"], []))
        self.assertEqual(ls(prefix="a/", delimiter=""), ("INTERNAL", "object key must not end with '/'"))
        self.assertEqual(ls(prefix="a//b"), ("INTERNAL", "object key must not contain an empty path segment ('//')"))
        self.assertEqual(ls(delimiter=None), (["abc.txt"], ["a/"]))  # not a string: folder style
        first = fcall(svc, "files.read.list", {"namespace": "default", "limit": 1})["payload"]
        self.assertEqual((first["prefixes"], first["is_truncated"]), (["a/"], True))
        rest = fcall(svc, "files.read.list", {"namespace": "default", "limit": 0, "cursor": first["cursor"]})["payload"]
        self.assertEqual([o["key"] for o in rest["objects"]], ["abc.txt"])
        self.assertEqual(rest["objects"][0]["content_type"], "application/octet-stream")  # listings: by extension
        self.assertEqual(fcall(svc, "files.read.list", {"namespace": "nope"})["code"], "NO_SUCH_NAMESPACE")

    def test_list_limit_clamp_and_scan(self):
        svc, _ = file_service()
        for i in range(1005):
            svc.objects[("default", "k%04d" % i)] = {"data": b"", "content_type": "", "etag": "e",
                                                     "modified": __import__("datetime").datetime(2026, 1, 1)}
        page = fcall(svc, "files.read.list", {"namespace": "default", "limit": 5000})["payload"]
        self.assertEqual((len(page["objects"]), page["is_truncated"]), (1000, True))
        scan = fcall(svc, "files.read.list", {"namespace": "*", "search": "k100", "offset": 0})["payload"]
        self.assertEqual([o["key"] for o in scan["objects"]], ["k1000", "k1001", "k1002", "k1003", "k1004"])

    def test_keys_are_validated_everywhere(self):
        svc, _ = file_service()
        for uri, a in [("files.read.stat", {"key": "dir/"}), ("files.read.get", {"key": "a/../b"}),
                       ("files.write.delete", {"key": ""}), ("files.read.url", {"key": "/x"}),
                       ("files.write.copy", {"key": "ok", "to": "bad/"})]:
            with self.subTest(uri):
                self.assertEqual(fcall(svc, uri, dict({"namespace": "default"}, **a))["code"], "INTERNAL")

    def test_get_put_copy_delete(self):
        svc, events = file_service()
        answer = put(svc, "a/b c.txt", b"hello")
        self.assertNotIn("last_modified", answer)  # PutObject returns no LastModified
        self.assertEqual(answer["etag"], "5d41402abc4b2a76b9719d911017c592")
        got = fcall(svc, "files.read.get", {"namespace": "default", "key": "a/b c.txt", "offset": 1, "length": 3})["payload"]
        self.assertEqual((base64.b64decode(got["data"]), got["offset"], got["length"], got["size"]), (b"ell", 1, 3, 5))
        self.assertEqual(fcall(svc, "files.read.get", {"namespace": "default", "key": "a/b c.txt", "offset": 6})["code"],
                         "INVALID_RANGE")
        self.assertEqual(fcall(svc, "files.read.get", {"namespace": "default", "key": "a/b c.txt", "length": 0})["code"],
                         "INVALID_RANGE")
        put(svc, "big.bin", b"y" * 700)
        svc.configure(inline_max_bytes=500)
        self.assertEqual(fcall(svc, "files.read.get", {"namespace": "default", "key": "big.bin"})["code"], "TOO_LARGE")
        ranged = fcall(svc, "files.read.get", {"namespace": "default", "key": "big.bin", "offset": 0, "length": 10 ** 9})
        self.assertEqual(ranged["payload"]["length"], 500)  # clamped to the inline cap
        copied = fcall(svc, "files.write.copy", {"namespace": "default", "key": "a/b c.txt", "to": "c.txt"})["payload"]
        self.assertEqual((copied["key"], copied["size"]), ("c.txt", 5))
        self.assertIn("last_modified", copied)  # the destination's stat
        self.assertEqual(fcall(svc, "files.write.delete", {"namespace": "default", "key": "gone.txt"})["payload"],
                         {"deleted": True})
        self.assertEqual(fcall(svc, "files.read.stat", {"namespace": "default", "key": "gone.txt"})["code"],
                         "NO_SUCH_OBJECT")
        self.assertEqual(fcall(svc, "files.write.put", {"namespace": "frames", "key": "x.png", "data": "",
                                                        "content_type": "image/png"})["code"], "CONTENT_TYPE_NOT_ALLOWED")
        self.assertEqual(fcall(svc, "files.write.put", {"namespace": "default", "key": "x"}, role="app_reader")["code"],
                         "NOT_AUTHORIZED")
        self.assertEqual([e[1][0]["action"] for e in events][:3], ["put", "put", "put"])

    def test_presigned_urls(self):
        svc, _ = file_service()
        put(svc, "a/b c.txt", b"hello")
        minted = fcall(svc, "files.read.url", {"namespace": "default", "key": "a/b c.txt", "expires_in": 5})["payload"]
        self.assertEqual((minted["method"], minted["expires_in"], minted["size"]), ("GET", 60, 5))  # TTL clamped
        url = urllib.parse.urlsplit(minted["url"])
        self.assertEqual(url.path, "/if-2-3317/default/a/b%20c.txt")
        status, headers, body = svc.s3_get(url.path, url.query, None)
        self.assertEqual((status, body), (200, b"hello"))
        self.assertEqual(headers["Content-Disposition"], 'attachment; filename="b c.txt"')
        self.assertEqual(svc.s3_get(url.path, url.query, "bytes=1-2")[2], b"el")
        self.assertEqual(svc.s3_get(url.path.replace("b%20c", "x"), url.query, None)[0], 403)  # signature
        reader = fcall(svc, "files.read.url", {"namespace": "default", "key": "a/b c.txt", "expires_in": 3600},
                       role="app_reader")["payload"]
        self.assertEqual(reader["expires_in"], 300)
        upload = fcall(svc, "files.write.url", {"namespace": "default", "key": "u.bin", "size": 10})["payload"]
        self.assertEqual(upload["headers"], {"Content-Type": "application/octet-stream"})
        up = urllib.parse.urlsplit(upload["url"])
        self.assertEqual(svc.s3_put(up.path, up.query, b"0123456789", None)[0], 200)
        stat = fcall(svc, "files.read.stat", {"namespace": "default", "key": "u.bin"})["payload"]
        self.assertEqual((stat["size"], stat["content_type"]), (10, "binary/octet-stream"))
        svc.now = lambda: time.time() + 3600
        self.assertEqual(svc.s3_put(up.path, up.query, b"late", None)[0], 403)  # expired
        svc.configure(presign_available=False)
        self.assertEqual(fcall(svc, "files.write.url", {"namespace": "default", "key": "u.bin"})["code"], "NOT_SUPPORTED")
        self.assertFalse(fcall(svc, "files.read.namespaces")["payload"]["presign_available"])

    def test_catalog(self):
        svc, _ = file_service()
        catalog = fcall(svc, "files.read.namespaces")["payload"]
        self.assertEqual([n["name"] for n in catalog["namespaces"]], ["default", "frames"])
        self.assertEqual(catalog["namespaces"][0], {"name": "default", "description": "", "private": False,
                                                    "content_types": ["*/*"], "max_object_bytes": 104857600})
        self.assertEqual((catalog["inline_max_bytes"], catalog["sad_key"], catalog["list_max_limit"]), (1024, 3317, 1000))


if __name__ == "__main__":
    unittest.main()
