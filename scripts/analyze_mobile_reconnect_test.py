import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("analyze_mobile_reconnect", Path(__file__).with_name("analyze_mobile_reconnect.py"))
a = importlib.util.module_from_spec(spec)
spec.loader.exec_module(a)


def record(event=1, environment=1, arm="old", latency=100, version=4, **changes):
    context = {"id": f"1800000000000-event-{event}", "timestamp": "2026-10-17T12:00:00.000Z",
        "launch": "resume", "wakeup": "application-active-reconnect", "backgroundMs": 20000,
        "timingOrigin": "wakeup", "environments": [1, 2], "visibleThreadEnvironment": 1}
    return {"id": f"1800000000000-{event * 10 + environment}", "recordVersion": version,
        "eventId": context["id"], "event": context if version == 4 else None,
        "eventAssignment": "random", "experiment": True, "arm": arm,
        "networkType": "wifi", "networkChanged": False, "environment": environment,
        "eventEnvironmentCount": 2, "legacyPacingSupported": True,
        **{key: context[key] for key in a.CONTEXT_FIELDS},
        "outcome": "connected", "guess": arm, "interruptedAfterMs": None, "attempts": [],
        "shell": {"completion": "marker", "outcome": "live", "liveAfterMs": latency},
        "visibleThread": {"completion": "marker", "outcome": "live" if environment == 1 else "not-visible", "liveAfterMs": latency if environment == 1 else None}, **changes}


class ReconnectAnalysis(unittest.TestCase):
    def test_idle_interruption_does_not_invalidate_enabled_environment(self):
        idle = record(environment=2, outcome="idle", interruptedAfterMs=150, legacyPacingSupported=None)
        event = a.group_events([record(), idle])[0]
        self.assertEqual(event["environment_count"], 1)
        self.assertEqual(event["unknown_environments"], 0)
        self.assertEqual(event["shell"]["liveAfterMs"], 100)
        self.assertEqual(event["environment_outcomes"], {"connected": 1, "idle": 1})

    def test_unknown_failed_connection_still_blocks_list_but_not_pinned_thread(self):
        failed = record(environment=2, outcome="failed", legacyPacingSupported=None)
        event = a.group_events([record(), failed])[0]
        self.assertIsNone(event["shell"]["liveAfterMs"])
        self.assertEqual(event["visibleThread"]["liveAfterMs"], 100)
        self.assertEqual(event["unknown_environments"], 1)

    def test_v4_canonical_metadata_is_independent_of_export_order(self):
        rows = [record(latency=100), record(environment=2, latency=200)]
        normal = a.group_events(rows)[0]
        reverse = a.group_events(list(reversed(rows)))[0]
        self.assertEqual(normal["launch"], "resume")
        self.assertEqual(normal["shell"], reverse["shell"])
        self.assertEqual(normal["visibleThread"]["liveAfterMs"], 100)
        self.assertEqual(normal["shell"]["liveAfterMs"], 200)

    def test_v4_rejects_conflicting_or_missing_context_and_unpinned_thread(self):
        for changed in [{"event": None}, {"launch": "cold"}, {"timestamp": "2026-10-17T12:00:05.000Z"}, {"eventEnvironmentCount": 3}, {"environment": 3}]:
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                a.group_events([record(), {**record(environment=2), **changed}])
        unpinned = record(environment=2)
        unpinned["visibleThread"]["outcome"] = "live"
        with self.assertRaises(ValueError):
            a.group_events([record(), unpinned])

    def test_missing_expected_identity_cannot_be_replaced_with_another_row(self):
        event = a.group_events([record()])[0]
        self.assertFalse(event["cohort_complete"])
        self.assertIsNone(event["shell"]["liveAfterMs"])
        with self.assertRaises(ValueError):
            a.group_events([record(), record(environment=3)])

    def test_historical_idle_cold_origin_does_not_relabel_wakeup(self):
        idle = record(environment=3, version=3, outcome="idle", interruptedAfterMs=100, legacyPacingSupported=None,
            timingOrigin="js-launch", launch="cold", backgroundMs=None, timestamp="2026-10-17T11:00:00.000Z")
        event = a.group_events([idle, record(version=3), record(environment=2, version=3)])[0]
        self.assertEqual(a.kind(event), "short background")
        self.assertEqual(event["shell"]["liveAfterMs"], 100)
        self.assertIn("historical_event_reclassified", event["warnings"])

    def test_mixed_historical_clocks_are_missing_instead_of_wall_clock_rebased(self):
        first = record(version=3, latency=3926)
        later = record(environment=2, version=3, latency=2653, timestamp="2026-10-17T12:00:05.773Z", timingOrigin="attempt", launch="running", backgroundMs=None)
        event = a.group_events([first, later])[0]
        self.assertIsNone(event["shell"]["liveAfterMs"])
        self.assertEqual(event["visibleThread"]["liveAfterMs"], 3926)
        self.assertIn("historical_event_origin_mismatch", event["warnings"])

    def test_versions_continuations_and_guesses_are_separate(self):
        rows = [record(version=2), record(version=3), record(environment=2, version=3),
            record(event=2), record(event=2, environment=2),
            record(event=3, eventAssignment="continuation"), record(event=3, environment=2, eventAssignment="continuation")]
        result = a.analyze(rows, 1000)
        self.assertEqual(result["legacy_per_environment"]["experiment_records"], 1)
        self.assertEqual(result["historical_v3"]["randomized_events"], 1)
        self.assertEqual(result["current_v4"]["randomized_events"], 1)
        self.assertEqual(result["current_v4"]["continuation_events"], 1)
        self.assertEqual(result["current_v4"]["guesses_overall"]["n"], 1)

    def test_threshold_counts_successful_events_not_environment_rows(self):
        rows = [record(event=offset+i, environment=env, arm=arm, latency=latency)
            for arm, latency, offset in [("old", 500, 0), ("new", 100, 100)]
            for i in range(1, 11) for env in (1, 2)]
        metric = a.analyze(rows, 1000)["current_v4"]["groups"][0]["metrics"]["thread_list"]
        self.assertEqual(metric["old"]["n"], 10)
        self.assertTrue(metric["improvement"])
        rows = [row for row in rows if row["eventId"] not in ("1800000000000-event-10", "1800000000000-event-110")]
        metric = a.analyze(rows, 1000)["current_v4"]["groups"][0]["metrics"]["thread_list"]
        self.assertEqual(metric["status"], "not enough data")

    def test_load_mixed_export_rejects_duplicate_and_invalid_latencies(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "connections.json"
            rows = [record(), record(environment=2)]
            path.write_text(json.dumps({"format": "t3-mobile-connection-log", "version": 4, "records": rows}))
            self.assertEqual(a.load(path)[1], 4)
            for invalid in [rows + [rows[0]], [record(latency=-1)], [record(latency=True)]]:
                path.write_text(json.dumps(invalid))
                with self.assertRaises(ValueError):
                    a.load(path)


if __name__ == "__main__":
    unittest.main()
