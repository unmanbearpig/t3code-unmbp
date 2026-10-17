#!/usr/bin/env python3
"""Analyze versioned mobile reconnect logs. All timings are milliseconds."""
import argparse
from collections import Counter, defaultdict
import hashlib
import json
import math
from pathlib import Path
import random
import re
import statistics

LEGACY_RULE_VERSION = "reconnect-field-ab-20261016-v1"
METRICS = {"visible_thread": "visibleThread", "thread_list": "shell"}
KINDS = ("refocus", "short background", "long background", "cold launch")
NETWORKS = ("wifi", "cellular", "other")

def quantile(values, q):
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * q
    lo, hi = math.floor(position), math.ceil(position)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (position - lo)

def kind(record):
    if record.get("launch") == "cold":
        return "cold launch"
    background = record.get("backgroundMs")
    if isinstance(background, (int, float)) and not isinstance(background, bool) and background >= 0:
        if background < 10000:
            return "refocus"
        return "short background" if background < 300000 else "long background"
    return "unclassified"

def binomial_test(correct, n):
    """Exact two-sided test at p=.5, equal/lower-probability outcomes."""
    if not n:
        return None
    tail = min(correct, n - correct)
    probability = math.fsum(math.exp(math.lgamma(n + 1) - math.lgamma(i + 1) -
        math.lgamma(n - i + 1) - n * math.log(2)) for i in range(tail + 1))
    return min(1.0, 2 * probability)

def guesses(records):
    answered = [r for r in records if r.get("guess") in ("old", "new")]
    correct = sum(r["guess"] == r["arm"] for r in answered)
    return {"n": len(answered), "correct": correct,
        "accuracy": correct / len(answered) if answered else None,
        "binomial_p_two_sided": binomial_test(correct, len(answered)),
        "skip": sum(r.get("guess") == "skip" for r in records),
        "unanswered": sum(r.get("guess") is None for r in records)}

def outcome(record):
    # An idle environment was not a reconnect, even in older buggy logs.
    if record.get("outcome") == "idle":
        return "idle"
    if record.get("interruptedAfterMs") is not None or record.get("outcome") in (
            "interrupted", "superseded", "process-ended"):
        return "interrupted"
    if record.get("outcome") in ("failed", "offline", "blocked"):
        return "failed"
    if record.get("outcome") in ("connected", "healthy"):
        return "connected"
    if record.get("outcome") == "idle":
        return "idle"
    return "incomplete"

def value(record, field):
    milestone = record.get(field, {})
    # A legacy optimistic 'live' transition is not a catch-up measurement.
    if outcome(record) != "connected" or milestone.get("completion") != "marker" or milestone.get("outcome") != "live":
        return None
    number = milestone.get("liveAfterMs")
    return number if isinstance(number, (int, float)) and not isinstance(number, bool) and math.isfinite(number) and number >= 0 else None

def bootstrap(old, new, repetitions, seed):
    rng = random.Random(seed)
    differences = [statistics.median(rng.choices(new, k=len(new))) -
                   statistics.median(rng.choices(old, k=len(old))) for _ in range(repetitions)]
    return [quantile(differences, .025), quantile(differences, .975)]

def compare(old, new, repetitions, seed):
    summary = {arm: {"n": len(values), "median_ms": quantile(values, .5),
        "p90_ms": quantile(values, .9)} for arm, values in (("old", old), ("new", new))}
    summary.update({"median_difference_new_minus_old_ms": None, "ci95_ms": None,
        "improvement": False, "regression": False, "status": "not enough data"})
    if not old or not new:
        return summary
    old_median, new_median = summary["old"]["median_ms"], summary["new"]["median_ms"]
    old_p90, new_p90 = summary["old"]["p90_ms"], summary["new"]["p90_ms"]
    summary["median_difference_new_minus_old_ms"] = new_median - old_median
    summary["regression_threshold_median_ms"] = max(.05 * old_median, 50)
    summary["regression_threshold_p90_ms"] = max(.05 * old_p90, 50)
    if min(len(old), len(new)) < 10:
        return summary
    ci = bootstrap(old, new, repetitions, seed)
    improvement = new_median < old_median and ci[1] < 0
    regression = new_median - old_median > max(.05 * old_median, 50) or new_p90 - old_p90 > max(.05 * old_p90, 50)
    summary.update({"ci95_ms": ci, "improvement": improvement, "regression": regression,
        "status": "improvement and regression" if improvement and regression else
            "regression" if regression else "improvement" if improvement else "no demonstrated improvement"})
    return summary

def analyze_legacy(records, repetitions=20000, seed=20261016):
    included = [r for r in records if r.get("experiment") is True]
    groups = defaultdict(list)
    for record in included:
        groups[(record["networkType"], kind(record))].append(record)
    results = []
    for network in NETWORKS:
        for reconnect_kind in (*KINDS, "unclassified"):
            rows = groups.get((network, reconnect_kind), [])
            if reconnect_kind == "unclassified" and not rows:
                continue
            arms = {}
            for arm in ("old", "new"):
                selected = [r for r in rows if r["arm"] == arm]
                arms[arm] = {"n_total": len(selected), "outcomes": dict(Counter(map(outcome, selected))),
                    "failed_attempts": sum(a.get("outcome") == "failed" for r in selected for a in r.get("attempts", [])),
                    "interrupted_attempts": sum(a.get("outcome") == "interrupted" for r in selected for a in r.get("attempts", [])),
                    "network_changed": sum(r.get("networkChanged") is True for r in selected)}
            metrics = {}
            for metric, field in METRICS.items():
                values = {arm: [v for r in rows if r["arm"] == arm and (v := value(r, field)) is not None] for arm in ("old", "new")}
                stable_seed = seed + int.from_bytes(hashlib.sha256(f"{network}/{reconnect_kind}/{metric}".encode()).digest()[:8], "big")
                metrics[metric] = compare(values["old"], values["new"], repetitions, stable_seed)
                for arm in ("old", "new"):
                    selected = [r for r in rows if r["arm"] == arm]
                    metrics[metric][arm]["missing_or_failed"] = len(selected) - len(values[arm])
                    metrics[metric][arm]["not_visible"] = sum(r.get(field, {}).get("outcome") == "not-visible" for r in selected)
            results.append({"network": network, "kind": reconnect_kind, "arms": arms, "metrics": metrics, "guesses": guesses(rows)})
    primary = [m for group in results if group["kind"] in KINDS for m in group["metrics"].values()]
    return {"rule_version": LEGACY_RULE_VERSION, "bootstrap_repetitions": repetitions, "seed": seed,
        "records_total": len(records), "experiment_records": len(included),
        "ignored_non_experiment": len(records) - len(included), "groups": results,
        "guesses_overall": guesses(included),
        "timing_improvement_observed": any(m["improvement"] for m in primary),
        "timing_regression_observed": any(m["regression"] for m in primary),
        "upstream_gate": "Review timing evidence and regressions before considering a PR" if any(m["improvement"] for m in primary) else "No upstream PR: timing improvement not established",
        "notes": [
            "Negative median differences favor New. Regression thresholds use Old as baseline.",
            "n is the number of successful, marker-confirmed observations for that metric; eligibility requires 10 per arm.",
            "Failed, interrupted, unfinished and not-visible observations are never converted to zero or imputed.",
            "Retries stay in one observation. Superseded wakeups count as interrupted, not extra successful reconnects.",
            "Network groups use the type at wakeup; mid-reconnect changes are counted explicitly.",
            "Running network changes without a background duration remain unclassified and do not enter the four primary groups.",
            "Cold timing starts at JavaScript entry, excluding native process/bootstrap time. Warm timing starts at the active callback.",
            "Current means the completion marker has been applied to client state; this does not time pixel presentation.",
            "M/I batching and completion-marker requests are shared in both arms. This trial compares B+C+L+S only.",
            "Percentile bootstrap resamples reconnects independently. Repeated trials may correlate; small strata and multiple comparisons warrant care.",
            "Guess results are secondary. Exact two-sided binomial tests use p=0.5 and exclude Skip/unanswered.",
        ]}

RULE_VERSION = "reconnect-field-events-v4"
CONTEXT_FIELDS = ("timestamp", "timingOrigin", "launch", "wakeup", "backgroundMs")


def load(path):
    data = json.loads(Path(path).read_text())
    if isinstance(data, list):
        rows, version = data, None
    elif isinstance(data, dict) and data.get("format") == "t3-mobile-connection-log" and isinstance(data.get("records"), list):
        rows, version = data["records"], data.get("version")
        if version not in (1, 2, 3, 4):
            raise ValueError(f"Unsupported export version: {version}")
    else:
        raise ValueError("Expected a Connections export or record array")
    seen = set()
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("id"), str) or row["id"] in seen:
            raise ValueError("Invalid or duplicate record id")
        seen.add(row["id"])
        if row.get("recordVersion", 2) not in (1, 2, 3, 4):
            raise ValueError("Unsupported record version")
        if not row.get("experiment"):
            continue
        if row.get("arm") not in ("old", "new") or row.get("networkType") not in NETWORKS:
            raise ValueError("Invalid experiment arm or network")
        if row.get("recordVersion", 2) >= 3:
            if not re.fullmatch(r"\d+-event-\d+", row.get("eventId", "")):
                raise ValueError("Missing event id")
            if row.get("eventAssignment") not in ("random", "continuation"):
                raise ValueError("Invalid event assignment")
        for field in METRICS.values():
            metric = row.get(field)
            if not isinstance(metric, dict):
                raise ValueError(f"Missing {field}")
            number = metric.get("liveAfterMs")
            if number is not None and (isinstance(number, bool) or not isinstance(number, (int, float)) or not math.isfinite(number) or number < 0):
                raise ValueError(f"Invalid {field}.liveAfterMs")
    return rows, version


def context_for(members, latest):
    """V4 is explicit and strict. Historical V3 origin repair is conservative."""
    if members[0]["recordVersion"] == 4:
        context = members[0].get("event")
        if not isinstance(context, dict) or context.get("id") != members[0]["eventId"] or any(key not in context for key in CONTEXT_FIELDS):
            raise ValueError("Version 4 requires canonical event metadata")
        ids = context.get("environments")
        if not isinstance(ids, list) or not ids or any(isinstance(n, bool) or not isinstance(n, int) or n < 1 for n in ids) or len(set(ids)) != len(ids):
            raise ValueError("Invalid event cohort")
        if context.get("visibleThreadEnvironment") is not None and context["visibleThreadEnvironment"] not in ids:
            raise ValueError("Visible thread is outside the event cohort")
        for row in members:
            if row.get("event") != context or row.get("environment") not in ids or row.get("eventEnvironmentCount") != len(ids):
                raise ValueError("Conflicting canonical event metadata or cohort")
            if any(row.get(field) != context.get(field) for field in CONTEXT_FIELDS):
                raise ValueError("Record timing differs from its canonical event")
        if context.get("launch") not in ("cold", "resume", "running") or context.get("timingOrigin") not in ("js-launch", "wakeup", "attempt"):
            raise ValueError("Invalid canonical event origin")
        if not isinstance(context.get("timestamp"), str) or not isinstance(context.get("wakeup"), str):
            raise ValueError("Missing canonical event origin")
        background = context.get("backgroundMs")
        if background is not None and (isinstance(background, bool) or not isinstance(background, (int, float)) or not math.isfinite(background) or background < 0):
            raise ValueError("Invalid background duration")
        return context, set(ids).issubset(latest), []
    active = [row for row in latest.values() if row.get("outcome") != "idle"]
    # Ignore an idle environment's old JS origin; never approximate missing
    # monotonic origins by adding wall-clock timestamp differences.
    wakeups = [row for row in active if row.get("timingOrigin") == "wakeup"]
    anchor = min(wakeups or active or members, key=lambda row: tuple(map(int, row["id"].split("-"))))
    context = {field: anchor.get(field) for field in CONTEXT_FIELDS}
    expected = max(row.get("eventEnvironmentCount", len(active)) for row in members)
    warnings = []
    if any(kind(row) != kind(anchor) for row in members):
        warnings.append("historical_event_reclassified")
    if any(any(row.get(field) != context[field] for field in CONTEXT_FIELDS) for row in active):
        warnings.append("historical_event_origin_mismatch")
    return context, len(active) >= expected, warnings


def group_events(rows):
    grouped = defaultdict(list)
    for row in rows:
        grouped[row["eventId"]].append(row)
    events = []
    for event_id, members in grouped.items():
        for field in ("recordVersion", "arm", "networkType", "eventAssignment", "experiment"):
            if len({row.get(field) for row in members}) != 1:
                raise ValueError(f"Event {event_id} has mixed {field}")
        answers = {row.get("guess") for row in members if row.get("guess") is not None}
        if len(answers) > 1 or not answers.issubset({"old", "new", "skip"}):
            raise ValueError(f"Event {event_id} has conflicting or invalid guesses")
        latest = {}
        for row in sorted(members, key=lambda row: tuple(map(int, row["id"].split("-")))):
            latest[row["environment"]] = row
        context, complete, warnings = context_for(members, latest)
        active = [row for row in latest.values() if row.get("outcome") != "idle"]
        supported = [row for row in active if row.get("legacyPacingSupported") is True]
        unknown = [row for row in active if row.get("legacyPacingSupported") is None]
        event = {**members[0], **{key: context[key] for key in CONTEXT_FIELDS},
            "id": event_id, "guess": next(iter(answers), None),
            "networkChanged": any(row.get("networkChanged") for row in active),
            "environment_count": len(active), "supported_environments": len(supported),
            "unknown_environments": len(unknown),
            "unsupported_environments": len(active) - len(supported) - len(unknown),
            "cohort_complete": complete, "warnings": warnings,
            "environment_outcomes": dict(Counter(outcome(row) for row in latest.values()))}
        attempts = {}
        for row in members:
            for index, attempt in enumerate(row.get("attempts", [])):
                attempts[(row["environment"], attempt.get("id", (row["id"], index)))] = attempt
        event["attempts"] = list(attempts.values())
        for field in METRICS.values():
            applicable = [row for row in [*supported, *unknown] if row[field]["outcome"] != "not-visible"]
            if field == "visibleThread" and members[0]["recordVersion"] == 4:
                pinned = context["visibleThreadEnvironment"]
                if any(row[field]["outcome"] != "not-visible" and row["environment"] != pinned for row in active):
                    raise ValueError("Unpinned thread measurement")
                applicable = [row for row in applicable if row["environment"] == pinned]
            values = []
            for row in applicable:
                same_origin = all(row.get(key) == context[key] for key in CONTEXT_FIELDS)
                values.append(value(row, field) if row.get("legacyPacingSupported") is True and same_origin else None)
            successful = complete and bool(values) and all(v is not None for v in values)
            event[field] = {"liveAfterMs": max(values) if successful else None,
                "completion": "marker" if successful else None,
                "outcome": "live" if successful else "not-visible" if field == "visibleThread" and not applicable else "incomplete"}
        events.append(event)
    return events


def analyze_events(rows, repetitions, seed):
    events = group_events(rows)
    randomized = [event for event in events if event["eventAssignment"] == "random"]
    groups = []
    for network in NETWORKS:
        for reconnect_kind in (*KINDS, "unclassified"):
            selected = [event for event in randomized if event["networkType"] == network and kind(event) == reconnect_kind]
            if not selected:
                continue
            arms = {}
            for arm in ("old", "new"):
                trials = [event for event in selected if event["arm"] == arm]
                arms[arm] = {"n_total": len(trials),
                    "environments": sum(event["environment_count"] for event in trials),
                    "unsupported_environments": sum(event["unsupported_environments"] for event in trials),
                    "unknown_environments": sum(event["unknown_environments"] for event in trials),
                    "partial_cohorts": sum(not event["cohort_complete"] for event in trials),
                    "environment_outcomes": dict(sum((Counter(event["environment_outcomes"]) for event in trials), Counter())),
                    "failed_attempts": sum(attempt.get("outcome") == "failed" for event in trials for attempt in event["attempts"]),
                    "interrupted_attempts": sum(attempt.get("outcome") == "interrupted" for event in trials for attempt in event["attempts"]),
                    "network_changed": sum(event["networkChanged"] for event in trials)}
            metrics = {}
            for metric, field in METRICS.items():
                values = {arm: [event[field]["liveAfterMs"] for event in selected if event["arm"] == arm and event[field]["liveAfterMs"] is not None] for arm in ("old", "new")}
                stable_seed = seed + int.from_bytes(hashlib.sha256(f"{network}/{reconnect_kind}/{metric}".encode()).digest()[:8], "big")
                result = compare(values["old"], values["new"], repetitions, stable_seed)
                for arm in ("old", "new"):
                    result[arm]["missing_or_failed"] = arms[arm]["n_total"] - len(values[arm])
                    result[arm]["not_visible"] = sum(event["arm"] == arm and event[field]["outcome"] == "not-visible" for event in selected)
                metrics[metric] = result
            groups.append({"network": network, "kind": reconnect_kind, "arms": arms, "metrics": metrics, "guesses": guesses(selected)})
    primary = [metric for group in groups if group["kind"] in KINDS for metric in group["metrics"].values()]
    return {"event_count": len(events), "randomized_events": len(randomized),
        "continuation_events": len(events) - len(randomized), "groups": groups,
        "guesses_overall": guesses(randomized),
        "continuation_guesses": guesses([event for event in events if event["eventAssignment"] == "continuation"]),
        "warnings": [{"event_id": event["id"], "issues": event["warnings"]} for event in events if event["warnings"]],
        "timing_improvement_observed": any(metric["improvement"] for metric in primary),
        "timing_regression_observed": any(metric["regression"] for metric in primary)}


def analyze(rows, repetitions=20000, seed=20261016):
    experimental = [row for row in rows if row.get("experiment") is True]
    return {"rule_version": RULE_VERSION, "comparison": "B+C+M+I+L+S", "unit": "event",
        "records_total": len(rows), "bootstrap_repetitions": repetitions, "seed": seed,
        "current_v4": analyze_events([row for row in experimental if row.get("recordVersion") == 4], repetitions, seed),
        "historical_v3": analyze_events([row for row in experimental if row.get("recordVersion") == 3], repetitions, seed),
        "legacy_per_environment": analyze_legacy([row for row in rows if row.get("recordVersion", 2) < 3], repetitions, seed),
        "notes": ["Versions are analyzed separately, never pooled.",
            "V4 validates explicit canonical event metadata, anonymous cohort identities and one event clock.",
            "Historical V3 excludes idle rows and classifies from reconnecting wakeups. Mixed timer origins remain missing; wall-clock rebasing is not used.",
            "Continuation events and unclassified reconnects remain outside primary randomized inference.",
            "Each network/background group and metric needs ten successful events per arm. False flags with smaller samples do not establish absence of regression.",
            "Cold timing starts at JavaScript entry. Markers time applied state, not pixel presentation.",
            "Repeated events on one phone and unadjusted multiple strata limit inference."]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("export")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--bootstrap", type=int, default=20000)
    parser.add_argument("--seed", type=int, default=20261016)
    args = parser.parse_args()
    if args.bootstrap < 1000:
        parser.error("Use at least 1000 bootstrap replicates")
    rows, version = load(args.export)
    result = analyze(rows, args.bootstrap, args.seed)
    result.update(export_version=version, source_sha256=hashlib.sha256(Path(args.export).read_bytes()).hexdigest())
    if args.output:
        args.output.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
    for version_key in ("current_v4", "historical_v3"):
        part = result[version_key]
        print(f"{version_key}: {part['event_count']} events; {part['randomized_events']} randomized, {part['continuation_events']} continuation")
        for group in part["groups"]:
            print(group["network"], group["kind"], json.dumps(group["metrics"]))
        print("Guesses", json.dumps(part["guesses_overall"]))
    print(f"Legacy: {result['legacy_per_environment']['experiment_records']} per-environment observations")


if __name__ == "__main__":
    main()
