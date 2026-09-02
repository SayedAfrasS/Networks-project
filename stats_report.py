import csv
import math
from collections import defaultdict
from pathlib import Path

EXPERIMENT_FILE = Path("experiment_results.csv")
MULTIPATH_FILE = Path("multipath_results.csv")
OUTPUT_FILE = Path("stats_report.csv")


def scenario_label(emulator):
    if "multipath=on" in emulator:
        prefix = "multipath"
    else:
        prefix = "single"

    if "loss=0.0%" in emulator:
        return f"{prefix} good"

    if "loss=20.0%" in emulator:
        return f"{prefix} lossy"

    if "loss=35.0%" in emulator:
        return f"{prefix} bad"

    return f"{prefix} unknown"


def mean(values):
    if not values:
        return 0.0
    return sum(values) / len(values)


def stdev(values):
    if len(values) < 2:
        return 0.0

    m = mean(values)
    var = sum((v - m) ** 2 for v in values) / (len(values) - 1)
    return math.sqrt(var)


def ci95(values):
    if not values:
        return 0.0
    return 1.96 * stdev(values) / math.sqrt(len(values))


def stats_for(values):
    return {
        "n": len(values),
        "mean": mean(values),
        "std": stdev(values),
        "min": min(values) if values else 0.0,
        "max": max(values) if values else 0.0,
        "ci95": ci95(values),
    }


def collect_experiment():
    groups = defaultdict(lambda: defaultdict(list))

    if not EXPERIMENT_FILE.exists():
        return groups

    with EXPERIMENT_FILE.open(newline="", encoding="utf-8") as file:
        for row in csv.DictReader(file):
            try:
                controller = row["controller"]
                scenario = scenario_label(row["emulator"])
                key = f"{controller} / {scenario}"

                reliable = int(row["reliable_sent"])
                acks = int(row["acks"])

                success = (acks / reliable) if reliable > 0 else 0.0

                groups[key]["success_percent"].append(success * 100.0)
                groups[key]["rtt_ms"].append(float(row["rtt_avg_us"]) / 1000.0)
                groups[key]["retransmits"].append(int(row["retransmits"]))
                groups[key]["losses"].append(int(row["losses"]))
                groups[key]["duration_ms"].append(float(row["duration_ms"]))
            except (KeyError, ValueError):
                continue

    return groups


def collect_multipath():
    groups = defaultdict(lambda: defaultdict(list))

    if not MULTIPATH_FILE.exists():
        return groups

    with MULTIPATH_FILE.open(newline="", encoding="utf-8") as file:
        for row in csv.DictReader(file):
            try:
                controller = row["controller"]
                path = row["path_name"]
                key = f"{controller} / path {path}"

                samples = int(row["rtt_samples"])

                if samples > 0:
                    groups[key]["rtt_ms"].append(
                        float(row["rtt_avg_us"]) / 1000.0
                    )

                groups[key]["retransmits"].append(int(row["retransmits"]))
                groups[key]["losses"].append(int(row["losses"]))
                groups[key]["quality"].append(float(row["quality"]))
            except (KeyError, ValueError):
                continue

    return groups


def write_report():
    rows_out = []

    sources = (
        ("experiment", collect_experiment()),
        ("multipath", collect_multipath()),
    )

    for source, groups in sources:
        for group in sorted(groups):
            for metric in sorted(groups[group]):
                s = stats_for(groups[group][metric])

                rows_out.append(
                    {
                        "source": source,
                        "group": group,
                        "metric": metric,
                        "n": s["n"],
                        "mean": round(s["mean"], 3),
                        "std": round(s["std"], 3),
                        "min": round(s["min"], 3),
                        "max": round(s["max"], 3),
                        "ci95": round(s["ci95"], 3),
                    }
                )

    with OUTPUT_FILE.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "source",
                "group",
                "metric",
                "n",
                "mean",
                "std",
                "min",
                "max",
                "ci95",
            ],
        )

        writer.writeheader()
        writer.writerows(rows_out)

    return rows_out


def main():
    rows = write_report()

    if not rows:
        print("No data found.")
        print("Run benchmark.ps1 and benchmark_multipath.ps1 first.")
        return

    print(f"Wrote {OUTPUT_FILE}")

    current = None

    for row in rows:
        label = f"{row['source']} | {row['group']}"

        if label != current:
            current = label
            print()
            print(label)

        print(
            "  {}: n={} mean={} std={} ci95={} min={} max={}".format(
                row["metric"],
                row["n"],
                row["mean"],
                row["std"],
                row["ci95"],
                row["min"],
                row["max"],
            )
        )


if __name__ == "__main__":
    main()