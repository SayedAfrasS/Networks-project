import csv
from pathlib import Path

SUMMARY_FILE = Path("summary_results.csv")
GRAPH_DATA_FILE = Path("graph_data.csv")


def network_label(emulator):
    multipath = "multipath=on" in emulator

    if "loss=0.0%" in emulator:
        scenario = "good"
    elif "loss=20.0%" in emulator:
        scenario = "lossy"
    elif "loss=35.0%" in emulator:
        scenario = "bad"
    else:
        scenario = "unknown"

    if multipath:
        return f"multipath {scenario}"
    else:
        return f"single {scenario}"


def load_rows():
    if not SUMMARY_FILE.exists():
        print("summary_results.csv not found.")
        print("Run benchmark.ps1 or benchmark_multipath.ps1 first.")
        return []

    rows = []

    with SUMMARY_FILE.open(newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            try:
                label = f"{row['controller']} / {network_label(row['emulator'])}"

                rows.append(
                    {
                        "label": label,
                        "success_percent": float(row["success_rate"]) * 100.0,
                        "avg_rtt_ms": float(row["weighted_avg_rtt_us"]) / 1000.0,
                        "retransmits": int(row["retransmits"]),
                        "losses": int(row["losses"]),
                        "avg_final_cwnd_bytes": int(row["avg_final_cwnd_bytes"]),
                        "avg_final_risk": float(row["avg_final_risk"]),
                        "avg_duration_ms": int(row["avg_duration_ms"]),
                    }
                )
            except (KeyError, ValueError):
                continue

    return rows


def write_graph_data(rows):
    fieldnames = [
        "label",
        "success_percent",
        "avg_rtt_ms",
        "retransmits",
        "losses",
        "avg_final_cwnd_bytes",
        "avg_final_risk",
        "avg_duration_ms",
    ]

    with GRAPH_DATA_FILE.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)

        writer.writeheader()

        for row in rows:
            writer.writerow(row)


def make_graphs(rows):
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print()
        print("matplotlib is not installed.")
        print("graph_data.csv was still created.")
        print()
        print("Install matplotlib with:")
        print("  python -m pip install matplotlib")
        return

    labels = [row["label"] for row in rows]

    charts = [
        (
            "success_rate.png",
            "Success Rate",
            "Success Percent",
            "success_percent",
        ),
        (
            "avg_rtt.png",
            "Average RTT",
            "RTT ms",
            "avg_rtt_ms",
        ),
        (
            "retransmits.png",
            "Retransmissions",
            "Retransmit Count",
            "retransmits",
        ),
        (
            "losses.png",
            "Losses",
            "Loss Count",
            "losses",
        ),
        (
            "final_risk.png",
            "Average Final Risk",
            "Risk",
            "avg_final_risk",
        ),
        (
            "duration.png",
            "Average Experiment Duration",
            "Duration ms",
            "avg_duration_ms",
        ),
    ]

    for filename, title, ylabel, key in charts:
        values = [row[key] for row in rows]

        plt.figure(figsize=(13, 5))

        plt.bar(labels, values)

        plt.title(title)
        plt.ylabel(ylabel)

        plt.xticks(rotation=45, ha="right")

        plt.tight_layout()

        plt.savefig(filename, dpi=200)
        plt.close()

        print(f"Wrote {filename}")


def main():
    rows = load_rows()

    if not rows:
        return

    write_graph_data(rows)

    print(f"Wrote {GRAPH_DATA_FILE}")

    make_graphs(rows)


if __name__ == "__main__":
    main()