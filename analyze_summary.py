import csv
import sys
from pathlib import Path


def main():
    path = Path("summary_results.csv")

    if not path.exists():
        print("summary_results.csv not found.")
        print("Run benchmark.ps1 first.")
        sys.exit(1)

    with path.open(newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        rows = list(reader)

    if not rows:
        print("No summary rows found.")
        sys.exit(0)

    columns = [
        "controller",
        "emulator",
        "runs",
        "success_rate",
        "weighted_avg_rtt_us",
        "retransmits",
        "losses",
        "avg_final_cwnd_bytes",
        "avg_final_risk",
    ]

    print(",".join(columns))

    for row in rows:
        values = []

        for column in columns:
            values.append(row.get(column, ""))

        print(",".join(values))


if __name__ == "__main__":
    main()