"""
Plots the results of one or more experiment CSVs produced by
run_experiment.py.

Usage:
    python experiments/plot_results.py --files window5 window10 --outdir experiments/results
"""

import argparse
import pandas as pd
import matplotlib.pyplot as plt
import os


def plot_throughput(df, label, outdir):
    plt.figure(figsize=(8, 5))
    plt.plot(df["elapsed_s"], df["throughput_records_per_s"], color="blue")
    plt.xlabel("Elapsed time (s)")
    plt.ylabel("Throughput (records/s)")
    plt.title(f"Collector Throughput Over Time — {label}")
    plt.grid(True, alpha=0.3)
    path = os.path.join(outdir, f"throughput_{label}.png")
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Saved {path}")


def plot_latency(df, label, outdir):
    node_cols = [c for c in df.columns if c.endswith("_latency_s")]

    for col in sorted(node_cols):
        plt.figure(figsize=(8, 5))
        plt.plot(df["elapsed_s"], df[col], color="blue")
        plt.xlabel("Elapsed time (s)")
        plt.ylabel("Latency (s)")
        node_name = col.replace("_latency_s", "")
        plt.title(f"Latency Over Time — {node_name} — {label}")
        plt.grid(True, alpha=0.3)
        path = os.path.join(outdir, f"latency_{node_name}_{label}.png")
        plt.savefig(path, dpi=150, bbox_inches="tight")
        plt.close()
        print(f"Saved {path}")


def main(files, results_dir, outdir):
    os.makedirs(outdir, exist_ok=True)
    found_any = False

    for label in files:
        path = os.path.join(results_dir, f"{label}.csv")
        if not os.path.exists(path):
            print(f"Skipping missing file: {path}")
            continue
        df = pd.read_csv(path)
        plot_throughput(df, label, outdir)
        plot_latency(df, label, outdir)
        found_any = True

    if not found_any:
        print("No CSV files found. Run run_experiment.py first.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Plot Week 2 experiment results")
    parser.add_argument("--files", nargs="+", required=True,
                         help="Labels of experiments to plot (matches --label used in run_experiment.py)")
    parser.add_argument("--results-dir", type=str, default="experiments/results")
    parser.add_argument("--outdir", type=str, default="experiments/results")
    args = parser.parse_args()
    main(args.files, args.results_dir, args.outdir)
