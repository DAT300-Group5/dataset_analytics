#!/usr/bin/env python3
"""
Database performance benchmark results analysis tool.

This script reads benchmark test result data and generates performance comparison charts, including:
- Average wall clock time comparison for different engines across datasets
- Engine throughput (rows/sec) comparison
- Time To First Row (TTFR) share analysis by engine
"""

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

# Define the fixed order of engines for consistent chart display
ENGINE_ORDER = ['duckdb', 'sqlite', 'chdb']


def create_wall_time_charts(data_frame, output_dir):
    """
    Create wall clock time comparison charts for each query and dataset combination.
    
    Args:
        data_frame (pd.DataFrame): DataFrame containing benchmark test results
        output_dir (Path): Output directory path
    """
    for (query_id, dataset_name), group in data_frame.groupby(["query_id", "dataset"]):
        pivot_table = group.pivot_table(
            index="engine", 
            values="mean_wall_s", 
            aggfunc="mean"
        )
        # Reorder according to ENGINE_ORDER
        pivot_table = pivot_table.reindex(ENGINE_ORDER)
        
        ax = pivot_table.plot(
            kind="bar", 
            legend=False, 
            title=f"{query_id} — mean wall time by engine ({dataset_name})"
        )
        ax.set_ylabel("seconds")
        plt.tight_layout()
        
        output_file = output_dir / f"{query_id}__{dataset_name}__engine_wall.png"
        plt.savefig(output_file, dpi=160)
        plt.close()


def create_throughput_charts(data_frame, output_dir):
    """
    Create throughput comparison charts for each query.
    
    Args:
        data_frame (pd.DataFrame): DataFrame containing benchmark test results
        output_dir (Path): Output directory path
    """
    for query_id, group in data_frame.groupby("query_id"):
        pivot_table = group.pivot_table(
            index="engine", 
            values="rows_per_sec", 
            aggfunc="mean"
        )
        # Reorder according to ENGINE_ORDER
        pivot_table = pivot_table.reindex(ENGINE_ORDER)
        
        ax = pivot_table.plot(
            kind="bar", 
            legend=False, 
            title=f"{query_id} — rows/sec by engine (avg across datasets)"
        )
        ax.set_ylabel("rows/sec")
        plt.tight_layout()
        
        output_file = output_dir / f"{query_id}__engine_throughput.png"
        plt.savefig(output_file, dpi=160)
        plt.close()


def create_ttfr_chart(data_frame, output_dir):
    """
    Create Time To First Row (TTFR) share percentage chart.
    
    Args:
        data_frame (pd.DataFrame): DataFrame containing benchmark test results
        output_dir (Path): Output directory path
    """
    pivot_table = data_frame.pivot_table(
        index="engine", 
        values="ttfr_share_pct", 
        aggfunc="mean"
    )
    # Reorder according to ENGINE_ORDER
    pivot_table = pivot_table.reindex(ENGINE_ORDER)
    
    ax = pivot_table.plot(
        kind="bar", 
        legend=False, 
        title="Average TTFR share of wall time by engine"
    )
    ax.set_ylabel("%")
    plt.tight_layout()
    
    output_file = output_dir / "avg_ttfr_share_by_engine.png"
    plt.savefig(output_file, dpi=160)
    plt.close()


def create_cpu_usage_charts(data_frame, output_dir):
    """
    Create CPU usage comparison charts for each query and dataset combination.
    
    Args:
        data_frame (pd.DataFrame): DataFrame containing benchmark test results
        output_dir (Path): Output directory path
    """
    for (query_id, dataset_name), group in data_frame.groupby(["query_id", "dataset"]):
        pivot_table = group.pivot_table(
            index="engine", 
            values="mean_cpu_pct", 
            aggfunc="mean"
        )
        # Reorder according to ENGINE_ORDER
        pivot_table = pivot_table.reindex(ENGINE_ORDER)
        
        ax = pivot_table.plot(
            kind="bar", 
            legend=False, 
            title=f"{query_id} — mean CPU usage by engine ({dataset_name})",
            color=['#1f77b4', '#ff7f0e', '#2ca02c']  # Colors for duckdb, sqlite, chdb
        )
        ax.set_ylabel("CPU Usage (%)")
        plt.tight_layout()
        
        output_file = output_dir / f"{query_id}__{dataset_name}__engine_cpu.png"
        plt.savefig(output_file, dpi=160)
        plt.close()


def create_memory_usage_charts(data_frame, output_dir):
    """
    Create memory usage (RSS) comparison charts for each query and dataset combination.
    
    Args:
        data_frame (pd.DataFrame): DataFrame containing benchmark test results
        output_dir (Path): Output directory path
    """
    for (query_id, dataset_name), group in data_frame.groupby(["query_id", "dataset"]):
        pivot_table = group.pivot_table(
            index="engine", 
            values="mean_rss_mb", 
            aggfunc="mean"
        )
        # Reorder according to ENGINE_ORDER
        pivot_table = pivot_table.reindex(ENGINE_ORDER)
        
        ax = pivot_table.plot(
            kind="bar", 
            legend=False, 
            title=f"{query_id} — mean memory usage by engine ({dataset_name})",
            color=['#1f77b4', '#ff7f0e', '#2ca02c']  # Colors for duckdb, sqlite, chdb
        )
        ax.set_ylabel("Memory Usage (MB)")
        plt.tight_layout()
        
        output_file = output_dir / f"{query_id}__{dataset_name}__engine_memory.png"
        plt.savefig(output_file, dpi=160)
        plt.close()


def create_performance_scatter_plot(data_frame, output_dir):
    """
    Create scatter plot showing CPU usage vs Memory usage for different engines.
    
    Args:
        data_frame (pd.DataFrame): DataFrame containing benchmark test results
        output_dir (Path): Output directory path
    """
    plt.figure(figsize=(10, 8))
    
    engines = data_frame['engine'].unique()
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']
    
    for i, engine in enumerate(engines):
        engine_data = data_frame[data_frame['engine'] == engine]
        plt.scatter(
            engine_data['mean_cpu_pct'], 
            engine_data['mean_rss_mb'], 
            label=engine,
            color=colors[i % len(colors)],
            alpha=0.7,
            s=100
        )
    
    plt.xlabel('Mean CPU Usage (%)')
    plt.ylabel('Mean Memory Usage (MB)')
    plt.title('CPU vs Memory Usage by Engine')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    output_file = output_dir / "cpu_vs_memory_scatter.png"
    plt.savefig(output_file, dpi=160)
    plt.close()


def create_multi_metric_comparison(data_frame, output_dir):
    """
    Create side-by-side comparison of all key metrics.
    
    Args:
        data_frame (pd.DataFrame): DataFrame containing benchmark test results
        output_dir (Path): Output directory path
    """
    metrics = {
        'mean_wall_s': 'Wall Time (s)',
        'rows_per_sec': 'Throughput (rows/sec)', 
        'mean_cpu_pct': 'CPU Usage (%)',
        'mean_rss_mb': 'Memory Usage (MB)'
    }
    
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    axes = axes.flatten()
    
    for i, (metric, label) in enumerate(metrics.items()):
        pivot_table = data_frame.pivot_table(
            index="engine", 
            values=metric, 
            aggfunc="mean"
        )
        # Reorder according to ENGINE_ORDER
        pivot_table = pivot_table.reindex(ENGINE_ORDER)
        
        pivot_table.plot(
            kind="bar", 
            ax=axes[i],
            legend=False, 
            title=f"Average {label} by Engine",
            color=['#1f77b4', '#ff7f0e', '#2ca02c']  # Colors for duckdb, sqlite, chdb
        )
        axes[i].set_ylabel(label)
        axes[i].tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    
    output_file = output_dir / "multi_metric_comparison.png"
    plt.savefig(output_file, dpi=160)
    plt.close()


def main():
    """Main function: load data and generate all charts."""
    script_dir = Path(__file__).parent.resolve()
    results_file = script_dir / "results" / "manifest.csv"
    output_dir = script_dir / "results"
    
    # Load benchmark test result data
    results_df = pd.read_csv(results_file)
    
    # Generate original charts
    print("Generating wall time charts...")
    create_wall_time_charts(results_df, output_dir)
    
    print("Generating throughput charts...")
    create_throughput_charts(results_df, output_dir)
    
    print("Generating TTFR charts...")
    create_ttfr_chart(results_df, output_dir)
    
    # Generate new resource usage charts
    print("Generating CPU usage charts...")
    create_cpu_usage_charts(results_df, output_dir)
    
    print("Generating memory usage charts...")
    create_memory_usage_charts(results_df, output_dir)
    
    # Generate comprehensive performance analysis charts
    print("Generating performance scatter plot...")
    create_performance_scatter_plot(results_df, output_dir)
    
    print("Generating multi-metric comparison...")
    create_multi_metric_comparison(results_df, output_dir)
    
    print("All charts generated successfully and saved to results/ directory")


if __name__ == "__main__":
    main()
