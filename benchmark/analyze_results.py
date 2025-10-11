#!/usr/bin/env python3
"""
Database performance benchmark results analysis tool.

This script reads benchmark test result data from summary.json and generates 
comprehensive performance comparison charts.
"""

import json
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np

# Define colors for different engines
ENGINE_COLORS = {
    'duckdb': '#1f77b4',
    'sqlite': '#ff7f0e',
    'chdb': '#2ca02c'
}


def load_summary_data(file_path):
    """
    Load benchmark summary data from JSON file.
    
    Args:
        file_path (Path): Path to summary.json file
        
    Returns:
        dict: Parsed JSON data
    """
    with open(file_path, 'r') as f:
        return json.load(f)


def create_execution_time_comparison(data, output_dir):
    """
    Create execution time comparison chart across queries and engines.
    
    Args:
        data (dict): Summary data
        output_dir (Path): Output directory path
    """
    queries = list(data.keys())
    engines = set()
    for query_data in data.values():
        engines.update(query_data.keys())
    engines = sorted(list(engines))
    
    # Prepare data for plotting
    x = np.arange(len(queries)) * 1.5  # Increase spacing between groups
    width = 0.25  # Width of each bar
    
    fig, ax = plt.subplots(figsize=(12, 6))
    
    for i, engine in enumerate(engines):
        avg_times = []
        for query in queries:
            if engine in data[query]:
                avg_times.append(data[query][engine]['execution_time']['avg'])
            else:
                avg_times.append(0)
        
        offset = width * (i - len(engines) / 2 + 0.5)
        ax.bar(x + offset, avg_times, width, label=engine, 
               color=ENGINE_COLORS.get(engine, '#333333'), edgecolor='black', linewidth=1)
    
    ax.set_xlabel('Query')
    ax.set_ylabel('Execution Time (seconds)')
    ax.set_title('Average Execution Time by Query and Engine')
    ax.set_xticks(x)
    ax.set_xticklabels(queries)
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    output_file = output_dir / "execution_time_comparison.png"
    plt.savefig(output_file, dpi=160)
    print(f"✓ Generated: {output_file.name}")
    plt.close()


def create_memory_usage_comparison(data, output_dir):
    """
    Create memory usage comparison chart.
    
    Args:
        data (dict): Summary data
        output_dir (Path): Output directory path
    """
    queries = list(data.keys())
    engines = set()
    for query_data in data.values():
        engines.update(query_data.keys())
    engines = sorted(list(engines))
    
    x = np.arange(len(queries)) * 1.5  # Increase spacing between groups
    width = 0.25  # Width of each bar
    
    fig, ax = plt.subplots(figsize=(12, 6))
    
    for i, engine in enumerate(engines):
        avg_memory = []
        for query in queries:
            if engine in data[query]:
                # Convert bytes to MB
                memory_mb = data[query][engine]['peak_memory_bytes']['avg'] / (1024 * 1024)
                avg_memory.append(memory_mb)
            else:
                avg_memory.append(0)
        
        offset = width * (i - len(engines) / 2 + 0.5)
        ax.bar(x + offset, avg_memory, width, label=engine,
               color=ENGINE_COLORS.get(engine, '#333333'), edgecolor='black', linewidth=1)
    
    ax.set_xlabel('Query')
    ax.set_ylabel('Peak Memory (MB)')
    ax.set_title('Average Peak Memory Usage by Query and Engine')
    ax.set_xticks(x)
    ax.set_xticklabels(queries)
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    output_file = output_dir / "memory_usage_comparison.png"
    plt.savefig(output_file, dpi=160)
    print(f"✓ Generated: {output_file.name}")
    plt.close()


def create_cpu_usage_comparison(data, output_dir):
    """
    Create CPU usage comparison chart.
    
    Args:
        data (dict): Summary data
        output_dir (Path): Output directory path
    """
    queries = list(data.keys())
    engines = set()
    for query_data in data.values():
        engines.update(query_data.keys())
    engines = sorted(list(engines))
    
    x = np.arange(len(queries)) * 1.5  # Increase spacing between groups
    width = 0.25  # Width of each bar
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    # Peak CPU
    for i, engine in enumerate(engines):
        cpu_peak = []
        for query in queries:
            if engine in data[query]:
                cpu_peak.append(data[query][engine]['cpu_peek_percent']['avg'])
            else:
                cpu_peak.append(0)
        
        offset = width * (i - len(engines) / 2 + 0.5)
        ax1.bar(x + offset, cpu_peak, width, label=engine,
                color=ENGINE_COLORS.get(engine, '#333333'), edgecolor='black', linewidth=1)
    
    ax1.set_xlabel('Query')
    ax1.set_ylabel('CPU Peak (%)')
    ax1.set_title('Average Peak CPU Usage by Query and Engine')
    ax1.set_xticks(x)
    ax1.set_xticklabels(queries)
    ax1.legend()
    ax1.grid(True, alpha=0.3, axis='y')
    
    # Average CPU
    for i, engine in enumerate(engines):
        cpu_avg = []
        for query in queries:
            if engine in data[query]:
                cpu_avg.append(data[query][engine]['cpu_avg_percent']['avg'])
            else:
                cpu_avg.append(0)
        
        offset = width * (i - len(engines) / 2 + 0.5)
        ax2.bar(x + offset, cpu_avg, width, label=engine,
                color=ENGINE_COLORS.get(engine, '#333333'), edgecolor='black', linewidth=1)
    
    ax2.set_xlabel('Query')
    ax2.set_ylabel('CPU Average (%)')
    ax2.set_title('Average CPU Usage by Query and Engine')
    ax2.set_xticks(x)
    ax2.set_xticklabels(queries)
    ax2.legend()
    ax2.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    output_file = output_dir / "cpu_usage_comparison.png"
    plt.savefig(output_file, dpi=160)
    print(f"✓ Generated: {output_file.name}")
    plt.close()


def create_throughput_comparison(data, output_dir):
    """
    Create throughput (rows/sec) comparison chart.
    
    Args:
        data (dict): Summary data
        output_dir (Path): Output directory path
    """
    queries = list(data.keys())
    engines = set()
    for query_data in data.values():
        engines.update(query_data.keys())
    engines = sorted(list(engines))
    
    x = np.arange(len(queries)) * 1.5  # Increase spacing between groups
    width = 0.25  # Width of each bar
    
    fig, ax = plt.subplots(figsize=(12, 6))
    
    for i, engine in enumerate(engines):
        throughput = []
        for query in queries:
            if engine in data[query]:
                rows = data[query][engine]['output_rows']
                time_sec = data[query][engine]['execution_time']['avg']
                throughput.append(rows / time_sec if time_sec > 0 else 0)
            else:
                throughput.append(0)
        
        offset = width * (i - len(engines) / 2 + 0.5)
        ax.bar(x + offset, throughput, width, label=engine,
               color=ENGINE_COLORS.get(engine, '#333333'), edgecolor='black', linewidth=1)
    
    ax.set_xlabel('Query')
    ax.set_ylabel('Throughput (rows/sec)')
    ax.set_title('Throughput by Query and Engine')
    ax.set_xticks(x)
    ax.set_xticklabels(queries)
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    output_file = output_dir / "throughput_comparison.png"
    plt.savefig(output_file, dpi=160)
    print(f"✓ Generated: {output_file.name}")
    plt.close()


def create_performance_percentiles(data, output_dir):
    """
    Create box plot showing performance percentiles for execution time.
    
    Args:
        data (dict): Summary data
        output_dir (Path): Output directory path
    """
    for query, query_data in data.items():
        engines = list(query_data.keys())
        
        fig, ax = plt.subplots(figsize=(10, 6))
        
        positions = []
        labels = []
        box_data = []
        colors_list = []
        
        for i, engine in enumerate(engines):
            exec_time = query_data[engine]['execution_time']
            # Create box plot data: [min, p50, avg, p95, max]
            box_data.append([
                exec_time['min'],
                exec_time['p50'],
                exec_time['avg'],
                exec_time['p95'],
                exec_time['max']
            ])
            positions.append(i)
            labels.append(engine)
            colors_list.append(ENGINE_COLORS.get(engine, '#333333'))
        
        # Create box plot
        bp = ax.boxplot(box_data, positions=positions, widths=0.6,
                        patch_artist=True, showmeans=True,
                        labels=labels)
        
        # Color the boxes
        for patch, color in zip(bp['boxes'], colors_list):
            patch.set_facecolor(color)
            patch.set_alpha(0.7)
        
        ax.set_ylabel('Execution Time (seconds)')
        ax.set_title(f'{query} - Execution Time Distribution (min, p50, avg, p95, max)')
        ax.grid(True, alpha=0.3, axis='y')
        
        plt.tight_layout()
        output_file = output_dir / f"{query}_percentiles.png"
        plt.savefig(output_file, dpi=160)
        print(f"✓ Generated: {output_file.name}")
        plt.close()


def create_comprehensive_dashboard(data, output_dir):
    """
    Create a comprehensive dashboard with multiple metrics.
    
    Args:
        data (dict): Summary data
        output_dir (Path): Output directory path
    """
    queries = list(data.keys())
    engines = set()
    for query_data in data.values():
        engines.update(query_data.keys())
    engines = sorted(list(engines))
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    
    # 1. Execution Time
    ax = axes[0, 0]
    x = np.arange(len(queries)) * 1.5  # Increase spacing between groups
    width = 0.25  # Width of each bar
    for i, engine in enumerate(engines):
        times = [data[q][engine]['execution_time']['avg'] if engine in data[q] else 0 
                 for q in queries]
        offset = width * (i - len(engines) / 2 + 0.5)
        ax.bar(x + offset, times, width, label=engine,
               color=ENGINE_COLORS.get(engine, '#333333'), edgecolor='black', linewidth=1)
    ax.set_xlabel('Query')
    ax.set_ylabel('Time (s)')
    ax.set_title('Execution Time')
    ax.set_xticks(x)
    ax.set_xticklabels(queries)
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    
    # 2. Memory Usage
    ax = axes[0, 1]
    for i, engine in enumerate(engines):
        memory = [data[q][engine]['peak_memory_bytes']['avg'] / (1024 * 1024) 
                  if engine in data[q] else 0 for q in queries]
        offset = width * (i - len(engines) / 2 + 0.5)
        ax.bar(x + offset, memory, width, label=engine,
               color=ENGINE_COLORS.get(engine, '#333333'), edgecolor='black', linewidth=1)
    ax.set_xlabel('Query')
    ax.set_ylabel('Memory (MB)')
    ax.set_title('Peak Memory Usage')
    ax.set_xticks(x)
    ax.set_xticklabels(queries)
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    
    # 3. CPU Usage
    ax = axes[1, 0]
    for i, engine in enumerate(engines):
        cpu = [data[q][engine]['cpu_avg_percent']['avg'] if engine in data[q] else 0 
               for q in queries]
        offset = width * (i - len(engines) / 2 + 0.5)
        ax.bar(x + offset, cpu, width, label=engine,
               color=ENGINE_COLORS.get(engine, '#333333'), edgecolor='black', linewidth=1)
    ax.set_xlabel('Query')
    ax.set_ylabel('CPU (%)')
    ax.set_title('Average CPU Usage')
    ax.set_xticks(x)
    ax.set_xticklabels(queries)
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    
    # 4. Throughput
    ax = axes[1, 1]
    for i, engine in enumerate(engines):
        throughput = []
        for q in queries:
            if engine in data[q]:
                rows = data[q][engine]['output_rows']
                time_sec = data[q][engine]['execution_time']['avg']
                throughput.append(rows / time_sec if time_sec > 0 else 0)
            else:
                throughput.append(0)
        offset = width * (i - len(engines) / 2 + 0.5)
        ax.bar(x + offset, throughput, width, label=engine,
               color=ENGINE_COLORS.get(engine, '#333333'), edgecolor='black', linewidth=1)
    ax.set_xlabel('Query')
    ax.set_ylabel('Rows/sec')
    ax.set_title('Throughput')
    ax.set_xticks(x)
    ax.set_xticklabels(queries)
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    output_file = output_dir / "comprehensive_dashboard.png"
    plt.savefig(output_file, dpi=160)
    print(f"✓ Generated: {output_file.name}")
    plt.close()


def create_performance_summary_table(data, output_dir):
    """
    Create a text summary of performance metrics.
    
    Args:
        data (dict): Summary data
        output_dir (Path): Output directory path
    """
    output_file = output_dir / "performance_summary.txt"
    
    with open(output_file, 'w') as f:
        f.write("=" * 80 + "\n")
        f.write("BENCHMARK PERFORMANCE SUMMARY\n")
        f.write("=" * 80 + "\n\n")
        
        for query, query_data in data.items():
            f.write(f"\n{query}\n")
            f.write("-" * 80 + "\n")
            
            for engine, metrics in query_data.items():
                f.write(f"\n  Engine: {engine.upper()}\n")
                f.write(f"    Execution Time (avg): {metrics['execution_time']['avg']:.4f} s\n")
                f.write(f"    Execution Time (p50): {metrics['execution_time']['p50']:.4f} s\n")
                f.write(f"    Execution Time (p95): {metrics['execution_time']['p95']:.4f} s\n")
                f.write(f"    Peak Memory: {metrics['peak_memory_bytes']['avg'] / (1024 * 1024):.2f} MB\n")
                f.write(f"    CPU Peak: {metrics['cpu_peek_percent']['avg']:.2f}%\n")
                f.write(f"    CPU Average: {metrics['cpu_avg_percent']['avg']:.2f}%\n")
                f.write(f"    Output Rows: {metrics['output_rows']}\n")
                
                time_sec = metrics['execution_time']['avg']
                throughput = metrics['output_rows'] / time_sec if time_sec > 0 else 0
                f.write(f"    Throughput: {throughput:.2f} rows/sec\n")
            
            f.write("\n")
    
    print(f"✓ Generated: {output_file.name}")


def main():
    """Main function: load data and generate all visualizations."""
    script_dir = Path(__file__).parent.resolve()
    summary_file = script_dir / "results" / "summary.json"
    output_dir = script_dir / "results"
    
    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("Loading benchmark data...")
    data = load_summary_data(summary_file)
    print(f"Loaded data for {len(data)} queries\n")
    
    print("Generating visualizations...")
    print("-" * 50)
    
    create_execution_time_comparison(data, output_dir)
    create_memory_usage_comparison(data, output_dir)
    create_cpu_usage_comparison(data, output_dir)
    create_throughput_comparison(data, output_dir)
    create_performance_percentiles(data, output_dir)
    create_comprehensive_dashboard(data, output_dir)
    create_performance_summary_table(data, output_dir)
    
    print("-" * 50)
    print(f"\n✅ All visualizations generated successfully!")
    print(f"📁 Output directory: {output_dir.resolve()}")


if __name__ == "__main__":
    main()
