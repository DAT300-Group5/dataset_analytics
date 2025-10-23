#!/usr/bin/env python3
"""
Database performance benchmark results analysis tool.

This script reads benchmark test result data from summary.json and generates 
comprehensive performance comparison charts.
"""

import json
import sys
from pathlib import Path
from typing import Dict, List

import matplotlib.pyplot as plt
from matplotlib import colors as mcolors
import numpy as np

from config.config_loader import ConfigLoader
from models.plot_params import PlotParams
from cli.cli import parse_env_args
from util.file_utils import clean_path

# Default base colors per engine (deterministic mapping)
# SQLite -> blue, DuckDB -> orange, CHDB -> green (as requested)
ENGINE_BASE_COLORS = {
    'sqlite': '#1f77b4',   # blue
    'duckdb': '#ff7f0e',   # orange
    'chdb':   '#2ca02c',   # green
}


def _find_engine_in_label(label: str) -> str:
    """Try to extract the engine name from a label string.

    The label patterns in this file commonly include the engine name as a
    substring (for example: 'Q1_sqlite', 'sqlite_ops_default', 'group_chdb').
    This helper does a case-insensitive substring match against known
    engines and returns the matching engine key or None.
    """
    ll = label.lower()
    for engine in ENGINE_BASE_COLORS.keys():
        if engine in ll:
            return engine
    return None


def _generate_shades(hex_color: str, n: int):
    """Generate n visually distinct shades from a base hex color.

    We produce lighter variants by interpolating the base color towards
    white. The first shade is the base color and subsequent shades are
    progressively lighter.
    """
    if n <= 0:
        return []
    try:
        base_rgb = mcolors.to_rgb(hex_color)
    except Exception:
        # fallback to a neutral gray
        base_rgb = (0.5, 0.5, 0.5)

    shades = []
    for i in range(n):
        # t from 0.0 (base) to 0.7 (much lighter) depending on index
        t = (i / max(1, n - 1)) * 0.7 if n > 1 else 0.0
        mixed = tuple((1 - t) * c + t * 1.0 for c in base_rgb)  # interpolate to white
        shades.append(mcolors.to_hex(mixed))
    return shades


def get_colors_for_labels(labels):
    """Return a list of colors aligned with `labels`.

    Behavior:
      - If a label contains a known engine name, use that engine's base color.
      - If multiple labels map to the same engine, generate different shades
        for that engine so same-engine series are visually related but
        distinguishable.
      - If an engine cannot be detected, fall back to matplotlib's default
        color cycle.
    """
    # Map engine -> indices in labels
    engine_indices = {}
    fallback_indices = []
    for idx, label in enumerate(labels):
        engine = _find_engine_in_label(str(label))
        if engine:
            engine_indices.setdefault(engine, []).append(idx)
        else:
            fallback_indices.append(idx)

    colors = [None] * len(labels)

    # Assign shades for each engine group
    for engine, idxs in engine_indices.items():
        base = ENGINE_BASE_COLORS.get(engine, '#7f7f7f')
        shades = _generate_shades(base, len(idxs))
        for i, idx in enumerate(idxs):
            colors[idx] = shades[i]

    # Fill fallback indices using matplotlib default cycle
    default_cycle = list(mcolors.TABLEAU_COLORS.values()) + list(mcolors.CSS4_COLORS.values())
    # keep a small, deterministic selection from the default_cycle
    fallback_cycle = [list(mcolors.TABLEAU_COLORS.values())[i % len(mcolors.TABLEAU_COLORS)] for i in range(max(1, len(fallback_indices)))]
    for i, idx in enumerate(fallback_indices):
        # fallback_cycle entries are color names; ensure hex via to_hex
        try:
            colors[idx] = mcolors.to_hex(fallback_cycle[i % len(fallback_cycle)])
        except Exception:
            colors[idx] = '#7f7f7f'

    # For any remaining None (shouldn't happen), put a neutral gray
    for i, c in enumerate(colors):
        if c is None:
            colors[i] = '#7f7f7f'

    return colors


def plot_bar_chart(params : PlotParams):

    x = np.arange(len(params.values))
    fig, ax = plt.subplots(figsize=params.figsize)

    ax.bar(x, params.values, color=params.colors, linewidth=1)

    # y axis label and title
    ax.set_ylabel(params.ylabel)
    ax.set_title(params.title)
    ax.set_xticks(x)
    ax.set_xticklabels(params.labels, rotation=params.rotation, ha="right")

    # add grid
    ax.grid(True, alpha=0.3, axis="y")

    # add annotation
    if params.annotate:
        for i, v in enumerate(params.values):
            ax.text(
                i,
                v + (max(params.values) * 0.01),  # slightly move above the bar
                f"{v:.2f}",
                ha="center",
                va="bottom",
                fontsize=9,
            )

    plt.tight_layout()

    # save or show
    if params.output_path:
        output_path = Path(params.output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(output_path, dpi=160)
        print(f"✓ Saved: {output_path.name}")
    else:
        plt.show()
    plt.close()


def load_summary_data(file_path):
    """
    Load benchmark summary data from JSON file.
    
    Args:
        file_path (Path): Path to summary.json file
        
    Returns:
        dict: Parsed JSON data
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Summary file not found: {path}")

    try:
        with open(path, 'r') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        # Provide a clearer error for invalid JSON content
        raise ValueError(f"Invalid JSON in summary file {path}: {e}") from e
    except PermissionError as e:
        raise PermissionError(f"Permission denied reading summary file {path}: {e}") from e
    except Exception as e:
        # Catch-all to convert unexpected IO errors into a runtime error with context
        raise RuntimeError(f"Unexpected error reading summary file {path}: {e}") from e


def compare_specific_results(title : str , data_list, output_dir : Path):
    """
    Compare specific (group_id, engine) combinations and generate visualization.
    
    Args:
        data (dict): Summary data
        comparisons (list): List of tuples [(group_id, engine), ...]
        output_dir (Path): Output directory path
        
    Example:
        comparisons = [('Q1', 'duckdb'), ('Q1', 'sqlite'), ('Q2', 'duckdb')]
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    labels = []
    for data in data_list:
        labels.append(data['key'])
    
    # Extract metrics for comparison
    exec_times = []
    memory_usage = []
    cpu_avg = []
    cpu_peak = []
    throughput = []
    
    for data in data_list:
        metrics = data['data']
        exec_times.append(metrics['execution_time']['avg'])
        memory_usage.append(metrics['peak_memory_bytes']['avg'] / (1024 * 1024))
        cpu_avg.append(metrics['cpu_avg_percent']['avg'])
        cpu_peak.append(metrics['cpu_peak_percent']['avg'])
        
        time_sec = metrics['execution_time']['avg']
        rows = metrics['output_rows']
        throughput.append(rows / time_sec if time_sec > 0 else 0)
    
    # Create comparison visualization
    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    x = np.arange(len(data_list))
    bar_colors = get_colors_for_labels(labels)
    
    # 1. Execution Time
    ax = axes[0, 0]
    ax.bar(x, exec_times, color=bar_colors, edgecolor='black', linewidth=1)
    ax.set_ylabel('Time (seconds)')
    ax.set_title('Execution Time')
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha='right')
    ax.grid(True, alpha=0.3, axis='y')
    
    # 2. Memory Usage
    ax = axes[0, 1]
    ax.bar(x, memory_usage, color=bar_colors, edgecolor='black', linewidth=1)
    ax.set_ylabel('Memory (MB)')
    ax.set_title('Peak Memory Usage')
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha='right')
    ax.grid(True, alpha=0.3, axis='y')
    
    # 3. CPU Average
    ax = axes[0, 2]
    ax.bar(x, cpu_avg, color=bar_colors, edgecolor='black', linewidth=1)
    ax.set_ylabel('CPU (%)')
    ax.set_title('Average CPU Usage')
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha='right')
    ax.grid(True, alpha=0.3, axis='y')
    
    # 4. CPU Peak
    ax = axes[1, 0]
    ax.bar(x, cpu_peak, color=bar_colors, edgecolor='black', linewidth=1)
    ax.set_ylabel('CPU (%)')
    ax.set_title('Peak CPU Usage')
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha='right')
    ax.grid(True, alpha=0.3, axis='y')
    
    # 5. Throughput
    ax = axes[1, 1]
    ax.bar(x, throughput, color=bar_colors, edgecolor='black', linewidth=1)
    ax.set_ylabel('Rows/sec')
    ax.set_title('Throughput')
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha='right')
    ax.grid(True, alpha=0.3, axis='y')
    
    # 6. Summary Table
    ax = axes[1, 2]
    ax.axis('off')
    table_data = []
    for i, _ in enumerate(data_list):
        table_data.append([
            labels[i],
            f"{exec_times[i]:.3f}s",
            f"{memory_usage[i]:.1f}MB",
            f"{throughput[i]:.0f}/s"
        ])
    
    table = ax.table(cellText=table_data,
                    colLabels=['Label', 'Time', 'Memory', 'Throughput'],
                    cellLoc='center',
                    loc='center',
                    bbox=[0, 0, 1, 1])
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1, 2)
    
    # Style header
    for i in range(4):
        table[(0, i)].set_facecolor('#40466e')
        table[(0, i)].set_text_props(weight='bold', color='white')

    fig.tight_layout(rect=[0, 0.03, 1, 0.94])
    fig.suptitle(title, y=0.99, fontsize=18)
    
    # Generate filename from comparisons
    comparison_name = "_VS_".join([f"{item['key']}" for item in data_list[:3]])
    if len(data_list) > 3:
        comparison_name += "_and_more"
    
    output_file = output_dir / f"{comparison_name}.png"
    plt.savefig(output_file, dpi=160)
    print(f"✓ Generated: {output_file.name}")
    plt.close()


def create_execution_time_comparison(data, compare_pairs, output_dir):
    """
    Create execution time comparison chart using compare_pairs.
    
    Args:
        data (dict): Summary data with structure {group_id: {engine: metrics}}
        compare_pairs (list): List of tuples [(group_id, engine), ...]
        output_dir (Path): Output directory path
    """
    if not compare_pairs:
        print("⚠️  No compare_pairs provided for execution time comparison")
        return
    
    # Extract data for the specified compare_pairs
    values = []
    labels = []
    
    for group_id, engine in compare_pairs:
        # Validate that this combination exists in data
        if group_id not in data or engine.value not in data[group_id]:
            print(f"⚠️  Warning: ({group_id}, {engine}) not found in data, skipping")
            continue
        
        # Get execution time
        exec_time = data[group_id][engine.value]['execution_time']['avg']
        values.append(exec_time)
        
        # Create label
        label = f"{group_id}_{engine}"
        labels.append(label)
        
    
    if not values:
        print("❌ No valid data found for execution time comparison")
        return
    
    colors = get_colors_for_labels(labels)
    
    # Construct PlotParams
    params = PlotParams(
        values=values,
        labels=labels,
        colors=colors,
        ylabel='Execution Time (seconds)',
        title='Average Execution Time Comparison',
        output_path=str(output_dir / "execution_time_comparison.png"),
        figsize=(12, 6),
        rotation=0,
        annotate=True
    )
    
    # Call plot_bar_chart
    plot_bar_chart(params)


def create_memory_usage_comparison(data, compare_pairs, output_dir):
    """
    Create memory usage comparison chart using compare_pairs.
    
    Args:
        data (dict): Summary data with structure {group_id: {engine: metrics}}
        compare_pairs (list): List of tuples [(group_id, engine), ...]
        output_dir (Path): Output directory path
    """
    if not compare_pairs:
        print("⚠️  No compare_pairs provided for memory usage comparison")
        return
    
    # Extract data for the specified compare_pairs
    values = []
    labels = []
    
    for group_id, engine in compare_pairs:
        # Validate that this combination exists in data
        if group_id not in data or engine.value not in data[group_id]:
            print(f"⚠️  Warning: ({group_id}, {engine}) not found in data, skipping")
            continue
        
        # Get peak memory and convert to MB
        memory_mb = data[group_id][engine.value]['peak_memory_bytes']['avg'] / (1024 * 1024)
        values.append(memory_mb)
        
        # Create label
        label = f"{group_id}_{engine}"
        labels.append(label)
        
    if not values:
        print("❌ No valid data found for memory usage comparison")
        return
    
    colors = get_colors_for_labels(labels)
    # Construct PlotParams
    params = PlotParams(
        values=values,
        labels=labels,
        colors=colors,
        ylabel='Peak Memory (MB)',
        title='Peak Memory Usage Comparison',
        output_path=str(output_dir / "memory_usage_comparison.png"),
        figsize=(12, 6),
        rotation=0,
        annotate=True
    )
    
    # Call plot_bar_chart
    plot_bar_chart(params)


def create_cpu_usage_comparison(data, compare_pairs, output_dir):
    """
    Create CPU usage comparison chart using compare_pairs (Peak and Average).
    
    Args:
        data (dict): Summary data with structure {group_id: {engine: metrics}}
        compare_pairs (list): List of tuples [(group_id, engine), ...]
        output_dir (Path): Output directory path
    """
    if not compare_pairs:
        print("⚠️  No compare_pairs provided for CPU usage comparison")
        return
    
    # Extract data for the specified compare_pairs
    peak_values = []
    avg_values = []
    labels = []
    
    for group_id, engine in compare_pairs:
        # Validate that this combination exists in data
        if group_id not in data or engine.value not in data[group_id]:
            print(f"⚠️  Warning: ({group_id}, {engine}) not found in data, skipping")
            continue
        
        # Get CPU peak and average
        cpu_peak = data[group_id][engine.value]['cpu_peak_percent']['avg']
        cpu_avg = data[group_id][engine.value]['cpu_avg_percent']['avg']
        peak_values.append(cpu_peak)
        avg_values.append(cpu_avg)
        
        # Create label
        label = f"{group_id}_{engine}"
        labels.append(label)
        
    if not peak_values:
        print("❌ No valid data found for CPU usage comparison")
        return
    
    colors = get_colors_for_labels(labels)
    # Create Peak CPU chart
    params_peak = PlotParams(
        values=peak_values,
        labels=labels,
        colors=colors,
        ylabel='CPU Peak (%)',
        title='Peak CPU Usage Comparison',
        output_path=str(output_dir / "cpu_peak_comparison.png"),
        figsize=(12, 6),
        rotation=0,
        annotate=True
    )
    plot_bar_chart(params_peak)
    
    # Create Average CPU chart
    params_avg = PlotParams(
        values=avg_values,
        labels=labels,
        colors=colors,
        ylabel='CPU Average (%)',
        title='Average CPU Usage Comparison',
        output_path=str(output_dir / "cpu_avg_comparison.png"),
        figsize=(12, 6),
        rotation=0,
        annotate=True
    )
    plot_bar_chart(params_avg)


def create_throughput_comparison(data, compare_pairs, output_dir):
    """
    Create throughput (rows/sec) comparison chart using compare_pairs.
    
    Args:
        data (dict): Summary data with structure {group_id: {engine: metrics}}
        compare_pairs (list): List of tuples [(group_id, engine), ...]
        output_dir (Path): Output directory path
    """
    if not compare_pairs:
        print("⚠️  No compare_pairs provided for throughput comparison")
        return
    
    # Extract data for the specified compare_pairs
    values = []
    labels = []
    
    for group_id, engine in compare_pairs:
        # Validate that this combination exists in data
        if group_id not in data or engine.value not in data[group_id]:
            print(f"⚠️  Warning: ({group_id}, {engine}) not found in data, skipping")
            continue
        
        # Calculate throughput
        rows = data[group_id][engine.value]['output_rows']
        time_sec = data[group_id][engine.value]['execution_time']['avg']
        throughput = rows / time_sec if time_sec > 0 else 0
        values.append(throughput)
        
        # Create label
        label = f"{group_id}_{engine}"
        labels.append(label)
        
    if not values:
        print("❌ No valid data found for throughput comparison")
        return
    
    colors = get_colors_for_labels(labels)
    # Construct PlotParams
    params = PlotParams(
        values=values,
        labels=labels,
        colors=colors,
        ylabel='Throughput (rows/sec)',
        title='Throughput Comparison',
        output_path=str(output_dir / "throughput_comparison.png"),
        figsize=(12, 6),
        rotation=0,
        annotate=True
    )
    
    # Call plot_bar_chart
    plot_bar_chart(params)


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

        colors_list = get_colors_for_labels(labels)

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


def create_comprehensive_dashboard(title, data_list, output_dir):
    """
    Create a comprehensive dashboard with multiple metrics.
    Simply calls compare_specific_results with all available combinations.
    
    Args:
        title (str): Title of the dashboard
        data_list (list): List of tuples [(group_id, engine), ...]
        output_dir (Path): Output directory path
    """
    if len(data_list) < 2:
        print(f"Skipping comprehensive \"{title}\" dashboard: not enough data to compare.")
        return

    # Just call compare_specific_results which already does comprehensive comparison
    compare_specific_results(title, data_list, output_dir)


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
                f.write(f"    CPU Peak: {metrics['cpu_peak_percent']['avg']:.2f}%\n")
                f.write(f"    CPU Average: {metrics['cpu_avg_percent']['avg']:.2f}%\n")
                f.write(f"    Output Rows: {metrics['output_rows']}\n")
                
                time_sec = metrics['execution_time']['avg']
                throughput = metrics['output_rows'] / time_sec if time_sec > 0 else 0
                f.write(f"    Throughput: {throughput:.2f} rows/sec\n")
            
            f.write("\n")
    
    print(f"✓ Generated: {output_file.name}")



def aggregate_by_group_default(data: Dict) -> Dict[str, List[dict]]:
    result = {}
    for group_id, engines in data.items():
        if group_id not in result:
            result[group_id] = []
        for engine, states in engines.items():
            if "default" in states:
                key = engine + "_ops_default"
                result[group_id].append({"key": key, "data": states["default"]})
    return result

def create_dashboard_by_group(data: Dict, output_dir: Path):
    aggregated_data = aggregate_by_group_default(data)
    for group_id, entries in aggregated_data.items():
        create_comprehensive_dashboard( f"Comparison by group {group_id}", entries, output_dir / group_id)


def aggregate_by_engine_default(data: Dict) -> Dict[str, List[dict]]:
    result = {}
    for group_id, engines in data.items():
        for engine, states in engines.items():
            if engine not in result:
                result[engine] = []
            if "default" in states:
                key = group_id + "_" + engine + "_ops_default"
                result[engine].append({"key": key, "data": states["default"]})
    return result


def create_dashboard_by_engine(data: Dict, output_dir: Path):
    aggregated_data = aggregate_by_engine_default(data)
    for engine, entries in aggregated_data.items():
        create_comprehensive_dashboard(f"Comparison by engine {engine}" , entries, output_dir / engine)


def aggregate_by_optimizer(data: Dict) -> Dict[str, List[dict]]:
    result = {}
    for group_id, engines in data.items():
        for engine, states in engines.items():
            for optimizer_state, metrics in states.items():
                group_key = f"{group_id}_{engine}"
                if group_key not in result:
                    result[group_key] = []
                key = f"{engine}_{optimizer_state}"
                result[group_key].append({"key": key, "data": metrics})
    return result


def create_dashboard_by_optimizer(data: Dict, output_dir: Path):
    aggregated_data = aggregate_by_optimizer(data)
    for group_key, entries in aggregated_data.items():
        create_comprehensive_dashboard(f"Comparison by optimizer {group_key}" , entries, output_dir / group_key)


def generate_custom_comparison(summary_file_path, comparisons, output_dir=None):
    """
    Public API for generating custom comparisons.
    
    Args:
        summary_file_path (str or Path): Path to summary.json
        comparisons (list): List of tuples [(group_id, engine), ...]
        output_dir (str or Path, optional): Output directory. Defaults to results/visual/
        
    Example:
        generate_custom_comparison(
            'results/summary.json',
            [('Q1', 'duckdb'), ('Q1', 'sqlite'), ('Q2', 'duckdb')],
            'results/visual/'
        )
    """
    summary_file = Path(summary_file_path)
    
    if output_dir is None:
        output_dir = summary_file.parent / "visual"
    else:
        output_dir = Path(output_dir)
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Loading data from {summary_file}...")
    try:
        data = load_summary_data(summary_file)
    except (FileNotFoundError, ValueError, PermissionError, RuntimeError) as e:
        print(f"❌ Failed to load summary data: {e}")
        return False

    print(f"Generating comparison for {len(comparisons)} combinations...")

    print(f"✅ Comparison saved to {output_dir.resolve()}")
    return True


def main():
    """Main function: load data and generate all visualizations."""
    args = parse_env_args("Analyze benchmark experiment results")
    config_path = Path(__file__).parent / "config_yaml"
    config = ConfigLoader(config_path, env=args.env)
    summary_file = Path(config.config_data.cwd) / "summary.json"
    output_dir = Path(config.config_data.cwd)  / "visual"
    
    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("Loading benchmark data...")
    try:
        data = load_summary_data(summary_file)
    except FileNotFoundError as e:
        print(f"❌ {e}")
        sys.exit(2)
    except ValueError as e:
        print(f"❌ {e}")
        sys.exit(3)
    except PermissionError as e:
        print(f"❌ {e}")
        sys.exit(4)
    except RuntimeError as e:
        print(f"❌ {e}")
        sys.exit(5)

    print(f"Loaded data for {len(data)} queries\n")
    
    print("Generating visualizations...")
    print("-" * 50)

    clean_path(str(output_dir.resolve()))
    create_dashboard_by_group(data, output_dir / "comparison_by_group")
    create_dashboard_by_engine(data, output_dir / "comparison_by_engine")
    create_dashboard_by_optimizer(data, output_dir / "comparison_by_optimizer")
    # create_execution_time_comparison(data, config.config_data.compare_pairs, output_dir)
    # create_memory_usage_comparison(data, config.config_data.compare_pairs, output_dir)
    # create_cpu_usage_comparison(data, config.config_data.compare_pairs, output_dir)
    # create_throughput_comparison(data, config.config_data.compare_pairs, output_dir)
    # create_performance_percentiles(data, output_dir)
    # create_comprehensive_dashboard(data, config.config_data.compare_pairs, output_dir)
    # create_performance_summary_table(data, output_dir)


if __name__ == "__main__":
    main()
