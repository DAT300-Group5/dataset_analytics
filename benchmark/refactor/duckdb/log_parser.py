"""
DuckDB JSON Profiling Output Parser Module

This module provides functionality to parse DuckDB JSON profiling output
to extract performance metrics including timing, memory usage, and throughput.
"""

import json
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict


@dataclass
class TimingInfo:
    """Data class to store timing information from DuckDB profiling"""
    wall_time: Optional[float] = None  # seconds (execution time)


@dataclass
class MemoryInfo:
    """Data class to store memory statistics from DuckDB profiling"""
    memory_used: Optional[int] = None  # bytes


@dataclass
class QueryMetrics:
    """Complete metrics for a query execution"""
    query_number: Optional[int] = None
    query_description: Optional[str] = None
    timing: Optional[TimingInfo] = None
    memory: Optional[MemoryInfo] = None
    output_rows: Optional[int] = None  # Number of output rows


class DuckDBProfileParser:
    """Parser for DuckDB JSON profiling output"""
    
    def __init__(self, json_file_path: str):
        """
        Initialize the parser with a JSON profiling file path.
        
        Args:
            json_file_path: Path to the DuckDB profiling output JSON file
        """
        self.json_file_path = json_file_path
        self.content: Optional[Dict] = None
        self.queries: List[QueryMetrics] = []
        
    def read_json(self) -> Dict:
        """
        Read the JSON profiling file content.
        
        Returns:
            The parsed JSON content as a dictionary
            
        Raises:
            FileNotFoundError: If the JSON file doesn't exist
            json.JSONDecodeError: If the file is not valid JSON
            IOError: If there's an error reading the file
        """
        try:
            with open(self.json_file_path, 'r', encoding='utf-8') as f:
                self.content = json.load(f)
            return self.content
        except FileNotFoundError:
            raise FileNotFoundError(f"JSON file not found: {self.json_file_path}")
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(f"Invalid JSON format: {e}", e.doc, e.pos)
        except Exception as e:
            raise IOError(f"Error reading JSON file: {e}")
    
    def extract_timing_from_node(self, node: Dict) -> float:
        """
        Extract timing information from a profiling node.
        
        Args:
            node: A node in the profiling tree
            
        Returns:
            Execution time in seconds
        """
        # DuckDB uses 'latency' or 'operator_timing' for timing info
        if 'latency' in node:
            return float(node['latency'])
        elif 'operator_timing' in node:
            return float(node['operator_timing'])
        elif 'timing' in node:
            return float(node['timing'])
        elif 'cpu_time' in node:
            return float(node['cpu_time'])
        
        return 0.0
    
    def extract_memory_from_node(self, node: Dict) -> int:
        """
        Extract memory usage from a profiling node.
        
        Args:
            node: A node in the profiling tree
            
        Returns:
            Memory used in bytes
        """
        # DuckDB provides various memory metrics
        memory_fields = [
            'system_peak_buffer_memory',
            'peak_memory',
            'memory_usage',
            'result_set_size'
        ]
        
        for field in memory_fields:
            if field in node and node[field]:
                try:
                    return int(node[field])
                except (ValueError, TypeError):
                    pass
        
        return 0
    
    def extract_cardinality_from_node(self, node: Dict) -> int:
        """
        Extract output cardinality (row count) from a profiling node.
        
        Args:
            node: A node in the profiling tree
            
        Returns:
            Number of output rows
        """
        # DuckDB uses 'rows_returned' for output row count
        cardinality_fields = [
            'rows_returned',
            'cardinality',
            'operator_cardinality'
        ]
        
        for field in cardinality_fields:
            if field in node and node[field]:
                try:
                    return int(node[field])
                except (ValueError, TypeError):
                    pass
        
        return 0
    
    def parse_all(self) -> List[QueryMetrics]:
        """
        Parse all queries from the JSON profiling output.
        
        Returns:
            List of QueryMetrics objects
        """
        if self.content is None:
            self.read_json()
        
        self.queries = []
        
        # DuckDB profiling output structure varies, try to handle common formats
        if isinstance(self.content, dict):
            # Check if it's a single query profile
            if 'query' in self.content or 'children' in self.content:
                metrics = self._parse_single_query(self.content, 1)
                if metrics:
                    self.queries.append(metrics)
            # Check if it's a list of query profiles
            elif 'queries' in self.content and isinstance(self.content['queries'], list):
                for idx, query_data in enumerate(self.content['queries'], 1):
                    metrics = self._parse_single_query(query_data, idx)
                    if metrics:
                        self.queries.append(metrics)
        elif isinstance(self.content, list):
            # Handle list of query profiles directly
            for idx, query_data in enumerate(self.content, 1):
                metrics = self._parse_single_query(query_data, idx)
                if metrics:
                    self.queries.append(metrics)
        
        return self.queries
    
    def _parse_single_query(self, query_data: Dict, query_number: int) -> Optional[QueryMetrics]:
        """
        Parse a single query's profiling data.
        
        Args:
            query_data: Dictionary containing query profiling data
            query_number: Sequential query number
            
        Returns:
            QueryMetrics object or None
        """
        metrics = QueryMetrics()
        metrics.query_number = query_number
        
        # Extract query description if available
        if 'query_name' in query_data:
            metrics.query_description = query_data['query_name'][:100]
        elif 'query' in query_data and isinstance(query_data['query'], str):
            metrics.query_description = query_data['query'][:100]  # Truncate long queries
        
        # Extract timing information (DuckDB uses 'latency' field)
        timing = TimingInfo()
        wall_time = self.extract_timing_from_node(query_data)
        if wall_time > 0:
            timing.wall_time = wall_time
            metrics.timing = timing
        
        # Extract memory information
        memory = MemoryInfo()
        memory_bytes = self.extract_memory_from_node(query_data)
        if memory_bytes > 0:
            memory.memory_used = memory_bytes
            metrics.memory = memory
        
        # Extract output rows (cardinality)
        cardinality = self.extract_cardinality_from_node(query_data)
        if cardinality > 0:
            metrics.output_rows = cardinality
        
        return metrics
    
    def get_summary(self) -> Dict:
        """
        Generate a summary of all parsed queries.
        
        Returns:
            Dictionary with summary statistics
        """
        if not self.queries:
            return {
                "total_queries": 0,
                "total_wall_time": 0.0,
                "peak_memory_kb": 0,
                "total_output_rows": 0,
                "overall_throughput_rows_per_sec": 0.0
            }
        
        total_wall_time = 0.0
        peak_memory = 0
        total_output_rows = 0
        
        for query in self.queries:
            if query.timing and query.timing.wall_time:
                total_wall_time += query.timing.wall_time
            
            if query.memory and query.memory.memory_used:
                peak_memory = max(peak_memory, query.memory.memory_used)
            
            if query.output_rows:
                total_output_rows += query.output_rows
        
        # Calculate throughput
        overall_throughput = total_output_rows / total_wall_time if total_wall_time > 0 else 0
        
        # Calculate last query throughput
        last_query_throughput = 0.0
        if self.queries:
            last_query = self.queries[-1]
            if (last_query.timing and last_query.timing.wall_time and 
                last_query.timing.wall_time > 0 and last_query.output_rows):
                last_query_throughput = last_query.output_rows / last_query.timing.wall_time
        
        return {
            "total_queries": len(self.queries),
            "total_wall_time": round(total_wall_time, 4),
            "peak_memory_kb": round(peak_memory / 1024, 2) if peak_memory > 0 else 0,
            "total_output_rows": total_output_rows,
            "overall_throughput_rows_per_sec": round(overall_throughput, 2),
            "last_query_throughput_rows_per_sec": round(last_query_throughput, 2)
        }
    
    def export_to_dict(self) -> Dict:
        """
        Export all parsed data to a dictionary format suitable for JSON serialization.
        
        Returns:
            Dictionary containing all queries and summary
        """
        queries_dict = []
        for query in self.queries:
            query_dict = {
                "query_number": query.query_number,
                "query_description": query.query_description
            }
            
            if query.timing:
                query_dict["timing"] = asdict(query.timing)
            
            if query.memory:
                query_dict["memory"] = asdict(query.memory)
            
            if query.output_rows is not None:
                query_dict["output_rows"] = query.output_rows
            
            queries_dict.append(query_dict)
        
        return {
            "queries": queries_dict,
            "summary": self.get_summary()
        }


def parse_duckdb_profile(json_file_path: str) -> Dict:
    """
    Convenience function to parse a DuckDB profiling JSON file and return results.
    
    Args:
        json_file_path: Path to the JSON profiling file
        
    Returns:
        Dictionary with parsed queries and summary
    """
    parser = DuckDBProfileParser(json_file_path)
    parser.read_json()
    parser.parse_all()
    return parser.export_to_dict()
