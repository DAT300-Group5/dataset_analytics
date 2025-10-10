"""
SQLite Output Log Parser Module

This module provides functionality to parse SQLite output logs that contain
timing and statistics information.
"""

import re
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict


@dataclass
class TimingInfo:
    """Data class to store timing information from SQLite"""
    run_time: Optional[float] = None  # seconds
    user_time: Optional[float] = None  # seconds
    system_time: Optional[float] = None  # seconds


@dataclass
class MemoryInfo:
    """Data class to store memory statistics from SQLite"""
    memory_used: Optional[int] = None  # bytes
    heap_usage: Optional[int] = None  # bytes
    page_cache_hits: Optional[int] = None
    page_cache_misses: Optional[int] = None
    page_cache_size: Optional[int] = None


@dataclass
class QueryMetrics:
    """Complete metrics for a query execution"""
    query_number: Optional[int] = None
    query_description: Optional[str] = None
    timing: Optional[TimingInfo] = None
    memory: Optional[MemoryInfo] = None
    output_rows: Optional[int] = None  # Number of output rows


class SQLiteLogParser:
    """Parser for SQLite output logs with timing and stats enabled"""
    
    # Regex patterns for parsing
    # Matches: Run Time: real 0.000 user 0.000057 sys 0.000252
    TIMING_PATTERN = r'Run Time:\s+real\s+([\d.]+)\s+user\s+([\d.]+)\s+sys\s+([\d.]+)'
    # Matches: Memory Used: 749984 (max 1122720) bytes - captures the max value in parentheses
    MEMORY_PATTERN = r'Memory Used:\s+\d+\s+\(max\s+(\d+)\)'
    HEAP_PATTERN = r'Pager Heap Usage:\s+(\d+)\s+bytes'
    CACHE_HIT_PATTERN = r'Page cache hits:\s+(\d+)'
    CACHE_MISS_PATTERN = r'Page cache misses:\s+(\d+)'
    CACHE_SIZE_PATTERN = r'Page cache writes:\s+(\d+)'
    QUERY_MARKER_PATTERN = r'Query\s+(\d+):\s+(.+)'
    
    def __init__(self, log_file_path: str):
        """
        Initialize the parser with a log file path.
        
        Args:
            log_file_path: Path to the SQLite output log file
        """
        self.log_file_path = log_file_path
        self.content = ""
        self.queries: List[QueryMetrics] = []
        
    def read_log(self) -> str:
        """
        Read the log file content.
        
        Returns:
            The content of the log file
            
        Raises:
            FileNotFoundError: If the log file doesn't exist
            IOError: If there's an error reading the file
        """
        try:
            with open(self.log_file_path, 'r', encoding='utf-8') as f:
                self.content = f.read()
            return self.content
        except FileNotFoundError:
            raise FileNotFoundError(f"Log file not found: {self.log_file_path}")
        except Exception as e:
            raise IOError(f"Error reading log file: {e}")
    
    def parse_timing(self, text: str) -> Optional[TimingInfo]:
        """
        Parse timing information from text.
        Format: Run Time: real 0.000 user 0.000057 sys 0.000252
        
        Args:
            text: Text containing timing information
            
        Returns:
            TimingInfo object or None if no timing found
        """
        match = re.search(self.TIMING_PATTERN, text, re.IGNORECASE)
        if match:
            timing = TimingInfo()
            timing.run_time = float(match.group(1))
            timing.user_time = float(match.group(2))
            timing.system_time = float(match.group(3))
            return timing
            
        return None
    
    def parse_memory(self, text: str) -> Optional[MemoryInfo]:
        """
        Parse memory statistics from text.
        
        Args:
            text: Text containing memory statistics
            
        Returns:
            MemoryInfo object or None if no memory stats found
        """
        memory = MemoryInfo()
        found_any = False
        
        # Memory used
        match = re.search(self.MEMORY_PATTERN, text, re.IGNORECASE)
        if match:
            memory.memory_used = int(match.group(1))
            found_any = True
        
        # Heap usage
        match = re.search(self.HEAP_PATTERN, text, re.IGNORECASE)
        if match:
            memory.heap_usage = int(match.group(1))
            found_any = True
        
        # Page cache hits
        match = re.search(self.CACHE_HIT_PATTERN, text, re.IGNORECASE)
        if match:
            memory.page_cache_hits = int(match.group(1))
            found_any = True
        
        # Page cache misses
        match = re.search(self.CACHE_MISS_PATTERN, text, re.IGNORECASE)
        if match:
            memory.page_cache_misses = int(match.group(1))
            found_any = True
        
        # Page cache size
        match = re.search(self.CACHE_SIZE_PATTERN, text, re.IGNORECASE)
        if match:
            memory.page_cache_size = int(match.group(1))
            found_any = True
        
        return memory if found_any else None
    
    def parse_output_rows(self, section: str, next_section: str = "") -> int:
        """
        Count the number of output rows between query marker and next marker/stats.
        
        Args:
            section: Current section text
            next_section: Next section text (to find boundary)
            
        Returns:
            Number of output rows
        """
        # Find lines between query description and statistics/next query
        # Output lines typically contain | character (pipe-separated values)
        lines = section.split('\n')
        row_count = 0
        
        for line in lines:
            line = line.strip()
            # Skip empty lines, query markers, and statistics lines
            if not line:
                continue
            if line.startswith('Query '):
                continue
            if line.startswith('Memory Used:') or line.startswith('Run Time:'):
                break
            if line.startswith('Number of ') or line.startswith('Largest '):
                break
            if line.startswith('Lookaside ') or line.startswith('Pager '):
                break
            if line.startswith('Page cache') or line.startswith('Schema '):
                break
            if line.startswith('Statement ') or line.startswith('Fullscan '):
                break
            if line.startswith('Sort ') or line.startswith('Autoindex '):
                break
            if line.startswith('Virtual ') or line.startswith('Reprepare '):
                break
            if line.startswith('==='):
                break
            
            # Count lines that look like data (contain | or are single values)
            if '|' in line or re.match(r'^\d+$', line):
                row_count += 1
        
        return row_count
    
    def parse_all(self) -> List[QueryMetrics]:
        """
        Parse all timing and memory information from the log.
        
        Returns:
            List of QueryMetrics objects
        """
        if not self.content:
            self.read_log()
        
        # First, split by Query markers to identify queries
        query_pattern = r'(Query \d+:.*?)(?=Query \d+:|=== Demo Complete ===|$)'
        query_sections = re.findall(query_pattern, self.content, re.DOTALL)
        
        all_metrics = []
        query_index = 0
        
        # Process query sections first
        for query_section in query_sections:
            # Extract query number and description
            query_match = re.match(r'Query (\d+):\s+(.+)', query_section)
            if query_match:
                query_num = int(query_match.group(1))
                query_desc = query_match.group(2).strip()
                
                # Count output rows
                output_rows = self.parse_output_rows(query_section)
                
                # Find the corresponding timing section
                metrics = QueryMetrics()
                metrics.query_number = query_num
                metrics.query_description = query_desc
                metrics.output_rows = output_rows
                
                # Parse timing from this section
                timing = self.parse_timing(query_section)
                if timing:
                    metrics.timing = timing
                
                # Parse memory from this section
                memory = self.parse_memory(query_section)
                if memory:
                    metrics.memory = memory
                
                if metrics.timing or metrics.memory:
                    all_metrics.append(metrics)
                    query_index += 1
        
        # If no query markers found, treat the entire log as a single execution
        if not all_metrics:
            # Check if there's any timing information in the content
            timing = self.parse_timing(self.content)
            if timing:
                metrics = QueryMetrics()
                metrics.timing = timing
                
                # Parse memory from the entire content
                memory = self.parse_memory(self.content)
                if memory:
                    metrics.memory = memory
                
                # Count output rows (data lines before statistics)
                output_rows = self.parse_output_rows(self.content)
                if output_rows > 0:
                    metrics.output_rows = output_rows
                
                all_metrics.append(metrics)
        
        self.queries = all_metrics
        return all_metrics
    
    def get_summary(self) -> Dict:
        """
        Get a summary of all parsed metrics.
        
        Returns:
            Dictionary containing summary statistics
        """
        if not self.queries:
            self.parse_all()
        
        total_queries = len(self.queries)
        
        # Calculate timing statistics
        run_times = [q.timing.run_time for q in self.queries if q.timing and q.timing.run_time]
        user_times = [q.timing.user_time for q in self.queries if q.timing and q.timing.user_time]
        sys_times = [q.timing.system_time for q in self.queries if q.timing and q.timing.system_time]
        
        # Calculate memory statistics
        memory_used = [q.memory.memory_used for q in self.queries if q.memory and q.memory.memory_used]
        heap_usage = [q.memory.heap_usage for q in self.queries if q.memory and q.memory.heap_usage]
        
        # Calculate throughput statistics
        # Find queries with both timing and output_rows
        queries_with_data = [q for q in self.queries if q.timing and q.timing.run_time and q.output_rows]
        
        total_output_rows = sum(q.output_rows for q in queries_with_data)
        total_time = sum(run_times) if run_times else 0
        
        # Calculate throughput (rows per second)
        throughput = total_output_rows / total_time if total_time > 0 else 0
        
        # Find the last query with output (usually the most important one)
        last_query_with_output = None
        for q in reversed(self.queries):
            if q.output_rows and q.output_rows > 0 and q.timing and q.timing.run_time:
                last_query_with_output = q
                break
        
        # Calculate last query throughput
        last_query_throughput = 0
        last_query_rows = 0
        last_query_time = 0
        if last_query_with_output:
            last_query_rows = last_query_with_output.output_rows
            last_query_time = last_query_with_output.timing.run_time
            last_query_throughput = last_query_rows / last_query_time if last_query_time > 0 else 0
        
        summary = {
            'total_queries': total_queries,
            'timing': {
                'total_run_time': sum(run_times) if run_times else 0,
                'avg_run_time': sum(run_times) / len(run_times) if run_times else 0,
                'max_run_time': max(run_times) if run_times else 0,
                'min_run_time': min(run_times) if run_times else 0,
                'total_user_time': sum(user_times) if user_times else 0,
                'total_system_time': sum(sys_times) if sys_times else 0,
            },
            'memory': {
                'avg_memory_used': sum(memory_used) / len(memory_used) if memory_used else 0,
                'max_memory_used': max(memory_used) if memory_used else 0,
                'avg_heap_usage': sum(heap_usage) / len(heap_usage) if heap_usage else 0,
                'max_heap_usage': max(heap_usage) if heap_usage else 0,
            },
            'throughput': {
                'total_output_rows': total_output_rows,
                'overall_throughput_rows_per_sec': throughput,
                'last_query_rows': last_query_rows,
                'last_query_time': last_query_time,
                'last_query_throughput_rows_per_sec': last_query_throughput,
            }
        }
        
        return summary
    
    def export_to_dict(self) -> List[Dict]:
        """
        Export all metrics to a list of dictionaries.
        
        Returns:
            List of dictionaries containing all metrics
        """
        if not self.queries:
            self.parse_all()
        
        result = []
        for i, query in enumerate(self.queries, 1):
            data = {
                'query_id': i,
            }
            
            # Add query metadata
            if query.query_number:
                data['query_number'] = query.query_number
            if query.query_description:
                data['query_description'] = query.query_description
            if query.output_rows is not None:
                data['output_rows'] = query.output_rows
            
            if query.timing:
                data['run_time'] = query.timing.run_time
                data['user_time'] = query.timing.user_time
                data['system_time'] = query.timing.system_time
                
                # Calculate throughput for this query
                if query.output_rows and query.timing.run_time and query.timing.run_time > 0:
                    data['throughput_rows_per_sec'] = query.output_rows / query.timing.run_time
            
            if query.memory:
                data['memory_used'] = query.memory.memory_used
                data['heap_usage'] = query.memory.heap_usage
                data['page_cache_hits'] = query.memory.page_cache_hits
                data['page_cache_misses'] = query.memory.page_cache_misses
                data['page_cache_size'] = query.memory.page_cache_size
            
            result.append(data)
        
        return result


def parse_sqlite_log(log_file_path: str) -> Dict:
    """
    Convenience function to parse a SQLite log file and return summary.
    
    Args:
        log_file_path: Path to the SQLite output log file
        
    Returns:
        Dictionary containing parsed metrics and summary
    """
    parser = SQLiteLogParser(log_file_path)
    parser.read_log()
    metrics = parser.parse_all()
    summary = parser.get_summary()
    
    return {
        'summary': summary,
        'details': parser.export_to_dict()
    }
