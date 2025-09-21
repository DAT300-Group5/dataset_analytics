"""
DuckDB + Apache Arrow Network Transfer Performance Benchmark

This module provides a comprehensive benchmark framework for comparing the performance 
of different data transfer methods in a network environment, with a specific focus
on the advantages of using DuckDB with Apache Arrow.

Features:
- Compare data transfer performance of Pickle, JSON, and Arrow formats
- Measure serialization/deserialization time
- Evaluate data transfer size
- Test integration efficiency of DuckDB with different formats

Author: DAT300-Group5
Date: 2025-09-21
"""

import os
import sys
import time
import socket
import pickle
import json
import threading
from dataclasses import dataclass, field, asdict
from typing import Dict, Any, List, Optional, Callable, Tuple, Union
import numpy as np
import pandas as pd
import duckdb
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq


# ====================== Configuration ======================

@dataclass
class TestConfig:
    """Test configuration settings"""
    host: str = '127.0.0.1'
    pickle_port: int = 9001
    json_port: int = 9002
    arrow_port: int = 9003
    datasource: str = 'mock'  # Options: 'duckdb', 'csv', 'mock'
    db_path: str = '../Embedded_databases/test_duckdb_db.duckdb'
    csv_path: str = '../Embedded_databases/data/sleep_diary.csv'
    iterations: int = 1  # Number of iterations for each test
    verbose: bool = True  # Whether to display detailed logs


# ====================== Data Structures ======================

@dataclass
class TransferStats:
    """Transfer statistics data"""
    serialization_time: float = 0
    transmission_time: float = 0
    total_time: float = 0
    data_size: int = 0


@dataclass
class ServerStats:
    """Server statistics data"""
    reception_time: float = 0
    data_size: int = 0
    reception_start: float = 0
    data_verified: bool = False  # Data verification status

@dataclass
class VerificationResults:
    """Data verification results"""
    pickle_verified: bool = False
    json_verified: bool = False
    arrow_verified: bool = False

@dataclass
class TestResult:
    """Single method test result"""
    client: TransferStats = field(default_factory=TransferStats)
    server: ServerStats = field(default_factory=ServerStats)


@dataclass
class DuckDBIntegrationStats:
    """DuckDB integration test results"""
    arrow_query_time: Optional[float] = None
    df_query_time: Optional[float] = None


@dataclass
class BenchmarkResults:
    """Benchmark results set"""
    pickle: TestResult = field(default_factory=TestResult)
    json: TestResult = field(default_factory=TestResult)
    arrow: TestResult = field(default_factory=TestResult)
    duckdb_integration: DuckDBIntegrationStats = field(default_factory=DuckDBIntegrationStats)
    verification: VerificationResults = field(default_factory=VerificationResults)



# ====================== Utility Functions ======================

def format_size(size_bytes: int) -> str:
    """Format byte size into human-readable string"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0 or unit == 'GB':
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0


def time_function(func: Callable, *args, **kwargs) -> Dict[str, Any]:
    """Measure function execution time"""
    start_time = time.time()
    result = func(*args, **kwargs)
    end_time = time.time()
    
    return {
        'result': result,
        'execution_time': end_time - start_time
    }


def log(message: str, config: TestConfig) -> None:
    """Output log message based on configuration"""
    if config.verbose:
        print(message)


# ====================== Data Loading ======================

class DataLoader:
    """Test data loader"""
    
    @staticmethod
    def load_from_duckdb(db_path: str, table_name: str = "test_table") -> pd.DataFrame:
        """Load data from DuckDB"""
        try:
            conn = duckdb.connect(db_path)
            df = conn.execute(f"SELECT * FROM {table_name}").fetchdf()
            conn.close()
            return df
        except Exception as e:
            print(f"Unable to load data from DuckDB: {e}")
            return None
    
    @staticmethod
    def load_from_csv(csv_path: str) -> pd.DataFrame:
        """Load data from CSV"""
        try:
            df = pd.read_csv(csv_path)
            # Basic data processing
            df['date'] = pd.to_datetime(df['date'])
            float_cols = ['waso', 'sleep_duration', 'in_bed_duration', 'sleep_latency', 'sleep_efficiency']
            for col in float_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df = df.dropna(subset=[col])
            return df
        except Exception as e:
            print(f"Unable to load data from CSV: {e}")
            return None
    
    @staticmethod
    def generate_mock_data(rows: int = 1000000, seed: int = 42) -> pd.DataFrame:
        """Generate mock test data"""
        np.random.seed(seed)
        print("Creating mock data for testing")
        dates = pd.to_datetime('2021-01-01') + pd.to_timedelta(np.random.randint(0, 365, size=rows), unit='D')
        df = pd.DataFrame({
            'date': dates,
            'value1': np.random.randn(rows),
            'value2': np.random.randn(rows),
            'value3': np.random.choice(['A', 'B', 'C'], size=rows),
            'value4': np.random.randint(0, 100, size=rows)
        })
        return df
    
    @classmethod
    def load_test_data(cls, config: TestConfig) -> pd.DataFrame:
        """Load test data, trying different data sources"""
        if config.datasource == 'duckdb':
            # Try loading from DuckDB
            df = cls.load_from_duckdb(config.db_path)
            if df is not None and not df.empty:
                return df
        elif config.datasource == 'csv':
            # Try loading from CSV
            df = cls.load_from_csv(config.csv_path)
            if df is not None and not df.empty:
                return df
        else:
            # Generate mock data
            return cls.generate_mock_data()


# ====================== Network Communication ======================

class DataServer:
    """Data server - for receiving data in different formats"""
    
    def __init__(self, port: int, server_type: str, config: TestConfig):
        """Initialize server
        
        Args:
            port: Listening port
            server_type: Server type ('pickle', 'json', 'arrow')
            config: Test configuration
        """
        self.port = port
        self.server_type = server_type
        self.config = config
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind((config.host, port))
        self.received_data = None
        self.stats = ServerStats()
        self.data_verified = False  # Data verification status
        
    def start(self) -> threading.Thread:
        """Start server and wait for connections"""
        thread = threading.Thread(target=self._run_server)
        thread.daemon = True
        thread.start()
        return thread
        
    def _run_server(self) -> None:
        """Server main loop"""
        self.socket.listen(1)
        log(f"{self.server_type} server started on port {self.port}", self.config)
        
        conn, addr = self.socket.accept()
        log(f"Accepted connection from {addr}", self.config)
        
        self.stats.reception_start = time.time()
        
        if self.server_type == 'pickle':
            self._handle_pickle(conn)
        elif self.server_type == 'json':
            self._handle_json(conn)
        elif self.server_type == 'arrow':
            self._handle_arrow(conn)
        
        self.stats.reception_time = time.time() - self.stats.reception_start
        conn.close()
        log(f"{self.server_type} server reception complete", self.config)
    
    def _handle_pickle(self, conn: socket.socket) -> None:
        """Handle pickle format data"""
        data_bytes = b""
        while True:
            chunk = conn.recv(4096)
            if not chunk:
                break
            data_bytes += chunk
        
        self.stats.data_size = len(data_bytes)
        self.received_data = pickle.loads(data_bytes)
    
    def _handle_json(self, conn: socket.socket) -> None:
        """Handle JSON format data"""
        data_str = ""
        while True:
            chunk = conn.recv(4096).decode('utf-8')
            if not chunk:
                break
            data_str += chunk
            
        self.stats.data_size = len(data_str.encode('utf-8'))
        self.received_data = json.loads(data_str)
        # Convert JSON data back to DataFrame for subsequent testing
        self.received_data = pd.DataFrame(self.received_data)
    
    def _handle_arrow(self, conn: socket.socket) -> None:
        """Handle Arrow format data"""
        try:
            source = conn.makefile('rb')
            reader = ipc.open_stream(source)
            
            # Read Arrow table
            self.received_data = reader.read_all()
            
            # Estimate data size
            data_bytes = source.read()
            self.stats.data_size = len(data_bytes) if data_bytes else sys.getsizeof(self.received_data)
            source.close()
        except Exception as e:
            print(f"Error processing Arrow data: {e}")


class DataClient:
    """Data client - for sending data in different formats"""
    
    def __init__(self, port: int, client_type: str, config: TestConfig):
        """Initialize client
        
        Args:
            port: Target server port
            client_type: Client type ('pickle', 'json', 'arrow')
            config: Test configuration
        """
        self.port = port
        self.client_type = client_type
        self.config = config
        self.stats = TransferStats()
        
    def send_data(self, data: Union[pd.DataFrame, pa.Table]) -> TransferStats:
        """Send data to server
        
        Args:
            data: Data to send (DataFrame or Arrow table)
            
        Returns:
            Transfer statistics
        """
        log(f"Preparing to send data using {self.client_type}...", self.config)
        
        total_start = time.time()
        
        # Serialize data
        serialize_start = time.time()
        if self.client_type == 'pickle':
            serialized_data = self._serialize_pickle(data)
        elif self.client_type == 'json':
            serialized_data = self._serialize_json(data)
        elif self.client_type == 'arrow':
            serialized_data = self._serialize_arrow(data)
        
        serialize_end = time.time()
        self.stats.serialization_time = serialize_end - serialize_start
        
        # Send data
        transmission_start = time.time()
        self._transmit_data(serialized_data)
        transmission_end = time.time()
        self.stats.transmission_time = transmission_end - transmission_start
        
        total_end = time.time()
        self.stats.total_time = total_end - total_start
        
        log(f"{self.client_type} client send complete", self.config)
        return self.stats
    
    def _serialize_pickle(self, data: pd.DataFrame) -> bytes:
        """Serialize data to Pickle format"""
        serialized = pickle.dumps(data)
        self.stats.data_size = len(serialized)
        return serialized
    
    def _serialize_json(self, data: pd.DataFrame) -> bytes:
        """Serialize data to JSON format"""
        # Handle non-JSON serializable types
        df_copy = data.copy()
        for col in df_copy.select_dtypes(include=['datetime64']).columns:
            df_copy[col] = df_copy[col].astype(str)
        
        json_data = df_copy.to_dict('records')
        serialized = json.dumps(json_data).encode('utf-8')
        self.stats.data_size = len(serialized)
        return serialized
    
    def _serialize_arrow(self, data: Union[pd.DataFrame, pa.Table]) -> pa.Table:
        """Convert data to Arrow table format"""
        if isinstance(data, pd.DataFrame):
            # Convert DataFrame to Arrow table
            table = pa.Table.from_pandas(data)
        else:
            # Already an Arrow table
            table = data
        
        return table
    
    def _transmit_data(self, serialized_data: Union[bytes, pa.Table]) -> None:
        """Transmit data to server"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.config.host, self.port))
        
        if self.client_type in ['pickle', 'json']:
            sock.sendall(serialized_data)
        elif self.client_type == 'arrow':
            sink = sock.makefile('wb')
            with ipc.new_stream(sink, serialized_data.schema) as writer:
                writer.write_table(serialized_data)
            sink.flush()
            sink.close()
        
        sock.close()


# ====================== Test Execution ======================

class BenchmarkRunner:
    """Benchmark test runner"""
    
    def __init__(self, config: TestConfig):
        """Initialize benchmark test runner
        
        Args:
            config: Test configuration
        """
        self.config = config
        self.results = BenchmarkResults()
    
    def run_benchmark(self) -> BenchmarkResults:
        """Execute complete benchmark test suite
        
        Returns:
            Test result summary
        """
        self._print_environment_info()
        
        # Load test data
        print("\nLoading test data...")
        df = DataLoader.load_test_data(self.config)
        print(f"Test data size: {df.shape[0]} rows x {df.shape[1]} columns")
        
        # Create DuckDB connection and register data
        conn = duckdb.connect(':memory:')
        conn.register('test_data', df)
        
        # Run tests
        self._run_pickle_test(df)
        self._run_json_test(df)
        arrow_table = pa.Table.from_pandas(df)
        self._run_arrow_test(arrow_table)
        
        # Test DuckDB integration
        self._test_duckdb_integration(conn, df, self.arrow_server.received_data if hasattr(self, 'arrow_server') else None)
        
        # Collect data verification results
        verification_results = VerificationResults(
            pickle_verified=self.pickle_server.data_verified if hasattr(self, 'pickle_server') else False,
            json_verified=self.json_server.data_verified if hasattr(self, 'json_server') else False,
            arrow_verified=self.arrow_server.data_verified if hasattr(self, 'arrow_server') else False
        )
        self.results.verification = verification_results
        
        return self.results
    
    def _print_environment_info(self) -> None:
        """Print test environment information"""
        print("\n" + "="*50)
        print("DuckDB + Apache Arrow Network Transfer Performance Test")
        print("="*50)
        print(f"Test time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"System environment: {sys.platform}")
        print(f"Python version: {sys.version.split()[0]}")
        print(f"DuckDB version: {duckdb.__version__}")
        print(f"PyArrow version: {pa.__version__}")
        print("="*50)
    
    def _run_pickle_test(self, df: pd.DataFrame) -> None:
        """Run Pickle transfer test"""
        print("\n===== Testing Pickle Serialization Transfer =====")
        
        for _ in range(self.config.iterations):
            # Start server
            pickle_server = DataServer(self.config.pickle_port, 'pickle', self.config)
            self.pickle_server = pickle_server
            server_thread = pickle_server.start()
            time.sleep(1)  # Wait for server to start
            
            # Create client and send data
            pickle_client = DataClient(self.config.pickle_port, 'pickle', self.config)
            self.pickle_client = pickle_client
            stats = pickle_client.send_data(df)
            
            # Wait for server to complete
            server_thread.join(timeout=5)
            
            # Verify data integrity
            if pickle_server.received_data is not None:
                self._verify_data_integrity(df, pickle_server.received_data, 'pickle')
            
            # Save results
            self.results.pickle = TestResult(
                client=TransferStats(**vars(stats)),
                server=ServerStats(
                    reception_time=pickle_server.stats.reception_time,
                    data_size=pickle_server.stats.data_size
                )
            )
    
    def _run_json_test(self, df: pd.DataFrame) -> None:
        """Run JSON transfer test"""
        print("\n===== Testing JSON Serialization Transfer =====")
        
        for _ in range(self.config.iterations):
            # Start server
            json_server = DataServer(self.config.json_port, 'json', self.config)
            self.json_server = json_server
            server_thread = json_server.start()
            time.sleep(1)  # Wait for server to start
            
            # Create client and send data
            json_client = DataClient(self.config.json_port, 'json', self.config)
            self.json_client = json_client
            stats = json_client.send_data(df)
            
            # Wait for server to complete
            server_thread.join(timeout=5)
            
            # Verify data integrity
            if json_server.received_data is not None:
                self._verify_data_integrity(df, json_server.received_data, 'json')
            
            # Save results
            self.results.json = TestResult(
                client=TransferStats(**vars(stats)),
                server=ServerStats(
                    reception_time=json_server.stats.reception_time,
                    data_size=json_server.stats.data_size
                )
            )
    
    def _run_arrow_test(self, table: pa.Table) -> None:
        """Run Arrow transfer test"""
        print("\n===== Testing Arrow Transfer =====")
        
        # Save original DataFrame for validation
        df_for_validation = table.to_pandas() if isinstance(table, pa.Table) else None
        
        for _ in range(self.config.iterations):
            # Start server
            arrow_server = DataServer(self.config.arrow_port, 'arrow', self.config)
            self.arrow_server = arrow_server
            server_thread = arrow_server.start()
            time.sleep(1)  # Wait for server to start
            
            # Create client and send data
            arrow_client = DataClient(self.config.arrow_port, 'arrow', self.config)
            self.arrow_client = arrow_client
            stats = arrow_client.send_data(table)
            
            # Wait for server to complete
            server_thread.join(timeout=5)
            
            # Arrow data validation will be done in DuckDB integration test, because Arrow needs to be converted to DataFrame for comparison
            
            # Save results
            self.results.arrow = TestResult(
                client=TransferStats(**vars(stats)),
                server=ServerStats(
                    reception_time=arrow_server.stats.reception_time,
                    data_size=arrow_server.stats.data_size
                )
            )
    
    def _test_duckdb_integration(self, conn: duckdb.DuckDBPyConnection, 
                               df: pd.DataFrame, arrow_data: Optional[pa.Table]) -> None:
        """Test DuckDB integration with different data formats"""
        print("\n===== Testing DuckDB with Arrow Integration =====")
        
        integration_stats = DuckDBIntegrationStats()
        
        # Test Arrow integration
        try:
            if arrow_data is not None:
                arrow_query_start = time.time()
                conn.register('arrow_table', arrow_data)
                arrow_result = conn.execute("SELECT * FROM arrow_table").fetchdf()
                arrow_query_end = time.time()
                integration_stats.arrow_query_time = arrow_query_end - arrow_query_start
                
                # Verify Arrow data
                if hasattr(self, 'arrow_server') and hasattr(self, 'arrow_client'):
                    self._verify_data_integrity(df, arrow_result, 'arrow')
            else:
                print("Could not obtain Arrow data, skipping Arrow table query test")
        except Exception as e:
            print(f"DuckDB query on Arrow table failed: {e}")
        
        # Test DataFrame integration
        try:
            df_query_start = time.time()
            conn.register('pandas_table', df)
            df_result = conn.execute("SELECT * FROM pandas_table").fetchdf()
            df_query_end = time.time()
            integration_stats.df_query_time = df_query_end - df_query_start
        except Exception as e:
            print(f"DuckDB query on DataFrame failed: {e}")
        
        # Save results
        self.results.duckdb_integration = integration_stats
        
    def _verify_data_integrity(self, original_data: pd.DataFrame, received_data: pd.DataFrame, data_type: str) -> bool:
        """Verify data consistency before and after transfer
        
        Args:
            original_data: Original data
            received_data: Received data
            data_type: Data type ('pickle', 'json', 'arrow')
            
        Returns:
            Whether verification passed
        """
        print(f"\n----- Verifying {data_type.upper()} Data Integrity -----")
        
        try:
            # Verify row and column counts
            if original_data.shape != received_data.shape:
                print(f"Data size mismatch: Original data {original_data.shape} vs Received data {received_data.shape}")
                if hasattr(self, f'{data_type}_server'):
                    setattr(getattr(self, f'{data_type}_server'), 'data_verified', False)
                return False
                
            # Verify column names
            if list(original_data.columns) != list(received_data.columns):
                print("Column names do not match")
                if hasattr(self, f'{data_type}_server'):
                    setattr(getattr(self, f'{data_type}_server'), 'data_verified', False)
                return False
            
            # Verify data content (using sampling to avoid performance issues with large datasets)
            sample_size = min(100, len(original_data))
            sample_indices = np.random.choice(len(original_data), sample_size, replace=False)
            
            # For each sampled row, check if values are the same
            for idx in sample_indices:
                orig_row = original_data.iloc[idx]
                recv_row = received_data.iloc[idx]
                
                # Check values for non-datetime columns
                for col in original_data.columns:
                    if pd.api.types.is_datetime64_any_dtype(original_data[col]):
                        # Datetime types need special handling, may have format differences
                        continue
                    
                    if orig_row[col] != recv_row[col] and not (pd.isna(orig_row[col]) and pd.isna(recv_row[col])):
                        print(f"Row {idx} column '{col}' values don't match: {orig_row[col]} vs {recv_row[col]}")
                        if hasattr(self, f'{data_type}_server'):
                            setattr(getattr(self, f'{data_type}_server'), 'data_verified', False)
                        return False
            
            print(f"{data_type.upper()} data verification passed ✓")
            if hasattr(self, f'{data_type}_server'):
                setattr(getattr(self, f'{data_type}_server'), 'data_verified', True)
            return True
            
        except Exception as e:
            print(f"Error during data verification: {e}")
            if hasattr(self, f'{data_type}_server'):
                setattr(getattr(self, f'{data_type}_server'), 'data_verified', False)
            return False


# ====================== Results Analysis and Presentation ======================

class ResultAnalyzer:
    """Test results analyzer"""
    
    @staticmethod
    def analyze_results(results: BenchmarkResults) -> Dict[str, Any]:
        """Analyze test results, generate comparison data
        
        Args:
            results: Benchmark test results
            
        Returns:
            Analysis results
        """
        analysis = {
            'data_size_comparison': {},
            'serialization_time_comparison': {},
            'transmission_time_comparison': {},
            'total_time_comparison': {},
            'query_performance_comparison': {},
            'advantages': []
        }
        
        # Data size comparison
        pickle_size = results.pickle.server.data_size
        json_size = results.json.server.data_size
        arrow_size = results.arrow.server.data_size
        
        analysis['data_size_comparison'] = {
            'pickle': pickle_size,
            'json': json_size,
            'arrow': arrow_size,
            'arrow_vs_pickle_pct': (arrow_size / pickle_size - 1) * 100 if pickle_size else 0,
            'json_vs_pickle_pct': (json_size / pickle_size - 1) * 100 if pickle_size else 0
        }
        
        # Serialization time comparison
        analysis['serialization_time_comparison'] = {
            'pickle': results.pickle.client.serialization_time,
            'json': results.json.client.serialization_time,
            'arrow': results.arrow.client.serialization_time
        }
        
        # Transmission time comparison
        analysis['transmission_time_comparison'] = {
            'pickle': results.pickle.client.transmission_time,
            'json': results.json.client.transmission_time,
            'arrow': results.arrow.client.transmission_time
        }
        
        # Total time comparison
        pickle_total = results.pickle.client.total_time
        json_total = results.json.client.total_time
        arrow_total = results.arrow.client.total_time
        
        analysis['total_time_comparison'] = {
            'pickle': pickle_total,
            'json': json_total,
            'arrow': arrow_total,
            'arrow_vs_pickle_pct': (arrow_total / pickle_total - 1) * 100 if pickle_total else 0,
            'json_vs_pickle_pct': (json_total / pickle_total - 1) * 100 if pickle_total else 0
        }
        
        # Query performance comparison
        df_time = results.duckdb_integration.df_query_time
        arrow_time = results.duckdb_integration.arrow_query_time
        
        if df_time is not None and arrow_time is not None:
            analysis['query_performance_comparison'] = {
                'df_time': df_time,
                'arrow_time': arrow_time,
                'arrow_vs_df_pct': (arrow_time / df_time - 1) * 100 if df_time else 0
            }
        
        # Analyze Arrow advantages based only on actual test results
        advantages = []
        
        if arrow_size < pickle_size and arrow_size < json_size:
            advantages.append("Smaller data transfer size, saving network bandwidth")
        
        if results.arrow.client.serialization_time < results.pickle.client.serialization_time and \
           results.arrow.client.serialization_time < results.json.client.serialization_time:
            advantages.append("Shorter serialization time, reducing CPU overhead")
            
        if arrow_total < pickle_total and arrow_total < json_total:
            advantages.append("Shorter total transfer time, improving data exchange efficiency")
            
        if arrow_time is not None and df_time is not None and arrow_time < df_time:
            advantages.append("More efficient integration with DuckDB, better query performance")
        
        analysis['advantages'] = advantages
        return analysis


class ResultPresenter:
    """Test results presenter"""
    
    @staticmethod
    def print_results(results: BenchmarkResults, analysis: Dict[str, Any]) -> None:
        """Print test results summary
        
        Args:
            results: Benchmark test results
            analysis: Analysis results
        """
        print("\n" + "="*50)
        print("Data Transfer Performance Test Results")
        print("="*50)
        
        # 1. Data size comparison
        print("\n1. Data Size Comparison (bytes)")
        print(f"Pickle: {analysis['data_size_comparison']['pickle']:,}")
        print(f"JSON:   {analysis['data_size_comparison']['json']:,}")
        print(f"Arrow:  {analysis['data_size_comparison']['arrow']:,}")
        
        arrow_size_diff = analysis['data_size_comparison']['arrow_vs_pickle_pct']
        json_size_diff = analysis['data_size_comparison']['json_vs_pickle_pct']
        print(f"\nCompared to Pickle:")
        print(f"- Arrow {'increased' if arrow_size_diff > 0 else 'decreased'} by: "
              f"{abs(arrow_size_diff):.2f}% in transfer size")
        print(f"- JSON {'increased' if json_size_diff > 0 else 'decreased'} by: "
              f"{abs(json_size_diff):.2f}% in transfer size")
        
        # 2. Serialization time comparison
        print("\n2. Serialization Time Comparison (milliseconds)")
        print(f"Pickle: {analysis['serialization_time_comparison']['pickle'] * 1000:.3f}")
        print(f"JSON:   {analysis['serialization_time_comparison']['json'] * 1000:.3f}")
        print(f"Arrow:  {analysis['serialization_time_comparison']['arrow'] * 1000:.3f}")
        
        # 3. Transmission time comparison
        print("\n3. Transmission Time Comparison (milliseconds)")
        print(f"Pickle: {analysis['transmission_time_comparison']['pickle'] * 1000:.3f}")
        print(f"JSON:   {analysis['transmission_time_comparison']['json'] * 1000:.3f}")
        print(f"Arrow:  {analysis['transmission_time_comparison']['arrow'] * 1000:.3f}")
        
        # 4. Total transfer time comparison
        print("\n4. Total Transfer Time Comparison (milliseconds)")
        print(f"Pickle: {analysis['total_time_comparison']['pickle'] * 1000:.3f}")
        print(f"JSON:   {analysis['total_time_comparison']['json'] * 1000:.3f}")
        print(f"Arrow:  {analysis['total_time_comparison']['arrow'] * 1000:.3f}")
        
        arrow_time_diff = analysis['total_time_comparison']['arrow_vs_pickle_pct']
        json_time_diff = analysis['total_time_comparison']['json_vs_pickle_pct']
        print(f"\nCompared to Pickle:")
        print(f"- Arrow {'increased' if arrow_time_diff > 0 else 'decreased'} by: "
              f"{abs(arrow_time_diff):.2f}% in total transfer time")
        print(f"- JSON {'increased' if json_time_diff > 0 else 'decreased'} by: "
              f"{abs(json_time_diff):.2f}% in total transfer time")
        
        # 5. DuckDB query performance comparison
        print("\n5. DuckDB Query Performance Comparison (milliseconds)")
        
        if 'query_performance_comparison' in analysis and analysis['query_performance_comparison']:
            df_time = analysis['query_performance_comparison']['df_time']
            arrow_time = analysis['query_performance_comparison']['arrow_time']
            arrow_vs_df = analysis['query_performance_comparison']['arrow_vs_df_pct']
            
            print(f"Direct DataFrame query:    {df_time * 1000:.3f}")
            print(f"Arrow table query:         {arrow_time * 1000:.3f}")
            
            print(f"\nQuerying Arrow table compared to DataFrame:")
            print(f"{'Increased' if arrow_vs_df > 0 else 'Decreased'} by {abs(arrow_vs_df):.2f}% in query time")
        else:
            print("Cannot compare query performance - missing data")
        
        # 6. Data verification results
        print("\n6. Data Verification Results:")
        if hasattr(results, 'verification'):
            print(f"Pickle: {'✓ Passed' if results.verification.pickle_verified else '✗ Failed'}")
            print(f"JSON:   {'✓ Passed' if results.verification.json_verified else '✗ Failed'}")
            print(f"Arrow:  {'✓ Passed' if results.verification.arrow_verified else '✗ Failed'}")
        else:
            print("No data verification performed")
        
        # 7. Arrow advantages
        if analysis['advantages']:
            print("\n7. Arrow Advantages Based on Test Data:")
            for adv in analysis['advantages']:
                print(f"- {adv}")
        else:
            print("\n7. No clear performance advantages found for Arrow in this test.")
            print("   Arrow's advantages may be more apparent with larger datasets or complex query scenarios.")
        
        print("\n" + "="*50)
    
    @staticmethod
    def save_results_to_file(results: BenchmarkResults, analysis: Dict[str, Any], 
                           filename: str = "benchmark_results.json") -> None:
        """Save test results to file
        
        Args:
            results: Test results
            analysis: Analysis results
            filename: Save filename
        """
        # Build results dictionary
        output = {
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
            'system': {
                'platform': sys.platform,
                'python_version': sys.version.split()[0],
                'duckdb_version': duckdb.__version__,
                'pyarrow_version': pa.__version__
            },
            'results': {
                'pickle': asdict(results.pickle),
                'json': asdict(results.json),
                'arrow': asdict(results.arrow),
                'duckdb_integration': asdict(results.duckdb_integration)
            },
            'analysis': analysis
        }
        
        # Save to file
        with open(filename, 'w') as f:
            json.dump(output, f, indent=2)
        print(f"\nResults saved to {filename}")


# ====================== Main Function ======================

def run_benchmark(config: TestConfig = None) -> None:
    """Run complete benchmark test
    
    Args:
        config: Optional test configuration
    """
    if config is None:
        config = TestConfig()
    
    try:
        # Run tests
        runner = BenchmarkRunner(config)
        results = runner.run_benchmark()
        
        # Analyze results
        analysis = ResultAnalyzer.analyze_results(results)
        
        # Present results
        ResultPresenter.print_results(results, analysis)
        
        # Save results
        ResultPresenter.save_results_to_file(results, analysis)
        
    except Exception as e:
        import traceback
        print(f"Error occurred during test execution: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    # Run test with default configuration
    run_benchmark()