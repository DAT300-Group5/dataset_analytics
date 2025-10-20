#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <algorithm>
#include <cctype>
#include <clocale>
#include <cstdlib>
#include <cstdint>
#include <unistd.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <chdb.h>

// Only support Linux and macOS

// Helper function to get peak memory usage in MB
double getPeakMemoryMB() {
    struct rusage usage;
    getrusage(RUSAGE_SELF, &usage);
    // ru_maxrss is in bytes on macOS, kilobytes on Linux
#ifdef __APPLE__
    return usage.ru_maxrss / (1024.0 * 1024.0);  // Convert bytes to MB
#else
    return usage.ru_maxrss / 1024.0;  // Convert KB to MB
#endif
}

class ChDBConnection {
private:
    chdb_connection* conn;
    bool verbose;
    bool profile_memory;
    
public:
    ChDBConnection(
        const std::vector<std::string>& args,
        bool verbose = false,
        bool profile_memory = false
    ): conn(nullptr), verbose(verbose), profile_memory(profile_memory) {
        // Convert string vector to char* array
        std::vector<char*> argv;
        for (const auto& arg : args) {
            argv.push_back(const_cast<char*>(arg.c_str()));
        }
        
        conn = chdb_connect(argv.size(), argv.data());
        if (!conn) {
            throw std::runtime_error("Failed to connect to chDB");
        }
    }
    
    ~ChDBConnection() {
        if (conn) {
            chdb_close_conn(conn);
        }
    }
    
    std::string query(const std::string& sql, const std::string& format) {
        chdb_result* result = chdb_query(*conn, sql.c_str(), format.c_str());
        if (!result) {
            throw std::runtime_error("Query execution failed");
        }
        
        const char* error = chdb_result_error(result);
        if (error) {
            std::string error_msg(error);
            chdb_destroy_query_result(result);
            throw std::runtime_error("Query error: " + error_msg);
        }
        
        size_t len = chdb_result_length(result);
        const char* buf = chdb_result_buffer(result);
        std::string data = (len > 0 && buf) ? std::string(buf, len) : std::string();
        
        // Get peak memory IMMEDIATELY after query execution (before any post-processing)
        double mem_after = 0.0;
        if (profile_memory) {
            mem_after = getPeakMemoryMB();
        }
        
        // Get query statistics from chdb result (these are SQL execution stats)
        double elapsed = chdb_result_elapsed(result);
        uint64_t rows_read = chdb_result_rows_read(result);
        uint64_t bytes_read = chdb_result_bytes_read(result);
        
        // Output query statistics (only if verbose mode)
        if (verbose) {
            std::cout << "Query statistics:\n";
            std::cout << "  Elapsed: " << elapsed << " seconds\n";
            std::cout << "  Rows read: " << rows_read << "\n";
            std::cout << "  Bytes read: " << bytes_read << " bytes\n";
        }
        
        // Memory profiling (if enabled)
        if (profile_memory) {
            std::cout << "Peak memory: " << mem_after << " MB\n";
        }
        
        chdb_destroy_query_result(result);
        return data;
    }
};

void print_usage(const char* program_name) {
    std::cerr << "Usage: " << program_name << " <dbpath> [options]\n";
    std::cerr << "  <dbpath>       Database directory path\n";
    std::cerr << "  -csv, --csv    Output format: CSV (default with headers)\n";
    std::cerr << "  -v, --verbose  Show query statistics (elapsed time and so on)\n";
    std::cerr << "  -m, --memory   Show peak memory usage\n";
    std::cerr << "\nReads SQL from stdin\n";
    std::cerr << "\nExamples:\n";
    std::cerr << "  " << program_name << " /tmp/mydb < query.sql\n";
    std::cerr << "  " << program_name << " /tmp/mydb -m < query.sql\n";
    std::cerr << "  " << program_name << " /tmp/mydb -v -m < query.sql\n";
}

int main(int argc, char* argv[]) {
    // Set locale to fix macOS locale issues with chdb
    std::setlocale(LC_ALL, "C");
    setenv("LC_ALL", "C", 1);
    setenv("LANG", "C", 1);
    
    try {
        // Check arguments
        if (argc < 2) {
            print_usage(argv[0]);
            return 1;
        }
        
        std::string dbpath = argv[1];
        bool verbose = false;
        bool profile_memory = false;
        std::string output_format = "CSVWithNames";
        
        // Parse options
        for (int i = 2; i < argc; i++) {
            std::string arg = argv[i];
            if (arg == "-v" || arg == "--verbose") {
                verbose = true;
            } else if (arg == "-m" || arg == "--memory") {
                profile_memory = true;
            } else if (arg == "-h" || arg == "--help") {
                print_usage(argv[0]);
                return 0;
            } else if (arg == "-csv" || arg == "--csv") {
                output_format = "CSVWithNames";
            } else {
                std::cerr << "Unknown option: " << arg << "\n";
                print_usage(argv[0]);
                return 1;
            }
        }
        
        // Create connection with logging disabled
        std::vector<std::string> args = {
            "chdb",
            "--path", dbpath,
            "--logger.console", "0",
            "--logger.level", "none"
        };
        ChDBConnection db(args, verbose, profile_memory);
        
        // Read SQL from stdin
        std::stringstream sql_buffer;
        std::string line;
        while (std::getline(std::cin, line)) {
            sql_buffer << line << "\n";
        }
        
        std::string sql = sql_buffer.str();
        if (sql.empty()) {
            std::cerr << "Error: No SQL input provided\n";
            return 1;
        }
         
        // Execute query and output CSV with headers to stdout
        std::string result = db.query(sql, output_format);
        std::cout << result;
        std::cout.flush();
        
        return 0;
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
