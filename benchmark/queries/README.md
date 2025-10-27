# QUERIES README

Each folder corresponds to a use case. 3 USE CASES: tremor detection, anomaly detection and activity level categorization.

PROBLEMS: 

- SQLite limited functionalities, needed to modify queries and usecases
- SQLite does not have a optimizer turn off option
- ClickHouse does not support TEMP TABLES directly
- trend cannot be done for SQLite, deprecated 

REALIZATIONS:

- without saving CTE or temp tables, SQLite is terribly slow
- DuckDB and ClickHouse much faster than SQLite on average
- DuckDB supports many functions SQLite doesn't (statistics and even basic math like power calculation, root, std...)
- LIT does not have values as expected, 


FOLDER: queries:
- tremor:
  - Q1
    - duckdb
    - sqlite
    - chdb
  - Q2 ...
  - Q3 ...
 
- anomaly:
  - Q1 - subquery
  - Q2 - TEMP tables
  - Q3 - CTE
  - Q4 - less optimized subquery, joining on strings
  
- category
  - Q1 - filtering before the join in CTEs
  - Q2 - filtering mid join 
  - Q3 - filtering after the join
