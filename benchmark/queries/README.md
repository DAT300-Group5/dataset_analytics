# QUERIES README

Each folder corresponds to a use case.

TODO: ClickHouse equivalents, more usecases where filtering comes to light more?

PROBLEMS: SQLite limited functionalities, needed to modify queries and usecases

KEEP IN MIND: sql files that use TEMP tables consist of multiple queries because seperate queries are needed for storing intermediate results in temp tables. This refers to the files in the anomaly folder

REALIZATIONS:

- without saving CTE or temp tables, SQLite is terribly slow
- DuckDB is much faster than SQLite for queries in the folder
- DuckDB supports many functions SQLite doesn't (statistics and even basic math like power calculation, root, std...)

FOLDER: queries:

- tremor:
  - Q1
    - duckdb
    - sqlite
    - chdb
  - Q2 ...
  - Q3 ...
- anomaly:
  - Q1
  - Q2 ...
- trend
  - Q1
  - Q2 ...
- category
  - Q1 ...
  - Q2 ...
