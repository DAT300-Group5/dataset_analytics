#!/usr/bin/env bash

set -e

# clean
echo "🧹 Cleaning temporary files and directories under $(pwd)..."

find . -type d -name "__pycache__" -print -exec rm -rf {} +

find . -type f -name "*_profiling_tmp.sql" -print -delete

find . -type f -name "*_validate_tmp.sql" -print -delete

echo "✅ Cleanup complete."

# Modify config.yaml to use chdb_cli from PATH
echo "🔧 Modifying config.yaml to use chdb_cli from PATH..."

sed -i 's|chdb: ./chdb_cli/chdb_cli|chdb: chdb_cli|' config_yaml/config.yaml

echo "✅ Updated config.yaml to use chdb_cli from PATH."


# prepare benchmark databases
echo "🛠️  Preparing benchmark databases..."

# conda init
# conda activate DAT300

rm -rf db_vs14/
rm -rf db_ba30/

NAME="vs14"
DB_NAME="db_${NAME}"
mkdir -p $DB_NAME
python create_db.py $NAME "./${DB_NAME}/${NAME}_data.sqlite" --engine sqlite
python create_db.py $NAME "./${DB_NAME}/${NAME}_data.duckdb" --engine duckdb
python create_db.py $NAME "./${DB_NAME}/${NAME}_data_chdb" --engine chdb

NAME="ba30"
DB_NAME="db_${NAME}"
mkdir -p $DB_NAME
python create_db.py $NAME "./${DB_NAME}/${NAME}_data.sqlite" --engine sqlite
python create_db.py $NAME "./${DB_NAME}/${NAME}_data.duckdb" --engine duckdb
python create_db.py $NAME "./${DB_NAME}/${NAME}_data_chdb" --engine chdb

echo "✅ Benchmark databases prepared."