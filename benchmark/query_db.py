import sqlite3
import duckdb

# RUN QUERY ON SQLITE DB ================================
def run_query_sqlite(query, db_path=None):

    # CONNECT TO DATABASE
    con = sqlite3.connect(db_path)

    # EXECUTE QUERY
    result = con.execute(query).fetchall()
    
    # CLOSE CONNECTION
    con.close()

    # TODO: possibly returning the result of the query 
    return len(result)


# RUN QUERY ON DUCKDB DB ==============================

def run_query_duckdb(query, db_path=None, ):
    if db_path is None:
        db_path = './data_duckdb.db'
    
    # CONNECT TO DATABASE 
    con = duckdb.connect(db_path)

    # EXECTURE QUERY 
    result = con.execute(query).fetchall()

    # CLOSE CONNECTION 
    con.close()
    return len(result)