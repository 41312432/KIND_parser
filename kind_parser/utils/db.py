import pymysql
import pymysql.cursors
from contextlib import contextmanager
import os
from dotenv import load_dotenv

load_dotenv()

config = {
    "host": os.environ.get("DB_HOST", "10.20.49.50"),
    "port": int(os.environ.get("DB_PORT", 13306)),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "database": os.environ.get("DB_DATABASE", "dbkind"),
    "cursorclass": pymysql.cursors.DictCursor
}

@contextmanager
def get_connection():
    
    connection = pymysql.connect(**config)
    try:
        yield connection
    finally:
        connection.close()

def execute(query):

    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            res = cursor.fetchall()

    return res

def get_status_target_list_query() -> str:
    query = f"""
        SELECT 
            psi.mcp_id AS id,
            psi.sale_start_date,
            mpi.pdf_filepath,
            psi.revision_date,
            psi.product_code
        FROM product_status_info psi
        INNER JOIN mcp_product_info mpi
            ON psi.mcp_id = mpi.id
            AND psi.sale_start_date = mpi.sale_start_date
        WHERE psi.status_code = 3
        """
    return query