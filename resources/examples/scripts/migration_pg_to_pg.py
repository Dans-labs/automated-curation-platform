import psycopg2
from contextlib import closing

# Source PostgreSQL database connection parameters
SOURCE_DB_PARAMS = {
    'host': 'localhost',
    'database': 'acp_ohsmart',
    'user': 'username',  # Change this
    'password': 'password',  # Change this
    'port': '5432'
}

# Target PostgreSQL database connection parameters
TARGET_DB_PARAMS = {
    'host': 'localhost',
    'database': 'acp_ohsmart',
    'user': 'username',  # Change this
    'password': 'password',  # Change this
    'port': '5423'
}

def migrate_table(source_conn, target_conn, table_name):
    """Migrate data from a specific table."""
    with closing(source_conn.cursor()) as source_cur, closing(target_conn.cursor()) as target_cur:
        # Fetch all data from the source table
        source_cur.execute(f"SELECT * FROM {table_name}")
        rows = source_cur.fetchall()

        # Get column names
        col_names = [desc[0] for desc in source_cur.description]
        col_placeholders = ", ".join(["%s"] * len(col_names))
        col_names_str = ", ".join(col_names)

        # Insert data into the target table
        for row in rows:
            target_cur.execute(
                f"INSERT INTO {table_name} ({col_names_str}) VALUES ({col_placeholders})",
                row
            )

def main():
    # Connect to source and target databases
    source_conn = psycopg2.connect(**SOURCE_DB_PARAMS)
    target_conn = psycopg2.connect(**TARGET_DB_PARAMS)

    try:
        # Start a transaction in the target database
        target_conn.autocommit = False

        # List of tables to migrate
        tables_to_migrate = ['dataset', 'target_repo']  # Replace with your table names

        # Migrate each table
        for table in tables_to_migrate:
            migrate_table(source_conn, target_conn, table)

        # Commit the transaction if everything succeeded
        target_conn.commit()
        print("Migration completed successfully!")

    except Exception as e:
        # Rollback if any error occurs
        target_conn.rollback()
        print(f"Migration failed: {str(e)}")
        raise
    finally:
        # Close connections
        source_conn.close()
        target_conn.close()

if __name__ == "__main__":
    main()