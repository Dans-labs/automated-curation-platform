import sqlite3
import psycopg2
from contextlib import closing

# SQLite (OLD) database file path
OLD_SQLITE_DB_PATH = '/Users/akmi/Downloads/dans_packaging-prod14.db'

# PostgreSQL (NEW) database connection parameters
NEW_POSTGRESQL_DB_PARAMS = {
    'host': 'localhost',
    'database': 'acp_ohsmart',
    'user': 'myuser',  # Change this
    'password': 'mypassword',  # Change this
    'port': '5432'
}

def migrate_datasets(old_conn, new_conn):
    """Migrate dataset records with transformations"""
    with closing(old_conn.cursor()) as old_cur, closing(new_conn.cursor()) as new_cur:
        # Get all datasets from old SQLite database
        old_cur.execute("""
            SELECT id, title, owner_id, created_date, saved_date, submitted_date, 
                   md, release_version, state 
            FROM dataset
        """)

        for row in old_cur:
            (id_, title, owner_id, created_date, saved_date, submitted_date,
             md, release_version, state) = row

            # Metadata type is always JSON now
            metadata_type = 'JSON'

            # release_version migrates to status
            status = "SUBMITTED" if release_version == "PUBLISH" else "DRAFT"

            # state (READY/NOT READY) migrates to submission_ready (boolean)
            submission_ready = state == 'READY'  # This will evaluate to True/False

            # Insert into PostgreSQL database
            new_cur.execute("""
                INSERT INTO dataset (
                    id, title, owner_id, created_at, saved_at, submitted_at, 
                    metadata_content, metadata_type, status, submission_ready
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                id_, title, owner_id, created_date, saved_date, submitted_date,
                md, metadata_type, status, submission_ready
            ))


def migrate_target_repos(old_conn, new_conn):
    """Migrate target repository records with transformations"""
    with closing(old_conn.cursor()) as old_cur, closing(new_conn.cursor()) as new_cur:
        # Get all target repos from old SQLite database with dataset version
        old_cur.execute("""
            SELECT tr.id, tr.ds_id, tr.name, tr.display_name, tr.config, tr.url, 
                   tr.deposit_status, tr.deposit_time, tr.duration, tr.target_output,
                   d.release_version
            FROM target_repo tr
            JOIN dataset d ON tr.ds_id = d.id
        """)

        for row in old_cur:
            (id_, ds_id, name, display_name, config, url, deposit_status,
             deposit_time, duration, target_output, release_version) = row

            # Map deposit status
            status_map = {
                'COMPLETED': 'success',
                'FAILED': 'failed',
                'PENDING': 'in_progress',
                'FINISH' : 'FINISH',
                'ERROR': 'ERROR',
                'PROGRESS' : 'PROGRESS',
            }
            new_deposit_status = status_map.get(deposit_status)
            new_deposit_status = 'PREPARING' if new_deposit_status is None else new_deposit_status

            # Insert into PostgreSQL database
            new_cur.execute("""
                INSERT INTO target_repo (
                    id, dataset_id, name, display_name, configuration, url,
                    deposit_status, deposited_at, deposit_duration,
                    target_service_response, deposited_version, deposited_identifiers
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                id_, ds_id, name, display_name, config, url,
                new_deposit_status, deposit_time, duration,
                target_output, release_version, None  # deposited_identifiers set to None
            ))


def migrate_data_files(old_conn, new_conn):
    """Migrate data file records with transformations"""
    i = 0
    with closing(old_conn.cursor()) as old_cur, closing(new_conn.cursor()) as new_cur:
        # Get all data files from old SQLite database
        old_cur.execute("""
                    SELECT id, ds_id, name, path, size, mime_type, checksum_value, 
                           date_added, permissions, state
                    FROM data_file
                """)

        for row in old_cur:
            i+= 1
            print("i: ", i)

            (id_, ds_id, name, path, size, mime_type,
             checksum_value, date_added, permissions, state) = row

            # Insert into PostgreSQL database
            new_cur.execute("""
                        INSERT INTO data_file (id, dataset_id, name, path, size, mime_type,
                                               checksum, added_at, access_level, state)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                id_, ds_id, name, path, size, mime_type,
                checksum_value, date_added, permissions, state
            ))


def main():
    # Connect to SQLite (old) and PostgreSQL (new) databases
    old_conn = sqlite3.connect(OLD_SQLITE_DB_PATH)
    new_conn = psycopg2.connect(**NEW_POSTGRESQL_DB_PARAMS)

    try:
        # Start a transaction in PostgreSQL
        new_conn.autocommit = True

        # Migrate tables in order to respect foreign key constraints
        migrate_datasets(old_conn, new_conn)
        migrate_target_repos(old_conn, new_conn)
        migrate_data_files(old_conn, new_conn)

        # Commit the transaction if everything succeeded
        new_conn.commit()
        print("Migration completed successfully!")

    except Exception as e:
        # Rollback if any error occurs
        new_conn.rollback()
        print(f"Migration failed: {str(e)}")
        raise
    finally:
        # Close connections
        old_conn.close()
        new_conn.close()


if __name__ == "__main__":
    main()