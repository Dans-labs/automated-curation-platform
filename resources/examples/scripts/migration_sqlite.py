import json
import sqlite3
from contextlib import closing

# Database file paths
OLD_DB_PATH = 'path/to/old_database.db'
NEW_DB_PATH = 'path/to/new_database.db'

def migrate_datasets(old_conn, new_conn):
    """Migrate dataset records with transformations"""
    with closing(old_conn.cursor()) as old_cur, closing(new_conn.cursor()) as new_cur:
        # Get all datasets from old database
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

            # state (READY/NOT READY) migrates to submission_ready (1/0)
            submission_ready = 1 if state == 'READY' else 0

            # Insert into new database
            new_cur.execute("""
                INSERT INTO dataset (
                    id, title, owner_id, created_at, saved_at, submitted_at, 
                    metadata_content, metadata_type, status, submission_ready
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                id_, title, owner_id, created_date, saved_date, submitted_date,
                md, metadata_type, status, submission_ready
            ))


def migrate_target_repos(old_conn, new_conn):
    """Migrate target repository records with transformations"""
    with closing(old_conn.cursor()) as old_cur, closing(new_conn.cursor()) as new_cur:
        # Get all target repos from old database with dataset version
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

            deposited_identifiers = ""
            if target_output:
                tsr = json.loads(target_output)
                deposited_identifiers = json.dumps(tsr['response']["identifiers"])
                print(deposited_identifiers)

            # Insert into new database
            new_cur.execute("""
                INSERT INTO target_repo (
                    id, dataset_id, name, display_name, configuration, url, 
                    deposit_status, deposited_at, deposit_duration, 
                    target_service_response, deposited_version, deposited_identifiers
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                id_, ds_id, name, display_name, config, url,
                new_deposit_status, deposit_time, duration,
                target_output, release_version,  deposited_identifiers
            ))


def migrate_data_files(old_conn, new_conn):
    """Migrate data file records with transformations"""
    with closing(old_conn.cursor()) as old_cur, closing(new_conn.cursor()) as new_cur:
        # Get all data files from old database
        old_cur.execute("""
            SELECT id, ds_id, name, path, size, mime_type, 
                   checksum_value, date_added, permissions, state
            FROM data_file
        """)

        for row in old_cur:
            (id_, ds_id, name, path, size, mime_type,
             checksum_value, date_added, permissions, state) = row

            # Insert into new database
            new_cur.execute("""
                INSERT INTO data_file (
                    id, dataset_id, name, path, size, mime_type, 
                    checksum, added_at, access_level, state
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                id_, ds_id, name, path, size, mime_type,
                checksum_value, date_added, permissions, state
            ))


def main():
    # Connect to both databases
    with sqlite3.connect(OLD_DB_PATH) as old_conn, \
            sqlite3.connect(NEW_DB_PATH) as new_conn:
        # Enable foreign key constraints in new database
        new_conn.execute("PRAGMA foreign_keys = ON")

        # Begin transaction
        with new_conn:
            # Migrate tables in order to respect foreign key constraints
            migrate_datasets(old_conn, new_conn)
            migrate_target_repos(old_conn, new_conn)
            migrate_data_files(old_conn, new_conn)

        print("Migration completed successfully!")


if __name__ == "__main__":
    main()