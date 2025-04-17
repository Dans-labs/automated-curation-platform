import sqlite3
import re
from datetime import datetime


def extract_dataset_id_from_backup(backup_file):
    """
    Extract dataset ID from backup file comments.
    Looks for line like: "-- For dataset ID: your-dataset-id"
    """
    with open(backup_file, 'r') as f:
        for line in f:
            if line.startswith('-- For dataset ID:'):
                return line.split(':')[-1].strip()
    raise ValueError("Dataset ID not found in backup file")


# Improved statement splitting
def split_sql_statements(sql_script):
    insert_statements = []

    # Read the file and extract lines
    with open(sql_script, 'r') as file:
        for line in file:
            # Check if the line starts with "INSERT INTO dataset"
            if line.startswith("INSERT INTO"):
                insert_statements.append(line.strip())

    return insert_statements

def import_dataset_backup(db_path, backup_file, replace_existing=True):
    """
    Import a dataset backup into an SQLite database.

    Args:
        db_path (str): Path to SQLite database file
        backup_file (str): Path to the backup SQL file
        replace_existing (bool): Whether to replace existing records (True) or skip them (False)
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Read the backup file
    with open(backup_file, 'r') as f:
        sql_script = f.read()

    if replace_existing:
        try:
            dataset_id = extract_dataset_id_from_backup(backup_file)
            print(f"Preparing to replace dataset {dataset_id}...")

            # Disable foreign keys temporarily for clean deletion
            cursor.execute("PRAGMA foreign_keys = OFF")

            # Delete in reverse order of foreign key dependencies
            cursor.execute("DELETE FROM data_file WHERE dataset_id = ?", (dataset_id,))
            cursor.execute("DELETE FROM target_repo WHERE dataset_id = ?", (dataset_id,))
            cursor.execute("DELETE FROM dataset WHERE id = ?", (dataset_id,))

            conn.commit()
            cursor.execute("PRAGMA foreign_keys = ON")
        except Exception as e:
            cursor.execute("PRAGMA foreign_keys = ON")  # Ensure re-enable if error occurs
            conn.close()
            raise ValueError(f"Error preparing to replace dataset: {e}")

    # Remove comments and split into individual statements
    # In your import function, replace the splitting line with:
    statements = split_sql_statements(backup_file)

    # Execute each statement
    success_count = 0
    for stmt in statements:
        try:
            cursor.execute(stmt)
            success_count += 1
        except sqlite3.IntegrityError as e:
            if not replace_existing:
                print(f"Skipping existing record: {e}")
                conn.rollback()
            else:
                print(f"Unexpected integrity error despite replacement: {e}")
                conn.rollback()
                raise
        except Exception as e:
            print(f"Error executing statement: {stmt}\nError: {e}")
            conn.rollback()
            raise

    conn.commit()
    conn.close()
    print(f"Import completed successfully. {success_count} statements executed.")


# Example usage
if __name__ == "__main__":
    db_path = "/Users/akmi/surfdrive/WORK-2025/INFRA-DANS-LABS/automated-curation-platform/data/db/acp-ohsmart.db"  # Change to your DB path
    backup_file = "dataset_backup_398b1f9f-11f4-444c-bfdb-a06af7d7fa13.sql"  # Change to your backup file

    # To replace existing dataset:
    import_dataset_backup(db_path, backup_file, replace_existing=True)

    # Or to skip existing records:
    # import_dataset_backup(db_path, backup_file, replace_existing=False)