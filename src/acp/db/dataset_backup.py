import sqlite3
from datetime import datetime


def backup_dataset_by_id(db_path, dataset_id, output_file):
    """
    Backup all rows related to a specific dataset ID.

    Args:
        db_path (str): Path to SQLite database file
        dataset_id (str): The dataset ID to backup
        output_file (str): Path to output SQL file
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # Access columns by name

    with open(output_file, 'w') as f:
        # Write header with timestamp
        f.write(f"-- Backup created at {datetime.now().isoformat()}\n")
        f.write(f"-- For dataset ID: {dataset_id}\n\n")

        # Backup dataset table
        f.write("-- Dataset record\n")
        cursor = conn.execute("SELECT * FROM dataset WHERE id = ?", (dataset_id,))
        dataset_row = cursor.fetchone()

        if not dataset_row:
            raise ValueError(f"No dataset found with ID: {dataset_id}")

        columns = dataset_row.keys()
        values = []
        for value in dataset_row:
            if value is None:
                values.append("NULL")
            elif isinstance(value, (int, float)):
                values.append(str(value))
            elif isinstance(value, str):
                values.append(f"'{value.replace("'", "''")}'")
            elif isinstance(value, datetime):
                values.append(f"'{value.isoformat()}'")
            else:
                values.append(f"'{str(value)}'")

        f.write(f"INSERT INTO dataset ({', '.join(columns)}) VALUES ({', '.join(values)});\n\n")

        # Backup target_repo records
        f.write("-- Target repository records\n")
        cursor = conn.execute("SELECT * FROM target_repo WHERE dataset_id = ?", (dataset_id,))
        for row in cursor:
            columns = row.keys()
            values = []
            for value in row:
                if value is None:
                    values.append("NULL")
                elif isinstance(value, (int, float)):
                    values.append(str(value))
                elif isinstance(value, str):
                    values.append(f"'{value.replace("'", "''")}'")
                elif isinstance(value, datetime):
                    values.append(f"'{value.isoformat()}'")
                else:
                    values.append(f"'{str(value)}'")

            f.write(f"INSERT INTO target_repo ({', '.join(columns)}) VALUES ({', '.join(values)});\n")
        f.write("\n")

        # Backup data_file records
        f.write("-- Data file records\n")
        cursor = conn.execute("SELECT * FROM data_file WHERE dataset_id = ?", (dataset_id,))
        for row in cursor:
            columns = row.keys()
            values = []
            for value in row:
                if value is None:
                    values.append("NULL")
                elif isinstance(value, (int, float)):
                    values.append(str(value))
                elif isinstance(value, str):
                    values.append(f"'{value.replace("'", "''")}'")
                elif isinstance(value, datetime):
                    values.append(f"'{value.isoformat()}'")
                else:
                    values.append(f"'{str(value)}'")

            f.write(f"INSERT INTO data_file ({', '.join(columns)}) VALUES ({', '.join(values)});\n")

    conn.close()
    print(f"Backup successfully created at {output_file}")


# Example usage
if __name__ == "__main__":
    db_path = "/data/db/acp-ohsmart.db"  # Change to your DB path
    dataset_id = "398b1f9f-11f4-444c-bfdb-a06af7d7fa13"  # Change to your dataset ID
    output_file = f"dataset_backup_{dataset_id}.sql"

    backup_dataset_by_id(db_path, dataset_id, output_file)