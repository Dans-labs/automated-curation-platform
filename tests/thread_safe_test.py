import threading
from src.acp.db.dbz import DatabaseManager

# Initialize the DatabaseManager
db_manager = DatabaseManager(
    db_dialect="sqlite",
    db_url="///test_database.db",  # Use a file-based SQLite database
    encryption_key="YourEncryptionKeyHere",  # Replace with your actual encryption key
)

# Ensure the database and tables are created
db_manager.create_db_and_tables()

# Function to perform database operations
def db_operations(thread_id):
    dataset_id = f"dataset_{thread_id}"
    owner_id = f"owner_{thread_id}"
    title = f"Thread {thread_id} Dataset"

    print("Insert a dataset")
    db_manager.create_initial_dataset_record(dataset_id, owner_id, title)

    print("Retrieve the dataset")
    dataset = db_manager.find_dataset_by_id(dataset_id)
    assert dataset is not None, f"Dataset {dataset_id} not found!"

    print("Update the dataset")
    dataset.title = f"Updated {title}"
    db_manager.update_dataset(dataset)

    print("Verify the update")
    updated_dataset = db_manager.find_dataset_by_id(dataset_id)
    assert updated_dataset.title == f"Updated {title}", f"Dataset {dataset_id} update failed!"

# Create and start multiple threads
threads = []
for i in range(25):  # Adjust the number of threads as needed
    thread = threading.Thread(target=db_operations, args=(i,))
    threads.append(thread)
    thread.start()

# Wait for all threads to complete
for thread in threads:
    thread.join()

print("Thread-safety test completed successfully.")