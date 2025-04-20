# Step 1: Create a virtual environment (recommended)
import os
import subprocess
import time
import pandas as pd
import requests
import json


# Create and activate a virtual environment
def setup_environment():
    if not os.path.exists("trino_env"):
        subprocess.run(["python", "-m", "venv", "trino_env"])

    # Install required packages
    if os.name == 'nt':  # Windows
        pip_path = os.path.join("trino_env", "Scripts", "pip")
    else:  # Linux/Mac
        pip_path = os.path.join("trino_env", "bin", "pip")

    subprocess.run([pip_path, "install", "trino", "pandas", "requests"])


# Step 2: Download and set up Trino server
def download_trino():
    if not os.path.exists("trino-server"):
        print("Downloading Trino server...")
        # Download Trino server (adjust version as needed)
        subprocess.run(["wget", "https://repo1.maven.org/maven2/io/trino/trino-server/413/trino-server-413.tar.gz"])
        subprocess.run(["mkdir", "-p", "trino-server"])
        subprocess.run(["tar", "-xzf", "trino-server-413.tar.gz", "-C", "trino-server", "--strip-components=1"])
        subprocess.run(["rm", "trino-server-413.tar.gz"])

        # Create necessary directories for configuration
        subprocess.run(["mkdir", "-p", "trino-server/etc/catalog"])


# Step 3: Configure Trino server
def configure_trino(data_directory):
    # Create config.properties
    with open("trino-server/etc/config.properties", "w") as f:
        f.write("coordinator=true\n")
        f.write("node-scheduler.include-coordinator=true\n")
        f.write("http-server.http.port=8080\n")
        f.write("discovery.uri=http://localhost:8080\n")

    # Create jvm.config
    with open("trino-server/etc/jvm.config", "w") as f:
        f.write("-server\n")
        f.write("-Xmx4G\n")
        f.write("-XX:+UseG1GC\n")
        f.write("-XX:G1HeapRegionSize=32M\n")
        f.write("-XX:+UseGCOverheadLimit\n")
        f.write("-XX:+ExplicitGCInvokesConcurrent\n")
        f.write("-XX:+HeapDumpOnOutOfMemoryError\n")
        f.write("-XX:+ExitOnOutOfMemoryError\n")
        f.write("-Djdk.attach.allowAttachSelf=true\n")

    # Create node.properties
    with open("trino-server/etc/node.properties", "w") as f:
        f.write(f"node.environment=development\n")
        f.write(f"node.id=trino-local\n")
        f.write(f"node.data-dir={data_directory}\n")

    # Create catalog/hive.properties for Parquet files
    os.makedirs("trino-server/etc/catalog", exist_ok=True)
    with open("trino-server/etc/catalog/hive.properties", "w") as f:
        f.write("connector.name=hive\n")
        f.write("hive.metastore=file\n")
        f.write(f"hive.metastore.catalog.dir=file://{data_directory}/metastore\n")
        f.write("hive.non-managed-table-writes-enabled=true\n")
        f.write("hive.storage-format=PARQUET\n")
        f.write("hive.allow-drop-table=true\n")
        f.write("hive.allow-rename-table=true\n")
        f.write("hive.allow-add-column=true\n")
        f.write("hive.allow-drop-column=true\n")
        f.write("hive.allow-rename-column=true\n")


# Step 4: Start Trino server
def start_trino_server():
    print("Starting Trino server...")

    # Use nohup to run the server in the background
    with open("trino.log", "w") as log_file:
        process = subprocess.Popen(
            ["trino-server/bin/launcher", "run"],
            stdout=log_file,
            stderr=log_file,
            preexec_fn=os.setsid if os.name != 'nt' else None
        )

    # Wait for server to start
    print("Waiting for Trino server to start...")
    max_attempts = 30
    attempts = 0

    while attempts < max_attempts:
        try:
            response = requests.get("http://localhost:8080/v1/info")
            if response.status_code == 200:
                print("Trino server started successfully!")
                # Add additional wait time to ensure the server is fully initialized
                print("Waiting for full initialization (30 seconds)...")
                time.sleep(30)
                return True
        except requests.exceptions.ConnectionError:
            pass

        time.sleep(2)
        attempts += 1

    print("Failed to start Trino server within the timeout period")
    return False


# Step 5: Check if Trino server is ready for queries
def wait_for_trino_ready():
    max_attempts = 20
    attempts = 0

    print("Checking if Trino is ready for queries...")
    while attempts < max_attempts:
        try:
            # Try a simple query to verify server is ready
            import trino
            conn = trino.dbapi.connect(
                host="localhost",
                port=8080,
                user="trino",
                catalog="system",  # Use system catalog which is always available
                schema="runtime"
            )
            cursor = conn.cursor()

            try:
                cursor.execute("SELECT 1")
                cursor.fetchall()
                conn.close()
                print("Trino is ready to accept queries!")
                return True
            except trino.exceptions.TrinoQueryError as e:
                if "SERVER_STARTING_UP" in str(e):
                    print(f"Server still initializing... waiting (attempt {attempts + 1}/{max_attempts})")
                else:
                    print(f"Error: {e}")

            conn.close()
        except Exception as e:
            print(f"Connection error: {e}")

        time.sleep(5)
        attempts += 1

    print("Server startup timed out or has issues. Check trino.log for details.")
    return False


# Step 5: Create a schema for Parquet files
def create_schema_and_table(parquet_file_path, schema_name, table_name):
    # Get DataFrame schema to infer Parquet schema
    df = pd.read_parquet(parquet_file_path)

    # Connect to Trino
    import trino
    conn = trino.dbapi.connect(
        host="localhost",
        port=8080,
        user="trino",
        catalog="hive",
        schema="default"
    )
    cursor = conn.cursor()

    # Create schema if it doesn't exist
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS hive.{schema_name}")

    # Create a table definition based on the Parquet schema
    columns = []
    for col_name, dtype in df.dtypes.items():
        if pd.api.types.is_integer_dtype(dtype):
            col_type = "BIGINT"
        elif pd.api.types.is_float_dtype(dtype):
            col_type = "DOUBLE"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            col_type = "TIMESTAMP"
        else:
            col_type = "VARCHAR"
        columns.append(f"{col_name} {col_type}")

    # Create table
    columns_str = ", ".join(columns)
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        {columns_str}
    )
    WITH (
        format = 'PARQUET',
        external_location = 'file://{os.path.abspath(os.path.dirname(parquet_file_path))}'
    )
    """
    cursor.execute(create_table_query)

    # Close connection
    conn.close()

    print(f"Table {schema_name}.{table_name} created successfully!")


# Step 6: Query the data
def query_data(schema_name, table_name):
    import trino
    conn = trino.dbapi.connect(
        host="localhost",
        port=8080,
        user="trino",
        catalog="hive",
        schema=schema_name
    )
    cursor = conn.cursor()

    # Execute a test query
    cursor.execute(f"SELECT * FROM {table_name} LIMIT 10")
    rows = cursor.fetchall()

    # Get column names
    column_names = [desc[0] for desc in cursor.description]

    # Create a DataFrame
    results_df = pd.DataFrame(rows, columns=column_names)

    # Close connection
    conn.close()

    return results_df


# Main function to run everything
def setup_local_trino_for_parquet(parquet_file_path, schema_name="parquet_data", table_name="user_actions"):
    # Create data directory
    data_dir = os.path.abspath("./trino-data")
    os.makedirs(data_dir, exist_ok=True)

    # Set up environment
    setup_environment()

    # Download and configure Trino
    download_trino()
    configure_trino(data_dir)

    # Start Trino server
    if start_trino_server():
        # Wait for Trino to be fully ready
        if wait_for_trino_ready():
            # Create schema and table for Parquet data
            try:
                create_schema_and_table(parquet_file_path, schema_name, table_name)

                # Run a sample query
                print("\nSample query results:")
                results = query_data(schema_name, table_name)
                print(results)

                # Show an example of running a custom SQL query
                print("\nExample code to run your own SQL queries:")
                print("""
                import trino
                conn = trino.dbapi.connect(
                    host="localhost",
                    port=8080,
                    user="trino",
                    catalog="hive",
                    schema="parquet_data"
                )
                cursor = conn.cursor()

                # Run your SQL query
                cursor.execute("SELECT timestamp, user_id, action, COUNT(*) FROM user_actions GROUP BY 1, 2, 3 ORDER BY COUNT(*) DESC LIMIT 10")

                # Fetch and print results
                for row in cursor.fetchall():
                    print(row)

                # Close connection
                conn.close()
                """)
            except Exception as e:
                print(f"Error creating schema or querying data: {e}")
                print("Check the logs for more details.")


# Example usage
if __name__ == "__main__":
    # Replace with the path to your Parquet file
    parquet_path = "/home/cortica/2nd_degree/work_search_projects/spotify/data/output_tests/FULL_DATA_RUN1/format_converter/parquet/date=2024-01-01%2000%3A00%3A00.000000000/cfc2c0b79feb49e98ea03d019d6b3b7b-0.parquet"
    setup_local_trino_for_parquet(parquet_path)