import trino
import pandas as pd

# Connect to your Trino server
conn = trino.dbapi.connect(
    host="localhost",
    port=8080,
    user="trino",  # Default user for local setup
    catalog="hive",
    schema="parquet_data"  # The schema name you used when setting up
)

# Create a cursor
cursor = conn.cursor()

# Execute your SQL query
sql_query = """
SELECT
    episode_id,
    COUNT(*) AS like_count
FROM
    user_actions
WHERE
    action = 'like'
GROUP BY
    episode_id
ORDER BY
    like_count DESC
LIMIT 10

"""

cursor.execute(sql_query)

# Fetch and process results
rows = cursor.fetchall()
column_names = [desc[0] for desc in cursor.description]
results_df = pd.DataFrame(rows, columns=column_names)

# Display results
print(results_df)

# Close the connection
conn.close()