import trino
import pandas as pd
import argparse
from io import StringIO


def execute_trino_query(host, port, user, catalog, schema, sql_query):
    # Connect to Trino server
    conn = trino.dbapi.connect(
        host=host,
        port=port,
        user=user,
        catalog=catalog,
        schema=schema
    )

    # Create a cursor
    cursor = conn.cursor()

    try:
        # Execute the SQL query
        cursor.execute(sql_query)

        # Fetch and process results
        rows = cursor.fetchall()
        if cursor.description:
            column_names = [desc[0] for desc in cursor.description]
            results_df = pd.DataFrame(rows, columns=column_names)
        else:
            results_df = pd.DataFrame()  # For queries that don't return results

        return results_df
    finally:
        # Close the connection
        conn.close()


def split_sql_queries(sql_content):
    """Split SQL content into individual queries, handling semicolons within strings/comments"""
    queries = []
    current_query = []
    in_string = False
    string_char = None
    in_comment = False

    for line in sql_content.split('\n'):
        line = line.strip()
        if not line:
            continue

        # Handle comments
        if line.startswith('--'):
            continue

        # Process each character in the line
        i = 0
        while i < len(line):
            char = line[i]

            # Handle string literals
            if char in ("'", '"') and not in_comment:
                if in_string and char == string_char:
                    in_string = False
                    string_char = None
                elif not in_string:
                    in_string = True
                    string_char = char

            # Handle comments
            elif char == '-' and i + 1 < len(line) and line[i + 1] == '-' and not in_string:
                in_comment = True
                break  # skip rest of line
            elif char == '*' and i + 1 < len(line) and line[i + 1] == '/' and in_string is False:
                in_comment = False
                i += 1  # skip next char
            elif char == '/' and i + 1 < len(line) and line[i + 1] == '*' and in_string is False:
                in_comment = True
                i += 1  # skip next char

            # Handle semicolons (query separators)
            elif char == ';' and not in_string and not in_comment:
                current_query.append(line[:i])
                full_query = ' '.join(current_query).strip()
                if full_query:  # Only add non-empty queries
                    queries.append(full_query)
                current_query = []
                line = line[i + 1:]
                i = 0
                continue

            i += 1

        if not in_comment:  # If we're not in a comment, add the remaining part of the line
            current_query.append(line)

    # Add the last query if there's anything left
    if current_query and not in_comment:
        full_query = ' '.join(current_query).strip()
        if full_query:
            queries.append(full_query)

    return queries


def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Execute queries on Trino database')

    # Connection parameters
    parser.add_argument('--host', default='localhost', help='Trino server host')
    parser.add_argument('--port', type=int, default=8080, help='Trino server port')
    parser.add_argument('--user', default='trino', help='Trino user')
    parser.add_argument('--catalog', default='hive', help='Trino catalog')
    parser.add_argument('--schema', default='parquet_data', help='Trino schema')

    # Query input options (either file or direct query)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--query', help='SQL query to execute')
    group.add_argument('--query-file', help='File containing SQL query to execute')

    # Output options
    parser.add_argument('--output', help='Output file to save results (CSV format)')

    args = parser.parse_args()

    # Read query from file if specified
    if args.query_file:
        with open(args.query_file, 'r') as f:
            sql_content = f.read()
        queries = split_sql_queries(sql_content)
    else:
        queries = [args.query]

    # Execute each query
    all_results = []
    for i, query in enumerate(queries, 1):
        print(f"\nExecuting Query {i}:\n{query}\n")
        try:
            results_df = execute_trino_query(
                host=args.host,
                port=args.port,
                user=args.user,
                catalog=args.catalog,
                schema=args.schema,
                sql_query=query
            )

            # Display results
            print(f"Results for Query {i}:")
            print(results_df)

            # Save to file if requested
            if args.output:
                output_file = f"{args.output}_{i}.csv" if len(queries) > 1 else args.output
                results_df.to_csv(output_file, index=False)
                print(f"Results saved to {output_file}")

            all_results.append(results_df)

        except Exception as e:
            print(f"Error executing Query {i}: {str(e)}")
            all_results.append(None)


if __name__ == '__main__':
    main()