import argparse
import fastavro
import os

def view_avro(file_path):
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return

    print(f"Reading Avro file: {file_path}")

    with open(file_path, 'rb') as f:
        reader = fastavro.reader(f)
        #schema = reader.schema
        schema = reader.writer_schema
        print("\n Schema:")
        print(schema)

        print("\n First few rows:")
        for i, record in enumerate(reader):
            print(record)
            if i == 4:  # limit to 5 rows
                break

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="View info about an Avro file")
    parser.add_argument("--file_path", type=str, help="Path to the Avro file")

    args = parser.parse_args()
    view_avro(args.file_path)
