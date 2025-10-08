#!/usr/bin/env python3
"""
Script to convert temporal workflow runs from JSONL format to SQLite database.
Flattens nested JSON objects before storing in the database.

Usage:
    python -m scripts.temporal_workflow_runs_to_sqlite --prod prod.jsonl --staging staging.jsonl output.db
"""

import argparse
import hashlib
import json
import os
import re
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Tuple


def flatten_json(obj: Dict[str, Any], parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
    """
    Flatten a nested JSON object.
    
    Args:
        obj: The JSON object to flatten
        parent_key: The parent key for nested objects
        sep: Separator to use between nested keys
    
    Returns:
        Flattened dictionary
    """
    items = []
    
    for key, value in obj.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        
        if isinstance(value, dict):
            # Recursively flatten nested dictionaries
            items.extend(flatten_json(value, new_key, sep=sep).items())
        elif isinstance(value, list):
            # Convert lists to JSON strings for storage
            items.append((new_key, json.dumps(value)))
        else:
            items.append((new_key, value))
    
    return dict(items)


def get_columns_by_workflow_type(jsonl_files: List[Tuple[str, str]]) -> Dict[str, set]:
    """
    Scan JSONL files and collect column names per workflow type.

    Args:
        jsonl_files: List of tuples (file_path, environment)

    Returns:
        Mapping of workflow_type to discovered column names
    """
    columns_by_workflow_type: Dict[str, set] = defaultdict(set)

    for jsonl_file, environment in jsonl_files:
        with open(jsonl_file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue

                try:
                    json_obj = json.loads(line)
                    flattened = flatten_json(json_obj)
                    workflow_type = flattened.get('workflow_type') or 'unknown'
                    columns_by_workflow_type[workflow_type].update(flattened.keys())
                except json.JSONDecodeError as e:
                    print(f"Warning: Skipping invalid JSON in {jsonl_file} on line {line_num}: {e}")
                    continue

    for columns in columns_by_workflow_type.values():
        columns.add('workflow_type')
        columns.add('environment')

    return columns_by_workflow_type


def _sanitize_workflow_type(workflow_type: str) -> str:
    sanitized = re.sub(r'\W+', '_', workflow_type.strip().lower())
    sanitized = sanitized.strip('_') or 'unknown'
    if sanitized[0].isdigit():
        sanitized = f'_{sanitized}'
    return sanitized


def make_table_name(workflow_type: str, used_names: set) -> str:
    base = _sanitize_workflow_type(workflow_type)
    table_name = f'workflow_runs_{base}'

    if table_name not in used_names:
        used_names.add(table_name)
        return table_name

    suffix = hashlib.sha1(workflow_type.encode('utf-8')).hexdigest()[:6]
    candidate = f'{table_name}_{suffix}'
    while candidate in used_names:
        suffix = hashlib.sha1(f'{workflow_type}_{len(used_names)}'.encode('utf-8')).hexdigest()[:6]
        candidate = f'{table_name}_{suffix}'

    used_names.add(candidate)
    return candidate


def create_table_schema(table_name: str, columns: set) -> str:
    """
    Create a SQL CREATE TABLE statement with all columns as TEXT type.

    Args:
        table_name: Name of the SQLite table
        columns: Set of column names

    Returns:
        SQL CREATE TABLE statement
    """
    sorted_columns = sorted(columns)
    column_defs = [f'"{col}" TEXT' for col in sorted_columns]

    return (
        f'CREATE TABLE "{table_name}" (\n'
        f'    id INTEGER PRIMARY KEY AUTOINCREMENT,\n'
        f'    {", ".join(column_defs)}\n'
        f')'
    )


def insert_records(
    db_path: str,
    jsonl_files: List[Tuple[str, str]],
    columns_by_workflow_type: Dict[str, List[str]],
    table_name_by_workflow_type: Dict[str, str]
) -> Dict[str, int]:
    """
    Insert all records from JSONL files into the SQLite database.

    Args:
        db_path: Path to the SQLite database
        jsonl_files: List of tuples (file_path, environment)
        columns_by_workflow_type: Ordered column names per workflow type
        table_name_by_workflow_type: Mapping from workflow type to table name

    Returns:
        Number of records inserted per workflow type
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    prepared_statements: Dict[str, Tuple[str, List[str]]] = {}
    records_inserted: Dict[str, int] = defaultdict(int)
    total_inserted = 0

    for jsonl_file, environment in jsonl_files:
        print(f"Processing {environment} environment file: {jsonl_file}")

        with open(jsonl_file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue

                try:
                    json_obj = json.loads(line)
                    flattened = flatten_json(json_obj)
                    workflow_type = flattened.get('workflow_type') or 'unknown'
                    
                    if workflow_type not in columns_by_workflow_type:
                        print(
                            f"Warning: Skipping record with unknown workflow_type '{workflow_type}' "
                            f"in {jsonl_file} on line {line_num}"
                        )
                        continue

                    flattened['workflow_type'] = workflow_type
                    flattened['environment'] = environment

                    if workflow_type not in prepared_statements:
                        columns = columns_by_workflow_type[workflow_type]
                        placeholders = ', '.join(['?' for _ in columns])
                        column_names = ', '.join([f'"{col}"' for col in columns])
                        table_name = table_name_by_workflow_type[workflow_type]
                        insert_sql = (
                            f'INSERT INTO "{table_name}" ({column_names}) '
                            f'VALUES ({placeholders})'
                        )
                        prepared_statements[workflow_type] = (insert_sql, columns)

                    insert_sql, ordered_columns = prepared_statements[workflow_type]
                    values = [flattened.get(col) for col in ordered_columns]

                    cursor.execute(insert_sql, values)
                    records_inserted[workflow_type] += 1
                    total_inserted += 1

                    if total_inserted % 100 == 0:
                        print(f"Processed {total_inserted} records...")

                except json.JSONDecodeError as e:
                    print(f"Warning: Skipping invalid JSON in {jsonl_file} on line {line_num}: {e}")
                    continue
                except sqlite3.Error as e:
                    print(f"Database error in {jsonl_file} on line {line_num}: {e}")
                    continue

    conn.commit()
    conn.close()

    return dict(records_inserted)


def convert_jsonl_to_sqlite(
    jsonl_files: List[Tuple[str, str]],
    output_db_path: str
) -> Tuple[str, Dict[str, str]]:
    """
    Convert multiple JSONL files to a single SQLite database.
    
    Args:
        jsonl_files: List of tuples (file_path, environment)
        output_db_path: Path to the output SQLite database
    
    Returns:
        Tuple containing the database path and the table mapping
    """
    db_path = Path(output_db_path)
    
    print(f"Converting {len(jsonl_files)} JSONL files to {db_path}")
    for jsonl_file, env in jsonl_files:
        print(f"  - {env}: {jsonl_file}")
    
    # Remove existing database if it exists
    if db_path.exists():
        print(f"Removing existing database: {db_path}")
        db_path.unlink()
    
    # Step 1: Scan all files to get columns per workflow type
    print("Scanning files to determine schema...")
    columns_by_workflow_type = get_columns_by_workflow_type(jsonl_files)
    if not columns_by_workflow_type:
        raise ValueError("No workflow runs found in provided files")

    print(f"Found {len(columns_by_workflow_type)} workflow types")

    table_name_by_workflow_type: Dict[str, str] = {}
    ordered_columns_by_workflow_type: Dict[str, List[str]] = {}
    used_table_names: set = set()

    for workflow_type, columns in columns_by_workflow_type.items():
        table_name = make_table_name(workflow_type, used_table_names)
        table_name_by_workflow_type[workflow_type] = table_name
        ordered_columns_by_workflow_type[workflow_type] = sorted(columns)

    # Step 2: Create database and tables
    print("Creating database and tables...")
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    for workflow_type, table_name in table_name_by_workflow_type.items():
        create_table_sql = create_table_schema(table_name, columns_by_workflow_type[workflow_type])
        cursor.execute(create_table_sql)

    conn.commit()
    conn.close()

    # Step 3: Insert all records
    print("Inserting records...")
    records_by_workflow_type = insert_records(
        str(db_path),
        jsonl_files,
        ordered_columns_by_workflow_type,
        table_name_by_workflow_type
    )

    total_records = sum(records_by_workflow_type.values())
    print(f"Successfully converted {total_records} records to {db_path}")

    # Print some statistics
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    print("\nDatabase Statistics:")
    for workflow_type, table_name in table_name_by_workflow_type.items():
        cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
        table_records = cursor.fetchone()[0]
        cursor.execute(f'PRAGMA table_info("{table_name}")')
        columns_info = cursor.fetchall()
        print(
            f"  {workflow_type} -> table '{table_name}': {table_records} records, "
            f"{len(columns_info) - 1} columns"
        )

    conn.close()
    print(f"  Database size: {db_path.stat().st_size / 1024 / 1024:.2f} MB")

    return str(db_path), table_name_by_workflow_type


def main():
    """Main function to handle command line arguments and execute conversion."""
    parser = argparse.ArgumentParser(
        description="Convert temporal workflow runs from JSONL format to SQLite database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m scripts.temporal_workflow_runs_to_sqlite --prod prod.jsonl --staging staging.jsonl output.db
  python -m scripts.temporal_workflow_runs_to_sqlite --prod prod.jsonl staging.jsonl
        """
    )
    
    parser.add_argument('--prod',
                       help='Path to production JSONL file',
                       required=False)
    parser.add_argument('--staging',
                       help='Path to staging JSONL file',
                       required=False)
    parser.add_argument('output_db',
                       help='Path to output SQLite database file')
    
    args = parser.parse_args()
    
    # Validate that at least one environment file is provided
    if not args.prod and not args.staging:
        print("Error: At least one of --prod or --staging must be provided")
        sys.exit(1)
    
    # Build list of files to process
    jsonl_files = []
    
    if args.prod:
        if not os.path.exists(args.prod):
            print(f"Error: Production file '{args.prod}' does not exist")
            sys.exit(1)
        if not args.prod.endswith('.jsonl'):
            print(f"Warning: Production file '{args.prod}' does not have .jsonl extension")
        jsonl_files.append((args.prod, 'prod'))
    
    if args.staging:
        if not os.path.exists(args.staging):
            print(f"Error: Staging file '{args.staging}' does not exist")
            sys.exit(1)
        if not args.staging.endswith('.jsonl'):
            print(f"Warning: Staging file '{args.staging}' does not have .jsonl extension")
        jsonl_files.append((args.staging, 'staging'))
    
    try:
        db_path, table_name_by_workflow_type = convert_jsonl_to_sqlite(jsonl_files, args.output_db)
        print(f"\nConversion completed successfully!")
        print(f"SQLite database created: {db_path}")
        
        # Show sample queries
        print(f"\nSample queries to explore the data:")
        print(f"sqlite3 {db_path} \"SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;\"")

        if table_name_by_workflow_type:
            first_workflow_type, first_table = next(iter(table_name_by_workflow_type.items()))
            sample_query = (
                'SELECT environment, COUNT(*) as count '
                f'FROM "{first_table}" GROUP BY environment;'
            )
            escaped_query = sample_query.replace('"', '\\"')
            print(f"Example query for workflow_type '{first_workflow_type}':")
            print(f"sqlite3 {db_path} \"{escaped_query}\"")
        
    except Exception as e:
        print(f"Error during conversion: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
