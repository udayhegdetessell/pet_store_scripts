import sys
import os
from datetime import datetime

try:
    import oracledb
except ImportError:
    oracledb = None


def init_oracle_client():
    """Initialize Oracle client in thick mode"""
    try:
        # Set Oracle client library directory
        ORACLE_CLIENT_LIB_DIR = os.getenv('ORACLE_CLIENT_LIB_DIR',
                                          '/home/azureuser/pet_store_demo/oracle/instantclient_23_9')

        print(f"Initializing Oracle client in thick mode...")
        print(f"Using client library: {ORACLE_CLIENT_LIB_DIR}")

        # Initialize thick mode
        oracledb.init_oracle_client(lib_dir=ORACLE_CLIENT_LIB_DIR)
        print("✓ Oracle client initialized successfully in thick mode!")

    except Exception as e:
        print(f"⚠️  Warning: Could not initialize thick mode: {e}")
        print("Continuing with thin mode...")


def connect_to_oracle():
    """Connect to Oracle database"""
    try:
        # Database connection parameters
        username = input("Enter username: ")
        password = input("Enter password: ")
        host = input("Enter host (default: localhost): ") or "localhost"
        port = input("Enter port (default: 1521): ") or "1521"
        service_name = input("Enter service name (default: ORCL): ") or "ORCL"

        # Create connection string
        dsn = f"{host}:{port}/{service_name}"

        print(f"\nConnecting to Oracle database: {dsn}")
        connection = oracledb.connect(
            user=username,
            password=password,
            dsn=dsn
        )

        print("✓ Connected successfully!")
        return connection

    except oracledb.Error as e:
        print(f"✗ Connection failed: {e}")
        sys.exit(1)


def get_all_schemas(cursor):
    """Get all user schemas (excluding system schemas)"""
    try:
        query = """
                SELECT username
                FROM dba_users
                WHERE username NOT IN (
                                       'SYS', 'SYSTEM', 'DBSNMP', 'SYSMAN', 'OUTLN', 'MGMT_VIEW',
                                       'FLOWS_FILES', 'MDSYS', 'ORDSYS', 'EXFSYS', 'WMSYS', 'APPQOSSYS',
                                       'APEX_030200', 'APEX_PUBLIC_USER', 'FLOWS_030100', 'OWBSYS',
                                       'ORDDATA', 'CTXSYS', 'ANONYMOUS', 'XDB', 'ORDPLUGINS', 'OWBSYS_AUDIT',
                                       'SI_INFORMTN_SCHEMA', 'OLAPSYS', 'SCOTT', 'HR'
                    )
                  AND account_status = 'OPEN'
                ORDER BY username \
                """

        cursor.execute(query)
        schemas = [row[0] for row in cursor.fetchall()]
        return schemas

    except oracledb.Error:
        # If DBA_USERS is not accessible, try USER_USERS or ALL_USERS
        try:
            cursor.execute("SELECT user FROM dual")
            current_user = cursor.fetchone()[0]
            return [current_user]
        except oracledb.Error as e:
            print(f"Error getting schemas: {e}")
            return []


def get_tables_in_schema(cursor, schema):
    """Get all tables in a specific schema"""
    try:
        query = """
                SELECT table_name
                FROM all_tables
                WHERE owner = :schema
                ORDER BY table_name \
                """

        cursor.execute(query, schema=schema)
        tables = [row[0] for row in cursor.fetchall()]
        return tables

    except oracledb.Error as e:
        print(f"Error getting tables for schema {schema}: {e}")
        return []


def get_table_row_count(cursor, schema, table):
    """Get row count for a specific table"""
    try:
        # Use dynamic SQL to count rows
        query = f'SELECT COUNT(*) FROM "{schema}"."{table}"'
        cursor.execute(query)
        count = cursor.fetchone()[0]
        return count

    except oracledb.Error as e:
        print(f"    Error counting rows in {schema}.{table}: {e}")
        return "Error"


def format_number(num):
    """Format number with thousand separators"""
    if isinstance(num, int):
        return f"{num:,}"
    return str(num)


def main():
    print("=" * 60)
    print("Oracle Database Schema and Table Analysis")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Connect to database
    connection = connect_to_oracle()
    cursor = connection.cursor()

    try:
        # Get database info
        cursor.execute("SELECT banner FROM v$version WHERE rownum = 1")
        db_version = cursor.fetchone()[0]
        print(f"Database Version: {db_version}")
        print()

        # Get all schemas
        print("Fetching schemas...")
        schemas = get_all_schemas(cursor)

        if not schemas:
            print("No accessible schemas found.")
            return

        print(f"Found {len(schemas)} schema(s)")
        print()

        total_tables = 0
        total_rows = 0

        # Process each schema
        for schema in schemas:
            print(f"📁 SCHEMA: {schema}")
            print("-" * 50)

            # Get tables in this schema
            tables = get_tables_in_schema(cursor, schema)

            if not tables:
                print("    No tables found.")
                print()
                continue

            schema_total_rows = 0

            # Process each table
            for table in tables:
                row_count = get_table_row_count(cursor, schema, table)
                if isinstance(row_count, int):
                    schema_total_rows += row_count
                    total_rows += row_count

                print(f"    📋 {table:<30} | Rows: {format_number(row_count):>10}")

            total_tables += len(tables)
            print(f"    {'-' * 45}")
            print(f"    📊 Total tables in {schema}: {len(tables)}")
            print(f"    📊 Total rows in {schema}: {format_number(schema_total_rows)}")
            print()

        # Summary
        print("=" * 60)
        print("SUMMARY")
        print("=" * 60)
        print(f"📈 Total Schemas: {len(schemas)}")
        print(f"📈 Total Tables: {total_tables}")
        print(f"📈 Total Rows: {format_number(total_rows)}")
        print(f"🕐 Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    except oracledb.Error as e:
        print(f"Database error: {e}")

    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user.")

    finally:
        # Close connection
        cursor.close()
        connection.close()
        print("\n✓ Database connection closed.")


if __name__ == "__main__":
    # Check if oracledb is installed
    try:
        import oracledb
    except ImportError:
        print("Error: oracledb module not found.")
        print("Please install it using: pip install oracledb")
        sys.exit(1)

    # Initialize Oracle client in thick mode
    init_oracle_client()

    main()
