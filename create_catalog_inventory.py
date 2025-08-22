#!/usr/bin/env python3
"""
Catalog, Inventory, and Items Table Creator and Data Generator
Creates tables and populates them with fake data using Faker
"""

import argparse
import os
import random

from faker import Faker

try:
    import oracledb
except ImportError:
    oracledb = None

# Initialize Faker
fake = Faker()

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Create catalog, inventory, and items tables and populate with fake data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage with defaults
  python %(prog)s --host localhost --port 1521 --service ORCL --user master --password 12345678

  # Custom row counts
  python %(prog)s --host localhost --port 1521 --service ORCL --user master --password 12345678 \\
                   --catalog-rows 100 --inventory-rows 200 --items-rows 500

  # Using environment variables for sensitive data
  export DB_PASSWORD=mysecretpassword
  python %(prog)s --host localhost --port 1521 --service ORCL --user master
        """
    )

    # Database Connection Arguments
    db_group = parser.add_argument_group('Database Connection')
    db_group.add_argument('--host', '-H',
                          default=os.getenv('DB_HOST', 'localhost'),
                          help='Database host (default: localhost, env: DB_HOST)')
    db_group.add_argument('--port', '-P',
                          default=os.getenv('DB_PORT', '1521'),
                          help='Database port (default: 1521, env: DB_PORT)')
    db_group.add_argument('--service', '-s',
                          default=os.getenv('ORACLE_SERVICE_NAME', 'ORCL'),
                          help='Oracle service name or SID (default: ORCL, env: ORACLE_SERVICE_NAME)')
    db_group.add_argument('--user', '-u',
                          default=os.getenv('DB_USER', 'master'),
                          help='Database username (default: master, env: DB_USER)')
    db_group.add_argument('--password', '-p',
                          default=os.getenv('DB_PASSWORD'),
                          help='Database password (env: DB_PASSWORD)')
    db_group.add_argument('--oracle-client-lib',
                          default=os.getenv('ORACLE_CLIENT_LIB_DIR'),
                          help='Path to Oracle Instant Client libraries (env: ORACLE_CLIENT_LIB_DIR)')

    # Row Count Arguments
    rows_group = parser.add_argument_group('Row Counts')
    rows_group.add_argument('--catalog-rows', type=int, default=100,
                            help='Number of catalog records to create (default: 100)')
    rows_group.add_argument('--inventory-rows', type=int, default=200,
                            help='Number of inventory records to create (default: 200)')
    rows_group.add_argument('--items-rows', type=int, default=500,
                            help='Number of items records to create (default: 500)')

    # Control Options
    control_group = parser.add_argument_group('Control Options')
    control_group.add_argument('--drop-existing', action='store_true',
                               help='Drop existing tables before creating new ones')
    control_group.add_argument('--no-truncate', action='store_true',
                               help='Skip truncating tables (keep existing data)')
    control_group.add_argument('--dry-run', action='store_true',
                               help='Show what would be done without executing')
    control_group.add_argument('--verbose', '-v', action='store_true',
                               help='Enable verbose output')

    return parser.parse_args()

def get_db_connection(host, port, service, user, password, oracle_client_lib=None):
    """Establish database connection."""
    if oracle_client_lib:
        oracledb.init_oracle_client(lib_dir=oracle_client_lib)
    
    try:
        connection_string = f"{host}:{port}/{service}"
        conn = oracledb.connect(user=user, password=password, dsn=connection_string)
        print(f"Connected to Oracle database: {host}:{port}/{service}")
        return conn
    except oracledb.Error as e:
        print(f"Database connection failed: ORA-{getattr(e, 'code', 'N/A')}: {getattr(e, 'message', str(e))}")
        raise

def drop_existing_tables(cur, verbose=False):
    """Drop existing tables if they exist."""
    tables = ['items', 'inventory', 'catalog']
    
    for table in tables:
        try:
            cur.execute(f"DROP TABLE {table} CASCADE CONSTRAINTS")
            if verbose:
                print(f"  Dropped table: {table}")
        except oracledb.Error as e:
            if getattr(e, 'code', None) != 942:  # Table doesn't exist
                print(f"  Warning: Error dropping table {table}: ORA-{getattr(e, 'code', 'N/A')}: {getattr(e, 'message', str(e))}")

def create_tables(cur, verbose=False):
    """Create the catalog, inventory, and items tables."""
    
    # Create catalog table
    catalog_sql = """
    CREATE TABLE catalog (
        catalog_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        catalog_name VARCHAR2(100) NOT NULL,
        description VARCHAR2(500),
        category VARCHAR2(50) NOT NULL,
        created_date DATE DEFAULT SYSDATE,
        is_active CHAR(1) DEFAULT 'Y' CHECK (is_active IN ('Y', 'N')),
        catalog_code VARCHAR2(20) UNIQUE NOT NULL
    )
    """
    
    # Create inventory table
    inventory_sql = """
    CREATE TABLE inventory (
        inventory_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        catalog_id NUMBER NOT NULL,
        warehouse_location VARCHAR2(100),
        quantity_available NUMBER DEFAULT 0,
        quantity_reserved NUMBER DEFAULT 0,
        reorder_level NUMBER DEFAULT 10,
        last_updated DATE DEFAULT SYSDATE,
        CONSTRAINT fk_inventory_catalog FOREIGN KEY (catalog_id) REFERENCES catalog (catalog_id)
    )
    """
    
    # Create items table
    items_sql = """
    CREATE TABLE items (
        item_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        catalog_id NUMBER NOT NULL,
        inventory_id NUMBER NOT NULL,
        item_name VARCHAR2(100) NOT NULL,
        sku VARCHAR2(50) UNIQUE NOT NULL,
        price NUMBER(10,2) NOT NULL,
        cost NUMBER(10,2),
        weight_kg NUMBER(8,3),
        dimensions VARCHAR2(50),
        color VARCHAR2(30),
        brand VARCHAR2(50),
        created_date DATE DEFAULT SYSDATE,
        last_modified DATE DEFAULT SYSDATE,
        CONSTRAINT fk_items_catalog FOREIGN KEY (catalog_id) REFERENCES catalog (catalog_id),
        CONSTRAINT fk_items_inventory FOREIGN KEY (inventory_id) REFERENCES inventory (inventory_id)
    )
    """
    
    try:
        if verbose:
            print("Creating catalog table...")
        cur.execute(catalog_sql)
        
        if verbose:
            print("Creating inventory table...")
        cur.execute(inventory_sql)
        
        if verbose:
            print("Creating items table...")
        cur.execute(items_sql)
        
        print("All tables created successfully!")
        
    except oracledb.Error as e:
        print(f"Error creating tables: ORA-{getattr(e, 'code', 'N/A')}: {getattr(e, 'message', str(e))}")
        raise

def truncate_tables(cur, verbose=False):
    """Truncate all tables."""
    tables = ['items', 'inventory', 'catalog']
    
    for table in tables:
        try:
            cur.execute(f"TRUNCATE TABLE {table} CASCADE")
            if verbose:
                print(f"  Truncated table: {table}")
        except oracledb.Error as e:
            print(f"  Warning: Error truncating table {table}: ORA-{getattr(e, 'code', 'N/A')}: {getattr(e, 'message', str(e))}")

def generate_catalog_data(cur, num_rows, verbose=False):
    """Generate and insert catalog data."""
    print(f"Generating {num_rows} catalog records...")
    
    categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books', 'Toys', 'Automotive', 'Health & Beauty']
    
    for i in range(num_rows):
        catalog_name = fake.company()
        description = fake.text(max_nb_chars=200)
        category = random.choice(categories)
        catalog_code = f"CAT-{fake.unique.random_number(digits=6)}"
        
        sql = """
        INSERT INTO catalog (catalog_name, description, category, catalog_code)
        VALUES (:1, :2, :3, :4)
        """
        
        cur.execute(sql, (catalog_name, description, category, catalog_code))
        
        if verbose and (i + 1) % 100 == 0:
            print(f"  Inserted {i + 1} catalog records...")
    
    print(f"  Completed: {num_rows} catalog records")

def generate_inventory_data(cur, num_rows, verbose=False):
    """Generate and insert inventory data."""
    print(f"Generating {num_rows} inventory records...")
    
    # Get catalog IDs
    cur.execute("SELECT catalog_id FROM catalog")
    catalog_ids = [row[0] for row in cur.fetchall()]
    
    if not catalog_ids:
        print("  Error: No catalog records found. Please create catalog data first.")
        return
    
    warehouse_locations = ['Warehouse A', 'Warehouse B', 'Warehouse C', 'Distribution Center 1', 'Distribution Center 2']
    
    for i in range(num_rows):
        catalog_id = random.choice(catalog_ids)
        warehouse_location = random.choice(warehouse_locations)
        quantity_available = random.randint(0, 1000)
        quantity_reserved = random.randint(0, 100)
        reorder_level = random.randint(5, 50)
        
        sql = """
        INSERT INTO inventory (catalog_id, warehouse_location, quantity_available, quantity_reserved, reorder_level)
        VALUES (:1, :2, :3, :4, :5)
        """
        
        cur.execute(sql, (catalog_id, warehouse_location, quantity_available, quantity_reserved, reorder_level))
        
        if verbose and (i + 1) % 100 == 0:
            print(f"  Inserted {i + 1} inventory records...")
    
    print(f"  Completed: {num_rows} inventory records")

def generate_items_data(cur, num_rows, verbose=False):
    """Generate and insert items data."""
    print(f"Generating {num_rows} items records...")
    
    # Get catalog and inventory IDs
    cur.execute("SELECT c.catalog_id, i.inventory_id FROM catalog c JOIN inventory i ON c.catalog_id = i.catalog_id")
    id_pairs = cur.fetchall()
    
    if not id_pairs:
        print("  Error: No catalog-inventory pairs found. Please create catalog and inventory data first.")
        return
    
    brands = ['Nike', 'Apple', 'Samsung', 'Sony', 'LG', 'Adidas', 'Puma', 'Under Armour', 'Canon', 'Nikon']
    colors = ['Red', 'Blue', 'Green', 'Black', 'White', 'Yellow', 'Purple', 'Orange', 'Pink', 'Brown']
    
    for i in range(num_rows):
        catalog_id, inventory_id = random.choice(id_pairs)
        item_name = fake.catch_phrase()
        sku = f"SKU-{fake.unique.random_number(digits=8)}"
        price = round(random.uniform(10.0, 1000.0), 2)
        cost = round(price * random.uniform(0.3, 0.7), 2)
        weight_kg = round(random.uniform(0.1, 50.0), 3)
        dimensions = f"{random.randint(1, 100)}x{random.randint(1, 100)}x{random.randint(1, 100)}cm"
        color = random.choice(colors)
        brand = random.choice(brands)
        
        sql = """
        INSERT INTO items (catalog_id, inventory_id, item_name, sku, price, cost, weight_kg, dimensions, color, brand)
        VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10)
        """
        
        cur.execute(sql, (catalog_id, inventory_id, item_name, sku, price, cost, weight_kg, dimensions, color, brand))
        
        if verbose and (i + 1) % 100 == 0:
            print(f"  Inserted {i + 1} items records...")
    
    print(f"  Completed: {num_rows} items records")

def main():
    """Main function."""
    args = parse_arguments()
    
    # Validate required parameters
    if not args.password:
        print("Error: Database password is required. Use --password or set DB_PASSWORD environment variable.")
        return 1
    
    if args.dry_run:
        print("=== DRY RUN MODE ===")
        print(f"Would connect to: {args.host}:{args.port}/{args.service}")
        print(f"Would create: {args.catalog_rows} catalog, {args.inventory_rows} inventory, {args.items_rows} items records")
        if args.drop_existing:
            print("Would drop existing tables")
        if not args.no_truncate:
            print("Would truncate existing tables")
        return 0
    
    try:
        # Connect to database
        conn = get_db_connection(
            args.host, args.port, args.service, 
            args.user, args.password, args.oracle_client_lib
        )
        cur = conn.cursor()
        
        # Drop existing tables if requested
        if args.drop_existing:
            print("Dropping existing tables...")
            drop_existing_tables(cur, args.verbose)
            create_tables(cur, args.verbose)
        else:
            # Check if tables exist, create if they don't
            try:
                cur.execute("SELECT COUNT(*) FROM catalog")
                print("Tables already exist.")
            except oracledb.Error:
                print("Creating tables...")
                create_tables(cur, args.verbose)
        
        # Truncate tables if not preserving data
        if not args.no_truncate:
            print("Truncating existing tables...")
            truncate_tables(cur, args.verbose)
        else:
            print("Preserving existing data...")
        
        # Generate data
        print("\nGenerating data...")
        generate_catalog_data(cur, args.catalog_rows, args.verbose)
        generate_inventory_data(cur, args.inventory_rows, args.verbose)
        generate_items_data(cur, args.items_rows, args.verbose)
        
        # Commit all changes
        conn.commit()
        print("\nAll data generated and committed successfully!")
        
        # Show summary
        cur.execute("SELECT COUNT(*) FROM catalog")
        catalog_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM inventory")
        inventory_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM items")
        items_count = cur.fetchone()[0]
        
        print(f"\nFinal row counts:")
        print(f"  Catalog: {catalog_count}")
        print(f"  Inventory: {inventory_count}")
        print(f"  Items: {items_count}")
        
        cur.close()
        conn.close()
        return 0
        
    except oracledb.Error as e:
        print(f"Oracle database error: ORA-{getattr(e, 'code', 'N/A')}: {getattr(e, 'message', str(e))}")
        return 1
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return 1

if __name__ == "__main__":
    exit(main())
