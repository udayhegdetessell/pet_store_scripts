import os
import sys
import argparse
import oracledb


def parse_arguments():
    """Parse command line arguments for database connection."""
    parser = argparse.ArgumentParser(
        description='Create Pet Store Database Schema for Oracle Database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage
  python create_schema.py --host localhost --port 1521 --service ORCL --user master --password 12345678

  python3 create_schema.py --host 127.0.0.1 --port 1521 --service orcl --user master --password Tessell123ZX# --oracle-client-lib /home/azureuser/pet_store_demo/oracle/instantclient_23_9 --drop-existin

  # With custom Oracle client path
  python create_schema.py --host localhost --port 1521 --service ORCL --user master --password mypass \\
                         --oracle-client-lib /opt/oracle/instantclient_23_9

  python create_schema.py \
  --host localhost \
  --port 1521 \
  --service ORCL \
  --user master \
  --password mypassword \
  --oracle-client-lib /opt/oracle/instantclient_23_9

  # Drop existing tables first
  python create_schema.py --host localhost --port 1521 --service ORCL --user master --password mypass \\
                         --drop-existing

  # Create tables only (no sequences)
  python create_schema.py --host localhost --port 1521 --service ORCL --user master --password mypass \\
                         --tables-only
        """
    )

    # Database Connection Arguments
    parser.add_argument('--host', '-H',
                        default=os.getenv('DB_HOST', 'localhost'),
                        help='Database host (default: localhost, env: DB_HOST)')
    parser.add_argument('--port', '-P',
                        default=os.getenv('DB_PORT', '1521'),
                        help='Database port (default: 1521, env: DB_PORT)')
    parser.add_argument('--service', '-s',
                        default=os.getenv('ORACLE_SERVICE_NAME', 'ORCL'),
                        help='Oracle service name or SID (default: ORCL, env: ORACLE_SERVICE_NAME)')
    parser.add_argument('--user', '-u',
                        default=os.getenv('DB_USER', 'master'),
                        help='Database username (default: master, env: DB_USER)')
    parser.add_argument('--password', '-p',
                        default=os.getenv('DB_PASSWORD'),
                        help='Database password (env: DB_PASSWORD)')
    parser.add_argument('--oracle-client-lib',
                        default=os.getenv('ORACLE_CLIENT_LIB_DIR'),
                        help='Path to Oracle Instant Client libraries (env: ORACLE_CLIENT_LIB_DIR)')

    # Control Options
    parser.add_argument('--drop-existing', action='store_true',
                        help='Drop existing tables and sequences before creating new ones')
    parser.add_argument('--tables-only', action='store_true',
                        help='Create tables only (skip sequences)')
    parser.add_argument('--sequences-only', action='store_true',
                        help='Create sequences only (skip tables)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show SQL statements without executing them')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Verbose output')

    args = parser.parse_args()

    # Validate required arguments
    if not args.password:
        parser.error("Database password is required. Use --password or set DB_PASSWORD environment variable.")

    if args.tables_only and args.sequences_only:
        parser.error("Cannot specify both --tables-only and --sequences-only")

    return args


def get_db_connection(config):
    """Establishes and returns a new Oracle database connection."""

    # Initialize Oracle Client in thick mode if path is provided
    if config['oracle_client_lib']:
        try:
            if not os.path.exists(config['oracle_client_lib']):
                raise FileNotFoundError(f"Oracle Instant Client directory not found: {config['oracle_client_lib']}")

            oracledb.init_oracle_client(lib_dir=config['oracle_client_lib'])
            print(f"Oracle Client initialized from: {config['oracle_client_lib']}")
        except oracledb.Error as e:
            print(f"Error initializing Oracle Client: {e}")
            print("Please ensure --oracle-client-lib is set correctly and Instant Client libraries are installed.")
            raise
        except FileNotFoundError as e:
            print(f"Error: {e}")
            raise
        except Exception as e:
            print(f"Unexpected error during Oracle Client initialization: {e}")
            raise
    else:
        print("Using Oracle Client thin mode (no --oracle-client-lib specified)")

    # Oracle connection string format: user/password@hostname:port/service_name
    dsn = f"{config['host']}:{config['port']}/{config['service']}"

    try:
        return oracledb.connect(user=config['user'], password=config['password'], dsn=dsn)
    except oracledb.Error as e:
        print(f"Database connection failed: ORA-{getattr(e, 'code', 'N/A')}: {getattr(e, 'message', str(e))}")
        print(f"DSN: {config['user']}@{dsn}")
        raise


def get_table_ddl():
    """Returns dictionary of table creation SQL statements."""
    return {
        'suppliers': """
                     CREATE TABLE suppliers
                     (
                         supplier_id    NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                         supplier_name  VARCHAR2(100) NOT NULL,
                         contact_person VARCHAR2(100),
                         phone_number   VARCHAR2(20),
                         email          VARCHAR2(100),
                         address        VARCHAR2(500),
                         created_date   DATE DEFAULT SYSDATE,
                         CONSTRAINT uk_suppliers_name UNIQUE (supplier_name),
                         CONSTRAINT ck_suppliers_email CHECK (email LIKE '%@%.%')
                     )
                     """,

        'employees': """
                     CREATE TABLE employees
                     (
                         employee_id  NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                         first_name   VARCHAR2(50) NOT NULL,
                         last_name    VARCHAR2(50) NOT NULL,
                         email        VARCHAR2(100) UNIQUE NOT NULL,
                         phone_number VARCHAR2(20),
                         hire_date    DATE NOT NULL,
                         job_title    VARCHAR2(50),
                         salary       NUMBER(10,2),
                         manager_id   NUMBER,
                         created_date DATE DEFAULT SYSDATE,
                         CONSTRAINT fk_employees_manager FOREIGN KEY (manager_id) REFERENCES employees (employee_id),
                         CONSTRAINT ck_employees_email CHECK (email LIKE '%@%.%'),
                         CONSTRAINT ck_employees_salary CHECK (salary >= 0)
                     )
                     """,

        'customers': """
                     CREATE TABLE customers
                     (
                         customer_id       NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                         first_name        VARCHAR2(50) NOT NULL,
                         last_name         VARCHAR2(50) NOT NULL,
                         email             VARCHAR2(100) UNIQUE NOT NULL,
                         phone_number      VARCHAR2(20),
                         address_line1     VARCHAR2(100),
                         address_line2     VARCHAR2(100),
                         city              VARCHAR2(50),
                         state             VARCHAR2(50),
                         zip_code          VARCHAR2(10),
                         registration_date DATE DEFAULT SYSDATE,
                         CONSTRAINT ck_customers_email CHECK (email LIKE '%@%.%')
                     )
                     """,

        'products': """
                    CREATE TABLE products
                    (
                        product_id          NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                        product_name        VARCHAR2(100) NOT NULL,
                        product_description CLOB,
                        product_type        VARCHAR2(50),
                        price               NUMBER(10,2) NOT NULL,
                        cost                NUMBER(10,2),
                        quantity_in_stock   NUMBER DEFAULT 0,
                        supplier_id         NUMBER,
                        created_date        DATE   DEFAULT SYSDATE,
                        last_updated        DATE   DEFAULT SYSDATE,
                        CONSTRAINT fk_products_supplier FOREIGN KEY (supplier_id) REFERENCES suppliers (supplier_id),
                        CONSTRAINT ck_products_price CHECK (price >= 0),
                        CONSTRAINT ck_products_cost CHECK (cost >= 0),
                        CONSTRAINT ck_products_stock CHECK (quantity_in_stock >= 0),
                        CONSTRAINT ck_products_type CHECK (product_type IN
                                                           ('Food', 'Toy', 'Accessory', 'Pet', 'Grooming', 'Medicine',
                                                            'Service'))
                    )
                    """,

        'pets': """
                CREATE TABLE pets
                (
                    pet_id          NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    product_id      NUMBER UNIQUE,
                    pet_name        VARCHAR2(50),
                    species         VARCHAR2(50),
                    breed           VARCHAR2(50),
                    date_of_birth   DATE,
                    gender          CHAR(1),
                    color           VARCHAR2(50),
                    health_status   VARCHAR2(500),
                    microchip_id    VARCHAR2(50) UNIQUE,
                    adoption_status VARCHAR2(20) DEFAULT 'Available',
                    entry_date      DATE DEFAULT SYSDATE,
                    adopted_date    DATE,
                    CONSTRAINT fk_pets_product FOREIGN KEY (product_id) REFERENCES products (product_id),
                    CONSTRAINT ck_pets_gender CHECK (gender IN ('M', 'F')),
                    CONSTRAINT ck_pets_adoption_status CHECK (adoption_status IN ('Available', 'Adopted', 'On Hold', 'Medical Care')),
                    CONSTRAINT ck_pets_dates CHECK (adopted_date IS NULL OR adopted_date >= entry_date)
                )
                """,

        'orders': """
                  CREATE TABLE orders
                  (
                      order_id         NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                      customer_id      NUMBER NOT NULL,
                      order_date       DATE DEFAULT SYSDATE,
                      total_amount     NUMBER(10,2) DEFAULT 0,
                      order_status     VARCHAR2(20) DEFAULT 'Pending',
                      shipping_address VARCHAR2(200),
                      city             VARCHAR2(50),
                      state            VARCHAR2(50),
                      zip_code         VARCHAR2(10),
                      payment_method   VARCHAR2(50),
                      shipped_date     DATE,
                      delivered_date   DATE,
                      CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES customers (customer_id),
                      CONSTRAINT ck_orders_total CHECK (total_amount >= 0),
                      CONSTRAINT ck_orders_status CHECK (order_status IN
                                                         ('Pending', 'Processing', 'Shipped', 'Delivered', 'Cancelled',
                                                          'Returned')),
                      CONSTRAINT ck_orders_dates CHECK (shipped_date IS NULL OR shipped_date >= order_date),
                      CONSTRAINT ck_orders_delivery CHECK (delivered_date IS NULL OR
                                                           delivered_date >= NVL(shipped_date, order_date))
                  )
                  """,

        'order_items': """
                       CREATE TABLE order_items
                       (
                           order_item_id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                           order_id      NUMBER NOT NULL,
                           product_id    NUMBER NOT NULL,
                           quantity      NUMBER NOT NULL,
                           unit_price    NUMBER(10,2) NOT NULL,
                           item_total    NUMBER(10,2) GENERATED ALWAYS AS (quantity * unit_price),
                           CONSTRAINT fk_order_items_order FOREIGN KEY (order_id) REFERENCES orders (order_id) ON DELETE CASCADE,
                           CONSTRAINT fk_order_items_product FOREIGN KEY (product_id) REFERENCES products (product_id),
                           CONSTRAINT ck_order_items_quantity CHECK (quantity > 0),
                           CONSTRAINT ck_order_items_price CHECK (unit_price >= 0)
                       )
                       """,

        'pet_care_logs': """
                         CREATE TABLE pet_care_logs
                         (
                             log_id        NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                             pet_id        NUMBER NOT NULL,
                             employee_id   NUMBER NOT NULL,
                             log_datetime  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                             activity_type VARCHAR2(50),
                             notes         CLOB,
                             CONSTRAINT fk_pet_care_pet FOREIGN KEY (pet_id) REFERENCES pets (pet_id) ON DELETE CASCADE,
                             CONSTRAINT fk_pet_care_employee FOREIGN KEY (employee_id) REFERENCES employees (employee_id),
                             CONSTRAINT ck_care_activity CHECK (activity_type IN
                                                                ('Feeding', 'Grooming', 'Medication', 'Vet Visit',
                                                                 'Cleaning', 'Playtime', 'Training', 'Exercise'))
                         )
                         """,
        'oracle_datatypes_demo': """
                                 CREATE TABLE oracle_datatypes_demo
                                 (
                                     -- Primary Key
                                     demo_id                    NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,

                                     -- VARCHAR2 - Variable-length character string
                                     varchar2_column            VARCHAR2(100),
                                     varchar2_large_column      VARCHAR2(4000),

                                     -- NVARCHAR2 - Variable-length Unicode character string
                                     nvarchar2_column           NVARCHAR2(50),
                                     nvarchar2_large_column     NVARCHAR2(2000),

                                     -- NUMBER - Numeric data
                                     number_column              NUMBER,
                                     number_precision_column    NUMBER(10,2),
                                     number_integer_column      NUMBER(8),

                                     -- FLOAT - Floating-point number
                                     float_column               FLOAT,
                                     float_precision_column     FLOAT(126),

                                     -- LONG - Variable-length character data
                                     long_column                LONG,

                                     -- DATE - Date and time
                                     date_column                DATE,

                                     -- BINARY_FLOAT - 32-bit floating-point number
                                     binary_float_column        BINARY_FLOAT,

                                     -- BINARY_DOUBLE - 64-bit floating-point number  
                                     binary_double_column       BINARY_DOUBLE,

                                     -- TIMESTAMP - Date and time with fractional seconds
                                     timestamp_column           TIMESTAMP,
                                     timestamp_precision_column TIMESTAMP(9),

                                     -- TIMESTAMP WITH TIME ZONE
                                     timestamp_tz_column        TIMESTAMP WITH TIME ZONE,
                                     timestamp_tz_precision     TIMESTAMP(6) WITH TIME ZONE,

                                     -- INTERVAL YEAR TO MONTH
                                     interval_ym_column         INTERVAL YEAR TO MONTH,
                                     interval_ym_precision      INTERVAL YEAR(4) TO MONTH,

                                     -- INTERVAL DAY TO SECOND
                                     interval_ds_column         INTERVAL DAY TO SECOND,
                                     interval_ds_precision      INTERVAL DAY(2) TO SECOND(6),

                                     -- ROWID - Physical address of a row
                                     rowid_column               ROWID,

                                     -- CHAR - Fixed-length character string
                                     char_column                CHAR(10),
                                     char_large_column          CHAR(2000),

                                     -- NCHAR - Fixed-length Unicode character string
                                     nchar_column               NCHAR(10),
                                     nchar_large_column         NCHAR(1000),

                                     -- Metadata columns
                                     created_date               DATE      DEFAULT SYSDATE,
                                     last_updated               TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                                     -- Constraints
                                     CONSTRAINT ck_datatypes_number_positive CHECK (number_precision_column IS NULL OR number_precision_column >= 0),
                                     CONSTRAINT ck_datatypes_binary_float_range CHECK (binary_float_column IS NULL OR
                                                                                       binary_float_column BETWEEN -3.40282E+38 AND 3.40282E+38)
                                 )
                                 """
    }


def get_sequence_ddl():
    """Returns dictionary of sequence creation SQL statements."""
    return {
        'supplier_id_seq': """
        CREATE SEQUENCE supplier_id_seq
            START WITH 1
            INCREMENT BY 1
            NOCACHE
            NOCYCLE
    """,

        'employee_id_seq': """
        CREATE SEQUENCE employee_id_seq
            START WITH 1
            INCREMENT BY 1
            NOCACHE
            NOCYCLE
    """,

        'customer_id_seq': """
        CREATE SEQUENCE customer_id_seq
            START WITH 1
            INCREMENT BY 1
            NOCACHE
            NOCYCLE
    """,

        'product_id_seq': """
        CREATE SEQUENCE product_id_seq
            START WITH 1
            INCREMENT BY 1
            NOCACHE
            NOCYCLE
    """,

        'pet_id_seq': """
        CREATE SEQUENCE pet_id_seq
            START WITH 1
            INCREMENT BY 1
            NOCACHE
            NOCYCLE
    """,

        'order_id_seq': """
        CREATE SEQUENCE order_id_seq
            START WITH 1
            INCREMENT BY 1
            NOCACHE
            NOCYCLE
    """,

        'log_id_seq': """
        CREATE SEQUENCE log_id_seq
            START WITH 1
            INCREMENT BY 1
            NOCACHE
            NOCYCLE
    """,
        'oracle_datatypes_demo_seq': """
                CREATE SEQUENCE oracle_datatypes_demo_seq
                    START WITH 1
                    INCREMENT BY 1
                    NOCACHE
                    NOCYCLE
            """
    }


def get_index_ddl():
    """Returns dictionary of index creation SQL statements."""
    return {
        'idx_products_supplier': 'CREATE INDEX idx_products_supplier ON products(supplier_id)',
        'idx_products_type': 'CREATE INDEX idx_products_type ON products(product_type)',
        'idx_pets_species': 'CREATE INDEX idx_pets_species ON pets(species)',
        'idx_pets_adoption_status': 'CREATE INDEX idx_pets_adoption_status ON pets(adoption_status)',
        'idx_orders_customer': 'CREATE INDEX idx_orders_customer ON orders(customer_id)',
        'idx_orders_date': 'CREATE INDEX idx_orders_date ON orders(order_date)',
        'idx_orders_status': 'CREATE INDEX idx_orders_status ON orders(order_status)',
        'idx_order_items_order': 'CREATE INDEX idx_order_items_order ON order_items(order_id)',
        'idx_order_items_product': 'CREATE INDEX idx_order_items_product ON order_items(product_id)',
        'idx_care_logs_pet': 'CREATE INDEX idx_care_logs_pet ON pet_care_logs(pet_id)',
        'idx_care_logs_employee': 'CREATE INDEX idx_care_logs_employee ON pet_care_logs(employee_id)',
        'idx_care_logs_datetime': 'CREATE INDEX idx_care_logs_datetime ON pet_care_logs(log_datetime)',
        'idx_datatypes_demo_date': 'CREATE INDEX idx_datatypes_demo_date ON oracle_datatypes_demo(created_date)'
    }


def drop_existing_objects(cur, verbose=False):
    """Drop existing tables and sequences."""
    print("Dropping existing tables and sequences...")

    # Drop tables in reverse dependency order
    tables = ['oracle_datatypes_demo', 'pet_care_logs', 'order_items', 'orders', 'pets', 'products', 'employees',
              'customers', 'suppliers']
    sequences = ['oracle_datatypes_demo_seq', 'log_id_seq', 'order_id_seq', 'pet_id_seq', 'product_id_seq',
                 'employee_id_seq', 'customer_id_seq',
                 'supplier_id_seq']

    # Drop tables
    for table in tables:
        try:
            sql = f"DROP TABLE {table} CASCADE CONSTRAINTS"
            if verbose:
                print(f"  Executing: {sql}")
            cur.execute(sql)
            print(f"  Dropped table: {table}")
        except oracledb.Error as e:
            if getattr(e, 'code', 'N/A') == 942:  # ORA-00942: table or view does not exist
                if verbose:
                    print(f"  Table {table} does not exist (skipping)")
            else:
                print(
                    f"  Warning: Error dropping table {table}: ORA-{getattr(e, 'code', 'N/A')}: {getattr(e, 'message', str(e))}")

    # Drop sequences
    for seq in sequences:
        try:
            sql = f"DROP SEQUENCE {seq}"
            if verbose:
                print(f"  Executing: {sql}")
            cur.execute(sql)
            print(f"  Dropped sequence: {seq}")
        except oracledb.Error as e:
            if getattr(e, 'code', 'N/A') == 2289:  # ORA-02289: sequence does not exist
                if verbose:
                    print(f"  Sequence {seq} does not exist (skipping)")
            else:
                print(
                    f"  Warning: Error dropping sequence {seq}: ORA-{getattr(e, 'code', 'N/A')}: {getattr(e, 'message', str(e))}")


def create_sequences(cur, verbose=False, dry_run=False):
    """Create database sequences."""
    print("Creating sequences...")
    sequences = get_sequence_ddl()

    for seq_name, seq_ddl in sequences.items():
        try:
            if verbose or dry_run:
                print(f"  Creating sequence: {seq_name}")
                if verbose:
                    print(f"    SQL: {seq_ddl.strip()}")

            if not dry_run:
                cur.execute(seq_ddl)
                print(f"  ✓ Created sequence: {seq_name}")
            else:
                print(f"  [DRY RUN] Would create sequence: {seq_name}")

        except oracledb.Error as e:
            if getattr(e, 'code', 'N/A') == 955:  # ORA-00955: name is already used by an existing object
                print(f"  ⚠ Sequence {seq_name} already exists")
            else:
                print(
                    f"  ✗ Error creating sequence {seq_name}: ORA-{getattr(e, 'code', 'N/A')}: {getattr(e, 'message', str(e))}")
                raise


def create_tables(cur, verbose=False, dry_run=False):
    """Create database tables."""
    print("Creating tables...")
    tables = get_table_ddl()

    # Create tables in dependency order
    table_order = ['suppliers', 'employees', 'customers', 'products', 'pets', 'orders', 'order_items',
                   'pet_care_logs', 'oracle_datatypes_demo']

    for table_name in table_order:
        try:
            table_ddl = tables[table_name]
            if verbose or dry_run:
                print(f"  Creating table: {table_name}")
                if verbose:
                    print(f"    SQL: {table_ddl.strip()}")

            if not dry_run:
                cur.execute(table_ddl)
                print(f"  ✓ Created table: {table_name}")
            else:
                print(f"  [DRY RUN] Would create table: {table_name}")

        except oracledb.Error as e:
            if getattr(e, 'code', 'N/A') == 955:  # ORA-00955: name is already used by an existing object
                print(f"  ⚠ Table {table_name} already exists")
            else:
                print(
                    f"  ✗ Error creating table {table_name}: ORA-{getattr(e, 'code', 'N/A')}: {getattr(e, 'message', str(e))}")
                raise


def create_indexes(cur, verbose=False, dry_run=False):
    """Create database indexes."""
    print("Creating indexes...")
    indexes = get_index_ddl()

    for idx_name, idx_ddl in indexes.items():
        try:
            if verbose or dry_run:
                print(f"  Creating index: {idx_name}")
                if verbose:
                    print(f"    SQL: {idx_ddl}")

            if not dry_run:
                cur.execute(idx_ddl)
                print(f"  ✓ Created index: {idx_name}")
            else:
                print(f"  [DRY RUN] Would create index: {idx_name}")

        except oracledb.Error as e:
            if getattr(e, 'code', 'N/A') == 955:  # ORA-00955: name is already used by an existing object
                print(f"  ⚠ Index {idx_name} already exists")
            else:
                print(
                    f"  ⚠ Warning: Error creating index {idx_name}: ORA-{getattr(e, 'code', 'N/A')}: {getattr(e, 'message', str(e))}")
                # Don't raise for indexes, as they're not critical


def main():
    """Main function to create the database schema."""
    args = parse_arguments()

    config = {
        'host': args.host,
        'port': args.port,
        'service': args.service,
        'user': args.user,
        'password': args.password,
        'oracle_client_lib': args.oracle_client_lib
    }

    print("Pet Store Database Schema Creator")
    print("=" * 50)
    print(f"Host: {config['host']}:{config['port']}")
    print(f"Service: {config['service']}")
    print(f"User: {config['user']}")
    if config['oracle_client_lib']:
        print(f"Oracle Client: {config['oracle_client_lib']}")
    print(f"Drop Existing: {args.drop_existing}")
    print(f"Dry Run: {args.dry_run}")
    print("=" * 50)

    conn = None
    cur = None

    try:
        # Connect to database
        print("Connecting to database...")
        conn = get_db_connection(config)
        cur = conn.cursor()
        print("✓ Connected successfully")

        # Drop existing objects if requested
        if args.drop_existing and not args.dry_run:
            drop_existing_objects(cur, args.verbose)
            conn.commit()
        elif args.drop_existing and args.dry_run:
            print("[DRY RUN] Would drop existing tables and sequences")

        # Create sequences (unless tables-only is specified)
        if not args.tables_only:
            create_sequences(cur, args.verbose, args.dry_run)
            if not args.dry_run:
                conn.commit()

        # Create tables (unless sequences-only is specified)
        if not args.sequences_only:
            create_tables(cur, args.verbose, args.dry_run)
            if not args.dry_run:
                conn.commit()

            # Create indexes
            create_indexes(cur, args.verbose, args.dry_run)
            if not args.dry_run:
                conn.commit()

        if not args.dry_run:
            print("\n✓ Schema creation completed successfully!")
            print("\nCreated objects:")
            if not args.sequences_only:
                print(
                    "  - 8 tables (suppliers, employees, customers, products, pets, orders, order_items, pet_care_logs)")
                print("  - Foreign key constraints")
                print("  - Check constraints")
                print("  - Indexes for optimal performance")
            if not args.tables_only:
                print("  - 7 sequences for primary key generation")
        else:
            print("\n[DRY RUN] Schema creation commands displayed above")

    except Exception as e:
        print(f"\n✗ Error: {e}")
        if conn and not args.dry_run:
            conn.rollback()
        return 1

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())