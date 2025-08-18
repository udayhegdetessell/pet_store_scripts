import os
import random
import time
import threading
import argparse
from faker import Faker
from datetime import datetime, timedelta

try:
    import oracledb
except ImportError:
    oracledb = None


# --- 1. COMMAND LINE ARGUMENT PARSING ---
def parse_arguments():
    """Parse command line arguments for database connection and configuration."""
    parser = argparse.ArgumentParser(
        description='Pet Store Data Generator for Oracle Database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage with defaults
  python %(prog)s --host localhost --port 1521 --service ORCL --user master --password 12345678

  # Custom configuration
  python %(prog)s --host db.example.com --port 1521 --service FREEPDB1 --user petstore --password mypass \\
                   --oracle-client-lib /opt/oracle/instantclient_23_9 \\
                   --initial-customers 500 --initial-products 200 \\
                   --order-interval 20 --orders-per-interval 10

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

    # Initial Data Configuration
    initial_group = parser.add_argument_group('Initial Data Configuration')
    initial_group.add_argument('--initial-suppliers', type=int, default=10,
                               help='Number of initial suppliers to create (default: 10)')
    initial_group.add_argument('--initial-employees', type=int, default=20,
                               help='Number of initial employees to create (default: 20)')
    initial_group.add_argument('--initial-customers', type=int, default=200,
                               help='Number of initial customers to create (default: 200)')
    initial_group.add_argument('--initial-products', type=int, default=100,
                               help='Number of initial products to create (default: 100)')
    initial_group.add_argument('--initial-pets', type=int, default=20,
                               help='Number of initial pets to create (default: 20)')
    initial_group.add_argument('--initial-care-logs', type=int, default=50,
                               help='Number of initial care logs to create (default: 50)')
    initial_group.add_argument('--initial-datatypes-demo', type=int, default=10,
                               help='Number of initial records to create for the datatypes demo table (default: 10)')

    # Real-time Generation Configuration
    realtime_group = parser.add_argument_group('Real-time Generation Configuration')
    realtime_group.add_argument('--order-interval', type=int, default=30,
                                help='Interval in seconds for generating new orders (default: 30)')
    realtime_group.add_argument('--product-interval', type=int, default=60,
                                help='Interval in seconds for generating new products (default: 60)')
    realtime_group.add_argument('--orders-per-interval', type=int, default=5,
                                help='Number of orders to create per interval (default: 5)')
    realtime_group.add_argument('--products-per-interval', type=int, default=3,
                                help='Number of products to create per interval (default: 3)')

    # Control Options
    control_group = parser.add_argument_group('Control Options')
    control_group.add_argument('--no-truncate', action='store_true',
                               help='Skip truncating tables and resetting sequences')
    control_group.add_argument('--no-initial-data', action='store_true',
                               help='Skip initial data population')
    control_group.add_argument('--no-realtime', action='store_true',
                               help='Skip real-time data generation')
    control_group.add_argument('--setup-only', action='store_true',
                               help='Only run initial setup, then exit (no real-time generation)')

    args = parser.parse_args()

    # Validate required arguments
    if not args.password:
        parser.error("Database password is required. Use --password or set DB_PASSWORD environment variable.")

    if not args.oracle_client_lib:
        parser.error(
            "Oracle client library path is required. Use --oracle-client-lib or set ORACLE_CLIENT_LIB_DIR environment variable.")

    return args


# --- 2. GLOBAL CONFIGURATION (will be set from command line args) ---
CONFIG = {}

# --- 3. GLOBAL DATA AND LOCKS ---
# Shared lists for IDs to be used across threads
supplier_ids = []
employee_ids = []
customer_ids = []
product_ids = []
pet_ids = []
# Use locks for thread-safe access to shared lists
list_lock = threading.Lock()

# --- 4. HELPER FUNCTIONS FOR SINGLE INSERTS ---
# Pre-defined lists for realism
fake = Faker()
pet_species = ['Dog', 'Cat', 'Fish', 'Bird', 'Hamster', 'Rabbit', 'Reptile']
dog_breeds = ['Golden Retriever', 'Labrador', 'German Shepherd', 'Poodle', 'Bulldog', 'Beagle', 'Pug', 'Dachshund',
              'Mixed Breed']
cat_breeds = ['Persian', 'Siamese', 'Maine Coon', 'Ragdoll', 'Bengal', 'Russian Blue', 'Domestic Shorthair']
fish_types = ['Clownfish', 'Goldfish', 'Betta', 'Angelfish', 'Blue Tang']
bird_types = ['African Grey Parrot', 'Canary', 'Macaw', 'Cockatiel', 'Parakeet']
product_types = ['Food', 'Toy', 'Accessory', 'Pet', 'Grooming', 'Medicine', 'Service']
employee_job_titles = ['Manager', 'Sales Associate', 'Vet Technician', 'Groomer', 'Warehouse Staff',
                       'Customer Service Rep']
order_statuses = ['Pending', 'Processing', 'Shipped', 'Delivered', 'Cancelled']
payment_methods = ['Credit Card', 'Debit Card', 'Cash', 'Online Transfer', 'UPI']
care_log_activities = ['Feeding', 'Grooming', 'Medication', 'Vet Visit', 'Cleaning', 'Playtime']


def get_db_connection():
    """Establishes and returns a new Oracle database connection."""
    if not oracledb:
        raise ImportError("oracledb not installed. Please run: pip install oracledb")

    # Initialize Oracle Client in thick mode
    try:
        # Check if the directory exists before initializing
        if not os.path.exists(CONFIG['oracle_client_lib']):
            raise FileNotFoundError(f"Oracle Instant Client directory not found: {CONFIG['oracle_client_lib']}")

        oracledb.init_oracle_client(lib_dir=CONFIG['oracle_client_lib'])
        print(f"Oracle Client initialized from: {CONFIG['oracle_client_lib']}")
    except oracledb.Error as e:
        print(f"Error initializing Oracle Client (oracledb.Error): {e}")
        print("Please ensure --oracle-client-lib is set correctly and Instant Client libraries are installed.")
        print(
            "You might also need to set LD_LIBRARY_PATH (Linux) or PATH (Windows) or DYLD_LIBRARY_PATH (macOS) if not using lib_dir.")
        raise  # Re-raise the exception after printing helpful message
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please double-check the '--oracle-client-lib' path in your command line arguments.")
        raise  # Re-raise the exception
    except Exception as e:  # Catch any other unexpected errors during init
        print(f"An unexpected error occurred during Oracle Client initialization: {e}")
        raise

    # Oracle connection string format: user/password@hostname:port/service_name
    dsn = f"{CONFIG['host']}:{CONFIG['port']}/{CONFIG['service']}"
    return oracledb.connect(user=CONFIG['user'], password=CONFIG['password'], dsn=dsn)


def get_table_columns(cur, table_name):
    """Get the list of columns for a table, excluding known virtual columns."""
    try:
        # First try the modern approach (Oracle 11g+)
        cur.execute("""
                    SELECT column_name
                    FROM user_tab_columns
                    WHERE table_name = UPPER(:1)
                      AND virtual_column = 'NO'
                    ORDER BY column_id
                    """, (table_name,))
        return [row[0].lower() for row in cur.fetchall()]
    except oracledb.Error:
        # Fallback for older Oracle versions - get all columns
        cur.execute("""
                    SELECT column_name
                    FROM user_tab_columns
                    WHERE table_name = UPPER(:1)
                    ORDER BY column_id
                    """, (table_name,))
        all_columns = [row[0].lower() for row in cur.fetchall()]

        # Manually exclude commonly known virtual columns based on your schema
        virtual_columns = {'total_amount', 'item_total'}  # Add other known virtual columns here
        return [col for col in all_columns if col not in virtual_columns]


def truncate_and_reset_sequences(cur, conn):
    """Truncates all tables and resets sequences for Oracle Database."""
    print("Truncating existing data and resetting sequences...")
    # Oracle requires separate TRUNCATE and sequence reset commands
    tables = ['pet_care_logs', 'order_items', 'orders', 'pets', 'products', 'employees', 'customers', 'suppliers']
    sequences = ['log_id_seq', 'order_id_seq', 'pet_id_seq', 'product_id_seq', 'employee_id_seq', 'customer_id_seq',
                 'supplier_id_seq']

    for table in tables:
        try:
            cur.execute(f"TRUNCATE TABLE {table} CASCADE")
        except oracledb.Error as e:
            # ORA-00942: table or view does not exist (if table was just created)
            # ORA-02266: unique/primary keys in table referenced by enabled foreign keys
            if e.code in (942, 2266):
                pass  # Table might not exist yet, or FKs are still enabled (handled by CASCADE)
            else:
                raise

    for seq in sequences:
        try:
            cur.execute(f"ALTER SEQUENCE {seq} RESTART")
        except oracledb.Error as e:
            if e.code == 2289:  # ORA-02289: sequence does not exist
                pass  # Sequence might not exist yet (if it was just created)
            else:
                raise

    conn.commit()  # Commit after all truncates and sequence resets


def insert_single_supplier(cur, conn, supplier_index=None):
    """Inserts one supplier and returns its ID."""
    # Generate unique company name to avoid duplicates
    if supplier_index is not None:
        name = f"{fake.company()} #{supplier_index}"
    else:
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        name = f"{fake.company()} {timestamp}"

    phone_number = fake.numerify('##########')
    address = f"{fake.street_address()}, {fake.city()}"

    # Generate unique email
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    random_num = random.randint(1000, 9999)
    unique_email = f"supplier_{timestamp}_{random_num}@{fake.domain_name()}"

    # Oracle uses RETURNING INTO ... with a bind variable
    new_id = cur.var(oracledb.NUMBER)
    cur.execute(
        """INSERT INTO suppliers (supplier_name, contact_person, phone_number, email, address)
           VALUES (:1, :2, :3, :4, :5) RETURNING supplier_id
           INTO :6""",
        (name, fake.name(), phone_number, unique_email, address, new_id)
    )
    return new_id.getvalue()[0]


def insert_single_employee(cur, conn, manager_id=None, job_title=None, employee_index=None):
    """Inserts one employee and returns its ID."""
    job = job_title if job_title else random.choice(employee_job_titles)

    # Generate unique email using timestamp and random number to avoid duplicates
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    random_num = random.randint(1000, 9999)
    if employee_index is not None:
        unique_email = f"employee_{employee_index}_{timestamp}_{random_num}@{fake.domain_name()}"
    else:
        unique_email = f"emp_{timestamp}_{random_num}@{fake.domain_name()}"

    new_id = cur.var(oracledb.NUMBER)
    cur.execute(
        """INSERT INTO employees (first_name, last_name, email, phone_number, hire_date, job_title, salary, manager_id)
           VALUES (:1, :2, :3, :4, :5, :6, :7, :8) RETURNING employee_id
           INTO :9""",
        (fake.first_name(), fake.last_name(), unique_email, fake.numerify('##########'), fake.date_this_century(), job,
         random.uniform(30000, 100000), manager_id, new_id)
    )
    return new_id.getvalue()[0]


def insert_single_customer(cur, conn, i):
    """Inserts one customer and returns its ID."""
    new_id = cur.var(oracledb.NUMBER)
    cur.execute(
        """INSERT INTO customers (first_name, last_name, email, phone_number, address_line1, city, state, zip_code,
                                  registration_date)
           VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9) RETURNING customer_id
           INTO :10""",
        (fake.first_name(), fake.last_name(), f"{fake.user_name()}_{i}@{fake.domain_name()}",
         fake.numerify('##########'), fake.street_address(), fake.city(), fake.state_abbr(), fake.zipcode(),
         fake.date_this_decade(), new_id)
    )
    return new_id.getvalue()[0]


def insert_single_product(cur, conn, product_type=None, supplier_id=None):
    """Inserts one product and returns its ID."""
    prod_type = product_type if product_type else random.choice(product_types)
    price = random.uniform(5, 500)
    cost = price * random.uniform(0.5, 0.8)

    with list_lock:
        if not supplier_ids:
            print("No suppliers available to generate a product.")
            return None
        supp_id = supplier_id if supplier_id else random.choice(supplier_ids)

    new_id = cur.var(oracledb.NUMBER)
    cur.execute(
        """INSERT INTO products (product_name, product_description, product_type, price, cost, quantity_in_stock,
                                 supplier_id)
           VALUES (:1, :2, :3, :4, :5, :6, :7) RETURNING product_id
           INTO :8""",
        (f"Live Pet: {fake.word().title()}" if prod_type == 'Pet' else fake.word().title(), fake.text(), prod_type,
         price, cost, random.randint(10, 200), supp_id, new_id)
    )
    return new_id.getvalue()[0]


def insert_single_pet(cur, conn, product_id):
    """Inserts one pet and returns its ID."""
    species = random.choice(pet_species)
    breed = None
    if species == 'Dog':
        breed = random.choice(dog_breeds)
    elif species == 'Cat':
        breed = random.choice(cat_breeds)
    elif species == 'Fish':
        breed = random.choice(fish_types)
    elif species == 'Bird':
        breed = random.choice(bird_types)

    new_id = cur.var(oracledb.NUMBER)
    cur.execute(
        """INSERT INTO pets (product_id, pet_name, species, breed, date_of_birth, gender, color, health_status,
                             microchip_id, adoption_status, entry_date)
           VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11) RETURNING pet_id
           INTO :12""",
        (product_id, fake.first_name(), species, breed, fake.date_of_birth(), random.choice(['M', 'F']),
         fake.color_name(), fake.sentence(nb_words=5), fake.ssn(), 'Available', fake.date_this_decade(), new_id)
    )
    return new_id.getvalue()[0]


def insert_single_order(cur, conn):
    """Inserts one order and its items."""
    with list_lock:
        if not customer_ids:
            print("No customers available to generate an order.")
            return None
        customer_id = random.choice(customer_ids)

    order_date = fake.date_this_year()
    order_status = random.choice(order_statuses)

    cur.execute("SELECT address_line1, city, state, zip_code FROM customers WHERE customer_id = :1", (customer_id,))
    address_info = cur.fetchone()

    if not address_info:
        print(f"Skipping order: Customer ID {customer_id} not found.")
        return None

    # Get the actual column names for the orders table (excluding virtual columns)
    order_columns = get_table_columns(cur, 'orders')

    # Prepare the order data - only include columns that exist and are not virtual
    order_data = {
        'customer_id': customer_id,
        'order_date': order_date,
        'order_status': order_status,
        'shipping_address': address_info[0],
        'city': address_info[1],
        'state': address_info[2],
        'zip_code': address_info[3],
        'payment_method': random.choice(payment_methods)
    }

    # Filter order_data to only include columns that actually exist in the table
    filtered_data = {k: v for k, v in order_data.items() if k in order_columns}

    # Build the INSERT statement dynamically
    columns = list(filtered_data.keys())
    placeholders = [f':{i + 1}' for i in range(len(columns))]
    values = list(filtered_data.values())

    new_id = cur.var(oracledb.NUMBER)
    insert_sql = f"""INSERT INTO orders ({', '.join(columns)})
                     VALUES ({', '.join(placeholders)}) RETURNING order_id
                     INTO :{len(placeholders) + 1}"""

    values.append(new_id)
    cur.execute(insert_sql, values)
    order_id = new_id.getvalue()[0]

    num_items = random.randint(1, 5)
    order_total = 0.0

    with list_lock:
        if not product_ids:
            print("No products available to generate an order.")
            return None
        order_products = random.sample(product_ids, k=min(num_items, len(product_ids)))

    for prod_id in order_products:
        cur.execute("SELECT price FROM products WHERE product_id = :1", (prod_id,))
        price_tuple = cur.fetchone()
        if not price_tuple:
            print(f"Skipping order item: Product ID {prod_id} not found.")
            continue

        price = float(price_tuple[0])
        quantity = random.randint(1, 4)
        item_total = price * quantity
        order_total += item_total

        cur.execute(
            """INSERT INTO order_items (order_id, product_id, quantity, unit_price)
               VALUES (:1, :2, :3, :4)""",
            (order_id, prod_id, quantity, price)
        )

    # Only update total_amount if it's a real column (not virtual)
    if 'total_amount' in order_columns:
        cur.execute("UPDATE orders SET total_amount = :1 WHERE order_id = :2", (order_total, order_id))

    return order_id


def insert_single_care_log(cur, conn):
    """Inserts one pet care log."""
    with list_lock:
        if not pet_ids:
            print("No pets available to generate a care log.")
            return None
        pet_id = random.choice(pet_ids)

        if not employee_ids:
            print("No employees available to generate a care log.")
            return None

        # Get a list of employees with specific job titles from the employee_ids list
        placeholders = ', '.join([f':{i + 1}' for i in range(len(employee_ids))])
        query = f"SELECT employee_id FROM employees WHERE job_title IN ('Vet Technician', 'Groomer') AND employee_id IN ({placeholders})"
        cur.execute(query, employee_ids)

        care_employees = [row[0] for row in cur.fetchall()]

        if not care_employees:
            print("No qualified employees to generate a care log.")
            return None
        employee_id = random.choice(care_employees)

    log_datetime = datetime.now() - timedelta(days=random.randint(0, 365))
    activity_type = random.choice(care_log_activities)
    notes = f"Performed {activity_type.lower()} for pet. Notes: {fake.sentence()}"

    cur.execute(
        """INSERT INTO pet_care_logs (pet_id, employee_id, log_datetime, activity_type, notes)
           VALUES (:1, :2, :3, :4, :5)""",
        (pet_id, employee_id, log_datetime, activity_type, notes)
    )


def insert_single_datatypes_demo_record(cur, conn, i):
    """Inserts a single row into the oracle_datatypes_demo table."""
    try:
        # Generate data for each column type
        varchar2_col = fake.word()
        varchar2_large_col = fake.text(max_nb_chars=4000)
        nvarchar2_col = fake.word()
        nvarchar2_large_col = fake.text(max_nb_chars=2000)
        number_col = fake.pyint()
        number_precision_col = fake.pydecimal(left_digits=8, right_digits=2, positive=True)
        number_integer_col = fake.pyint(min_value=0, max_value=99999999)

        # Generate float data - ensuring proper values
        float_col = fake.pyfloat(left_digits=5, right_digits=10, positive=True)
        float_precision_col = fake.pyfloat(left_digits=3, right_digits=2, positive=True)

        # Generate LONG column data (deprecated but still supported)
        long_col = fake.text(max_nb_chars=1000)  # Keep it reasonable for LONG

        # Date column
        date_col = fake.date_object()

        # Binary float and double
        binary_float_col = fake.pyfloat()
        binary_double_col = fake.pyfloat()

        # Timestamp columns
        timestamp_col = datetime.now()
        timestamp_precision_col = datetime.now()
        timestamp_tz_col = datetime.now().astimezone()
        timestamp_tz_precision = datetime.now().astimezone()

        # Generate INTERVAL data properly for Oracle
        # For YEAR TO MONTH intervals - create as timedelta representing months
        months_1 = random.randint(1, 1199)  # 1 to 99 years 11 months
        months_2 = random.randint(1, 99999)  # For precision version

        # For DAY TO SECOND intervals - use timedelta objects
        days = random.randint(1, 99)
        hours = random.randint(0, 23)
        minutes = random.randint(0, 59)
        seconds = random.randint(0, 59)
        microseconds = random.randint(0, 999999)

        interval_ds_col = timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
        interval_ds_precision = timedelta(days=days, hours=hours, minutes=minutes,
                                          seconds=seconds, microseconds=microseconds)

        # Fixed-length strings with proper padding
        char_col = fake.word()[:10].ljust(10)  # Ensure exactly 10 chars
        char_large_col = fake.text(max_nb_chars=2000)  # For CHAR_LARGE_COLUMN
        nchar_col = fake.word()[:10].ljust(10)  # Ensure exactly 10 chars
        nchar_large_col = fake.text(max_nb_chars=1000)  # For NCHAR_LARGE_COLUMN

        # SQL statement using Oracle's INTERVAL constructors for YEAR TO MONTH intervals
        sql = """INSERT INTO oracle_datatypes_demo (
                    varchar2_column, varchar2_large_column, nvarchar2_column, nvarchar2_large_column,
                    number_column, number_precision_column, number_integer_column,
                    float_column, float_precision_column, long_column,
                    date_column, binary_float_column, binary_double_column,
                    timestamp_column, timestamp_precision_column, timestamp_tz_column, timestamp_tz_precision,
                    interval_ym_column, interval_ym_precision, interval_ds_column, interval_ds_precision,
                    char_column, char_large_column, nchar_column, nchar_large_column
                 ) VALUES (
                    :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17,
                    NUMTOYMINTERVAL(:18, 'MONTH'), NUMTOYMINTERVAL(:19, 'MONTH'), :20, :21, :22, :23, :24, :25
                 )"""

        # Execute the insert with all parameters
        cur.execute(sql, (
            varchar2_col, varchar2_large_col, nvarchar2_col, nvarchar2_large_col,
            number_col, number_precision_col, number_integer_col,
            float_col, float_precision_col, long_col,
            date_col, binary_float_col, binary_double_col,
            timestamp_col, timestamp_precision_col, timestamp_tz_col, timestamp_tz_precision,
            months_1, months_2, interval_ds_col, interval_ds_precision,
            char_col, char_large_col, nchar_col, nchar_large_col
        ))

        return True

    except Exception as e:
        print(f"Error inserting into oracle_datatypes_demo: {e}")
        return False


# --- 5. REAL-TIME THREADS ---
def product_generator():
    """Continuously generates products at a defined interval."""
    conn = get_db_connection()
    cur = conn.cursor()
    while True:
        try:
            inserted_count = 0
            # Check if there are suppliers before trying to insert a product
            with list_lock:
                if not supplier_ids:
                    print("No suppliers available to generate products. Waiting...")
                    time.sleep(5)
                    continue

            for _ in range(CONFIG['products_per_interval']):
                new_product_id = insert_single_product(cur, conn)
                if new_product_id:
                    with list_lock:
                        product_ids.append(new_product_id)
                    inserted_count += 1
                    time.sleep(0.1)  # Small delay to avoid duplicate timestamps
            conn.commit()
            print(f"Generated {inserted_count} new products at {datetime.now()}")
        except Exception as e:
            conn.rollback()
            print(f"Error in product_generator: {e}")
        time.sleep(CONFIG['product_interval'])
    cur.close()
    conn.close()


def order_generator():
    """Continuously generates orders at a defined interval."""
    conn = get_db_connection()
    cur = conn.cursor()
    while True:
        try:
            with list_lock:
                if not customer_ids or not product_ids:
                    print(
                        f"Waiting for initial data to be generated... (Customers: {len(customer_ids)}, Products: {len(product_ids)})")
                    time.sleep(5)
                    continue

            inserted_count = 0
            for _ in range(CONFIG['orders_per_interval']):
                new_order_id = insert_single_order(cur, conn)
                if new_order_id:
                    inserted_count += 1
            conn.commit()
            print(f"Generated {inserted_count} new orders at {datetime.now()}")
        except Exception as e:
            conn.rollback()
            print(f"Error in order_generator: {e}")
        time.sleep(CONFIG['order_interval'])
    cur.close()
    conn.close()


# --- 6. INITIAL SETUP ---
def initial_setup():
    """Performs a one-time setup to truncate tables and populate initial data."""
    conn = None  # Initialize conn to None
    cur = None  # Initialize cur to None

    try:
        print("Starting initial data setup...")
        conn = get_db_connection()
        cur = conn.cursor()

        # Truncate tables and reset sequences for a fresh start (unless --no-truncate is specified)
        if not CONFIG['no_truncate']:
            truncate_and_reset_sequences(cur, conn)
        else:
            print("Skipping table truncation (--no-truncate specified)")

        # Skip initial data population if --no-initial-data is specified
        if CONFIG['no_initial_data']:
            print("Skipping initial data population (--no-initial-data specified)")
            return

        # --- Suppliers ---
        print(f"Generating {CONFIG['initial_suppliers']} suppliers...")
        for i in range(CONFIG['initial_suppliers']):
            supplier_ids.append(insert_single_supplier(cur, conn, supplier_index=i))
        conn.commit()  # Commit after suppliers

        # --- Employees ---
        print(f"Generating {CONFIG['initial_employees']} employees...")
        employee_ids.append(insert_single_employee(cur, conn, manager_id=None, job_title='CEO', employee_index=0))
        for i, job in enumerate(['Vet Technician', 'Groomer'], 1):
            employee_ids.append(
                insert_single_employee(cur, conn, manager_id=employee_ids[0], job_title=job, employee_index=i))
        for i in range(3, CONFIG['initial_employees']):
            employee_ids.append(insert_single_employee(cur, conn, manager_id=employee_ids[0], employee_index=i))
        conn.commit()  # Commit after employees

        # --- Customers ---
        print(f"Generating {CONFIG['initial_customers']} customers...")
        for i in range(CONFIG['initial_customers']):
            customer_ids.append(insert_single_customer(cur, conn, i))
        conn.commit()  # Commit after customers

        # --- Products ---
        print(f"Generating {CONFIG['initial_products']} products...")
        for _ in range(CONFIG['initial_products']):
            product_ids.append(insert_single_product(cur, conn))
        conn.commit()  # Commit after products

        # --- Pets ---
        print(f"Generating {CONFIG['initial_pets']} pets...")
        # Oracle check
        cur.execute(
            "SELECT product_id FROM products WHERE product_type = 'Pet' ORDER BY dbms_random.value FETCH FIRST :1 ROWS ONLY",
            (CONFIG['initial_pets'],))

        pet_product_ids = [row[0] for row in cur.fetchall()]
        if not pet_product_ids:
            # Fallback: create a pet product if none exist
            pet_product_ids.append(insert_single_product(cur, conn, product_type='Pet'))

        for prod_id in pet_product_ids:
            pet_ids.append(insert_single_pet(cur, conn, prod_id))
        conn.commit()  # Commit after pets

        # --- Pet Care Logs ---
        print(f"Generating {CONFIG['initial_care_logs']} initial pet care logs...")

        if not pet_ids or not employee_ids:
            print("Skipping pet_care_logs: Not enough pets or qualified employees to create initial logs.")
        else:
            care_employees = []
            # Corrected query with proper string formatting for Oracle
            placeholders = ', '.join([f':{i + 1}' for i in range(len(employee_ids))])
            query = f"SELECT employee_id FROM employees WHERE job_title IN ('Vet Technician', 'Groomer') AND employee_id IN ({placeholders})"
            # Pass employee_ids as a list of arguments
            cur.execute(query, employee_ids)
            care_employees = [row[0] for row in cur.fetchall()]

            if care_employees:
                for _ in range(CONFIG['initial_care_logs']):
                    pet_id = random.choice(pet_ids)
                    employee_id = random.choice(care_employees)
                    log_datetime = datetime.now() - timedelta(days=random.randint(0, 365))
                    activity_type = random.choice(care_log_activities)
                    notes = f"Performed {activity_type.lower()} for pet. Notes: {fake.sentence()}"

                    cur.execute(
                        """INSERT INTO pet_care_logs (pet_id, employee_id, log_datetime, activity_type, notes)
                           VALUES (:1, :2, :3, :4, :5)""",
                        (pet_id, employee_id, log_datetime, activity_type, notes)
                    )
            else:
                print("Skipping pet_care_logs: No qualified employees found.")

        # --- Oracle Datatypes Demo Table ---
        print(f"Generating {CONFIG['initial_datatypes_demo']} records for oracle_datatypes_demo...")
        for i in range(CONFIG['initial_datatypes_demo']):
            insert_single_datatypes_demo_record(cur, conn, i)

        conn.commit()  # Final commit for initial setup
        print("Initial data setup complete.")
    except oracledb.Error as e:  # Catch specific oracledb errors first
        if conn:
            conn.rollback()
        print(
            f"An Oracle database error occurred during initial setup: ORA-{getattr(e, 'code', 'N/A')}: {getattr(e, 'message', str(e))}")
    except Exception as e:  # Catch any other unexpected Python errors
        if conn:
            conn.rollback()
        print(f"An unexpected error occurred during initial setup: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


# --- 7. MAIN EXECUTION ---
if __name__ == "__main__":
    # Parse command line arguments
    args = parse_arguments()

    # Set global configuration from command line arguments
    CONFIG = {
        'host': args.host,
        'port': args.port,
        'service': args.service,
        'user': args.user,
        'password': args.password,
        'oracle_client_lib': args.oracle_client_lib,
        'initial_suppliers': args.initial_suppliers,
        'initial_employees': args.initial_employees,
        'initial_customers': args.initial_customers,
        'initial_products': args.initial_products,
        'initial_pets': args.initial_pets,
        'initial_care_logs': args.initial_care_logs,
        'initial_datatypes_demo': args.initial_datatypes_demo,
        'order_interval': args.order_interval,
        'product_interval': args.product_interval,
        'orders_per_interval': args.orders_per_interval,
        'products_per_interval': args.products_per_interval,
        'no_truncate': args.no_truncate,
        'no_initial_data': args.no_initial_data,
        'no_realtime': args.no_realtime,
        'setup_only': args.setup_only
    }

    print("Pet Store Data Generator for Oracle Database")
    print("=" * 50)
    print(f"Host: {CONFIG['host']}:{CONFIG['port']}")
    print(f"Service: {CONFIG['service']}")
    print(f"User: {CONFIG['user']}")
    print(f"Oracle Client: {CONFIG['oracle_client_lib']}")
    print(f"Initial Data: Suppliers={CONFIG['initial_suppliers']}, Employees={CONFIG['initial_employees']}, "
          f"Customers={CONFIG['initial_customers']}, Products={CONFIG['initial_products']}")
    if not CONFIG['no_realtime'] and not CONFIG['setup_only']:
        print(f"Real-time: Orders every {CONFIG['order_interval']}s ({CONFIG['orders_per_interval']} per batch), "
              f"Products every {CONFIG['product_interval']}s ({CONFIG['products_per_interval']} per batch)")
    print("=" * 50)

    # Run initial setup
    initial_setup()

    # Exit if setup-only mode
    if CONFIG['setup_only']:
        print("Setup complete. Exiting (--setup-only specified).")
        exit(0)

    # Skip real-time generation if --no-realtime is specified
    if CONFIG['no_realtime']:
        print("Skipping real-time generation (--no-realtime specified). Exiting.")
        exit(0)

    print("\nStarting real-time data generation threads...")
    # Create threads for continuous data generation
    product_thread = threading.Thread(target=product_generator, daemon=True)
    order_thread = threading.Thread(target=order_generator, daemon=True)

    # Start the threads
    product_thread.start()
    order_thread.start()

    # Keep the main thread alive to allow daemon threads to run
    print("Real-time generation started. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nReal-time generation stopped.")
