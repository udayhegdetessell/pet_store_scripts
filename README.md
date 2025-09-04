# Pet Store & Catalog Demo Setup Scripts

This repository contains shell scripts that automate the complete setup and execution of demo environments with Oracle Database connectivity. It handles Oracle Instant Client installation, Python dependencies, script downloads, and database operations.

## Available Demos

1. **Pet Store Demo** - Complete e-commerce pet store with customers, products, orders, and pets
2. **Catalog Demo** - Inventory management system with catalog, inventory, and store items

## Prerequisites

- Linux environment (tested on Ubuntu/CentOS)
- `sudo` privileges
- Internet connectivity for downloading Oracle Instant Client and Python scripts
- Python 3.x installed
- `pip` package manager
- Access to an Oracle Database instance

## Quick Start

### Pet Store Demo

1. **Download and make the script executable:**
   ```bash
   wget https://raw.githubusercontent.com/udayhegdetessell/pet_store_scripts/main/setup_pet_store.sh && chmod +x setup_pet_store.sh
   ```

2. **Run with default settings:**
   ```bash
   ./setup_pet_store.sh --service ORCL --user master --password 12345678
   ```

### Catalog Demo

1. **Download and make the script executable:**
   ```bash
   wget https://raw.githubusercontent.com/udayhegdetessell/pet_store_scripts/main/setup_catalog.sh && chmod +x setup_catalog.sh
   ```

2. **Run with default settings:**
   ```bash
   ./setup_catalog.sh --service ORCL --user master --password 12345678
   ```

## What the Scripts Do

### Pet Store Demo (`setup_pet_store.sh`)

**Setup Phase:**
1. Creates `pet_store_demo` directory structure
2. Downloads Oracle Instant Client (23.9.0.25.07)
3. Configures Oracle environment variables
4. Installs Python dependencies (`faker`, `oracledb`)
5. Downloads Python scripts from GitHub repository

**Execution Phase:**
1. **Schema Creation** - Creates database schema and tables
2. **Data Generation** - Populates database with sample data
3. **Row Count Verification** - Validates data creation

**Tables Created:**
- `customers` - Customer information
- `suppliers` - Supplier details
- `employees` - Employee records
- `products` - Product catalog
- `pets` - Pet information
- `orders` - Order transactions
- `order_items` - Order line items
- `pet_care_logs` - Pet care history
- `oracle_datatypes_demo` - Oracle data types demonstration

### Catalog Demo (`setup_catalog.sh`)

**Setup Phase:**
1. Creates `catalog_demo` directory structure
2. Downloads Oracle Instant Client (23.9.0.25.07)
3. Configures Oracle environment variables
4. Installs Python dependencies (`faker`, `oracledb`)
5. Downloads Python script from GitHub repository

**Execution Phase:**
1. **Table Creation** - Creates catalog, inventory, and store_items tables
2. **Data Generation** - Populates tables with realistic fake data

**Tables Created:**
- `catalog` - Product catalog information
- `inventory` - Inventory management with warehouse locations
- `store_items` - Individual product items with detailed attributes

## Command Line Options

### Pet Store Demo (`setup_pet_store.sh`)

| Option | Description | Default Value |
|--------|-------------|---------------|
| `--host HOST` | Database host address | `127.0.0.1` |
| `--port PORT` | Database port number | `1521` |
| `--service SERVICE` | Database service name | `ORCL` |
| `--user USER` | Database username | `master` |
| `--password PASSWORD` | Database password | `12345678` |
| `--oracle-client-lib PATH` | Oracle client library path | `/home/azureuser/pet_store_demo/oracle/instantclient_23_9` |
| `--initial-customers NUM` | Initial number of customers | `10000` |
| `--initial-products NUM` | Initial number of products | `5000` |
| `--order-interval SECONDS` | Order generation interval | `1` |
| `--orders-per-interval NUM` | Orders generated per interval | `50` |
| `--product-interval SECONDS` | Product generation interval | `2` |
| `--products-per-interval NUM` | Products generated per interval | `25` |
| `--initial-datatypes-demo NUM` | Initial datatypes demo records | `10` |
| `--base-dir PATH` | Base installation directory | `/home/azureuser` |
| `--drop-existing` | Drop existing schema (flag) | Not set |
| `--help` | Show usage information | - |

### Catalog Demo (`setup_catalog.sh`)

| Option | Description | Default Value |
|--------|-------------|---------------|
| `--host HOST` | Database host address | `127.0.0.1` |
| `--port PORT` | Database port number | `1521` |
| `--service SERVICE` | Database service name | `ORCL` |
| `--user USER` | Database username | `master` |
| `--password PASSWORD` | Database password | `12345678` |
| `--oracle-client-lib PATH` | Oracle client library path | `/home/azureuser/catalog_demo/oracle/instantclient_23_9` |
| `--catalog-rows NUM` | Number of catalog records | `1000` |
| `--inventory-rows NUM` | Number of inventory records | `2000` |
| `--items-rows NUM` | Number of items records | `5000` |
| `--base-dir PATH` | Base installation directory | `/home/azureuser` |
| `--drop-existing` | Drop existing tables (flag) | Not set |
| `--no-truncate` | Skip truncating tables (flag) | Not set |
| `--verbose` | Enable verbose output (flag) | Not set |
| `--help` | Show usage information | - |

## Usage Examples

### Pet Store Demo

**Run with all defaults:**
```bash
./setup_pet_store.sh --service ORCL --user master --password 12345678
```

**Show help information:**
```bash
./setup_pet_store.sh --help
```

### Catalog Demo

**Run with all defaults:**
```bash
./setup_catalog.sh --service ORCL --user master --password 12345678
```

**Show help information:**
```bash
./setup_catalog.sh --help
```

### Database Connection Examples

**Connect to remote database:**
```bash
./setup_pet_store.sh \
  --host prod-oracle.company.com \
  --port 1521 \
  --service PRODPDB \
  --user petstore_user \
  --password "SecurePassword123!"
```

**Connect to localhost with custom service:**
```bash
./setup_pet_store.sh \
  --host localhost \
  --service XEPDB1 \
  --user hr \
  --password hr
```

### Data Generation Examples

**Generate smaller dataset:**
```bash
./setup_pet_store.sh \
  --initial-customers 1000 \
  --initial-products 500 \
  --orders-per-interval 10
```

**Generate large dataset with faster intervals:**
```bash
./setup_pet_store.sh \
  --initial-customers 50000 \
  --initial-products 20000 \
  --order-interval 0.5 \
  --orders-per-interval 100 \
  --product-interval 1 \
  --products-per-interval 50
```

### Advanced Configuration

**Custom installation directory with schema drop:**
```bash
./setup_pet_store.sh \
  --base-dir /opt/demos \
  --drop-existing \
  --host db.internal.com \
  --service TESTPDB \
  --user demo_user \
  --password "DemoPass2024#"
```

**Keep existing schema AND data
```bash
./setup_pet_store.sh \
  --host localhost \
  --port 1521 \
  --service orcl \
  --user master \
  --password 12345678 \
  --base-dir /home/azureuser \
  --oracle-client-lib /home/azureuser/pet_store_demo/oracle/instantclient_23_9 \
  --initial-customers 25000 \
  --initial-products 12000 \
  --no-truncate
```

**Complete custom configuration:**
```bash
./setup_pet_store.sh \
  --host oracle-db.example.com \
  --port 1522 \
  --service CUSTOMPDB \
  --user pet_admin \
  --password "CustomPass123!" \
  --base-dir /home/azureuser \
  --oracle-client-lib /home/azureuser/pet_store_demo/oracle/instantclient_23_9 \
  --initial-customers 25000 \
  --initial-products 12000 \
  --order-interval 2 \
  --orders-per-interval 75 \
  --product-interval 3 \
  --products-per-interval 40 \
  --initial-datatypes-demo 50 \
  --drop-existing
```

### Catalog Demo Examples

**Custom row counts:**
```bash
./setup_catalog.sh \
  --host localhost \
  --port 1521 \
  --service orcl \
  --user master \
  --password 12345678 \
  --catalog-rows 1000 \
  --inventory-rows 2000 \
  --items-rows 5000
```

**Drop existing tables and start fresh:**
```bash
./setup_catalog.sh \
  --host localhost \
  --port 1521 \
  --service orcl \
  --user master \
  --password 12345678 \
  --catalog-rows 1000 \
  --inventory-rows 2000 \
  --items-rows 5000 \
  --drop-existing
```

**Preserve existing data (append mode):**
```bash
./setup_catalog.sh \
  --host localhost \
  --port 1521 \
  --service orcl \
  --user master \
  --password 12345678 \
  --catalog-rows 1000 \
  --inventory-rows 2000 \
  --items-rows 5000 \
  --no-truncate
```

**Verbose output:**
```bash
./setup_catalog.sh \
  --host localhost \
  --port 1521 \
  --service orcl \
  --user master \
  --password 12345678 \
  --catalog-rows 1000 \
  --inventory-rows 2000 \
  --items-rows 5000 \
  --verbose
```


## Environment Variables

The script automatically sets up the following environment variables:

```bash
export ORACLE_HOME="/path/to/catalog_demo/oracle/instantclient_23_9"
export LD_LIBRARY_PATH="$ORACLE_HOME:$LD_LIBRARY_PATH"
```

These are added to `~/.bashrc` for persistence across sessions.

## Directory Structure

### Pet Store Demo
After successful execution, the following structure is created:

```
{base-dir}/pet_store_demo/
├── oracle/
│   ├── instantclient_23_9/          # Oracle Instant Client files
│   └── META-INF/
├── create_schema.py                  # Schema creation script
├── generate_data.py                  # Data generation script
├── row_count.py                      # Row count verification script
└── instantclient-basiclite-linux.x64-23.9.0.25.07.zip
```

### Catalog Demo
After successful execution, the following structure is created:

```
{base-dir}/catalog_demo/
├── oracle/
│   ├── instantclient_23_9/          # Oracle Instant Client files
│   └── META-INF/
├── create_catalog_inventory.py       # Catalog demo script
└── instantclient-basiclite-linux.x64-23.9.0.25.07.zip
```

## Troubleshooting

### Common Issues

**Permission denied errors:**
```bash
# Ensure script is executable
chmod +x setup_pet_store.sh

# Check sudo access
sudo -l
```

**Oracle connection errors:**
```bash
# Verify database connectivity
telnet your-db-host 1521

# Check Oracle client installation
ls -la /path/to/oracle/instantclient_23_9/
```

**Python dependency errors:**
```bash
# Upgrade pip
pip install --upgrade pip

# Install dependencies manually
pip install faker oracledb
```

### Log Output

The script provides detailed progress information:
- Setup phase progress
- Configuration summary
- Python script execution status
- Success/failure messages

### Recovery

If the script fails partway through:

1. **Clean installation:**
   ```bash
   sudo rm -rf pet_store_demo
   ./setup_pet_store.sh
   ```

2. **Skip setup, run scripts only:**
   ```bash
   cd pet_store_demo
   # Run individual Python scripts manually
   ```

## Python Scripts

### Pet Store Demo Scripts
The `setup_pet_store.sh` script downloads and executes three Python scripts:

1. **`create_schema.py`** - Creates database schema and tables
2. **`generate_data.py`** - Generates sample data with configurable parameters
3. **`row_count.py`** - Verifies data creation by counting rows

### Catalog Demo Scripts
The `setup_catalog.sh` script downloads and executes one Python script:

1. **`create_catalog_inventory.py`** - Creates catalog, inventory, and store_items tables with fake data

### Standalone Usage
You can also run the Python scripts directly:

**Pet Store Demo:**
```bash
python3 create_schema.py --host localhost --service ORCL --user master --password 12345678
python3 generate_data.py --host localhost --service ORCL --user master --password 12345678
python3 row_count.py --host localhost --service ORCL --user master --password 12345678
```

**Catalog Demo:**
```bash
python3 create_catalog_inventory.py \
  --host localhost \
  --service ORCL \
  --user master \
  --password 12345678 \
  --catalog-rows 1000 \
  --inventory-rows 2000 \
  --items-rows 5000
```
