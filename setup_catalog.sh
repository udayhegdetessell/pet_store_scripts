#!/bin/bash

# Catalog Demo Setup Script
# This script sets up the catalog demo environment with Oracle client and Python scripts

set -e # Exit on any error

# Default values
DEFAULT_HOST="127.0.0.1"
DEFAULT_PORT="1521"
DEFAULT_ORACLE_CLIENT_LIB="$HOME/catalog_demo/oracle/instantclient_23_9"
DEFAULT_CATALOG_ROWS="1000"
DEFAULT_INVENTORY_ROWS="2000"
DEFAULT_ITEMS_ROWS="5000"
DEFAULT_BASE_DIR="$HOME"

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --host HOST             Database host (default: $DEFAULT_HOST)"
    echo "  --port PORT             Database port (default: $DEFAULT_PORT)"
    echo "  --service SERVICE       Database service name (required)"
    echo "  --user USER             Database user (required)"
    echo "  --password PASSWORD     Database password (required)"
    echo "  --oracle-client-lib PATH    Oracle client library path (default: $DEFAULT_ORACLE_CLIENT_LIB)"
    echo "  --catalog-rows NUM          Number of catalog records (default: $DEFAULT_CATALOG_ROWS)"
    echo "  --inventory-rows NUM        Number of inventory records (default: $DEFAULT_INVENTORY_ROWS)"
    echo "  --items-rows NUM            Number of items records (default: $DEFAULT_ITEMS_ROWS)"
    echo "  --base-dir PATH         Base directory path (default: $DEFAULT_BASE_DIR)"
    echo "  --drop-existing         Drop existing tables (flag)"
    echo "  --no-truncate           Skip truncating tables (flag)"
    echo "  --verbose               Enable verbose output (flag)"
    echo "  --help                  Show this help message"
    echo ""
    echo "Example:"
    echo "  $0 --service ORCL --user master --password 'Tessell123ZX#' --host localhost --catalog-rows 1000"
}

# Initialize variables with defaults
HOST="$DEFAULT_HOST"
PORT="$DEFAULT_PORT"
SERVICE=""
USER=""
PASSWORD=""
ORACLE_CLIENT_LIB="$DEFAULT_ORACLE_CLIENT_LIB"
CATALOG_ROWS="$DEFAULT_CATALOG_ROWS"
INVENTORY_ROWS="$DEFAULT_INVENTORY_ROWS"
ITEMS_ROWS="$DEFAULT_ITEMS_ROWS"
BASE_DIR="$DEFAULT_BASE_DIR"
DROP_EXISTING=""
NO_TRUNCATE=""
VERBOSE=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --host)
            HOST="$2"
            shift 2
            ;;
        --port)
            PORT="$2"
            shift 2
            ;;
        --service)
            SERVICE="$2"
            shift 2
            ;;
        --user)
            USER="$2"
            shift 2
            ;;
        --password)
            PASSWORD="$2"
            shift 2
            ;;
        --oracle-client-lib)
            ORACLE_CLIENT_LIB="$2"
            shift 2
            ;;
        --catalog-rows)
            CATALOG_ROWS="$2"
            shift 2
            ;;
        --inventory-rows)
            INVENTORY_ROWS="$2"
            shift 2
            ;;
        --items-rows)
            ITEMS_ROWS="$2"
            shift 2
            ;;
        --base-dir)
            BASE_DIR="$2"
            shift 2
            ;;
        --drop-existing)
            DROP_EXISTING="--drop-existing"
            shift
            ;;
        --no-truncate)
            NO_TRUNCATE="--no-truncate"
            shift
            ;;
        --verbose)
            VERBOSE="--verbose"
            shift
            ;;
        --help)
            show_usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Validate required parameters
if [[ -z "$SERVICE" ]]; then
    echo "Error: --service is required"
    show_usage
    exit 1
fi

if [[ -z "$USER" ]]; then
    echo "Error: --user is required"
    show_usage
    exit 1
fi

if [[ -z "$PASSWORD" ]]; then
    echo "Error: --password is required"
    show_usage
    exit 1
fi

echo "Starting Catalog Demo Setup..."
echo "Configuration:"
echo "  Host: $HOST"
echo "  Port: $PORT"
echo "  Service: $SERVICE"
echo "  User: $USER"
echo "  Base Directory: $BASE_DIR"
echo "  Catalog Rows: $CATALOG_ROWS"
echo "  Inventory Rows: $INVENTORY_ROWS"
echo "  Items Rows: $ITEMS_ROWS"
echo ""

# Create main directory
echo "Creating catalog_demo directory..."
cd "$BASE_DIR"
sudo mkdir -p catalog_demo
sudo chmod 777 catalog_demo
cd catalog_demo

# Create oracle directory and download Oracle client
echo "Setting up Oracle Instant Client..."
sudo mkdir -p oracle
sudo wget https://download.oracle.com/otn_software/linux/instantclient/2390000/instantclient-basiclite-linux.x64-23.9.0.25.07.zip
sudo unzip instantclient-basiclite-linux.x64-23.9.0.25.07.zip -d oracle/
sudo chmod 777 oracle
cd oracle
sudo chmod 777 instantclient_23_9
sudo chmod 777 META-INF

# Set environment variables
echo "Setting up environment variables..."
export ORACLE_HOME="$ORACLE_CLIENT_LIB"
export LD_LIBRARY_PATH=$ORACLE_HOME:$LD_LIBRARY_PATH

# Add environment variables to current session and .bashrc for persistence
echo "export ORACLE_HOME=\"$ORACLE_CLIENT_LIB\"" >> ~/.bashrc
echo "export LD_LIBRARY_PATH=\$ORACLE_HOME:\$LD_LIBRARY_PATH" >> ~/.bashrc

# Go back to main directory
cd "$BASE_DIR/catalog_demo"

# Install Python dependencies
echo "Installing Python dependencies..."
pip install faker
pip install oracledb

# Download Python script from GitHub repository
echo "Downloading Python script from GitHub repository..."
wget https://raw.githubusercontent.com/udayhegdetessell/pet_store_scripts/main/create_catalog_inventory.py

# Make script executable (optional)
chmod +x create_catalog_inventory.py

echo "Setup completed successfully!"
echo "Environment variables set:"
echo "ORACLE_HOME: $ORACLE_HOME"
echo "LD_LIBRARY_PATH: $LD_LIBRARY_PATH"

# Execute the script
echo ""
echo "Executing Python script..."

echo "Creating catalog, inventory, and store_items tables with data..."
python3 create_catalog_inventory.py \
  --host "$HOST" \
  --port "$PORT" \
  --service "$SERVICE" \
  --user "$USER" \
  --password "$PASSWORD" \
  --oracle-client-lib "$ORACLE_CLIENT_LIB" \
  --catalog-rows "$CATALOG_ROWS" \
  --inventory-rows "$INVENTORY_ROWS" \
  --items-rows "$ITEMS_ROWS" \
  $DROP_EXISTING \
  $NO_TRUNCATE \
  $VERBOSE

echo ""
echo "Catalog Demo setup and execution completed successfully!"
echo "Tables created: catalog, inventory, store_items"
echo "All data has been generated and committed to the database."
