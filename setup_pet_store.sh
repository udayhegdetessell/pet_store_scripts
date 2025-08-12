#!/bin/bash

# Pet Store Demo Setup Script
# This script sets up the pet store demo environment with Oracle client and Python scripts

set -e  # Exit on any error

# Default values
DEFAULT_HOST="127.0.0.1"
DEFAULT_PORT="1521"
DEFAULT_SERVICE="PDBORCL56"
DEFAULT_USER="master"
DEFAULT_PASSWORD="Tessell123ZX#"
DEFAULT_ORACLE_CLIENT_LIB="/home/azureuser/pet_store_demo/oracle/instantclient_23_9"
DEFAULT_INITIAL_CUSTOMERS="10000"
DEFAULT_INITIAL_PRODUCTS="5000"
DEFAULT_ORDER_INTERVAL="1"
DEFAULT_ORDERS_PER_INTERVAL="50"
DEFAULT_PRODUCT_INTERVAL="2"
DEFAULT_PRODUCTS_PER_INTERVAL="25"
DEFAULT_INITIAL_DATATYPES_DEMO="10"
DEFAULT_BASE_DIR="/home/azureuser"

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --host HOST                    Database host (default: $DEFAULT_HOST)"
    echo "  --port PORT                    Database port (default: $DEFAULT_PORT)"
    echo "  --service SERVICE              Database service name (default: $DEFAULT_SERVICE)"
    echo "  --user USER                    Database user (default: $DEFAULT_USER)"
    echo "  --password PASSWORD            Database password (default: $DEFAULT_PASSWORD)"
    echo "  --oracle-client-lib PATH       Oracle client library path (default: $DEFAULT_ORACLE_CLIENT_LIB)"
    echo "  --initial-customers NUM        Initial number of customers (default: $DEFAULT_INITIAL_CUSTOMERS)"
    echo "  --initial-products NUM         Initial number of products (default: $DEFAULT_INITIAL_PRODUCTS)"
    echo "  --order-interval SECONDS       Order generation interval (default: $DEFAULT_ORDER_INTERVAL)"
    echo "  --orders-per-interval NUM      Orders per interval (default: $DEFAULT_ORDERS_PER_INTERVAL)"
    echo "  --product-interval SECONDS     Product generation interval (default: $DEFAULT_PRODUCT_INTERVAL)"
    echo "  --products-per-interval NUM    Products per interval (default: $DEFAULT_PRODUCTS_PER_INTERVAL)"
    echo "  --initial-datatypes-demo NUM   Initial datatypes demo records (default: $DEFAULT_INITIAL_DATATYPES_DEMO)"
    echo "  --base-dir PATH                Base directory path (default: $DEFAULT_BASE_DIR)"
    echo "  --drop-existing                Drop existing schema (flag)"
    echo "  --help                         Show this help message"
    echo ""
    echo "Example:"
    echo "  $0 --host localhost --user myuser --password mypass --initial-customers 5000"
}

# Initialize variables with defaults
HOST="$DEFAULT_HOST"
PORT="$DEFAULT_PORT"
SERVICE="$DEFAULT_SERVICE"
USER="$DEFAULT_USER"
PASSWORD="$DEFAULT_PASSWORD"
ORACLE_CLIENT_LIB="$DEFAULT_ORACLE_CLIENT_LIB"
INITIAL_CUSTOMERS="$DEFAULT_INITIAL_CUSTOMERS"
INITIAL_PRODUCTS="$DEFAULT_INITIAL_PRODUCTS"
ORDER_INTERVAL="$DEFAULT_ORDER_INTERVAL"
ORDERS_PER_INTERVAL="$DEFAULT_ORDERS_PER_INTERVAL"
PRODUCT_INTERVAL="$DEFAULT_PRODUCT_INTERVAL"
PRODUCTS_PER_INTERVAL="$DEFAULT_PRODUCTS_PER_INTERVAL"
INITIAL_DATATYPES_DEMO="$DEFAULT_INITIAL_DATATYPES_DEMO"
BASE_DIR="$DEFAULT_BASE_DIR"
DROP_EXISTING=""

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
        --initial-customers)
            INITIAL_CUSTOMERS="$2"
            shift 2
            ;;
        --initial-products)
            INITIAL_PRODUCTS="$2"
            shift 2
            ;;
        --order-interval)
            ORDER_INTERVAL="$2"
            shift 2
            ;;
        --orders-per-interval)
            ORDERS_PER_INTERVAL="$2"
            shift 2
            ;;
        --product-interval)
            PRODUCT_INTERVAL="$2"
            shift 2
            ;;
        --products-per-interval)
            PRODUCTS_PER_INTERVAL="$2"
            shift 2
            ;;
        --initial-datatypes-demo)
            INITIAL_DATATYPES_DEMO="$2"
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

echo "Starting Pet Store Demo Setup..."
echo "Configuration:"
echo "  Host: $HOST"
echo "  Port: $PORT"
echo "  Service: $SERVICE"
echo "  User: $USER"
echo "  Base Directory: $BASE_DIR"
echo "  Initial Customers: $INITIAL_CUSTOMERS"
echo "  Initial Products: $INITIAL_PRODUCTS"
echo ""

# Create main directory
echo "Creating pet_store_demo directory..."
cd "$BASE_DIR"
sudo mkdir -p pet_store_demo
sudo chmod 777 pet_store_demo
cd pet_store_demo

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
cd "$BASE_DIR/pet_store_demo"

# Install Python dependencies
echo "Installing Python dependencies..."
pip install faker
pip install oracledb

# Download Python scripts from GitHub repository
echo "Downloading Python scripts from GitHub repository..."
wget https://raw.githubusercontent.com/udayhegdetessell/pet_store_scripts/main/create_schema.py
wget https://raw.githubusercontent.com/udayhegdetessell/pet_store_scripts/main/generate_data.py
wget https://raw.githubusercontent.com/udayhegdetessell/pet_store_scripts/main/row_count.py

# Make scripts executable (optional)
chmod +x create_schema.py generate_data.py row_count.py

echo "Setup completed successfully!"
echo "Environment variables set:"
echo "ORACLE_HOME: $ORACLE_HOME"
echo "LD_LIBRARY_PATH: $LD_LIBRARY_PATH"

# Execute the scripts
echo ""
echo "Executing Python scripts..."

echo "1. Creating schema..."
python3 create_schema.py \
  --host "$HOST" \
  --port "$PORT" \
  --service "$SERVICE" \
  --user "$USER" \
  --password "$PASSWORD" \
  --oracle-client-lib "$ORACLE_CLIENT_LIB" \
  $DROP_EXISTING

echo "2. Generating data..."
python3 generate_data.py \
  --host "$HOST" \
  --port "$PORT" \
  --service "$SERVICE" \
  --user "$USER" \
  --password "$PASSWORD" \
  --oracle-client-lib "$ORACLE_CLIENT_LIB" \
  --initial-customers "$INITIAL_CUSTOMERS" \
  --initial-products "$INITIAL_PRODUCTS" \
  --order-interval "$ORDER_INTERVAL" \
  --orders-per-interval "$ORDERS_PER_INTERVAL" \
  --product-interval "$PRODUCT_INTERVAL" \
  --products-per-interval "$PRODUCTS_PER_INTERVAL" \
  --initial-datatypes-demo "$INITIAL_DATATYPES_DEMO"

echo "3. Checking row counts..."
python3 row_count.py \
  --host "$HOST" \
  --port "$PORT" \
  --service "$SERVICE" \
  --user "$USER" \
  --password "$PASSWORD" \
  --oracle-client-lib "$ORACLE_CLIENT_LIB"

echo ""
echo "Pet Store Demo setup and execution completed successfully!"
echo "All scripts have been executed in the correct order."