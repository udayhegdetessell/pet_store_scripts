Usage:

./setup_pet_store.sh \
  --host localhost \
  --port 1521 \
  --service MYSERVICE \
  --user dbuser \
  --password "MyPassword123!" \
  --base-dir /opt \
  --initial-customers 15000 \
  --initial-products 8000 \
  --order-interval 2 \
  --orders-per-interval 100 \
  --drop-existing

./setup_pet_store.sh --help
