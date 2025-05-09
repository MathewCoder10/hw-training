## Task
1. Write a Python script `filter_edit_customer_status.py` that:
   - Reads `orders.jsonl` .
   - Reads `customer_status.csv`  to get customer statuses.
   - Filters orders where:
     - The customer’s `location` is "NY".
     - The total order value (sum of `price * quantity` for all items) is at least $1000.
     - The customer’s `status` is "active" (matched by `email` from `customer_status.csv`).
   - Updates `customer_status.csv` by adding a `last_order_value` column, setting it to the total value of the filtered order for matching customers (0.0 if no filtered orders).
   - Writes the filtered orders to `ny_high_value.csv` with columns: `order_id`, `customer_name`, `email`, `total_value`, `item_count`.
   - Writes the filtered orders to `ny_high_value.jsonl`, preserving the original structure and adding a `filtered_reason` field (e.g., "NY, total value >= $1000, active").


## Deliverables
- `filter_edit_customer_status.py`
- Updated `customer_status.csv`
- `ny_high_value.csv`
- `ny_high_value.jsonl`

## Sample Output
**`ny_high_value.csv`**:
```csv
order_id,customer_name,email,total_value,item_count
1,Alice Smith,alice.smith@example.com,1059.97,2
```

**`ny_high_value.jsonl`**:
```
{"order_id": 1, "customer": {"name": "Alice Smith", "email": "alice.smith@example.com", "location": "NY"}, "items": [{"product": "Laptop", "price": 999.99, "quantity": 1}, {"product": "Mouse", "price": 29.99, "quantity": 2}], "order_date": "2025-04-01", "filtered_reason": "NY, total value >= $1000, active"}
```

**Updated `customer_status.csv` (partial)**:
```csv
email,status,last_order_value
alice.smith@example.com,active,1059.97
bob.johnson@example.com,inactive,0.0
charlie.brown@example.com,active,0.0
```