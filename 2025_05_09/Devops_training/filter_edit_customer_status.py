import json
import csv

customers = {}
with open('customer_status.csv', newline='') as f:
    reader = csv.DictReader(f)
    for row in reader:
        email = row.get('email', '')
        status = row.get('status', '')
        customers[email] = {
            'status': status,
            'last_order_value': 0.0
        }

with open('ny_high_value.csv', 'w', newline='') as csv_out_file, open('ny_high_value.jsonl', 'w') as jsonl_out_file, open('orders.jsonl') as orders_file:
    writer = csv.DictWriter(csv_out_file, fieldnames=['order_id', 'customer_name', 'email', 'total_value', 'item_count'])
    writer.writeheader()
    for line in orders_file:
        order = json.loads(line)

        order_id      = order.get('order_id', '')
        customer_info = order.get('customer', {})
        email         = customer_info.get('email', '')
        location      = customer_info.get('location', '')
        name          = customer_info.get('name', '')

        items_list    = order.get('items', [])
        total_value = sum(item.get('price', 0.0) * item.get('quantity', 0) for item in items_list)
        item_count = sum(item.get('quantity', 0) for item in items_list)

        status = customers.get(email, {}).get('status', '')

        if location == 'NY' and total_value >= 1000.0 and status == 'active':
            row = {
                'order_id': order_id,
                'customer_name': name,
                'email': email,
                'total_value': f"{total_value:.2f}",
                'item_count': item_count
            }
            writer.writerow(row)

            order['filtered_reason'] = "NY, total value >= $1000, active"
            customers[email]['last_order_value'] = total_value
            jsonl_out_file.write(json.dumps(order) + '\n')

with open('customer_status.csv', 'w', newline='') as f_out:
    writer = csv.DictWriter(f_out, fieldnames=['email', 'status', 'last_order_value'])
    writer.writeheader()
    for email, info in customers.items():
        status = info.get('status', '')
        last_value = info.get('last_order_value', 0.0)
        writer.writerow({
            'email': email,
            'status': status,
            'last_order_value': f"{last_value:.2f}"
        })







