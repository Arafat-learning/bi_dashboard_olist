# import libarary
import json
import polars as pl

def get_operations(orders):
    operation_kpis = {}

    # filter delivered orders as analysis will be performed on those
    orders = orders.filter(pl.col("order_status")=="delivered")
    
    # calculate total delivered orders
    print("Calculating total delivered orders...")
    delivered_orders = orders.get_column("order_status").count()

    # calculate delivery time in hours
    orders = orders.with_columns(
        (pl.col("order_delivered_customer_date") - pl.col("order_approved_at"))
        .dt.total_hours().alias("dur")
    )
    # calculate average delivery time
    delivery_time = orders.get_column("dur").mean()
    # convert delivery time in hours to days
    delivery_time = round(delivery_time/24,1)
    
    # storing kpi values into dictionary
    operation_kpis["total_deliveries"] = delivered_orders
    operation_kpis["delivery_time"] = delivery_time

    return operation_kpis

def write_json(kpis, path):
    
    with open(path, "w") as file:
        json.dump(kpis, file)
