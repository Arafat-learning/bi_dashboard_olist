## Import libraries
import polars as pl

def get_orders():
    print("Loading orders data...")
    orders = pl.read_csv(source = "data/orders.csv",
               schema_overrides = {"order_purchase_timestamp": pl.Datetime,
                                   "order_approved_at": pl.Datetime,
                                   "order_delivered_carrier_date": pl.Datetime,
                                   "order_delivered_customer_date": pl.Datetime,
                                   "order_estimated_delivery_date": pl.Datetime})
    return orders

def get_items():
    print("Loading items data...")
    items = pl.read_csv(source = "data/order_items.csv",
                        schema_overrides = {"shipping_limit_date": pl.Datetime})
    return items
