## Import libraries
import polars as pl
from src import data_loader
from src import sales
from src import operations
from src import order_stats

## Read datafiles
# Load orders dataset
orders = data_loader.get_orders()

# Load order_items dataset
items = data_loader.get_items()

## Extract kpis
# Sales
monthly_sales = sales.get_sales(items, orders)
# Operations
delivery = operations.get_operations(orders)
# Customers
average_order = order_stats.get_order_stats(items)

## Write kpis into jsonfiles 
# Sales
print("Writing sales into json...")
monthly_sales = monthly_sales.filter((pl.col("year")==2018)&
                                     (pl.col("month")!="September"))
monthly_sales.write_json("kpis/sales.json")
# Operations
print("Writing operations into json...")
operations.write_json(delivery, path = "kpis/operations.json")
# Orders
print("Writing statistics about order in json...")
order_stats.write_json(average_order, path = "kpis/order_stat.json")
