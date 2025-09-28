## Import libraries
import polars as pl
from src import data_loader

## Read datafiles
# Load orders dataset
orders = data_loader.get_orders()

# Load order_items dataset
items = data_loader.get_items()

## Extract monthly sales
# Join tables
print("Joining tables...")
sales = items.join(other = orders,
                  on = "order_id",
                  how = "left")
# Add month and year
sales = sales.with_columns(
    pl.col("order_purchase_timestamp").dt.month().alias("month"),
    pl.col("order_purchase_timestamp").dt.year().alias("year")
)
# Sum sales for each month
print("Calculating sales for each month...")
monthly_sales = (sales.group_by(["year", "month"])
                      .agg(pl.col("freight_value")
                      .sum()
                      .alias("sales")))

## Export sales to json
print("Writing results into json...")
monthly_sales.write_json("kpis/sales.json")
