## Import libraries
import polars as pl

## Read datafiles
print("Loading orders data...")
orders = pl.read_csv(source = "data/orders.csv",
           schema_overrides = {"order_purchase_timestamp": pl.Datetime,
                               "order_approved_at": pl.Datetime,
                               "order_delivered_carrier_date": pl.Datetime,
                               "order_delivered_customer_date": pl.Datetime,
                               "order_estimated_delivery_date": pl.Datetime})

print("Loading items data...")
items = pl.read_csv(source = "data/order_items.csv",
                    schema_overrides = {"shipping_limit_date": pl.Datetime})

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
