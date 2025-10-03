# Import liblaries
import polars as pl

def get_sales(items, orders):
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
                          .alias("sales"))
                          .sort(["year", "month"]))
    # Change month specification from numeric into words
    monthly_sales = (monthly_sales.with_columns(
                         pl.date(2018, pl.col("month"), 1)
                         .dt.strftime("%B")
                         .alias("month")))
    # Add monthly change in sales
    monthly_sales = monthly_sales.with_columns(
            pl.col("sales").diff()
            .alias("growth"))

    return monthly_sales
