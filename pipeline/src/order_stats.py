# import liblaries
import json
import polars as pl

def get_order_stats(items):
    order_stats = {}

    # Average spending per order
    print("Calculating average spending per order...")
    average_order_spending = (items.group_by("order_id")
                                   .agg(pl.col("price").sum())
                                   .get_column("price").mean())

    # Average items per order
    print("Calculating average items bought per order...")
    average_item_in_order = (items.group_by("order_id")
                                  .agg(pl.col("order_item_id").count())
                                  .get_column("order_item_id").mean())

    # store kpis in dictionary
    order_stats["order_spending"] = average_order_spending
    order_stats["avg_item_count"] = average_item_in_order

    return order_stats

def write_json(kpis, path):
    with open(path, "w") as file:
        json.dump(kpis, file)
