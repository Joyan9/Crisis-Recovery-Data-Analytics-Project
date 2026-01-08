"""
DLT pipeline to load CSV data from Datasets folder into BigQuery
"""

import os
import pandas as pd
import dlt


@dlt.resource(name="dim_customer", write_disposition="replace")
def load_dim_customer():
    """Load dim_customer.csv into BigQuery"""
    df = pd.read_csv("/workspaces/Crisis-Recovery-Data-Analytics-Project/Datasets/dim_customer.csv")
    yield df


@dlt.resource(name="dim_delivery_partner", write_disposition="replace")
def load_dim_delivery_partner():
    """Load dim_delivery_partner_.csv into BigQuery"""
    df = pd.read_csv("/workspaces/Crisis-Recovery-Data-Analytics-Project/Datasets/dim_delivery_partner_.csv")
    yield df


@dlt.resource(name="dim_menu_item", write_disposition="replace")
def load_dim_menu_item():
    """Load dim_menu_item.csv into BigQuery"""
    df = pd.read_csv("/workspaces/Crisis-Recovery-Data-Analytics-Project/Datasets/dim_menu_item.csv")
    yield df


@dlt.resource(name="dim_restaurant", write_disposition="replace")
def load_dim_restaurant():
    """Load dim_restaurant.csv into BigQuery"""
    df = pd.read_csv("/workspaces/Crisis-Recovery-Data-Analytics-Project/Datasets/dim_restaurant.csv")
    yield df


@dlt.resource(name="fact_delivery_performance", write_disposition="replace")
def load_fact_delivery_performance():
    """Load fact_delivery_performance.csv into BigQuery"""
    df = pd.read_csv("/workspaces/Crisis-Recovery-Data-Analytics-Project/Datasets/fact_delivery_performance.csv")
    yield df


@dlt.resource(name="fact_order_items", write_disposition="replace")
def load_fact_order_items():
    """Load fact_order_items.csv into BigQuery"""
    df = pd.read_csv("/workspaces/Crisis-Recovery-Data-Analytics-Project/Datasets/fact_order_items.csv")
    yield df


@dlt.resource(name="fact_orders", write_disposition="replace")
def load_fact_orders():
    """Load fact_orders.csv into BigQuery"""
    df = pd.read_csv("/workspaces/Crisis-Recovery-Data-Analytics-Project/Datasets/fact_orders.csv")
    yield df


@dlt.resource(name="fact_ratings", write_disposition="replace")
def load_fact_ratings():
    """Load fact_ratings.csv into BigQuery"""
    df = pd.read_csv("/workspaces/Crisis-Recovery-Data-Analytics-Project/Datasets/fact_ratings.csv")
    yield df


@dlt.source
def crisis_recovery_source():
    """Source containing all dataset tables"""
    return [
        load_dim_customer(),
        load_dim_delivery_partner(),
        load_dim_menu_item(),
        load_dim_restaurant(),
        load_fact_delivery_performance(),
        load_fact_order_items(),
        load_fact_orders(),
        load_fact_ratings(),
    ]


def load_all_data():
    """
    Main function to load all CSV files into BigQuery.
    Uses the dlt pipeline with BigQuery as the destination.
    """
    pipeline = dlt.pipeline(
        pipeline_name="crisis_recovery_pipeline",
        destination="bigquery",
        dataset_name="crisis_recovery_data"
    )
    
    # Load all resources from the source
    load_info = pipeline.run(crisis_recovery_source())
    
    print(load_info)
    return load_info


if __name__ == "__main__":
    # Run the pipeline
    load_all_data()
