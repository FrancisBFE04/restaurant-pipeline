from kedro.pipeline import Pipeline, node
from .nodes import clean_customers, clean_orders, clean_tickets

def create_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=clean_customers,
                inputs="raw_customers",
                outputs="cleaned_customers",
                name="clean_customers_node",
            ),
            node(
                func=clean_orders,
                inputs="raw_orders",
                outputs="cleaned_orders",
                name="clean_orders_node",
            ),
            node(
                func=clean_tickets,
                inputs="tickets_jsonl",
                outputs="cleaned_tickets",
                name="clean_tickets_node",
            ),
        ]
    )