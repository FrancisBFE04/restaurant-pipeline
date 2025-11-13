from kedro.pipeline import Pipeline, node
from .nodes import create_average_order_value, create_tickets_per_order, create_total_revenue

def create_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=create_average_order_value,
                inputs="cleaned_orders",
                outputs="average_order_value",
                name="create_average_order_value_node",
            ),
            node(
                func=create_tickets_per_order,
                inputs=["cleaned_orders", "cleaned_tickets"],
                outputs="tickets_per_order",
                name="create_tickets_per_order_node",
            ),
            node(
                func=create_total_revenue,
                inputs="cleaned_orders",
                outputs="total_revenue",
                name="create_total_revenue_node",
            ),
        ]
    )