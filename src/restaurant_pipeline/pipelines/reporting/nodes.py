"""
Analytics and reporting nodes for the restaurant pipeline.

This module creates gold-layer analytics models from cleaned silver-layer data.
Outputs are consumable by dashboards, BI tools, and stakeholders.
"""

import pandas as pd


def create_average_order_value(orders: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the mean order value across all orders.
    
    Args:
        orders: Cleaned orders DataFrame with 'subtotal' column
        
    Returns:
        Single-row DataFrame with column 'average_order_value'
    """
    avg_order_value = orders["subtotal"].mean()
    return pd.DataFrame({"average_order_value": [avg_order_value]})


def create_tickets_per_order(
    orders: pd.DataFrame, tickets: pd.DataFrame
) -> pd.DataFrame:
    """
    Count support tickets per order to measure customer support load.
    
    Joins orders and tickets on order_id, groups by order, and counts.
    
    Args:
        orders: Cleaned orders DataFrame with 'order_id' column
        tickets: Cleaned tickets DataFrame with 'order_id' and
                 'ticket_id' columns
        
    Returns:
        DataFrame with columns [order_id, number_of_tickets]
    """
    # Join orders and tickets on the shared order_id key
    order_tickets = pd.merge(orders, tickets, on="order_id")
    # Group by order and count distinct tickets per order
    tickets_per_order = (
        order_tickets.groupby("order_id")["ticket_id"]
        .count()
        .reset_index()
    )
    # Rename for clarity in gold-layer output
    tickets_per_order.rename(
        columns={"ticket_id": "number_of_tickets"}, inplace=True
    )
    return tickets_per_order


def create_total_revenue(orders: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate total restaurant revenue across all orders.
    
    Args:
        orders: Cleaned orders DataFrame with 'subtotal' column
        
    Returns:
        Single-row DataFrame with column 'total_revenue'
    """
    total_revenue = orders["subtotal"].sum()
    return pd.DataFrame({"total_revenue": [total_revenue]})
