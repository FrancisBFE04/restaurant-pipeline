"""
Data processing nodes for the restaurant pipeline.

This module handles cleaning and standardization of raw data from multiple sources:
- Customers: local CSV (bronze layer)
- Orders: local CSV (bronze layer)  
- Tickets: Azure Blob JSONL (bronze layer)

All nodes output to the silver layer as deduplicated, typed parquet files.
"""

import pandas as pd
import json
from typing import Any


def clean_customers(customers: pd.DataFrame) -> pd.DataFrame:
    """
    Clean customer data by removing duplicates and missing values.
    
    Args:
        customers: Raw customer DataFrame from CSV
        
    Returns:
        Cleaned customer DataFrame with no duplicates or nulls
    """
    customers.drop_duplicates(inplace=True)
    customers.dropna(inplace=True)
    return customers


def clean_orders(orders: pd.DataFrame) -> pd.DataFrame:
    """
    Clean order data: rename 'id' to 'order_id' (for reporting joins), remove duplicates,
    handle missing values, and parse datetime.
    
    Args:
        orders: Raw orders DataFrame from CSV with columns [id, customer, ordered_at, ...]
        
    Returns:
        Cleaned orders DataFrame with order_id column and datetime-typed ordered_at
    """
    # Rename 'id' → 'order_id' for consistency with tickets data and reporting layer
    if "id" in orders.columns:
        orders.rename(columns={"id": "order_id"}, inplace=True)
    
    orders.drop_duplicates(inplace=True)
    orders.dropna(inplace=True)
    # Parse timestamp string to datetime for temporal filtering/analysis
    orders["ordered_at"] = pd.to_datetime(orders["ordered_at"])
    return orders


def _safe_serialize(x: Any) -> Any:
    """
    Serialize unhashable types (dict, list) to JSON strings for deduplication.
    This is necessary because pandas.drop_duplicates() requires hashable types.
    
    Args:
        x: Value to serialize (can be dict, list, or scalar)
        
    Returns:
        JSON string for dict/list, original value for scalars
    """
    if isinstance(x, (dict, list)):
        try:
            # Use sort_keys for deterministic serialization across rows
            return json.dumps(x, sort_keys=True)
        except (TypeError, ValueError):
            # Fallback for non-serializable objects (e.g., custom classes)
            return str(x)
    return x


def clean_tickets(tickets: pd.DataFrame) -> pd.DataFrame:
    """
    Clean support ticket data from Azure Blob JSONL.
    
    Challenge: JSONL contains nested objects/lists (e.g., tags column) which are unhashable
    and cause pandas.drop_duplicates() to fail with TypeError.
    Solution: Serialize nested columns to JSON strings before deduplication, then clean.
    
    Args:
        tickets: Raw ticket DataFrame from JSONL with nested columns
        
    Returns:
        Cleaned tickets DataFrame with no duplicates, no nulls, serialized tags
    """
    # Serialize unhashable nested types to strings (dict/list → JSON string)
    tickets = tickets.applymap(_safe_serialize)
    
    # Ensure tags column is always a string type (no nested dicts)
    if "tags" in tickets.columns:
        tickets["tags"] = tickets["tags"].astype(str)

    tickets.drop_duplicates(inplace=True)
    tickets.dropna(inplace=True)
    return tickets
