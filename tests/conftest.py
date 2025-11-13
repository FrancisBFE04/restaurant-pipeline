"""
Pytest configuration and shared fixtures for restaurant-pipeline tests.

This module provides reusable mock data and fixtures for unit testing
data processing and reporting nodes.
"""

import pytest
import pandas as pd
from typing import Tuple


@pytest.fixture
def sample_customers_df() -> pd.DataFrame:
    """
    Mock customer DataFrame for unit tests.
    Includes duplicates and null values to test cleaning logic.
    """
    return pd.DataFrame({
        "id": [1, 2, 1, 3],
        "name": ["Alice", "Bob", "Alice", None],
        "email": ["a@test.com", "b@test.com", "a@test.com", "c@test.com"],
    })


@pytest.fixture
def sample_orders_df() -> pd.DataFrame:
    """
    Mock orders DataFrame with raw CSV structure.
    Uses 'id' column (which should be renamed to 'order_id' by clean_orders).
    Includes duplicates and null values.
    """
    return pd.DataFrame({
        "id": ["ORD-001", "ORD-002", "ORD-001", "ORD-003"],
        "customer": [1, 2, 1, None],
        "ordered_at": ["2024-01-01", "2024-01-02", "2024-01-01", "2024-01-03"],
        "subtotal": [100.00, 150.00, 100.00, 200.00],
        "tax_paid": [10.00, 15.00, 10.00, None],
        "order_total": [110.00, 165.00, 110.00, 220.00],
    })


@pytest.fixture
def sample_tickets_df() -> pd.DataFrame:
    """
    Mock tickets DataFrame from JSONL with nested structures.
    Includes dict/list values in tags and other columns.
    Includes duplicates and null values to test serialization.
    """
    return pd.DataFrame({
        "ticket_id": ["TCK-001", "TCK-002", "TCK-001", "TCK-003"],
        "order_id": ["ORD-001", "ORD-002", "ORD-001", "ORD-003"],
        "customer_external_id": [1, 2, 1, None],
        "channel": ["email", "chat", "email", "phone"],
        "priority": ["normal", "high", "normal", "low"],
        "status": ["resolved", "open", "resolved", None],
        "tags": [
            ["order", "delivery"],  # List value (unhashable)
            {"category": "billing"},  # Dict value (unhashable)
            ["order", "delivery"],  # Duplicate
            ["refund"],  # List value
        ],
        "subject": ["Delivery Status", "Billing Issue", "Delivery Status", None],
        "sentiment": ["neutral", "negative", "neutral", "positive"],
    })


@pytest.fixture
def cleaned_customers_df() -> pd.DataFrame:
    """
    Expected output from clean_customers node.
    No duplicates, no nulls.
    """
    return pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Bob"],
        "email": ["a@test.com", "b@test.com", "c@test.com"],
    })


@pytest.fixture
def cleaned_orders_df() -> pd.DataFrame:
    """
    Expected output from clean_orders node.
    - 'id' renamed to 'order_id'
    - ordered_at parsed to datetime
    - No duplicates, no nulls
    """
    return pd.DataFrame({
        "order_id": ["ORD-002", "ORD-003"],
        "customer": [2.0, 3.0],
        "ordered_at": pd.to_datetime(["2024-01-02", "2024-01-03"]),
        "subtotal": [150.00, 200.00],
        "tax_paid": [15.00, 20.00],
        "order_total": [165.00, 220.00],
    })


@pytest.fixture
def cleaned_tickets_df() -> pd.DataFrame:
    """
    Expected output from clean_tickets node.
    - Nested structures serialized to JSON strings
    - No duplicates, no nulls
    - All values are hashable
    """
    return pd.DataFrame({
        "ticket_id": ["TCK-002", "TCK-003"],
        "order_id": ["ORD-002", "ORD-003"],
        "customer_external_id": [2, 3],
        "channel": ["chat", "phone"],
        "priority": ["high", "low"],
        "status": ["open", "open"],
        "tags": [
            '{"category": "billing"}',  # Dict serialized to JSON string
            '["refund"]',  # List serialized to JSON string
        ],
        "subject": ["Billing Issue", "Refund Request"],
        "sentiment": ["negative", "positive"],
    })


@pytest.fixture
def sample_analytics_df() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Fixture combining cleaned orders and tickets for reporting tests.
    
    Returns:
        Tuple of (cleaned_orders_df, cleaned_tickets_df)
    """
    orders = pd.DataFrame({
        "order_id": ["ORD-001", "ORD-002", "ORD-003"],
        "customer": [1, 2, 3],
        "ordered_at": pd.to_datetime([
            "2024-01-01", "2024-01-02", "2024-01-03"
        ]),
        "subtotal": [100.00, 150.00, 200.00],
    })
    
    tickets = pd.DataFrame({
        "ticket_id": ["TCK-001", "TCK-002", "TCK-003", "TCK-004"],
        "order_id": ["ORD-001", "ORD-001", "ORD-002", "ORD-003"],
        "status": ["resolved", "resolved", "open", "open"],
    })
    
    return orders, tickets


@pytest.fixture(scope="session")
def project_root() -> str:
    """
    Return the root directory of the project.
    Useful for accessing config and data files in integration tests.
    """
    import os
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
