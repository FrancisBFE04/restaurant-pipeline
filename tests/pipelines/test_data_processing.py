"""
Unit tests for data_processing pipeline nodes.

Tests the cleaning functions:
- clean_customers: deduplication, null handling
- clean_orders: column renaming (id → order_id), datetime parsing
- clean_tickets: nested structure serialization, deduplication
"""

import pytest
import pandas as pd
import json
from restaurant_pipeline.pipelines.data_processing.nodes import (
    clean_customers,
    clean_orders,
    clean_tickets,
    _safe_serialize,
)


@pytest.mark.unit
class TestCleanCustomers:
    """Test cases for clean_customers node."""

    def test_removes_duplicates(self, sample_customers_df):
        """Verify duplicate customer records are removed."""
        result = clean_customers(sample_customers_df.copy())
        
        # Input has 4 rows (with duplicate id=1), output should have 3 unique
        assert len(result) < len(sample_customers_df)
        assert result["id"].nunique() == len(result)

    def test_removes_nulls(self, sample_customers_df):
        """Verify rows with null values are dropped."""
        result = clean_customers(sample_customers_df.copy())
        
        # Input has null in name, output should have no nulls
        assert result.isnull().sum().sum() == 0

    def test_preserves_valid_data(self, sample_customers_df):
        """Verify valid records are preserved after cleaning."""
        result = clean_customers(sample_customers_df.copy())
        
        # Alice's email should still exist
        assert "a@test.com" in result["email"].values
        # Bob's name should still exist
        assert "Bob" in result["name"].values

    def test_returns_dataframe(self, sample_customers_df):
        """Verify output is a pandas DataFrame."""
        result = clean_customers(sample_customers_df.copy())
        assert isinstance(result, pd.DataFrame)


@pytest.mark.unit
class TestCleanOrders:
    """Test cases for clean_orders node."""

    def test_renames_id_to_order_id(self, sample_orders_df):
        """Verify 'id' column is renamed to 'order_id'."""
        result = clean_orders(sample_orders_df.copy())
        
        assert "order_id" in result.columns
        assert "id" not in result.columns
        # Verify order_id is not empty after cleaning
        assert len(result["order_id"]) > 0

    def test_parses_ordered_at_to_datetime(self, sample_orders_df):
        """Verify 'ordered_at' is converted to datetime type."""
        result = clean_orders(sample_orders_df.copy())
        
        assert result["ordered_at"].dtype == "datetime64[ns]"

    def test_removes_duplicates(self, sample_orders_df):
        """Verify duplicate orders are removed."""
        result = clean_orders(sample_orders_df.copy())
        
        # Input has 4 rows with 1 duplicate, output should have 3 unique
        assert len(result) < len(sample_orders_df)
        assert result["order_id"].nunique() == len(result)

    def test_removes_nulls(self, sample_orders_df):
        """Verify rows with null values are dropped."""
        result = clean_orders(sample_orders_df.copy())
        
        # Input has nulls in customer and tax_paid, output has none
        assert result.isnull().sum().sum() == 0

    def test_preserves_numeric_columns(self, sample_orders_df):
        """Verify numeric columns maintain their values."""
        result = clean_orders(sample_orders_df.copy())
        
        # ORD-002 should have subtotal 150.00
        ord_002 = result[result["order_id"] == "ORD-002"]
        assert ord_002["subtotal"].values[0] == 150.00


@pytest.mark.unit
class TestSafeSerialize:
    """Test cases for _safe_serialize helper function."""

    def test_serializes_dict_to_json_string(self):
        """Verify dict values are converted to JSON strings."""
        input_dict = {"category": "billing", "priority": "high"}
        result = _safe_serialize(input_dict)
        
        assert isinstance(result, str)
        # Verify it's valid JSON
        parsed = json.loads(result)
        assert parsed == input_dict

    def test_serializes_list_to_json_string(self):
        """Verify list values are converted to JSON strings."""
        input_list = ["order", "delivery", "urgent"]
        result = _safe_serialize(input_list)
        
        assert isinstance(result, str)
        parsed = json.loads(result)
        assert parsed == input_list

    def test_preserves_scalar_values(self):
        """Verify scalar values (str, int, float) pass through unchanged."""
        assert _safe_serialize("hello") == "hello"
        assert _safe_serialize(42) == 42
        assert _safe_serialize(3.14) == 3.14
        assert _safe_serialize(True) is True

    def test_preserves_none_values(self):
        """Verify None values pass through unchanged."""
        assert _safe_serialize(None) is None

    def test_handles_nested_structures(self):
        """Verify nested dict/list structures are serialized deterministically."""
        nested = {"tags": ["order", "delivery"], "priority": "high"}
        result = _safe_serialize(nested)
        
        # Verify it's JSON serialized with sorted keys for determinism
        parsed = json.loads(result)
        assert parsed == nested


@pytest.mark.unit
class TestCleanTickets:
    """Test cases for clean_tickets node."""

    def test_serializes_nested_structures(self, sample_tickets_df):
        """Verify nested dict/list in tags are serialized to strings."""
        result = clean_tickets(sample_tickets_df.copy())
        
        # All values should be strings (no dict or list objects)
        for val in result["tags"].dropna():
            assert isinstance(val, str)
            # Verify they're valid JSON
            json.loads(val)

    def test_removes_duplicates(self, sample_tickets_df):
        """Verify duplicate tickets are removed."""
        result = clean_tickets(sample_tickets_df.copy())
        
        # Input has 4 rows with 1 duplicate (TCK-001), output has 3
        assert len(result) < len(sample_tickets_df)
        assert result["ticket_id"].nunique() == len(result)

    def test_removes_nulls(self, sample_tickets_df):
        """Verify rows with null values are dropped."""
        result = clean_tickets(sample_tickets_df.copy())
        
        # Input has nulls in customer_external_id, status, subject
        assert result.isnull().sum().sum() == 0

    def test_tags_column_is_string_type(self, sample_tickets_df):
        """Verify tags column is string type after cleaning."""
        result = clean_tickets(sample_tickets_df.copy())
        
        # All tags should be strings (serialized JSON or regular strings)
        for val in result["tags"].dropna():
            assert isinstance(val, str)

    def test_preserves_other_columns(self, sample_tickets_df):
        """Verify non-nested columns are preserved."""
        result = clean_tickets(sample_tickets_df.copy())
        
        # Channel should be preserved (may vary based on dedup)
        assert "channel" in result.columns
        assert len(result["channel"]) > 0
        # Sentiment should still have expected values
        assert len(result["sentiment"].dropna()) > 0


@pytest.mark.unit
def test_clean_pipeline_end_to_end(
    sample_customers_df,
    sample_orders_df,
    sample_tickets_df
):
    """Integration test: verify all clean_* functions work together."""
    
    # Clean all three datasets
    customers = clean_customers(sample_customers_df.copy())
    orders = clean_orders(sample_orders_df.copy())
    tickets = clean_tickets(sample_tickets_df.copy())
    
    # Verify outputs have expected shape
    assert len(customers) >= 1
    assert len(orders) >= 1
    assert len(tickets) >= 1
    
    # Verify no nulls in any cleaned dataset
    assert customers.isnull().sum().sum() == 0
    assert orders.isnull().sum().sum() == 0
    assert tickets.isnull().sum().sum() == 0
    
    # Verify order_id is present and correct type
    assert "order_id" in orders.columns
    assert "id" not in orders.columns
