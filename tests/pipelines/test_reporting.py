"""
Unit tests for reporting pipeline nodes.

Tests the analytics generation functions:
- create_average_order_value: calculates mean order subtotal
- create_tickets_per_order: counts support tickets per order
- create_total_revenue: sums order revenue
"""

import pytest
import pandas as pd
from restaurant_pipeline.pipelines.reporting.nodes import (
    create_average_order_value,
    create_tickets_per_order,
    create_total_revenue,
)


@pytest.mark.unit
class TestCreateAverageOrderValue:
    """Test cases for create_average_order_value node."""

    def test_returns_single_row_dataframe(self, sample_analytics_df):
        """Verify output is a single-row DataFrame."""
        orders, _ = sample_analytics_df
        result = create_average_order_value(orders)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    def test_calculates_correct_average(self, sample_analytics_df):
        """Verify average order value is calculated correctly."""
        orders, _ = sample_analytics_df
        result = create_average_order_value(orders)
        
        # Expected: (100 + 150 + 200) / 3 = 150.0
        expected_avg = orders["subtotal"].mean()
        assert result["average_order_value"].values[0] == expected_avg

    def test_column_name_is_correct(self, sample_analytics_df):
        """Verify output column is named 'average_order_value'."""
        orders, _ = sample_analytics_df
        result = create_average_order_value(orders)
        
        assert "average_order_value" in result.columns
        assert len(result.columns) == 1

    def test_handles_empty_orders(self):
        """Verify function handles empty DataFrame gracefully."""
        empty_orders = pd.DataFrame({"subtotal": []})
        result = create_average_order_value(empty_orders)
        
        # Result should have NaN (mean of empty is NaN)
        assert pd.isna(result["average_order_value"].values[0])

    def test_handles_single_order(self):
        """Verify function works with single order."""
        single_order = pd.DataFrame({"subtotal": [100.0]})
        result = create_average_order_value(single_order)
        
        assert result["average_order_value"].values[0] == 100.0


@pytest.mark.unit
class TestCreateTicketsPerOrder:
    """Test cases for create_tickets_per_order node."""

    def test_returns_dataframe_with_correct_columns(self, sample_analytics_df):
        """Verify output has order_id and number_of_tickets columns."""
        orders, tickets = sample_analytics_df
        result = create_tickets_per_order(orders, tickets)
        
        assert isinstance(result, pd.DataFrame)
        assert "order_id" in result.columns
        assert "number_of_tickets" in result.columns

    def test_counts_tickets_correctly(self, sample_analytics_df):
        """Verify ticket counts are accurate per order."""
        orders, tickets = sample_analytics_df
        result = create_tickets_per_order(orders, tickets)
        
        # ORD-001 should have 2 tickets
        ord_001 = result[result["order_id"] == "ORD-001"]
        assert ord_001["number_of_tickets"].values[0] == 2
        
        # ORD-002 should have 1 ticket
        ord_002 = result[result["order_id"] == "ORD-002"]
        assert ord_002["number_of_tickets"].values[0] == 1

    def test_only_includes_orders_with_tickets(self, sample_analytics_df):
        """Verify only orders with at least one ticket appear in output."""
        orders, tickets = sample_analytics_df
        result = create_tickets_per_order(orders, tickets)
        
        # Input: 3 orders; only 2 have tickets (ORD-001, ORD-002, ORD-003)
        # Output should only have orders that appear in both joins
        assert len(result) == 3
        assert set(result["order_id"]) == {"ORD-001", "ORD-002", "ORD-003"}

    def test_handles_no_matching_orders_and_tickets(self):
        """Verify function handles non-matching orders and tickets."""
        orders = pd.DataFrame({
            "order_id": ["ORD-001", "ORD-002"],
        })
        tickets = pd.DataFrame({
            "order_id": ["ORD-999", "ORD-998"],  # No matching orders
            "ticket_id": ["TCK-001", "TCK-002"],
        })
        result = create_tickets_per_order(orders, tickets)
        
        # Result should be empty (no joins)
        assert len(result) == 0


@pytest.mark.unit
class TestCreateTotalRevenue:
    """Test cases for create_total_revenue node."""

    def test_returns_single_row_dataframe(self, sample_analytics_df):
        """Verify output is a single-row DataFrame."""
        orders, _ = sample_analytics_df
        result = create_total_revenue(orders)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    def test_calculates_correct_total(self, sample_analytics_df):
        """Verify total revenue is calculated correctly."""
        orders, _ = sample_analytics_df
        result = create_total_revenue(orders)
        
        # Expected: 100 + 150 + 200 = 450.0
        expected_total = orders["subtotal"].sum()
        assert result["total_revenue"].values[0] == expected_total

    def test_column_name_is_correct(self, sample_analytics_df):
        """Verify output column is named 'total_revenue'."""
        orders, _ = sample_analytics_df
        result = create_total_revenue(orders)
        
        assert "total_revenue" in result.columns
        assert len(result.columns) == 1

    def test_handles_empty_orders(self):
        """Verify function handles empty DataFrame gracefully."""
        empty_orders = pd.DataFrame({"subtotal": []})
        result = create_total_revenue(empty_orders)
        
        # Result should be 0 (sum of empty is 0)
        assert result["total_revenue"].values[0] == 0

    def test_handles_single_order(self):
        """Verify function works with single order."""
        single_order = pd.DataFrame({"subtotal": [500.0]})
        result = create_total_revenue(single_order)
        
        assert result["total_revenue"].values[0] == 500.0


@pytest.mark.unit
def test_reporting_pipeline_end_to_end(sample_analytics_df):
    """Integration test: verify all analytics nodes work together."""
    orders, tickets = sample_analytics_df
    
    # Generate all three analytics
    avg_value = create_average_order_value(orders)
    tickets_per_order = create_tickets_per_order(orders, tickets)
    total_revenue = create_total_revenue(orders)
    
    # Verify structure of outputs
    assert len(avg_value) == 1
    assert len(tickets_per_order) >= 1
    assert len(total_revenue) == 1
    
    # Verify values are reasonable (non-negative)
    assert avg_value["average_order_value"].values[0] > 0
    assert (tickets_per_order["number_of_tickets"] > 0).all()
    assert total_revenue["total_revenue"].values[0] > 0
    
    # Verify total_revenue >= avg_value (for multiple orders)
    assert (
        total_revenue["total_revenue"].values[0]
        >= avg_value["average_order_value"].values[0]
    )
