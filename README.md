## Restaurant Pipeline 



A production-grade **Kedro-based data pipeline** that ingests restaurant data from several sources (local CSVs + Azure Blob Storage), cleans and transforms it using a bronze-silver-gold layered architecture, and generates business analytics models. This project demonstrates a data pipeline for a restaurant that was constructed using [Kedro](https://kedro.org/).



## Overview 


This pipeline showcases best practices for: The pipeline receives raw data from two CSV files ('raw_customers.csv' and 'raw_orders.csv') as well as Azure Blob Storage (a JSONL file containing support tickets).  It then cleans and transforms the data to create two final datasets: one with the average order value and one with the number of tickets per order.  The pipeline follows the bronze-silver-gold architecture:

- **Multi-source data ingestion** (local files + cloud blob storage)

- **Layered architecture** (bronze → silver → gold)*   **Bronze:** Raw, unprocessed data.

- **Data quality** (deduplication, null handling, type validation)*   **Silver:** Cleaned and transformed data.

- **Reproducibility** (Kedro framework with versioned configs)*   **Gold:** Final, aggregated data ready for reporting or analysis.

- **Analytics models** (average order value, support ticket metrics, revenue)

## Project Structure

### Key Features

Ingests 4 local CSV files (customers, orders, products, items)  The project is structured as a standard Kedro project:

Connects to Azure Blob Storage for support tickets (JSONL format)  

Cleans nested JSONL objects without data loss  *   `conf/`: Configuration files, including the data catalog (`catalog.yml`).

Generates 3 gold-layer analytics CSVs  *   `data/`: Data files, organized into `bronze`, `silver`, and `gold` layers.

Fully tested with pytest fixtures  *   `src/`: Source code, including the pipeline and node definitions.

Production-ready code with inline documentation  

## How to run the pipeline

---

To run this pipeline, you need to have Python 3.10+ and `pip` installed.

## Architecture

1.  **Clone the repository:**

### Data Flow

    ```bash

```    git clone <repository-url>

┌─────────────────────────────────────────────────────────┐    cd <repository-name>

│           BRONZE LAYER (Raw Ingestion)                  │    ```

├─────────────────────────────────────────────────────────┤

│                                                         │2.  **Create a virtual environment:**

│  📂 Local CSVs              🌐 Azure Blob Storage       │

│  ├─ raw_customers.csv       └─ support_tickets.jsonl   │    ```bash

│  ├─ raw_orders.csv              (SAS URL)              │    python -m venv venv

│  ├─ raw_products.csv                                   │    ```

│  └─ raw_items.csv                                      │

│                                                         │3.  **Activate the virtual environment:**

└──────────────────────┬──────────────────────────────────┘

                       │ clean_*_node    *   On Windows:

                       ▼

┌─────────────────────────────────────────────────────────┐        ```bash

│         SILVER LAYER (Cleaned & Standardized)           │        .\venv\Scripts\activate

├─────────────────────────────────────────────────────────┤        ```

│                                                         │

│  📦 Deduplicated Parquet Files                          │    *   On macOS and Linux:

│  ├─ cleaned_customers.parquet                          │

│  ├─ cleaned_orders.parquet  (order_id renamed)        │        ```bash

│  └─ cleaned_tickets.parquet (nested fields serialized) │        source venv/bin/activate

│                                                         │        ```

└──────────────────────┬──────────────────────────────────┘

                       │ create_*_node4.  **Install the dependencies:**

                       ▼

┌─────────────────────────────────────────────────────────┐    ```bash

│        GOLD LAYER (Analytics & Reporting)              │    pip install -r requirements.txt

├─────────────────────────────────────────────────────────┤    ```

│                                                         │

│  📊 Consumer-Ready CSV Files                            │5.  **Run the pipeline:**

│  ├─ average_order_value.csv    ($993.17)              │

│  ├─ tickets_per_order.csv      (2.5M rows)            │    ```bash

│  └─ total_revenue.csv          ($62.7M)               │    kedro run

│                                                         │    ```

└─────────────────────────────────────────────────────────┘

```6.  **Check the results:**



### Directory Structure    The final output will be in the `data/gold` directory. You will find two files: `average_order_value.csv` and `tickets_per_order.csv`.


---

## Quick Start

### Prerequisites

- **Python 3.9+** (tested on 3.13)
- **Pip** or **Conda**

### Installation

1. **Navigate to project:**
   ```bash
   cd restaurant-pipeline
   ```

2. **Create and activate virtual environment:**
   ```bash
   python -m venv venv
   venv\Scripts\activate  # Windows
   source venv/bin/activate  # macOS/Linux
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

### Running the Pipeline

**Full pipeline (all 6 nodes):**
```bash
kedro run
```

**Data processing only (bronze → silver):**
```bash
kedro run --pipeline data_processing
```

**Reporting only (silver → gold):**
```bash
kedro run --pipeline reporting
```

**From specific node:**
```bash
kedro run --from-nodes clean_orders_node
```

### Running Tests

```bash
pytest
pytest --cov=src/restaurant_pipeline tests/  # with coverage
```

---

## Data Sources

### Local CSV Files (Bronze Layer)

| File | Rows |
|------|------|
| `raw_customers.csv` | Customer master data |
| `raw_orders.csv` | Transaction records |
| `raw_products.csv` | Product catalog |
| `raw_items.csv` | Order line items |

### Azure Blob Storage (JSONL)

- **URL:**  Enter your SAS URL here
- **Format:** JSON Lines (one JSON object per line)
- **Rows:** ~90K support tickets
- **Auth:** SAS token

**Sample record:**
```json
{
  "ticket_id": "TCK-317F60A0AA",
  "order_id": "0000dda0-bedb-4109-bdfb-1bbbed16af12",
  "tags": ["order", "delivery"],
  "subject": "Delivery Status",
  "status": "resolved"
}
```

---

## Data Cleaning Logic

### Bronze → Silver Transformations

#### `clean_customers`
- Remove duplicate rows
- Remove rows with null values
- **Output:** `cleaned_customers.parquet`

#### `clean_orders`
- **Rename:** `id` → `order_id` (for consistency with tickets join key)
- Remove duplicates
- Remove nulls
- **Parse:** `ordered_at` to datetime (enables temporal analysis)
- **Output:** `cleaned_orders.parquet`

#### `clean_tickets`
- **Challenge:** JSONL contains nested dict/list values (e.g., `tags: ["order", "delivery"]`)
  - pandas.drop_duplicates() fails: `TypeError: unhashable type: 'dict'`
- **Solution:** Serialize dict/list to JSON strings before deduplication
  - Use helper `_safe_serialize()` with `sort_keys=True` for deterministic serialization
- Remove duplicates
- Remove nulls
- **Output:** `cleaned_tickets.parquet`

---

## Analytics Models (Gold Layer)

### 1. Average Order Value
**File:** `data/gold/average_order_value.csv`  
**Formula:** `mean(orders.subtotal)`  
**Value:** $993.17  
**Use Case:** Revenue per transaction KPI

### 2. Tickets per Order
**File:** `data/gold/tickets_per_order.csv`  
**Formula:** Join orders ↔ tickets on order_id, count tickets per order  
**Rows:** ~2.5M  
**Use Case:** Support load analysis

### 3. Total Revenue
**File:** `data/gold/total_revenue.csv`  
**Formula:** `sum(orders.subtotal)`  
**Value:** $62,716,500  
**Use Case:** Financial reporting

---

## Configuration

### `conf/base/catalog.yml`

Defines all datasets (sources and outputs):

```yaml
raw_customers:
  type: pandas.CSVDataset
  filepath: data/bronze/raw_customers.csv

tickets_jsonl:
  type: pandas.JSONDataset
  filepath: Your filepath here
  load_args:
    lines: True  # JSONL format

cleaned_customers:
  type: pandas.ParquetDataset
  filepath: data/silver/cleaned_customers.parquet

average_order_value:
  type: pandas.CSVDataset
  filepath: data/gold/average_order_value.csv
```

### `conf/base/parameters.yml`

Pipeline parameters (extensible for data quality rules, thresholds, etc.)

### `conf/local/credentials.yml`

Git-ignored local secrets (currently unused; Azure auth via SAS token in URL)

---

## Testing

### Test Structure

```
tests/
├── conftest.py                    # Shared fixtures & mock data
├── pipelines/
│   ├── test_data_processing.py   # Test clean_* nodes
│   └── test_reporting.py         # Test create_* nodes
```

### Running Tests

```bash
pytest  # All tests
pytest tests/pipelines/test_data_processing.py -v  # Specific module, verbose
pytest --cov=src/restaurant_pipeline tests/  # With coverage report
```
---
##  Final Project Structure
```
restaurant-pipeline/
├── README.md                           # This file
├── pyproject.toml                      # Project metadata & dependencies
├── requirements.txt                    # Python packages
├── pytest.ini                          # Pytest configuration
│
├── conf/                               # Configuration (non-code)
│   ├── base/
│   │   ├── catalog.yml                 # Data sources & outputs
│   │   └── parameters.yml              # Pipeline parameters
│   └── local/
│       └── credentials.yml             # Local secrets (git-ignored)
│
├── data/                               # Data directory
│   ├── bronze/                         # Raw data (ingestion sources)
│   │   ├── raw_customers.csv
│   │   ├── raw_orders.csv
│   │   ├── raw_products.csv
│   │   └── raw_items.csv
│   ├── silver/                         # Cleaned data (intermediates)
│   │   ├── cleaned_customers.parquet
│   │   ├── cleaned_orders.parquet
│   │   └── cleaned_tickets.parquet
│   └── gold/                           # Analytics output (final)
│       ├── average_order_value.csv
│       ├── tickets_per_order.csv
│       └── total_revenue.csv
│
├── src/restaurant_pipeline/            # Main source code
│   ├── __init__.py
│   ├── __main__.py
│   ├── pipeline_registry.py            # Kedro pipeline registry
│   ├── settings.py                     # Kedro configuration
│   └── pipelines/
│       ├── data_processing/            # Bronze → Silver layer
│       │   ├── __init__.py
│       │   ├── nodes.py               # clean_customers, clean_orders, clean_tickets
│       │   └── pipeline.py            # Data processing DAG
│       └── reporting/                 # Silver → Gold layer
│           ├── __init__.py
│           ├── nodes.py               # Average value, tickets/order, revenue
│           └── pipeline.py            # Reporting DAG
│
├── tests/                              # Unit & integration tests
│   ├── conftest.py                    # Pytest fixtures & setup
│   ├── pipelines/
│   │   ├── test_data_processing.py    # Test data_processing nodes
│   │   └── test_reporting.py          # Test reporting nodes
│   └── ...
│
└── docs/                               # Documentation
    └── ARCHITECTURE.md                 # Extended design docs
```
---
