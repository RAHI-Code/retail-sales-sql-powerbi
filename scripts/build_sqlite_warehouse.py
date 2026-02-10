# scripts/build_sqlite_warehouse.py
#
# Builds a SQLite star-schema warehouse from your CSV:
# Columns (confirmed): Invoice, StockCode, Description, Quantity, InvoiceDate, Price, Customer ID, Country
#
# Outputs:
# - online_retail.db (SQLite database)  [ignored by .gitignore]
# - exports/*.csv (dimension + fact tables)  [ignored by .gitignore]

import os
import sqlite3
import pandas as pd


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_PATH = os.path.join(ROOT, "data", "online_retail_II.csv")
DB_PATH = os.path.join(ROOT, "online_retail.db")
EXPORT_DIR = os.path.join(ROOT, "exports")

os.makedirs(EXPORT_DIR, exist_ok=True)


def col(df: pd.DataFrame, name: str) -> str:
    """Find a column in df matching name (case/space-insensitive)."""
    target = name.strip().lower()
    for c in df.columns:
        if c.strip().lower() == target:
            return c
    raise KeyError(f"Missing column '{name}'. Found: {list(df.columns)}")


def main():
    print("Loading CSV...")
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV not found at: {CSV_PATH}")

    # Read CSV (Online Retail datasets commonly need this encoding)
    df = pd.read_csv(CSV_PATH, encoding="ISO-8859-1")

    # ===== EXACT MAPPING FOR YOUR CSV =====
    InvoiceCol = col(df, "Invoice")
    StockCodeCol = col(df, "StockCode")
    DescriptionCol = col(df, "Description")
    QuantityCol = col(df, "Quantity")
    InvoiceDateCol = col(df, "InvoiceDate")
    PriceCol = col(df, "Price")
    CustomerIdCol = col(df, "Customer ID")
    CountryCol = col(df, "Country")

    # ===== CLEANING / NORMALIZATION =====
    # Parse datetime robustly
    df[InvoiceDateCol] = pd.to_datetime(df[InvoiceDateCol], errors="coerce")
    df = df[df[InvoiceDateCol].notna()].copy()

    # Numeric fields
    df[QuantityCol] = pd.to_numeric(df[QuantityCol], errors="coerce")
    df[PriceCol] = pd.to_numeric(df[PriceCol], errors="coerce")
    df = df[df[QuantityCol].notna() & df[PriceCol].notna()].copy()

    # Remove obviously invalid rows (keep returns, but remove 0 qty and non-positive prices)
    df = df[(df[QuantityCol] != 0) & (df[PriceCol] > 0)].copy()

    # Standardize text fields
    df[InvoiceCol] = df[InvoiceCol].astype(str).str.strip()
    df[StockCodeCol] = df[StockCodeCol].astype(str).str.strip()
    df[DescriptionCol] = df[DescriptionCol].astype(str).str.strip()
    df[CountryCol] = df[CountryCol].astype(str).str.strip()

    # Customer IDs: keep missing as GUEST (useful for dashboards)
    # Some files have floats like 12345.0; normalize to clean string IDs
    df[CustomerIdCol] = pd.to_numeric(df[CustomerIdCol], errors="coerce")
    df["customer_id"] = df[CustomerIdCol].astype("Int64").astype(str).str.strip()
    df.loc[df["customer_id"].isin(["<NA>", "nan", "None"]), "customer_id"] = "GUEST"

    # Derived columns
    df["invoice_no"] = df[InvoiceCol]
    df["invoice_datetime"] = df[InvoiceDateCol].dt.strftime("%Y-%m-%d %H:%M:%S")
    df["full_date"] = df[InvoiceDateCol].dt.strftime("%Y-%m-%d")
    df["year"] = df[InvoiceDateCol].dt.year.astype(int)
    df["month"] = df[InvoiceDateCol].dt.month.astype(int)
    df["month_name"] = df[InvoiceDateCol].dt.strftime("%b")
    df["quarter"] = ((df["month"] - 1) // 3 + 1).astype(int)

    df["stock_code"] = df[StockCodeCol]
    df["description"] = df[DescriptionCol]
    df["country"] = df[CountryCol]

    df["quantity"] = df[QuantityCol].astype(int)
    df["unit_price"] = df[PriceCol].astype(float)
    df["revenue"] = df["quantity"].astype(float) * df["unit_price"].astype(float)
    df["is_return"] = (df["quantity"] < 0).astype(int)

    # ===== SQLITE BUILD =====
    print(f"Building SQLite DB: {DB_PATH}")
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("PRAGMA foreign_keys = ON;")

    print("Creating schema...")
    cur.executescript(
        """
        DROP TABLE IF EXISTS fact_sales;
        DROP TABLE IF EXISTS dim_date;
        DROP TABLE IF EXISTS dim_country;
        DROP TABLE IF EXISTS dim_product;
        DROP TABLE IF EXISTS dim_customer;

        CREATE TABLE dim_customer (
          customer_key INTEGER PRIMARY KEY AUTOINCREMENT,
          customer_id TEXT UNIQUE
        );

        CREATE TABLE dim_product (
          product_key INTEGER PRIMARY KEY AUTOINCREMENT,
          stock_code TEXT,
          description TEXT,
          UNIQUE(stock_code, description)
        );

        CREATE TABLE dim_country (
          country_key INTEGER PRIMARY KEY AUTOINCREMENT,
          country TEXT UNIQUE
        );

        CREATE TABLE dim_date (
          date_key INTEGER PRIMARY KEY AUTOINCREMENT,
          full_date TEXT UNIQUE,
          year INTEGER,
          month INTEGER,
          month_name TEXT,
          quarter INTEGER
        );

        CREATE TABLE fact_sales (
          line_id INTEGER PRIMARY KEY AUTOINCREMENT,
          invoice_no TEXT,
          invoice_datetime TEXT,
          date_key INTEGER,
          customer_key INTEGER,
          product_key INTEGER,
          country_key INTEGER,
          quantity INTEGER,
          unit_price REAL,
          revenue REAL,
          is_return INTEGER,
          FOREIGN KEY(date_key) REFERENCES dim_date(date_key),
          FOREIGN KEY(customer_key) REFERENCES dim_customer(customer_key),
          FOREIGN KEY(product_key) REFERENCES dim_product(product_key),
          FOREIGN KEY(country_key) REFERENCES dim_country(country_key)
        );
        """
    )

    print("Loading dimensions...")

    # dim_customer
    df[["customer_id"]].drop_duplicates().to_sql("tmp_customer", con, if_exists="replace", index=False)
    cur.execute("INSERT OR IGNORE INTO dim_customer(customer_id) SELECT customer_id FROM tmp_customer;")
    cur.execute("DROP TABLE tmp_customer;")

    # dim_product
    df[["stock_code", "description"]].drop_duplicates().to_sql("tmp_product", con, if_exists="replace", index=False)
    cur.execute(
        """
        INSERT OR IGNORE INTO dim_product(stock_code, description)
        SELECT stock_code, description FROM tmp_product;
        """
    )
    cur.execute("DROP TABLE tmp_product;")

    # dim_country
    df[["country"]].drop_duplicates().to_sql("tmp_country", con, if_exists="replace", index=False)
    cur.execute("INSERT OR IGNORE INTO dim_country(country) SELECT country FROM tmp_country;")
    cur.execute("DROP TABLE tmp_country;")

    # dim_date
    df[["full_date", "year", "month", "month_name", "quarter"]].drop_duplicates().to_sql(
        "tmp_date", con, if_exists="replace", index=False
    )
    cur.execute(
        """
        INSERT OR IGNORE INTO dim_date(full_date, year, month, month_name, quarter)
        SELECT full_date, year, month, month_name, quarter FROM tmp_date;
        """
    )
    cur.execute("DROP TABLE tmp_date;")

    con.commit()

    print("Loading fact table...")

    # Pull dims for key lookup
    dim_customer = pd.read_sql_query("SELECT customer_key, customer_id FROM dim_customer", con)
    dim_product = pd.read_sql_query("SELECT product_key, stock_code, description FROM dim_product", con)
    dim_country = pd.read_sql_query("SELECT country_key, country FROM dim_country", con)
    dim_date = pd.read_sql_query("SELECT date_key, full_date FROM dim_date", con)

    fact = df[
        [
            "invoice_no",
            "invoice_datetime",
            "full_date",
            "customer_id",
            "stock_code",
            "description",
            "country",
            "quantity",
            "unit_price",
            "revenue",
            "is_return",
        ]
    ].copy()

    # Merge keys
    fact = (
        fact.merge(dim_date, on="full_date", how="left")
        .merge(dim_customer, on="customer_id", how="left")
        .merge(dim_country, on="country", how="left")
        .merge(dim_product, on=["stock_code", "description"], how="left")
    )

    missing = fact[["date_key", "customer_key", "country_key", "product_key"]].isna().sum().to_dict()
    if any(v > 0 for v in missing.values()):
        print("WARNING: Missing keys after merges:", missing)

    fact_out = fact[
        [
            "invoice_no",
            "invoice_datetime",
            "date_key",
            "customer_key",
            "product_key",
            "country_key",
            "quantity",
            "unit_price",
            "revenue",
            "is_return",
        ]
    ].copy()

    # Load fact
    fact_out.to_sql("fact_sales", con, if_exists="append", index=False)
    con.commit()

    # ===== SANITY CHECKS =====
    n = pd.read_sql_query("SELECT COUNT(*) AS n FROM fact_sales", con)["n"][0]
    returns = pd.read_sql_query("SELECT is_return, COUNT(*) AS n FROM fact_sales GROUP BY is_return", con)
    print("fact_sales rows:", n)
    print(returns)

    # ===== EXPORT FOR POWER BI =====
    print("Exporting tables for Power BI...")
    for t in ["dim_date", "dim_customer", "dim_product", "dim_country", "fact_sales"]:
        out = os.path.join(EXPORT_DIR, f"{t}.csv")
        pd.read_sql_query(f"SELECT * FROM {t}", con).to_csv(out, index=False)
        print("Exported:", out)

    con.close()
    print("DONE ✅  Your DB is:", DB_PATH)


if __name__ == "__main__":
    main()

