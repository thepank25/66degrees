CREATE TABLE IF NOT EXISTS silver_supermarket (
    invoice_id               TEXT,
    branch                   TEXT,
    city                     TEXT,
    customer_type            TEXT,
    gender                   TEXT,
    product_line             TEXT,
    unit_price               REAL,
    quantity                 INTEGER,
    tax_5_percent            REAL,
    total                    REAL,
    date                     TEXT,
    time                     TEXT,
    payment                  TEXT,
    cogs                     REAL,
    gross_margin_percentage  REAL,
    gross_income             REAL,
    rating                   REAL
);