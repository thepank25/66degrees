Insert into silver_supermarket
SELECT 
    CAST("Invoice ID" AS TEXT) AS "invoice_id",
    CAST("Branch" AS TEXT) AS "branch",
    CAST("City" AS TEXT) AS "city",
    CAST("Customer type" AS TEXT) AS "customer_type",
    CAST("Gender" AS TEXT) AS "gender",
    CAST("Product line" AS TEXT) AS "product_line",
    CAST("Unit price" AS REAL) AS "unit_price",
    CAST("Quantity" AS INTEGER) AS "quantity",
    CAST("Tax 5%" AS REAL) AS "tax_5_percent",
    CAST("Total" AS REAL) AS "total",    

    date(
        substr("date", instr("date", '/') + instr(substr("date", instr("date", '/') + 1), '/') + 1)  -- year
        || '-' ||
        printf('%02d', CAST(substr("date", 1, instr("date", '/') - 1) AS INTEGER))  -- month
        || '-' ||
        printf('%02d', CAST(
            substr("date", instr("date", '/') + 1,
            instr(substr("date", instr("date", '/') + 1), '/') - 1)
        AS TEXT))  -- day
    ) AS "date",
    CAST("Time" AS TEXT) AS "time",
    CAST("Payment" AS TEXT) AS "payment",
    CAST("cogs" AS REAL) AS "cogs",
    CAST("gross margin percentage" AS REAL) AS "gross_margin_percentage",
    CAST("gross income" AS REAL) AS "gross_income",
    CAST("Rating" AS REAL) AS "rating"
FROM original_data;