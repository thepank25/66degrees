
Create table "Fact_Sales" as 
Select 
"invoice_id",
"branch",
"quantity",
"tax_5_percent",
"total",
"date",
"time", 
"payment",
"cogs",
"gross_income",
-- customer info
"customer_type",
"gender",
--product info 
"product_line",
"unit_price",
"gross_margin_percentage"
from "silver_supermarket"
;