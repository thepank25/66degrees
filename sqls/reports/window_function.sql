select s.branch, 
sum(total) as total_sales,
first_value(customer_type) over (partition by s.branch order by sum(total) desc) as top_customer_type
from Fact_Sales s 
join Dim_Branch b on s.branch = b.branch
group by s.branch
