select s.branch, 
strftime('%m', date) as month,
sum(total) as total_sales

from Fact_Sales s 

join Dim_Branch b on s.branch = b.branch
where strftime('%Y', date)= '2019'
group by s.branch, strftime('%m', date)
order by s.branch, strftime('%m', date)


