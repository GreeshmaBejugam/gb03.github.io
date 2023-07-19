-- exercise 1 ( implicit )
-- select s.name as shipper,
-- 	p.name as product
-- from shippers s , products p
-- order by s.name

-- exercise 2 (Explicit)
-- select s.name as shipper,
-- 	p.name as product
-- from shippers s 
-- cross join  products p
-- order by s.name

-- exercise 3
-- select customer_id,
-- 	first_name,
--     points,
--     'Bronze' as type
-- from customers c
-- where points < 2000
-- union
-- select customer_id,
-- 	first_name,
--     points,
--     'Silver' as type
-- from customers c
-- where points between 2000 and  3000
-- union 
-- select customer_id,
-- 	first_name,
--     points,
--     'Gold' as type
-- from customers c
-- where points > 3000

-- exercise - 4
-- Insert into products (name, quantity_in_stock, unit_price)
-- values ('N1',467,20),
-- 		('N2',679,89),
--         ('N3',4678,20)

-- exercise 5

-- create table invoice_archived as 
-- select * from invoices

-- create table invoice_archived2
-- Select i.invoice_id,
-- 	i.number,
--     c.name as client,
--     i.invoice_total,
--     i.payment_total,
--     i.invoice_date,
--      i.payment_date,
--      i.due_date
--     
-- from invoices i
-- join clients c on c.client_id = i.client_id
-- where payment_date is not null 












