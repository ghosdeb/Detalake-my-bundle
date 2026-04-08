create streaming table deltalake_catalog.bronze_ingestion.orders_bronze
as
select *,
_metadata.file_name as file_name,
current_timestamp() as ingestion_ts
 from 
 cloud_files('/Volumes/deltalake_catalog/ingestion/raw_vol/input/orders/','csv',map("cloudFiles.inferColumnTypes","true"));

create streaming table deltalake_catalog.bronze_ingestion.customers_bronze
as
select *,
_metadata.file_name as file_name,
current_timestamp() as ingestion_ts
 from cloud_files("/Volumes/deltalake_catalog/ingestion/raw_vol/input/customers/","csv",map("cloudFiles.inferColumnTypes","true"));


create streaming table deltalake_catalog.silver_cleaned.orders_silver_cleaned(
  constraint valid_order expect (order_id is not null) on violation drop row,
  constraint valid_customer expect (customer_id is not null) on violation drop row
)
as
select 
orderid as order_id,
orderdate as order_date,
customerid as customer_id,
totalamount as total_amount,
status,
file_name as file_name,
ingestion_ts as ingestion_ts
from stream(deltalake_catalog.bronze_ingestion.orders_bronze);

create streaming table deltalake_catalog.silver_cleaned.customers_silver_cleaned(
  constraint valid_customer expect (customer_id is not null) on violation drop row
)
as
select 
customerid as customer_id,
customername as customer_name,
contactnumber as contact_number,
email as email,
address as city,
dateofbirth as dob,
registrationdate as customer_since,
file_name as file_name,
ingestion_ts as ingestion_ts
from stream(deltalake_catalog.bronze_ingestion.customers_bronze);

create streaming table deltalake_catalog.silver_cleaned.customers_silver;
apply changes into deltalake_catalog.silver_cleaned.customers_silver
from stream(deltalake_catalog.silver_cleaned.customers_silver_cleaned)
keys(customer_id)
sequence by ingestion_ts
stored as scd type 2;

create streaming table deltalake_catalog.silver_cleaned.orders_silver;

create flow orders_silver_flow as auto cdc into deltalake_catalog.silver_cleaned.orders_silver
from stream(deltalake_catalog.silver_cleaned.orders_silver_cleaned)
keys(order_id)
sequence by ingestion_ts;


create materialized view deltalake_catalog.gold_aggregated.city_wise_sales_aggregated_gold
as
select 
c.city,
round(sum(o.total_amount),2) as total_sales
from deltalake_catalog.silver_cleaned.customers_silver as c,
deltalake_catalog.silver_cleaned.orders_silver as o
where c.customer_id = o.customer_id
group by c.city;