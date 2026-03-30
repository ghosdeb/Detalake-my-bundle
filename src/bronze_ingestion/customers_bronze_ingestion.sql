
-- STEP 1: Ensure Bronze Schema Exists
CREATE SCHEMA IF NOT EXISTS dev_bronze;

-- STEP 2: 
CREATE OR REPLACE TABLE dev_bronze.customers_bronze AS
SELECT 
  CustomerID,
  CustomerName,
  ContactNumber,
  Email,
  Address,
  DateOfBirth,
  RegistrationDate,
  EffectiveStartDate,
  EffectiveEndDate
FROM landing.customers;
  
