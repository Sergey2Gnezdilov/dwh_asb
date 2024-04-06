-- Drop tables if they exist with 
DROP TABLE IF EXISTS stg_Customers;
DROP TABLE IF EXISTS stg_Sub_Categories;
DROP TABLE IF EXISTS stg_Products;
DROP TABLE IF EXISTS stg_Categories;
DROP TABLE IF EXISTS stg_Segments;
DROP TABLE IF EXISTS stg_Person;
DROP TABLE IF EXISTS stg_City;
DROP TABLE IF EXISTS stg_State;
DROP TABLE IF EXISTS stg_Country;
DROP TABLE IF EXISTS stg_Region;

-- Create Region table
CREATE TABLE IF NOT EXISTS stg_region (
  region_id INT,
  region_name VARCHAR(255)
);

-- Create Country table
CREATE TABLE IF NOT EXISTS stg_country (
  country_id INT,
  region_id INT,
  country_name VARCHAR(255)
);

-- Create State table
CREATE TABLE IF NOT EXISTS stg_state (
  state_id INT,
  country_id INT,
  state_name VARCHAR(255)
);

-- Create City table
CREATE TABLE IF NOT EXISTS stg_city (
  city_id INT,
  state_id INT,
  city_name VARCHAR(255)
);

-- Create Person table
CREATE TABLE IF NOT EXISTS stg_person (
  person_id INT ,
  region_id INT ,
  person_name VARCHAR(255) NOT NULL
);

-- Create Segments table
CREATE TABLE IF NOT EXISTS stg_segments (
  segment_id INT,
  segment_name VARCHAR(255) NOT NULL
);

-- Create Categories table
CREATE TABLE IF NOT EXISTS stg_categories (
  category_id INT ,
  category_name VARCHAR(255) NOT NULL
);

-- Create Sub_Categories table
CREATE TABLE IF NOT EXISTS stg_sub_categories (
  sub_category_id INT,
  category_id INT,
  sub_category_name VARCHAR(255) NOT NULL
);

-- Create Products table
CREATE TABLE IF NOT EXISTS stg_products (
 product_key INT,
 product_id VARCHAR(255),
 sub_category_id INT ,
 product_name VARCHAR(255) NOT NULL
);

-- Create Customers table
CREATE TABLE IF NOT EXISTS stg_customers (
  customer_id VARCHAR(255) PRIMARY KEY,
  customer_name VARCHAR(255) NOT NULL,
  segment_id INT REFERENCES Segments(segment_id) NOT NULL
);

-- Drop tables if they exist 
DROP TABLE IF EXISTS stg_Returns;
DROP TABLE IF EXISTS stg_Shipments;
DROP TABLE IF EXISTS stg_Shipment_types;
DROP TABLE IF EXISTS stg_Ord_Detail;
DROP TABLE IF EXISTS stg_Ord_Head;

-- Create Ord_Head table 
CREATE TABLE IF NOT EXISTS stg_ord_head (
  order_id VARCHAR(255) NOT NULL,
  customer_id VARCHAR(255) NOT NULL,
  city_id INT NOT NULL,
  state_id INT NOT NULL,
  order_date DATE NOT NULL
);

-- Create Ord_Detail table 
CREATE TABLE IF NOT EXISTS stg_ord_detail (
  order_id  VARCHAR(255)  NOT NULL,
  product_key INT NOT NULL,
  product_id VARCHAR(255) NOT NULL,
  sales_amt DECIMAL(10,2),
  quantity INT,
  discount DECIMAL(5,2),
  profit DECIMAL(10,2)

);


-- Create Shipment_mode table
CREATE TABLE IF NOT EXISTS stg_shipment_types (
  shipment_type_id INT NOT NULL,
  shipment_mode VARCHAR(255) NOT NULL
);

-- Create Shipments table
CREATE TABLE IF NOT EXISTS stg_shipments (
  shipment_id VARCHAR(255)   NOT NULL,
  order_id VARCHAR(255)  NOT NULL,
  shipment_type_id INT NOT NULL,
  shipment_date DATE NOT NULL,
  ship_desc VARCHAR(255)
);

-- Create Returns table 
CREATE TABLE IF NOT EXISTS stg_returns (
  return_id VARCHAR(255) NOT NULL,
  order_id VARCHAR(255) NOT NULL,
  product_id VARCHAR(255) NOT NULL,
  return_date DATE, 
 product_key INT ,
  return_desc VARCHAR(255), 
  return_reason VARCHAR(255) 
);
