-- Drop tables if they exist with CASCADE
DROP TABLE IF EXISTS Customers CASCADE;
DROP TABLE IF EXISTS Sub_Categories CASCADE;
DROP TABLE IF EXISTS Products CASCADE;
DROP TABLE IF EXISTS Categories CASCADE;
DROP TABLE IF EXISTS Segments CASCADE;
DROP TABLE IF EXISTS Person CASCADE;
DROP TABLE IF EXISTS City CASCADE;
DROP TABLE IF EXISTS State CASCADE;
DROP TABLE IF EXISTS Country CASCADE;
DROP TABLE IF EXISTS Region CASCADE;

-- Create Region table
CREATE TABLE IF NOT EXISTS Region (
  region_id INT PRIMARY KEY,
  region_name VARCHAR(255) NOT NULL
);

-- Create Country table
CREATE TABLE IF NOT EXISTS Country (
  country_id INT PRIMARY KEY,
  region_id INT REFERENCES Region(region_id),
  country_name VARCHAR(255) NOT NULL
);

-- Create State table
CREATE TABLE IF NOT EXISTS State (
  state_id INT PRIMARY KEY,
  country_id INT REFERENCES Country(country_id),
  state_name VARCHAR(255) NOT NULL
);

-- Create City table
CREATE TABLE IF NOT EXISTS City (
  city_id INT PRIMARY KEY,
  state_id INT REFERENCES State(state_id),
  city_name VARCHAR(255) NOT NULL
);

-- Create Person table
CREATE TABLE IF NOT EXISTS Person (
  person_id INT PRIMARY KEY,
  region_id INT REFERENCES Region(region_id),
  person_name VARCHAR(255) NOT NULL
);

-- Create Segments table
CREATE TABLE IF NOT EXISTS Segments (
  segment_id INT PRIMARY KEY,
  segment_name VARCHAR(255) NOT NULL
);

-- Create Categories table
CREATE TABLE IF NOT EXISTS Categories (
  category_id INT PRIMARY KEY,
  category_name VARCHAR(255) NOT NULL
);

-- Create Sub_Categories table
CREATE TABLE IF NOT EXISTS Sub_Categories (
  sub_category_id INT PRIMARY KEY,
  category_id INT REFERENCES Categories(category_id) NOT NULL,
  sub_category_name VARCHAR(255) NOT NULL
);

-- Create Products table
CREATE TABLE IF NOT EXISTS Products (
 product_key INT,
product_id VARCHAR(255),
  sub_category_id INT REFERENCES Sub_Categories(sub_category_id),
  product_name VARCHAR(255) NOT NULL
);

-- Create Customers table
CREATE TABLE IF NOT EXISTS Customers (
  customer_id VARCHAR(255) PRIMARY KEY,
  customer_name VARCHAR(255) NOT NULL,
  segment_id INT REFERENCES Segments(segment_id) NOT NULL
);

-- Drop tables if they exist with CASCADE
DROP TABLE IF EXISTS Returns CASCADE;
DROP TABLE IF EXISTS Shipments CASCADE;
DROP TABLE IF EXISTS Shipment_types CASCADE;
DROP TABLE IF EXISTS Ord_Detail CASCADE;
DROP TABLE IF EXISTS Ord_Head CASCADE;

-- Create Ord_Head table (Ord is short for Order)
CREATE TABLE IF NOT EXISTS Ord_Head (
  order_id VARCHAR(255) PRIMARY KEY,
  customer_id VARCHAR(255) REFERENCES Customers(customer_id) NOT NULL,
  city_id INT REFERENCES City(city_id) NOT NULL,
state_id INT REFERENCES state (state_id) NOT NULL,

  order_date DATE NOT NULL
);

-- Create Ord_Detail table 
CREATE TABLE IF NOT EXISTS Ord_Detail (
  ord_det_id INT PRIMARY KEY,
  order_id VARCHAR(255) REFERENCES Ord_Head(order_id),
  product_key INT ,
  product_id VARCHAR(255),
  sales_amt DECIMAL(10,2),
  quantity INT,
  discount DECIMAL(5,2),
  profit DECIMAL(10,2)

);


-- Create Shipment_mode table
CREATE TABLE IF NOT EXISTS Shipment_types (
  shipment_type_id INT PRIMARY KEY,
  shipment_mode VARCHAR(255)
);

-- Create Shipments table
CREATE TABLE IF NOT EXISTS Shipments (
  shipment_id VARCHAR(255)  PRIMARY KEY,
  order_id VARCHAR(255) REFERENCES Ord_Head(order_id),
  shipment_type_id INT REFERENCES Shipment_types(shipment_type_id),
  shipment_date DATE,
  ship_desc VARCHAR(255)
);

-- Create Returns table 
CREATE TABLE IF NOT EXISTS Returns (
  return_id VARCHAR(255) PRIMARY KEY,
  order_id VARCHAR(255) REFERENCES Ord_Head(order_id),
  product_id VARCHAR(255) , -- optional
  return_date DATE, -- optional
 product_key INT ,
  return_desc VARCHAR(255), -- optional
  return_reason VARCHAR(255) -- optional
);

