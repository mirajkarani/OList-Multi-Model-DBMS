-- DROP DATABASE olist;

CREATE DATABASE olist;

-- DROP SCHEMA dbo;

CREATE SCHEMA dbo;
-- olist.dbo.Geolocation definition

-- Drop table

-- DROP TABLE olist.dbo.Geolocation;

CREATE TABLE olist.dbo.Geolocation (
	geolocation_id int NOT NULL,
	geolocation_zip_code_prefix varchar(10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	geolocation_lat float NULL,
	geolocation_lng float NULL,
	geolocation_city varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	geolocation_state varchar(10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	CONSTRAINT PK__Geolocat__D8A499469E2693FE PRIMARY KEY (geolocation_id)
);


-- olist.dbo.Product definition

-- Drop table

-- DROP TABLE olist.dbo.Product;

CREATE TABLE olist.dbo.Product (
	product_id varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	product_name_length int NULL,
	product_description_length int NULL,
	product_photos_qty int NULL,
	product_weight_g int NULL,
	product_length_cm int NULL,
	product_height_cm int NULL,
	product_width_cm int NULL,
	product_category_name varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	CONSTRAINT PK__Product__47027DF5F6F5913D PRIMARY KEY (product_id)
);


-- olist.dbo.Customer definition

-- Drop table

-- DROP TABLE olist.dbo.Customer;

CREATE TABLE olist.dbo.Customer (
	customer_id varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	customer_unique_id varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	geolocation_id int NULL,
	CONSTRAINT PK__Customer__CD65CB85D5CBD69D PRIMARY KEY (customer_id),
	CONSTRAINT FK__Customer__geoloc__0F624AF8 FOREIGN KEY (geolocation_id) REFERENCES olist.dbo.Geolocation(geolocation_id)
);


-- olist.dbo.Orders definition

-- Drop table

-- DROP TABLE olist.dbo.Orders;

CREATE TABLE olist.dbo.Orders (
	order_id varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	customer_id varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	order_status varchar(25) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	order_purchase_timestamp datetime NULL,
	order_approved_at datetime NULL,
	order_delivered_carrier_date datetime NULL,
	order_delivered_customer_date datetime NULL,
	order_estimated_delivery_date datetime NULL,
	CONSTRAINT PK__Orders__465962297F6A959E PRIMARY KEY (order_id),
	CONSTRAINT FK__Orders__customer__25518C17 FOREIGN KEY (customer_id) REFERENCES olist.dbo.Customer(customer_id)
);


-- olist.dbo.Seller definition

-- Drop table

-- DROP TABLE olist.dbo.Seller;

CREATE TABLE olist.dbo.Seller (
	seller_id varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	geolocation_id int NULL,
	CONSTRAINT PK__Seller__780A0A97A9864ED2 PRIMARY KEY (seller_id),
	CONSTRAINT FK__Seller__geolocat__18EBB532 FOREIGN KEY (geolocation_id) REFERENCES olist.dbo.Geolocation(geolocation_id)
);


-- olist.dbo.OrderItems definition

-- Drop table

-- DROP TABLE olist.dbo.OrderItems;

CREATE TABLE olist.dbo.OrderItems (
	order_processing_id int NOT NULL,
	order_item_id int NULL,
	order_id varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	product_id varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	seller_id varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	shipping_limit_date date NULL,
	price float NULL,
	freight_value float NULL,
	CONSTRAINT PK__OrderIte__5FD7DC054D9DBE39 PRIMARY KEY (order_processing_id),
	CONSTRAINT FK__OrderItem__order__31B762FC FOREIGN KEY (order_id) REFERENCES olist.dbo.Orders(order_id),
	CONSTRAINT FK__OrderItem__produ__32AB8735 FOREIGN KEY (product_id) REFERENCES olist.dbo.Product(product_id),
	CONSTRAINT FK__OrderItem__selle__339FAB6E FOREIGN KEY (seller_id) REFERENCES olist.dbo.Seller(seller_id)
);


-- olist.dbo.OrderPayments definition

-- Drop table

-- DROP TABLE olist.dbo.OrderPayments;

CREATE TABLE olist.dbo.OrderPayments (
	payment_id int NOT NULL,
	order_id varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	payment_sequential int NULL,
	payment_type varchar(25) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	payment_installations int NULL,
	payment_value float NULL,
	CONSTRAINT PK__OrderPay__ED1FC9EA50E8DED9 PRIMARY KEY (payment_id),
	CONSTRAINT FK__OrderPaym__order__2B0A656D FOREIGN KEY (order_id) REFERENCES olist.dbo.Orders(order_id)
);


-- olist.dbo.OrderReviews definition

-- Drop table

-- DROP TABLE olist.dbo.OrderReviews;

CREATE TABLE olist.dbo.OrderReviews (
	review_id varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	order_id varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	review_score int NULL,
	review_creation_date date NULL,
	review_answer_timestamp datetime NULL,
	CONSTRAINT PK__OrderRev__60883D903671DC35 PRIMARY KEY (review_id),
	CONSTRAINT FK__OrderRevi__order__2EDAF651 FOREIGN KEY (order_id) REFERENCES olist.dbo.Orders(order_id)
);
