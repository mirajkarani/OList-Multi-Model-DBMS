import pymssql
import json
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

client = WasbHook(wasb_conn_id="OListJSON")

conn = pymssql.connect(
        server="***.**.**.*",
        database="olist",
        user="aniket",
        password="*********",
        port="1433"
)


def top_100_prod():
    query = f"""
    SELECT CAST
    ((  
        SELECT
            p.product_id as "id",
            p.product_category_name as "Product Name",
            product_weight_g as "Weight",
            product_length_cm as "Length",
            (
                SELECT
                    order_id as "OrderId",
                    COUNT(order_id) as "Qty",
                    SUM(price) as "TotalCost"
                FROM OrderItems oi
                WHERE oi.product_id = p.product_id
                GROUP BY order_id
                FOR JSON PATH
            ) Orders
        FROM Product p
        FOR JSON PATH
    ) AS VARCHAR(MAX));
    """

    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchone()[0]
    decoded_json = json.loads(rows)

    reformatted_json = json.dumps(decoded_json)

    container_name = 'productsales'
    blob_name='ProductSales.json'

    if client.check_for_blob(container_name=container_name,blob_name=blob_name) == True:
        client.delete_file(container_name=container_name,blob_name=blob_name)

    client.load_string(string_data=reformatted_json,
                       container_name=container_name,
                       blob_name=blob_name)
    
def top_100_cust():
    query = f"""
    SELECT CAST
    (( 
		SELECT
            ord.customer_id AS "id",
            g.geolocation_city as "City",
            g.geolocation_state as "State",
            g.geolocation_lat as "Latitude",
            g.geolocation_lng as "Longitude",
            g.geolocation_zip_code_prefix as "Zipcode",
			(
			SELECT
				p.product_id AS "ProductId",
				p.product_category_name AS "ProductName",
				oi.order_id AS "OrderId",
				COUNT(oi.order_id) AS "OrderQty",
				CAST(sum(oi.price) as INT) AS "TotalCost"
			FROM OrderItems oi JOIN Product p ON oi.product_id = p.product_id
                 WHERE ord.order_id = oi.order_id
			GROUP BY p.product_id, p.product_category_name, oi.order_id
			FOR JSON PATH) AS CustomerOrders,
			ord.order_purchase_timestamp as "OrderDate"
		FROM Orders ord JOIN Customer c ON ord.customer_id = c.customer_id 
                JOIN Geolocation g ON c.geolocation_id = g.geolocation_id
        FOR JSON PATH   
    ) AS VARCHAR(MAX));      
    """

    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchone()[0]
    decoded_json = json.loads(rows)

    reformatted_json = json.dumps(decoded_json)

    container_name = 'customerorders' 
    blob_name='CustomerOrders.json'

    if client.check_for_blob(container_name=container_name,blob_name=blob_name) == True:
        client.delete_file(container_name=container_name,blob_name=blob_name) 

    client.load_string(string_data=reformatted_json,
                       container_name=container_name,
                       blob_name=blob_name)


def seller_by_geolocation():
    query = f"""
    SELECT CAST
    ((
        SELECT
            s.seller_id as "id",
            g.geolocation_city as "City",
            g.geolocation_state as "State",
            g.geolocation_lat as "Latitude",
            g.geolocation_lng as "Longitude",
            g.geolocation_zip_code_prefix as "Zipcode",
            (
                SELECT 
                    order_id AS "OrderId",
                    p2.product_id AS "ProductId",
                    product_category_name AS "ProductName",
                    COUNT(order_id) AS "OrderQuantity",
                    CAST(SUM(price) AS INT) AS "TotalCost"
                FROM OrderItems oi JOIN Product p2 
                ON oi.product_id = p2.product_id
                WHERE oi.seller_id  = s.seller_id 
                GROUP BY seller_id, order_id, p2.product_id, product_category_name
                FOR JSON PATH
            ) Orders
        FROM Seller s 
        JOIN Geolocation g 
        ON s.geolocation_id = g.geolocation_id
        WHERE g.geolocation_city != 'Unknown'
        FOR JSON PATH
    ) AS VARCHAR(MAX));
    """
    
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchone()[0]
    decoded_json = json.loads(rows)

    reformatted_json = json.dumps(decoded_json)

    container_name='sellergeo' 
    blob_name='SellerByLocation.json'

    if client.check_for_blob(container_name=container_name,blob_name=blob_name) == True:
        client.delete_file(container_name=container_name,blob_name=blob_name)   

    client.load_string(string_data=reformatted_json,
                       container_name=container_name,
                       blob_name=blob_name)
    
def cust_payment_methods():
    query = f"""
    SELECT CAST
    ((
        SELECT 
            c.customer_id  as "id",
            g.geolocation_city as "City",
            g.geolocation_state as "State",
            g.geolocation_zip_code_prefix as "Zipcode",
            g.geolocation_lat as "Latitude",
            g.geolocation_lng as "Longitude",
        (
            SELECT
                order_id as "OrderId",
                payment_type as "PaymentType",
                payment_installations  as "PaymentInstallations",
                CAST(payment_value as INT)  as "PaymentValue"
            FROM OrderPayments op 
            WHERE op.order_id = o.order_id
            FOR JSON PATH 
        ) Payments
        FROM Customer c JOIN Geolocation g 
        ON c.geolocation_id  = g.geolocation_id
        JOIN Orders o 
        ON c.customer_id = o.customer_id 
        FOR JSON PATH
    ) AS VARCHAR(MAX));    
    """

    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchone()[0]
    decoded_json = json.loads(rows)

    reformatted_json = json.dumps(decoded_json)

    container_name='customerpayments' 
    blob_name='CustPaymentMethods.json' 

    if client.check_for_blob(container_name=container_name,blob_name=blob_name) == True:
        client.delete_file(container_name=container_name,blob_name=blob_name) 

    client.load_string(string_data=reformatted_json,
                       container_name=container_name,
                       blob_name=blob_name)    
    
def delivery_by_gelocation():
    query = f"""
    SELECT CAST
    ((
        SELECT 
            o.order_id as "id",
            c.customer_id as "CustomerId",
            g.geolocation_city as "City",
            g.geolocation_state as "State",
            g.geolocation_zip_code_prefix as "Zipcode",
            g.geolocation_lat as "Latitude",
            g.geolocation_lng as "Longitude",
            order_purchase_timestamp as "Purchase DateTime",
            order_status as "Status",
            order_delivered_carrier_date as "Delivered Carrier Date",
            order_delivered_customer_date as "Delivered Custimer Date",
            COUNT(oi.order_id) as "OrderQuantity",
            CAST(sum(price) as INT) as "OrderValue"
        FROM Orders o JOIN OrderItems oi ON o.order_id  = oi.order_id
            JOIN Customer c ON o.customer_id = c.customer_id 
            JOIN Geolocation g ON c.geolocation_id = g.geolocation_id 
        GROUP BY o.order_id, c.customer_id, g.geolocation_city, g.geolocation_state, g.geolocation_zip_code_prefix, g.geolocation_lat, g.geolocation_lng,order_purchase_timestamp, order_status, order_delivered_carrier_date, order_delivered_customer_date
        FOR JSON PATH
    ) AS VARCHAR(MAX));    
    """ 

    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchone()[0]
    decoded_json = json.loads(rows)

    reformatted_json = json.dumps(decoded_json)

    container_name='deliverylocation' 
    blob_name='DeliveryByLoc.json' 

    if client.check_for_blob(container_name=container_name,blob_name=blob_name) == True:
        client.delete_file(container_name=container_name,blob_name=blob_name) 

    client.load_string(string_data=reformatted_json,
                       container_name=container_name,
                       blob_name=blob_name)
       
def cust_order_reviews():
    query = f"""   
    SELECT CAST
    ((
        SELECT 
            c.customer_id as "id",
            (
                SELECT 
                    p.product_id as "ProductId",
                    p.product_category_name as "ProductName",
                    oi.order_id  as "OrderId",
                    review_id as "ReviewId",
                    review_score as "Ratings",
                    review_answer_timestamp as "Date"
                FROM Product p JOIN OrderItems oi ON p.product_id = oi.product_id 
                    JOIN OrderReviews or2 ON oi.order_id = or2.order_id 
                WHERE oi.order_id  = o.order_id 
                FOR JSON PATH
            ) ProductsRatings
        FROM Customer c JOIN Orders o 
        ON c.customer_id = o.customer_id
        FOR JSON PATH
    ) AS VARCHAR(MAX)); 
    """

    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchone()[0]
    decoded_json = json.loads(rows)

    reformatted_json = json.dumps(decoded_json)

    container_name='customerreviews'
    blob_name='CustReviews.json'    

    if client.check_for_blob(container_name=container_name,blob_name=blob_name) == True:
        client.delete_file(container_name=container_name,blob_name=blob_name) 

    client.load_string(string_data=reformatted_json,
                       container_name=container_name,
                       blob_name=blob_name)   
