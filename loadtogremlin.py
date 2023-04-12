import pymssql
import json
from gremlin_python.driver import client, serializer
from gremlin_python.driver.aiohttp.transport import AiohttpTransport

conn = pymssql.connect(
        server="172.22.80.1",
        database="olist",
        user="aniket",
        password="root@1234",
        port="1433"
)

client = client.Client('wss://olist-graph-db.gremlin.cosmosdb.azure.com:443/','g', 
             username="/dbs/db-olist/colls/graph-olist",
             password="vZv7Yp1zWqH3gfsUlu68Q9qoJwmLwleuUjLZ4UCDyO2g1Pf0zR3s9bDYa2g0WLTdVLeXnDw039OyACDbLTVg3Q==",
             message_serializer=serializer.GraphSONSerializersV2d0(),
             transport_factory=lambda: AiohttpTransport(call_from_event_loop=True)
)


def graph_customer():
    
    query = f"""
    SELECT CAST
    ((
        SELECT
            *
        FROM Customer
        FOR JSON PATH)
    AS VARCHAR(MAX));     
    """

    cursor = conn.cursor()
    cursor.execute(query)

    rows = cursor.fetchone()[0]
    decoded_json = json.loads(rows)

    reformatted_json = json.dumps(decoded_json)

    for obj in reformatted_json:
    
        query_vertex = f"g.addV('customer').property('id', '{obj['customer_id']}').property('customer_unique_id', '{obj['customer_unique_id']}').property('geolocation_id', '{obj['geolocation_id']}').property('pk','pk')"
        client.submitAsync(query_vertex)


def graph_orders():
    
    query = f"""
    SELECT CAST
    ((
        SELECT
            *
        FROM Orders
        FOR JSON PATH)
    AS VARCHAR(MAX));     
    """

    cursor = conn.cursor()
    cursor.execute(query)

    rows = cursor.fetchone()[0]
    decoded_json = json.loads(rows)

    reformatted_json = json.dumps(decoded_json)

    for obj in reformatted_json:
        query_vertex = f"g.addV('order').property('id', '{obj.get('order_id')}') \
                .property('customer_id', '{obj.get('customer_id')}') \
                .property('order_status', '{obj.get('order_status') or 'Unknown'}') \
                .property('order_purchase_timestamp', '{obj.get('order_purchase_timestamp')}') \
                .property('order_approved_at', '{obj.get('order_approved_at')}') \
                .property('order_delivered_carrier_date', '{obj.get('order_delivered_carrier_date')}') \
                .property('order_delivered_customer_date', '{obj.get('order_delivered_customer_date')}') \
                .property('order_estimated_delivery_date', '{obj.get('order_estimated_delivery_date')}') \
                .property('pk','pk')"
        
        client.submit(query_vertex)
        
        query_edge = f"g.V().hasLabel('customer').has('id', '{obj.get('customer_id')}').addE('placeOrder').to(g.V().hasLabel('order').has('id', '{obj.get('order_id')}'))"
        
        client.submit(query_edge)

def graph_product():
    
    query = f"""
    SELECT CAST
    ((
        SELECT
            *
        FROM Product
        FOR JSON PATH)
    AS VARCHAR(MAX));     
    """

    cursor = conn.cursor()
    cursor.execute(query)

    rows = cursor.fetchone()[0]
    decoded_json = json.loads(rows)

    reformatted_json = json.dumps(decoded_json)    

    for obj in reformatted_json:
    
        query_vertex = (f"g.addV('product').property('id', '{obj.get('product_id')}')"
                        f".property('product_name_length', '{obj.get('product_name_length') or -99}')"
                        f".property('product_description_length', '{obj.get('product_description_length') or -99}')"
                        f".property('product_photos_qty', '{obj.get('product_photos_qty') or -99}')"
                        f".property('product_weight_g', '{obj.get('product_weight_g') or -99}')"
                        f".property('product_length_cm', '{obj.get('product_length_cm') or -99}')"
                        f".property('product_height_cm', '{obj.get('product_height_cm') or -99}')"
                        f".property('product_width_cm', '{obj.get('product_width_cm') or -99}')"
                        f".property('product_category_name', '{obj.get('product_category_name') or 'Unknown'}')"
                        f".property('pk','pk')")

        client.submitAsync(query_vertex)

def graph_reviews():

    query = f"""
    SELECT CAST
    ((
        SELECT
            *
        FROM OrderReviews
        FOR JSON PATH)
    AS VARCHAR(MAX));     
    """

    cursor = conn.cursor()
    cursor.execute(query)

    rows = cursor.fetchone()[0]
    decoded_json = json.loads(rows)

    reformatted_json = json.dumps(decoded_json) 

    for obj in reformatted_json:
    
        query_vertex = f"g.addV('review').property('id', '{obj.get('review_id')}') \
                    .property('order_id', '{obj.get('order_id')}') \
                    .property('review_score', '{obj.get('review_score') or 0}') \
                    .property('review_creation_date', '{obj.get('review_creation_date') or 'Unknown'}') \
                    .property('review_answer_timestamp', '{obj.get('review_answer_timestamp') or 'Unknown'}') \
                    .property('pk','pk')"
        
        client.submit(query_vertex)
        
        query_edge = f"g.V().hasLabel('order').has('id', '{obj.get('order_id')}').addE('hasReviews').to(g.V().hasLabel('review').has('id', '{obj.get('review_id')}'))"
        
        client.submit(query_edge)

def graph_orderItems():

    query = f"""
    SELECT CAST
    ((
        SELECT
            *
        FROM OrderItems
        FOR JSON PATH)
    AS VARCHAR(MAX));     
    """

    cursor = conn.cursor()
    cursor.execute(query)

    rows = cursor.fetchone()[0]
    decoded_json = json.loads(rows)

    reformatted_json = json.dumps(decoded_json) 

    for obj in reformatted_json:
    
        query_vertex = f"g.addV('orderItem').property('id', '{obj.get('order_processing_id')}') \
                        .property('order_item_id', '{obj.get('order_item_id') or 1}') \
                        .property('order_id', '{obj.get('order_id')}') \
                        .property('product_id', '{obj.get('product_id') or 'Unknown'}') \
                        .property('seller_id', '{obj.get('seller_id') or 'Unknown'}') \
                        .property('shipping_limit_date', '{obj.get('shipping_limit_date') or 'Unknown'}') \
                        .property('price', '{obj.get('price') or 0}') \
                        .property('freight_value', '{obj.get('freight_value') or 0}') \
                        .property('pk','pk')"
        
        client.submit(query_vertex)
        
        query_edge1 = f"g.V().hasLabel('order').has('id', '{obj.get('order_id')}').addE('containItems').to(g.V().hasLabel('orderItem').has('id', '{obj.get('order_processing_id')}'))"
        
        client.submit(query_edge1)

        query_edge2 = f"g.V().hasLabel('orderItem').has('id', '{obj.get('order_processing_id')}').addE('hasProducts').to(g.V().hasLabel('product').has('id', '{obj.get('product_id')}'))"
        
        client.submit(query_edge2)


#####################################
############ MAIN SCRIPT ############
#####################################

#Calling the functions
graph_customer()
graph_orderItems()
graph_orders()
graph_product()
graph_reviews()
