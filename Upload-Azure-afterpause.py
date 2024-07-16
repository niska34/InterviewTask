import json
import pyodbc
import datetime

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=denisa-dtb.database.windows.net;"
    "DATABASE=DenisaProjects;"
    "UID=dmehesov;"
    "PWD=******;"  # add password before running
)

cursor = conn.cursor()


# Function to convert Unix timestamp to datetime object
def unix_timestamp_to_datetime(unix_timestamp):
    return datetime.datetime.fromtimestamp(unix_timestamp, datetime.timezone.utc)


# Open and read the ndjson file
with open("data.ndjson", "r") as file:
    for line in file:
        data = json.loads(line.strip())

        # Insert into Users table using MERGE (only insert if not exists)
        user_id = data["user"]["id"]
        user_name = data["user"]["name"]
        city = data["user"]["city"]
        cursor.execute(
            """
            MERGE Users AS target
            USING (SELECT ? AS user_id, ? AS user_name, ? AS city) AS source
            ON (target.user_id = source.user_id)
            WHEN NOT MATCHED THEN
                INSERT (user_id, user_name, city)
                VALUES (source.user_id, source.user_name, source.city);
            """,
            user_id,
            user_name,
            city,
        )

        # Insert into Products table using MERGE (only insert if not exists)
        for product in data["products"]:
            product_id = product["id"]
            product_name = product["name"]
            price = product["price"]
            cursor.execute(
                """
                MERGE Products AS target
                USING (SELECT ? AS product_id, ? AS product_name, ? AS price) AS source
                ON (target.product_id = source.product_id)
                WHEN NOT MATCHED THEN
                    INSERT (product_id, product_name, price)
                    VALUES (source.product_id, source.product_name, source.price);
                """,
                product_id,
                product_name,
                price,
            )

        # Convert Unix timestamp to datetime object
        created_unix_timestamp = data["created"]
        created_datetime = unix_timestamp_to_datetime(created_unix_timestamp)

        # Insert into Orders table using MERGE (only insert if not exists)
        order_id = data["id"]
        cursor.execute(
            """
            MERGE Orders AS target
            USING (SELECT ? AS order_id, ? AS created, ? AS user_id) AS source
            ON (target.order_id = source.order_id)
            WHEN NOT MATCHED THEN
                INSERT (order_id, created, user_id)
                VALUES (source.order_id, source.created, source.user_id);
            """,
            order_id,
            created_datetime,
            user_id,
        )

        # Calculate unique product counts per order
        unique_products = {}  # Dictionary to store unique product counts

        for product in data["products"]:
            product_id = product["id"]
            if product_id in unique_products:
                unique_products[product_id] += 1
            else:
                unique_products[product_id] = 1

        # Insert into Order_Products table with calculated quantities
        for product_id, quantity in unique_products.items():
            cursor.execute(
                """
                IF NOT EXISTS (SELECT 1 FROM Order_Products WHERE order_id = ? AND product_id = ?)
                BEGIN
                    INSERT INTO Order_Products (order_id, product_id, quantity)
                    VALUES (?, ?, ?)
                END
                """,
                order_id,
                product_id,
                order_id,
                product_id,
                quantity,
            )

        # Commit after each order to ensure data integrity
        conn.commit()

# Close cursor and connection
cursor.close()
conn.close()
