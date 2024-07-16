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


# Function to check if a user exists in the Users table
def user_exists(user_id):
    cursor.execute("SELECT COUNT(1) FROM Users WHERE user_id = ?", user_id)
    return cursor.fetchone()[0] > 0


# Function to check if a product exists in the Products table
def product_exists(product_id):
    cursor.execute("SELECT COUNT(1) FROM Products WHERE product_id = ?", product_id)
    return cursor.fetchone()[0] > 0


# Open and read the ndjson file
with open("data.ndjson", "r") as file:
    for line in file:
        data = json.loads(line.strip())

        # Insert into Users table if user does not exist
        user_id = data["user"]["id"]
        user_name = data["user"]["name"]
        city = data["user"]["city"]
        if not user_exists(user_id):
            cursor.execute(
                "INSERT INTO Users (user_id, user_name, city) VALUES (?, ?, ?)",
                user_id,
                user_name,
                city,
            )

        # Insert into Products table if product does not exist
        for product in data["products"]:
            product_id = product["id"]
            product_name = product["name"]
            price = product["price"]
            if not product_exists(product_id):
                cursor.execute(
                    "INSERT INTO Products (product_id, product_name, price) VALUES (?, ?, ?)",
                    product_id,
                    product_name,
                    price,
                )

        # Convert Unix timestamp to datetime object
        created_unix_timestamp = data["created"]
        created_datetime = unix_timestamp_to_datetime(created_unix_timestamp)

        # Insert into Orders table
        order_id = data["id"]
        cursor.execute(
            "INSERT INTO Orders (order_id, created, user_id) VALUES (?, ?, ?)",
            order_id,
            created_datetime,  # Insert datetime object here
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
                "INSERT INTO Order_Products (order_id, product_id, quantity) VALUES (?, ?, ?)",
                order_id,
                product_id,
                quantity,
            )

        # Commit after each order to ensure data integrity
        conn.commit()

# Close cursor and connection
cursor.close()
conn.close()
