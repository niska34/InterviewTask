import json

file_path = "data.ndjson"
product_id_to_find = 11
product_name_to_find = "Product L"
city_to_find = "Brno"

# Initialize a counter to keep track of occurrences
count_product_in_city = 0

# Open and read the ndjson file
with open(file_path, "r") as file:
    for line in file:
        data = json.loads(line.strip())

        # Check if the city matches "Brno"
        if data["user"]["city"] == city_to_find:
            # Iterate through products to find Product L with id 11
            for product in data["products"]:
                if (
                    product["id"] == product_id_to_find
                    and product["name"] == product_name_to_find
                ):
                    count_product_in_city += 1

# Print the result
print(
    f"Number of times Product L (id {product_id_to_find}) appears in {city_to_find}: {count_product_in_city}"
)
