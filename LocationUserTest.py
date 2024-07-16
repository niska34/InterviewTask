import ndjson

def extract_users_and_cities(file_path):
    user_city_pairs = set()

    with open(file_path, "r") as f:
        data = ndjson.load(f)
        for entry in data:
            user_id = entry["user"]["id"]
            city = entry["user"]["city"]
            user_city_pairs.add((user_id, city))

    return user_city_pairs

# Example usage
file_path = "data.ndjson"
user_city_pairs = extract_users_and_cities(file_path)

print("Unique User-City Combinations:")
for user_id, city in user_city_pairs:
    print(f"User ID: {user_id}, City: {city}")
