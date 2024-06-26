import requests
import json

# URL de base de votre cluster Elasticsearch
base_url = 'http://localhost:9200'  # Remplacez par l'URL de votre cluster

# Nom de l'index que vous souhaitez créer
index_name = 'taxi_data_index'  # Remplacez par le nom de votre index

# Mapping pour spécifier la structure des données
index_mapping = {
    "mappings": {
        "properties": {
            "VendorID": {"type": "integer"},
            "lpep_pickup_datetime": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss"},
            "lpep_dropoff_datetime": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss"},
            "store_and_fwd_flag": {"type": "keyword"},
            "RatecodeID": {"type": "integer"},
            "PULocationID": {"type": "integer"},
            "DOLocationID": {"type": "integer"},
            "passenger_count": {"type": "integer"},
            "trip_distance": {"type": "float"},
            "fare_amount": {"type": "float"},
            "extra": {"type": "float"},
            "mta_tax": {"type": "float"},
            "tip_amount": {"type": "float"},
            "tolls_amount": {"type": "float"},
            "ehail_fee": {"type": "float"},
            "improvement_surcharge": {"type": "float"},
            "total_amount": {"type": "float"},
            "payment_type": {"type": "integer"},
            "trip_type": {"type": "integer"},
            "congestion_surcharge": {"type": "float"}
        }
    }
}

# Fonction pour créer l'index avec le mapping spécifié
def create_index(index_name, mapping):
    url = f"{base_url}/{index_name}"
    headers = {'Content-Type': 'application/json'}
    response = requests.put(url, headers=headers, json=mapping)
    return response.json()

def delete_index(index_name):
    url = f"{base_url}/{index_name}"
    headers = {'Content-Type': 'application/json'}
    response = requests.delete(url, headers=headers)
    return response.json()

# Appel de la fonction pour créer l'index avec mapping
if __name__ == "__main__":
    response = delete_index(index_name)
    print(json.dumps(response, indent=2))
    print(f"Index '{index_name}' supprimé avec succès.")
