# etl_pipeline.py
import requests
import json

# === Fonction 1 : Récupérer les événements depuis l'API ===
def fetch_all_events(total_expected=2962, page_size=100):
    url_base = "https://opendata.paris.fr/api/v2/catalog/datasets/que-faire-a-paris-/records"
    all_events = []

    for start in range(0, total_expected, page_size):
        url = f"{url_base}?limit={page_size}&start={start}"
        response = requests.get(url)

        if response.status_code == 200:
            records = response.json()["records"]
            events = [record["record"]["fields"] for record in records]
            all_events.extend(events)
            print(f"✅ {len(events)} événements récupérés (start={start})")
        else:
            print(f"❌ Erreur API (code {response.status_code}) à start={start}")
            break

    print(f"🔁 Total récupéré : {len(all_events)} événements")
    return all_events

# === Fonction 2 : Sauvegarder les événements bruts dans un fichier JSON ===
def save_raw_data(events, filename="raw_data.json"):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(events, f, ensure_ascii=False, indent=4)

# === Fonction 3 : Charger les données depuis le fichier JSON ===
def load_raw_data(filename="raw_data.json"):
    try:
        with open(filename, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        print("❌ Fichier non trouvé : raw_data.json")
        return []

# === Fonction 4 : Nettoyer les données ===
def clean_events(events):
    return [clean_event(event) for event in events]
def clean_event(event):
    try:
        locations = event.get("locations")
        location = locations[0] if locations and isinstance(locations, list) and len(locations) > 0 else {}

        latlon = event.get("lat_lon", {}) or {}
        latitude = latlon.get("lat")
        longitude = latlon.get("lon")

        return {
            "title": event.get("title", ""),
            "description": event.get("description", ""),
            "date_start": event.get("date_start", ""),
            "date_end": event.get("date_end", ""),
            "latitude": latitude,
            "longitude": longitude,
            "cover_url": event.get("cover_url", ""),
            "address_name": location.get("address_name") or event.get("address_name", ""),
            "address_street": location.get("address_street") or event.get("address_street", ""),
            "address_zipcode": location.get("address_zipCode") or event.get("address_zipcode", ""),
            "address_city": location.get("address_city") or event.get("address_city", "Paris"),
        }
    except Exception as e:
        print(f"⚠️ Erreur de nettoyage : {e}")
        return None
    # Étape 4 : Enregistrer les données nettoyées dans MongoDB
    # Remplace 'uri' par ton URI MongoDB Atlas si besoin



from pymongo import MongoClient


# === Fonction 5 : Enregistrer les événements nettoyés dans MongoDB ===
def save_to_mongodb(events, uri="mongodb://localhost:27017/", db_name="paris_events", collection_name="events"):
    try:
        # Connexion au client MongoDB
        client = MongoClient(uri)

        # Sélection de la base de données
        db = client[db_name]

        # Sélection de la collection
        collection = db[collection_name]

        # Insertion des documents (événements)
        if events:
            result = collection.insert_many(events)
            print(f"✅ {len(result.inserted_ids)} documents insérés dans MongoDB.")
        else:
            print("⚠️ Pas d'événements à insérer.")

        # Fermeture de la connexion
        client.close()
    except Exception as e:
        print(f"❌ Erreur lors de l'insertion MongoDB : {e}")
from pymongo import MongoClient
import json

def export_events_to_json(uri="mongodb://localhost:27017/", db_name="paris_events", collection_name="events", filename="events_map.json"):
    try:
        client = MongoClient(uri)
        db = client[db_name]
        collection = db[collection_name]

        events_cursor = collection.find({}, {"_id": 0, "title": 1, "latitude": 1, "longitude": 1, "address_name": 1, "description": 1})
        events_list = list(events_cursor)

        # Sauvegarde dans un fichier JSON
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(events_list, f, ensure_ascii=False, indent=4)

        print(f"✅ Exporté {len(events_list)} événements dans {filename}")
        client.close()
    except Exception as e:
        print(f"❌ Erreur export JSON : {e}")



# === Bloc principal d'exécution ===
if __name__ == "__main__":
    # Étape 1 : Récupérer les événements
    events = fetch_all_events(total_expected=2962, page_size=100)
    # Étape 2 : Sauvegarder les données brutes
    save_raw_data(events)

    # Étape 2.1 : Lire le fichier JSON brut
    data = load_raw_data()
    print(f"Nombre d’événements chargés : {len(data)}")

    if data:
        print("Premier événement (brut) :")
        print(json.dumps(data[0], indent=4, ensure_ascii=False))

    # Étape 3 : Nettoyage des données
    cleaned = clean_events(data)
    print(f"Nombre d’événements nettoyés : {len(cleaned)}")
    print("Premier événement nettoyé :")
    print(json.dumps(cleaned[0], indent=4, ensure_ascii=False))

    # Étape 4 : Insérer les événements dans MongoDB
    save_to_mongodb(cleaned, uri="mongodb://localhost:27017/")

    # Étape 5 : Exporter les événements dans un fichier JSON pour Leaflet
    export_events_to_json(uri="mongodb://localhost:27017/")

