from confluent_kafka import Producer
import json, time, random
from datetime import datetime, timezone

# Configuration Kafka
conf = {'bootstrap.servers': "127.0.0.1:9092"}
producer = Producer(conf)

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def delivery_report(err, msg):
    if err is not None:
        print(f"Erreur d'envoi: {err}")

def main():
    print("Démarrage de la simulation SmartCity (Transport, Air, Incidents)")
    cities = ["Marrakech", "Agadir", "Rabat","Tanger"]
    lines = ["L1", "L3", "L5", "L9"]

    try:
        while True:
            city = random.choice(cities)
            # 1. Génération Transport
            transport_event = {
                "vehicle_id": f"BUS_{random.randint(1, 20)}",
                "line": random.choice(lines),
                "city": city,
                "lat": round(random.uniform(31.0, 32.0), 4),
                "lon": round(random.uniform(-8.5, -7.5), 4),
                "speed": round(random.uniform(10, 60), 1),
                "delay_min": random.choice([0, 0, 2, 5, 8, 12]),  # Génère parfois du retard
                "ts": now_iso()
            }
            producer.produce('transport_events', value=json.dumps(transport_event).encode('utf-8'),
                             callback=delivery_report)

            # 2. Génération Qualité de l'Air
            air_event = {
                "sensor_id": f"AQ_{random.randint(1, 10)}",
                "city": city,
                "pm25": round(random.uniform(5.0, 50.0), 1),  # Parfois pollué > 35
                "co2": round(random.uniform(400, 1000), 0),  # Parfois pollué > 900
                "ts": now_iso()
            }
            producer.produce('air_quality', value=json.dumps(air_event).encode('utf-8'), callback=delivery_report)

            # 3. Génération Incidents (plus rare)
            if random.random() < 0.2:  # 20% de chance d'avoir un incident
                incident_event = {
                    "incident_id": f"INC_{random.randint(100, 999)}",
                    "city": city,
                    "type": random.choice(["ACCIDENT", "ROUTE_FERMEE", "TRAVAUX"]),
                    "severity": random.randint(1, 5),  # Sévérité de 1 à 5
                    "ts": now_iso()
                }
                producer.produce('incidents', value=json.dumps(incident_event).encode('utf-8'),
                                 callback=delivery_report)

            producer.poll(0)
            print(f"✅ Données envoyées pour {city}")
            time.sleep(1)  # Pause d'une seconde entre chaque cycle

    except KeyboardInterrupt:
        print("\nArrêt de la simulation.")
    finally:
        producer.flush()

if __name__ == "__main__":
    main()