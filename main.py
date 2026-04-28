import time
import json
import random
import os
from kafka import KafkaProducer

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
TOPIC = os.getenv('KAFKA_TOPIC', 'health_telemetry')

PATIENTS = [
    {"id": "P-001", "condition": "Healthy", "hr": 75, "sys": 120, "dia": 80},
    {"id": "P-002", "condition": "Hypertension", "hr": 85, "sys": 150, "dia": 95},
    {"id": "P-003", "condition": "Bradycardia", "hr": 50, "sys": 110, "dia": 70},
    {"id": "P-004", "condition": "Tachycardia", "hr": 115, "sys": 130, "dia": 85},
    {"id": "P-005", "condition": "Athlete", "hr": 45, "sys": 115, "dia": 75},
    {"id": "P-006", "condition": "Hypotension", "hr": 70, "sys": 90, "dia": 60},
    {"id": "P-007", "condition": "Atrial Fibrillation", "hr": 95, "sys": 125, "dia": 80},
    {"id": "P-008", "condition": "Healthy Senior", "hr": 65, "sys": 135, "dia": 85},
    {"id": "P-009", "condition": "Stress/Anxiety", "hr": 105, "sys": 140, "dia": 90},
    {"id": "P-010", "condition": "Sleep Apnea", "hr": 80, "sys": 145, "dia": 95},
    {"id": "P-011", "condition": "Healthy", "hr": 72, "sys": 118, "dia": 78},
    {"id": "P-012", "condition": "Diabetic", "hr": 82, "sys": 138, "dia": 88},
    {"id": "P-013", "condition": "Post-Surgery", "hr": 90, "sys": 115, "dia": 75},
    {"id": "P-014", "condition": "Arrhythmia", "hr": 78, "sys": 122, "dia": 80},
    {"id": "P-015", "condition": "Pregnancy", "hr": 88, "sys": 110, "dia": 70},
    {"id": "P-016", "condition": "Healthy", "hr": 76, "sys": 121, "dia": 81},
    {"id": "P-017", "condition": "Obesity", "hr": 92, "sys": 142, "dia": 92},
    {"id": "P-018", "condition": "Smoker", "hr": 86, "sys": 136, "dia": 86},
    {"id": "P-019", "condition": "Asthma", "hr": 98, "sys": 128, "dia": 82},
    {"id": "P-020", "condition": "Thyroid Issue", "hr": 102, "sys": 132, "dia": 84},
]

def generate_telemetry():
    """Simulates physiological data for multiple patients."""
    patient = random.choice(PATIENTS)
    
    base_hr = patient["hr"]
    base_sys_bp = patient["sys"]
    base_dia_bp = patient["dia"]
    
    # Introduce sudden spikes 5% of the time (cardiovascular spikes)
    if random.random() < 0.05:
        return {
            "patient_id": patient["id"],
            "condition": patient["condition"],
            "heart_rate": base_hr + random.uniform(40, 80), # Spike
            "systolic_bp": base_sys_bp + random.uniform(30, 60),
            "diastolic_bp": base_dia_bp + random.uniform(20, 40),
            "timestamp": time.time(),
            "type": "spike"
        }
    else:
        # Normal fluctuation (varies based on condition, Arrhythmia has more variance)
        variance = 15 if patient["condition"] in ["Arrhythmia", "Atrial Fibrillation"] else 5
        return {
            "patient_id": patient["id"],
            "condition": patient["condition"],
            "heart_rate": base_hr + random.uniform(-variance, variance),
            "systolic_bp": base_sys_bp + random.uniform(-variance, variance),
            "diastolic_bp": base_dia_bp + random.uniform(-variance/2, variance/2),
            "timestamp": time.time(),
            "type": "normal"
        }

def main():
    print(f"Connecting to Kafka at {KAFKA_BROKER}")
    # Wait for Kafka to be ready
    producer = None
    while producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
        except Exception as e:
            print(f"Waiting for Kafka: {e}")
            time.sleep(5)
            
    print("Connected to Kafka. Starting data generation...")
    while True:
        data = generate_telemetry()
        producer.send(TOPIC, value=data)
        print(f"Sent telemetry: {data}")
        time.sleep(1) # 1 event per second

if __name__ == "__main__":
    main()
