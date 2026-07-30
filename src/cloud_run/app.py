import os
import json
import random
import time
from datetime import datetime, timezone
from flask import Flask, jsonify, request
from google.cloud import pubsub_v1
from faker import Faker

app = Flask(__name__)
fake = Faker('pt_BR')

PROJECT_ID = os.environ.get("GCP_PROJECT", os.environ.get("PROJECT_ID", "brk-demo-gcp"))
TOPIC_ID = os.environ.get("PUBSUB_TOPIC", "brk-telemetry-events")

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

# Unidades operacionais e estações de tratamento da BRK Ambiental
UNIDADES_OPERACIONAIS = [
    {"estado": "SP", "cidade": "Limeira", "unidade": "ETA-Limeira-Central"},
    {"estado": "SP", "cidade": "Sumaré", "unidade": "ETA-Sumare-Praiatur"},
    {"estado": "PE", "cidade": "Recife", "unidade": "ETE-Cabanga"},
    {"estado": "AL", "cidade": "Maceió", "unidade": "ETA-Benedito-Bentes"},
    {"estado": "TO", "cidade": "Palmas", "unidade": "ETA-Palmas-Sul"},
    {"estado": "RS", "cidade": "Uruguaiana", "unidade": "ETA-Uruguaiana-Rio-Uruguai"}
]

PARAMETROS_QUALIDADE = {
    "pressao_psi": (25.0, 75.0),       # Pressão normal em PSI na rede
    "vazao_lps": (10.0, 150.0),        # Vazão em Litros por segundo
    "ph_agua": (6.5, 8.5),             # pH padrão potável
    "turbidez_unt": (0.1, 5.0),        # Turbidez da água (UNT)
    "cloro_residual_mg_l": (0.2, 2.0)  # Cloro residual livre (mg/L)
}

def generate_telemetry_event():
    unidade = random.choice(UNIDADES_OPERACIONAIS)
    status_sensor = random.choices(
        population=["NORMAL", "ALERTA_PRESSAO", "ALERTA_VAZAO", "MANUTENCAO_REQUERIDA"],
        weights=[0.85, 0.05, 0.05, 0.05]
    )[0]

    # Introduz variação caso o status não seja NORMAL
    pressao_mult = 1.4 if status_sensor == "ALERTA_PRESSAO" else 1.0
    vazao_mult = 0.3 if status_sensor == "ALERTA_VAZAO" else 1.0

    payload = {
        "event_id": fake.uuid4(),
        "hidrometro_id": f"BRK-SENS-{random.randint(10000, 99999)}",
        "unidade_operacional": unidade["unidade"],
        "cidade": unidade["cidade"],
        "estado": unidade["estado"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "leitura": {
            "vazao_lps": round(random.uniform(*PARAMETROS_QUALIDADE["vazao_lps"]) * vazao_mult, 2),
            "pressao_psi": round(random.uniform(*PARAMETROS_QUALIDADE["pressao_psi"]) * pressao_mult, 2),
            "ph_agua": round(random.uniform(*PARAMETROS_QUALIDADE["ph_agua"]), 2),
            "turbidez_unt": round(random.uniform(*PARAMETROS_QUALIDADE["turbidez_unt"]), 2),
            "cloro_residual_mg_l": round(random.uniform(*PARAMETROS_QUALIDADE["cloro_residual_mg_l"]), 2)
        },
        "status_sistema": status_sensor,
        "operador_responsavel": fake.name()
    }
    return payload

@app.route("/", methods=["GET"])
def health_check():
    return jsonify({"status": "healthy", "service": "BRK Telemetry Generator"}), 200

@app.route("/publish", methods=["POST"])
def publish_event():
    try:
        data = request.get_json(silent=True) or {}
        count = data.get("count", 1)
        count = min(max(1, int(count)), 100) # Limita entre 1 e 100 eventos por requisição

        published_ids = []
        for _ in range(count):
            event = generate_telemetry_event()
            data_bytes = json.dumps(event).encode("utf-8")
            future = publisher.publish(topic_path, data_bytes)
            published_ids.append(future.result())

        return jsonify({
            "message": f"{count} eventos de telemetria BRK enviados com sucesso para o Pub/Sub.",
            "topic": TOPIC_ID,
            "published_message_ids": published_ids
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
