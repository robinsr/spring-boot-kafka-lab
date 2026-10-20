import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

rng = random.Random(42)
previous = None

for i in range(1_000):
    # Every twentieth record repeats the previous event exactly.
    if i > 0 and i % 20 == 0:
        event = previous
    else:
        now = datetime.now(timezone.utc)
        delay = 300 if i % 10 == 0 else 0

        event = {
            "schemaVersion": 1,
            "eventId": str(uuid.uuid4()),
            "deviceId": f"sensor-{rng.randrange(10):02d}",
            "occurredAt": (now - timedelta(seconds=delay)).isoformat(),
            "temperatureC": round(rng.uniform(20, 110), 1),
        }
        previous = event

    # Tab-separated Kafka key and JSON value.
    print(
        event["deviceId"] + "\t" + json.dumps(event),
        flush=True,
    )
    # time.sleep(0.1)  # Approximately 10 records/second
    time.sleep(2.0)  # Approximately 2 records per 2 second
