import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

rng = random.Random(42)
previous = None

for i in range(1_000_000):
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

    # '#'-separated Kafka key and JSON value. Couldn't get tab \t to split on the cli very well
    print(
        event["deviceId"] + "#" + json.dumps(event),
        flush=True,
    )
    time.sleep(0.2)  # 5 records per second
