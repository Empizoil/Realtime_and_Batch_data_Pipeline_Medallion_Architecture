import requests
import json

WIKIMEDIA_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
NIFI_URL = "http://localhost:8081/contentListener"

headers = {
    "Accept": "text/event-stream",
    "User-Agent": "MedallionDataEngineering/1.0"
}

print("Connecting to Wikimedia EventStreams...")

with requests.get(
    WIKIMEDIA_URL,
    headers=headers,
    stream=True,
    timeout=None
) as response:

    response.raise_for_status()

    print("Connected to Wikimedia EventStreams.")
    print("Waiting for events...\n")

    data_lines = []

    for line in response.iter_lines(decode_unicode=True):

        if line is None:
            continue

        # SSE event finished
        if line == "":
            if data_lines:

                data = "\n".join(data_lines)
                data_lines = []

                try:
                    event = json.loads(data)

                    nifi_response = requests.post(
                        NIFI_URL,
                        json=event,
                        timeout=10
                    )

                    print(
                        f"NiFi: {nifi_response.status_code} | "
                        f"{event.get('wiki')} | "
                        f"{event.get('type')} | "
                        f"{event.get('title')}"
                    )

                except json.JSONDecodeError:
                    print("Could not decode Wikimedia event")

                except requests.RequestException as e:
                    print(f"NiFi error: {e}")

            continue

        # Only collect SSE data lines
        if line.startswith("data:"):
            data_lines.append(line[5:].strip())