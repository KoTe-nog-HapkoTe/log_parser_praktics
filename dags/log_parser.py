import json
import logging
from datetime import datetime
import pandas as pd

def parse_logs(log_file: str = "/opt/airflow/dags/viqube_api.txt", output_csv: str = "parsed_logs.csv"):
    logging.basicConfig(filename='log_parser_errors.log', level=logging.ERROR)

    requests = {}
    responses = {}

    with open(log_file, 'r', encoding='utf-8') as f:
        for lineno, line in enumerate(f, 1):
            try:
                entry = json.loads(line.strip())
                uuid = entry["message"].get("uuid")
                if not uuid:
                    continue

                if entry["function"] == "HttpRouter::service":
                    requests[uuid] = entry
                elif entry["function"].endswith("setResponse"):
                    code = entry["message"].get("result", {}).get("code")
                    if code is not None:
                        responses[uuid] = code
            except Exception as e:
                logging.error(f"Line {lineno}: {e}")

    rows = []
    for uuid, req in requests.items():
        try:
            dt = datetime.strptime(req["datetime"], "%Y-%m-%d %H:%M:%S.%f")
            user = req["message"].get("user", {}).get("name", "unknown")
            info = req["message"].get("info", [])
            ip = info[0] if len(info) > 0 else ""
            method = info[1] if len(info) > 1 else ""
            url = info[2] if len(info) > 2 else ""
            user_agent = info[4] if len(info) > 4 else ""
            code = responses.get(uuid)

            rows.append({
                "datetime": dt,
                "date": dt.date(),
                "hour": dt.hour,
                "user_name": user,
                "ip": ip,
                "method": method,
                "url": url,
                "user_agent": user_agent,
                "code": code
            })

        except Exception as e:
            logging.error(f"UUID {uuid} parsing error: {e}")

    df = pd.DataFrame(rows)
    df.to_csv(output_csv, index=False, encoding="utf-8")
    print(f"[parser] Parsed {len(df)} rows.")
