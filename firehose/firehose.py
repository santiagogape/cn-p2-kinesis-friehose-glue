import json
import base64

def lambda_handler(event, context):
    output = []

    for record in event["records"]:
        payload = base64.b64decode(record["data"]).decode("utf-8")
        data_json = json.loads(payload)

        album_release_date = data_json.get("album_release_date", "")

        if len(album_release_date) >= 7:
            year = album_release_date[:4]
            month = album_release_date[5:7]
        else:
            year = "unknown"
            month = "unknown"

        output_record = {
            "recordId": record["recordId"],
            "result": "Ok",
            "data": base64.b64encode(
                (json.dumps(data_json) + "\n").encode("utf-8")
            ).decode("utf-8"),
            "metadata": {
                "partitionKeys": {
                    "year": year,
                    "month": month
                }
            }
        }

        output.append(output_record)

    return {"records": output}
