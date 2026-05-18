import time


def handler(event, context):
    sleep_ms = int(event.get("sleep_ms", 0) or 0)
    if sleep_ms > 0:
        time.sleep(sleep_ms / 1000)

    name = event.get("name", "world")
    return {
        "statusCode": 200,
        "message": f"Hello, {name}!",
        "requestId": context.aws_request_id,
    }
