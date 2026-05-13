def handler(event, context):
    name = event.get("name", "world")
    return {
        "statusCode": 200,
        "message": f"Hello, {name}!",
        "requestId": context.aws_request_id,
    }
