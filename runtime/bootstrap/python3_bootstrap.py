#!/usr/bin/env python3
"""
Python bootstrap — 实现 Lambda Runtime API 轮询循环。
用户函数只需导出一个 handler(event, context) 函数。
"""
import importlib
import json
import os
import sys
import urllib.request
import urllib.error

RUNTIME_API = os.environ.get("RUNTIME_API", "http://localhost:9000")
HANDLER_ENV = os.environ.get("FUNCTION_HANDLER", "handler.handler")
FUNCTION_DIR = os.environ.get("FUNCTION_DIR", "/function")

# 把函数目录加入模块搜索路径
if FUNCTION_DIR not in sys.path:
    sys.path.insert(0, FUNCTION_DIR)


def load_handler():
    module_name, func_name = HANDLER_ENV.rsplit(".", 1)
    module = importlib.import_module(module_name)
    return getattr(module, func_name)


class Context:
    def __init__(self, request_id: str, deadline_ms: int):
        self.aws_request_id = request_id
        self.deadline_ms = deadline_ms


def post(url: str, data: bytes, content_type: str = "application/json"):
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": content_type},
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        return resp.status


def main():
    handler = load_handler()
    print(f"[bootstrap] loaded handler: {HANDLER_ENV}", flush=True)

    while True:
        # 1. 轮询下一个事件（阻塞）
        try:
            with urllib.request.urlopen(f"{RUNTIME_API}/runtime/invocation/next") as resp:
                if resp.status == 204:
                    # 无任务，继续轮询
                    continue
                request_id = resp.headers.get("Lambda-Runtime-Aws-Request-Id", "")
                deadline_ms = int(resp.headers.get("Lambda-Runtime-Deadline-Ms", "0"))
                payload = json.loads(resp.read())
        except urllib.error.URLError as e:
            print(f"[bootstrap] next error: {e}", flush=True)
            continue

        ctx = Context(request_id, deadline_ms)

        # 2. 执行用户函数
        try:
            result = handler(payload, ctx)
            if not isinstance(result, dict):
                result = {"result": result}
            body = json.dumps(result).encode()
            post(f"{RUNTIME_API}/runtime/invocation/{request_id}/response", body)
        except Exception as exc:
            print(f"[bootstrap] handler error: {exc}", flush=True)
            err = json.dumps({
                "errorType": type(exc).__name__,
                "errorMessage": str(exc),
            }).encode()
            post(f"{RUNTIME_API}/runtime/invocation/{request_id}/error", err)


if __name__ == "__main__":
    main()
