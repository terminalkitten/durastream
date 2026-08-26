import asyncio
import json
import pathlib
import tempfile
from collections.abc import AsyncIterator

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from utils import FakeOpenAI

from durastream import AsyncStore, from_token, to_token

TASKS: set = set()  # prevent background tasks from being GC
PAGE = (pathlib.Path(__file__).parent / "resume.html").read_text()
STORE = AsyncStore(tempfile.mkdtemp())

app = FastAPI()


async def generate(key: str) -> None:
    """Stream a (fake) LLM completions"""
    s = await STORE.open(key)
    stream = FakeOpenAI().chat.completions.create()
    async for chunk in stream:
        delta = chunk.choices[0].delta.content
        if delta:
            await s.append(delta.encode())
    await s.close()


async def sse_stream(key: str, offset_token: str) -> AsyncIterator[bytes]:
    """Turn a stream into SSE"""
    try:
        s = await STORE.open(key)
    except KeyError:
        # stale reconnect to a stream that has been deleted
        yield b'event: control\ndata: {"streamClosed": true}\n\n'
        return
    offset = from_token(offset_token, s.next_offset)
    while True:
        records = await s.read(offset)
        for i, rec in enumerate(records):
            yield f"id: {to_token(offset + i)}\ndata: {rec.decode()}\n\n".encode()
        offset += len(records)
        if s.closed and offset >= s.next_offset:
            yield b'event: control\ndata: {"streamClosed": true}\n\n'
            return
        ctl = json.dumps({"streamNextOffset": to_token(offset)})
        yield f"event: control\ndata: {ctl}\n\n".encode()
        await asyncio.sleep(0.05)


@app.post("/chat/{chat_id}")
async def start(chat_id: str) -> dict:
    key = f"chat.{chat_id}"
    await STORE.create(key, "text/plain")
    task = asyncio.create_task(generate(key))
    TASKS.add(task)
    task.add_done_callback(TASKS.discard)
    return {"key": key}


@app.get("/chat/{chat_id}/stream")
async def stream(chat_id: str, request: Request) -> StreamingResponse:
    offset = request.query_params.get("offset", "-1")
    events = sse_stream(f"chat.{chat_id}", offset)
    return StreamingResponse(events, media_type="text/event-stream")


@app.get("/")
async def index() -> HTMLResponse:
    return HTMLResponse(PAGE)


def serve() -> None:
    """Run the server and open the browser demo."""
    import webbrowser

    import uvicorn

    url = "http://127.0.0.1:8000"
    print(f"open {url}, resumable SSE demo (Ctrl-C to stop)")
    webbrowser.open(url)
    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="warning")


if __name__ == "__main__":
    serve()
