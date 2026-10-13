
import asyncio
import json
import logging
import os
import time
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from telegram import Update
from telegram.ext import (
    Application,
    ContextTypes,
    MessageHandler,
    filters,
)
from telegram.request import HTTPXRequest
import uvicorn

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
HOST = "0.0.0.0"
PORT = int(os.getenv("PORT", "8000"))

if not BOT_TOKEN:
    raise RuntimeError(
        "BOT_TOKEN is missing. Set it in Render Environment."
    )

BASE = Path(__file__).resolve().parent
STATIC = BASE / "static"
MEDIA = BASE / "media"

STATIC.mkdir(exist_ok=True)
MEDIA.mkdir(exist_ok=True)

INDEX_FILE = STATIC / "index.html"

if not INDEX_FILE.exists():
    raise RuntimeError(
        "static/index.html not found. "
        "Check that the file exists in the static folder."
    )

app = FastAPI(title="Telegram Circle Wall")

app.mount(
    "/media",
    StaticFiles(directory=str(MEDIA)),
    name="media",
)

HTML = INDEX_FILE.read_text(encoding="utf-8")

# Events are sent to connected web screens.
subscribers = set()


@app.get("/", response_class=HTMLResponse)
async def index():
    return HTML


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/api/videos")
async def videos():
    result = []

    for p in sorted(
        MEDIA.glob("*.mp4"),
        key=lambda x: x.stat().st_mtime,
    ):
        result.append({
            "id": p.stem,
            "url": f"/media/{p.name}",
            "created": p.stat().st_mtime,
        })

    return result
@app.post("/api/videos/clear")
async def clear_videos():
    deleted = 0

    for p in MEDIA.glob("*.mp4"):
        try:
            p.unlink()
            deleted += 1
        except OSError:
            logging.exception("Could not delete video: %s", p)

    await broadcast({"type": "clear"})

    logging.info("Cleared %s video notes", deleted)
    return {"status": "ok", "deleted": deleted}

@app.get("/api/events")
async def events_stream():
    queue = asyncio.Queue()
    subscribers.add(queue)

    async def generator():
        try:
            # Tell the browser that the connection is active.
            yield "retry: 3000\n\n"

            # Send a heartbeat periodically.
            while True:
                try:
                    item = await asyncio.wait_for(
                        queue.get(),
                        timeout=20,
                    )
                    yield (
                        "data: "
                        + json.dumps(item, ensure_ascii=False)
                        + "\n\n"
                    )
                except asyncio.TimeoutError:
                    yield ": heartbeat\n\n"
        finally:
            subscribers.discard(queue)

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


async def broadcast(item):
    # Each connected screen receives its own event.
    for queue in list(subscribers):
        try:
            queue.put_nowait(item)
        except Exception:
            logging.exception("Could not send video event")


async def handle_video(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    message = update.effective_message

    if message is None or message.video_note is None:
        return

    note = message.video_note

    try:
        tg_file = await context.bot.get_file(note.file_id)

        # Unique filename to avoid overwriting videos.
        filename = f"{time.time_ns()}.mp4"
        target = MEDIA / filename

        await tg_file.download_to_drive(
            custom_path=str(target)
        )

        item = {
            "id": target.stem,
            "url": f"/media/{filename}",
            "created": time.time(),
            "sender": (
                message.from_user.first_name
                if message.from_user
                else "Guest"
            ),
        }

        await broadcast(item)

        logging.info("New video note saved: %s", filename)

        try:
            await message.reply_text(
                "✅ Кружечок додано на екран!"
            )
        except Exception:
            logging.warning(
                "Could not send confirmation to Telegram"
            )

    except Exception:
        logging.exception("Failed to process video note")

        try:
            await message.reply_text(
                "❌ Не вдалося обробити кружечок. "
                "Спробуй надіслати його ще раз."
            )
        except Exception:
            pass


async def run_bot():
    # Separate HTTP clients for normal requests and polling.
    request = HTTPXRequest(
        connection_pool_size=8,
        connect_timeout=30,
        read_timeout=30,
        write_timeout=30,
        pool_timeout=30,
    )

    updates_request = HTTPXRequest(
        connection_pool_size=2,
        connect_timeout=30,
        read_timeout=45,
        write_timeout=30,
        pool_timeout=30,
    )

    bot_app = (
        Application.builder()
        .token(BOT_TOKEN)
        .request(request)
        .get_updates_request(updates_request)
        .build()
    )

    bot_app.add_handler(
        MessageHandler(
            filters.VIDEO_NOTE,
            handle_video,
        )
    )

    try:
        await bot_app.initialize()
        await bot_app.start()

        if bot_app.updater is None:
            raise RuntimeError("Telegram updater is unavailable")

        await bot_app.updater.start_polling(
            poll_interval=1.0,
            timeout=30,
            bootstrap_retries=5,
            drop_pending_updates=False,
        )

        logging.info("Telegram bot started successfully")

        while True:
            await asyncio.sleep(3600)

    finally:
        if bot_app.updater is not None:
            if bot_app.updater.running:
                await bot_app.updater.stop()

        if bot_app.running:
            await bot_app.stop()

        await bot_app.shutdown()


async def run_web():
    config = uvicorn.Config(
        app,
        host=HOST,
        port=PORT,
        log_level="info",
        timeout_keep_alive=75,
    )

    server = uvicorn.Server(config)
    await server.serve()


async def main():
    await asyncio.gather(
        run_web(),
        run_bot(),
    )


if __name__ == "__main__":
    asyncio.run(main())
