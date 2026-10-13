import asyncio
import json
import os
import time
from pathlib import Path
from typing import Set

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from telegram import Update
from telegram.ext import Application, ContextTypes, MessageHandler, filters
import uvicorn

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8000"))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing. Put it in .env")

BASE = Path(__file__).parent
MEDIA = BASE / "media"
MEDIA.mkdir(exist_ok=True)

app = FastAPI(title="Telegram Circle Wall")
app.mount("/media", StaticFiles(directory=MEDIA), name="media")

events: asyncio.Queue = asyncio.Queue()
known: Set[str] = set()

HTML = (BASE / "static" / "index.html").read_text(encoding="utf-8")


@app.get("/", response_class=HTMLResponse)
async def index():
    return HTML


@app.get("/api/videos")
async def videos():
    result = []
    for p in sorted(MEDIA.glob("*.mp4"), key=lambda x: x.stat().st_mtime):
        result.append({
            "id": p.stem,
            "url": f"/media/{p.name}",
            "created": p.stat().st_mtime,
        })
    return result


@app.get("/api/events")
async def sse():
    from starlette.responses import StreamingResponse

    async def generator():
        while True:
            item = await events.get()
            yield f"data: {json.dumps(item, ensure_ascii=False)}\n\n"

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )


async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    if not message or not message.video_note:
        return

    note = message.video_note
    tg_file = await context.bot.get_file(note.file_id)

    stamp = int(time.time() * 1000)
    filename = f"{stamp}.mp4"
    target = MEDIA / filename
    await tg_file.download_to_drive(custom_path=str(target))

    item = {
        "id": target.stem,
        "url": f"/media/{filename}",
        "created": time.time(),
        "sender": (
            message.from_user.first_name
            if message.from_user else "Guest"
        ),
    }

    known.add(target.stem)
    await events.put(item)

    try:
        await message.reply_text("✅ Кружечок додано на екран!")
    except Exception:
        pass


async def run_bot():
    bot_app = Application.builder().token(BOT_TOKEN).build()
    bot_app.add_handler(MessageHandler(filters.VIDEO_NOTE, handle_video))
    await bot_app.initialize()
    await bot_app.start()
    await bot_app.updater.start_polling()
    print("Telegram bot started.")

    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        await bot_app.updater.stop()
        await bot_app.stop()
        await bot_app.shutdown()


async def run_web():
    config = uvicorn.Config(app, host=HOST, port=PORT, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()


async def main():
    await asyncio.gather(run_web(), run_bot())


if __name__ == "__main__":
    asyncio.run(main())
