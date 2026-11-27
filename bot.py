"""Listing Radar: Telegram-бот, который ловит листинги на топ-биржах
и ищет соответствующие токены на Solana (DEX Screener). Всё бесплатно."""
import asyncio
import html
import json
import logging
import os
import re
import sys
import time

import aiohttp
import aiosqlite
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject
from aiogram.types import (BotCommand, CallbackQuery, InlineKeyboardButton,
                           InlineKeyboardMarkup, Message)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("radar")

TOKEN = os.getenv("BOT_TOKEN", "")
ALLOWED = {int(x) for x in (os.getenv("ALLOWED_IDS") or os.getenv("CHAT_ID", "")).replace(" ", "").split(",") if x}
DB_PATH = os.getenv("DB_PATH", "bot.db")
SOL_RPC = os.getenv("SOLANA_RPC", "https://api.mainnet-beta.solana.com")
AUTO_UPDATE = os.getenv("AUTO_UPDATE", "1") == "1"
PAIRS_EVERY = int(os.getenv("PAIRS_EVERY", "60"))
ANN_EVERY = int(os.getenv("ANN_EVERY", "45"))
POS_EVERY = int(os.getenv("POS_EVERY", "45"))

EXCHANGES = ["binance", "coinbase", "upbit", "bithumb", "bybit", "okx", "kucoin"]
NAMES = {"binance": "Binance", "coinbase": "Coinbase", "upbit": "Upbit", "bithumb": "Bithumb",
         "bybit": "Bybit", "okx": "OKX", "kucoin": "KuCoin"}
LIST_RE = re.compile(r"list|상장|추가|add|launch|new|support", re.I)

session: aiohttp.ClientSession
db: aiosqlite.Connection
router = Router()
if ALLOWED:
    router.message.filter(F.from_user.id.in_(ALLOWED))
    router.callback_query.filter(F.from_user.id.in_(ALLOWED))


# ───────────────────────── сеть ─────────────────────────
async def jget(url, params=None):
    try:
        async with session.get(url, params=params) as r:
            if r.status != 200:
                log.warning("%s -> HTTP %s", url, r.status)
                return None
            return await r.json(content_type=None)
    except Exception as e:
        log.warning("GET %s: %s", url, e)
        return None


async def rpc(method, params):
    try:
        async with session.post(SOL_RPC, json={"jsonrpc": "2.0", "id": 1, "method": method,
                                               "params": params}) as r:
            return (await r.json(content_type=None)).get("result")
    except Exception as e:
        log.warning("RPC %s: %s", method, e)
        return None


# ───────────────────────── база ─────────────────────────
async def init_db():
    await db.executescript("""
    CREATE TABLE IF NOT EXISTS kv(k TEXT PRIMARY KEY, v TEXT);
    CREATE TABLE IF NOT EXISTS users(
        chat_id INTEGER PRIMARY KEY, min_liq REAL DEFAULT 20000, min_mcap REAL DEFAULT 1000000,
        max_mcap REAL DEFAULT 500000000, size_usd REAL DEFAULT 50, target_pct REAL DEFAULT 30,
        stop_pct REAL DEFAULT 15, exchanges TEXT, alerts INTEGER DEFAULT 1);
    CREATE TABLE IF NOT EXISTS positions(
        id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER, mint TEXT, symbol TEXT,
        entry REAL, usd REAL, ts INTEGER, open INTEGER DEFAULT 1, notified INTEGER DEFAULT 0,
        exit_price REAL);
    CREATE TABLE IF NOT EXISTS signals(
        id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER, exchange TEXT, kind TEXT,
        symbol TEXT, mint TEXT, title TEXT);
    """)
    await db.commit()


async def kv_get(k):
    async with db.execute("SELECT v FROM kv WHERE k=?", (k,)) as c:
        row = await c.fetchone()
    return row[0] if row else None


async def kv_set(k, v):
    await db.execute("INSERT INTO kv(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, v))
    await db.commit()


async def get_user(chat_id):
    await db.execute("INSERT OR IGNORE INTO users(chat_id, exchanges) VALUES(?,?)",
                     (chat_id, ",".join(EXCHANGES)))
    await db.commit()
    db.row_factory = aiosqlite.Row
    async with db.execute("SELECT * FROM users WHERE chat_id=?", (chat_id,)) as c:
        row = await c.fetchone()
    db.row_factory = None
    return dict(row)


async def all_users():
    async with db.execute("SELECT chat_id FROM users") as c:
        ids = [r[0] for r in await c.fetchall()]
    return [await get_user(i) for i in ids]


async def set_user(chat_id, **kw):
    await get_user(chat_id)
    for k, v in kw.items():
        await db.execute(f"UPDATE users SET {k}=? WHERE chat_id=?", (v, chat_id))
    await db.commit()


# ───────────────────────── форматирование ─────────────────────────
def usd(n):
    n = float(n or 0)
    for lim, s in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if abs(n) >= lim:
            return f"${n / lim:.2f}{s}"
    return f"${n:.2f}"


def price_fmt(p):
    p = float(p or 0)
    return f"${p:.8f}".rstrip("0").rstrip(".") if p < 1 else f"${p:.4f}"


def pct(n):
    n = float(n or 0)
    return f"{'🟢' if n >= 0 else '🔴'} {n:+.1f}%"


# ───────────────────────── DEX Screener / Solana ─────────────────────────
def tok_from_pair(p, liq):
    return {"mint": p["baseToken"]["address"], "symbol": p["baseToken"]["symbol"],
            "name": p["baseToken"].get("name", ""), "price": float(p.get("priceUsd") or 0),
            "liq": liq, "mcap": p.get("marketCap") or p.get("fdv") or 0,
            "vol24": (p.get("volume") or {}).get("h24", 0),
            "ch1h": (p.get("priceChange") or {}).get("h1", 0),
            "ch24": (p.get("priceChange") or {}).get("h24", 0), "url": p.get("url", ""),
            "created": p.get("pairCreatedAt"), "dupes": 0}


def best_tokens(pairs):
    best = {}
    for p in pairs:
        liq = (p.get("liquidity") or {}).get("usd", 0) or 0
        m = p["baseToken"]["address"]
        if m not in best or liq > best[m]["liq"]:
            best[m] = tok_from_pair(p, liq)
    return sorted(best.values(), key=lambda t: -t["liq"])


async def find_solana_token(sym):
    j = await jget("https://api.dexscreener.com/latest/dex/search", {"q": sym})
    pairs = [p for p in (j or {}).get("pairs") or []
             if p.get("chainId") == "solana" and p["baseToken"]["symbol"].upper() == sym.upper()]
    toks = best_tokens(pairs)
    if not toks:
        return None
    toks[0]["dupes"] = sum(1 for t in toks[1:] if t["liq"] > 10000)
    return toks[0]


async def token_by_mint(mint):
    j = await jget(f"https://api.dexscreener.com/latest/dex/tokens/{mint}")
    pairs = [p for p in (j or {}).get("pairs") or []
             if p.get("chainId") == "solana" and p["baseToken"]["address"] == mint]
    toks = best_tokens(pairs)
    return toks[0] if toks else None


async def safety(mint):
    flags = []
    try:
        info = await rpc("getAccountInfo", [mint, {"encoding": "jsonParsed"}])
        d = info["value"]["data"]["parsed"]["info"]
        if d.get("mintAuthority"):
            flags.append("🔴 mint authority не отозван (могут выпустить ещё токенов)")
        if d.get("freezeAuthority"):
            flags.append("🟠 есть freeze authority (кошельки можно заморозить)")
        supply = int(d["supply"]) / 10 ** d["decimals"]
        la = await rpc("getTokenLargestAccounts", [mint])
        vals = [float(x.get("uiAmount") or 0) for x in la["value"]][:10]
        top10 = sum(vals) / supply * 100 if supply else 0
        if top10 > 60:
            flags.append(f"🟠 топ-10 держателей: {top10:.0f}% (включая пул)")
        else:
            flags.append(f"🟢 топ-10 держателей: {top10:.0f}% (включая пул)")
    except Exception:
        flags.append("⚪ не удалось проверить контракт напрямую")
    rc = await jget(f"https://api.rugcheck.xyz/v1/tokens/{mint}/report/summary")
    if rc:
        risks = [r.get("name") for r in rc.get("risks") or [] if r.get("level") in ("danger", "warn")][:4]
        if risks:
            flags.append("🟠 RugCheck: " + ", ".join(map(str, risks)))
    if not any(f.startswith(("🔴", "🟠")) for f in flags):
        flags.append("🟢 явных красных флагов нет (это не гарантия)")
    return flags


# ───────────────────────── источники сигналов ─────────────────────────
async def p_binance():
    j = await jget("https://data-api.binance.vision/api/v3/exchangeInfo")
    return {s["baseAsset"] for s in j["symbols"] if s.get("status") == "TRADING"} if j else None


async def p_coinbase():
    j = await jget("https://api.exchange.coinbase.com/products")
    return {s["base_currency"] for s in j if s.get("status") == "online"} if j else None


async def p_bybit():
    j = await jget("https://api.bybit.com/v5/market/instruments-info", {"category": "spot"})
    return {s["baseCoin"] for s in j["result"]["list"]} if j else None


async def p_okx():
    j = await jget("https://www.okx.com/api/v5/public/instruments", {"instType": "SPOT"})
    return {s["instId"].split("-")[0] for s in j["data"]} if j else None


async def p_kucoin():
    j = await jget("https://api.kucoin.com/api/v2/symbols")
    return {s["baseCurrency"] for s in j["data"] if s.get("enableTrading")} if j else None


async def p_upbit():
    j = await jget("https://api.upbit.com/v1/market/all")
    return {s["market"].split("-")[1] for s in j} if j else None


async def p_bithumb():
    j = await jget("https://api.bithumb.com/public/ticker/ALL_KRW")
    return {k for k in j["data"] if k != "date"} if j else None


PAIR_FETCHERS = {"binance": p_binance, "coinbase": p_coinbase, "bybit": p_bybit, "okx": p_okx,
                 "kucoin": p_kucoin, "upbit": p_upbit, "bithumb": p_bithumb}


async def a_binance():
    j = await jget("https://www.binance.com/bapi/composite/v1/public/cms/article/list/query",
                   {"type": 1, "catalogId": 48, "pageNo": 1, "pageSize": 10})
    arts = j["data"]["catalogs"][0]["articles"]
    return [(str(a["id"]), a["title"], f"https://www.binance.com/en/support/announcement/{a['code']}")
            for a in arts]


async def a_upbit():
    j = await jget("https://api-manager.upbit.com/api/v1/announcements",
                   {"os": "web", "page": 1, "per_page": 10, "category": "trade"})
    return [(str(n["id"]), n["title"], f"https://upbit.com/service_center/notice?id={n['id']}")
            for n in j["data"]["notices"]]


async def a_bybit():
    j = await jget("https://api.bybit.com/v5/announcements/index",
                   {"locale": "en-US", "type": "new_crypto", "limit": 10})
    return [(x["url"], x["title"], x["url"]) for x in j["result"]["list"]]


async def a_okx():
    j = await jget("https://www.okx.com/api/v5/support/announcements",
                   {"annType": "announcements-new-listings"})
    return [(x["url"], x["title"], x["url"]) for x in j["data"][0]["details"]]


async def a_kucoin():
    j = await jget("https://api.kucoin.com/api/v3/announcements",
                   {"currentPage": 1, "pageSize": 10, "annType": "new-listings", "lang": "en_US"})
    return [(str(x["annId"]), x["annTitle"], x["annUrl"]) for x in j["data"]["items"]]


ANN_FETCHERS = {"binance": a_binance, "upbit": a_upbit, "bybit": a_bybit, "okx": a_okx,
                "kucoin": a_kucoin}


async def check_pairs(bot):
    for ex, fn in PAIR_FETCHERS.items():
        try:
            cur = await fn()
        except Exception as e:
            log.warning("pairs %s: %s", ex, e)
            continue
        if not cur:
            continue
        old = await kv_get(f"pairs:{ex}")
        await kv_set(f"pairs:{ex}", json.dumps(sorted(cur)))
        if old is None:
            continue
        new = cur - set(json.loads(old))
        if new and len(new) < 15:
            await handle_signal(bot, ex, "pair", sorted(new), "", "")


async def check_announcements(bot):
    for ex, fn in ANN_FETCHERS.items():
        try:
            items = await fn()
        except Exception as e:
            log.warning("ann %s: %s", ex, e)
            continue
        if not items:
            continue
        old = await kv_get(f"ann:{ex}")
        ids = [i[0] for i in items]
        await kv_set(f"ann:{ex}", json.dumps((ids + (json.loads(old) if old else []))[:60]))
        if old is None:
            continue
        seen = set(json.loads(old))
        for aid, title, url in items:
            if aid in seen or not LIST_RE.search(title):
                continue
            syms = list(dict.fromkeys(re.findall(r"\(([A-Z0-9]{2,12})\)", title)))
            if syms:
                await handle_signal(bot, ex, "ann", syms, title, url)


# ───────────────────────── обработка сигнала ─────────────────────────
def alert_kb(tok, url):
    rows = [[InlineKeyboardButton(text="📊 DEX Screener", url=tok["url"] or "https://dexscreener.com"),
             InlineKeyboardButton(text="🦅 Birdeye", url=f"https://birdeye.so/token/{tok['mint']}?chain=solana")],
            [InlineKeyboardButton(text="🔍 Проверка", callback_data=f"chk:{tok['mint']}"),
             InlineKeyboardButton(text="💰 Купил", callback_data=f"buy:{tok['mint']}")]]
    if url:
        rows.append([InlineKeyboardButton(text="📰 Анонс", url=url)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def token_card(tok, flags=None):
    t = (f"<b>{html.escape(tok['symbol'])}</b> · {html.escape(tok['name'])}\n"
         f"Цена: {price_fmt(tok['price'])}\n"
         f"Кап.: {usd(tok['mcap'])} · Ликвидность: {usd(tok['liq'])}\n"
         f"Объём 24ч: {usd(tok['vol24'])}\n"
         f"1ч: {pct(tok['ch1h'])} · 24ч: {pct(tok['ch24'])}\n"
         f"<code>{tok['mint']}</code>")
    if tok.get("dupes"):
        t += "\n⚠️ Есть другие токены с таким же тикером. Сверьте адрес!"
    if flags:
        t += "\n\n<b>Проверка:</b>\n" + "\n".join(flags)
    return t


async def handle_signal(bot, ex, kind, symbols, title, url):
    users = await all_users()
    for sym in symbols:
        key = f"sig:{ex}:{kind}:{sym}"
        if await kv_get(key):
            continue
        await kv_set(key, str(int(time.time())))
        log.info("SIGNAL %s %s %s", ex, kind, sym)
        tok = await find_solana_token(sym)
        await db.execute("INSERT INTO signals(ts,exchange,kind,symbol,mint,title) VALUES(?,?,?,?,?,?)",
                         (int(time.time()), ex, kind, sym, tok["mint"] if tok else None, title))
        await db.commit()
        async with db.execute("SELECT chat_id FROM positions WHERE open=1 AND UPPER(symbol)=?",
                              (sym.upper(),)) as c:
            holders = [r[0] for r in await c.fetchall()]
        for cid in set(holders):
            await bot.send_message(cid, f"🚨 <b>{sym}</b>: листинг на {NAMES[ex]} "
                                        f"({'анонс' if kind == 'ann' else 'пара уже торгуется'}). "
                                        f"У вас открыта позиция, подумайте о фиксации.")
        if not tok:
            continue
        flags = await safety(tok["mint"])
        label = "📢 Анонс листинга" if kind == "ann" else "⚡ Новая пара на бирже"
        for u in users:
            if not u["alerts"] or ex not in (u["exchanges"] or "").split(","):
                continue
            if tok["liq"] < u["min_liq"] or not (u["min_mcap"] <= (tok["mcap"] or 0) <= u["max_mcap"]):
                continue
            head = f"{label}: <b>{NAMES[ex]}</b>\n"
            if title:
                head += f"<i>{html.escape(title)}</i>\n"
            try:
                await bot.send_message(u["chat_id"], head + "\n" + token_card(tok, flags),
                                       reply_markup=alert_kb(tok, url), disable_web_page_preview=True)
            except Exception as e:
                log.warning("send: %s", e)


# ───────────────────────── позиции ─────────────────────────
async def positions_loop(bot):
    while True:
        await asyncio.sleep(POS_EVERY)
        try:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM positions WHERE open=1") as c:
                rows = [dict(r) for r in await c.fetchall()]
            db.row_factory = None
            prices = {}
            for r in rows:
                if r["mint"] not in prices:
                    tok = await token_by_mint(r["mint"])
                    prices[r["mint"]] = tok["price"] if tok else 0
                    await asyncio.sleep(0.5)
                now = prices[r["mint"]]
                if not now or not r["entry"]:
                    continue
                u = await get_user(r["chat_id"])
                ch = (now / r["entry"] - 1) * 100
                if ch >= u["target_pct"] and not r["notified"] & 1:
                    await bot.send_message(r["chat_id"], f"🎯 <b>{r['symbol']}</b> достиг цели: {ch:+.1f}%. "
                                                          f"Позиция #{r['id']}. Закрыть: /sell {r['id']}")
                    await db.execute("UPDATE positions SET notified=notified|1 WHERE id=?", (r["id"],))
                if ch <= -u["stop_pct"] and not r["notified"] & 2:
                    await bot.send_message(r["chat_id"], f"🛑 <b>{r['symbol']}</b> просел до {ch:+.1f}%. "
                                                          f"Позиция #{r['id']}. Закрыть: /sell {r['id']}")
                    await db.execute("UPDATE positions SET notified=notified|2 WHERE id=?", (r["id"],))
            await db.commit()
        except Exception as e:
            db.row_factory = None
            log.warning("positions loop: %s", e)


async def open_position(chat_id, tok, price, usd_amount):
    await db.execute("INSERT INTO positions(chat_id,mint,symbol,entry,usd,ts) VALUES(?,?,?,?,?,?)",
                     (chat_id, tok["mint"], tok["symbol"], price, usd_amount, int(time.time())))
    await db.commit()


async def send_positions(bot, chat_id):
    async with db.execute("SELECT id,mint,symbol,entry,usd FROM positions WHERE chat_id=? AND open=1",
                          (chat_id,)) as c:
        rows = await c.fetchall()
    if not rows:
        return await bot.send_message(chat_id, "Открытых позиций нет.")
    lines = ["<b>💼 Открытые позиции</b>"]
    for pid, mint, sym, entry, amt in rows:
        tok = await token_by_mint(mint)
        now = tok["price"] if tok else 0
        ch = (now / entry - 1) * 100 if entry and now else 0
        lines.append(f"#{pid} <b>{html.escape(sym)}</b> · вход {price_fmt(entry)} → {price_fmt(now)} "
                     f"{pct(ch)} · ${amt:.0f} ({'+' if ch >= 0 else '-'}${abs(amt * ch / 100):.2f})")
    lines.append("\nЗакрыть: /sell номер")
    await bot.send_message(chat_id, "\n".join(lines))


# ───────────────────────── интерфейс ─────────────────────────
def main_menu():
    b = InlineKeyboardButton
    return InlineKeyboardMarkup(inline_keyboard=[
        [b(text="📡 Сигналы", callback_data="m:signals"), b(text="💼 Позиции", callback_data="m:positions")],
        [b(text="🏦 Биржи", callback_data="m:exchanges"), b(text="⚙️ Настройки", callback_data="m:settings")],
        [b(text="❓ Помощь", callback_data="m:help"), b(text="📈 Статус", callback_data="m:status")]])


HELP = ("<b>Listing Radar</b>: ловит листинги на топ-биржах и находит токен на Solana.\n\n"
        "<b>Команды</b>\n"
        "/menu · главное меню\n/signals · последние сигналы\n/check &lt;тикер или mint&gt; · проверить токен\n"
        "/positions · мои позиции\n/buy &lt;mint&gt; &lt;цена&gt; &lt;$&gt; · записать покупку\n"
        "/sell &lt;№&gt; · закрыть позицию\n/exchanges · выбрать биржи\n/settings · настройки\n"
        "/setliq &lt;$&gt; · мин. ликвидность\n/setmcap &lt;min&gt; &lt;max&gt; · диапазон капитализации\n"
        "/setsize &lt;$&gt; · размер сделки\n/settarget &lt;%&gt; · цель\n/setstop &lt;%&gt; · стоп\n"
        "/alerts on|off · уведомления\n/update · обновить бота (админ)\n\n"
        "Бот только сообщает. Покупаете и продаёте вы сами. Это не финансовая рекомендация.")


async def send_settings(bot, chat_id):
    u = await get_user(chat_id)
    await bot.send_message(chat_id, (
        "<b>⚙️ Настройки</b>\n"
        f"Уведомления: {'вкл ✅' if u['alerts'] else 'выкл ⛔'}\n"
        f"Мин. ликвидность: {usd(u['min_liq'])}\n"
        f"Капитализация: {usd(u['min_mcap'])} – {usd(u['max_mcap'])}\n"
        f"Размер сделки: ${u['size_usd']:.0f}\nЦель: +{u['target_pct']:.0f}% · Стоп: -{u['stop_pct']:.0f}%\n"
        f"Биржи: {', '.join(NAMES[e] for e in (u['exchanges'] or '').split(',') if e) or 'нет'}"))


async def send_exchanges(bot, chat_id, edit=None):
    u = await get_user(chat_id)
    on = set((u["exchanges"] or "").split(","))
    rows = [[InlineKeyboardButton(text=f"{'✅' if e in on else '⬜'} {NAMES[e]}", callback_data=f"ex:{e}")]
            for e in EXCHANGES]
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    if edit:
        await edit.edit_reply_markup(reply_markup=kb)
    else:
        await bot.send_message(chat_id, "<b>🏦 Какие биржи отслеживать</b>\nНажмите, чтобы включить или выключить.",
                                   reply_markup=kb)


async def send_signals(bot, chat_id):
    async with db.execute("SELECT ts,exchange,kind,symbol,mint FROM signals ORDER BY id DESC LIMIT 10") as c:
        rows = await c.fetchall()
    if not rows:
        return await bot.send_message(chat_id, "Сигналов пока нет. Бот сначала запоминает текущее состояние бирж, "
                                               "а уведомляет только о новом.")
    lines = ["<b>📡 Последние сигналы</b>"]
    for ts, ex, kind, sym, mint in rows:
        when = time.strftime("%d.%m %H:%M", time.gmtime(ts))
        lines.append(f"{when} UTC · {NAMES[ex]} · {'анонс' if kind == 'ann' else 'пара'} · "
                     f"<b>{html.escape(sym)}</b>{' · Solana ✅' if mint else ''}")
    await bot.send_message(chat_id, "\n".join(lines))


async def send_status(bot, chat_id):
    async with db.execute("SELECT COUNT(*) FROM signals") as c:
        n = (await c.fetchone())[0]
    await bot.send_message(chat_id, f"📈 <b>Статус</b>\nБот работает ✅\nБирж: {len(EXCHANGES)}\n"
                                    f"Сигналов за всё время: {n}\n"
                                    f"Опрос пар: каждые {PAIRS_EVERY}с · анонсов: каждые {ANN_EVERY}с")


bot_ref: Bot


@router.message(Command("start", "menu"))
async def cmd_start(m: Message):
    await get_user(m.chat.id)
    await m.answer("👋 <b>Listing Radar</b>\nСлежу за листингами на 7 топ-биржах и ищу токены на Solana. "
                   "Выберите раздел:", reply_markup=main_menu())


@router.message(Command("help"))
async def cmd_help(m: Message):
    await m.answer(HELP)


@router.message(Command("status"))
async def cmd_status(m: Message):
    await send_status(m.bot, m.chat.id)


@router.message(Command("signals"))
async def cmd_signals(m: Message):
    await send_signals(m.bot, m.chat.id)


@router.message(Command("positions"))
async def cmd_positions(m: Message):
    await send_positions(m.bot, m.chat.id)


@router.message(Command("settings"))
async def cmd_settings(m: Message):
    await send_settings(m.bot, m.chat.id)


@router.message(Command("exchanges"))
async def cmd_exchanges(m: Message):
    await send_exchanges(m.bot, m.chat.id)


@router.message(Command("check"))
async def cmd_check(m: Message, command: CommandObject):
    arg = (command.args or "").strip()
    if not arg:
        return await m.answer("Использование: /check тикер или mint-адрес")
    tok = await (token_by_mint(arg) if len(arg) > 30 else find_solana_token(arg))
    if not tok:
        return await m.answer("Токен на Solana не найден.")
    flags = await safety(tok["mint"])
    await m.answer(token_card(tok, flags), reply_markup=alert_kb(tok, ""), disable_web_page_preview=True)


@router.message(Command("buy"))
async def cmd_buy(m: Message, command: CommandObject):
    try:
        mint, price, amt = (command.args or "").split()
        tok = await token_by_mint(mint)
        if not tok:
            return await m.answer("Токен не найден по этому mint.")
        await open_position(m.chat.id, tok, float(price), float(amt))
        await m.answer(f"✅ Записано: {tok['symbol']} · вход {price_fmt(float(price))} · ${float(amt):.0f}")
    except ValueError:
        await m.answer("Использование: /buy mint цена сумма_в_$")


@router.message(Command("sell"))
async def cmd_sell(m: Message, command: CommandObject):
    try:
        pid = int((command.args or "").split()[0])
    except (ValueError, IndexError):
        return await m.answer("Использование: /sell номер_позиции")
    async with db.execute("SELECT mint,symbol,entry,usd FROM positions WHERE id=? AND chat_id=? AND open=1",
                          (pid, m.chat.id)) as c:
        row = await c.fetchone()
    if not row:
        return await m.answer("Позиция не найдена.")
    tok = await token_by_mint(row[0])
    now = tok["price"] if tok else 0
    await db.execute("UPDATE positions SET open=0, exit_price=? WHERE id=?", (now, pid))
    await db.commit()
    ch = (now / row[2] - 1) * 100 if row[2] and now else 0
    await m.answer(f"🔒 Закрыто #{pid} {row[1]}: {pct(ch)} (≈ ${row[3] * ch / 100:+.2f})")


async def _num(m, command, field, label, mult=1):
    try:
        v = float((command.args or "").replace(",", ".").replace("$", "").replace("%", ""))
        await set_user(m.chat.id, **{field: v * mult})
        await m.answer(f"✅ {label}: {v:g}")
    except ValueError:
        await m.answer(f"Укажите число, например: /{command.command} 50")


@router.message(Command("setliq"))
async def c1(m: Message, command: CommandObject):
    await _num(m, command, "min_liq", "Мин. ликвидность, $")


@router.message(Command("setsize"))
async def c2(m: Message, command: CommandObject):
    await _num(m, command, "size_usd", "Размер сделки, $")


@router.message(Command("settarget"))
async def c3(m: Message, command: CommandObject):
    await _num(m, command, "target_pct", "Цель, %")


@router.message(Command("setstop"))
async def c4(m: Message, command: CommandObject):
    await _num(m, command, "stop_pct", "Стоп, %")


@router.message(Command("setmcap"))
async def c5(m: Message, command: CommandObject):
    try:
        lo, hi = [float(x) for x in (command.args or "").split()]
        await set_user(m.chat.id, min_mcap=lo, max_mcap=hi)
        await m.answer(f"✅ Капитализация: {usd(lo)} – {usd(hi)}")
    except ValueError:
        await m.answer("Использование: /setmcap мин макс (например /setmcap 5000000 300000000)")


@router.message(Command("alerts"))
async def c6(m: Message, command: CommandObject):
    on = (command.args or "").strip().lower() != "off"
    await set_user(m.chat.id, alerts=int(on))
    await m.answer("Уведомления " + ("включены ✅" if on else "выключены ⛔"))


@router.message(Command("update"))
async def c7(m: Message):
    if not ALLOWED or m.from_user.id != min(ALLOWED):
        return await m.answer("Только для администратора.")
    if os.getenv("GITHUB_ACTIONS"):
        return await m.answer("Бот работает на GitHub Actions и обновляется сам при каждом перезапуске (раз в 6 часов). "
                              "Чтобы обновить сразу: Actions → Listing Radar → Run workflow.")
    await m.answer("⏳ Обновляю…")
    out = await git_pull()
    await m.answer(f"<pre>{html.escape(out[:1500])}</pre>\nПерезапускаюсь…")
    await asyncio.sleep(1)
    os._exit(42)


@router.callback_query(F.data.startswith("m:"))
async def cb_menu(cb: CallbackQuery):
    await cb.answer()
    act, bot, cid = cb.data[2:], cb.bot, cb.message.chat.id
    fn = {"signals": send_signals, "positions": send_positions, "settings": send_settings,
          "exchanges": send_exchanges, "status": send_status}.get(act)
    if fn:
        await fn(bot, cid)
    elif act == "help":
        await bot.send_message(cid, HELP)


@router.callback_query(F.data.startswith("ex:"))
async def cb_ex(cb: CallbackQuery):
    ex = cb.data[3:]
    u = await get_user(cb.message.chat.id)
    on = set(filter(None, (u["exchanges"] or "").split(",")))
    on.symmetric_difference_update({ex})
    await set_user(cb.message.chat.id, exchanges=",".join(e for e in EXCHANGES if e in on))
    await cb.answer("Сохранено")
    await send_exchanges(cb.bot, cb.message.chat.id, edit=cb.message)


@router.callback_query(F.data.startswith("chk:"))
async def cb_chk(cb: CallbackQuery):
    await cb.answer("Проверяю…")
    mint = cb.data[4:]
    tok = await token_by_mint(mint)
    if tok:
        await cb.message.answer(token_card(tok, await safety(mint)), disable_web_page_preview=True)


@router.callback_query(F.data.startswith("buy:"))
async def cb_buy(cb: CallbackQuery):
    mint = cb.data[4:]
    tok = await token_by_mint(mint)
    if not tok or not tok["price"]:
        return await cb.answer("Не удалось получить цену", show_alert=True)
    u = await get_user(cb.message.chat.id)
    await open_position(cb.message.chat.id, tok, tok["price"], u["size_usd"])
    await cb.answer("Позиция записана")
    await cb.message.answer(f"✅ {tok['symbol']}: вход {price_fmt(tok['price'])}, ${u['size_usd']:.0f}. "
                            f"Цель +{u['target_pct']:.0f}%, стоп -{u['stop_pct']:.0f}%. /positions")


# ───────────────────────── автообновление и запуск ─────────────────────────
async def git_pull():
    p = await asyncio.create_subprocess_shell("git pull --ff-only", stdout=asyncio.subprocess.PIPE,
                                              stderr=asyncio.subprocess.STDOUT)
    out, _ = await p.communicate()
    return out.decode(errors="ignore") or "(пусто)"


async def autoupdate_loop():
    if not (AUTO_UPDATE and os.path.isdir(".git")):
        return
    while True:
        await asyncio.sleep(1800)
        try:
            async def sh(c):
                p = await asyncio.create_subprocess_shell(c, stdout=asyncio.subprocess.PIPE,
                                                          stderr=asyncio.subprocess.DEVNULL)
                return (await p.communicate())[0].decode().strip()
            await sh("git fetch")
            if await sh("git rev-parse HEAD") != await sh("git rev-parse @{u}"):
                log.info("Найдено обновление, перезапуск")
                await git_pull()
                os._exit(42)
        except Exception as e:
            log.warning("autoupdate: %s", e)


async def loop_runner(name, every, coro_fn, bot):
    await asyncio.sleep(5)
    while True:
        try:
            await coro_fn(bot)
        except Exception as e:
            log.exception("%s: %s", name, e)
        await asyncio.sleep(every)


async def main():
    global session, db, bot_ref
    if not TOKEN or not ALLOWED:
        sys.exit("Заполните BOT_TOKEN и ALLOWED_IDS в файле .env")
    session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20),
                                    headers={"User-Agent": "Mozilla/5.0 listing-radar"})
    db = await aiosqlite.connect(DB_PATH)
    await init_db()
    bot_ref = Bot(TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp.include_router(router)
    await bot_ref.set_my_commands([
        BotCommand(command="menu", description="Главное меню"),
        BotCommand(command="signals", description="Последние сигналы"),
        BotCommand(command="check", description="Проверить токен"),
        BotCommand(command="positions", description="Мои позиции"),
        BotCommand(command="buy", description="Записать покупку"),
        BotCommand(command="sell", description="Закрыть позицию"),
        BotCommand(command="exchanges", description="Выбор бирж"),
        BotCommand(command="settings", description="Настройки"),
        BotCommand(command="status", description="Статус бота"),
        BotCommand(command="help", description="Помощь"),
        BotCommand(command="update", description="Обновить бота (админ)")])
    tasks = [asyncio.create_task(loop_runner("pairs", PAIRS_EVERY, check_pairs, bot_ref)),
             asyncio.create_task(loop_runner("ann", ANN_EVERY, check_announcements, bot_ref)),
             asyncio.create_task(positions_loop(bot_ref)),
             asyncio.create_task(autoupdate_loop())]
    log.info("Бот запущен")
    try:
        await dp.start_polling(bot_ref)
    finally:
        for t in tasks:
            t.cancel()
        await session.close()
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
