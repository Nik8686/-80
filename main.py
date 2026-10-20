# ============================================================
# SOLANA EARLY SIGNAL + AUTO BUY/SELL (Hybrid RPC)
# Public للاستماع | Helius فقط للشراء والبيع
# قابل للضبط: قيمة الصفقة + سليباج + TP + SL
# ============================================================

import asyncio
import json
import os
import base64
import hashlib
import struct
import time
from collections import deque
from dotenv import load_dotenv

import aiohttp
import websockets
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction
from solana.rpc.async_api import AsyncClient
from solana.rpc.commitment import Confirmed

from pumpfun import (
    build_buy,
    build_sell,
    build_message,
    fetch_latest_blockhash,
    fetch_bonding_curve_state,
)

load_dotenv()

# ====================== CONFIGURATION (عدل هنا بسهولة) ======================
BUY_AMOUNT_SOL = float(os.getenv("BUY_AMOUNT_SOL", "0.05"))      # قيمة الصفقة
SLIPPAGE_BPS = int(os.getenv("SLIPPAGE_BPS", "800"))             # 800 = 8%
TAKE_PROFIT_PCT = float(os.getenv("TAKE_PROFIT_PCT", "120.0"))   # +120%
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "50.0"))        # -50%

# RPC
HELIUS_RPC = os.getenv("RPC_URL", "https://mainnet.helius-rpc.com/?api-key=XXXX")
PUBLIC_WSS_NODES = [
    "wss://solana-rpc.publicnode.com",
    "wss://rpc.ankr.com/solana/ws",
    "wss://api.mainnet-beta.solana.com",
]

PRIVATE_KEY = os.getenv("PRIVATE_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Filters الأصلية
MIN_TRANSACTIONS = 80
MIN_UNIQUE_WALLETS = 77
MIN_TRANSACTION_SOL = 0.008
MAX_WALLET_TRANSACTIONS = 3
MIN_MCAP_SOL = 60.0
MAX_LIQUIDITY_MCAP_PCT = 40.0
MAX_VOLUME_LIQUIDITY_PCT = 400.0
MIN_BUY_PRESSURE_PCT = 59.0
MIN_LIQUIDITY_SOL = 50.0
CLUSTER_WINDOW_SECONDS = 30
TOTAL_SUPPLY = 1_000_000_000_000_000
LAMPORTS_PER_SOL = 1_000_000_000
TOKEN_CLEANUP_SECONDS = 7200
MAX_PROCESSED_SIGNATURES = 100_000
BUNDLER_MAX_SECONDS = 5.0
MAX_TOP_30_BUY_PCT = 16.0

PUMP_FUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
TRADE_EVENT_DISCRIMINATOR = hashlib.sha256(b"event:TradeEvent").digest()[:8]
ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

# ====================== GLOBALS ======================
wallet = Keypair.from_base58_string(PRIVATE_KEY) if PRIVATE_KEY else None
tokens = {}
positions = {}
processed_signatures = set()

# Cache Blockhash
cached_blockhash = None
blockhash_time = 0
BLOCKHASH_CACHE_SECONDS = 25

# ====================== UTILS ======================
def base58_encode(data):
    n = int.from_bytes(data, "big")
    result = ""
    while n > 0:
        n, r = divmod(n, 58)
        result = ALPHABET[r] + result
    pad = sum(1 for b in data if b == 0)
    return "1" * pad + result

def decode_trade_event(raw):
    try:
        if len(raw) < 8 or raw[:8] != TRADE_EVENT_DISCRIMINATOR:
            return None
        data = raw[8:]
        if len(data) < 121:
            return None
        offset = 0
        mint_bytes = data[offset:offset+32]; offset += 32
        sol_amount = struct.unpack_from("<Q", data, offset)[0]; offset += 8
        token_amount = struct.unpack_from("<Q", data, offset)[0]; offset += 8
        is_buy = bool(data[offset]); offset += 1
        user_bytes = data[offset:offset+32]; offset += 32
        timestamp = struct.unpack_from("<q", data, offset)[0]; offset += 8
        virtual_sol = struct.unpack_from("<Q", data, offset)[0]; offset += 8
        virtual_token = struct.unpack_from("<Q", data, offset)[0]; offset += 8
        real_sol = struct.unpack_from("<Q", data, offset)[0]; offset += 8
        real_token = struct.unpack_from("<Q", data, offset)[0]
        return {
            "mint": base58_encode(mint_bytes),
            "sol_amount": sol_amount,
            "token_amount": token_amount,
            "is_buy": is_buy,
            "user": base58_encode(user_bytes),
            "timestamp": timestamp,
            "virtual_sol_reserves": virtual_sol,
            "virtual_token_reserves": virtual_token,
            "real_sol_reserves": real_sol,
            "real_token_reserves": real_token,
        }
    except:
        return None

async def send_telegram(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=15) as resp:
                return resp.status == 200
    except Exception as e:
        print(f"Telegram error: {e}")
        return False

def calculate_mcap(event):
    try:
        vs = event["virtual_sol_reserves"]
        vt = event["virtual_token_reserves"]
        if vt <= 0:
            return 0.0
        return (vs / LAMPORTS_PER_SOL) / (vt / TOTAL_SUPPLY)
    except:
        return 0.0

async def get_cached_blockhash(http_client):
    """Cache blockhash باش ينقص التأخير"""
    global cached_blockhash, blockhash_time
    now = time.time()
    if cached_blockhash is None or (now - blockhash_time) > BLOCKHASH_CACHE_SECONDS:
        cached_blockhash = await fetch_latest_blockhash(HELIUS_RPC, http_client=http_client)
        blockhash_time = now
    return cached_blockhash

# ====================== BUY / SELL (Helius فقط) ======================
async def execute_buy(mint: str):
    if not wallet:
        print("❌ No wallet")
        return None
    try:
        async with aiohttp.ClientSession() as http:
            plan = await build_buy(
                HELIUS_RPC,
                wallet.pubkey(),
                mint,
                sol_lamports=int(BUY_AMOUNT_SOL * LAMPORTS_PER_SOL),
                slippage_bps=SLIPPAGE_BPS,
                http_client=http
            )
            blockhash = await get_cached_blockhash(http)
            msg = build_message(wallet.pubkey(), plan.instructions, blockhash)
            tx = VersionedTransaction(msg, [wallet])

            client = AsyncClient(HELIUS_RPC, commitment=Confirmed)
            sig = await client.send_raw_transaction(bytes(tx))
            await client.close()

            print(f"✅ BUY: {mint} | {BUY_AMOUNT_SOL} SOL | {sig.value}")
            return {
                "signature": str(sig.value),
                "sol_spent": BUY_AMOUNT_SOL,
                "expected_tokens": plan.expected_tokens,
            }
    except Exception as e:
        print(f"❌ BUY failed: {e}")
        await send_telegram(f"❌ BUY FAILED\n<code>{mint}</code>\n{e}")
        return None

async def execute_sell(mint: str, token_amount: int, reason: str):
    if not wallet:
        return False
    try:
        state = await fetch_bonding_curve_state(HELIUS_RPC, mint)
        is_graduated = state.complete if state else True

        async with aiohttp.ClientSession() as http:
            plan = await build_sell(
                HELIUS_RPC,
                wallet.pubkey(),
                mint,
                token_amount=token_amount,
                slippage_bps=SLIPPAGE_BPS,
                http_client=http
            )
            blockhash = await get_cached_blockhash(http)
            msg = build_message(wallet.pubkey(), plan.instructions, blockhash)
            tx = VersionedTransaction(msg, [wallet])

            client = AsyncClient(HELIUS_RPC, commitment=Confirmed)
            sig = await client.send_raw_transaction(bytes(tx))
            await client.close()

            print(f"✅ SELL ({reason}): {mint} | graduated={is_graduated}")
            await send_telegram(
                f"✅ SELL ({reason})\n<code>{mint}</code>\nGraduated: {is_graduated}"
            )
            return True
    except Exception as e:
        print(f"❌ SELL failed: {e}")
        await send_telegram(f"❌ SELL FAILED ({reason})\n<code>{mint}</code>\n{e}")
        return False

# ====================== TOKEN LOGIC ======================
def create_token(event, signature):
    return {
        "mint": event["mint"],
        "first_timestamp": event["timestamp"],
        "trades": 0,
        "wallets": set(),
        "first_30_wallets": [],
        "top30_buy_tokens": 0,
        "top30_completion_time": None,
        "buy_sol": 0.0,
        "sell_sol": 0.0,
        "volume_sol": 0.0,
        "mcap_sol": 0.0,
        "peak_mcap_sol": 0.0,
        "liquidity_sol": 0.0,
        "wallet_stats": {},
        "excluded_wallets": set(),
        "wallet_buy_sol": {},
        "wallet_sell_sol": {},
        "cluster_events": deque(),
        "cluster_wallets": set(),
        "last_activity": time.time(),
        "alert_sent": False,
        "bought": False,
    }

async def process_trade(event, signature):
    mint = event["mint"]
    sol_amount = event["sol_amount"] / LAMPORTS_PER_SOL
    token_amount = event["token_amount"]
    wallet_addr = event["user"]

    if sol_amount < MIN_TRANSACTION_SOL:
        return

    if mint not in tokens:
        tokens[mint] = create_token(event, signature)

    token = tokens[mint]
    token["last_activity"] = time.time()

    if token["alert_sent"] and not token.get("bought"):
        return

    if wallet_addr in token["excluded_wallets"]:
        return

    current_tx = token["wallet_stats"].get(wallet_addr, 0)
    new_tx = current_tx + 1

    if new_tx > MAX_WALLET_TRANSACTIONS:
        prev = current_tx
        token["wallets"].discard(wallet_addr)
        token["excluded_wallets"].add(wallet_addr)
        token["wallet_stats"].pop(wallet_addr, None)
        token["trades"] = max(0, token["trades"] - prev)
        prev_buy = token["wallet_buy_sol"].pop(wallet_addr, 0.0)
        prev_sell = token["wallet_sell_sol"].pop(wallet_addr, 0.0)
        token["buy_sol"] = max(0.0, token["buy_sol"] - prev_buy)
        token["sell_sol"] = max(0.0, token["sell_sol"] - prev_sell)
        token["volume_sol"] = token["buy_sol"] + token["sell_sol"]
        return

    token["wallet_stats"][wallet_addr] = new_tx
    token["wallets"].add(wallet_addr)
    token["trades"] += 1

    if event["is_buy"]:
        if wallet_addr not in token["first_30_wallets"] and len(token["first_30_wallets"]) < 30:
            token["first_30_wallets"].append(wallet_addr)
            if len(token["first_30_wallets"]) == 30:
                token["top30_completion_time"] = event["timestamp"]
        if wallet_addr in token["first_30_wallets"]:
            token["top30_buy_tokens"] += token_amount
        token["buy_sol"] += sol_amount
        token["wallet_buy_sol"][wallet_addr] = token["wallet_buy_sol"].get(wallet_addr, 0.0) + sol_amount
    else:
        token["sell_sol"] += sol_amount
        token["wallet_sell_sol"][wallet_addr] = token["wallet_sell_sol"].get(wallet_addr, 0.0) + sol_amount

    token["volume_sol"] = token["buy_sol"] + token["sell_sol"]
    current_mcap = calculate_mcap(event)
    token["mcap_sol"] = current_mcap
    if current_mcap > token["peak_mcap_sol"]:
        token["peak_mcap_sol"] = current_mcap
    token["liquidity_sol"] = event["real_sol_reserves"] / LAMPORTS_PER_SOL

    now_ts = event["timestamp"]
    token["cluster_events"].append((now_ts, wallet_addr))
    while token["cluster_events"] and now_ts - token["cluster_events"][0][0] > CLUSTER_WINDOW_SECONDS:
        token["cluster_events"].popleft()
    token["cluster_wallets"] = {w for _, w in token["cluster_events"]}

    # تحديث الـ Position + TP / SL
    if mint in positions:
        pos = positions[mint]
        pos["current_mcap"] = current_mcap
        entry = pos.get("entry_mcap")
        if entry and entry > 0:
            pnl_pct = ((current_mcap - entry) / entry) * 100
            pos["pnl_pct"] = pnl_pct

            if pnl_pct >= TAKE_PROFIT_PCT and not pos.get("sold"):
                success = await execute_sell(mint, pos["tokens"], f"TAKE PROFIT +{pnl_pct:.1f}%")
                if success:
                    pos["sold"] = True

            elif pnl_pct <= -STOP_LOSS_PCT and not pos.get("sold"):
                success = await execute_sell(mint, pos["tokens"], f"STOP LOSS {pnl_pct:.1f}%")
                if success:
                    pos["sold"] = True

    await qualify_token(token)

async def qualify_token(token):
    if token["alert_sent"]:
        return

    if token["trades"] < MIN_TRANSACTIONS:
        return
    if len(token["wallets"]) < MIN_UNIQUE_WALLETS:
        return
    if token["mcap_sol"] < MIN_MCAP_SOL:
        return
    if token["liquidity_sol"] < MIN_LIQUIDITY_SOL:
        return

    liq_mcap = (token["liquidity_sol"] / token["mcap_sol"]) * 100
    if liq_mcap >= MAX_LIQUIDITY_MCAP_PCT:
        return
    if token["volume_sol"] <= 0:
        return
    vol_liq = (token["volume_sol"] / token["liquidity_sol"]) * 100
    if vol_liq >= MAX_VOLUME_LIQUIDITY_PCT:
        return

    top30_pct = (token["top30_buy_tokens"] / TOTAL_SUPPLY) * 100
    if top30_pct > MAX_TOP_30_BUY_PCT:
        return

    buy_pressure = (token["buy_sol"] / token["volume_sol"]) * 100
    if buy_pressure < MIN_BUY_PRESSURE_PCT:
        return

    token["alert_sent"] = True
    mint = token["mint"]

    # === AUTO BUY ===
    buy_result = await execute_buy(mint)
    if buy_result:
        token["bought"] = True
        positions[mint] = {
            "entry_mcap": token["mcap_sol"],
            "sol_spent": buy_result["sol_spent"],
            "tokens": buy_result["expected_tokens"],
            "current_mcap": token["mcap_sol"],
            "pnl_pct": 0.0,
            "sold": False,
            "buy_time": time.time(),
        }
        await send_telegram(
            f"🟢 AUTO BUY\n<code>{mint}</code>\n"
            f"Amount: {BUY_AMOUNT_SOL} SOL\n"
            f"Entry MCAP: {token['mcap_sol']:.2f}\n"
            f"TP: +{TAKE_PROFIT_PCT}% | SL: -{STOP_LOSS_PCT}%"
        )

# ====================== LOOPS ======================
async def cleanup_loop():
    while True:
        await asyncio.sleep(300)
        now = time.time()
        remove = [m for m, t in tokens.items() if now - t["last_activity"] > TOKEN_CLEANUP_SECONDS]
        for m in remove:
            tokens.pop(m, None)
            positions.pop(m, None)

async def status_loop():
    while True:
        await asyncio.sleep(30)
        active = sum(1 for p in positions.values() if not p.get("sold"))
        print(f"📡 Tokens: {len(tokens)} | Open positions: {active}")

async def listen_solana():
    """الاستماع فقط على Public Nodes"""
    node_index = 0
    delay = 5
    while True:
        wss = PUBLIC_WSS_NODES[node_index]
        try:
            print(f"⏳ Connecting → {wss}")
            async with websockets.connect(wss, ping_interval=15, ping_timeout=60) as ws:
                sub = {
                    "jsonrpc": "2.0", "id": 1, "method": "logsSubscribe",
                    "params": [{"mentions": [PUMP_FUN_PROGRAM]}, {"commitment": "confirmed"}]
                }
                await ws.send(json.dumps(sub))
                await ws.recv()
                print(f"✅ Connected to {wss}")
                delay = 5

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        if msg.get("method") != "logsNotification":
                            continue
                        value = msg.get("params", {}).get("result", {}).get("value", {})
                        sig = value.get("signature")
                        if not sig or sig in processed_signatures:
                            continue
                        processed_signatures.add(sig)
                        if len(processed_signatures) > MAX_PROCESSED_SIGNATURES:
                            processed_signatures.pop()

                        for log in value.get("logs", []):
                            if log.startswith("Program data:"):
                                try:
                                    encoded = log.split("Program data:", 1)[1].strip()
                                    raw_bytes = base64.b64decode(encoded)
                                    event = decode_trade_event(raw_bytes)
                                    if event:
                                        await process_trade(event, sig)
                                except:
                                    continue
                    except:
                        continue
        except Exception as e:
            print(f"❌ WS error {wss}: {e}")
            node_index = (node_index + 1) % len(PUBLIC_WSS_NODES)
            await asyncio.sleep(delay)
            delay = min(delay * 1.5, 30)

async def main():
    print("=" * 60)
    print("SOLANA EARLY SIGNAL + AUTO BUY/SELL (Hybrid)")
    print(f"Buy: {BUY_AMOUNT_SOL} SOL | Slippage: {SLIPPAGE_BPS/100}%")
    print(f"TP: +{TAKE_PROFIT_PCT}% | SL: -{STOP_LOSS_PCT}%")
    print("=" * 60)
    if not wallet:
        print("❌ PRIVATE_KEY missing!")
        return
    print(f"Wallet: {wallet.pubkey()}")
    await asyncio.gather(listen_solana(), cleanup_loop(), status_loop())

if __name__ == "__main__":
    asyncio.run(main())
