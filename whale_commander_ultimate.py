import asyncio
import numpy as np
from collections import deque
from ib_insync import *
import datetime as dt
import os
import csv
import json
import urllib.request
import urllib.error

# --- ANSI COLOR CODES (MATCHING WOWII VISUAL MATRIX) ---
RESET = "\033[0m"
YELLOW = "\033[93m"
RED = "\033[91m"
GREEN = "\033[92m"
CYAN = "\033[96m"
BOLD = "\033[1m"

# --- TARGET CONFIGURATION (SINGLE TICKER MODE) ---
SYMBOL = "PLTR"        
EXCHANGE = "SMART"     
CURRENCY = "USD"       

PORT = 7497 
CLIENT_ID = 11  

# --- PRODUCTION TELEGRAM ROUTING PIPELINE ---
BOT_TOKEN = "8990071141:AAHdFjyEimLiZZ24OXUkS6BlizFy9H_D_Pw"
CHAT_ID = "8822560650"

# --- HARPOON LOGGER SETUP ---
HARPOON_LOG = r"C:\Trading_Data\Harpoon_Deployments.csv"

def log_harpoon_event(symbol, price, score, z, pressure, mtt, status):
    if not os.path.exists(r"C:\Trading_Data"):
        os.makedirs(r"C:\Trading_Data")
    file_exists = os.path.isfile(HARPOON_LOG)
    with open(HARPOON_LOG, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["Timestamp", "Symbol", "Price", "Score", "Z_Score", "Pressure", "MTT_Delta", "Event"])
        timestamp = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        writer.writerow([timestamp, symbol, price, score, f"{z:.2f}", f"{pressure:.2f}", f"{mtt:.2f}", status])


def dispatch_telegram_text(message: str) -> bool:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        data = json.dumps(payload).encode('utf-8')
        req = urllib.request.Request(url, data=data, headers={'Content-Type': 'application/json'}, method='POST')
        with urllib.request.urlopen(req, timeout=5) as res:
            return res.status == 200
    except Exception:
        return False


def dispatch_telegram_screenshot() -> bool:
    import subprocess
    import sys
    try:
        from PIL import ImageGrab
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pillow"])
        from PIL import ImageGrab

    screenshot_path = os.path.join(os.environ.get('TEMP', 'C:\\'), 'whale_terminal.png')
    try:
        img = ImageGrab.grab()
        img.save(screenshot_path, "PNG")
        if not os.path.exists(screenshot_path): return False
            
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
        boundary = "----WhaleBoundaryDataPartition"
        with open(screenshot_path, "rb") as image_file:
            img_data = image_file.read()
            
        body = (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="chat_id"\r\n\r\n{CHAT_ID}\r\n'
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="photo"; filename="dashboard.png"\r\n'
            f"Content-Type: image/png\r\n\r\n"
        ).encode('utf-8') + img_data + f"\r\n--{boundary}--\r\n".encode('utf-8')
        
        req = urllib.request.Request(url, data=body, headers={'Content-Type': f'multipart/form-data; boundary={boundary}'}, method='POST')
        with urllib.request.urlopen(req, timeout=10) as res:
            success = res.status == 200
        if os.path.exists(screenshot_path): os.remove(screenshot_path)
        return success
    except Exception:
        if os.path.exists(screenshot_path):
            try: os.remove(screenshot_path)
            except: pass
        return False


class ExecutionRouter:
    """Handles the lifecycle of staged limit buy orders with hardover overnight routing."""
    def __init__(self):
        self.active_order = None
        self.active_trade = None

    def stage_floor_order(self, ib, contract, floor_price, size):
        order = LimitOrder('BUY', size, floor_price)
        order.tif = 'DAY'  # Ensures it goes into the overnight book 
        order.outsideRth = True    # Bypasses RTH restrictions for overnight execution
        order.transmit = False     # Holds staged in framework memory
        
        self.active_order = order
        self.active_trade = ib.placeOrder(contract, order)
        return self.active_trade.order.orderId

    def transmit_pending(self, ib):
        if self.active_order and self.active_trade and not self.active_trade.isDone():
            self.active_order.transmit = True
            ib.placeOrder(self.active_trade.contract, self.active_order)
            return True
        return False

    def cancel_all(self, ib):
        if self.active_trade and not self.active_trade.isDone():
            ib.cancelOrder(self.active_order)
            self.active_order = None
            self.active_trade = None
            return True
        return False


class WhaleEntryLogic:
    """Preserves 100% of the internal math arrays and indicators from WOWII.py"""
    def __init__(self):
        self.msg_bucket = deque(maxlen=1000)   
        self.trade_bucket = deque(maxlen=1000) 
        self.trade_sizes = deque(maxlen=1000) 
        self.mtt_gauge_history = deque(maxlen=240) 
        self.total_session_trades = 0
        self.tick_sizes = deque(maxlen=500)
        self.price_history = deque(maxlen=60) 

        self.vwap_price_history = deque(maxlen=60)
        self.vwap_size_history = deque(maxlen=60)

        self.aggressive_inst_vol = deque(maxlen=200) 
        self.passive_inst_vol = deque(maxlen=200)
        self.retail_noise_vol = deque(maxlen=500)
        
        self.last_green_alert = dt.datetime.min
        self.last_yellow_alert = dt.datetime.min
        self.last_stealth_alert = dt.datetime.min

        self.spread_history = deque(maxlen=100)
        self.ask_ping_tracker = deque(maxlen=100)
        self.last_spread_snap_alert = dt.datetime.min
        self.last_ask_ping_alert = dt.datetime.min
        self.rolling_10min_vol = 0
        self.last_vol_reset = dt.datetime.now()

        self.manual_floor = None
        self.last_floor_proximity_alert = dt.datetime.min

    def on_pending_tick(self, ticker):
        now = dt.datetime.now()
        if (now - self.last_vol_reset).total_seconds() >= 600:
            self.rolling_10min_vol = 0
            self.last_vol_reset = now

        for t in ticker.ticks:
            if t.tickType in [1, 2, 6, 7]:
                self.msg_bucket.append(now)
                bid = ticker.bid or 0
                ask = ticker.ask or 0
                if bid > 0 and ask > 0:
                    self.spread_history.append(ask - bid)
                    
            elif t.tickType in [4, 5]:
                self.trade_bucket.append(now)
                if t.tickType == 5:
                    self.total_session_trades += 1
                    if t.size > 0:
                        self.trade_sizes.append(t.size)
                        self.tick_sizes.append(t.size)
                        self.rolling_10min_vol += t.size
                        
                        if t.price > 0:
                            self.vwap_price_history.append(t.price)
                            self.vwap_size_history.append(t.size)
                            
                            ask_p = ticker.ask or 0
                            if ask_p > 0 and abs(t.price - ask_p) < 0.001:
                                self.ask_ping_tracker.append((now, t.size))
                            else:
                                self.ask_ping_tracker.clear()
                        
                        if len(self.trade_sizes) > 30:
                            historical_median = np.median(list(self.trade_sizes))
                            whale_threshold = max(200, historical_median * 5)
                        else:
                            whale_threshold = 500

                        bid = ticker.bid or 0
                        ask = ticker.ask or 0
                        if t.size >= whale_threshold:
                            if t.price == bid or t.price == ask: self.passive_inst_vol.append(t.size)
                            else: self.aggressive_inst_vol.append(t.size)

    def calculate_metrics(self, price, size, bid_size, ask_size):
        msgs = len(self.msg_bucket)
        trades = len(self.trade_bucket)
        mtt_live = msgs / trades if trades > 0 else float(msgs)
        self.mtt_gauge_history.append(mtt_live)
        
        price, size = price or 0, size or 0
        bid_size, ask_size = bid_size or 1, ask_size or 1
        if price > 0: self.price_history.append(price)

        abs_pct = 0.0
        if len(self.trade_sizes) > 20:
            sizes_list = list(self.trade_sizes)
            avg_retail = np.median(sizes_list)
            whale_vol = sum(s for s in sizes_list if s >= max(200, avg_retail * 5))
            total_vol = sum(sizes_list)
            abs_pct = (whale_vol / total_vol) * 100 if total_vol > 0 else 0

        pass_sum = sum(self.passive_inst_vol)
        aggr_sum = sum(self.aggressive_inst_vol)
        v_delta = aggr_sum / pass_sum if pass_sum > 0 else float(aggr_sum) if aggr_sum > 0 else 1.0

        stealth_score = 0
        if len(self.price_history) >= 10:
            high_p = max(self.price_history)
            low_p = min(self.price_history)
            displacement = (high_p - low_p) * 100 
            recent_vol = sum(list(self.tick_sizes)[-60:])
            stealth_score = recent_vol / max(0.01, displacement)

        if len(self.price_history) < 10 or len(self.mtt_gauge_history) < 10 or len(self.vwap_price_history) < 10:
            return 0, 0, 1.0, 1.0, v_delta, self.total_session_trades, abs_pct, stealth_score

        prices_arr = np.array(list(self.vwap_price_history))
        sizes_arr = np.array(list(self.vwap_size_history))
        sum_sizes = np.sum(sizes_arr)
        v_mu = np.sum(prices_arr * sizes_arr) / sum_sizes if sum_sizes > 0 else price
        v_variance = np.sum(sizes_arr * ((prices_arr - v_mu) ** 2)) / sum_sizes if sum_sizes > 0 else 0.01
        v_std = np.sqrt(v_variance)
        v_z = (price - v_mu) / v_std if v_std > 0.001 else 0.0

        score = 0
        gauge_avg = np.mean(list(self.mtt_gauge_history))
        mtt_delta = mtt_live / gauge_avg if gauge_avg > 0 else 1.0
        if mtt_delta > 2.5: score += 30 
        if size > (np.percentile(list(self.tick_sizes), 90) if len(self.tick_sizes) > 50 else 500) * 3: score += 30
        if bid_size / ask_size > 2.0: score += 20
        if abs(v_z) > 2.0: score += 20
        
        return score, v_z, bid_size / ask_size, mtt_delta, v_delta, self.total_session_trades, abs_pct, stealth_score


async def telegram_command_worker(qualified_contract, logic, router, ib):
    offset = 0
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
    
    while True:
        try:
            poll_url = f"{url}?offset={offset}&timeout=2"
            req = urllib.request.Request(poll_url, headers={'User-Agent': 'Mozilla/5.0'})
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(None, lambda: urllib.request.urlopen(req, timeout=5).read())
            res_data = json.loads(response.decode('utf-8'))
            
            for update in res_data.get("result", []):
                offset = update.get("update_id", 0) + 1
                message = update.get("message", {})
                raw_text = message.get("text", "").strip()
                text = raw_text.upper()
                
                if not text: continue

                # COMMAND 1: CONFIG ALERT BOUNDARIES
                if text.startswith("SET"):
                    try:
                        parts = text.split()
                        if len(parts) == 2:
                            new_level = float(parts[1])
                            logic.manual_floor = new_level
                            msg = f"🎯 **FLOOR SET:** Boundary for `{SYMBOL}` updated to **`${new_level:.2f}`**"
                            print(f"\n🎯 {CYAN}[TELEGRAM]{RESET} Floor set to {new_level}")
                            await loop.run_in_executor(None, dispatch_telegram_text, msg)
                    except ValueError:
                        await loop.run_in_executor(None, dispatch_telegram_text, "❌ Format error. Use: SET 24.50")

                # COMMAND 2: STAGE PASSIVE OVERNIGHT LIMIT BUY
                elif text.startswith("STAGE"):
                    if logic.manual_floor is None:
                        await loop.run_in_executor(None, dispatch_telegram_text, "❌ Order blocked. Assign a target floor first via 'SET [PRICE]'.")
                        continue
                    try:
                        parts = text.split()
                        if len(parts) == 2:
                            target_size = int(parts[1])
                            floor_px = logic.manual_floor
                            
                            router.cancel_all(ib) 
                            order_id = router.stage_floor_order(ib, qualified_contract, floor_px, target_size)
                            
                            msg = (
                                f"📝 **OVERNIGHT BUY LINE STAGED:**\n"
                                f"• **Asset:** `{SYMBOL}`\n"
                                f"• **Price Floor:** `${floor_px:.2f}`\n"
                                f"• **Size:** `{target_size}` shares\n"
                                f"• **TWS ID:** `{order_id}`\n"
                                f"👉 Send `TRANSMIT` to route order onto overnight books."
                            )
                            await loop.run_in_executor(None, dispatch_telegram_text, msg)
                    except Exception as e:
                        await loop.run_in_executor(None, dispatch_telegram_text, f"❌ Engine failure: {str(e)}")

                # COMMAND 3: AUTHORIZE AND TRANSMIT STAGED LINE
                elif text == "TRANSMIT":
                    success = router.transmit_pending(ib)
                    msg = f"🚀 **TRANSMITTED:** Staged floor buy line for `{SYMBOL}` is now live in overnight book!" if success else "⚠️ No working staged order found."
                    await loop.run_in_executor(None, dispatch_telegram_text, msg)

                # 🟢 INTEGRATED COMMAND 4: IMMEDIATE OVERNIGHT MARKET EXIT
                elif text.startswith("SELL MARKET"):
                    try:
                        parts = text.split()
                        if len(parts) == 3:
                            target_size = int(parts[2])
                            
                            order = MarketOrder('SELL', target_size)
                            order.tif = 'DAY'
                            order.outsideRth = True
                            
                            ib.placeOrder(qualified_contract, order)
                            
                            msg = f"🛑 **MARKET SELL ORDER ROUTED:** Dumping `{target_size}` shares of `{SYMBOL}` overnight."
                            await loop.run_in_executor(None, dispatch_telegram_text, msg)
                    except Exception as e:
                        await loop.run_in_executor(None, dispatch_telegram_text, f"❌ Sell Engine Failure: {str(e)}")

                # 🟢 INTEGRATED COMMAND 5: OVERHEAD TARGET OVERNIGHT LIMIT OFFER
                elif text.startswith("SELL LIMIT"):
                    try:
                        parts = text.split()
                        if len(parts) == 4:
                            target_price = float(parts[2])
                            target_size = int(parts[3])
                            
                            order = LimitOrder('SELL', target_size, target_price)
                            order.tif = 'DAY'
                            order.outsideRth = True
                            
                            ib.placeOrder(qualified_contract, order)
                            
                            msg = f"🎯 **LIMIT SELL OFFER PLACED:** Offering `{target_size}` shares of `{SYMBOL}` at **`${target_price:.2f}`** overnight."
                            await loop.run_in_executor(None, dispatch_telegram_text, msg)
                    except Exception as e:
                        await loop.run_in_executor(None, dispatch_telegram_text, f"❌ Sell Engine Failure: {str(e)}")

                # COMMAND 6: ABORT ORDERS IMMEDIATELY
                elif text == "CANCEL":
                    success = router.cancel_all(ib)
                    msg = f"🛑 **CANCELLED:** Staged entries yanked out of TWS framework." if success else "⚠️ No active items."
                    await loop.run_in_executor(None, dispatch_telegram_text, msg)

                # COMMAND 7: SCREENSHOT TELEMETRY
                elif text == "SNAP":
                    await loop.run_in_executor(None, dispatch_telegram_screenshot)

                # COMMAND 8: SYSTEM DATA LOOKUP
                elif text == "STATUS":
                    ticker_data = ib.ticker(qualified_contract)
                    price = ticker_data.last or ticker_data.close or 0
                    ord_status = f"`{router.active_trade.status}` at `${router.active_order.lmtPrice:.2f}` ({router.active_order.totalQuantity} shrs)" if router.active_trade else "NONE WORKING"
                    
                    status_msg = (
                        f"📊 **CORE TELEMETRY: {SYMBOL}**\n"
                        f"• **Last Price:** `${price:.2f}`\n"
                        f"• **Current Alert Floor:** `${f'{logic.manual_floor:.2f}' if logic.manual_floor else 'None Assigned'}`\n"
                        f"• **Order Router:** {ord_status}"
                    )
                    await loop.run_in_executor(None, dispatch_telegram_text, status_msg)

        except Exception:
            pass
        await asyncio.sleep(2)


async def run_scanner():
    ib = IB()
    router = ExecutionRouter()
    try:
        await ib.connectAsync('127.0.0.1', PORT, clientId=CLIENT_ID)
        contract = Stock(SYMBOL, EXCHANGE, CURRENCY)
        cds = await ib.qualifyContractsAsync(contract)
        if not cds: return
            
        qualified_contract = cds[0]
        logic = WhaleEntryLogic() 
        
        ticker = ib.reqMktData(qualified_contract, '233', False, False)
        ticker.updateEvent += logic.on_pending_tick
            
        os.system('cls' if os.name == 'nt' else 'clear')
        print(f"\n{BOLD}[INTERACTIVE WHALE INTERFACE ONLINE]{RESET} Monitoring: {SYMBOL}")
        print("Awaiting dynamic routing instructions from phone...\n")

        asyncio.create_task(telegram_command_worker(qualified_contract, logic, router, ib))

        while True:
            await asyncio.sleep(1)
            now = dt.datetime.now()
            ticker_data = ib.ticker(qualified_contract)
            
            # Extracts trade tape, falling back to close or spread midpoint if flat overnight
            price = ticker_data.last or ticker_data.close or ((ticker_data.bid + ticker_data.ask) / 2 if (ticker_data.bid and ticker_data.ask) else 0)
            
            score, v_z, pressure, mtt_delta, v_delta, t_count, abs_pct, stlth = logic.calculate_metrics(
                price=price, size=ticker_data.lastSize or 0, 
                bid_size=ticker_data.bidSize or 0, ask_size=ticker_data.askSize or 0
            )

            # Automated Boundary Corridor Alerts
            if logic.manual_floor is not None and price > 0:
                if abs(price - logic.manual_floor) <= 0.0501:
                    if (now - logic.last_floor_proximity_alert).total_seconds() >= 120:
                        logic.last_floor_proximity_alert = now
                        floor_alert = (
                            f"🎯 **FLOOR POCKET CONTACT: {SYMBOL}**\n"
                            f"Price matching inside your tracking zone:\n"
                            f"• **Last Tape:** `${price:.2f}` | **Your Floor:** `${logic.manual_floor:.2f}`\n"
                            f"• **HFT Score:** `{score}` | **Abs%:** `{abs_pct:.1f}%`\n"
                            f"👉 To stage from this floor, send: `STAGE 500` then `TRANSMIT`"
                        )
                        loop = asyncio.get_event_loop()
                        loop.run_in_executor(None, dispatch_telegram_text, floor_alert)

            # HFT Indicators
            if len(logic.spread_history) >= 20 and ticker_data.bid > 0 and ticker_data.ask > 0:
                if (ticker_data.ask - ticker_data.bid) > (np.median(list(logic.spread_history)) * 3.0):
                    if (now - logic.last_spread_snap_alert).total_seconds() >= 180:
                        logic.last_spread_snap_alert = now
                        loop = asyncio.get_event_loop()
                        loop.run_in_executor(None, dispatch_telegram_text, f"⚠️ **SPREAD SNAP:** Liquidity pulled on `{SYMBOL}`. Spread: `{ticker_data.ask - ticker_data.bid:.2f}`")

            # Color attributes copied from original WOWII panel format
            s_color = YELLOW if score >= 50 else RESET
            if score >= 80: s_color = GREEN + BOLD
            z_color = RED + BOLD if abs(v_z) >= 3.0 else (YELLOW if abs(v_z) >= 2.0 else RESET)
            st_color = GREEN if stlth > 5000 else RESET 
            vd_str = f"{RED}{v_delta:>4.1f}{RESET}" if v_delta > 3.0 else (f"{CYAN}{v_delta:>4.1f}{RESET}" if v_delta < 0.4 else f"{v_delta:>4.1f}")
            
            status = f"{GREEN}ROUTING  {RESET}" if router.active_trade and not router.active_trade.isDone() else "STALKING "
            flr_lbl = f" | Floor: {logic.manual_floor:.2f}" if logic.manual_floor else " | Floor: Unset"

            # Unified dashboard output matching visual style of WOWII
            print(
                f"[{SYMBOL:<4}] {status} | "
                f"Price: {price:>7.2f} | "
                f"Score: {s_color}{score:>3}{RESET} | "
                f"Z: {z_color}{v_z:>5.2f}{RESET} | "
                f"Press: {pressure:>5.2f} | "
                f"Abs%: {abs_pct:>5.1f} | "
                f"V-Dlt: {vd_str} | "
                f"Stlth: {st_color}{stlth:>7.0f}{RESET} | "
                f"MTTΔ: {mtt_delta:>5.2f} | "
                f"T-Cnt: {t_count:>5}{flr_lbl} \033[K", 
                end='\r'
            )

    except Exception as e:
        print(f"\n[CRASH PREVENTED]: {e}")
    finally:
        ib.disconnect()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_scanner())





