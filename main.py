import asyncio
import logging
import os
from decimal import Decimal, ROUND_DOWN
from datetime import datetime, timedelta, timezone
from typing import List

from dotenv import load_dotenv
from x10.perpetual.accounts import StarkPerpetualAccount
from x10.perpetual.configuration import MAINNET_CONFIG
from x10.perpetual.orders import OrderSide, TimeInForce, SelfTradeProtectionLevel
from x10.perpetual.order_object import create_order_object
from x10.perpetual.trading_client import PerpetualTradingClient
from x10.perpetual.orderbooks import OrderbookUpdateModel
from x10.perpetual.stream_client.stream_client import PerpetualStreamClient
from x10.utils.http import StreamDataType
from sortedcontainers import SortedDict

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("scalper")

# --- CONFIGURATION ---
API_KEY = os.getenv("API_KEY")
PRIVATE_KEY = os.getenv("PRIVATE_KEY")
PUBLIC_KEY = os.getenv("PUBLIC_KEY")
VAULT_ID = int(os.getenv("VAULT_ID"))

MARKET = "SOL-USD"
ORDER_EXPIRY = 60
TP_REPRICE_DELAY = 180
TOTAL_USD_CAP = Decimal("7000")
RETRY_LIMIT = 3

MARKET_CONFIG = {
    "START_SIZE": Decimal("0.1"),
    "TICK_SIZE": Decimal("0.01"),
    "MIN_SIZE_INCREMENT": Decimal("0.01"),
    "PROFIT_MARGIN": Decimal("0.0002"),
}


# --- ORDERBOOK ---
class OrderBookEntry:
    def __init__(self, price: Decimal, amount: Decimal):
        self.price = price
        self.amount = amount


class OrderBook:
    def __init__(self, market_name):
        self.market_name = market_name
        self.bids = SortedDict()
        self.asks = SortedDict()
        self._stream_client = PerpetualStreamClient(api_url=MAINNET_CONFIG.stream_url)

    async def start(self):
        async with self._stream_client.subscribe_to_orderbooks(self.market_name) as stream:
            async for event in stream:
                if event.type == StreamDataType.SNAPSHOT.value:
                    self._init(event.data)
                elif event.type == StreamDataType.DELTA.value:
                    self._update(event.data)

    def _init(self, data: OrderbookUpdateModel):
        self.bids.clear()
        self.asks.clear()
        for b in data.bid:
            self.bids[b.price] = OrderBookEntry(b.price, b.qty)
        for a in data.ask:
            self.asks[a.price] = OrderBookEntry(a.price, a.qty)

    def _update(self, data: OrderbookUpdateModel):
        for b in data.bid:
            if b.qty == 0:
                self.bids.pop(b.price, None)
            else:
                self.bids[b.price] = OrderBookEntry(b.price, b.qty)
        for a in data.ask:
            if a.qty == 0:
                self.asks.pop(a.price, None)
            else:
                self.asks[a.price] = OrderBookEntry(a.price, a.qty)

    def best_ask(self):
        return self.asks.peekitem(0)[1] if self.asks else None

    def best_bid(self):
        return self.bids.peekitem(-1)[1] if self.bids else None


# --- SCALPING BOT ---
class ScalpingBot:
    def __init__(self, client: PerpetualTradingClient, orderbook: OrderBook):
        self.client = client
        self.orderbook = orderbook
        self.open_buys = []
        self.tp_order = None

    async def run(self):
        while True:
            try:
                await self.handle_cycle()
            except Exception as e:
                logger.exception(f"Cycle error: {e}")
            await asyncio.sleep(60)

    async def handle_cycle(self):
        await self.cleanup_stale_orders()
        await self.detect_filled_buys()
        await self.recheck_tp_fill()
        await self.place_tp_if_needed()
        await self.place_new_buy()

    async def cleanup_stale_orders(self):
        now = datetime.utcnow().timestamp()
        updated = []
        for order in self.open_buys:
            if not order["filled"] and now - order["timestamp"] > ORDER_EXPIRY:
                try:
                    await self.client.orders.cancel_order(order_id=order["id"])
                    logger.info(f"Canceled stale BUY: {order['id']}")
                except Exception as e:
                    logger.warning(f"Cancel failed: {e}")
            else:
                updated.append(order)
        self.open_buys = updated

    async def detect_filled_buys(self):
        remaining = []
        for order in self.open_buys:
            if order["filled"]:
                remaining.append(order)
                continue
            try:
                status = await self.client.orders.get_order(order_id=order["id"])
                if status.data and status.data.status == "FILLED":
                    logger.info(f"BUY filled: {order['id']}")
                    order["filled"] = True
                remaining.append(order)
            except Exception as e:
                logger.warning(f"Order fetch error: {e}")
                remaining.append(order)
        self.open_buys = remaining

    async def recheck_tp_fill(self):
        if not self.tp_order:
            return
        try:
            status = await self.client.orders.get_order(order_id=self.tp_order["id"])
            if status.data.status == "FILLED":
                logger.info(f"TP filled: {self.tp_order['id']}")
                self.open_buys = []
                self.tp_order = None
        except Exception as e:
            logger.warning(f"TP fetch failed: {e}")

    async def place_tp_if_needed(self):
        if self.tp_order:
            # Cancel if older than repricing window
            if datetime.utcnow().timestamp() - self.tp_order["timestamp"] > TP_REPRICE_DELAY:
                try:
                    await self.client.orders.cancel_order(order_id=self.tp_order["id"])
                    logger.info("Repricing TP after timeout.")
                    self.tp_order = None
                except Exception as e:
                    logger.warning(f"Failed to cancel TP: {e}")
            else:
                return

        filled = [o for o in self.open_buys if o["filled"]]
        if not filled:
            return

        qty = sum(o["qty"] for o in filled)
        avg_price = sum(o["qty"] * o["price"] for o in filled) / qty
        tp_price = (avg_price * (1 + MARKET_CONFIG["PROFIT_MARGIN"])).quantize(MARKET_CONFIG["TICK_SIZE"])

        for _ in range(RETRY_LIMIT):
            try:
                market_model = (await self.client.markets_info.get_markets([MARKET])).data[0]
                order = create_order_object(
                    account=self.client.account._get_stark_account(),
                    market=market_model,
                    amount_of_synthetic=qty.quantize(MARKET_CONFIG["MIN_SIZE_INCREMENT"], ROUND_DOWN),
                    price=tp_price,
                    side=OrderSide.SELL,
                    time_in_force=TimeInForce.GTT,
                    expire_time=datetime.now(timezone.utc) + timedelta(minutes=5),
                    self_trade_protection_level=SelfTradeProtectionLevel.ACCOUNT
                )
                placed = await self.client.orders.place_order(order=order)
                if placed.data:
                    self.tp_order = {
                        "id": placed.data.id,
                        "timestamp": datetime.utcnow().timestamp()
                    }
                    logger.info(f"TP placed: {qty} @ {tp_price}")
                    return
            except Exception as e:
                logger.warning(f"TP place failed: {e}")
        logger.error("❌ TP placement failed after retries.")

    async def place_new_buy(self):
        best = self.orderbook.best_ask()
        if not best:
            logger.warning("No ask available.")
            return

        qty = MARKET_CONFIG["START_SIZE"].quantize(MARKET_CONFIG["MIN_SIZE_INCREMENT"])
        price = best.price.quantize(MARKET_CONFIG["TICK_SIZE"])
        notional = qty * price

        total_pos = sum(o["qty"] * o["price"] for o in self.open_buys if o["filled"])
        if total_pos + notional > TOTAL_USD_CAP:
            logger.warning("Exposure cap reached.")
            return

        balance = await self.client.account.get_balance()
        if notional > Decimal(balance.data.available_for_trade):
            logger.warning("Insufficient balance.")
            return

        try:
            market_model = (await self.client.markets_info.get_markets([MARKET])).data[0]
            order = create_order_object(
                account=self.client.account._get_stark_account(),
                market=market_model,
                amount_of_synthetic=qty,
                price=price,
                side=OrderSide.BUY,
                time_in_force=TimeInForce.GTT,
                expire_time=datetime.now(timezone.utc) + timedelta(minutes=1),
                self_trade_protection_level=SelfTradeProtectionLevel.ACCOUNT
            )
            placed = await self.client.orders.place_order(order=order)
            if placed.data:
                self.open_buys.append({
                    "id": placed.data.id,
                    "qty": qty,
                    "price": price,
                    "timestamp": datetime.utcnow().timestamp(),
                    "filled": False
                })
                logger.info(f"Placed BUY: {qty} @ {price}")
        except Exception as e:
            logger.warning(f"Buy failed: {e}")


# --- ENTRY POINT ---
async def main():
    stark = StarkPerpetualAccount(
        api_key=API_KEY,
        public_key=PUBLIC_KEY,
        private_key=PRIVATE_KEY,
        vault=VAULT_ID
    )
    client = PerpetualTradingClient(MAINNET_CONFIG, stark)
    ob = OrderBook(MARKET)
    asyncio.create_task(ob.start())
    await asyncio.sleep(2)  # wait for snapshot
    bot = ScalpingBot(client, ob)
    await bot.run()

if __name__ == "__main__":
    asyncio.run(main())
