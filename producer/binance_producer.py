import asyncio
import json
import time

from pathlib import Path
from aiokafka import AIOKafkaProducer
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed
async def stream_trade(producer):
    project_dir = Path(__file__).resolve().parent.parent
    config_path = project_dir / "config" / "market.json"
    config = json.loads(config_path.read_text(encoding="utf-8"))

    streams = "/".join(
        f"{item['symbol'].lower()}@{config['stream_type']}"
        for item in config["instruments"]
    )

    url = f"wss://stream.binance.com:9443/stream?streams={streams}"
    sent_count = 0
    total_latency = 0
    report_started = time.monotonic()
    async for websocket in connect(url):
        print("Đã kết nối Binance")
        try:
            async for message in websocket:
                payload = json.loads(message)
                trade = payload["data"]

                if trade.get("e") != "trade":
                    continue
                record = {
                    "symbol": trade["s"],
                    "trade_id": trade["t"],
                    "price": trade["p"],
                    "quantity": trade["q"],
                    "trade_time": trade["T"],
                    "event_time": trade["E"],
                    "is_buyer_maker": trade["m"],
                    "ingested_at": time.time_ns() // 1_000_000
                }
                metadata = await producer.send_and_wait(
                    "market_trades_raw", 
                    key=record["symbol"].encode("utf-8"),
                    value=json.dumps(record).encode("utf-8")
                )

                sent_count += 1
                latency_ms = record["ingested_at"] - record["trade_time"]
                total_latency += latency_ms
                now = time.monotonic()
                elapsed = now - report_started
                if elapsed >= 10:
                    throughput = sent_count / elapsed

                    print(
                        f"Đã gửi {sent_count} bản tin "
                        f"trong {elapsed:.1f} giây | "
                        f"throughput={throughput:.1f} msg/s | "
                        f"avg_latency_ms={total_latency/sent_count:.1f} ms"
                    )

                    sent_count = 0
                    report_started = now

        except ConnectionClosed as e:
            print(f"Kết nối Binance bị ngắt: {e}")
        print("Chờ 5 giấy trước khi kết nối lại...")
        await asyncio.sleep(5)

async def main():
    produce = AIOKafkaProducer(\
        bootstrap_servers="localhost:9094",
    )
    await produce.start()
    try:
        await stream_trade(produce)
    finally:
        await produce.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nĐã dừng")