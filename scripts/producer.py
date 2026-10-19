"""업비트 WebSocket 의 체결(trade) 메시지를 받은 그대로 Kafka 토픽에 넣는다."""
import asyncio
import json
import logging
import os
import uuid

import websockets
from kafka import KafkaProducer

UPBIT_WS_URI = "wss://api.upbit.com/websocket/v1"

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "crypto-trades")
MARKETS = [m.strip() for m in os.getenv("UPBIT_MARKETS", "KRW-BTC,KRW-ETH,KRW-DOGE").split(",") if m.strip()]
RECONNECT_MAX_WAIT_SEC = 30

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")


def subscribe_message() -> str:
    # 재접속마다 내려오는 스냅샷(마지막 체결 1건)이 다시 집계되지 않도록 실시간분만 받는다
    return json.dumps([
        {"ticket": str(uuid.uuid4())},
        {"type": "trade", "codes": MARKETS, "isOnlyRealtime": True},
        {"format": "DEFAULT"},
    ])


async def stream_trades(producer: KafkaProducer) -> None:
    # 업비트는 120초 동안 ping 이 없으면 연결을 끊으므로 그보다 짧은 주기로 보낸다
    async with websockets.connect(UPBIT_WS_URI, ping_interval=20, ping_timeout=20) as ws:
        await ws.send(subscribe_message())
        log.info("subscribed type=trade markets=%s topic=%s", MARKETS, TOPIC)
        async for raw in ws:
            payload = raw if isinstance(raw, bytes) else raw.encode("utf-8")
            try:
                code = json.loads(payload)["code"]
            except (ValueError, KeyError, TypeError):
                log.warning("skip message without code: %r", payload[:200])
                continue
            # 같은 종목의 체결이 한 파티션에 순서대로 쌓이도록 종목 코드를 키로 쓴다
            producer.send(TOPIC, key=code.encode("utf-8"), value=payload)


async def run_forever(producer: KafkaProducer) -> None:
    wait = 1
    while True:
        try:
            await stream_trades(producer)
            log.info("connection closed by server, reconnecting")
            wait = 1
        except (websockets.exceptions.WebSocketException, OSError) as e:
            log.warning("websocket error: %s (reconnect in %ss)", e, wait)
            await asyncio.sleep(wait)
            wait = min(wait * 2, RECONNECT_MAX_WAIT_SEC)


def main() -> None:
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS, acks="all")
    try:
        asyncio.run(run_forever(producer))
    except KeyboardInterrupt:
        log.info("stopped by user")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
