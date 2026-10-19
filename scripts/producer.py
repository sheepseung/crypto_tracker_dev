"""업비트 WebSocket 의 체결(trade)·호가(orderbook) 메시지를 받은 그대로 Kafka 토픽에 넣는다."""
import asyncio
import json
import logging
import os
import uuid

import websockets
from kafka import KafkaProducer

UPBIT_WS_URI = "wss://api.upbit.com/websocket/v1"

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TRADE_TOPIC = os.getenv("KAFKA_TOPIC", "crypto-trades")
ORDERBOOK_TOPIC = os.getenv("KAFKA_ORDERBOOK_TOPIC", "crypto-orderbook")
MARKETS = [m.strip() for m in os.getenv("UPBIT_MARKETS", "KRW-BTC,KRW-ETH,KRW-DOGE").split(",") if m.strip()]
RECONNECT_MAX_WAIT_SEC = 30

# 메시지 type 별로 보낼 토픽. 다른 type 은 버린다
TOPIC_BY_TYPE = {"trade": TRADE_TOPIC, "orderbook": ORDERBOOK_TOPIC}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")


def subscribe_message() -> str:
    # 재접속마다 내려오는 스냅샷(마지막 체결 1건)이 다시 집계되지 않도록 체결은 실시간분만 받는다.
    # 호가는 스냅샷이 곧 현재 상태라 그대로 받는다
    return json.dumps([
        {"ticket": str(uuid.uuid4())},
        {"type": "trade", "codes": MARKETS, "isOnlyRealtime": True},
        {"type": "orderbook", "codes": MARKETS},
        {"format": "DEFAULT"},
    ])


async def stream_messages(producer: KafkaProducer) -> None:
    # 업비트는 120초 동안 ping 이 없으면 연결을 끊으므로 그보다 짧은 주기로 보낸다
    async with websockets.connect(UPBIT_WS_URI, ping_interval=20, ping_timeout=20) as ws:
        await ws.send(subscribe_message())
        log.info("subscribed types=trade,orderbook markets=%s topics=%s", MARKETS, TOPIC_BY_TYPE)
        async for raw in ws:
            payload = raw if isinstance(raw, bytes) else raw.encode("utf-8")
            try:
                message = json.loads(payload)
                code = message["code"]
                topic = TOPIC_BY_TYPE[message["type"]]
            except (ValueError, KeyError, TypeError):
                log.warning("skip message: %r", payload[:200])
                continue
            # 같은 종목의 메시지가 한 파티션에 순서대로 쌓이도록 종목 코드를 키로 쓴다
            producer.send(topic, key=code.encode("utf-8"), value=payload)


async def run_forever(producer: KafkaProducer) -> None:
    wait = 1
    while True:
        try:
            await stream_messages(producer)
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
