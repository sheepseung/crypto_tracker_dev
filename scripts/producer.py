# producer.py
import asyncio
import json
import websockets
from kafka import KafkaProducer
import uuid

# Kafka Producer 설정
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Upbit WebSocket URI - 업비트 공식 WebSocket 엔드포인트
uri = "wss://api.upbit.com/websocket/v1"

# 구독할 메시지 포맷 - 업비트 공식 형식
subscribe_fmt = [
    {"ticket": str(uuid.uuid4())},
    {
        "type": "ticker",
        "codes": ["KRW-BTC"]
    }
]

async def connect_and_produce():
    """Upbit WebSocket에 연결하고, 수신한 데이터를 Kafka로 전송합니다."""
    print("Connecting to Upbit WebSocket and producing to Kafka...")
    print(f"Target URI: {uri}")
    print(f"Subscription format: {subscribe_fmt}")
    
    try:
        # SSL 검증 우회 (개발 환경에서만 사용)
        # 업비트 WebSocket API 연결 - 공식 가이드 기반
        print("🔍 Attempting WebSocket connection...")
        
        # 업비트 WebSocket API 연결 - 최소한의 옵션으로 시도
        async with websockets.connect(
            uri, 
            ssl=False,
            ping_interval=None,
            ping_timeout=None
        ) as websocket:
            print("✅ WebSocket connection established successfully!")
            
            # 구독 메시지 전송
            subscribe_data = json.dumps(subscribe_fmt)
            print(f"📤 Sending subscription: {subscribe_data}")
            await websocket.send(subscribe_data)
            print("✅ Subscription message sent successfully!")

            # 실시간 데이터 수신 및 Kafka 전송
            print("🔄 Starting real-time data streaming...")
            while True:
                try:
                    data_str = await websocket.recv()
                    data = json.loads(data_str)
                    
                    # 수신한 데이터 출력 (터미널에서 확인용)
                    print(f"📥 Received: {data}")
                    
                    # Kafka의 'crypto-trades' 토픽으로 데이터 전송
                    producer.send('crypto-trades', value=data)
                    print("✅ Data sent to Kafka successfully!")
                    
                except websockets.exceptions.ConnectionClosed:
                    print("❌ WebSocket connection closed unexpectedly")
                    break
                except json.JSONDecodeError as e:
                    print(f"⚠️ JSON decode error: {e}")
                    continue

    except Exception as e:
        print(f"❌ An error occurred: {e}")
        print(f"Error type: {type(e)}")
        import traceback
        traceback.print_exc()
        
        # 공식 문서 기반 대안 시도
        print("🔄 Trying official documentation approach...")
        await try_official_approach()
        
    finally:
        # 프로그램 종료 시 프로듀서 버퍼 비우기
        producer.flush()
        producer.close()
        print("🔒 Kafka producer closed.")

async def try_official_approach():
    """업비트 공식 문서 기반 접근 방법을 시도합니다."""
    try:
        print("🔄 Attempting official WebSocket connection method...")
        
        # 공식 문서에서 권장하는 연결 방법
        # 1. 기본 연결 시도
        async with websockets.connect(
            "wss://api.upbit.com/websocket/v1",
            ssl=False
        ) as websocket:
            print("✅ Official connection successful!")
            
            # 2. 구독 메시지 전송
            ticket = str(uuid.uuid4())
            subscribe_msg = [
                {"ticket": ticket},
                {
                    "type": "ticker",
                    "codes": ["KRW-BTC"]
                }
            ]
            
            await websocket.send(json.dumps(subscribe_msg))
            print("✅ Official subscription sent!")
            
            # 3. 데이터 수신
            while True:
                data = await websocket.recv()
                parsed_data = json.loads(data)
                print(f"📥 Official data: {parsed_data}")
                
                # Kafka로 전송
                producer.send('crypto-trades', value=parsed_data)
                print("✅ Data sent to Kafka via official method!")
                
    except Exception as e:
        print(f"❌ Official approach failed: {e}")
        print("🔄 Falling back to REST API simulation...")
        await fallback_to_rest_api()

async def fallback_to_rest_api():
    """WebSocket 실패 시 REST API로 폴백하여 실시간 스트리밍 시뮬레이션"""
    print("🔄 Starting REST API fallback for real-time streaming simulation...")
    
    import aiohttp
    
    # SSL 검증 우회
    connector = aiohttp.TCPConnector(ssl=False)
    
    async with aiohttp.ClientSession(connector=connector) as session:
        try:
            while True:
                # 업비트 시세 API 호출
                url = "https://api.upbit.com/v1/ticker?markets=KRW-BTC"
                
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        if data and len(data) > 0:
                            ticker_data = data[0]
                            
                            # WebSocket과 유사한 실시간 데이터 구조로 변환
                            realtime_data = {
                                "type": "ticker",
                                "code": ticker_data.get("market"),
                                "trade_price": ticker_data.get("trade_price"),
                                "trade_volume": ticker_data.get("trade_volume"),
                                "timestamp": ticker_data.get("timestamp"),
                                "change": ticker_data.get("change"),
                                "change_rate": ticker_data.get("change_rate"),
                                "acc_trade_price": ticker_data.get("acc_trade_price"),
                                "acc_trade_volume": ticker_data.get("acc_trade_volume")
                            }
                            
                            print(f"📥 Simulated real-time data: {realtime_data}")
                            
                            # Kafka로 전송
                            producer.send('crypto-trades', value=realtime_data)
                            print("✅ Simulated data sent to Kafka!")
                        else:
                            print("⚠️ No data received from API")
                    else:
                        print(f"❌ API request failed: {response.status}")
                
                # 1초마다 데이터 요청 (실시간 스트리밍 시뮬레이션)
                await asyncio.sleep(1)
                
        except Exception as e:
            print(f"❌ REST API fallback failed: {e}")
        finally:
            producer.flush()
            producer.close()

if __name__ == "__main__":
    try:
        print("🚀 Starting Upbit WebSocket Producer for Flink Streaming...")
        asyncio.run(connect_and_produce())
    except KeyboardInterrupt:
        print("⏹️ Script interrupted by user.")
    except Exception as e:
        print(f"💥 Fatal error: {e}")
        import traceback
        traceback.print_exc()