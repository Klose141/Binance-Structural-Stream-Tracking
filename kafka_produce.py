import asyncio
import json
import logging
import websockets
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
symbols = ["btcusdt", "ethusdt", "solusdt", "adausdt", "dogeusdt"]
stream_names = "/".join([f"{s}@miniTicker" for s in symbols])
BINANCE_WS_URL = f"wss://stream.binance.com:9443/ws/{stream_names}"

# Initialize Kafka producer with better configuration
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8") if k else None,
    acks='all',  # Wait for all replicas to acknowledge
    retries=3,
    max_in_flight_requests_per_connection=1,
    enable_idempotence=True,
    batch_size=16384,
    linger_ms=10
)


def send_to_kafka(data):
    """Send data to Kafka with error handling"""
    try:
        # Extract symbol for partitioning
        symbol = data.get('s', 'unknown').lower()

        # Send to Kafka with symbol as key for proper partitioning
        future = producer.send(
            topic='crypto_prices',
            key=symbol,
            value=data
        )

        # Optional: Add callback for delivery confirmation
        future.add_callback(lambda metadata: logger.debug(f"Message sent to {metadata.topic}:{metadata.partition}"))
        future.add_errback(lambda exception: logger.error(f"Failed to send message: {exception}"))

        logger.info(f"Sent to Kafka: {symbol} - Price: {data.get('c', 'N/A')}")

    except KafkaError as e:
        logger.error(f"Kafka error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error sending to Kafka: {e}")


async def listen():
    """Main WebSocket listener with reconnection logic"""
    max_retries = 5
    retry_count = 0

    while retry_count < max_retries:
        try:
            logger.info("Connecting to Binance WebSocket...")

            async with websockets.connect(
                    BINANCE_WS_URL,
                    ping_interval=20,  # Send ping every 20 seconds
                    ping_timeout=10,  # Wait 10 seconds for pong
                    close_timeout=10,
                    max_size=10 ** 7,  # 10MB max message size
                    compression=None
            ) as ws:
                logger.info("Connected to Binance WebSocket")
                retry_count = 0  # Reset retry count on successful connection

                async for message in ws:
                    try:
                        # Parse JSON message
                        data = json.loads(message)

                        # Handle different message formats
                        if 'stream' in data and 'data' in data:
                            # Wrapped stream format
                            stream_name = data['stream']
                            ticker_data = data['data']
                            ticker_data['stream'] = stream_name
                            ticker_data['timestamp'] = ticker_data.get('E', 0)
                            send_to_kafka(ticker_data)

                        elif 'e' in data and data['e'] == '24hrMiniTicker':
                            # Direct miniTicker format
                            ticker_data = data.copy()
                            ticker_data['timestamp'] = ticker_data.get('E', 0)
                            send_to_kafka(ticker_data)

                        else:
                            logger.warning(f"Unexpected message format: {data}")

                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to parse JSON: {e}")
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")

        except websockets.exceptions.ConnectionClosed as e:
            logger.warning(f"WebSocket connection closed: {e}")
            retry_count += 1
            if retry_count < max_retries:
                wait_time = min(2 ** retry_count, 60)  # Exponential backoff, max 60s
                logger.info(f"Reconnecting in {wait_time} seconds... (attempt {retry_count}/{max_retries})")
                await asyncio.sleep(wait_time)
            else:
                logger.error("Max retries reached. Exiting.")
                break

        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            retry_count += 1
            if retry_count < max_retries:
                await asyncio.sleep(5)
            else:
                break


async def main():
    """Main function with cleanup"""
    try:
        await listen()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        logger.info("Closing Kafka producer...")
        producer.flush()  # Ensure all messages are sent
        producer.close()
        logger.info("Cleanup complete")


if __name__ == "__main__":
    asyncio.run(main())