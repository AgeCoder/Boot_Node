import asyncio
import websockets
import json
import logging
import gzip
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

PEERS = set()  # Store peer URIs
PORT = 10000   # Render port

async def boot_handler(websocket):
    client_address = f"{websocket.remote_address[0]}:{websocket.remote_address[1]}"
    peer_uri = None  # Initialize peer_uri
    try:
        async for message in websocket:
            try:
                # Handle compressed or uncompressed messages
                if isinstance(message, bytes):
                    try:
                        decompressed = gzip.decompress(message).decode('utf-8')
                        msg = json.loads(decompressed)
                    except (gzip.BadGzipFile, UnicodeDecodeError) as e:
                        logger.error(f"Invalid message format from {client_address}: {e}")
                        continue
                else:
                    msg = json.loads(message)

                msg_type = msg.get('type')
                msg_data = msg.get('data')

                if not msg_type or not isinstance(msg_data, str):
                    logger.warning(f"Invalid message from {client_address}: missing type or data")
                    continue

                if msg_type == "REGISTER_PEER":
                    peer_uri = msg_data.strip()
                    if not peer_uri.startswith(('ws://', 'wss://')):
                        logger.warning(f"Invalid peer URI from {client_address}: {peer_uri}")
                        continue
                    PEERS.add(peer_uri)
                    logger.info(f"Registered peer: {peer_uri} from {client_address}")
                    response = json.dumps({"type": "PEER_LIST", "data": list(PEERS - {peer_uri})})
                    compressed_response = gzip.compress(response.encode('utf-8'))
                    await websocket.send(compressed_response)
                    logger.debug(f"Sent peer list to {peer_uri}")

            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error from {client_address}: {e}")
                continue
            except Exception as e:
                logger.error(f"Error processing message from {client_address}: {e}")
                continue
    except websockets.exceptions.ConnectionClosed as e:
        logger.info(f"Connection closed for {client_address}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in connection from {client_address}: {e}")
    finally:
        if peer_uri and peer_uri in PEERS:
            PEERS.remove(peer_uri)
            logger.info(f"Removed peer: {peer_uri}")

async def main():
    try:
        server = await websockets.serve(
            boot_handler,
            "0.0.0.0",
            PORT,
            max_size=1024 * 1024,
            ping_interval=30,
            ping_timeout=60,
            close_timeout=10
        )
        logger.info(f"Boot node running on ws://0.0.0.0:{PORT}")
        await server.wait_closed()
    except Exception as e:
        logger.error(f"Fatal error starting server: {e}")
        sys.exit(1)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Boot node stopped by user")
    except Exception as e:
        logger.error(f"Fatal error in boot node: {e}")
        sys.exit(1)
