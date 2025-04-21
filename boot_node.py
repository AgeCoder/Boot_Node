import asyncio
import websockets
import json
import logging
import os
import sys
from websockets.exceptions import ConnectionClosed
from urllib.parse import urlparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('boot_node.log')
    ]
)
logger = logging.getLogger(__name__)

# Global set for registered nodes
REGISTERED_NODES = set()

# Get port from environment variable (Render assigns dynamically)
PORT = int(os.getenv('PORT', 9000))  # Fallback to 9000 for local testing

# Validate WebSocket URI
def is_valid_uri(uri):
    try:
        parsed = urlparse(uri)
        return parsed.scheme in ('ws', 'wss') and parsed.hostname and parsed.port
    except Exception:
        return False

async def boot_handler(websocket):
    client_address = f"{websocket.remote_address[0]}:{websocket.remote_address[1]}"
    logger.info(f"New connection from {client_address}")
    try:
        async for message in websocket:
            try:
                msg = json.loads(message)
                msg_type = msg.get('type')
                msg_data = msg.get('data')

                if not msg_type or not isinstance(msg_data, str):
                    logger.warning(f"Invalid message format from {client_address}")
                    await websocket.send(json.dumps({
                        'type': 'ERROR',
                        'data': 'Invalid message format'
                    }))
                    continue

                if msg_type == 'REGISTER_PEER':
                    uri = msg_data.strip()
                    if not is_valid_uri(uri):
                        logger.warning(f"Invalid URI from {client_address}: {uri}")
                        await websocket.send(json.dumps({
                            'type': 'ERROR',
                            'data': 'Invalid WebSocket URI'
                        }))
                        continue

                    REGISTERED_NODES.add(uri)
                    logger.info(f"Registered peer: {uri} from {client_address}")
                    peer_list = list(REGISTERED_NODES - {uri})
                    await websocket.send(json.dumps({
                        'type': 'PEER_LIST',
                        'data': peer_list
                    }))
                    logger.debug(f"Sent peer list to {client_address}: {peer_list}")

                else:
                    logger.warning(f"Unknown message type from {client_address}: {msg_type}")
                    await websocket.send(json.dumps({
                        'type': 'ERROR',
                        'data': f'Unknown message type: {msg_type}'
                    }))

            except json.JSONDecodeError:
                logger.error(f"Failed to parse JSON from {client_address}")
                await websocket.send(json.dumps({
                    'type': 'ERROR',
                    'data': 'Invalid JSON format'
                }))
            except Exception as e:
                logger.error(f"Error processing message from {client_address}: {e}")
                await websocket.send(json.dumps({
                    'type': 'ERROR',
                    'data': f'Server error: {str(e)}'
                }))

    except ConnectionClosed:
        logger.info(f"Connection closed by {client_address}")
    except Exception as e:
        logger.error(f"Unexpected error in connection from {client_address}: {e}")

async def main():
    try:
        server = await websockets.serve(
            boot_handler,
            "0.0.0.0",
            PORT,
            max_size=1024 * 1024,  # 1MB max message size
            ping_interval=30,
            ping_timeout=60
        )
        logger.info(f"Boot node running on port {PORT}")
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