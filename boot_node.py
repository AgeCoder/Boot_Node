import asyncio
import websockets
import json
import logging
import os
import sys
import gzip
import requests
from urllib.parse import urlparse
from websockets.exceptions import ConnectionClosed

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Environment variables
ENV = os.getenv('ENV', 'development')
PORT = int(os.getenv('PORT', 9000))

class BootNode:
    def __init__(self):
        self.registered_nodes = set()
        self.public_ip = self.get_public_ip()

    def get_public_ip(self) -> str:
        try:
            return requests.get('https://api.ipify.org').text
        except Exception as e:
            logger.error(f"Could not determine public IP: {e}")
            return "127.0.0.1"

    def is_valid_uri(self, uri: str) -> bool:
        try:
            parsed = urlparse(uri)
            valid_schemes = ('wss',) if ENV == 'production' else ('ws', 'wss')
            return (parsed.scheme in valid_schemes and parsed.hostname and parsed.port and
                    parsed.hostname not in ['localhost', '127.0.0.1'])
        except Exception:
            return False

    async def handle_connection(self, websocket):
        client_address = f"{websocket.remote_address[0]}:{websocket.remote_address[1]}"
        try:
            async for message in websocket:
                try:
                    # Decompress message if necessary
                    if isinstance(message, bytes):
                        msg = json.loads(gzip.decompress(message).decode('utf-8'))
                    else:
                        msg = json.loads(message)

                    msg_type = msg.get('type')
                    msg_data = msg.get('data')

                    if msg_type == 'REGISTER_PEER' and isinstance(msg_data, str):
                        uri = msg_data.strip()
                        parsed = urlparse(uri)
                        # Replace local IPs with client's public IP
                        if parsed.hostname in ['localhost', '127.0.0.1']:
                            uri = f"{parsed.scheme}://{websocket.remote_address[0]}:{parsed.port}"
                        if ENV == 'production' and not uri.startswith('wss://'):
                            uri = f"wss://{parsed.hostname}:{parsed.port}"
                        if not self.is_valid_uri(uri):
                            logger.warning(f"Invalid URI from {client_address}: {uri}")
                            continue

                        self.registered_nodes.add(uri)
                        logger.info(f"Registered peer: {uri} from {client_address}")

                        # Send peer list (excluding the registering peer)
                        peer_list = list(self.registered_nodes - {uri})
                        response = json.dumps({'type': 'PEER_LIST', 'data': peer_list})
                        await websocket.send(gzip.compress(response.encode('utf-8')))
                except (json.JSONDecodeError, gzip.BadGzipFile):
                    logger.error(f"Invalid message from {client_address}")
                    continue
                except Exception as e:
                    logger.error(f"Error processing message from {client_address}: {e}")
        except ConnectionClosed:
            logger.info(f"Connection closed from {client_address}")
        except Exception as e:
            logger.error(f"Connection error from {client_address}: {e}")

    async def start(self):
        try:
            server = await websockets.serve(
                self.handle_connection,
                "0.0.0.0",
                PORT,
                max_size=1024 * 1024,
                ping_interval=30,
                ping_timeout=60,
                close_timeout=10
            )
            logger.info(f"Bootnode running on wss://{self.public_ip}:{PORT}")
            await server.wait_closed()
        except Exception as e:
            logger.error(f"Fatal error starting bootnode: {e}")
            sys.exit(1)

if __name__ == "__main__":
    bootnode = BootNode()
    asyncio.run(bootnode.start())
