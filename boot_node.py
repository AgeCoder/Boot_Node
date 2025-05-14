import asyncio
import websockets
import json
import logging
import os
import sys
import gzip
import socket
import requests
from websockets.exceptions import ConnectionClosed
from urllib.parse import urlparse

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

REGISTERED_NODES = set()
PORT = int(os.getenv('PORT', 9000))
ENV = os.getenv('ENV', 'development')

def get_public_ip():
    try:
        # Try multiple methods to get public IP
        try:
            return requests.get('https://api.ipify.org').text
        except:
            return socket.gethostbyname(socket.gethostname())
    except Exception as e:
        logger.error(f"Could not determine public IP: {e}")
        return "127.0.0.1"

def is_valid_uri(uri):
    try:
        parsed = urlparse(uri)
        valid_schemes = ('ws', 'wss') if ENV == 'production' else ('ws',)
        return parsed.scheme in valid_schemes and parsed.hostname and parsed.port
    except Exception:
        return False

async def boot_handler(websocket):
    client_address = f"{websocket.remote_address[0]}:{websocket.remote_address[1]}"
    try:
        async for message in websocket:
            try:
                if isinstance(message, bytes):
                    try:
                        decompressed = gzip.decompress(message).decode('utf-8')
                        msg = json.loads(decompressed)
                    except gzip.BadGzipFile:
                        logger.error(f"Invalid gzip data from {client_address}")
                        continue
                else:
                    msg = json.loads(message)

                msg_type = msg.get('type')
                msg_data = msg.get('data')

                if not msg_type or not isinstance(msg_data, str):
                    continue

                if msg_type == 'REGISTER_PEER':
                    uri = msg_data.strip()
                    
                    # If client sends local IP, replace with their public IP (from connection)
                    parsed = urlparse(uri)
                    if parsed.hostname in ['localhost', '127.0.0.1']:
                        # Use the connecting IP as the public IP
                        public_ip = websocket.remote_address[0]
                        uri = f"{parsed.scheme}://{public_ip}:{parsed.port}"
                    
                    if ENV == 'production' and not uri.startswith('wss://'):
                        parsed = urlparse(uri)
                        uri = f"wss://{parsed.hostname}:{parsed.port}"
                    
                    if not is_valid_uri(uri):
                        logger.warning(f"Invalid URI from {client_address}: {uri}")
                        continue

                    REGISTERED_NODES.add(uri)
                    logger.info(f"Registered peer: {uri} from {client_address}")
                    
                    peer_list = list(REGISTERED_NODES - {uri})
                    response = json.dumps({'type': 'PEER_LIST', 'data': peer_list})
                    
                    if isinstance(message, bytes):
                        compressed_response = gzip.compress(response.encode('utf-8'))
                        await websocket.send(compressed_response)
                    else:
                        await websocket.send(response)

            except json.JSONDecodeError:
                continue
            except Exception as e:
                logger.error(f"Error processing message from {client_address}: {e}")

    except ConnectionClosed:
        pass
    except Exception as e:
        logger.error(f"Unexpected error in connection from {client_address}: {e}")

async def main():
    public_ip = get_public_ip()
    logger.info(f"Public IP detected: {public_ip}")
    
    try:
        server = await websockets.serve(
            boot_handler,
            "0.0.0.0",  # Listen on all interfaces
            PORT,
            max_size=1024 * 1024,
            ping_interval=30,
            ping_timeout=60,
            close_timeout=10
        )
        
        logger.info(f"Boot node running on ws://{public_ip}:{PORT}")
        logger.info(f"Local access: ws://localhost:{PORT}")
        
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
