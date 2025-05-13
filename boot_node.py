# import asyncio
# import websockets
# import json
# import logging
# import os
# import sys
# import gzip
# from websockets.exceptions import ConnectionClosed
# from urllib.parse import urlparse

# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s [%(levelname)s] %(message)s',
#     handlers=[logging.StreamHandler(sys.stdout)]
# )
# logger = logging.getLogger(__name__)

# REGISTERED_NODES = set()
# PORT = int(os.getenv('PORT', 9000))

# def is_valid_uri(uri):
#     try:
#         parsed = urlparse(uri)
#         return parsed.scheme in ('ws', 'wss') and parsed.hostname and parsed.port
#     except Exception:
#         return False

# async def boot_handler(websocket):
#     client_address = f"{websocket.remote_address[0]}:{websocket.remote_address[1]}"
#     try:
#         async for message in websocket:
#             try:
#                 if isinstance(message, bytes):
#                     try:
#                         decompressed = gzip.decompress(message).decode('utf-8')
#                         msg = json.loads(decompressed)
#                     except gzip.BadGzipFile:
#                         logger.error(f"Invalid gzip data from {client_address}")
#                         continue
#                 else:
#                     msg = json.loads(message)

#                 msg_type = msg.get('type')
#                 msg_data = msg.get('data')

#                 if not msg_type or not isinstance(msg_data, str):
#                     continue

#                 if msg_type == 'REGISTER_PEER':
#                     uri = msg_data.strip()
#                     if not is_valid_uri(uri):
#                         continue

#                     REGISTERED_NODES.add(uri)
#                     logger.info(f"Registered peer: {uri} from {client_address}")
#                     peer_list = list(REGISTERED_NODES - {uri})
#                     response = json.dumps({'type': 'PEER_LIST', 'data': peer_list})
#                     compressed_response = gzip.compress(response.encode('utf-8'))
#                     await websocket.send(compressed_response)

#             except json.JSONDecodeError:
#                 continue
#             except Exception as e:
#                 logger.error(f"Error processing message from {client_address}: {e}")

#     except ConnectionClosed:
#         pass
#     except Exception as e:
#         logger.error(f"Unexpected error in connection from {client_address}: {e}")

# async def main():
#     try:
#         server = await websockets.serve(
#             boot_handler,
#             "0.0.0.0",
#             PORT,
#             max_size=1024 * 1024,
#             ping_interval=30,
#             ping_timeout=60,
#             close_timeout=10
#         )
#         logger.info(f"Boot node running on ws://0.0.0.0:{PORT}")
#         await server.wait_closed()
#     except Exception as e:
#         logger.error(f"Fatal error starting server: {e}")
#         sys.exit(1)

# if __name__ == "__main__":
#     try:
#         asyncio.run(main())
#     except KeyboardInterrupt:
#         logger.info("Boot node stopped by user")
#     except Exception as e:
#         logger.error(f"Fatal error in boot node: {e}")
#         sys.exit(1)
import asyncio
import websockets
import json
import logging
import os
import ssl
import gzip
from urllib.parse import urlparse
import stun  # Replacing pystun3

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Globals
REGISTERED_NODES = set()
PORT = int(os.getenv('PORT', 9000))
ENV = os.getenv('ENV', 'production')


def is_valid_uri(uri):
    try:
        parsed = urlparse(uri)
        return parsed.scheme == 'wss' and parsed.hostname and parsed.port
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

                if not msg_type or not isinstance(msg_data, dict):
                    continue

                if msg_type == 'REGISTER_PEER':
                    uri = msg_data.get('uri', '').strip()
                    if not is_valid_uri(uri):
                        logger.error(f"Invalid URI from {client_address}: {uri}")
                        continue

                    REGISTERED_NODES.add(uri)
                    logger.info(f"Registered peer: {uri} from {client_address}")
                    peer_list = list(REGISTERED_NODES - {uri})
                    response = json.dumps({'type': 'PEER_LIST', 'data': peer_list})
                    compressed_response = gzip.compress(response.encode('utf-8'))
                    await websocket.send(compressed_response)

            except json.JSONDecodeError:
                logger.error(f"Invalid JSON from {client_address}")
                continue
            except Exception as e:
                logger.error(f"Error processing message from {client_address}: {e}")

    except websockets.exceptions.ConnectionClosed:
        pass
    except Exception as e:
        logger.error(f"Unexpected error in connection from {client_address}: {e}")


async def main():
    # Use STUN to get external IP
    try:
        nat_type, external_ip, external_port = stun.get_ip_info(stun_host='stun.l.google.com', stun_port=19302)
        logger.info(f"NAT Type: {nat_type}, External IP: {external_ip}, Port: {external_port}")
    except Exception as e:
        logger.warning(f"Could not determine external IP using STUN: {e}")

    try:
        ssl_context = None
        if ENV == 'production':
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            certfile = os.getenv('SSL_CERT_FILE', 'server.crt')
            keyfile = os.getenv('SSL_KEY_FILE', 'server.key')
            try:
                ssl_context.load_cert_chain(certfile=certfile, keyfile=keyfile)
                logger.info("SSL certificates loaded successfully")
            except FileNotFoundError as e:
                logger.warning(f"SSL certificate files not found: {e}. Falling back to default SSL context")
                ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            except Exception as e:
                logger.error(f"Error loading SSL certificates: {e}. Falling back to default SSL context")
                ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)

        server = await websockets.serve(
            boot_handler,
            "0.0.0.0",
            PORT,
            ssl=ssl_context,
            max_size=1024 * 1024,
            ping_interval=30,
            ping_timeout=60,
            close_timeout=10
        )
        logger.info(f"Boot node running on wss://0.0.0.0:{PORT}")
        await server.wait_closed()
    except Exception as e:
        logger.error(f"Fatal error starting server: {e}")
        raise


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Boot node stopped by user")
    except Exception as e:
        logger.error(f"Fatal error in boot node: {e}")
        raise
