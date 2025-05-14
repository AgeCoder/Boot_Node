import asyncio
import websockets
import json
import logging
import gzip
import base64
import time
from aiohttp import web

# Custom logging filter to suppress 400 Bad Request logs
class SuppressBadRequestFilter(logging.Filter):
    def filter(self, record):
        return "connection rejected (400 Bad Request)" not in record.getMessage()

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
websockets_logger = logging.getLogger('websockets.server')
websockets_logger.addFilter(SuppressBadRequestFilter())

PEERS = {}  # Store peer URI -> websocket
PEER_LAST_PING = {}  # Store last ping time for each peer
PORT = 10000
PING_INTERVAL = 30  # Ping every 30 seconds
PING_TIMEOUT = 60  # Remove peer if no response for 60 seconds
HTTP_PORT = 8080  # HTTP port for health checks

async def health_check(request):
    """Handle Render health checks."""
    return web.Response(status=200, text="OK")

async def ping_peers():
    """Periodically ping peers and remove those that are not live."""
    while True:
        current_time = time.time()
        dead_peers = []
        for uri, ws in list(PEERS.items()):
            if current_time - PEER_LAST_PING.get(uri, 0) > PING_TIMEOUT:
                logger.warning(f"Peer {uri} timed out, marking for removal")
                dead_peers.append(uri)
                continue
            try:
                await ws.ping()
                PEER_LAST_PING[uri] = current_time
                logger.debug(f"Pinged peer {uri}")
            except Exception as e:
                logger.warning(f"Failed to ping peer {uri}: {e}, marking for removal")
                dead_peers.append(uri)
        for uri in dead_peers:
            if uri in PEERS:
                try:
                    await PEERS[uri].close()
                except:
                    pass
                del PEERS[uri]
                PEER_LAST_PING.pop(uri, None)
                logger.info(f"Removed dead peer: {uri}")
        await asyncio.sleep(PING_INTERVAL)

async def boot_handler(websocket):
    client_address = f"{websocket.remote_address[0]}:{websocket.remote_address[1]}"
    peer_uri = None
    try:
        PEER_LAST_PING[client_address] = time.time()  # Initialize ping time
        async for message in websocket:
            PEER_LAST_PING[client_address] = time.time()  # Update on message receipt
            try:
                # Handle compressed bytes or JSON string
                if isinstance(message, bytes):
                    msg = json.loads(gzip.decompress(message).decode('utf-8'))
                else:
                    msg = json.loads(message)

                msg_type = msg.get('type')
                msg_data = msg.get('data')
                from_id = msg.get('from', 'unknown')

                if msg_type == "REGISTER_PEER":
                    peer_uri = msg_data.strip()
                    if not peer_uri.startswith('ws://'):
                        logger.warning(f"Invalid peer URI from {client_address}: {peer_uri}")
                        continue
                    PEERS[peer_uri] = websocket
                    PEER_LAST_PING[peer_uri] = time.time()
                    logger.info(f"Registered peer: {peer_uri} from {client_address}")
                    # Send compressed peer list excluding self
                    response = {
                        "type": "PEER_LIST",
                        "data": [uri for uri in PEERS.keys() if uri != peer_uri],
                        "from": "boot_node"
                    }
                    await websocket.send(gzip.compress(json.dumps(response).encode('utf-8')))

                elif msg_type == "RELAY_MESSAGE":
                    target_uri = msg_data.get('target_uri')
                    relayed_data = msg_data.get('data')
                    if not target_uri or not relayed_data:
                        logger.warning(f"Invalid RELAY_MESSAGE from {peer_uri}: missing target_uri or data")
                        continue
                    if isinstance(relayed_data, str):
                        # Decode base64 if string
                        try:
                            relayed_data = base64.b64decode(relayed_data)
                        except Exception as e:
                            logger.error(f"Failed to decode base64 data in RELAY_MESSAGE from {peer_uri}: {e}")
                            continue
                    if target_uri in PEERS:
                        logger.debug(f"Relaying message from {peer_uri} to {target_uri}")
                        # Relay the original compressed message
                        await PEERS[target_uri].send(relayed_data)
                    else:
                        logger.warning(f"Target peer {target_uri} not found for relay from {peer_uri}")

                # Relay all other message types to all peers except sender
                else:
                    logger.info(f"Relaying message type {msg_type} from {peer_uri} to all peers")
                    compressed_msg = gzip.compress(json.dumps(msg).encode('utf-8'))
                    failed_peers = []
                    for uri, peer in list(PEERS.items()):
                        if uri != peer_uri:
                            try:
                                await peer.send(compressed_msg)
                                logger.debug(f"Relayed {msg_type} to {uri}")
                            except Exception as e:
                                logger.error(f"Failed to relay {msg_type} to {uri}: {e}")
                                failed_peers.append(uri)
                    for uri in failed_peers:
                        if uri in PEERS:
                            try:
                                await PEERS[uri].close()
                            except:
                                pass
                            del PEERS[uri]
                            PEER_LAST_PING.pop(uri, None)
                            logger.info(f"Removed peer {uri} due to relay failure")

            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON from {client_address}: {e}")
            except Exception as e:
                logger.error(f"Error processing message from {client_address}: {e}")
                continue
    except websockets.exceptions.ConnectionClosed:
        logger.info(f"Connection closed for {client_address}")
    finally:
        if peer_uri and peer_uri in PEERS:
            try:
                await PEERS[peer_uri].close()
            except:
                pass
            del PEERS[peer_uri]
            PEER_LAST_PING.pop(peer_uri, None)
            logger.info(f"Removed peer: {peer_uri}")
        PEER_LAST_PING.pop(client_address, None)

async def main():
    # Start HTTP server for health checks
    app = web.Application()
    app.add_routes([web.get('/health', health_check)])
    runner = web.AppRunner(app)
    await runner.setup()
    http_site = web.TCPSite(runner, '0.0.0.0', HTTP_PORT)
    await http_site.start()
    logger.info(f"HTTP health check running on http://0.0.0.0:{HTTP_PORT}/health")

    # Start ping task
    asyncio.create_task(ping_peers())

    # Start WebSocket server
    server = await websockets.serve(boot_handler, "0.0.0.0", PORT, max_size=1024*1024)
    logger.info(f"Boot node running on ws://0.0.0.0:{PORT}")
    await server.wait_closed()

if __name__ == "__main__":
    asyncio.run(main())
