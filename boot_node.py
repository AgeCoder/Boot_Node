import asyncio
import websockets
import json
import logging
import gzip
import base64
import time
from aiohttp import web

cclass SuppressBadRequestFilter(logging.Filter):
    def filter(self, record):
        return not ("connection rejected (400 Bad Request)" in record.getMessage() or "connection closed" in record.getMessage())

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
websockets_logger = logging.getLogger('websockets.server')
websockets_logger.addFilter(SuppressBadRequestFilter())

PEERS = {}  # uri -> websocket
PEER_LAST_PING = {}
PORT = 10000
HTTP_PORT = 8080
PING_INTERVAL = 60  # Increased to reduce load
PING_TIMEOUT = 120  # Adjusted for stability

async def ping_peers():
    while True:
        current_time = time.time()
        dead_peers = []
        for uri, ws in list(PEERS.items()):
            if current_time - PEER_LAST_PING.get(uri, 0) > PING_TIMEOUT:
                dead_peers.append(uri)
                continue
            try:
                await ws.ping()
                PEER_LAST_PING[uri] = current_time
            except Exception as e:
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
        PEER_LAST_PING[client_address] = time.time()
        async for message in websocket:
            PEER_LAST_PING[client_address] = time.time()
            try:
                if isinstance(message, bytes):
                    msg = json.loads(gzip.decompress(message).decode('utf-8'))
                else:
                    msg = json.loads(message)
                msg_type = msg.get('type')
                msg_data = msg.get('data')

                if msg_type == "REGISTER_PEER":
                    peer_uri = msg_data.strip()
                    if not peer_uri.startswith('ws://'):
                        continue
                    PEERS[peer_uri] = websocket
                    PEER_LAST_PING[peer_uri] = time.time()
                    logger.info(f"Registered peer: {peer_uri} from {client_address}")
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
                        continue
                    if isinstance(relayed_data, str):
                        try:
                            relayed_data = base64.b64decode(relayed_data)
                        except Exception as e:
                            logger.error(f"Failed to decode base64: {e}")
                            continue
                    if target_uri in PEERS:
                        await PEERS[target_uri].send(relayed_data)
                    else:
                        logger.warning(f"Target peer {target_uri} not found")

                else:
                    compressed_msg = gzip.compress(json.dumps(msg).encode('utf-8'))
                    for uri, peer in list(PEERS.items()):
                        if uri != peer_uri:
                            try:
                                await peer.send(compressed_msg)
                            except Exception as e:
                                logger.error(f"Failed to relay to {uri}: {e}")
                                try:
                                    await peer.close()
                                    del PEERS[uri]
                                    PEER_LAST_PING.pop(uri, None)
                                except:
                                    pass

            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON from {client_address}: {e}")
            except Exception as e:
                logger.error(f"Error processing message from {client_address}: {e}")
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
        PEER_LAST_PING.pop(client_address, None)

async def main():
    app = web.Application()
    app.add_routes([web.get('/health', health_check)])
    runner = web.AppRunner(app)
    await runner.setup()
    http_site = web.TCPSite(runner, '0.0.0.0', HTTP_PORT)
    await http_site.start()
    logger.info(f"HTTP health check running on http://0.0.0.0:{HTTP_PORT}/health")

    asyncio.create_task(ping_peers())
    server = await websockets.serve(boot_handler, "0.0.0.0", PORT, max_size=1024*1024)
    logger.info(f"Boot node running on ws://0.0.0.0:{PORT}")
    await server.wait_closed()

if __name__ == "__main__":
    asyncio.run(main())
