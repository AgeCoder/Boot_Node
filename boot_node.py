import asyncio
import websockets
import json
import logging
import gzip
import base64
import time
from aiohttp import web

class SuppressBadRequestFilter(logging.Filter):
    def filter(self, record):
        return not ("connection rejected (400 Bad Request)" in record.getMessage() or "connection closed" in record.getMessage())

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
websockets_logger = logging.getLogger('websockets.server')
websockets_logger.addFilter(SuppressBadRequestFilter())

PEERS = {}  # uri -> websocket
PEER_ADDRESSES = {}  # uri -> client_address
PEER_LAST_PING = {}
PORT = 10000
HTTP_PORT = 8080
PING_INTERVAL = 60
PING_TIMEOUT = 120

async def health_check(request):
    """Handle Render health checks."""
    return web.Response(status=200, text="OK")

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
                PEER_LAST_PING[uri] = time.time()
            except Exception:
                dead_peers.append(uri)
        for uri in dead_peers:
            if uri in PEERS:
                try:
                    await PEERS[uri].close()
                except:
                    pass
                del PEERS[uri]
                PEER_ADDRESSES.pop(uri, None)
                PEER_LAST_PING.pop(uri, None)
                logger.info(f"Removed dead peer: {uri}")
        await asyncio.sleep(PING_INTERVAL)

async def process_request(path, headers):
    """
    Intercept HTTP requests before WebSocket handshake.
    Handle HEAD or non-WebSocket GET requests for health checks.
    """
    if headers.get("Upgrade", "").lower() != "websocket":
        # Handle HEAD or GET requests that aren't WebSocket handshakes
        if headers.get("REQUEST_METHOD") in ("HEAD", "GET"):
            logger.info(f"Received HTTP {headers.get('REQUEST_METHOD')} request on WebSocket port, responding with 200 OK")
            return web.Response(status=200, text="OK")
        # Reject other HTTP methods
        logger.warning(f"Received unsupported HTTP method {headers.get('REQUEST_METHOD')} on WebSocket port")
        return web.Response(status=400, text="Bad Request")
    # Let WebSocket handshake proceed
    return None

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
                        logger.warning(f"Invalid peer URI from {client_address}: {peer_uri}")
                        continue
                    PEERS[peer_uri] = websocket
                    PEER_ADDRESSES[peer_uri] = client_address
                    PEER_LAST_PING[peer_uri] = time.time()
                    logger.info(f"Registered peer: {peer_uri} from {client_address}")
                    response = {
                        "type": "PEER_LIST",
                        "data": [uri for uri in PEERS.keys() if uri != peer_uri],
                        "from": "boot_node"
                    }
                    compressed_response = gzip.compress(json.dumps(response).encode('utf-8'))
                    await websocket.send(compressed_response)

                elif msg_type == "RELAY_MESSAGE":
                    target_uri = msg_data.get('target_uri')
                    relayed_data = msg_data.get('data')
                    sender = msg.get('from', 'unknown')
                    
                    if not target_uri or not relayed_data:
                        logger.warning(f"Invalid RELAY_MESSAGE from {client_address}: missing target_uri or data")
                        continue
                        
                    # Decode base64 data if it's a string
                    if isinstance(relayed_data, str):
                        try:
                            decoded_data = base64.b64decode(relayed_data)
                        except Exception as e:
                            logger.error(f"Failed to decode base64 in RELAY_MESSAGE from {client_address}: {e}")
                            continue
                    else:
                        logger.error(f"Relay data must be base64 encoded string, got {type(relayed_data)}")
                        continue
                        
                    if target_uri in PEERS:
                        try:
                            # Send the decoded data to the target peer
                            await PEERS[target_uri].send(decoded_data)
                            logger.info(f"Relayed message from {sender} to {target_uri}")
                        except Exception as e:
                            logger.error(f"Failed to relay message to {target_uri}: {e}")
                            # Send relay failure response to original sender
                            if peer_uri:
                                try:
                                    failure_msg = {
                                        "type": "RELAY_FAILURE",
                                        "data": {"target_uri": target_uri, "reason": str(e)},
                                        "from": "boot_node"
                                    }
                                    await websocket.send(gzip.compress(json.dumps(failure_msg).encode('utf-8')))
                                except Exception:
                                    pass
                    else:
                        logger.warning(f"Target peer {target_uri} not found for relay")
                        # Inform sender that target peer is not connected
                        if peer_uri:
                            try:
                                not_found_msg = {
                                    "type": "RELAY_FAILURE",
                                    "data": {"target_uri": target_uri, "reason": "peer not connected"},
                                    "from": "boot_node"
                                }
                                await websocket.send(gzip.compress(json.dumps(not_found_msg).encode('utf-8')))
                            except Exception:
                                pass

                # For non-relay messages, just broadcast to all peers
                else:
                    compressed_msg = gzip.compress(json.dumps(msg).encode('utf-8'))
                    failed_peers = []
                    for uri, peer in list(PEERS.items()):
                        if uri != peer_uri:
                            try:
                                await peer.send(compressed_msg)
                                logger.debug(f"Relayed message to {uri}")
                            except Exception as e:
                                logger.error(f"Failed to relay to {uri}: {e}")
                                failed_peers.append(uri)
                    for uri in failed_peers:
                        if uri in PEERS:
                            try:
                                await PEERS[uri].close()
                            except:
                                pass
                            del PEERS[uri]
                            PEER_ADDRESSES.pop(uri, None)
                            PEER_LAST_PING.pop(uri, None)
                            logger.info(f"Removed peer {uri} due to relay failure")

            except json.JSONDecodeError as e:
                 logger.error(f"Invalid JSON from {client_address}: {e}")
            except Exception as e:
                logger.error(f"Error processing message from {client_address}: {e}")
    except websockets.exceptions.ConnectionClosed:
        pass  # Suppressed logging due to filter
    except Exception as e:
        logger.error(f"Unexpected error in boot_handler for {client_address}: {e}")
    finally:
        if peer_uri and peer_uri in PEERS:
            try:
                await PEERS[peer_uri].close()
            except:
                pass
            del PEERS[peer_uri]
            PEER_ADDRESSES.pop(peer_uri, None)
            PEER_LAST_PING.pop(peer_uri, None)
            logger.info(f"Cleaned up peer: {peer_uri}")
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
    server = await websockets.serve(
        boot_handler,
        "0.0.0.0",
        PORT,
        max_size=1024*1024,
        ping_interval=PING_INTERVAL,
        ping_timeout=PING_TIMEOUT,
        process_request=process_request  # Add custom request processor
    )
    logger.info(f"Boot node running on ws://0.0.0.0:{PORT}")
    await server.wait_closed()

if __name__ == "__main__":
    asyncio.run(main())
