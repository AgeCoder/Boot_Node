import asyncio
import websockets
import json
import logging
import gzip
import base64
import time
from aiohttp import web
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError

class SuppressNoiseFilter(logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        return not ("connection rejected" in msg or "connection closed" in msg or "was not awaited" in msg)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
websockets_logger = logging.getLogger('websockets.server')
websockets_logger.addFilter(SuppressNoiseFilter())
aiohttp_access_logger = logging.getLogger('aiohttp.access')
aiohttp_access_logger.addFilter(SuppressNoiseFilter())


PEERS = {}
PEER_LAST_PING = {}
PORT = 10000
HTTP_PORT = 8080
PING_INTERVAL = 60
PING_TIMEOUT = 120

async def health_check(request):
    return web.Response(status=200, text="OK")

async def ping_peers():
    while True:
        await asyncio.sleep(PING_INTERVAL)
        current_time = time.time()
        dead_peers = []

        for uri, ws in list(PEERS.items()):
            if current_time - PEER_LAST_PING.get(uri, 0) > PING_TIMEOUT:
                dead_peers.append(uri)
                continue

            try:
                await ws.ping()
            except Exception:
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

async def boot_handler(websocket):
    client_address = f"ws://{websocket.remote_address[0]}:{websocket.remote_address[1]}"
    peer_uri = None

    PEERS[client_address] = websocket
    PEER_LAST_PING[client_address] = time.time()

    try:
        async for message in websocket:
            current_id = peer_uri if peer_uri else client_address
            PEER_LAST_PING[current_id] = time.time()

            try:
                if isinstance(message, bytes):
                    msg = json.loads(gzip.decompress(message).decode('utf-8'))
                else:
                    msg = json.loads(message)

                msg_type = msg.get('type')
                msg_data = msg.get('data')
                from_id = msg.get('from', 'unknown')

                if msg_type == "REGISTER_PEER":
                    new_peer_uri = msg_data.strip()
                    if not new_peer_uri or not new_peer_uri.startswith('ws://'):
                        await websocket.close(code=1008, reason="Invalid peer URI")
                        return

                    if client_address in PEERS and PEERS[client_address] == websocket and not peer_uri:
                         del PEERS[client_address]
                         PEER_LAST_PING.pop(client_address, None)

                    peer_uri = new_peer_uri
                    PEERS[peer_uri] = websocket
                    PEER_LAST_PING[peer_uri] = time.time()
                    logger.info(f"Registered peer: {peer_uri}")

                    response = {
                        "type": "PEER_LIST",
                        "data": [uri for uri in PEERS.keys() if uri != peer_uri],
                        "from": "boot_node"
                    }
                    compressed_response = gzip.compress(json.dumps(response).encode('utf-8'))
                    await websocket.send(compressed_response)

                elif msg_type == "RELAY_MESSAGE":
                    target_uri = msg_data.get('target_uri')
                    relayed_data_content = msg_data.get('data')

                    if not target_uri or relayed_data_content is None:
                        continue

                    message_to_relay = relayed_data_content

                    if target_uri in PEERS:
                        target_ws = PEERS[target_uri]
                        if target_ws.closed:
                             del PEERS[target_uri]
                             PEER_LAST_PING.pop(target_uri, None)
                             continue
                        try:
                            await target_ws.send(message_to_relay)
                        except Exception as e:
                            logger.error(f"Failed to relay message to {target_uri}: {e}")
                            logger.info(f"Removing peer {target_uri} due to relay failure.")
                            try:
                               await target_ws.close()
                            except: pass
                            if target_uri in PEERS:
                                del PEERS[target_uri]
                            PEER_LAST_PING.pop(target_uri, None)

                    else:
                        logger.warning(f"Target peer {target_uri} not found for relay requested by {current_id}")

                else:
                    compressed_msg = gzip.compress(json.dumps(msg).encode('utf-8'))
                    failed_peers = []
                    for uri, peer_ws in list(PEERS.items()):
                        if peer_ws != websocket:
                            if peer_ws.closed:
                                failed_peers.append(uri)
                                continue

                            try:
                                await peer_ws.send(compressed_msg)
                            except Exception as e:
                                logger.error(f"Failed to broadcast message {msg_type} to {uri}: {e}")
                                failed_peers.append(uri)

                    for uri in failed_peers:
                         if uri in PEERS:
                            try:
                               await PEERS[uri].close()
                            except: pass
                            del PEERS[uri]
                            PEER_LAST_PING.pop(uri, None)


            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON from {current_id}: {e}")
            except Exception as e:
                logger.error(f"Error processing message from {current_id}: {e}", exc_info=True)

    except ConnectionClosedOK:
        pass
    except ConnectionClosedError:
        pass
    except Exception as e:
        logger.error(f"Unexpected error in boot_handler for {client_address}: {e}", exc_info=True)
    finally:
        final_id = peer_uri if peer_uri else client_address
        if final_id in PEERS and PEERS[final_id] == websocket:
            try:
                await PEERS[final_id].close()
            except Exception:
                pass
            del PEERS[final_id]
            PEER_LAST_PING.pop(final_id, None)
        elif final_id in PEERS:
             pass
        else:
             pass


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
        max_size=1024*1024*10,
        ping_interval=PING_INTERVAL,
        ping_timeout=PING_TIMEOUT
    )
    logger.info(f"Boot node running on ws://0.0.0.0:{PORT}")

    await server.wait_closed()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.error(f"Boot node crashed: {e}", exc_info=True)
