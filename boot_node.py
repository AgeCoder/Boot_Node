import asyncio
import json
import logging
import gzip
import base64
import time
import signal
from aiohttp import web
from pubnub.pnconfiguration import PNConfiguration
from pubnub.pubnub_asyncio import PubNubAsyncio
from pubnub.callbacks import SubscribeCallback

class SuppressBadRequestFilter(logging.Filter):
    def filter(self, record):
        return not ("connection rejected (400 Bad Request)" in record.getMessage())

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
websockets_logger = logging.getLogger('pubnub')
websockets_logger.addFilter(SuppressBadRequestFilter())

# Global variables
PEERS = {}  # uri -> node_id
PEER_LAST_PING = {}
PORT = 10000
HTTP_PORT = 8080
PING_INTERVAL = 60
PING_TIMEOUT = 120
BOOT_CHANNEL = "boot-channel"

# Configure PubNub (replace with your actual keys)
pnconfig = PNConfiguration()
pnconfig.subscribe_key = "sub-c-f65aee46-02ff-4296-ac70-8c545d222ed8"
pnconfig.publish_key = "pub-c-fffb4e62-2213-45e6-b9bf-1ed03312d268"
pnconfig.user_id = "boot-node"
pubnub = PubNubAsyncio(pnconfig)

class MySubscribeCallback(SubscribeCallback):
    async def message(self, pubnub, event):
        try:
            decoded_data = base64.b64decode(event.message)
            decompressed_data = gzip.decompress(decoded_data)
            msg = json.loads(decompressed_data.decode('utf-8'))
            msg_type = msg.get('type')
            msg_data = msg.get('data')
            client_address = event.publisher

            if not isinstance(msg, dict) or 'type' not in msg or 'data' not in msg:
                logger.warning(f"Malformed message from {client_address}: {msg}")
                return

            if msg_type == "REGISTER_PEER":
                peer_uri = msg_data.strip()
                if not peer_uri.startswith('ws://'):
                    logger.warning(f"Invalid peer URI from {client_address}: {peer_uri}")
                    return
                PEERS[peer_uri] = client_address
                PEER_LAST_PING[peer_uri] = time.time()
                logger.info(f"Registered peer: {peer_uri} from {client_address}")
                response = {
                    "type": "PEER_LIST",
                    "data": [uri for uri in PEERS.keys() if uri != peer_uri],
                    "from": "boot_node"
                }
                compressed_response = gzip.compress(json.dumps(response).encode('utf-8'))
                encoded_response = base64.b64encode(compressed_response).decode('utf-8')
                await pubnub.publish().channel(BOOT_CHANNEL).message(encoded_response).pn_async()

            elif msg_type == "RELAY_MESSAGE":
                target_uri = msg_data.get('target_uri')
                relayed_data = msg_data.get('data')
                if not target_uri or not relayed_data:
                    logger.warning(f"Invalid RELAY_MESSAGE from {client_address}")
                    return
                if isinstance(relayed_data, str):
                    try:
                        relayed_data = base64.b64decode(relayed_data)
                    except Exception as e:
                        logger.error(f"Failed to decode base64 in RELAY_MESSAGE: {e}")
                        return
                if target_uri in PEERS:
                    target_channel = f"peer-{PEERS[target_uri]}"
                    encoded_data = base64.b64encode(relayed_data).decode('utf-8')
                    await pubnub.publish().channel(target_channel).message(encoded_data).pn_async()
                    logger.debug(f"Relayed message to {target_uri} on {target_channel}")
                else:
                    logger.warning(f"Target peer {target_uri} not found for relay")

            elif msg_type == "PING":
                PEER_LAST_PING[client_address] = time.time()

            else:
                compressed_msg = gzip.compress(json.dumps(msg).encode('utf-8'))
                for uri, node_id in list(PEERS.items()):
                    if uri != msg_data.get('peer_uri'):
                        target_channel = f"peer-{node_id}"
                        encoded_msg = base64.b64encode(compressed_msg).decode('utf-8')
                        await pubnub.publish().channel(target_channel).message(encoded_msg).pn_async()
                        logger.debug(f"Relayed message to {uri} on {target_channel}")

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON from {client_address}: {e}")
        except Exception as e:
            logger.error(f"Error processing message from {client_address}: {e}")

async def ping_peers():
    while True:
        current_time = time.time()
        dead_peers = []
        for uri, node_id in list(PEERS.items()):
            if current_time - PEER_LAST_PING.get(uri, 0) > PING_TIMEOUT:
                dead_peers.append(uri)
                continue
            try:
                ping_message = {
                    "type": "PING",
                    "data": None
                }
                compressed_ping = gzip.compress(json.dumps(ping_message).encode('utf-8'))
                encoded_ping = base64.b64encode(compressed_ping).decode('utf-8')
                await pubnub.publish().channel(f"peer-{node_id}").message(encoded_ping).pn_async()
                PEER_LAST_PING[uri] = current_time
            except Exception:
                dead_peers.append(uri)
        for uri in dead_peers:
            if uri in PEERS:
                del PEERS[uri]
                PEER_LAST_PING.pop(uri, None)
                logger.info(f"Removed dead peer: {uri}")
        await asyncio.sleep(PING_INTERVAL)

async def health_check(request):
    """Handle health checks."""
    return web.Response(status=200, text="OK")

async def shutdown(app):
    await pubnub.stop()
    await app['runner'].cleanup()

def setup_signal_handlers(loop, app):
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown(app)))

async def main():
    # Subscribe to boot channel
    pubnub.add_listener(MySubscribeCallback())
    pubnub.subscribe().channels(BOOT_CHANNEL).execute()
    logger.info(f"Subscribed to PubNub channel: {BOOT_CHANNEL}")

    # Start HTTP health check server
    app = web.Application()
    app.add_routes([web.get('/health', health_check)])
    runner = web.AppRunner(app)
    await runner.setup()
    http_site = web.TCPSite(runner, '0.0.0.0', HTTP_PORT)
    await http_site.start()
    logger.info(f"HTTP health check running on http://0.0.0.0:{HTTP_PORT}/health")

    # Start ping task
    asyncio.create_task(ping_peers())

    # Keep the application running
    await asyncio.Event().wait()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        loop.close()
