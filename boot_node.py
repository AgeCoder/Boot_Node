import asyncio
import json
import logging
import gzip
import base64
import time
from aiohttp import web
from pubnub.pnconfiguration import PNConfiguration
from pubnub.pubnub_asyncio import PubNubAsyncio
from pubnub.callbacks import SubscribeCallback

class SuppressBadRequestFilter(logging.Filter):
    def filter(self, record):
        return not ("connection rejected (400 Bad Request)" in record.getMessage())

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
websockets_logger = logging.getLogger('pubnub')
websockets_logger.addFilter(SuppressBadRequestFilter())

PEERS = {}  # uri -> node_id
PEER_LAST_PING = {}
PORT = 10000
HTTP_PORT = 8080
PING_INTERVAL = 60
PING_TIMEOUT = 120
BOOT_CHANNEL = "boot-channel"

# Configure PubNub (add your keys later)
pnconfig = PNConfiguration()
pnconfig.subscribe_key = "your-subscribe-key"  # Add your PubNub Subscribe Key
pnconfig.publish_key = "your-publish-key"      # Add your PubNub Publish Key
pnconfig.user_id = "boot-node"
pubnub = PubNubAsyncio(pnconfig)

class MySubscribeCallback(SubscribeCallback):
    async def message(self, pubnub, event):
        try:
            msg = json.loads(gzip.decompress(base64.b64decode(event.message)).decode('utf-8'))
            msg_type = msg.get('type')
            msg_data = msg.get('data')
            client_address = event.publisher

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
                await pubnub.publish().channel(BOOT_CHANNEL).message(
                    base64.b64encode(gzip.compress(json.dumps(response).encode('utf-8'))).decode('utf-8')
                ).pn_async()

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
                    await pubnub.publish().channel(target_channel).message(
                        base64.b64encode(relayed_data).decode('utf-8')
                    ).pn_async()
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
                        await pubnub.publish().channel(target_channel).message(
                            base64.b64encode(compressed_msg).decode('utf-8')
                        ).pn_async()
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
                await pubnub.publish().channel(f"peer-{node_id}").message(
                    base64.b64encode(gzip.compress(json.dumps({"type": "PING", "data": None}).encode('utf-8'))).decode('utf-8')
                ).pn_async()
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
    """Handle Render health checks."""
    return web.Response(status=200, text="OK")

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
    asyncio.run(main())
