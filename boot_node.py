import asyncio
import websockets
import json
import logging
import gzip

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PEERS = {}  # Store peer URI -> websocket
PORT = 10000

async def boot_handler(websocket):
    client_address = f"{websocket.remote_address[0]}:{websocket.remote_address[1]}"
    peer_uri = None
    try:
        async for message in websocket:
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
                    logger.info(f"Registered peer: {peer_uri} from {client_address}")
                    response = json.dumps({"type": "PEER_LIST", "data": list(PEERS.keys() - {peer_uri})})
                    await websocket.send(gzip.compress(response.encode('utf-8')))

                elif msg_type == "RELAY_MESSAGE":
                    target_uri = msg.get('target_uri')
                    if target_uri in PEERS:
                        logger.debug(f"Relaying message from {peer_uri} to {target_uri}")
                        await PEERS[target_uri].send(message)
                    else:
                        logger.warning(f"Target peer {target_uri} not found")

            except Exception as e:
                logger.error(f"Error processing message from {client_address}: {e}")
                continue
    except websockets.exceptions.ConnectionClosed:
        logger.info(f"Connection closed for {client_address}")
    finally:
        if peer_uri and peer_uri in PEERS:
            del PEERS[peer_uri]
            logger.info(f"Removed peer: {peer_uri}")

async def main():
    server = await websockets.serve(boot_handler, "0.0.0.0", PORT, max_size=1024*1024)
    logger.info(f"Boot node running on ws://0.0.0.0:{PORT}")
    await server.wait_closed()

if __name__ == "__main__":
    asyncio.run(main())
