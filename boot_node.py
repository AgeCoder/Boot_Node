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
                            del PEERS[uri]
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
            del PEERS[uri]
            logger.info(f"Removed peer: {peer_uri}")

async def main():
    server = await websockets.serve(boot_handler, "0.0.0.0", PORT, max_size=1024*1024)
    logger.info(f"Boot node running on ws://0.0.0.0:{PORT}")
    await server.wait_closed()

if __name__ == "__main__":
    asyncio.run(main())
