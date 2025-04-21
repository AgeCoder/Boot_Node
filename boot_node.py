import asyncio
import websockets
import json
import logging

logging.basicConfig(level=logging.INFO)
REGISTERED_NODES = set()

async def boot_handler(websocket):
    async for message in websocket:
        try:
            msg = json.loads(message)
            if msg["type"] == "REGISTER_PEER":
                uri = msg["data"]
                REGISTERED_NODES.add(uri)
                logging.info(f"Registered peer: {uri}")
                # Exclude the registering node's URI from the peer list
                peer_list = list(REGISTERED_NODES - {uri})
                await websocket.send(json.dumps({
                    "type": "PEER_LIST",
                    "data": peer_list
                }))
        except Exception as e:
            logging.error(f"Error handling registration: {e}")

async def main():
    server = await websockets.serve(boot_handler, "0.0.0.0", 9000)
    logging.info("Boot node running on port 9000")
    await server.wait_closed()

if __name__ == "__main__":
    asyncio.run(main())