"""Slopio Party relay server.

A tiny WebSocket relay: a host opens a lobby and gets a short code, friends join with
the code, and the server passes messages between the people in a lobby. It knows
nothing about the game itself (the host's game runs the lobby; every game simulates
the minigame from the shared inputs).

Run locally:   python3 server/relay.py            (listens on ws://localhost:8765)
On a host:     the PORT environment variable picks the port (Render, Fly.io, ...).

Messages are JSON objects with an "op":
  client -> server
    {"op": "host", "name": str}                  open a lobby
    {"op": "join", "code": str, "name": str}     join one
    {"op": "send", "to": id or -1, "d": str}     pass "d" to one member (or everyone else)
    {"op": "lock", "on": bool}                   host only: refuse new joins (e.g. mid-game)
    {"op": "ping"}
  server -> client
    {"op": "hosted", "code", "id"}               you are the host (id 0)
    {"op": "joined", "code", "id", "peers": [{"id", "name"}]}
    {"op": "peer_joined", "id", "name"} / {"op": "peer_left", "id"}
    {"op": "msg", "from": id, "d": str}
    {"op": "closed", "msg"}                      the host left: the lobby is gone
    {"op": "error", "msg"}
    {"op": "pong"}
"""

import asyncio
import http
import json
import os
import random
import time

import websockets

CODE_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"   # no 0/O or 1/I mix-ups
CODE_LEN = 5
MAX_MEMBERS = 4            # one computer per player slot for now
MAX_MESSAGE = 256 * 1024
IDLE_LOBBY_SECONDS = 6 * 3600

lobbies = {}               # code -> Lobby
TEST_LAG = float(os.environ.get("RELAY_TEST_LAG_MS", "0")) / 1000.0   # dev: delay every message this much


class Lobby:
    def __init__(self, code, host_ws, host_name):
        self.code = code
        self.members = {0: (host_ws, host_name)}   # id -> (websocket, name)
        self.next_id = 1
        self.locked = False
        self.touched = time.time()

    async def send(self, mid, obj):
        m = self.members.get(mid)
        if m:
            try:
                await m[0].send(json.dumps(obj))
            except websockets.ConnectionClosed:
                pass

    async def broadcast(self, obj, skip=None):
        for mid in list(self.members):
            if mid != skip:
                await self.send(mid, obj)


async def deliver_later(lobby, me, to, out):
    await asyncio.sleep(TEST_LAG)
    if to == -1:
        await lobby.broadcast(out, skip=me)
    else:
        await lobby.send(to, out)


def new_code():
    while True:
        code = "".join(random.choice(CODE_CHARS) for _ in range(CODE_LEN))
        if code not in lobbies:
            return code


async def handler(ws):
    lobby = None
    me = None
    try:
        async for raw in ws:
            if isinstance(raw, bytes) or len(raw) > MAX_MESSAGE:
                continue
            try:
                msg = json.loads(raw)
            except ValueError:
                continue
            op = msg.get("op")
            if op == "ping":
                await ws.send('{"op": "pong"}')
            elif op == "host" and lobby is None:
                code = new_code()
                lobby = Lobby(code, ws, str(msg.get("name", "Host"))[:24])
                lobbies[code] = lobby
                me = 0
                await ws.send(json.dumps({"op": "hosted", "code": code, "id": 0}))
                print(f"lobby {code} opened ({len(lobbies)} open)")
            elif op == "join" and lobby is None:
                code = str(msg.get("code", "")).strip().upper()
                target = lobbies.get(code)
                if target is None:
                    await ws.send(json.dumps({"op": "error", "msg": f"No lobby with code {code}"}))
                elif target.locked:
                    await ws.send(json.dumps({"op": "error", "msg": "That lobby is in a game right now"}))
                elif len(target.members) >= MAX_MEMBERS:
                    await ws.send(json.dumps({"op": "error", "msg": "That lobby is full"}))
                else:
                    lobby = target
                    me = lobby.next_id
                    lobby.next_id += 1
                    name = str(msg.get("name", "Player"))[:24]
                    peers = [{"id": mid, "name": m[1]} for mid, m in lobby.members.items()]
                    lobby.members[me] = (ws, name)
                    lobby.touched = time.time()
                    await ws.send(json.dumps({"op": "joined", "code": code, "id": me, "peers": peers}))
                    await lobby.broadcast({"op": "peer_joined", "id": me, "name": name}, skip=me)
            elif op == "send" and lobby is not None:
                lobby.touched = time.time()
                out = {"op": "msg", "from": me, "d": msg.get("d", "")}
                to = msg.get("to", -1)
                if TEST_LAG > 0:   # dev: pretend the players are far apart
                    asyncio.create_task(deliver_later(lobby, me, int(to), out))
                elif to == -1:
                    await lobby.broadcast(out, skip=me)
                else:
                    await lobby.send(int(to), out)
            elif op == "lock" and lobby is not None and me == 0:
                lobby.locked = bool(msg.get("on", True))
    except websockets.ConnectionClosed:
        pass
    finally:
        if lobby is not None and me in lobby.members:
            del lobby.members[me]
            if me == 0:   # the host left: close the lobby for everyone
                await lobby.broadcast({"op": "closed", "msg": "The host left the lobby"})
                for m in list(lobby.members.values()):
                    await m[0].close()
                lobbies.pop(lobby.code, None)
                print(f"lobby {lobby.code} closed ({len(lobbies)} open)")
            else:
                await lobby.broadcast({"op": "peer_left", "id": me})


async def sweep():
    while True:
        await asyncio.sleep(600)
        now = time.time()
        for code, lb in list(lobbies.items()):
            if now - lb.touched > IDLE_LOBBY_SECONDS:
                lobbies.pop(code, None)


def health(connection, request):
    # plain HTTP (health checks, waking a sleeping free instance) gets a short answer
    if request.headers.get("Upgrade", "").lower() != "websocket":
        return connection.respond(http.HTTPStatus.OK, f"slopio relay ok, {len(lobbies)} lobbies\n")
    return None


async def main():
    port = int(os.environ.get("PORT", "8765"))
    async with websockets.serve(handler, "0.0.0.0", port, process_request=health,
                                max_size=MAX_MESSAGE, ping_interval=20, ping_timeout=20):
        print(f"slopio relay listening on port {port}")
        asyncio.create_task(sweep())
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
