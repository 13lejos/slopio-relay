# Slopio Party relay

The tiny WebSocket relay server for Slopio Party's online lobbies: a host opens a lobby and gets a
5-letter code, friends join with the code, and the server passes messages between the computers in
the lobby. It knows nothing about the game itself.

- Run locally: `pip install -r requirements.txt` then `python relay.py` (ws://localhost:8765)
- On Render: the `render.yaml` blueprint sets it up as a free web service; the game connects to
  `wss://<service-name>.onrender.com`.
