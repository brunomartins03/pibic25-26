# Robot arm digital twin relay (Go + MQTT)

Bridges the ESP8266 (MQTT) and the Decentraland scene (WebSocket, unchanged
protocol from before). Run this alongside a Mosquitto broker on the same
on-premise Linux machine.

## 1. Install and configure Mosquitto

```
sudo apt-get install mosquitto mosquitto-clients
```

Recent Mosquitto versions default to localhost-only, no external
connections -- the ESP8266 needs LAN access, so add a listener. Create
`/etc/mosquitto/conf.d/robotarm.conf`:

```
listener 1883 0.0.0.0
allow_anonymous true
```

`allow_anonymous true` is fine for a closed home LAN; add username/password
auth (`password_file`) if this network isn't trusted.

Restart Mosquitto:

```
sudo systemctl restart mosquitto
```

Quick sanity check from another terminal:

```
mosquitto_sub -h localhost -t 'robotarm/#' -v
```

## 2. Run the relay

```
go mod tidy   # fetches paho.mqtt.golang and gorilla/websocket
go run main.go
```

You should see:

```
[relay] connected to MQTT broker
[relay] websocket listening on :8080
```

## 3. Point the ESP8266 and the Decentraland scene at this machine

- ESP8266 firmware: set `MQTT_BROKER` to this machine's LAN IP.
- Decentraland scene: `BRIDGE_URL` stays `ws://<this machine's LAN IP>:8080` --
  nothing changes on the scene side, since the relay still speaks the same
  WebSocket JSON protocol to viewers.

## Debugging

Watch state updates and inject test commands directly against MQTT, without
touching the relay or the scene:

```
mosquitto_sub -h localhost -t robotarm/state -v
mosquitto_pub -h localhost -t robotarm/cmd -m '{"target":"base","angle":45}'
```
