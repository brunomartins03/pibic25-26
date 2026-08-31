/*
Digital Twin Relay — Go + MQTT <-> WebSocket bridge
=====================================================

Bridges two worlds:
  - MQTT side: talks to the ESP8266 via a Mosquitto broker running on
    this same on-premise machine. Subscribes to "robotarm/state"
    (retained -- always has the last known pose), publishes to
    "robotarm/cmd".
  - WebSocket side: unchanged from the previous relay. Decentraland
    viewers connect here, send {"type":"cmd",...}, receive
    {"type":"state","angles":{...}}. No scene changes needed.

Policy: last command wins. Any cmd from any viewer is forwarded to
the arm as-is, no locking or queueing.

Run:

	go mod tidy
	go run main.go

Requires a Mosquitto broker reachable at mqttBroker below, configured
to accept LAN connections (see accompanying README).
*/
package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/gorilla/websocket"
)

const (
	mqttBroker   = "tcp://localhost:1883" // Mosquitto running on this same machine
	mqttClientID = "robotarm-relay"
	topicState   = "robotarm/state"
	topicCmd     = "robotarm/cmd"
	wsAddr       = ":8080"
)

// Angles mirrors the ESP8266's state payload.
type Angles struct {
	Base     int `json:"base"`
	Shoulder int `json:"shoulder"`
	Elbow    int `json:"elbow"`
	Gripper  int `json:"gripper"`
}

// ViewerMessage covers every shape exchanged with Decentraland viewers:
// {"type":"hello","role":"viewer"}
// {"type":"state","angles":{...}}                (relay -> viewer)
// {"type":"cmd","target":"base","angle":120}      (viewer -> relay)
// {"type":"cmd","action":"home"}                  (viewer -> relay)
type ViewerMessage struct {
	Type   string  `json:"type"`
	Role   string  `json:"role,omitempty"`
	Angles *Angles `json:"angles,omitempty"`
	Target string  `json:"target,omitempty"`
	Angle  *int    `json:"angle,omitempty"`
	Action string  `json:"action,omitempty"`
}

var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true }, // LAN-only relay; tighten if exposed further
	}

	viewersMu sync.Mutex
	viewers   = make(map[*websocket.Conn]bool)

	stateMu   sync.Mutex
	lastState = Angles{Base: 90, Shoulder: 90, Elbow: 90, Gripper: 90}

	mqttClient mqtt.Client
)

func main() {
	opts := mqtt.NewClientOptions().
		AddBroker(mqttBroker).
		SetClientID(mqttClientID).
		SetAutoReconnect(true).
		SetOnConnectHandler(func(c mqtt.Client) {
			log.Println("[relay] connected to MQTT broker")
			if token := c.Subscribe(topicState, 0, onArmState); token.Wait() && token.Error() != nil {
				log.Println("[relay] subscribe failed:", token.Error())
			}
		}).
		SetConnectionLostHandler(func(c mqtt.Client, err error) {
			log.Println("[relay] MQTT connection lost:", err)
		})

	mqttClient = mqtt.NewClient(opts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("[relay] initial MQTT connect failed: %v", token.Error())
	}

	http.HandleFunc("/", handleWS)
	log.Printf("[relay] websocket listening on %s", wsAddr)
	log.Fatal(http.ListenAndServe(wsAddr, nil))
}

// onArmState fires whenever the ESP8266 publishes a new confirmed pose.
// Because "robotarm/state" is retained, this also fires once immediately
// after we subscribe, so lastState is populated even before any viewer
// connects.
func onArmState(_ mqtt.Client, msg mqtt.Message) {
	var a Angles
	if err := json.Unmarshal(msg.Payload(), &a); err != nil {
		log.Println("[relay] bad state payload:", err)
		return
	}

	stateMu.Lock()
	lastState = a
	stateMu.Unlock()

	broadcastState(a)
}

func broadcastState(a Angles) {
	payload, err := json.Marshal(ViewerMessage{Type: "state", Angles: &a})
	if err != nil {
		return
	}

	viewersMu.Lock()
	defer viewersMu.Unlock()
	for conn := range viewers {
		if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
			conn.Close()
			delete(viewers, conn)
		}
	}
}

func handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("[relay] upgrade error:", err)
		return
	}
	defer func() {
		viewersMu.Lock()
		delete(viewers, conn)
		viewersMu.Unlock()
		conn.Close()
		log.Println("[relay] viewer disconnected")
	}()

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return
		}

		var msg ViewerMessage
		if err := json.Unmarshal(raw, &msg); err != nil {
			continue // ignore malformed messages
		}

		switch msg.Type {
		case "hello":
			if msg.Role == "viewer" {
				registerViewer(conn)
			}
		case "cmd":
			forwardCmd(msg)
		}
	}
}

func registerViewer(conn *websocket.Conn) {
	viewersMu.Lock()
	viewers[conn] = true
	count := len(viewers)
	viewersMu.Unlock()
	log.Printf("[relay] viewer connected (%d total)", count)

	stateMu.Lock()
	s := lastState
	stateMu.Unlock()

	payload, err := json.Marshal(ViewerMessage{Type: "state", Angles: &s})
	if err != nil {
		return
	}
	conn.WriteMessage(websocket.TextMessage, payload)
}

// forwardCmd relays a viewer's cmd straight to the arm over MQTT,
// unmodified, whoever sent it. Last write wins.
func forwardCmd(msg ViewerMessage) {
	cmdPayload := map[string]interface{}{}
	switch {
	case msg.Action != "":
		cmdPayload["action"] = msg.Action
	case msg.Target != "" && msg.Angle != nil:
		cmdPayload["target"] = msg.Target
		cmdPayload["angle"] = *msg.Angle
	default:
		return // malformed cmd, ignore
	}

	payload, err := json.Marshal(cmdPayload)
	if err != nil {
		return
	}
	mqttClient.Publish(topicCmd, 0, false, payload)
}
