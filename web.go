package main

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

//go:embed static/index.html
var staticFS embed.FS

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// WebSession manages a single browser <-> Volcano Engine proxy session.
type WebSession struct {
	browserConn *websocket.Conn
	apiConn     *websocket.Conn
	proto       *BinaryProtocol
	writeLock   sync.Mutex
	sequence    atomic.Int64
	sessionID   string
	dialogID    string
	cancel      context.CancelFunc
	audioData   []byte
	audioLock   sync.Mutex
	ready       chan struct{}
}

func newWebSession(browserConn *websocket.Conn) *WebSession {
	return &WebSession{
		browserConn: browserConn,
		proto:       protocol.Clone(),
		ready:       make(chan struct{}),
	}
}

// --- Send helpers ---

func (s *WebSession) sendToAPI(frame []byte) error {
	s.writeLock.Lock()
	defer s.writeLock.Unlock()
	if err := s.apiConn.WriteMessage(websocket.BinaryMessage, frame); err != nil {
		return fmt.Errorf("send to API: %w", err)
	}
	s.sequence.Add(1)
	return nil
}

func (s *WebSession) sendAudioToAPI(pcmData []byte) error {
	s.proto.SetSerialization(SerializationRaw)
	msg, err := NewMessage(MsgTypeAudioOnlyClient, MsgTypeFlagWithEvent)
	if err != nil {
		return err
	}
	msg.Event = 200
	msg.SessionID = s.sessionID
	msg.Payload = pcmData
	frame, err := s.proto.Marshal(msg)
	if err != nil {
		return err
	}
	return s.sendToAPI(frame)
}

func (s *WebSession) sendFullToAPI(event int32, sessionID string, payload []byte) error {
	s.proto.SetSerialization(SerializationJSON)
	msg, err := NewMessage(MsgTypeFullClient, MsgTypeFlagWithEvent)
	if err != nil {
		return err
	}
	msg.Event = event
	if sessionID != "" {
		msg.SessionID = sessionID
	}
	msg.Payload = payload
	frame, err := s.proto.Marshal(msg)
	if err != nil {
		return err
	}
	return s.sendToAPI(frame)
}

func (s *WebSession) sendStatus(status, message string) {
	data, _ := json.Marshal(map[string]string{
		"type":    "status",
		"status":  status,
		"message": message,
	})
	s.browserConn.WriteMessage(websocket.TextMessage, data)
}

func (s *WebSession) sendAudioToBrowser(audioData []byte) {
	msg := make([]byte, 1+len(audioData))
	msg[0] = 0x01
	copy(msg[1:], audioData)
	s.browserConn.WriteMessage(websocket.BinaryMessage, msg)
}

func (s *WebSession) sendEventToBrowser(payload []byte) {
	msg := make([]byte, 1+len(payload))
	msg[0] = 0x02
	copy(msg[1:], payload)
	s.browserConn.WriteMessage(websocket.BinaryMessage, msg)
}

// --- API lifecycle (adapted from client_request.go) ---

func (s *WebSession) dialAPI(ctx context.Context) error {
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL.String(), http.Header{
		"X-Api-Resource-Id": []string{"volc.speech.dialog"},
		"X-Api-Access-Key":  []string{accessToken},
		"X-Api-App-Key":     []string{"PlgvMymc7f3tQnJ6"},
		"X-Api-App-ID":      []string{appid},
		"X-Api-Connect-Id":  []string{uuid.New().String()},
	})
	if err != nil {
		return fmt.Errorf("dial API: %w", err)
	}
	s.apiConn = conn
	return nil
}

func (s *WebSession) apiStartConnection() error {
	if err := s.sendFullToAPI(1, "", []byte("{}")); err != nil {
		return fmt.Errorf("start connection: %w", err)
	}
	mt, frame, err := s.apiConn.ReadMessage()
	if err != nil {
		return fmt.Errorf("read ConnectionStarted: %w", err)
	}
	if mt != websocket.BinaryMessage && mt != websocket.TextMessage {
		return fmt.Errorf("unexpected message type: %d", mt)
	}
	msg, _, err := Unmarshal(frame, ContainsSequence)
	if err != nil {
		return fmt.Errorf("unmarshal ConnectionStarted: %w", err)
	}
	if msg.Event != 50 {
		return fmt.Errorf("unexpected event %d for StartConnection", msg.Event)
	}
	glog.Infof("API connection started: %s", msg.ConnectID)
	return nil
}

func (s *WebSession) apiStartSession() error {
	recvTimeout := int64(10)
	payload, _ := json.Marshal(&StartSessionPayload{
		ASR: ASRPayload{
			Extra: map[string]interface{}{
				"end_smooth_window_ms": 1500,
			},
		},
		TTS: TTSPayload{
			Speaker: speaker,
			AudioConfig: AudioConfig{
				Channel:    1,
				Format:     PcmS16LE, // S16LE for web mode
				SampleRate: 24000,
			},
		},
		Dialog: DialogPayload{
			BotName:    "张姐",
			SystemRole: systemRolePrompt,
			SpeakingStyle: "你的说话风格简洁明了，语速适中，语调自然。热情外向，像真实客户那样说话，用词生活化。",
			Location: &LocationInfo{
				City: "北京",
			},
			Extra: map[string]interface{}{
				"strict_audit":   false,
				"audit_response": "抱歉这个问题我无法回答，你可以换个其他话题，我会尽力为你提供帮助。",
				"recv_timeout":   &recvTimeout,
				"input_mod":      "audio",
			},
		},
	})

	if err := s.sendFullToAPI(100, s.sessionID, payload); err != nil {
		return fmt.Errorf("start session: %w", err)
	}

	mt, frame, err := s.apiConn.ReadMessage()
	if err != nil {
		return fmt.Errorf("read SessionStarted: %w", err)
	}
	if mt != websocket.BinaryMessage && mt != websocket.TextMessage {
		return fmt.Errorf("unexpected message type: %d", mt)
	}
	msg, _, err := Unmarshal(frame, ContainsSequence)
	if err != nil {
		return fmt.Errorf("unmarshal SessionStarted: %w", err)
	}
	if msg.Event != 150 {
		return fmt.Errorf("unexpected event %d for StartSession", msg.Event)
	}
	var data map[string]interface{}
	if err := json.Unmarshal(msg.Payload, &data); err != nil {
		return fmt.Errorf("unmarshal session payload: %w", err)
	}
	s.dialogID = data["dialog_id"].(string)
	glog.Infof("API session started: %s", s.dialogID)
	return nil
}

func (s *WebSession) apiSayHello(content string) error {
	payload, _ := json.Marshal(&SayHelloPayload{Content: content})
	return s.sendFullToAPI(300, s.sessionID, payload)
}

func (s *WebSession) apiFinishSession() {
	if s.apiConn == nil {
		return
	}
	_ = s.sendFullToAPI(102, s.sessionID, []byte("{}"))
}

func (s *WebSession) apiFinishConnection() {
	if s.apiConn == nil {
		return
	}
	_ = s.sendFullToAPI(2, "", []byte("{}"))
	// Best effort read of ConnectionFinished
	s.apiConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	mt, frame, err := s.apiConn.ReadMessage()
	if err == nil && (mt == websocket.BinaryMessage || mt == websocket.TextMessage) {
		msg, _, uerr := Unmarshal(frame, ContainsSequence)
		if uerr == nil {
			glog.Infof("API connection finished (event=%d)", msg.Event)
		}
	}
}

// --- Text forwarding ---

func (s *WebSession) forwardTextEvent(event int32, payload []byte) {
	var data map[string]interface{}
	if json.Unmarshal(payload, &data) != nil {
		return
	}

	// Try to extract text from various possible API field names
	text := extractString(data, "text", "content", "result", "msg",
		"asr_text", "tts_text", "value")
	if text == "" {
		// Try nested: data.text, result.text, etc.
		for _, key := range []string{"data", "result", "payload"} {
			if nested, ok := data[key].(map[string]interface{}); ok {
				text = extractString(nested, "text", "content", "msg", "value")
				if text != "" {
					break
				}
			}
		}
	}

	role := "ai"
	switch event {
	case 450, 459:
		role = "user"
	case 350:
		role = "ai"
	}

	if text != "" {
		msg, _ := json.Marshal(map[string]string{
			"type": "chat",
			"role": role,
			"text": text,
		})
		s.browserConn.WriteMessage(websocket.TextMessage, msg)
	} else {
		// Fallback: send raw payload so we can see the actual API structure
		glog.Infof("No text extracted from event %d, payload: %s", event, string(payload))
		msg, _ := json.Marshal(map[string]interface{}{
			"type":  "debug",
			"event": event,
			"data":  data,
		})
		s.browserConn.WriteMessage(websocket.TextMessage, msg)
	}
}

func extractString(m map[string]interface{}, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k].(string); ok && v != "" {
			return v
		}
	}
	return ""
}

// --- Read loop from API ---

func (s *WebSession) readAPILoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		mt, frame, err := s.apiConn.ReadMessage()
		if err != nil {
			glog.Errorf("Read from API error: %v", err)
			s.cancel()
			return
		}
		if mt != websocket.BinaryMessage && mt != websocket.TextMessage {
			continue
		}

		msg, _, err := Unmarshal(frame, ContainsSequence)
		if err != nil {
			glog.Infof("Unmarshal error: %v, data: %s", err, frame[:min(len(frame), 200)])
			continue
		}

		switch msg.Type {
		case MsgTypeFullServer:
			glog.Infof("API event=%d session=%s: %s", msg.Event, msg.SessionID, msg.Payload)

			// Forward text events to browser for display
			s.forwardTextEvent(msg.Event, msg.Payload)

			if msg.Event == 150 {
				var data map[string]interface{}
				if json.Unmarshal(msg.Payload, &data) == nil {
					if did, ok := data["dialog_id"].(string); ok {
						s.dialogID = did
					}
				}
			}

			if msg.Event == 152 || msg.Event == 153 {
				glog.Info("API session finished")
				s.cancel()
				return
			}

			if msg.Event == 359 {
				select {
				case <-s.ready:
				default:
					close(s.ready)
				}
				s.sendStatus("calling", "Session ready")
			}

		case MsgTypeAudioOnlyServer:
			// Save audio for recording
			s.audioLock.Lock()
			s.audioData = append(s.audioData, msg.Payload...)
			s.audioLock.Unlock()
			// Forward to browser
			s.sendAudioToBrowser(msg.Payload)

		case MsgTypeError:
			s.sendStatus("error", fmt.Sprintf("API error code=%d: %s", msg.ErrorCode, msg.Payload))
			s.cancel()
			return
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// --- Call management ---

func (s *WebSession) startCall() {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.sessionID = uuid.New().String()
	s.audioData = nil
	s.ready = make(chan struct{})

	if err := s.dialAPI(ctx); err != nil {
		s.sendStatus("error", "API connection failed: "+err.Error())
		cancel()
		return
	}

	if err := s.apiStartConnection(); err != nil {
		s.sendStatus("error", err.Error())
		s.apiConn.Close()
		s.apiConn = nil
		cancel()
		return
	}

	if err := s.apiStartSession(); err != nil {
		s.sendStatus("error", err.Error())
		s.apiConn.Close()
		s.apiConn = nil
		cancel()
		return
	}

	s.sendStatus("calling", "Connecting...")

	if err := s.apiSayHello("你好呀，我最近皮肤状态不太好，想了解一下你们这边有什么项目能改善一下吗？"); err != nil {
		glog.Errorf("SayHello error: %v", err)
	}

	// Send audio info to browser
	audioInfo, _ := json.Marshal(map[string]interface{}{
		"type":        "audio_info",
		"sample_rate": 24000,
		"format":      "s16le",
		"channels":    1,
	})
	s.browserConn.WriteMessage(websocket.TextMessage, audioInfo)

	// Start reading from API
	go s.readAPILoop(ctx)

	// Wait for completion
	<-ctx.Done()
	glog.Info("Call context done")
	s.finishCall()
}

func (s *WebSession) finishCall() {
	glog.Info("Finishing call...")
	s.apiFinishSession()
	s.apiFinishConnection()
	s.saveRecording()
	s.sendStatus("connected", "Call ended")
}

func (s *WebSession) saveRecording() {
	s.audioLock.Lock()
	defer s.audioLock.Unlock()
	if len(s.audioData) == 0 {
		return
	}
	filename := fmt.Sprintf("web_output_%d.pcm", time.Now().UnixMilli())
	if err := os.WriteFile(filename, s.audioData, 0644); err != nil {
		glog.Errorf("Save recording: %v", err)
	} else {
		glog.Infof("Saved recording: %s (%d bytes)", filename, len(s.audioData))
	}
}

func (s *WebSession) cleanup() {
	if s.cancel != nil {
		s.cancel()
	}
	if s.apiConn != nil {
		s.apiConn.Close()
	}
	s.browserConn.Close()
	glog.Info("Web session cleaned up")
}

// --- HTTP handler ---

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	browserConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		glog.Errorf("WebSocket upgrade error: %v", err)
		return
	}

	session := newWebSession(browserConn)
	defer session.cleanup()

	for {
		msgType, data, err := browserConn.ReadMessage()
		if err != nil {
			glog.Infof("Browser disconnected: %v", err)
			return
		}

		switch msgType {
		case websocket.TextMessage:
			var cmd map[string]string
			if json.Unmarshal(data, &cmd) != nil {
				continue
			}
			switch cmd["type"] {
			case "start":
				go session.startCall()
			case "stop":
				if session.cancel != nil {
					session.cancel()
				}
			}

		case websocket.BinaryMessage:
			if session.apiConn != nil && session.sessionID != "" {
				if err := session.sendAudioToAPI(data); err != nil {
					glog.Errorf("Forward audio to API: %v", err)
				}
			}
		}
	}
}

// --- Server entry ---

func StartWebServer(addr string) {
	staticContent, err := fs.Sub(staticFS, "static")
	if err != nil {
		glog.Exitf("Failed to create static FS: %v", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.FS(staticContent)))
	mux.HandleFunc("/ws", handleWebSocket)

	glog.Infof("Starting web server on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		glog.Exitf("Web server error: %v", err)
	}
}
