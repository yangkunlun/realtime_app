package main

import (
	"context"
	"embed"
	"strings"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"os/signal"
	"syscall"
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

// audioSegment represents one turn of audio (user or AI).
type audioSegment struct {
	role string // "user" or "ai"
	data []byte // raw S16LE PCM
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
	ready       chan struct{}
	aiTextBuf   strings.Builder
	// Audio recording: interleaved segments in conversation order
	segMu              sync.Mutex
	segments           []audioSegment
	curUserBuf         []byte // accumulating user audio for current turn
	curAIBuf           []byte // accumulating AI audio for current TTS turn
	userRecStarted     atomic.Bool // only record user audio after ASR detects speech
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
	s.apiConn.Close()
	s.apiConn = nil
}

// --- Text forwarding ---

func (s *WebSession) forwardTextEvent(event int32, payload []byte) {
	var data map[string]interface{}
	if json.Unmarshal(payload, &data) != nil {
		return
	}

	// Event 451: ASR streaming - only show final result
	if event == 451 {
		isInterim := true
		var asrText string
		if results, ok := data["results"].([]interface{}); ok && len(results) > 0 {
			if r, ok := results[0].(map[string]interface{}); ok {
				asrText, _ = r["text"].(string)
				// Check is_interim inside results[0] first, then top level
				if v, ok := r["is_interim"].(bool); ok {
					isInterim = v
				}
			}
		}
		if v, ok := data["is_interim"].(bool); ok {
			isInterim = v
		}
		// Start recording user audio from the first ASR detection
		s.userRecStarted.Store(true)
		glog.Infof("ASR 451: is_interim=%v text=%q", isInterim, asrText)
		if !isInterim && asrText != "" {
			// If AI text is being accumulated (TTS in progress), flush it first
			if s.aiTextBuf.Len() > 0 {
				aiText := s.aiTextBuf.String()
				s.aiTextBuf.Reset()
				s.sendChat("ai", aiText)
			}
			s.sendChat("user", asrText)
			// Save user audio segment (ASR final = end of user turn)
			s.segMu.Lock()
			if len(s.curUserBuf) > 0 {
				buf := make([]byte, len(s.curUserBuf))
				copy(buf, s.curUserBuf)
				s.segments = append(s.segments, audioSegment{role: "user", data: buf})
				s.curUserBuf = nil
			}
			s.segMu.Unlock()
		}
		return
	}

	// Event 350: TTS start - save any pending AI audio as a segment, start new one
	if event == 350 {
		s.aiTextBuf.Reset()
		s.segMu.Lock()
		if len(s.curAIBuf) > 0 {
			buf := make([]byte, len(s.curAIBuf))
			copy(buf, s.curAIBuf)
			s.segments = append(s.segments, audioSegment{role: "ai", data: buf})
			s.curAIBuf = nil
		}
		s.segMu.Unlock()
		return // don't accumulate text from this event
	}

	// All other events: extract text and accumulate for AI response
	text := ""
	if t, ok := data["text"].(string); ok {
		text = t
	} else if t, ok := data["content"].(string); ok {
		text = t
	}
	if text != "" {
		s.aiTextBuf.WriteString(text)
	}

	// Flush accumulated text on TTS end or session end
	if event == 351 || event == 459 {
		text := s.aiTextBuf.String()
		s.aiTextBuf.Reset()
		if text != "" {
			s.sendChat("ai", text)
		}
		// Save AI audio segment
		s.segMu.Lock()
		if len(s.curAIBuf) > 0 {
			buf := make([]byte, len(s.curAIBuf))
			copy(buf, s.curAIBuf)
			s.segments = append(s.segments, audioSegment{role: "ai", data: buf})
			s.curAIBuf = nil
		}
		s.segMu.Unlock()
		return // don't accumulate text from this event
	}
}

func (s *WebSession) sendChat(role, text string) {
	msg, _ := json.Marshal(map[string]string{
		"type": "chat",
		"role": role,
		"text": text,
	})
	s.browserConn.WriteMessage(websocket.TextMessage, msg)
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
			// Accumulate AI audio for current TTS turn
			s.segMu.Lock()
			s.curAIBuf = append(s.curAIBuf, msg.Payload...)
			s.segMu.Unlock()
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
	s.segments = nil
	s.curUserBuf = nil
	s.curAIBuf = nil
	s.userRecStarted.Store(false)
	s.aiTextBuf.Reset()
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
	s.segMu.Lock()
	// Flush any remaining audio buffers as final segments
	if len(s.curUserBuf) > 0 {
		s.segments = append(s.segments, audioSegment{role: "user", data: s.curUserBuf})
		s.curUserBuf = nil
	}
	if len(s.curAIBuf) > 0 {
		s.segments = append(s.segments, audioSegment{role: "ai", data: s.curAIBuf})
		s.curAIBuf = nil
	}
	segments := s.segments
	s.segments = nil
	s.segMu.Unlock()

	if len(segments) == 0 {
		glog.Info("No audio segments to save")
		return
	}

	// Concatenate segments in conversation order, trimming silence, resampling user to 24kHz
	var combined []byte
	for _, seg := range segments {
		var pcm []byte
		if seg.role == "user" {
			trimmed := trimSilence(seg.data, 16000)
			pcm = resampleS16LE(trimmed, 16000, 24000)
		} else {
			pcm = trimSilence(seg.data, 24000)
		}
		glog.Infof("Audio segment: role=%s, orig=%d bytes, trimmed=%d bytes",
			seg.role, len(seg.data), len(pcm))
		combined = append(combined, pcm...)
	}

	// Create uploads dir and write WAV
	os.MkdirAll("uploads", 0755)
	filename := fmt.Sprintf("uploads/recording_%d.wav", time.Now().UnixMilli())
	wavData := encodeWAV(combined, 24000, 1, 16)
	if err := os.WriteFile(filename, wavData, 0644); err != nil {
		glog.Errorf("Save WAV: %v", err)
	} else {
		glog.Infof("Saved WAV: %s (%d segments, %d bytes)", filename, len(segments), len(combined))
		// Notify browser with recording file path
		recMsg, _ := json.Marshal(map[string]string{
			"type":     "recording",
			"filename": filename,
		})
		s.browserConn.WriteMessage(websocket.TextMessage, recMsg)
	}
}

// trimSilence removes leading and trailing silence from S16LE PCM audio.
// Uses sliding window energy detection to handle mic noise.
// Keeps 150ms padding on each side.
func trimSilence(pcm []byte, sampleRate int) []byte {
	if len(pcm) < 4 {
		return pcm
	}
	samples := len(pcm) / 2
	windowSize := sampleRate / 10 // 100ms window
	if windowSize < 1 {
		windowSize = 1
	}
	energyThreshold := int64(800) // energy threshold per window
	padding := sampleRate * 15 / 100 // 150ms padding

	// Build energy array using sliding window
	energies := make([]int64, samples)
	var windowSum int64
	for i := 0; i < samples; i++ {
		s := int32(int16(pcm[i*2]) | int16(pcm[i*2+1])<<8)
		windowSum += int64(s * s)
		if i >= windowSize {
			s0 := int32(int16(pcm[(i-windowSize)*2]) | int16(pcm[(i-windowSize)*2+1])<<8)
			windowSum -= int64(s0 * s0)
		}
		energies[i] = windowSum / int64(windowSize)
	}

	start := 0
	for i := 0; i < samples; i++ {
		if energies[i] > energyThreshold {
			start = i - padding
			if start < 0 {
				start = 0
			}
			break
		}
	}

	end := samples - 1
	for i := samples - 1; i >= 0; i-- {
		if energies[i] > energyThreshold {
			end = i + padding
			if end > samples {
				end = samples
			}
			break
		}
	}

	if start >= end {
		return pcm
	}
	return pcm[start*2 : end*2]
}

// resampleS16LE resamples S16LE PCM audio from srcRate to dstRate using linear interpolation.
func resampleS16LE(pcm []byte, srcRate, dstRate int) []byte {
	if len(pcm) == 0 || srcRate == dstRate {
		return pcm
	}
	srcSamples := len(pcm) / 2 // 2 bytes per S16LE sample
	dstSamples := int(float64(srcSamples) * float64(dstRate) / float64(srcRate))
	out := make([]int16, dstSamples)
	ratio := float64(srcSamples-1) / float64(dstSamples-1)
	for i := 0; i < dstSamples; i++ {
		pos := float64(i) * ratio
		idx := int(pos)
		frac := pos - float64(idx)
		if idx+1 < srcSamples {
			s0 := int16(pcm[idx*2]) | int16(pcm[idx*2+1])<<8
			s1 := int16(pcm[(idx+1)*2]) | int16(pcm[(idx+1)*2+1])<<8
			out[i] = int16(float64(s0)*(1-frac) + float64(s1)*frac)
		} else {
			out[i] = int16(pcm[idx*2]) | int16(pcm[idx*2+1])<<8
		}
	}
	result := make([]byte, len(out)*2)
	for i, s := range out {
		result[i*2] = byte(s)
		result[i*2+1] = byte(s >> 8)
	}
	return result
}

// encodeWAV creates a WAV file header + PCM data.
func encodeWAV(pcm []byte, sampleRate, channels, bitsPerSample int) []byte {
	dataSize := len(pcm)
	fileSize := 36 + dataSize
	buf := make([]byte, 44+dataSize)
	// RIFF header
	copy(buf[0:4], "RIFF")
	writeLE32(buf[4:8], uint32(fileSize))
	copy(buf[8:12], "WAVE")
	// fmt chunk
	copy(buf[12:16], "fmt ")
	writeLE32(buf[16:20], 16)            // chunk size
	writeLE16(buf[20:22], 1)             // PCM format
	writeLE16(buf[22:24], uint16(channels))
	writeLE32(buf[24:28], uint32(sampleRate))
	writeLE32(buf[28:32], uint32(sampleRate*channels*bitsPerSample/8)) // byte rate
	writeLE16(buf[32:34], uint16(channels*bitsPerSample/8))             // block align
	writeLE16(buf[34:36], uint16(bitsPerSample))
	// data chunk
	copy(buf[36:40], "data")
	writeLE32(buf[40:44], uint32(dataSize))
	copy(buf[44:], pcm)
	return buf
}

func writeLE32(b []byte, v uint32) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
}

func writeLE16(b []byte, v uint16) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
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
			// Only accumulate user audio after ASR detects speech
			if session.userRecStarted.Load() {
				session.segMu.Lock()
				session.curUserBuf = append(session.curUserBuf, data...)
				session.segMu.Unlock()
			}
			// Forward to API
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
	mux.Handle("/uploads/", http.StripPrefix("/uploads/", http.FileServer(http.Dir("uploads"))))
	mux.HandleFunc("/ws", handleWebSocket)

	srv := &http.Server{Addr: addr, Handler: mux}

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		<-sigCh
		glog.Info("Shutting down web server...")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	}()

	glog.Infof("Starting web server on %s", addr)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		glog.Exitf("Web server error: %v", err)
	}
	glog.Info("Web server stopped")
}
