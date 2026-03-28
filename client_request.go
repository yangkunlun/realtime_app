package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/gordonklaus/portaudio"
	"github.com/gorilla/websocket"
)

type StartSessionPayload struct {
	ASR    ASRPayload    `json:"asr"`
	TTS    TTSPayload    `json:"tts"`
	Dialog DialogPayload `json:"dialog"`
}

type ASRPayload struct {
	Extra map[string]interface{} `json:"extra"`
}

type TTSPayload struct {
	Speaker     string      `json:"speaker"`
	AudioConfig AudioConfig `json:"audio_config"`
}

type AudioConfig struct {
	Channel    int    `json:"channel"`
	Format     string `json:"format"`
	SampleRate int    `json:"sample_rate"`
}

type SayHelloPayload struct {
	Content string `json:"content"`
}

type ChatTTSTextPayload struct {
	Start   bool   `json:"start"`
	End     bool   `json:"end"`
	Content string `json:"content"`
}

type ChatRAGTextPayload struct {
	ExternalRAG string `json:"external_rag"`
}

type RAGObject struct {
	Title   string `json:"title"`
	Content string `json:"content"`
}

type DialogPayload struct {
	DialogID          string                 `json:"dialog_id"`
	BotName           string                 `json:"bot_name"`
	SystemRole        string                 `json:"system_role"`
	SpeakingStyle     string                 `json:"speaking_style"`
	CharacterManifest string                 `json:"character_manifest,omitempty"`
	Location          *LocationInfo          `json:"location,omitempty"`
	Extra             map[string]interface{} `json:"extra"`
}

type LocationInfo struct {
	Longitude   float64 `json:"longitude"`
	Latitude    float64 `json:"latitude"`
	City        string  `json:"city"`
	Country     string  `json:"country"`
	Province    string  `json:"province"`
	District    string  `json:"district"`
	Town        string  `json:"town"`
	CountryCode string  `json:"country_code"`
	Address     string  `json:"address"`
}

type ChatTextQueryPayload struct {
	Content string `json:"content"`
}

func startConnection(conn *websocket.Conn) error {
	msg, err := NewMessage(MsgTypeFullClient, MsgTypeFlagWithEvent)
	if err != nil {
		return fmt.Errorf("create StartSession request message: %w", err)
	}
	msg.Event = 1
	msg.Payload = []byte("{}")

	frame, err := protocol.Marshal(msg)
	glog.Infof("StartConnection frame: %v", frame)
	if err != nil {
		return fmt.Errorf("marshal StartConnection request message: %w", err)
	}

	if err := sendRequest(conn, frame); err != nil {
		return fmt.Errorf("send StartConnection request: %w", err)
	}

	// Read ConnectionStarted message.
	mt, frame, err := conn.ReadMessage()
	if err != nil {
		return fmt.Errorf("read ConnectionStarted response: %w", err)
	}
	if mt != websocket.BinaryMessage && mt != websocket.TextMessage {
		return fmt.Errorf("unexpected Websocket message type: %d", mt)
	}

	msg, _, err = Unmarshal(frame, protocol.containsSequence)
	if err != nil {
		glog.Infof("StartConnection response: %s", frame)
		return fmt.Errorf("unmarshal ConnectionStarted response message: %w", err)
	}
	if msg.Type != MsgTypeFullServer {
		return fmt.Errorf("unexpected ConnectionStarted message type: %s", msg.Type)
	}
	if msg.Event != 50 {
		return fmt.Errorf("unexpected response event (%d) for StartConnection request", msg.Event)
	}
	glog.Infof("Connection started (event=%d) connectID: %s, payload: %s", msg.Event, msg.ConnectID, msg.Payload)

	return nil
}

func startSession(conn *websocket.Conn, sessionID string, req *StartSessionPayload) error {
	payload, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal StartSession request payload: %w", err)
	}

	msg, err := NewMessage(MsgTypeFullClient, MsgTypeFlagWithEvent)
	if err != nil {
		return fmt.Errorf("create StartSession request message: %w", err)
	}
	msg.Event = 100
	msg.SessionID = sessionID
	msg.Payload = payload

	frame, err := protocol.Marshal(msg)
	glog.Infof("StartSession request frame: %v", frame)
	if err != nil {
		return fmt.Errorf("marshal StartSession request message: %w", err)
	}

	if err := sendRequest(conn, frame); err != nil {
		return fmt.Errorf("send StartSession request: %w", err)
	}

	// Read SessionStarted message.
	mt, frame, err := conn.ReadMessage()
	if err != nil {
		return fmt.Errorf("read SessionStarted response: %w", err)
	}
	if mt != websocket.BinaryMessage && mt != websocket.TextMessage {
		return fmt.Errorf("unexpected Websocket message type: %d", mt)
	}

	// Validate SessionStarted message.
	msg, _, err = Unmarshal(frame, protocol.containsSequence)
	if err != nil {
		glog.Infof("StartSession response: %s", frame)
		return fmt.Errorf("unmarshal SessionStarted response message: %w", err)
	}
	if msg.Type != MsgTypeFullServer {
		return fmt.Errorf("unexpected SessionStarted message type: %s", msg.Type)
	}
	if msg.Event != 150 {
		return fmt.Errorf("unexpected response event (%d) for StartSession request", msg.Event)
	}
	glog.Infof("SessionStarted response payload: %v", string(msg.Payload))
	var jsonData map[string]interface{}
	if err := json.Unmarshal(msg.Payload, &jsonData); err != nil {
		return fmt.Errorf("unmarshal SessionStarted response payload: %w", err)
	}
	dialogID = jsonData["dialog_id"].(string)
	return nil
}

func sayHello(conn *websocket.Conn, sessionID string, req *SayHelloPayload) error {
	payload, err := json.Marshal(req)
	glog.Infof("SayHello request payload: %s", string(payload))
	if err != nil {
		return fmt.Errorf("marshal SayHello request payload: %w", err)
	}

	msg, err := NewMessage(MsgTypeFullClient, MsgTypeFlagWithEvent)
	if err != nil {
		return fmt.Errorf("create SayHello request message: %w", err)
	}
	msg.Event = 300
	msg.SessionID = sessionID
	msg.Payload = payload

	frame, err := protocol.Marshal(msg)
	glog.Infof("SayHello frame: %v", frame)
	if err != nil {
		return fmt.Errorf("marshal SayHello request message: %w", err)
	}

	if err := sendRequest(conn, frame); err != nil {
		return fmt.Errorf("send SayHello request: %w", err)
	}
	return nil
}

func chatTextQuery(conn *websocket.Conn, sessionID string, req *ChatTextQueryPayload) error {
	payload, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal ChatTextQuery request payload: %w", err)
	}
	msg, err := NewMessage(MsgTypeFullClient, MsgTypeFlagWithEvent)
	if err != nil {
		return fmt.Errorf("create ChatTextQuery request message: %w", err)
	}
	msg.Event = 501
	msg.SessionID = sessionID
	msg.Payload = payload
	frame, err := protocol.Marshal(msg)
	glog.Infof("ChatTextQuery request frame: %v", frame)
	if err != nil {
		return fmt.Errorf("marshal ChatTextQuery request message: %w", err)
	}
	if err := conn.WriteMessage(websocket.BinaryMessage, frame); err != nil {
		return fmt.Errorf("send ChatTextQuery request: %w", err)
	}
	return nil
}

func chatTTSText(conn *websocket.Conn, sessionID string, req *ChatTTSTextPayload) error {
	if isUserQuerying.Load() {
		glog.Errorf("chatTTSText cant be called while user is querying.")
		return nil
	}
	payload, err := json.Marshal(req)
	glog.Infof("ChatTTSText request payload: %s", string(payload))
	if err != nil {
		return fmt.Errorf("marshal ChatTTSText request payload: %w", err)
	}

	protocol.SetSerialization(SerializationJSON)
	msg, err := NewMessage(MsgTypeFullClient, MsgTypeFlagWithEvent)
	if err != nil {
		return fmt.Errorf("create ChatTTSText request message: %w", err)
	}
	msg.Event = 500
	msg.SessionID = sessionID
	msg.Payload = payload

	frame, err := protocol.Marshal(msg)
	glog.Infof("ChatTTSText frame: %v", frame)
	if err != nil {
		return fmt.Errorf("marshal ChatTTSText request message: %w", err)
	}
	if err := sendRequest(conn, frame); err != nil {
		return fmt.Errorf("send ChatTTSText request: %w", err)
	}
	return nil
}

func chatRAGText(conn *websocket.Conn, sessionID string, req *ChatRAGTextPayload) error {
	payload, err := json.Marshal(req)
	glog.Infof("ChatTTSText request payload: %s", string(payload))
	if err != nil {
		return fmt.Errorf("marshal ChatTTSText request payload: %w", err)
	}

	protocol.SetSerialization(SerializationJSON)
	msg, err := NewMessage(MsgTypeFullClient, MsgTypeFlagWithEvent)
	if err != nil {
		return fmt.Errorf("create ChatTTSText request message: %w", err)
	}
	msg.Event = 502
	msg.SessionID = sessionID
	msg.Payload = payload

	frame, err := protocol.Marshal(msg)
	glog.Infof("ChatRAGText frame: %v", frame)
	if err != nil {
		return fmt.Errorf("marshal ChatRAGText request message: %w", err)
	}
	if err := sendRequest(conn, frame); err != nil {
		return fmt.Errorf("send ChatRAGText request: %w", err)
	}
	return nil
}

func sendAudio(ctx context.Context, c *websocket.Conn, sessionID string) {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				glog.Errorf("panic: %v", err)
			}
		}()
		defaultInputDevice, err := portaudio.DefaultInputDevice()
		if err != nil {
			glog.Errorf("Failed to get default input device: %v", err)
			return
		}
		glog.Infof("Using default input device: %s", defaultInputDevice.Name)
		streamParameters := portaudio.StreamParameters{
			Input: portaudio.StreamDeviceParameters{
				Device:   defaultInputDevice,
				Channels: 1,
				Latency:  defaultInputDevice.DefaultLowInputLatency,
			},
			SampleRate:      16000,
			FramesPerBuffer: 160,
		}

		// 等待sayhello完成后再开始录音
		<-sayhelloOver

		stream, err := portaudio.OpenStream(streamParameters, func(in []int16) {
			//glog.Infof("Sending audio: %v", in)
			// 1. 将 int16 音频数据转换为 []byte (PCM S16LE)
			audioBytes := make([]byte, len(in)*2)
			for i, sample := range in {
				audioBytes[i*2] = byte(sample & 0xff)
				audioBytes[i*2+1] = byte((sample >> 8) & 0xff)
			}

			// 2. 设置序列化方式为原始数据
			// 你提供的 sendAudioData 示例中在此处设置。确保这对你的协议是正确的。
			protocol.SetSerialization(SerializationRaw)

			// 3. 创建并发送消息
			msg, err := NewMessage(MsgTypeAudioOnlyClient, MsgTypeFlagWithEvent)
			if err != nil {
				glog.Errorf("Error creating audio message: %v", err)
				return // 从回调中退出
			}

			msg.Event = 200
			msg.SessionID = sessionID
			msg.Payload = audioBytes

			frame, err := protocol.Marshal(msg)
			if err != nil {
				glog.Errorf("Error marshalling audio message: %v", err)
				return // 从回调中退出
			}

			//glog.Infof("Sent %d bytes of audio data for frame %v", len(audioBytes), frame)
			if err := sendRequest(c, frame); err != nil {
				glog.Errorf("Error sending audio message: %v", err)
				// 持续发送失败可能需要停止音频流，目前仅记录日志。
				return
			}
		})
		if err != nil {
			glog.Errorf("Failed to open microphone input stream: %v", err)
			return
		}
		defer stream.Close()

		if err := stream.Start(); err != nil {
			glog.Errorf("Failed to start microphone input stream: %v", err)
			return
		}
		glog.Info("Microphone input stream started. please speak...")

		// 保持 goroutine 运行以允许回调处理音频
		select {
		case <-ctx.Done():
			glog.Info("Stopping microphone input stream due to context cancellation...")
			if err := stream.Stop(); err != nil {
				glog.Errorf("Failed to stop microphone input stream: %v", err)
			}
			err = finishSession(c, sessionID)
			if err != nil {
				glog.Errorf("Failed to finish session: %v", err)
			}
		}
		glog.Info("Microphone input stream stopped.")
	}()
}

func sendAudioFromWav(ctx context.Context, c *websocket.Conn, sessionID string, audioFile string) error {
	content, err := os.ReadFile(audioFile)
	if err != nil {
		return err
	}
	if strings.HasSuffix(audioFile, ".wav") {
		content = content[44:]
	}

	protocol.SetSerialization(SerializationRaw)

	// 16000Hz, 16bit, 1 channel
	sleepDuration := 20 * time.Millisecond
	bufferSize := 640
	curPos := 0
	for curPos < len(content) {
		if curPos+bufferSize >= len(content) {
			bufferSize = len(content) - curPos
		}
		msg, err := NewMessage(MsgTypeAudioOnlyClient, MsgTypeFlagWithEvent)
		if err != nil {
			return fmt.Errorf("create Task request message: %w", err)
		}
		msg.Event = 200
		msg.SessionID = sessionID
		msg.Payload = content[curPos : curPos+bufferSize]

		frame, err := protocol.Marshal(msg)
		if err != nil {
			return fmt.Errorf("marshal TaskRequest message: %w", err)
		}
		if err = sendRequest(c, frame); err != nil {
			return fmt.Errorf("send TaskRequest request: %w", err)
		}

		curPos += bufferSize
		// 非最后一片时，休眠对应时长（模拟实时输入）
		if curPos < len(content) {
			// 休眠期间也监听上下文取消（避免长时间阻塞）
			select {
			case <-ctx.Done():
				return fmt.Errorf("context canceled during sleep: %w", ctx.Err())
			case <-time.After(sleepDuration):
			}
		}
	}
	return nil
}

func sendSilenceAudio(ctx context.Context, c *websocket.Conn, sessionID string) error {
	// 16000Hz, 16bit, 1 channel, 10ms silence
	silence := make([]byte, 320) // 160 samples * 2 bytes/sample = 320 bytes
	protocol.SetSerialization(SerializationRaw)
	msg, err := NewMessage(MsgTypeAudioOnlyClient, MsgTypeFlagWithEvent)
	if err != nil {
		glog.Errorf("create Task request message: %v", err)
		return err
	}
	msg.Event = 200
	msg.SessionID = sessionID
	msg.Payload = silence
	frame, err := protocol.Marshal(msg)
	if err != nil {
		glog.Errorf("marshal TaskRequest message: %v", err)
		return err
	}
	if err = sendRequest(c, frame); err != nil {
		glog.Errorf("send TaskRequest request: %v", err)
		return err
	}
	time.Sleep(10 * time.Millisecond)
	return nil
}

func finishSession(conn *websocket.Conn, sessionID string) error {
	msg, err := NewMessage(MsgTypeFullClient, MsgTypeFlagWithEvent)
	if err != nil {
		return fmt.Errorf("create FinishSession request message: %w", err)
	}
	msg.Event = 102
	msg.SessionID = sessionID
	msg.Payload = []byte("{}")

	frame, err := protocol.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal FinishSession request message: %w", err)
	}

	if err := sendRequest(conn, frame); err != nil {
		return fmt.Errorf("send FinishSession request: %w", err)
	}

	glog.Info("FinishSession request is sent.")
	return nil
}

func finishConnection(conn *websocket.Conn) error {
	msg, err := NewMessage(MsgTypeFullClient, MsgTypeFlagWithEvent)
	if err != nil {
		return fmt.Errorf("create FinishConnection request message: %w", err)
	}
	msg.Event = 2
	msg.Payload = []byte("{}")

	frame, err := protocol.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal FinishConnection request message: %w", err)
	}

	if err := sendRequest(conn, frame); err != nil {
		return fmt.Errorf("send FinishConnection request: %w", err)
	}

	// Read ConnectionStarted message.
	mt, frame, err := conn.ReadMessage()
	if err != nil {
		return fmt.Errorf("read ConnectionFinished response: %w", err)
	}
	if mt != websocket.BinaryMessage && mt != websocket.TextMessage {
		return fmt.Errorf("unexpected Websocket message type: %d", mt)
	}

	msg, _, err = Unmarshal(frame, protocol.containsSequence)
	if err != nil {
		glog.Infof("FinishConnection response: %s", frame)
		return fmt.Errorf("unmarshal ConnectionFinished response message: %w", err)
	}
	if msg.Type != MsgTypeFullServer {
		return fmt.Errorf("unexpected ConnectionFinished message type: %s", msg.Type)
	}
	if msg.Event != 52 {
		return fmt.Errorf("unexpected response event (%d) for FinishConnection request", msg.Event)
	}

	glog.Infof("Connection finished (event=%d).", msg.Event)
	return nil
}

func sendRequest(conn *websocket.Conn, frame []byte) error {
	wsWriteLock.Lock()
	defer wsWriteLock.Unlock()
	if err := conn.WriteMessage(websocket.BinaryMessage, frame); err != nil {
		return fmt.Errorf("send SayHello request: %w", err)
	}
	localSequence.Add(1)
	//glog.Infof("Send request, sequence: %d", localSequence.Load())
	return nil
}
