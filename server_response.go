package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
	"github.com/gordonklaus/portaudio"
	"github.com/gorilla/websocket"
	"github.com/samber/lo"
)

const (
	sampleRate      = 24000
	channels        = 1
	framesPerBuffer = 512
	bufferSeconds   = 100 // 最多缓冲100秒数据
	DefaultPCM      = "pcm"
	PcmS16LE        = "pcm_s16le"
)

var (
	audio                []byte
	bufferLock           sync.Mutex
	buffer               = make([]float32, 0, sampleRate*bufferSeconds)
	s16Buffer            = make([]int16, 0, sampleRate*bufferSeconds)
	isSendingChatTTSText = atomic.Bool{}
	isUserQuerying       = atomic.Bool{}
	sayhelloOver         = make(chan struct{})
	firstMsgOnce         = sync.Once{}
)

func realtimeAPIOutputAudio(ctx context.Context, conn *websocket.Conn) {
	if !isAudioFileInput() {
		go startPlayer(ctx)
	}

	for {
		if !isTextMod() {
			glog.Infof("Waiting for message...")
		}
		msg, err := receiveMessage(conn)
		if err != nil {
			glog.Errorf("Receive message error: %v", err)
			return
		}
		switch msg.Type {
		case MsgTypeFullServer:
			glog.Infof("Receive text message (event=%d, session_id=%s): %s", msg.Event, msg.SessionID, msg.Payload)
			// session finished event
			if msg.Event == 152 || msg.Event == 153 {
				return
			}
			if msg.Event == 359 && isAudioFileInput() {
				return
			}
			if msg.Event == 359 && !isAudioFileInput() {
				firstMsgOnce.Do(func() {
					sayhelloOver <- struct{}{}
				})
				if isTextMod() {
					glog.Info("请输入内容")
				}
			}

			// asr info event, clear audio buffer
			if msg.Event == 450 {
				// 清空本地音频缓存，等待接收下一轮的音频
				audio = audio[:0]
				buffer = buffer[:0]
				// 用户说话了，不需要触发连续SayHello引导用户交互了
				queryChan <- struct{}{}
				isUserQuerying.Store(true)
			}
			// 发送ChatTTSText请求事件之后，收到tts_type为chat_tts_text的事件，清空本地缓存的S2S模型闲聊音频数据
			if msg.Event == 350 && isSendingChatTTSText.Load() {
				var jsonData map[string]interface{}
				_ = json.Unmarshal(msg.Payload, &jsonData)
				ttsType := jsonData["tts_type"].(string)
				// 一种简单方式清空本地闲聊音频
				if lo.Contains([]string{"chat_tts_text", "external_rag"}, ttsType) {
					audio = audio[:0]
					buffer = buffer[:0]
					isSendingChatTTSText.Store(false)
				}
			}
			if msg.Event == 459 {
				isUserQuerying.Store(false)
			}
			// 概率触发发送ChatTTSText请求
			if msg.Event == 459 && rand.Intn(100000)%1 == 0 {
				go func() {
					isSendingChatTTSText.Store(true)
					glog.Infof("hit ChatTTSText event, start sending...")
					_ = chatTTSText(conn, msg.SessionID, &ChatTTSTextPayload{
						Start:   true,
						End:     false,
						Content: "我查查",
					})
					_ = chatTTSText(conn, msg.SessionID, &ChatTTSTextPayload{
						Start:   false,
						End:     true,
						Content: "",
					})
					time.Sleep(5 * time.Second) // 模拟查询外部RAG数据耗时，这里简单起见直接sleep5秒保证GTA安抚话术播报不受影响
					externalRAG := []RAGObject{
						{
							Title:   "北京天气",
							Content: "今天北京整体以晴到多云为主，但西部和北部地带可能会出现分散性雷阵雨，特别是午后至傍晚时段需注意突发降雨。\n💨 风况与湿度\n风力较弱，一般为 2–3 级南风或西南风\n白天湿度较高，早晚略凉爽",
						},
						{
							Title:   "北京空气质量",
							Content: "当前北京空气质量为良，AQI指数在50左右，适合户外活动。建议关注实时空气质量变化，尤其是敏感人群。",
						},
					}
					externalRAGBytes, _ := json.Marshal(externalRAG)
					_ = chatRAGText(conn, msg.SessionID, &ChatRAGTextPayload{
						ExternalRAG: string(externalRAGBytes),
					})
				}()
			}
		case MsgTypeAudioOnlyServer:
			glog.Infof("Receive audio message (event=%d): session_id=%s", msg.Event, msg.SessionID)
			handleIncomingAudio(msg.Payload)
			audio = append(audio, msg.Payload...)
		case MsgTypeError:
			glog.Exitf("Receive Error message (code=%d): %s", msg.ErrorCode, string(msg.Payload))
			return
		default:
			glog.Exitf("Received unexpected message type: %s", msg.Type)
			return
		}
	}
}

/**
 * 结合api接入文档对二进制协议进行理解，上下行统一理解
 * - header(4bytes)
 *     - (4bits)version(v1) + (4bits)header_size
 *     - (4bits)messageType + (4bits)messageTypeFlags
 *         -- 0001	CompleteClient  | -- 0001 optional has sequence
 *         -- 0010	AudioOnlyClient | -- 0100 optional has event
 *         -- 1001 CompleteServer   | -- 1111 optional has error code
 *         -- 1011 AudioOnlyServer  | --
 *     - (4bits)payloadFormat + (4bits)compression
 *     - (8bits) reserve
 * - payload
 *     - [optional 4 bytes] event
 *     - [optional] session ID
 *       -- (4 bytes)session ID len
 *       -- session ID data
 *     - (4 bytes)data len
 *     - data
 */
func receiveMessage(conn *websocket.Conn) (*Message, error) {
	mt, frame, err := conn.ReadMessage()
	if err != nil {
		return nil, err
	}
	if mt != websocket.BinaryMessage && mt != websocket.TextMessage {
		return nil, fmt.Errorf("unexpected Websocket message type: %d", mt)
	}

	framePrefix := frame
	if len(frame) > 100 {
		framePrefix = frame[:100]
	}
	glog.Infof("Receive frame prefix: %v", framePrefix)
	msg, _, err := Unmarshal(frame, ContainsSequence)
	if err != nil {
		if len(frame) > 500 {
			frame = frame[:500]
		}
		glog.Infof("Data response: %s", frame)
		return nil, fmt.Errorf("unmarshal response message: %w", err)
	}
	return msg, nil
}

func startPlayer(ctx context.Context) {
	outputDevice, err := portaudio.DefaultOutputDevice()
	if err != nil {
		glog.Errorf("Failed to get default output device: %v", err)
		return
	}
	outputParameters := portaudio.StreamParameters{
		Output: portaudio.StreamDeviceParameters{
			Device:   outputDevice,
			Channels: channels,
			Latency:  10 * time.Millisecond,
		},
		SampleRate:      float64(sampleRate),
		FramesPerBuffer: framesPerBuffer,
	}
	var outputStream *portaudio.Stream
	switch pcmFormat {
	case DefaultPCM:
		outputStream, err = portaudio.OpenStream(outputParameters, func(out []float32) {
			bufferLock.Lock()
			defer bufferLock.Unlock()
			if len(buffer) < len(out) {
				copy(out, buffer)
				for i := len(buffer); i < len(out); i++ {
					out[i] = 0
				}
				buffer = buffer[:0]
			} else {
				copy(out, buffer)
				buffer = buffer[len(out):]
			}
		})
	case PcmS16LE:
		outputStream, err = portaudio.OpenStream(outputParameters, func(out []int16) {
			bufferLock.Lock()
			defer bufferLock.Unlock()
			if len(s16Buffer) < len(out) {
				copy(out, s16Buffer)
				for i := len(s16Buffer); i < len(out); i++ {
					out[i] = 0
				}
				s16Buffer = s16Buffer[:0]
			} else {
				copy(out, s16Buffer)
				s16Buffer = s16Buffer[len(out):]
			}
		})
	}
	if outputStream == nil {
		glog.Errorf("Failed to open PortAudio output stream: %v", err)
		return
	}
	if err != nil {
		glog.Errorf("Failed to open PortAudio output stream: %v", err)
		return
	}
	defer outputStream.Close()

	if err := outputStream.Start(); err != nil {
		glog.Errorf("Failed to start PortAudio output stream: %v", err)
		return
	}
	glog.Info("PortAudio output stream started for playback.")
	<-ctx.Done()
	saveAudioToPCMFile("output.pcm")
	glog.Info("PortAudio output stream stopped.")
}

func handleIncomingAudio(data []byte) {
	if isSendingChatTTSText.Load() {
		return
	}
	switch pcmFormat {
	case PcmS16LE:
		glog.Infof("Received audio byte len: %d, float16 len: %d", len(data), len(data)/2)
		sampleCount := len(data) / 2
		samples := make([]int16, sampleCount)
		for i := 0; i < sampleCount; i++ {
			bits := binary.LittleEndian.Uint16(data[i*2 : (i+1)*2])
			samples[i] = int16(bits)
		}
		// 将音频加载到缓冲区
		bufferLock.Lock()
		defer bufferLock.Unlock()
		s16Buffer = append(s16Buffer, samples...)
		if len(buffer) > sampleRate*bufferSeconds {
			s16Buffer = s16Buffer[len(s16Buffer)-(sampleRate*bufferSeconds):]
		}
	case DefaultPCM:
		glog.Infof("Received audio byte len: %d, float32 len: %d", len(data), len(data)/4)
		sampleCount := len(data) / 4
		samples := make([]float32, sampleCount)
		for i := 0; i < sampleCount; i++ {
			bits := binary.LittleEndian.Uint32(data[i*4 : (i+1)*4])
			samples[i] = math.Float32frombits(bits)
		}
		// 将音频加载到缓冲区
		bufferLock.Lock()
		defer bufferLock.Unlock()
		buffer = append(buffer, samples...)
		if len(buffer) > sampleRate*bufferSeconds {
			buffer = buffer[len(buffer)-(sampleRate*bufferSeconds):]
		}
	}

}

func saveAudioToPCMFile(s string) {
	if len(audio) == 0 {
		glog.Info("No audio data to save.")
		return
	}
	pcmPath := filepath.Join("./", s)
	if err := os.WriteFile(pcmPath, audio, 0644); err != nil {
		glog.Exitf("Save pcm file: %v", err)
	}
}
