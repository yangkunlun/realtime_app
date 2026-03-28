package main

import (
	"bufio"
	"context"
	"flag"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/golang/glog"
	"github.com/google/uuid"
	"github.com/gordonklaus/portaudio"
	"github.com/gorilla/websocket"
)

var (
	// 客户接入需要修改的参数
	appid       = "6287539337"
	accessToken = "BiBy9u5zAMS7rKmuAnTGGq-BKkIl5z4O"
	// 如果传了这个,则走本地文件的wav作为输入,在demo中输出结果pcm后直接退出
	audioFilePath = flag.String("audio", "", "audio file to send server, if not set, use mic as input")

	// 纯文本模式text和audio模式
	mod         = flag.String("mod", "audio", "Use mod to select plain text input mode or audio mode, the default is audio mode")
	recvTimeout = flag.Int64("recv_timeout", 10, "Timeout for receiving messages,value range [10,120]")

	// 客户接入新增自定义参数
	speaker = "zh_female_vv_jupiter_bigtts" // 指定vv音色，其他音色可以在火山官网文档中查询
	// speaker   = "S_XXXXXX" // 指定自定义的复刻音色,需要填下character_manifest
	// speaker   = "ICL_zh_female_aojiaonvyou_tob" // 指定官方复刻音色，不需要填character_manifest
	pcmFormat = "pcm" // 音频格式，pcm_s16le 或者 pcm，其中pcm对应f32le格式，pcm_s16le对应s16le格式

	// 无需修改的参数
	wsURL         = url.URL{Scheme: "wss", Host: "openspeech.bytedance.com", Path: "/api/v3/realtime/dialogue"}
	protocol      = NewBinaryProtocol()
	dialogID      = ""
	wsWriteLock   sync.Mutex
	localSequence = atomic.Int64{}
	queryChan     = make(chan struct{}, 10)
)

func init() {
	protocol.SetVersion(Version1)
	protocol.SetHeaderSize(HeaderSize4)
	protocol.SetSerialization(SerializationJSON)
	protocol.SetCompression(CompressionNone, nil)
	protocol.containsSequence = ContainsSequence
	rand.New(rand.NewSource(time.Now().UnixNano()))
}

func isAudioFileInput() bool {
	if audioFilePath == nil {
		return false
	}
	return *audioFilePath != ""
}

func getMod() string {
	if isAudioFileInput() {
		return "audio_file"
	}
	if mod == nil {
		return "audio"
	}
	return *mod
}

func isTextMod() bool {
	return getMod() == "text"
}

// 流式合成
func realTimeDialog(ctx context.Context, c *websocket.Conn, sessionID string) {
	err := startConnection(c)
	if err != nil {
		glog.Errorf("realTimeDialog startConnection error: %v", err)
		return
	}

	err = startSession(c, sessionID, &StartSessionPayload{
		ASR: ASRPayload{
			Extra: map[string]interface{}{
				"end_smooth_window_ms": 1500,
			},
		},
		TTS: TTSPayload{
			Speaker: speaker,
			AudioConfig: AudioConfig{
				Channel:    1,
				Format:     pcmFormat,
				SampleRate: 24000,
			},
		},
		Dialog: DialogPayload{
			BotName: "张姐",
			SystemRole: `你现在是张姐，性格热情外向，扮演一位真实的客户角色。

对话场景：
- 角色：你是张姐，开放外向
- 对象：与你对话的是一位专业美容师
- 场景：美容院招揽客户

客户画像：
- 客户特点：热情型
- 客户偏好：爱好交友玩乐

【核心身份】
你是客户，不是美容师！始终以客户的身份和角度说话。对方是美容师，是服务提供者。

【对话进展管理】
对话应该按照以下阶段自然推进：
1. 开场交流 - 问候、自我介绍、表达需求
2. 咨询了解 - 询问服务内容、流程、价格等
3. 深入交流 - 详细了解服务细节、产品功效等
4. 确认细节 - 确认具体安排、注意事项等
5. 达成意向 - 表达兴趣、犹豫或决策

【主动提问策略】
作为真实客户，你应该主动提出问题，而不是等待美容师引导。根据美容院场景，你应该：

1. 服务流程类问题：
   - "这个项目大概需要多长时间？"
   - "具体流程是什么样的？"
   - "会不会有不适感？"

2. 价格相关类问题：
   - "这个需要多少钱？"
   - "有优惠活动吗？"
   - "价格有点高，能便宜点吗？"

3. 安全效果类问题：
   - "这个真的有效吗？"
   - "会不会有副作用？"
   - "和别家的有什么区别？"

【提问时机】
- 对话初期（1-3轮）：询问基础信息（时间、价格、流程）
- 对话中期（4-6轮）：深入了解细节（安全性、效果、对比）
- 对话后期（7轮+）：确认细节、表达态度（预约时间、决策）

【回复原则】
1. 角色一致：始终保持客户身份，不要角色混淆
2. 符合性格：回复要符合热情外向的性格特点
3. 自然真实：用词生活化，像真实客户那样说话
4. 简洁精炼：1-2句话即可，不要太长
5. 积极互动：主动提问，推动对话，保持流畅
6. 表现需求：适当表现出对服务的需求、兴趣或顾虑
7. 真实情绪：可以表现出犹豫、怀疑、期待等真实情绪
8. 状态感知：根据对话进展调整回复内容和情绪
9. 反应自然：对美容师的异常行为做出符合真实客户的反应`,
			SpeakingStyle: "你的说话风格简洁明了，语速适中，语调自然。热情外向，像真实客户那样说话，用词生活化。",
			// 如果要使用自定义的复刻音色，需要填写character_manifest
			// CharacterManifest: "外貌与穿着\n26岁，短发干净利落，眉眼分明，笑起来露出整齐有力的牙齿。体态挺拔，肌肉线条不夸张但明显。常穿简单的衬衫或夹克，看似随意，但每件衣服都干净整洁，给人一种干练可靠的感觉。平时冷峻，眼神锐利，专注时让人不自觉紧张。\n\n性格特点\n平时话不多，不喜欢多说废话，通常用“嗯”或者短句带过。但内心极为细腻，特别在意身边人的感受，只是不轻易表露。嘴硬是常态，“少管我”是他的常用台词，但会悄悄做些体贴的事情，比如把对方喜欢的饮料放在手边。战斗或训练后常说“没事”，但动作中透露出疲惫，习惯用小动作缓解身体酸痛。\n性格上坚毅果断，但不会冲动，做事有条理且有原则。\n\n常用表达方式与口头禅\n\t•\t认可对方时：\n“行吧，这次算你靠谱。”（声音稳重，手却不自觉放松一下，心里松口气）\n\t•\t关心对方时：\n“快点回去，别磨蹭。”（语气干脆，但眼神一直追着对方的背影）\n\t•\t想了解情况时：\n“刚刚……你看到那道光了吗？”（话语随意，手指敲着桌面，但内心紧张，小心隐藏身份）",
			Location: &LocationInfo{
				City: "北京",
			},
			Extra: map[string]interface{}{
				"strict_audit":   false,
				"audit_response": "抱歉这个问题我无法回答，你可以换个其他话题，我会尽力为你提供帮助。",
				"recv_timeout":   &recvTimeout, // 扩大这个参数，可以在一段时间内保持静默，主要用于text模式，参数范围[10,120]
				"input_mod":      getMod(),     // 这个参数，在text或者audio_file模式，可以在一段时间内保持静默
			},
		},
	})
	if err != nil {
		glog.Errorf("realTimeDialog startSession error: %v", err)
		return
	}

	switch getMod() {
	case "text":
		textInputMod(ctx, c, sessionID)
	default:
		if isAudioFileInput() {
			audioFileInputMod(ctx, c, sessionID)
		} else {
			audioRealtimeMod(ctx, c, sessionID)
		}
	}
}

func textInputMod(ctx context.Context, c *websocket.Conn, sessionID string) {
	defer func() {
		// 结束对话，断开websocket连接
		err := finishConnection(c)
		if err != nil {
			glog.Errorf("Failed to finish connection: %v", err)
		}
		glog.Info("realTimeDialog finished.")
	}()

	go func() {
		defer func() {
			err := finishSession(c, sessionID)
			if err != nil {
				glog.Errorf("Failed to finish session: %v", err)
			}
			glog.Info("session finished.")
		}()
		<-sayhelloOver
		scanner := bufio.NewScanner(os.Stdin)
		// 用channel接收输入（避免scanner.Scan()阻塞导致无法响应context）
		inputCh := make(chan string)
		go func() {
			for {
				scanned := scanner.Scan()
				if !scanned {
					close(inputCh) // 读取失败时关闭channel
					return
				}
				input := strings.TrimSpace(scanner.Text())
				inputCh <- input
			}
		}()
		// 主循环：监听输入和context结束信号
		for {
			select {
			case <-ctx.Done():
				return
			case input, ok := <-inputCh:
				if !ok {
					glog.Info("realTimeDialog inputCh closed.")
					return
				}
				if len(input) > 0 {
					err := chatTextQuery(c, sessionID, &ChatTextQueryPayload{Content: input})
					if err != nil {
						glog.Errorf("Failed to send chatTextQuery: %v", err)
					}
				}
			}
		}
	}()

	// 模拟发送问候语
	err := sayHello(c, sessionID, &SayHelloPayload{
		Content: "你好呀，我最近皮肤状态不太好，想了解一下你们这边有什么项目能改善一下吗？",
	})
	if err != nil {
		glog.Errorf("realTimeDialog sayHello error: %v", err)
		return
	}
	// 接收服务端返回数据
	realtimeAPIOutputAudio(ctx, c)
	return
}

func audioFileInputMod(ctx context.Context, c *websocket.Conn, sessionID string) {
	quit := make(chan struct{})
	var err error
	defer func() {
		if r := recover(); r != nil {
			glog.Errorf("realTimeDialog recover: %v", r)
		}
		close(quit)
		time.Sleep(10 * time.Millisecond)
		err = finishSession(c, sessionID)
		if err != nil {
			glog.Errorf("Failed to finish session: %v", err)
		}

		// 结束对话，断开websocket连接
		err = finishConnection(c)
		if err != nil {
			glog.Errorf("Failed to finish connection: %v", err)
		}
		glog.Info("realTimeDialog finished.")
		saveAudioToPCMFile("output.pcm")
	}()

	go func() {
		err = sendAudioFromWav(ctx, c, sessionID, *audioFilePath)
		if err != nil {
			panic(err)
		}
	}()
	// 接收服务端返回数据
	realtimeAPIOutputAudio(ctx, c)
	return
}

func audioRealtimeMod(ctx context.Context, c *websocket.Conn, sessionID string) {
	defer func() {
		// 结束对话，断开websocket连接
		err := finishConnection(c)
		if err != nil {
			glog.Errorf("Failed to finish connection: %v", err)
		}
		glog.Info("realTimeDialog finished.")
	}()
	// 模拟发送问候语
	err := sayHello(c, sessionID, &SayHelloPayload{
		Content: "你好呀，我最近皮肤状态不太好，想了解一下你们这边有什么项目能改善一下吗？",
	})
	if err != nil {
		glog.Errorf("realTimeDialog sayHello error: %v", err)
		return
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-queryChan:
				glog.Info("Received user query signal, starting real-time dialog...")
			case <-time.After(30 * time.Second):
				glog.Info("Timeout waiting for user query, start new SayHello request...")
				err = sayHello(c, sessionID, &SayHelloPayload{
					Content: "哎，你还在吗？我还在想刚才说的那个项目呢，到底怎么样啊？",
				})
			}
		}
	}()
	// 模拟发送音频流到服务端
	sendAudio(ctx, c, sessionID)

	// 接收服务端返回数据
	realtimeAPIOutputAudio(ctx, c)
}

func main() {
	_ = flag.Set("logtostderr", "true")
	flag.Parse()

	if !isAudioFileInput() {
		if err := portaudio.Initialize(); err != nil {
			glog.Fatalf("portaudio initialize error: %v", err)
			return
		}
		defer func() {
			err := portaudio.Terminate()
			if err != nil {
				glog.Errorf("Failed to terminate portaudio: %v", err)
			}
		}()
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	conn, resp, err := websocket.DefaultDialer.DialContext(ctx, wsURL.String(), http.Header{
		"X-Api-Resource-Id": []string{"volc.speech.dialog"},
		"X-Api-Access-Key":  []string{accessToken},
		"X-Api-App-Key":     []string{"PlgvMymc7f3tQnJ6"},
		"X-Api-App-ID":      []string{appid},
		"X-Api-Connect-Id":  []string{uuid.New().String()},
	})
	if err != nil {
		glog.Errorf("Websocket dial error: %v", err)
		return
	}
	defer func() {
		if resp != nil {
			glog.Infof("Websocket dial response logid: %s", resp.Header.Get("X-Tt-Logid"))
		}
		close(queryChan)
		glog.Infof("Websocket response dialogID: %s", dialogID)
		_ = conn.Close()
	}()

	realTimeDialog(ctx, conn, uuid.New().String())
}
