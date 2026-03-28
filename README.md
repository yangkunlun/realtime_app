# RealtimeDialog 项目

## 项目简介
RealtimeDialog 是一个用于实时对话的项目，使用 WebSocket 进行通信，并支持音频流的发送和接收。

## 环境要求
- Go 1.24 或更高版本
- 需要安装 `portaudio` 库
  - macOS: `brew install portaudio`
  - Linux: `sudo apt-get install libportaudio2`

## 其他工具
- 播放pcm音频的[在线工具](https://pcm.qer.im/)，选择对应的采样率、位深度即可播放，可以协助判断音频播放的问题

## 获取 API 参数
在使用此项目之前，您需要从火山引擎控制台获取以下参数：
- `appid`
- `access token`

### 获取步骤
1. 登录到 [火山引擎控制台](https://console.volcengine.com/).
2. 导航到 [语音技术](https://console.volcengine.com/speech/app) 管理页面。
3. 创建或选择一个应用，开通豆包端到端实时语音大模型，获取 `appid` 和 `access token`。
4. 将获取到的 `appid` 和 `access token` 替换到 `main.go` 文件中的相应位置。

## 运行项目
1. 下载项目到本地，在本地启动运行：
   ```bash
   go run . -v=0
1.1  麦克风收音启动成功的日志：
   ```bash
   Microphone stream started. Sending live audio...
   ```
1.2 播放器启动成功的日志：
   ```bash
   PortAudio output stream started for playback.
   ```

2. 上传音频文件启动端到端实时语音大模型：
   ```bash
   go run . -v=0 -audio=whoareyou.wav
   ```
2.1 模型输出音频存储到本地文件：
   ```bash
   output.pcm
   ```
2.2 在线浏览器播放pcm文件，采样率24000Hz，位深度16bit或者32bit，单声道，小端序：
   ```bash
   https://pcm.qer.im/
   ```

3. 纯文本输入模式启动端到端实时语音大模型
   ```bash
   go run . -v=0 -mod=text -recv_timeout=120
   ```