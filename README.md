# 实时语音识别WebSocket服务

## 项目介绍

本项目基于火山引擎的语音识别API，实现了一个WebSocket服务，可以实时接收用户的语音数据并返回识别结果。项目使用Spring Boot和WebSocket技术栈，提供了简单的Web界面用于测试。

## 技术架构

- **Spring Boot**: 提供Web应用框架
- **Spring WebSocket**: 处理WebSocket通信
- **OkHttp**: 与火山引擎ASR服务通信
- **GSON**: 处理JSON数据

## 核心组件

- **AudioWebSocketHandler**: 处理客户端WebSocket连接，接收音频数据
- **AsrService**: 语音识别服务接口
- **AsrServiceImpl**: 实现与火山引擎ASR的通信，处理音频数据和识别结果

## 配置说明

在`application.properties`中配置火山引擎ASR服务参数：

```properties
# 火山引擎ASR配置
volcengine.asr.appId=你的AppID
volcengine.asr.token=你的Token

# ASR服务配置
asr.url=wss://openspeech.bytedance.com/api/v3/sauc/bigmodel
asr.appId=${volcengine.asr.appId}
asr.token=${volcengine.asr.token}

# 音频格式配置
asr.audio.sampleRate=16000
asr.audio.sampleSizeInBits=16
asr.audio.channels=1
```

## 使用方法

### 运行服务

1. 确保已安装Java 8或更高版本
2. 使用Maven构建项目：`mvn clean package`
3. 运行应用：`java -jar target/asr-websocket-0.0.1-SNAPSHOT.jar`
4. 访问 http://localhost:8080 打开测试页面

### 测试流程

1. 点击「连接服务器」按钮建立WebSocket连接
2. 点击「开始录音」按钮，允许浏览器访问麦克风
3. 开始说话，语音识别结果会实时显示在页面上
4. 点击「停止录音」按钮结束录音
5. 点击「断开连接」按钮关闭WebSocket连接

## WebSocket协议

### 客户端到服务器

- 二进制消息：音频数据块
- 文本消息：`END_STREAM` 表示音频流结束

### 服务器到客户端

- 文本消息：JSON格式的识别结果

## 注意事项

- 当前仅支持16kHz、16bit、单声道的PCM音频格式
- 浏览器测试时，录制的音频会自动转换为适合的格式
- 生产环境中应限制WebSocket连接的来源

## 运行脚本
启动服务端
mvn exec:java -Dexec.mainClass="com.example.asrwebsocket.handler.YourWebSocketServer"
启动录音测试客户端
mvn exec:java -Dexec.mainClass="com.example.asrwebsocket.handler.TestWebSocketClient"
启动麦克风测试客户端
mvn exec:java -Dexec.mainClass="com.example.asrwebsocket.handler.MicrophoneWebSocketClient"