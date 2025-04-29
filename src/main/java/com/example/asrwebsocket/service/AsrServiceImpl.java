package com.example.asrwebsocket.service;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import okhttp3.*;
import okhttp3.logging.HttpLoggingInterceptor;
import okio.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import javax.annotation.PostConstruct;
import javax.sound.sampled.AudioFormat;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * 语音识别服务实现类
 * 负责与火山引擎ASR服务通信，处理实时语音识别
 */
@Service
public class AsrServiceImpl implements AsrService {

    private static final Logger logger = LoggerFactory.getLogger(AsrServiceImpl.class);

    // 协议常量，从BigASRWebsocketDemo复制
    private static final byte PROTOCOL_VERSION = 0b0001;
    private static final byte DEFAULT_HEADER_SIZE = 0b0001;
    // 消息类型
    private static final byte FULL_CLIENT_REQUEST = 0b0001;
    private static final byte AUDIO_ONLY_REQUEST = 0b0010;
    private static final byte FULL_SERVER_RESPONSE = 0b1001;
    private static final byte SERVER_ACK = 0b1011;
    private static final byte SERVER_ERROR_RESPONSE = 0b1111;
    // 消息类型特定标志
    private static final byte NO_SEQUENCE = 0b0000;// 无检查序列
    private static final byte POS_SEQUENCE = 0b0001;
    private static final byte NEG_SEQUENCE = 0b0010;
    private static final byte NEG_WITH_SEQUENCE = 0b0011;
    // 消息序列化
    private static final byte NO_SERIALIZATION = 0b0000;
    private static final byte JSON = 0b0001;
    // 消息压缩
    private static final byte NO_COMPRESSION = 0b0000;
    private static final byte GZIP = 0b0001;

    // 火山引擎ASR服务配置
    @Value("${asr.url:wss://openspeech.bytedance.com/api/v3/sauc/bigmodel}")
    private String asrUrl;

    @Value("${asr.appId:3467491207}")
    private String appId;

    @Value("${asr.token:kPHQMVliZeyYh2fWkiOC-e9eXIeZhCN8}")
    private String token;

    // 音频格式配置
    @Value("${asr.audio.sampleRate:16000}")
    private int sampleRate;

    @Value("${asr.audio.sampleSizeInBits:16}")
    private int sampleSizeInBits;

    @Value("${asr.audio.channels:1}")
    private int channels;

    private OkHttpClient okHttpClient;

    // 会话管理：客户端会话ID -> ASR会话信息
    private final Map<String, AsrSession> sessionMap = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        // 初始化OkHttpClient
        HttpLoggingInterceptor loggingInterceptor = new HttpLoggingInterceptor();
        loggingInterceptor.setLevel(HttpLoggingInterceptor.Level.HEADERS);
        okHttpClient = new OkHttpClient.Builder()
                .pingInterval(50, TimeUnit.SECONDS)
                .addInterceptor(loggingInterceptor)
                .readTimeout(100, TimeUnit.SECONDS)
                .writeTimeout(100, TimeUnit.SECONDS)
                .build();

        logger.info("ASR服务初始化完成，URL: {}", asrUrl);
    }

    @Override
    public void startAsrSession(WebSocketSession clientSession) {
        String sessionId = clientSession.getId();
        logger.info("启动ASR会话: {}", sessionId);

        // 创建与火山引擎ASR的WebSocket连接
        Request request = new Request.Builder()
                .url(asrUrl)
                .header("X-Api-App-Key", appId)
                .header("X-Api-Access-Key", token)
                .header("X-Api-Resource-Id", "volc.bigasr.sauc.duration")
                .header("X-Api-Connect-Id", UUID.randomUUID().toString())
                .build();

        // 创建会话对象
        AsrSession asrSession = new AsrSession(clientSession);
        sessionMap.put(sessionId, asrSession);

        // 建立与ASR服务的WebSocket连接
        WebSocket asrWebSocket = okHttpClient.newWebSocket(request, new AsrWebSocketListener(asrSession));
        asrSession.setAsrWebSocket(asrWebSocket);
    }

    @Override
    public void processAudioChunk(String sessionId, byte[] audioChunk, boolean isLast) {
        AsrSession asrSession = sessionMap.get(sessionId);
        if (asrSession == null) {
            logger.warn("找不到会话: {}", sessionId);
            return;
        }

        // 如果是首个音频块，先发送初始化请求
        if (!asrSession.isInitialized()) {
            sendFullClientRequest(asrSession);
            return; // 等待ASR服务响应后再发送音频数据
        }

        // 发送音频数据
        sendAudioOnlyRequest(asrSession, audioChunk, isLast);

        // 如果是最后一块，标记会话即将结束
        if (isLast) {
            asrSession.setEnding(true);
        }
    }

    @Override
    public void cleanupSession(String sessionId) {
        AsrSession asrSession = sessionMap.remove(sessionId);
        if (asrSession != null) {
            logger.info("清理ASR会话资源: {}", sessionId);
            WebSocket asrWebSocket = asrSession.getAsrWebSocket();
            if (asrWebSocket != null) {
                asrWebSocket.close(1000, "Client disconnected");
            }
        }
    }

    /**
     * 发送完整客户端请求（初始化请求）
     */
    private void sendFullClientRequest(AsrSession asrSession) {
        WebSocket asrWebSocket = asrSession.getAsrWebSocket();
        if (asrWebSocket == null) {
            logger.error("ASR WebSocket连接未建立");
            return;
        }

        try {
            // 构建音频格式信息
            AudioFormat format = new AudioFormat(sampleRate, sampleSizeInBits, channels, true, false);

            // 构建请求payload
            JsonObject user = new JsonObject();
            user.addProperty("uid", asrSession.getClientSessionId());

            JsonObject audio = new JsonObject();
            audio.addProperty("format", "pcm");
            audio.addProperty("sample_rate", (int) format.getSampleRate());
            audio.addProperty("bits", format.getSampleSizeInBits());
            audio.addProperty("channel", format.getChannels());
            audio.addProperty("codec", "raw");

            JsonObject request = new JsonObject();
            request.addProperty("model_name", "bigmodel");
            request.addProperty("enable_punc", true);

            JsonObject payload = new JsonObject();
            payload.add("user", user);
            payload.add("audio", audio);
            payload.add("request", request);

            String payloadStr = payload.toString();
            logger.debug("ASR初始化请求: {}", payloadStr);

            // 压缩payload
            byte[] payloadBytes = gzipCompress(payloadStr.getBytes());

            // 组装fullClientRequest
            byte[] header = getHeader(FULL_CLIENT_REQUEST, POS_SEQUENCE, JSON, GZIP, (byte) 0);
            byte[] payloadSize = intToBytes(payloadBytes.length);
            asrSession.setSeq(1); // 初始化序列号
            byte[] seqBytes = generateBeforPayload(asrSession.getSeq());

            byte[] fullClientRequest = new byte[header.length + seqBytes.length + payloadSize.length + payloadBytes.length];
            int destPos = 0;
            System.arraycopy(header, 0, fullClientRequest, destPos, header.length);
            destPos += header.length;
            System.arraycopy(seqBytes, 0, fullClientRequest, destPos, seqBytes.length);
            destPos += seqBytes.length;
            System.arraycopy(payloadSize, 0, fullClientRequest, destPos, payloadSize.length);
            destPos += payloadSize.length;
            System.arraycopy(payloadBytes, 0, fullClientRequest, destPos, payloadBytes.length);

            // 发送请求
            boolean success = asrWebSocket.send(ByteString.of(fullClientRequest));
            if (!success) {
                logger.error("发送ASR初始化请求失败");
            }
        } catch (Exception e) {
            logger.error("发送ASR初始化请求异常", e);
        }
    }

    /**
     * 发送音频数据请求
     */
    private boolean sendAudioOnlyRequest(AsrSession asrSession, byte[] buffer, boolean isLast) {
        WebSocket asrWebSocket = asrSession.getAsrWebSocket();
        if (asrWebSocket == null) {
            logger.error("ASR WebSocket连接未建立");
            return false;
        }

        try {
            // 更新序列号
            int seq = asrSession.incrementSeq();
            if (isLast) {
                seq = -seq; // 最后一块数据使用负序列号
            }

            byte messageTypeSpecificFlags = isLast ? NEG_WITH_SEQUENCE : POS_SEQUENCE;

            // 构建header
            byte[] header = getHeader(AUDIO_ONLY_REQUEST, messageTypeSpecificFlags, JSON, GZIP, (byte) 0);
            
            // 序列号
            byte[] sequenceBytes = generateBeforPayload(seq);
            
            // 压缩音频数据
            byte[] payloadBytes = gzipCompress(buffer);
            
            // payload大小
            byte[] payloadSize = intToBytes(payloadBytes.length);
            
            // 组装audio_only_request
            byte[] audioOnlyRequest = new byte[header.length + sequenceBytes.length + payloadSize.length + payloadBytes.length];
            int destPos = 0;
            System.arraycopy(header, 0, audioOnlyRequest, destPos, header.length);
            destPos += header.length;
            System.arraycopy(sequenceBytes, 0, audioOnlyRequest, destPos, sequenceBytes.length);
            destPos += sequenceBytes.length;
            System.arraycopy(payloadSize, 0, audioOnlyRequest, destPos, payloadSize.length);
            destPos += payloadSize.length;
            System.arraycopy(payloadBytes, 0, audioOnlyRequest, destPos, payloadBytes.length);

            // 发送请求
            return asrWebSocket.send(ByteString.of(audioOnlyRequest));
        } catch (Exception e) {
            logger.error("发送音频数据异常", e);
            return false;
        }
    }

    /**
     * ASR WebSocket监听器
     */
    private class AsrWebSocketListener extends WebSocketListener {
        private final AsrSession asrSession;

        public AsrWebSocketListener(AsrSession asrSession) {
            this.asrSession = asrSession;
        }

        @Override
        public void onOpen(WebSocket webSocket, Response response) {
            String logId = response.header("X-Tt-Logid");
            logger.info("ASR WebSocket连接已建立, X-Tt-Logid: {}", logId);
            
            // 连接建立后，等待客户端发送音频数据
            // 初始化标记设为true，表示可以开始处理音频数据
            asrSession.setInitialized(true);
            
            // 如果已经有缓存的音频数据，立即处理
            if (asrSession.hasPendingAudioChunk()) {
                byte[] audioChunk = asrSession.getPendingAudioChunk();
                boolean isLast = asrSession.isPendingAudioChunkLast();
                sendAudioOnlyRequest(asrSession, audioChunk, isLast);
                asrSession.clearPendingAudioChunk();
            }
        }

        @Override
        public void onMessage(WebSocket webSocket, String text) {
            logger.info("收到ASR文本消息: {}", text);
            // 转发文本消息给客户端
            sendMessageToClient(asrSession, text);
        }

        @Override
        public void onMessage(WebSocket webSocket, ByteString bytes) {
            byte[] res = bytes.toByteArray();
            int sequence = parserResponse(res);
            boolean isLastPackage = sequence < 0;

            // 如果是最后一个包，关闭连接
            if (isLastPackage) {
                logger.info("收到ASR最后一个响应包，关闭连接");
                webSocket.close(1000, "ASR completed");
                return;
            }

            // 如果有待处理的音频数据，发送给ASR服务
            if (asrSession.hasPendingAudioChunk()) {
                byte[] audioChunk = asrSession.getPendingAudioChunk();
                boolean isLast = asrSession.isPendingAudioChunkLast();
                sendAudioOnlyRequest(asrSession, audioChunk, isLast);
                asrSession.clearPendingAudioChunk();
            }
        }

        @Override
        public void onClosing(WebSocket webSocket, int code, String reason) {
            logger.info("ASR WebSocket正在关闭: code={}, reason={}", code, reason);
        }

        @Override
        public void onClosed(WebSocket webSocket, int code, String reason) {
            logger.info("ASR WebSocket已关闭: code={}, reason={}", code, reason);
            // 如果客户端会话仍然存在，通知客户端ASR会话已结束
            if (asrSession.getClientSession().isOpen()) {
                sendMessageToClient(asrSession, "{\"status\":\"completed\",\"message\":\"ASR session ended\"}");
            }
        }

        @Override
        public void onFailure(WebSocket webSocket, Throwable t, Response response) {
            logger.error("ASR WebSocket连接失败: {}", t.getMessage(), t);
            // 通知客户端发生错误
            sendMessageToClient(asrSession, "{\"status\":\"error\",\"message\":\"ASR connection failed\"}");
        }
    }

    /**
     * 向客户端发送消息
     */
    private void sendMessageToClient(AsrSession asrSession, String message) {
        WebSocketSession clientSession = asrSession.getClientSession();
        if (clientSession != null && clientSession.isOpen()) {
            try {
                clientSession.sendMessage(new TextMessage(message));
            } catch (IOException e) {
                logger.error("向客户端发送消息失败", e);
            }
        }
    }

    /**
     * 解析ASR响应
     */
    private int parserResponse(byte[] res) {
        if (res == null || res.length == 0) {
            return -1;
        }

        // 当符号位为1时进行 >> 运算后高位补1（预期是补0），导致结果错误，所以增加个数再与其& 运算，目的是确保高位是补0
        final byte num = 0b00001111;
        Map<String, Object> result = new HashMap<>();
        
        // 解析header (32 bit = 4 byte)
        int protocolVersion = (res[0] >> 4) & num;
        result.put("protocol_version", protocolVersion);
        int headerSize = res[0] & 0x0f;
        result.put("header_size", headerSize);

        int messageType = (res[1] >> 4) & num;
        result.put("message_type", messageType);
        int messageTypeSpecificFlags = res[1] & 0x0f;
        result.put("message_type_specific_flags", messageTypeSpecificFlags);
        int serializationMethod = res[2] >> num;
        result.put("serialization_method", serializationMethod);
        int messageCompression = res[2] & 0x0f;
        result.put("message_compression", messageCompression);
        int reserved = res[3];
        result.put("reserved", reserved);

        // 解析sequence (4 byte)
        byte[] temp = new byte[4];
        System.arraycopy(res, 4, temp, 0, temp.length);
        int sequence = bytesToInt(temp);

        // 解析payload size (4 byte)
        String payloadStr = null;
        System.arraycopy(res, 8, temp, 0, temp.length);
        int payloadSize = bytesToInt(temp);
        byte[] payload = new byte[res.length - 12];
        System.arraycopy(res, 12, payload, 0, payload.length);

        // 处理不同类型的响应
        if (messageType == FULL_SERVER_RESPONSE) {
            if (messageCompression == GZIP) {
                payloadStr = new String(gzipDecompress(payload));
            } else {
                payloadStr = new String(payload);
            }
            logger.debug("ASR响应payload: {}", payloadStr);
            result.put("payload_size", payloadSize);
            logger.debug("ASR响应: {}", new Gson().toJson(result));

        } else if (messageType == SERVER_ACK) {
            payloadStr = new String(payload);
            logger.debug("ASR确认payload: {}", payloadStr);
            result.put("payload_size", payloadSize);
            logger.debug("ASR确认: {}", new Gson().toJson(result));

        } else if (messageType == SERVER_ERROR_RESPONSE) {
            // 此时sequence含义是错误码code，payload是error msg
            payloadStr = new String(payload);
            result.put("code", sequence);
            result.put("error_msg", payloadStr);
            logger.error("ASR错误响应: {}", new Gson().toJson(result));
        }

        return sequence;
    }

    /**
     * 构建协议头
     */
    private byte[] getHeader(byte messageType, byte messageTypeSpecificFlags, byte serialMethod, byte compressionType,
                             byte reservedData) {
        final byte[] header = new byte[4];
        header[0] = (PROTOCOL_VERSION << 4) | DEFAULT_HEADER_SIZE; // Protocol version|header size
        header[1] = (byte) ((messageType << 4) | messageTypeSpecificFlags); // message type | messageTypeSpecificFlags
        header[2] = (byte) ((serialMethod << 4) | compressionType);
        header[3] = reservedData;
        return header;
    }

    /**
     * 整数转字节数组
     */
    private byte[] intToBytes(int a) {
        return new byte[]{
                (byte) ((a >> 24) & 0xFF),
                (byte) ((a >> 16) & 0xFF),
                (byte) ((a >> 8) & 0xFF),
                (byte) (a & 0xFF)
        };
    }

    /**
     * 字节数组转整数
     */
    private int bytesToInt(byte[] src) {
        if (src == null || (src.length != 4)) {
            throw new IllegalArgumentException("Invalid byte array");
        }
        return ((src[0] & 0xFF) << 24)
                | ((src[1] & 0xff) << 16)
                | ((src[2] & 0xff) << 8)
                | ((src[3] & 0xff));
    }

    /**
     * 生成序列号字节数组
     */
    private byte[] generateBeforPayload(int seq) {
        return intToBytes(seq);
    }

    /**
     * GZIP压缩
     */
    private byte[] gzipCompress(byte[] src) {
        return gzipCompress(src, src.length);
    }

    /**
     * GZIP压缩指定长度
     */
    private byte[] gzipCompress(byte[] src, int len) {
        if (src == null || len == 0) {
            return new byte[0];
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPOutputStream gzip = null;
        try {
            gzip = new GZIPOutputStream(out);
            gzip.write(src, 0, len);
        } catch (IOException e) {
            logger.error("GZIP压缩失败", e);
        } finally {
            if (gzip != null) {
                try {
                    gzip.close();
                } catch (IOException e) {
                    logger.error("关闭GZIP流失败", e);
                }
            }
        }
        return out.toByteArray();
    }

    /**
     * GZIP解压缩
     */
    private byte[] gzipDecompress(byte[] src) {
        if (src == null || src.length == 0) {
            return null;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayInputStream ins = new ByteArrayInputStream(src);
        GZIPInputStream gzip = null;
        try {
            gzip = new GZIPInputStream(ins);
            byte[] buffer = new byte[ins.available()];
            int len = 0;
            while ((len = gzip.read(buffer)) > 0) {
                out.write(buffer, 0, len);
            }
            out.close();
        } catch (IOException e) {
            logger.error("GZIP解压缩失败", e);
        } finally {
            if (gzip != null) {
                try {
                    gzip.close();
                } catch (IOException e) {
                    logger.error("关闭GZIP流失败", e);
                }
            }
        }
        return out.toByteArray();
    }

    /**
     * ASR会话类，用于管理与ASR服务的连接和状态
     */
    private static class AsrSession {
        private final WebSocketSession clientSession;
        private WebSocket asrWebSocket;
        private int seq = 0;
        private boolean initialized = false;
        private boolean ending = false;
        private byte[] pendingAudioChunk;
        private boolean pendingAudioChunkLast;

        public AsrSession(WebSocketSession clientSession) {
            this.clientSession = clientSession;
        }

        public WebSocketSession getClientSession() {
            return clientSession;
        }

        public String getClientSessionId() {
            return clientSession.getId();
        }

        public WebSocket getAsrWebSocket() {
            return asrWebSocket;
        }

        public void setAsrWebSocket(WebSocket asrWebSocket) {
            this.asrWebSocket = asrWebSocket;
        }

        public int getSeq() {
            return seq;
        }

        public void setSeq(int seq) {
            this.seq = seq;
        }

        public int incrementSeq() {
            return ++seq;
        }

        public boolean isInitialized() {
            return initialized;
        }

        public void setInitialized(boolean initialized) {
            this.initialized = initialized;
        }

        public boolean isEnding() {
            return ending;
        }

        public void setEnding(boolean ending) {
            this.ending = ending;
        }

        public void setPendingAudioChunk(byte[] audioChunk, boolean isLast) {
            this.pendingAudioChunk = audioChunk;
            this.pendingAudioChunkLast = isLast;
        }

        public byte[] getPendingAudioChunk() {
            return pendingAudioChunk;
        }

        public boolean isPendingAudioChunkLast() {
            return pendingAudioChunkLast;
        }

        public boolean hasPendingAudioChunk() {
            return pendingAudioChunk != null;
        }

        public void clearPendingAudioChunk() {
            pendingAudioChunk = null;
        }
    }
}
