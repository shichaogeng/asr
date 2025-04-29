package com.example.asrwebsocket.handler;

import com.example.asrwebsocket.service.AsrService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.*;
import org.springframework.web.socket.handler.AbstractWebSocketHandler;

import java.io.IOException;
import java.nio.ByteBuffer;

@Component
public class AudioWebSocketHandler extends AbstractWebSocketHandler {

    private static final Logger logger = LoggerFactory.getLogger(AudioWebSocketHandler.class);

    @Autowired
    private AsrService asrService;

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        logger.info("Client WebSocket connected: {}", session.getId());
        // 当客户端连接建立时，启动与火山引擎 ASR 的连接
        asrService.startAsrSession(session);
    }

    @Override
    protected void handleBinaryMessage(WebSocketSession session, BinaryMessage message) throws Exception {
        String sessionId = session.getId();
        ByteBuffer payload = message.getPayload();
        byte[] audioChunk = new byte[payload.remaining()];
        payload.get(audioChunk);
        logger.debug("Received audio chunk from client session {}: size={}", sessionId, audioChunk.length);
        // 将收到的音频块转发给 ASR 服务处理，标记为非最后一块
        asrService.processAudioChunk(sessionId, audioChunk, false);
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        String sessionId = session.getId();
        String payload = message.getPayload();
        logger.info("Received text message from client session {}: {}", sessionId, payload);

        // 约定一个特殊的文本消息来表示音频流结束
        if ("END_STREAM".equalsIgnoreCase(payload)) {
            logger.info("Client session {} signaled end of stream.", sessionId);
            // 发送一个空的音频块，并标记为最后一块，以通知 ASR 服务结束
            asrService.processAudioChunk(sessionId, new byte[0], true);
        } else {
            // 可以处理其他控制消息或忽略
            logger.warn("Received unexpected text message from client session {}: {}", sessionId, payload);
            // 可以选择向客户端发送错误或确认信息
            // session.sendMessage(new TextMessage("Unsupported command: " + payload));
        }
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) throws Exception {
        logger.error("Client WebSocket transport error for session {}: {}", session.getId(), exception.getMessage(), exception);
        // 发生传输错误时，清理会话资源
        asrService.cleanupSession(session.getId());
        // 确保会话关闭
        if (session.isOpen()) {
            session.close(CloseStatus.SERVER_ERROR.withReason("Transport error: " + exception.getMessage()));
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
        logger.info("Client WebSocket closed: {} with status {}", session.getId(), status);
        // 客户端连接关闭时，清理会话资源
        // 注意：如果客户端主动发送 END_STREAM，可能已经触发了 ASR 的结束流程
        // cleanupSession 会确保 ASR 连接也被关闭
        asrService.cleanupSession(session.getId());
    }

    @Override
    public boolean supportsPartialMessages() {
        // 根据需要决定是否支持部分消息，对于音频流通常不需要
        return false;
    }
}