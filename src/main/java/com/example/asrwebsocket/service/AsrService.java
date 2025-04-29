package com.example.asrwebsocket.service;

import org.springframework.web.socket.WebSocketSession;

/**
 * 语音识别服务接口
 * 负责处理与火山引擎ASR的通信，实时接收用户语音并返回识别文本
 */
public interface AsrService {

    /**
     * 启动ASR会话
     * 当客户端WebSocket连接建立时调用，初始化与火山引擎ASR的连接
     *
     * @param session 客户端WebSocket会话
     */
    void startAsrSession(WebSocketSession session);

    /**
     * 处理音频数据块
     * 将客户端发送的音频数据转发给火山引擎ASR进行识别
     *
     * @param sessionId 客户端会话ID
     * @param audioChunk 音频数据块
     * @param isLast 是否为最后一块数据
     */
    void processAudioChunk(String sessionId, byte[] audioChunk, boolean isLast);

    /**
     * 清理会话资源
     * 当客户端连接关闭或发生错误时调用，释放相关资源
     *
     * @param sessionId 客户端会话ID
     */
    void cleanupSession(String sessionId);
}