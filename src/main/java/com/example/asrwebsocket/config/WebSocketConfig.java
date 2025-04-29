package com.example.asrwebsocket.config;

import com.example.asrwebsocket.handler.AudioWebSocketHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

@Configuration
@EnableWebSocket // 启用 WebSocket 支持
public class WebSocketConfig implements WebSocketConfigurer {

    @Autowired
    private AudioWebSocketHandler audioWebSocketHandler; // 注入我们创建的处理器

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        // 注册处理器，并将其映射到路径 "/asr"
        // setAllowedOrigins("*") 允许所有来源的连接，生产环境中应配置具体的来源
        registry.addHandler(audioWebSocketHandler, "/asr")
                .setAllowedOrigins("*");
    }
}