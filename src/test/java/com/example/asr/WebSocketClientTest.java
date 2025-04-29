package com.example.asr;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.json.JSONObject;
import org.json.JSONException;
import org.junit.jupiter.api.Test;

public class WebSocketClientTest {

    private static final String SERVER_URL = "ws://localhost:8080/asr";
    // 修改为绝对路径
    private static final String PCM_FILE_PATH = "d:/Cursor/JProject/asr/16k16bit.pcm";
    private static final int CHUNK_SIZE = 3200; // 每次发送100ms的音频数据 (16000 * 2 / 10)
    private static final int SEND_INTERVAL_MS = 100; // 每100ms发送一次数据

    @Test
    public void testAsrWebSocket() throws URISyntaxException, InterruptedException {
        // 创建一个CountDownLatch来等待WebSocket连接关闭
        CountDownLatch latch = new CountDownLatch(1);
        
        // 创建WebSocket客户端
        AsrWebSocketClient client = new AsrWebSocketClient(new URI(SERVER_URL), latch);
        
        // 连接到服务器
        client.connect();
        
        // 等待连接关闭或超时
        boolean completed = latch.await(60, TimeUnit.SECONDS);
        
        if (!completed) {
            System.out.println("测试超时");
            client.close();
        }
    }
    
    private class AsrWebSocketClient extends WebSocketClient {
        private final CountDownLatch latch;
        private boolean initialized = false;
        private int sequence = 1;
        
        public AsrWebSocketClient(URI serverUri, CountDownLatch latch) {
            super(serverUri);
            this.latch = latch;
        }
        
        @Override
        public void onOpen(ServerHandshake handshakedata) {
            try {
                System.out.println("连接已建立");
                
                // 发送初始化请求，确保序列号与服务器期望的一致
                JSONObject initRequest = new JSONObject();
                initRequest.put("type", "init");
                // 移除sequence字段，让服务器自动分配
                
                initRequest.put("format", "pcm");
                initRequest.put("sampleRate", 16000);
                initRequest.put("bits", 16);
                initRequest.put("channel", 1);
                
                System.out.println("发送初始化请求: " + initRequest.toString());
                send(initRequest.toString());
                
                initialized = true;
                
                // 在单独的线程中发送音频数据
                new Thread(this::sendAudioData).start();
            } catch (JSONException e) {
                e.printStackTrace();
                latch.countDown();
            }
        }
        
        private void sendAudioData() {
            // 等待初始化完成
            while (!initialized) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    return;
                }
            }
            
            System.out.println("开始发送音频数据...");
            
            File audioFile = new File(PCM_FILE_PATH);
            if (!audioFile.exists()) {
                System.err.println("找不到音频文件: " + audioFile.getAbsolutePath());
                latch.countDown();
                return;
            }
            
            try (FileInputStream fis = new FileInputStream(audioFile)) {
                byte[] buffer = new byte[CHUNK_SIZE];
                int bytesRead;
                
                while ((bytesRead = fis.read(buffer)) != -1) {
                    if (bytesRead > 0) {
                        // 如果读取的字节数小于缓冲区大小，创建一个新的缓冲区
                        byte[] dataToSend = bytesRead < CHUNK_SIZE ? 
                                java.util.Arrays.copyOf(buffer, bytesRead) : buffer;
                        
                        // 直接发送二进制音频数据
                        send(dataToSend);
                        
                        Thread.sleep(SEND_INTERVAL_MS);
                    }
                }
                
                // 发送结束信号
                System.out.println("音频数据发送完毕，发送结束信号");
                send("END_STREAM");
                
                // 等待一段时间接收最终结果
                Thread.sleep(2000);
                
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
                latch.countDown();
            }
        }
        
        @Override
        public void onMessage(String message) {
            System.out.println("收到文本消息: " + message);
        }
        
        @Override
        public void onMessage(ByteBuffer data) {
            System.out.println("收到二进制消息，长度: " + data.remaining());
        }
        
        @Override
        public void onClose(int code, String reason, boolean remote) {
            System.out.println("连接已关闭: " + reason);
            latch.countDown();
        }
        
        @Override
        public void onError(Exception ex) {
            System.err.println("发生错误: " + ex.getMessage());
            ex.printStackTrace();
            latch.countDown();
        }
    }
}