package com.example.asrwebsocket.service;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import javax.sound.sampled.AudioFormat;
import javax.sound.sampled.AudioFormat.Encoding;
import javax.sound.sampled.AudioInputStream;
import javax.sound.sampled.AudioSystem;
import javax.sound.sampled.Clip;
import javax.sound.sampled.LineUnavailableException;
import javax.sound.sampled.UnsupportedAudioFileException;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;
import okhttp3.internal.ws.RealWebSocket;
import okhttp3.logging.HttpLoggingInterceptor;
import okhttp3.logging.HttpLoggingInterceptor.Level;
import okio.ByteString;
import ws.schild.jave.EncoderException;
import ws.schild.jave.MultimediaObject;
import ws.schild.jave.info.AudioInfo;

/**
 * Author：censhengde on 2024/11/19 15:39
 *
 * explain：<a href="https://www.volcengine.com/docs/6561/1354869">大模型流式语音识别API</a>
 */
public class BigASRWebsocketDemo {

    private static final byte PROTOCOL_VERSION = 0b0001;
    private static final byte DEFAULT_HEADER_SIZE = 0b0001;
    // Message Type:
    private static final byte FULL_CLIENT_REQUEST = 0b0001;
    private static final byte AUDIO_ONLY_REQUEST = 0b0010;
    private static final byte FULL_SERVER_RESPONSE = 0b1001;
    private static final byte SERVER_ACK = 0b1011;
    private static final byte SERVER_ERROR_RESPONSE = 0b1111;
    // Message Type Specific Flags
    private static final byte NO_SEQUENCE = 0b0000;// no check sequence
    private static final byte POS_SEQUENCE = 0b0001;
    private static final byte NEG_SEQUENCE = 0b0010;
    private static final byte NEG_WITH_SEQUENCE = 0b0011;
    private static final byte NEG_SEQUENCE_1 = 0b0011;
    // Message Serialization
    private static final byte NO_SERIALIZATION = 0b0000;
    private static final byte JSON = 0b0001;

    // Message Compression
    private static final byte NO_COMPRESSION = 0b0000;
    private static final byte GZIP = 0b0001;

    public static void main(String[] args) throws Exception {

        final String url = "wss://openspeech.bytedance.com/api/v3/sauc/bigmodel";
        final String appId = "3467491207";
        final String token = "kPHQMVliZeyYh2fWkiOC-e9eXIeZhCN8";
        // 当前仅支持单声道音频
        final String audioFilePath = "d:\\Cursor\\JProject\\asr\\16k16bit.pcm";

        // 解析音频元数据（需要文件读写权限）。
        final AudioInputStream ins;
        AudioFormat format;
        try {
            // 手动指定PCM格式参数
            format = new AudioFormat(16000, 16, 1, true, false); // 16kHz, 16bit, 单声道
            InputStream inputStream = new FileInputStream(audioFilePath);
            ins = new AudioInputStream(inputStream, format, new File(audioFilePath).length()/format.getFrameSize());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        final Request request = new Request.Builder()
                .url(url)
                .header("X-Api-App-Key", appId)
                .header("X-Api-Access-Key", token)
                .header("X-Api-Resource-Id", "volc.bigasr.sauc.duration")
                .header("X-Api-Connect-Id", UUID.randomUUID().toString())
                .build();

        HttpLoggingInterceptor loggingInterceptor = new HttpLoggingInterceptor();
        loggingInterceptor.setLevel(Level.HEADERS);
        final OkHttpClient okHttpClient = new OkHttpClient.Builder().pingInterval(50, TimeUnit.SECONDS)
                .addInterceptor(loggingInterceptor)
                .readTimeout(100, TimeUnit.SECONDS)
                .writeTimeout(100, TimeUnit.SECONDS)
                .build();

        okHttpClient.newWebSocket(request, new WebSocketListener() {
            byte[] buffer;
            int bufferSize;
            int seq;
            int lastSeq;


            @SuppressWarnings("[ByDesign12.1]UsingRuntimeExec")
            @Override
            public void onOpen(WebSocket webSocket, Response response) {
                String logId = response.header("X-Tt-Logid");
                System.out.println("===> onOpen,X-Tt-Logid:" + logId);

                // send full client request
                // step 1: append payload json string
                JsonObject user = new JsonObject();
                user.addProperty("uid", "test");

                JsonObject audio = new JsonObject();
                audio.addProperty("format", "pcm"); //
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
                System.out.println(payloadStr);
                // step2: 压缩 payload 字段。
                final byte[] payloadBytes = gzipCompress(payloadStr.getBytes());
                // step3:组装 fullClientRequest；fullClientRequest= header+ sequence + payload
                byte[] header = getHeader(FULL_CLIENT_REQUEST, POS_SEQUENCE, JSON, GZIP, (byte) 0);
                final byte[] payloadSize = intToBytes(payloadBytes.length);
                seq = 1;
                byte[] seqBytes = generateBeforPayload(seq);
                final byte[] fullClientRequest = new byte[header.length + seqBytes.length + payloadSize.length
                        + payloadBytes.length];
                int destPos = 0;
                System.arraycopy(header, 0, fullClientRequest, destPos, header.length);
                destPos += header.length;
                System.arraycopy(seqBytes, 0, fullClientRequest, destPos, seqBytes.length);
                destPos += seqBytes.length;
                System.arraycopy(payloadSize, 0, fullClientRequest, destPos, payloadSize.length);
                destPos += payloadSize.length;
                System.arraycopy(payloadBytes, 0, fullClientRequest, destPos, payloadBytes.length);
                boolean suc = webSocket.send(ByteString.of(fullClientRequest));
                if (!suc) {
                    return;
                }
                AudioFormat format = ins.getFormat();
                // 一次性传输的帧数可视内存及网络承载能力决定，不唯一。
                int frames = (int) Math.min(ins.getFrameLength(), ins.getFrameLength() / 10);// 切成10 段。
                bufferSize = (format.getSampleSizeInBits() / Byte.SIZE) * format.getChannels() * frames;
                buffer = new byte[bufferSize];

            }


            @Override
            public void onMessage(WebSocket webSocket, String text) {
                super.onMessage(webSocket, text);
                System.out.println("===> onMessage： text:" + text);
            }

            @Override
            public void onMessage(WebSocket webSocket, ByteString bytes) {
                byte[] res = bytes.toByteArray();
                int sequence = parserResponse(res);
                boolean is_last_package = sequence < 0;
                if (is_last_package) {
                    System.out.println("===>退出程序");
                    webSocket.close(1000, "finished");
                    System.exit(0);
                    return;
                }
                    // send audio only request
                    try {
                            final int len = ins.read(buffer, 0, bufferSize);
                            if (len <= 0) {
                                System.out.println("===>read len <= 0,exit");
                                return;
                            }
                            boolean isLast = ins.available() == 0;
                            System.out.println("===> read end:" + isLast + " available:" + ins.available());
                            sendAudioOnlyRequest(webSocket, buffer, len, isLast);
                            if (isLast) {
                                ins.close();
                            }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
            }


            // audio_only_request= header + sequence + payload size+ payload
            boolean sendAudioOnlyRequest(WebSocket webSocket, byte[] buffer, int len, boolean isLast) {
                seq++;
                System.out.println("seq:" + seq);
                if (isLast) {
                    seq = -seq;
                }
                byte messageTypeSpecificFlags = isLast ? NEG_WITH_SEQUENCE : POS_SEQUENCE;
                // header
                byte[] header = getHeader(AUDIO_ONLY_REQUEST, messageTypeSpecificFlags, JSON, GZIP, (byte) 0);
                // sequence
                byte[] sequenceBytes = generateBeforPayload(seq);
                // payload size
                byte[] payloadBytes = gzipCompress(buffer, len);
                // payload
                byte[] payloadSize = intToBytes(payloadBytes.length);
                byte[] audio_only_request = new byte[header.length + sequenceBytes.length + payloadSize.length
                        + payloadBytes.length];
                int destPos = 0;
                System.arraycopy(header, 0, audio_only_request, destPos, header.length);
                destPos += header.length;
                System.arraycopy(sequenceBytes, 0, audio_only_request, destPos, sequenceBytes.length);
                destPos += sequenceBytes.length;
                System.arraycopy(payloadSize, 0, audio_only_request, destPos, payloadSize.length);
                destPos += payloadSize.length;
                System.arraycopy(payloadBytes, 0, audio_only_request, destPos, payloadBytes.length);
                return webSocket.send(ByteString.of(audio_only_request));
            }

            @Override
            public void onClosing(WebSocket webSocket, int code, String reason) {
                super.onClosing(webSocket, code, reason);
                System.out.println("===> onClosing： code:" + code + " reason:" + reason);
            }

            @Override
            public void onClosed(WebSocket webSocket, int code, String reason) {
                super.onClosed(webSocket, code, reason);
                System.out.println("===> onClosed： code:" + code + " reason:" + reason);
            }

            @Override
            public void onFailure(WebSocket webSocket, Throwable t, Response response) {
                super.onFailure(webSocket, t, response);
                System.out.println(
                        "===> onFailure： Throwable:" + t.getMessage() + " Response:" + (response == null ? "null"
                                : response.toString()));
                System.exit(0);
            }
        });
        // 保活进程，非业务代码
        synchronized (Thread.currentThread()) {
            try {
                Thread.currentThread().wait();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    static byte[] getHeader(byte messageType, byte messageTypeSpecificFlags, byte serialMethod, byte compressionType,
            byte reservedData) {
        final byte[] header = new byte[4];
        header[0] = (PROTOCOL_VERSION << 4) | DEFAULT_HEADER_SIZE; // Protocol version|header size
        header[1] = (byte) ((messageType << 4) | messageTypeSpecificFlags); // message type | messageTypeSpecificFlags
        header[2] = (byte) ((serialMethod << 4) | compressionType);
        header[3] = reservedData;
        return header;
    }

    static byte[] intToBytes(int a) {
        return new byte[]{
                (byte) ((a >> 24) & 0xFF),
                (byte) ((a >> 16) & 0xFF),
                (byte) ((a >> 8) & 0xFF),
                (byte) (a & 0xFF)

        };
    }

    static int bytesToInt(byte[] src) {
        if (src == null || (src.length != 4)) {
            throw new IllegalArgumentException("");
        }
        return ((src[0] & 0xFF) << 24)
                | ((src[1] & 0xff) << 16)
                | ((src[2] & 0xff) << 8)
                | ((src[3] & 0xff));
    }

    static byte[] generateBeforPayload(int seq) {
        return intToBytes(seq);
    }

    static byte[] gzipCompress(byte[] src) {
        return gzipCompress(src, src.length);
    }

    static byte[] gzipCompress(byte[] src, int len) {
        if (src == null || len == 0) {
            return new byte[0];
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPOutputStream gzip = null;
        try {
            gzip = new GZIPOutputStream(out);
            gzip.write(src, 0, len);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (gzip != null) {
                try {
                    gzip.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return out.toByteArray();
    }

    static byte[] gzipDecompress(byte[] src) {
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
            e.printStackTrace();
        } finally {
            if (gzip != null) {
                try {
                    gzip.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return out.toByteArray();
    }


    static int parserResponse(byte[] res) {
        if (res == null || res.length == 0) {
            return -1;
        }
        // 当符号位为1时进行 >> 运算后高位补1（预期是补0），导致结果错误，所以增加个数再与其& 运算，目的是确保高位是补0.
        final byte num = 0b00001111;
        Map<String, Object> result = new HashMap<>();
        // header 32 bit=4 byte
        int protocol_version = (res[0] >> 4) & num;
        result.put("protocol_version", protocol_version);
        int header_size = res[0] & 0x0f;
        result.put("header_size", header_size);

        int message_type = (res[1] >> 4) & num;
        result.put("message_type", message_type);
        int message_type_specific_flags = res[1] & 0x0f;
        result.put("message_type_specific_flags", message_type_specific_flags);
        int serialization_method = res[2] >> num;
        result.put("serialization_method", serialization_method);
        int message_compression = res[2] & 0x0f;
        result.put("message_compression", message_compression);
        int reserved = res[3];
        result.put("reserved", reserved);

        // sequence 4 byte
        byte[] temp = new byte[4];
        System.arraycopy(res, 4, temp, 0, temp.length);
        int sequence = bytesToInt(temp);// sequence 4 byte

        // payload size 4 byte
        String payloadStr = null;
        System.arraycopy(res, 8, temp, 0, temp.length);
        int payloadSize = bytesToInt(temp);
        byte[] payload = new byte[res.length - 12];
        System.arraycopy(res, 12, payload, 0, payload.length);
        // 正常Response
        if (message_type == FULL_SERVER_RESPONSE) {
            if (message_compression == GZIP) {
                payloadStr = new String(gzipDecompress(payload));
            } else {
                payloadStr = new String(payload);
            }
            System.out.println("===>payload:" + payloadStr);
            result.put("payload_size", payloadSize);
            System.out.println("===>response:" + new Gson().toJson(result));

        } else if (message_type == SERVER_ACK) {
            payloadStr = new String(payload);
            System.out.println("===>payload:" + payloadStr);
            result.put("payload_size", payloadSize);
            System.out.println("===>response:" + new Gson().toJson(result));

        } else if (message_type == SERVER_ERROR_RESPONSE) {
            // 此时 sequence 含义就是 错误码 code，payload 就是 error msg。
            payloadStr = new String(payload);
            result.put("code", sequence);
            result.put("error msg", payloadStr);
            System.out.println("===>response:" + new Gson().toJson(result));

        }
        return sequence;
    }


}
