package com.example.asrwebsocket.handler;

// RealTimeASRClient.java (Refactored)
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import okhttp3.*;
import okhttp3.logging.HttpLoggingInterceptor;
import okio.ByteString;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ByteArrayInputStream; // Needed for Gzip Decompress
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.concurrent.atomic.AtomicBoolean;

public class RealTimeASRClient implements AutoCloseable {

    // --- Constants (Keep relevant protocol constants) ---
    private static final byte PROTOCOL_VERSION = 0b0001;
    private static final byte DEFAULT_HEADER_SIZE = 0b0001;
    private static final byte FULL_CLIENT_REQUEST = 0b0001;
    private static final byte AUDIO_ONLY_REQUEST = 0b0010;
    private static final byte FULL_SERVER_RESPONSE = 0b1001;
    private static final byte SERVER_ACK = 0b1011;
    private static final byte SERVER_ERROR_RESPONSE = 0b1111;
    private static final byte POS_SEQUENCE = 0b0001;
    private static final byte NEG_WITH_SEQUENCE = 0b0011;
    private static final byte JSON = 0b0001;
    private static final byte GZIP = 0b0001;
    // --- End Constants ---

    private static final String ASR_URL = "wss://openspeech.bytedance.com/api/v3/sauc/bigmodel";
    private static final String RESOURCE_ID = "volc.bigasr.sauc.duration";

    private final OkHttpClient httpClient;
    private WebSocket webSocket;
    private int sequence = 0;
    private final String connectionId;
    private final Gson gson = new Gson();
    private volatile boolean isVolcanoEngineConnectionOpen = false;
    private AtomicBoolean finishedSending = new AtomicBoolean(false);
    private final ASRResultListener listener;
    private final String appId;
    private final String token;
    private final AudioConfig audioConfig; // Store audio config

    // Configuration class
    public static class AudioConfig {
        final String format;
        final int sampleRate;
        final int bits;
        final int channels;
        final String codec;

        public AudioConfig(String format, int sampleRate, int bits, int channels, String codec) {
            this.format = format;
            this.sampleRate = sampleRate;
            this.bits = bits;
            this.channels = channels;
            this.codec = codec;
        }
    }

    public RealTimeASRClient(String appId, String token, AudioConfig audioConfig, ASRResultListener listener) {
        this.appId = appId;
        this.token = token;
        this.audioConfig = audioConfig; // Store config
        this.listener = listener;

        // Consider making HttpClient instance shared/static if creating many clients
        HttpLoggingInterceptor loggingInterceptor = new HttpLoggingInterceptor(System.out::println);
        loggingInterceptor.setLevel(HttpLoggingInterceptor.Level.NONE); // Reduce logging noise, maybe HEADERS for debug

        this.httpClient = new OkHttpClient.Builder()
                .pingInterval(30, TimeUnit.SECONDS)
                .addInterceptor(loggingInterceptor)
                .readTimeout(60, TimeUnit.SECONDS)
                .writeTimeout(60, TimeUnit.SECONDS)
                .connectTimeout(30, TimeUnit.SECONDS)
                .build();
        this.connectionId = UUID.randomUUID().toString();
    }

    public void connect() {
        if (webSocket != null || isVolcanoEngineConnectionOpen) {
            System.out.println("Already connected or connecting.");
            return;
        }
         if (appId == null || appId.isEmpty() || token == null || token.isEmpty()) {
             listener.onError(-1, "App ID or Token is missing.");
             return;
         }

        Request request = new Request.Builder()
                .url(ASR_URL)
                .header("X-Api-App-Key", appId)
                .header("X-Api-Access-Key", token)
                .header("X-Api-Resource-Id", RESOURCE_ID)
                .header("X-Api-Connect-Id", connectionId)
                .build();

        webSocket = httpClient.newWebSocket(request, new ASRWebSocketListener());
        System.out.println("Requesting WebSocket connection to Volcano Engine...");
    }

    public boolean isConnected() {
        return isVolcanoEngineConnectionOpen && webSocket != null;
    }

     /**
      * Call this method to send an audio chunk.
      * Should only be called after the connection is open (check isConnected() or wait for onVolcanoEngineConnectionOpen callback).
      * @param audioChunk The raw audio data.
      */
     public void sendAudio(byte[] audioChunk) {
         if (!isConnected()) {
             System.err.println("Cannot send audio: Not connected to Volcano Engine.");
             // Optionally notify listener: listener.onError(-1, "Cannot send audio: Not connected.");
             return;
         }
         if (finishedSending.get()) {
              System.err.println("Cannot send audio: Already marked as finished sending.");
             return;
         }
         sendAudioChunkInternal(audioChunk, false);
     }

     /**
      * Call this when you have sent the last audio chunk.
      */
     public void signalEndOfAudio() {
         if (!isConnected()) {
              System.err.println("Cannot signal end: Not connected to Volcano Engine.");
             return;
         }
         if (finishedSending.compareAndSet(false, true)) {
            System.out.println("Signaling end of audio stream to Volcano Engine...");
            sendAudioChunkInternal(new byte[0], true); // Send empty packet with end flag
         } else {
             System.out.println("End of audio already signaled.");
         }
     }


    private class ASRWebSocketListener extends WebSocketListener {
        @Override
        public void onOpen(WebSocket ws, Response response) {
            System.out.println("Volcano Engine WebSocket Opened. Log ID: " + response.header("X-Tt-Logid"));
            isVolcanoEngineConnectionOpen = true;
            // Send initial config request
            sendInitialRequest(ws);
            // Notify listener that connection is ready
             if(listener != null) {
                 listener.onVolcanoEngineConnectionOpen();
             }
        }

        @Override
        public void onMessage(WebSocket webSocket, String text) {
            System.out.println("Volcano Engine Received Text: " + text); // Should not happen in normal protocol
        }

        @Override
        public void onMessage(WebSocket webSocket, ByteString bytes) {
            Map<String, Object> parsedResponse = parseResponse(bytes.toByteArray());
            int messageType = (int) parsedResponse.getOrDefault("message_type", -1);
            int receivedSequence = (int) parsedResponse.getOrDefault("sequence", 0);
            String payloadStr = (String) parsedResponse.getOrDefault("payload_string", "");
             String errorMessage = (String) parsedResponse.getOrDefault("error_message", "");

            // System.out.println("Volcano Engine Received Binary: Type=" + messageType + ", Seq=" + receivedSequence); // Debug

            if (messageType == SERVER_ERROR_RESPONSE) {
                 System.err.println("Volcano Engine Error: Code=" + receivedSequence + ", Msg=" + errorMessage);
                if (listener != null) {
                    listener.onError(receivedSequence, errorMessage);
                }
                // Consider closing on error? Or let the user decide?
                // closeWebSocket(1011, "Server error received");
                return;
            }

            if (messageType == FULL_SERVER_RESPONSE && !payloadStr.isEmpty()) {
                // System.out.println("  Payload: " + payloadStr); // Debug
                if (listener != null) {
                    listener.onResult(payloadStr);
                }
            } else if (messageType == SERVER_ACK) {
                 System.out.println("Volcano Engine Server ACK received (Seq: " + receivedSequence + ")");
                 // ACK might contain task info, handle if needed
                 if (listener != null && !payloadStr.isEmpty()) {
                     // Decide if ACKs should be passed as results or handled differently
                     // listener.onResult(payloadStr);
                 }
            }

            // Check if this is the final response marker from the server
            if (receivedSequence < 0) {
                System.out.println("Final server response marker received from Volcano Engine (Seq " + receivedSequence + ").");
                if (listener != null) {
                    listener.onComplete(); // Notify listener that ASR is complete from server perspective
                }
                // Don't close the socket immediately, the containing server might want to reuse it
                // or explicitly close it based on the onComplete callback.
                 // closeWebSocket(1000, "Client finished sending and received final server ack");
            }
        }

        @Override
        public void onClosing(WebSocket webSocket, int code, String reason) {
            System.out.println("Volcano Engine WebSocket Closing: Code=" + code + ", Reason=" + reason);
            isVolcanoEngineConnectionOpen = false;
        }

        @Override
        public void onClosed(WebSocket webSocket, int code, String reason) {
            System.out.println("Volcano Engine WebSocket Closed: Code=" + code + ", Reason=" + reason);
            isVolcanoEngineConnectionOpen = false;
            // Notify listener of unexpected close? Only if not initiated by close()
             if(listener != null) {
                 // listener.onError(code, "Volcano Engine connection closed: " + reason);
             }
        }

        @Override
        public void onFailure(WebSocket webSocket, Throwable t, Response response) {
            String responseInfo = response != null ? ", Response Log ID: " + response.header("X-Tt-Logid") : "";
            System.err.println("Volcano Engine WebSocket Failure: " + t.getMessage() + responseInfo);
            t.printStackTrace(System.err);
            isVolcanoEngineConnectionOpen = false;
            if (listener != null) {
                listener.onError(-1, "Volcano Engine connection failure: " + t.getMessage());
            }
            // Don't auto-close here, let the wrapper handle retries or closure.
        }
    }

    private void sendInitialRequest(WebSocket ws) {
        try {
            JsonObject user = new JsonObject();
            user.addProperty("uid", "user-" + UUID.randomUUID().toString());

            JsonObject audio = new JsonObject();
            audio.addProperty("format", audioConfig.format);
            audio.addProperty("sample_rate", audioConfig.sampleRate);
            audio.addProperty("bits", audioConfig.bits);
            audio.addProperty("channel", audioConfig.channels);
            audio.addProperty("codec", audioConfig.codec);

            JsonObject request = new JsonObject();
            request.addProperty("model_name", "bigmodel");
            request.addProperty("enable_punc", true);
            // Add other parameters if needed

            JsonObject payload = new JsonObject();
            payload.add("user", user);
            payload.add("audio", audio);
            payload.add("request", request);

            String payloadStr = payload.toString();
            // System.out.println("Volcano Engine Initial Payload: " + payloadStr); // Debug

            byte[] payloadBytes = gzipCompress(payloadStr.getBytes());
            sequence = 1;
            byte[] header = getHeader(FULL_CLIENT_REQUEST, POS_SEQUENCE, JSON, GZIP, (byte) 0);
            byte[] seqBytes = intToBytes(sequence);
            byte[] sizeBytes = intToBytes(payloadBytes.length);

            ByteArrayOutputStream messageStream = new ByteArrayOutputStream();
            messageStream.write(header);
            messageStream.write(seqBytes);
            messageStream.write(sizeBytes);
            messageStream.write(payloadBytes);

            boolean sent = ws.send(ByteString.of(messageStream.toByteArray()));
            if (!sent) {
                System.err.println("Failed to send initial request to Volcano Engine.");
                 closeWebSocket(1000, "Failed to send initial request");
                 if (listener != null) listener.onError(-1, "Failed to send initial request to Volcano Engine");
            } else {
                 System.out.println("Initial request sent to Volcano Engine (Sequence: " + sequence + ").");
            }

        } catch (Exception e) {
            System.err.println("Error sending initial request to Volcano Engine: " + e.getMessage());
            e.printStackTrace(System.err);
             closeWebSocket(1011, "Error during initial request");
             if (listener != null) listener.onError(-1, "Client error during initial request: " + e.getMessage());
        }
    }

     // Renamed and made private, called by public sendAudio and signalEndOfAudio
     private void sendAudioChunkInternal(byte[] buffer, boolean isLast) {
         if (webSocket == null || !isVolcanoEngineConnectionOpen) {
             System.err.println("Cannot send audio chunk: Volcano Engine WebSocket not ready.");
             return;
         }

         try {
             sequence++;
             int currentSequence = isLast ? -sequence : sequence;
             byte messageFlag = isLast ? NEG_WITH_SEQUENCE : POS_SEQUENCE;
             byte[] payloadBytes = isLast ? new byte[0] : gzipCompress(buffer, buffer.length); // Compress non-empty chunks
             int payloadSize = payloadBytes.length;

             byte[] header = getHeader(AUDIO_ONLY_REQUEST, messageFlag, JSON, GZIP, (byte) 0);
             byte[] seqBytes = intToBytes(currentSequence);
             byte[] sizeBytes = intToBytes(payloadSize);

             ByteArrayOutputStream messageStream = new ByteArrayOutputStream();
             messageStream.write(header);
             messageStream.write(seqBytes);
             messageStream.write(sizeBytes);
             if (payloadSize > 0) {
                 messageStream.write(payloadBytes);
             }

             boolean sent = webSocket.send(ByteString.of(messageStream.toByteArray()));
             if (!sent) {
                 System.err.println("Failed to send audio chunk to Volcano Engine (Sequence: " + currentSequence + "). Queue full?");
                 // Handle potential backpressure or errors
             } else {
                 // System.out.println("Sent audio chunk/signal to Volcano Engine (Seq: " + currentSequence + ")"); // Debug
             }

         } catch (IOException e) {
             System.err.println("Error sending audio data to Volcano Engine: " + e.getMessage());
             e.printStackTrace(System.err);
              if (listener != null) listener.onError(-1, "Client error sending audio: " + e.getMessage());
             // Consider closing connection on I/O error
              closeWebSocket(1011, "Error processing audio stream");
         }
     }

    // --- Helper Methods (getHeader, intToBytes, bytesToInt, gzipCompress, gzipDecompress, parseResponse) ---
    // Keep these exactly as they were in the previous version of RealTimeASRClient
    // They are essential for the binary protocol.
    // --- Start Helper Methods ---
     private static byte[] getHeader(byte messageType, byte messageTypeSpecificFlags, byte serialMethod, byte compressionType, byte reservedData) {
        final byte[] header = new byte[4];
        header[0] = (PROTOCOL_VERSION << 4) | DEFAULT_HEADER_SIZE;
        header[1] = (byte) ((messageType << 4) | messageTypeSpecificFlags);
        header[2] = (byte) ((serialMethod << 4) | compressionType);
        header[3] = reservedData;
        return header;
    }

    private static byte[] intToBytes(int a) {
        return new byte[]{
                (byte) ((a >> 24) & 0xFF),
                (byte) ((a >> 16) & 0xFF),
                (byte) ((a >> 8) & 0xFF),
                (byte) (a & 0xFF)
        };
    }

    private static int bytesToInt(byte[] src) {
        if (src == null || (src.length != 4)) {
            throw new IllegalArgumentException("Byte array must be exactly 4 bytes long to convert to int.");
        }
        return ((src[0] & 0xFF) << 24)
                | ((src[1] & 0xFF) << 16)
                | ((src[2] & 0xFF) << 8)
                | (src[3] & 0xFF);
    }

    private static byte[] gzipCompress(byte[] src) throws IOException {
        return gzipCompress(src, src.length);
    }

    private static byte[] gzipCompress(byte[] src, int len) throws IOException {
        if (src == null || len == 0) {
            return new byte[0];
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
            gzip.write(src, 0, len);
        }
        return out.toByteArray();
    }

    private static byte[] gzipDecompress(byte[] src) throws IOException {
        if (src == null || src.length == 0) {
            return null;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayInputStream ins = new ByteArrayInputStream(src);
        try (GZIPInputStream gzip = new GZIPInputStream(ins)) {
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = gzip.read(buffer)) > 0) {
                out.write(buffer, 0, bytesRead);
            }
        }
        return out.toByteArray();
    }

    private static Map<String, Object> parseResponse(byte[] res) {
         Map<String, Object> result = new HashMap<>();
         String payloadStr = ""; // Initialize empty
         int messageType = -1; // Default invalid
         int sequence = 0; // Default
         int messageCompression = -1; // Default invalid

         if (res == null || res.length < 12) {
             System.err.println("Volcano Engine Received invalid/short message, length: " + (res == null ? 0 : res.length));
             messageType = SERVER_ERROR_RESPONSE;
             sequence = -1;
             payloadStr = "Invalid message format received from server";
         } else {
             try {
                 messageType = (res[1] >> 4) & 0x0f;
                 messageCompression = res[2] & 0x0f;

                 byte[] seqBytes = new byte[4];
                 System.arraycopy(res, 4, seqBytes, 0, 4);
                 sequence = bytesToInt(seqBytes);

                 byte[] sizeBytes = new byte[4];
                 System.arraycopy(res, 8, sizeBytes, 0, 4);
                 int payloadSize = bytesToInt(sizeBytes);
                 result.put("payload_size", payloadSize); // Store size regardless

                 if (res.length - 12 >= payloadSize && payloadSize > 0) {
                     byte[] payload = new byte[payloadSize];
                     System.arraycopy(res, 12, payload, 0, payloadSize);

                     if (messageType == FULL_SERVER_RESPONSE || messageType == SERVER_ACK || messageType == SERVER_ERROR_RESPONSE) {
                         if (messageCompression == GZIP) {
                             payloadStr = new String(gzipDecompress(payload));
                         } else {
                             payloadStr = new String(payload);
                         }
                     }
                 } else if (payloadSize > 0) {
                     System.err.println("Payload size mismatch. Expected: " + payloadSize + ", Available: " + (res.length - 12));
                     payloadStr = "[Error: Payload size mismatch]";
                 }

             } catch (Exception e) {
                 System.err.println("Error parsing Volcano Engine response: " + e.getMessage());
                 e.printStackTrace(System.err);
                 messageType = SERVER_ERROR_RESPONSE;
                 sequence = -1;
                 payloadStr = "Client-side error parsing server message: " + e.getMessage();
             }
         }

         result.put("message_type", messageType);
         result.put("sequence", sequence);
         result.put("payload_string", payloadStr);
         result.put("message_compression", messageCompression); // Store compression info

         if (messageType == SERVER_ERROR_RESPONSE) {
             result.put("error_code", sequence);
             result.put("error_message", payloadStr);
         }
         return result;
     }
    // --- End Helper Methods ---


    private void closeWebSocket(int code, String reason) {
        if (webSocket != null) {
            try {
                System.out.println("Closing Volcano Engine WebSocket (Code: " + code + ")");
                webSocket.close(code, reason);
            } catch (Exception e) {
                System.err.println("Exception closing Volcano Engine WebSocket: " + e.getMessage());
            } finally {
                webSocket = null;
                isVolcanoEngineConnectionOpen = false;
            }
        }
    }

    @Override
    public void close() {
        System.out.println("Closing RealTimeASRClient connection to Volcano Engine...");
        closeWebSocket(1000, "Client shutdown");
        // OkHttp client shutdown should happen if it's not shared.
        // If shared, manage lifecycle elsewhere.
         // httpClient.dispatcher().executorService().shutdown();
         // httpClient.connectionPool().evictAll();
         // ... (add awaitTermination logic if shutting down client here)
    }
}