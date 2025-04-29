// YourWebSocketServer.java
package com.example.asrwebsocket.handler;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class YourWebSocketServer extends WebSocketServer {

    // --- Configuration for your server ---
    private static final int SERVER_PORT = 8887; // Port your clients will connect to

     // --- Volcano Engine Credentials (Move to secure config later) ---
     private static final String APP_ID = "3467491207"; // Replace
     private static final String TOKEN = "kPHQMVliZeyYh2fWkiOC-e9eXIeZhCN8"; // Replace

     // --- Default Audio Config (Client might send this info later) ---
     // IMPORTANT: Ensure this matches the audio the client sends!
     private static final RealTimeASRClient.AudioConfig DEFAULT_AUDIO_CONFIG =
             new RealTimeASRClient.AudioConfig("pcm", 16000, 16, 1, "raw");


    // Map to hold the ASR client instance for each connected WebSocket client
    private final Map<WebSocket, RealTimeASRClient> clientConnections = new ConcurrentHashMap<>();

    public YourWebSocketServer(int port) {
        super(new InetSocketAddress(port));
        System.out.println("WebSocket Server created on port: " + port);
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        System.out.println("New client connected: " + conn.getRemoteSocketAddress());

        // Create a listener that forwards results back to the specific client
        ASRResultListener listener = new ASRResultListener() {
            @Override
            public void onResult(String jsonResult) {
                System.out.println("Sending result to client " + conn.getRemoteSocketAddress() + ": " + jsonResult.substring(0, Math.min(jsonResult.length(), 100)) + "..."); // Log snippet
                conn.send(jsonResult); // Send JSON result as text
            }

            @Override
            public void onError(int errorCode, String errorMessage) {
                System.err.println("ASR Error for client " + conn.getRemoteSocketAddress() + ": " + errorMessage);
                // Send error back to client (e.g., as JSON)
                 conn.send("{\"error\": true, \"code\": " + errorCode + ", \"message\": \"" + escapeJson(errorMessage) + "\"}");
                // Optionally close connection on severe errors
                 // conn.close(1011, "ASR Error: " + errorMessage);
            }

            @Override
            public void onComplete() {
                System.out.println("ASR processing complete for client: " + conn.getRemoteSocketAddress());
                 // Optionally send a completion message
                 conn.send("{\"event\": \"asr_complete\"}");
            }

             @Override
             public void onVolcanoEngineConnectionOpen() {
                 System.out.println("Volcano Engine connection ready for client: " + conn.getRemoteSocketAddress());
                 // Notify client it can start sending audio?
                 conn.send("{\"event\": \"ready_to_receive_audio\"}");
             }
        };

        // Create and store an ASR client instance for this connection
        RealTimeASRClient asrClient = new RealTimeASRClient(APP_ID, TOKEN, DEFAULT_AUDIO_CONFIG, listener);
        clientConnections.put(conn, asrClient);

        // Initiate connection to Volcano Engine
        asrClient.connect();
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        System.out.println("Client disconnected: " + conn.getRemoteSocketAddress() + " (Code: " + code + ", Reason: " + reason + ")");
        // Clean up the associated ASR client
        RealTimeASRClient asrClient = clientConnections.remove(conn);
        if (asrClient != null) {
            asrClient.close(); // Close connection to Volcano Engine
        }
    }

    @Override
    public void onMessage(WebSocket conn, String message) {
        System.out.println("Received text message from " + conn.getRemoteSocketAddress() + ": " + message);
        // Handle text messages (e.g., commands like "END_OF_STREAM")
        RealTimeASRClient asrClient = clientConnections.get(conn);
        if (asrClient != null && "END_OF_STREAM".equalsIgnoreCase(message)) {
            System.out.println("Client signaled end of stream.");
            asrClient.signalEndOfAudio();
        }
        // Could also handle config messages here if needed
    }

    @Override
    public void onMessage(WebSocket conn, ByteBuffer message) {
         // System.out.println("Received binary message (audio chunk) from " + conn.getRemoteSocketAddress() + " Size: " + message.remaining()); // Debug
        RealTimeASRClient asrClient = clientConnections.get(conn);
        if (asrClient != null && asrClient.isConnected()) {
            // Forward the audio chunk to the ASR client
            byte[] audioChunk = new byte[message.remaining()];
            message.get(audioChunk);
            asrClient.sendAudio(audioChunk);
        } else if (asrClient != null) {
            System.err.println("Received audio from client " + conn.getRemoteSocketAddress() + " but Volcano Engine connection not ready.");
            // Optionally queue or notify client?
        } else {
             System.err.println("Received binary message but no associated ASR client found for " + conn.getRemoteSocketAddress());
        }
    }

    @Override
    public void onError(WebSocket conn, Exception ex) {
        System.err.println("Server error occurred for connection " + (conn != null ? conn.getRemoteSocketAddress() : "UNKNOWN") + ": " + ex.getMessage());
        ex.printStackTrace();
        // Clean up if connection exists
        if (conn != null) {
            RealTimeASRClient asrClient = clientConnections.remove(conn);
            if (asrClient != null) {
                asrClient.close();
            }
             // Don't close client connection here, onClose will be called by the library
        }
    }

    @Override
    public void onStart() {
        System.out.println("Server started successfully on port " + getPort());
        setConnectionLostTimeout(100); // Adjust timeout as needed (seconds)
    }

    // Helper to escape JSON strings
    private String escapeJson(String str) {
        if (str == null) return null;
        return str.replace("\\", "\\\\")
                  .replace("\"", "\\\"")
                  .replace("\b", "\\b")
                  .replace("\f", "\\f")
                  .replace("\n", "\\n")
                  .replace("\r", "\\r")
                  .replace("\t", "\\t");
    }

    public static void main(String[] args) {
         if (APP_ID.equals("YOUR_APP_ID") || TOKEN.equals("YOUR_ACCESS_TOKEN")) {
             System.err.println("ERROR: Please replace YOUR_APP_ID and YOUR_ACCESS_TOKEN in YourWebSocketServer.java");
             return;
         }
        YourWebSocketServer server = new YourWebSocketServer(SERVER_PORT);
        server.start(); // Starts the server thread
        System.out.println("WebSocket server running. Press CTRL+C to stop.");
    }
}