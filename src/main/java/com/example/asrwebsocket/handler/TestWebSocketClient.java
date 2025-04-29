// TestWebSocketClient.java
package com.example.asrwebsocket.handler;

import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;

public class TestWebSocketClient extends WebSocketClient {

    // --- Configuration ---
    private static final String SERVER_URI = "ws://localhost:8887"; // URI of YourWebSocketServer
    private static final String AUDIO_FILE_PATH = "D:\\Cursor\\JProject\\asr\\16k16bit.wav"; // Audio file to send
    private static final int CHUNK_SIZE = 3200; // Bytes per chunk (e.g., 100ms for 16kHz 16bit mono)


    public TestWebSocketClient(URI serverUri) {
        super(serverUri);
        System.out.println("Test client created for server: " + serverUri);
    }

    @Override
    public void onOpen(ServerHandshake handshakedata) {
        System.out.println("Connected to server: " + getURI());
        System.out.println("Status: " + handshakedata.getHttpStatusMessage());
        // Connection is open, but wait for the server's "ready" message before sending audio
    }

    @Override
    public void onMessage(String message) {
        System.out.println("Received message from server: " + message);
         // Check for the server's ready signal before starting audio send
         if (message.contains("\"event\": \"ready_to_receive_audio\"")) {
             System.out.println("Server is ready. Starting to send audio file...");
             // Start sending the audio file in a separate thread to avoid blocking the receive thread
             new Thread(this::sendAudioFile).start();
         } else if (message.contains("\"event\": \"asr_complete\"")) {
             System.out.println("Server indicated ASR processing is complete.");
             // Consider closing the connection now if desired
             // close();
         }
         // Handle other messages (results, errors)
    }

     @Override
     public void onMessage(ByteBuffer bytes) {
         // This client sends binary but expects text results from the server
         System.out.println("Received unexpected binary message from server: " + bytes.remaining() + " bytes");
     }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        System.out.println("Disconnected from server: " + getURI() + " Code: " + code + ", Reason: " + reason + ", Remote: " + remote);
    }

    @Override
    public void onError(Exception ex) {
        System.err.println("Client error occurred: " + ex.getMessage());
        ex.printStackTrace();
    }

    // Method to read and send the audio file
    private void sendAudioFile() {
         Path path = Paths.get(AUDIO_FILE_PATH);
         if (!Files.exists(path)) {
             System.err.println("ERROR: Audio file not found at " + AUDIO_FILE_PATH);
             close(1000, "Audio file not found");
             return;
         }

        try (InputStream audioStream = new FileInputStream(AUDIO_FILE_PATH)) {
            byte[] buffer = new byte[CHUNK_SIZE];
            int bytesRead;

            while ((bytesRead = audioStream.read(buffer)) != -1) {
                if (bytesRead > 0) {
                    // Send the chunk as a binary message
                     // System.out.println("Sending audio chunk: " + bytesRead + " bytes"); // Debug
                    send(ByteBuffer.wrap(buffer, 0, bytesRead));
                }
                // Optional: Add a small delay to simulate real-time streaming
                 try { Thread.sleep(10); } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; } // ~100ms audio sent every 10ms
            }

            System.out.println("Finished sending audio file data.");
            // Signal end of stream to the server using a text message
            send("END_OF_STREAM");
            System.out.println("Sent END_OF_STREAM signal.");

        } catch (IOException e) {
            System.err.println("Error reading audio file: " + e.getMessage());
            e.printStackTrace();
            close(1011, "Error reading audio file");
        }
    }


    public static void main(String[] args) {
         if (AUDIO_FILE_PATH.equals("YOUR_TEST_AUDIO_FILE.wav")) {
             System.err.println("ERROR: Please replace YOUR_TEST_AUDIO_FILE.wav in TestWebSocketClient.java");
             return;
         }

        try {
            TestWebSocketClient client = new TestWebSocketClient(new URI(SERVER_URI));
            client.connect(); // Initiates connection attempt (non-blocking)

            // Keep main thread alive while client runs (or use client.connectBlocking())
            // This is a simple way, better mechanisms exist for real apps.
            while(!client.isClosed()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    client.close();
                    break;
                }
            }
            System.out.println("Test client finished or closed.");

        } catch (URISyntaxException e) {
            System.err.println("Invalid server URI: " + SERVER_URI);
            e.printStackTrace();
        }
    }
}