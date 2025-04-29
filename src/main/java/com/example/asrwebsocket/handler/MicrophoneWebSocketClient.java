// MicrophoneWebSocketClient.java
package com.example.asrwebsocket.handler;

import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import javax.sound.sampled.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class MicrophoneWebSocketClient extends WebSocketClient {

    // --- Configuration ---
    private static final String SERVER_URI = "ws://localhost:8887"; // URI of YourWebSocketServer
    // private static final String AUDIO_FILE_PATH = "D:\\Cursor\\JProject\\asr\\16k16bit.wav"; // Removed: No longer reading from file
    private static final int SAMPLE_RATE = 16000; // Hz
    private static final int SAMPLE_SIZE_IN_BITS = 16;
    private static final int CHANNELS = 1;
    private static final boolean SIGNED = true;
    private static final boolean BIG_ENDIAN = false;
    private static final AudioFormat AUDIO_FORMAT = new AudioFormat(SAMPLE_RATE, SAMPLE_SIZE_IN_BITS, CHANNELS, SIGNED, BIG_ENDIAN);
    private static final int CHUNK_SIZE = (int) (SAMPLE_RATE * SAMPLE_SIZE_IN_BITS / 8 * CHANNELS * 0.1); // 100ms chunks

    private TargetDataLine microphoneLine;
    private final AtomicBoolean capturing = new AtomicBoolean(false);
    private Thread captureThread;


    public MicrophoneWebSocketClient(URI serverUri) {
        super(serverUri);
        System.out.println("MicrophoneWebSocketClient created for server: " + serverUri);
    }

    @Override
    public void onOpen(ServerHandshake handshakedata) {
        System.out.println("Connected to server: " + getURI());
        System.out.println("Status: " + handshakedata.getHttpStatusMessage());
        // Connection is open, wait for the server's "ready" message
    }

    @Override
    public void onMessage(String message) {
        System.out.println("Received message from server: " + message);
         // Check for the server's ready signal before starting audio capture
         if (!capturing.get() && message.contains("\"event\": \"ready_to_receive_audio\"")) {
             System.out.println("Server is ready. Starting microphone capture...");
             startMicrophoneCapture(); // Start capturing from microphone
         } else if (message.contains("\"event\": \"asr_complete\"")) {
             System.out.println("Server indicated ASR processing is complete.");
             // Optionally stop capture automatically, or wait for user action
             // stopMicrophoneCapture(); 
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
        stopMicrophoneCapture(); // Ensure capture stops on disconnect
    }

    @Override
    public void onError(Exception ex) {
        System.err.println("Client error occurred: " + ex.getMessage());
        stopMicrophoneCapture(); // Ensure capture stops on error
        ex.printStackTrace();
    }

    // Method to start capturing audio from the microphone
    private void startMicrophoneCapture() {
        if (capturing.compareAndSet(false, true)) {
            try {
                DataLine.Info info = new DataLine.Info(TargetDataLine.class, AUDIO_FORMAT);
                if (!AudioSystem.isLineSupported(info)) {
                    System.err.println("Audio line format not supported: " + AUDIO_FORMAT);
                     close(1011, "Audio format not supported");
                    capturing.set(false);
                    return;
                }

                microphoneLine = (TargetDataLine) AudioSystem.getLine(info);
                microphoneLine.open(AUDIO_FORMAT);
                microphoneLine.start();

                captureThread = new Thread(() -> {
                     byte[] buffer = new byte[CHUNK_SIZE];
                     System.out.println("Microphone capture started. Sending audio data...");
                     try {
                         while (capturing.get() && isOpen()) {
                             int bytesRead = microphoneLine.read(buffer, 0, buffer.length);
                             if (bytesRead > 0) {
                                 send(ByteBuffer.wrap(buffer, 0, bytesRead));
                                 // No artificial delay needed, read is blocking
                             }
                         }
                     } catch (Exception e) {
                         if (capturing.get()) { // Avoid error message if stopped intentionally
                             System.err.println("Error during microphone capture: " + e.getMessage());
                             e.printStackTrace();
                             close(1011, "Microphone read error");
                         }
                     } finally {
                         System.out.println("Microphone capture loop finished.");
                         // Cleanup is handled in stopMicrophoneCapture
                     }
                });
                captureThread.setName("Microphone-Capture-Thread");
                captureThread.start();

            } catch (LineUnavailableException e) {
                System.err.println("Microphone line unavailable: " + e.getMessage());
                e.printStackTrace();
                 close(1011, "Microphone unavailable");
                capturing.set(false);
            }
        }
    }

    // Method to stop capturing audio
    public void stopMicrophoneCapture() {
        if (capturing.compareAndSet(true, false)) {
            System.out.println("Attempting to stop microphone capture...");
            if (microphoneLine != null) {
                 if (microphoneLine.isRunning()) {
                     microphoneLine.stop();
                 }
                 if (microphoneLine.isOpen()) {
                    microphoneLine.close();
                 }
                System.out.println("Microphone line stopped and closed.");
            }
            microphoneLine = null; // Release reference

             // Wait briefly for the capture thread to finish reading
             if (captureThread != null) {
                 try {
                     captureThread.join(500); // Wait max 500ms
                 } catch (InterruptedException e) {
                     Thread.currentThread().interrupt();
                     System.err.println("Interrupted while waiting for capture thread to stop.");
                 }
                 captureThread = null;
             }

            // Signal end of stream to the server AFTER stopping capture
             if (isOpen()) {
                 try {
                    send("END_OF_STREAM");
                    System.out.println("Sent END_OF_STREAM signal.");
                 } catch(Exception e) {
                    System.err.println("Error sending END_OF_STREAM signal: " + e.getMessage());
                 }
             } else {
                 System.out.println("Connection closed, cannot send END_OF_STREAM.");
             }
        } else {
            System.out.println("Capture already stopped or not started.");
        }
    }


    // Keeping the main method for now, might remove later if not needed for standalone testing
    public static void main(String[] args) {
         
        try {
            MicrophoneWebSocketClient client = new MicrophoneWebSocketClient(new URI(SERVER_URI));
            client.connect(); // Initiates connection attempt (non-blocking)

             System.out.println("Client connecting... Press Enter to stop capture and exit.");

             // Keep running until connection is closed OR user presses Enter
             Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                 System.out.println("Shutdown hook triggered. Stopping capture and closing client...");
                 client.stopMicrophoneCapture();
                 client.close();
             }));

             try {
                 // Wait for Enter key press
                 System.in.read();
             } catch (IOException e) {
                 e.printStackTrace();
             }

             System.out.println("Stopping capture...");
             client.stopMicrophoneCapture();
             System.out.println("Closing connection...");
            client.closeBlocking(); // Wait for close to complete

            System.out.println("MicrophoneWebSocketClient finished.");

        } catch (URISyntaxException e) {
            System.err.println("Invalid server URI: " + SERVER_URI);
            e.printStackTrace();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Main thread interrupted.");
        } catch (Exception e) { // Catch other potential exceptions during setup
            System.err.println("An unexpected error occurred in main: " + e);
        }
    }
}