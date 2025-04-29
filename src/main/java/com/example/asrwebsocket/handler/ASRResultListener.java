package com.example.asrwebsocket.handler;

// ASRResultListener.java
public interface ASRResultListener {
    /**
     * Called when a transcription result (JSON string) is received.
     * @param jsonResult The JSON payload from Volcano Engine.
     */
    void onResult(String jsonResult);

    /**
     * Called when an error occurs during the ASR process.
     * @param errorCode The error code (if available, often from sequence).
     * @param errorMessage The error message.
     */
    void onError(int errorCode, String errorMessage);

    /**
     * Called when the ASR process for a stream is considered complete
     * by the RealTimeASRClient (e.g., after receiving the final negative sequence).
     */
    void onComplete();

     /**
      * Called when the underlying WebSocket connection to Volcano Engine is opened.
      * Can be used to signal readiness to accept audio.
      */
     void onVolcanoEngineConnectionOpen();
}