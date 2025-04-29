const serverUrlInput = document.getElementById('serverUrl');
const audioFileInput = document.getElementById('audioFile');
const connectBtn = document.getElementById('connectBtn');
const disconnectBtn = document.getElementById('disconnectBtn');
const startSendBtn = document.getElementById('startSendBtn');
const statusDiv = document.getElementById('status');
const resultsDiv = document.getElementById('results');

let websocket = null;
let audioFile = null;
let fileReader = new FileReader();
const CHUNK_SIZE = 3200; // 与服务器和Java客户端匹配
let isServerReady = false; // 新增：标记服务器是否已发送 ready 信号
let waitingToSend = false; // 新增：标记用户是否已点击“开始发送”并正在等待 ready
let currentAsrText = ""; // 新增：存储最新的 ASR 文本

function logStatus(message) {
    console.log(message);
    statusDiv.textContent = message;
}

// 修改：这个函数现在只用于更新 ASR 结果显示区域
function updateResultDisplay(text) {
    resultsDiv.innerHTML = ''; // 清空之前的内容
    if (text) { // 只在有文本时创建 p 元素
        const p = document.createElement('p');
        p.textContent = text;
        resultsDiv.appendChild(p);
        resultsDiv.scrollTop = resultsDiv.scrollHeight; // 滚动到底部
    }
     console.log("Displaying Result:", text);
}

// logResult 函数可以保留用于显示非 ASR 结果的消息，或者如果不再需要可以移除
// function logResult(message) {
//     console.log("Log:", message);
//     const p = document.createElement('p');
//     p.textContent = message;
//     resultsDiv.appendChild(p);
//     resultsDiv.scrollTop = resultsDiv.scrollHeight; // 滚动到底部
// }


function connectWebSocket() {
    const url = serverUrlInput.value;
    if (!url) {
        logStatus("请输入服务器地址");
        return;
    }

    logStatus(`正在连接到 ${url}...`);
    websocket = new WebSocket(url);

    websocket.onopen = (event) => {
        logStatus("已连接到服务器");
        connectBtn.disabled = true;
        disconnectBtn.disabled = false;
        // 不再在这里启用 startSendBtn，等待服务器 ready
        // if (audioFile) {
        //     startSendBtn.disabled = false;
        // }
    };

    websocket.onmessage = (event) => {
        if (typeof event.data === 'string') {
            console.log("Raw message received:", event.data);
            try {
                const messageData = JSON.parse(event.data);
                console.log("Parsed message data:", messageData);

                // Corrected check: look inside messageData.result for the text field
                if (messageData.result && messageData.result.text !== undefined) {
                    console.log("Found 'text' field inside 'result':", messageData.result.text); // Updated log
                    currentAsrText = messageData.result.text; // Get text from the correct path
                    updateResultDisplay(currentAsrText);
                } else {
                     console.log("'text' field not found in messageData.result."); // Updated log
                }

                // 处理控制事件
                if (messageData.event === "ready_to_receive_audio") {
                    logStatus("服务器准备就绪，可以开始发送音频。");
                    isServerReady = true;
                    startSendBtn.disabled = !audioFile;
                    if (waitingToSend) {
                         sendAudioChunks();
                         waitingToSend = false;
                    }
                } else if (messageData.event === "asr_complete") {
                    logStatus("服务器指示 ASR 处理完成。");
                    // 最终结果已经在收到最后一条带 text 的消息时更新了
                    startSendBtn.disabled = true;
                    isServerReady = false;
                } else if (messageData.error) {
                    logStatus(`收到错误: ${messageData.message} (Code: ${messageData.code})`);
                     startSendBtn.disabled = true;
                     isServerReady = false;
                     waitingToSend = false;
                     // 可以在出错时也清空显示
                     // currentAsrText = "";
                     // updateResultDisplay("");
                }
                // 其他类型的 JSON 消息（如果有的话）可以在这里处理

            } catch (e) {
                // 非 JSON 消息或解析错误，记录到控制台，但不更新主显示区域
                // console.warn("Received non-JSON message or parse error:", e, "Data:", event.data); // 原有的 warn
                console.error("Error parsing message or processing:", e, "Raw data:", event.data); // 修改为 error 并包含原始数据
                // 可以选择性地用 logStatus 显示错误
                // logStatus(`收到无法解析的消息: ${event.data}`);
            }
        } else if (event.data instanceof Blob || event.data instanceof ArrayBuffer) {
            logStatus("收到二进制数据 (非预期)"); // 使用 logStatus 而不是 logResult
            console.log("Received binary data:", event.data);
        }
    };

    websocket.onerror = (event) => {
        logStatus(`WebSocket 错误: ${event}`);
        console.error("WebSocket Error: ", event);
        disconnectWebSocket(); // 出错时尝试断开清理
    };

    websocket.onclose = (event) => {
        logStatus(`连接已断开. Code: ${event.code}, Reason: ${event.reason || 'N/A'}`);
        connectBtn.disabled = false;
        disconnectBtn.disabled = true;
        startSendBtn.disabled = true;
        websocket = null;
        isServerReady = false;
        waitingToSend = false;
        currentAsrText = ""; // 清空存储的文本
        updateResultDisplay(""); // 清空显示
    };
}

function disconnectWebSocket() {
    if (websocket) {
        logStatus("正在断开连接...");
        websocket.close(); // onclose 事件会处理清理
    } else {
         // 如果 websocket 已经是 null，也确保清理显示
         currentAsrText = "";
         updateResultDisplay("");
    }
}

function handleFileSelect(event) {
    const files = event.target.files;
    if (files.length > 0) {
        audioFile = files[0];
        logStatus(`已选择文件: ${audioFile.name} (${audioFile.size} bytes)`);
        // 只有在服务器已就绪时才启用按钮
        if (websocket && websocket.readyState === WebSocket.OPEN && isServerReady) {
            startSendBtn.disabled = false;
        } else if (websocket && websocket.readyState === WebSocket.OPEN) {
            logStatus("文件已选择，请等待服务器就绪信号...");
        } else {
             logStatus("文件已选择，请先连接服务器。");
        }
    } else {
        audioFile = null;
        startSendBtn.disabled = true;
    }
}

function startSendingAudio() {
    if (!websocket || websocket.readyState !== WebSocket.OPEN) {
        logStatus("错误：未连接到服务器。");
        return;
    }
    if (!audioFile) {
        logStatus("错误：请先选择一个音频文件。");
        return;
    }

    // 清空上一次的结果
    currentAsrText = "";
    updateResultDisplay("");
    logStatus("准备发送音频..."); // 更新状态

    if (isServerReady) {
        sendAudioChunks();
    } else {
        logStatus("请求发送音频，等待服务器 'ready' 信号...");
        waitingToSend = true;
        startSendBtn.disabled = true;
    }
}


function sendAudioChunks() {
    if (!audioFile) return;

    logStatus(`开始读取并发送文件: ${audioFile.name}`);
    startSendBtn.disabled = true; // 发送过程中禁用按钮
    let offset = 0;

    // 检查是否是 WAV 文件并尝试跳过头部
    // 注意：这是一个简化的头部跳过，可能不适用于所有WAV格式
    const isWav = audioFile.name.toLowerCase().endsWith('.wav');
    const headerSize = isWav ? 44 : 0;
    if (isWav) {
        logStatus("检测到 WAV 文件，将尝试跳过 44 字节的头部。");
        offset = headerSize;
    }


    fileReader = new FileReader(); // 创建新的 FileReader 实例

    fileReader.onload = (e) => {
        const chunk = e.target.result; // ArrayBuffer
        if (chunk.byteLength > 0) {
            // 检查 WebSocket 是否仍然连接
            if (!websocket || websocket.readyState !== WebSocket.OPEN) {
                 logStatus("错误：在发送过程中连接已断开。");
                 startSendBtn.disabled = !isServerReady; // 根据服务器状态决定是否重置按钮
                 waitingToSend = false;
                 return;
            }
            // 不再在这里 logStatus 发送块信息，避免过多日志
            // logStatus(`发送音频块: ${chunk.byteLength} bytes`);
            websocket.send(chunk); // 发送 ArrayBuffer
            offset += chunk.byteLength;
            // 读取下一块，如果还有数据
            // 确保在读取下一块之前，WebSocket 仍然打开
            if (offset < audioFile.size && websocket && websocket.readyState === WebSocket.OPEN) {
                readNextChunk(offset);
            } else if (offset >= audioFile.size) {
                // 文件读取完毕
                logStatus("音频文件数据发送完毕。");
                sendEndOfStream();
                // 发送完成后可以考虑再次启用按钮，如果需要重发的话
                // startSendBtn.disabled = false; // 或者保持禁用直到完成或断开
            }
        } else if (offset >= audioFile.size) {
             // 处理最后一块为空的情况（理论上不应发生，但作为健壮性检查）
             logStatus("音频文件数据发送完毕 (空块检测)。");
             sendEndOfStream();
        }
    };

    fileReader.onerror = (e) => {
        logStatus(`读取文件时出错: ${e.target.error}`);
        console.error("FileReader Error:", e.target.error);
        startSendBtn.disabled = !isServerReady; // 根据服务器状态决定是否重置按钮
        waitingToSend = false;
    };

    // 读取第一块
    readNextChunk(offset);
}

function readNextChunk(startOffset) {
     if (!audioFile || !fileReader) return;
     const endOffset = Math.min(startOffset + CHUNK_SIZE, audioFile.size);
     const slice = audioFile.slice(startOffset, endOffset);
     fileReader.readAsArrayBuffer(slice);
}


function sendEndOfStream() {
    if (websocket && websocket.readyState === WebSocket.OPEN) {
        logStatus("发送 END_OF_STREAM 信号...");
        websocket.send("END_OF_STREAM");
    }
}

// 事件监听器
connectBtn.addEventListener('click', connectWebSocket);
disconnectBtn.addEventListener('click', disconnectWebSocket);
audioFileInput.addEventListener('change', handleFileSelect);
startSendBtn.addEventListener('click', startSendingAudio);

// 初始状态
logStatus("页面加载完成，请连接服务器并选择文件。");
startSendBtn.disabled = true; // 初始禁用发送按钮