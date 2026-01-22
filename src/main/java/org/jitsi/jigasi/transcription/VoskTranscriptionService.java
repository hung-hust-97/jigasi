/*
 * Jigasi, the JItsi GAteway to SIP.
 *
 * Copyright @ 2018 - present 8x8, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jitsi.jigasi.transcription;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.socket.client.IO;
import io.socket.client.Socket;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.*;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.jitsi.jigasi.JigasiBundleActivator;
import org.jitsi.jigasi.constant.EventWsAIEnum;
import org.jitsi.jigasi.transcription.config.ClientConfig;
import org.jitsi.jigasi.transcription.config.DataClientConfig;
import org.jitsi.jigasi.transcription.utils.Language;
import org.jitsi.utils.logging.Logger;
import org.json.JSONObject;

import javax.media.format.AudioFormat;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;


/**
 * Implements a TranscriptionService which uses local
 * Vosk websocket transcription service.
 * <p>
 * See https://github.com/alphacep/vosk-server for
 * information about server
 *
 * @author Nik Vaessen
 * @author Damian Minkov
 * @author Nickolay V. Shmyrev
 */
public class VoskTranscriptionService
        extends AbstractTranscriptionService {

    /**
     * The logger for this class
     */
    private final static Logger logger
            = Logger.getLogger(VoskTranscriptionService.class);

    /**
     * The config key of the websocket to the speech-to-text service.
     */
    public final static String WEBSOCKET_URL
            = "org.jitsi.jigasi.transcription.vosk.websocket_url";
    public final static String API_KEY
            = "org.jitsi.jigasi.transcription.google.api_key";
    public final static String DEFAULT_WEBSOCKET_URL = "ws://localhost:2700";
    public final static String END_POINT
            = "org.jitsi.jigasi.transcription.translate.endpoint";
    public final static String VOICE_WS
            = "org.jitsi.jigasi.voice.ai.stt";
    private final static String EOF_MESSAGE = "{\"eof\" : 1}";
    private final CountDownLatch latch = new CountDownLatch(1);

    /**
     * The config value of the websocket to the speech-to-text service.
     */
    private final String websocketUrlConfig;

    /**
     * The URL of the websocket to the speech-to-text service.
     */
    private String websocketUrl;

    private String username;

    /**
     * Assigns the websocketUrl to use to websocketUrl by reading websocketUrlConfig;
     */
    private void generateWebsocketUrl(Participant participant)
            throws org.json.simple.parser.ParseException {
        // Lấy base URL từ config (ví dụ: https://stream-voices.cmcati.vn)
        websocketUrl = JigasiBundleActivator.getConfigurationService().getString(VOICE_WS, "");
        username = participant.getName();
    }

    /**
     * Create a TranscriptionService which will send audio to the VOSK service
     * platform to get a transcription
     */
    public VoskTranscriptionService() {
        websocketUrlConfig = JigasiBundleActivator.getConfigurationService()
                .getString(WEBSOCKET_URL, DEFAULT_WEBSOCKET_URL);
    }

    /**
     * No configuration required yet
     */
    public boolean isConfiguredProperly() {
        return true;
    }

    /**
     * If the websocket url is a JSON, language routing is supported
     */
    public boolean supportsLanguageRouting() {
        return websocketUrlConfig.trim().startsWith("{");
    }

    /**
     * Sends audio as an array of bytes to Vosk service
     *
     * @param request        the TranscriptionRequest which holds the audio to be sent
     * @param resultConsumer a Consumer which will handle the
     *                       TranscriptionResult
     */
    @Override
    public void sendSingleRequest(final TranscriptionRequest request,
                                  final Consumer<TranscriptionResult> resultConsumer) {
        // Try to create the client, which can throw an IOException
        try {
            // Set the sampling rate and encoding of the audio
            AudioFormat format = request.getFormat();
            if (!format.getEncoding().equals("LINEAR")) {
                throw new IllegalArgumentException("Given AudioFormat" +
                        "has unexpected" +
                        "encoding");
            }
            Instant timeRequestReceived = Instant.now();

            WebSocketClient ws = new WebSocketClient();
            VoskWebsocketSession socket = new VoskWebsocketSession(request);
            ws.start();
            ws.connect(socket, new URI(websocketUrl));
            socket.awaitClose();
            resultConsumer.accept(
                    new TranscriptionResult(
                            null,
                            UUID.randomUUID(),
                            timeRequestReceived,
                            false,
                            request.getLocale().toLanguageTag(),
                            0,
                            new TranscriptionAlternative(socket.getResult())));
        } catch (Exception e) {
            logger.error("Error sending single req", e);
        }
    }

    @Override
    public StreamingRecognitionSession initStreamingSession(Participant participant)
            throws UnsupportedOperationException {
        try {
            generateWebsocketUrl(participant);
            VoskWebsocketStreamingSession streamingSession = new VoskWebsocketStreamingSession(
                    participant.getDebugName());
            streamingSession.transcriptionTag = participant.getTranslationLanguage();
            if (streamingSession.transcriptionTag == null) {
                streamingSession.transcriptionTag = participant.getSourceLanguage();
            }
            return streamingSession;
        } catch (Exception e) {
            throw new UnsupportedOperationException("Failed to create streaming session", e);
        }
    }

    @Override
    public boolean supportsFragmentTranscription() {
        return true;
    }

    @Override
    public boolean supportsStreamRecognition() {
        return true;
    }

    /**
     * A Transcription session for transcribing streams, handles
     * the lifecycle of Socket.IO connection
     */
    public class VoskWebsocketStreamingSession
            implements StreamingRecognitionSession {
        private final Socket socket;
        /* The name of the participant */
        private final String debugName;
        /* The sample rate of the audio stream we collect from the first request */
        private final double sampleRate = -1.0;
        /* Last returned result so we do not return the same string twice */
        private String lastResult = "";
        /* Transcription language requested by the user who requested the transcription */
        private String transcriptionTag = "en-US";
        private final CountDownLatch socketIoConnectLatch = new CountDownLatch(1);
        private final CountDownLatch sessionReadyLatch = new CountDownLatch(1);
        /* Session ID for Socket.IO connection */
        private String sessionId;
        /* Segment ID counter for audio chunks */
        private int segmentId = 0;
        /**
         * List of TranscriptionListeners which will be notified when a
         * result comes in
         */
        private final List<TranscriptionListener> listeners = new ArrayList<>();

        /**
         * Latest assigned UUID to a transcription result.
         * A new one has to be generated whenever a definitive result is received.
         */
        private UUID uuid = UUID.randomUUID();

        VoskWebsocketStreamingSession(String debugName)
                throws Exception {
            logger.info("=== Creating VoskWebsocketStreamingSession for " + debugName + " ===");
            this.debugName = debugName;
            logger.info("Configuring Socket.IO options for URL: " + websocketUrl);
            IO.Options opts = new IO.Options();
            opts.transports = new String[]{"websocket"};
            opts.path = "/raw";
            opts.reconnection = true;
            opts.reconnectionAttempts = 10;
            opts.reconnectionDelay = 3000;
            opts.reconnectionDelayMax = 10000;
            opts.randomizationFactor = 0.5;
            logger.info("Socket.IO options configured: path=" + opts.path + ", transports=" + Arrays.toString(opts.transports));

            socket = IO.socket(websocketUrl, opts);

            setupSocketListeners();

            socket.connect();

            boolean connected = latch.await(10, TimeUnit.SECONDS);
//            logger.info("Latch await completed, connected: " + connected);
        }

        private void setupSocketListeners() {
            logger.info("Setting up Socket.IO listeners for " + debugName);

            socket.on("connect", args -> {
                logger.info("Tuan meo === Connected to Socket.IO, socketId=" + socket.id());
                JSONObject payload = new JSONObject();
                payload.put("session_id", "11111111111111"); // byte[] -> attachment
                socket.emit("session_request", payload);
                JSONObject payload1 = new JSONObject();
                socket.emit("set_loop_emit_raw", payload1);
                socketIoConnectLatch.countDown();
            });

            socket.on("session_confirm", args -> {
                logger.info("Session confirmed: " + args.length);
                Object data = args[0];
                sessionId = data.toString();
                sessionReadyLatch.countDown();
            });
            socket.on("connect_error", args -> {
                if (args.length > 0 && args[0] instanceof Throwable) {
                    Throwable err = (Throwable) args[0];
                    logger.error("Socket.IO error for " + debugName + ": " + err.getMessage(), err);
                } else {
                    logger.error("Socket.IO error for " + debugName + ", args: " + Arrays.toString(args));
                }
            });

            // Lắng nghe event "asr_uttered" để nhận kết quả từ server
            socket.on("asr_uttered", args -> {
                if (args.length == 0) return;
                Object data = args[0];
                if (data instanceof JSONObject) {
                    JSONObject json = (JSONObject) data;
                    onMessage(json.toString());
                } else {
                    logger.info("data = {}" + data.toString());
                }
            });
        }

        private String translateAPI(Translation translation) {
            try {
                HttpClient client = HttpClient.newHttpClient();
                JSONObject jsonRequest = new JSONObject();
                jsonRequest.put("text", translation.getQ());
                jsonRequest.put("tgt", translation.getTarget());
                String api_key = JigasiBundleActivator.getConfigurationService()
                        .getString(API_KEY, "default");
                String url = JigasiBundleActivator.getConfigurationService()
                        .getString(END_POINT, "");
                if (!Objects.equals(api_key, "default")) {
                    url += api_key;
                }
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(jsonRequest.toString()))
                        .build();

                // Gửi request đồng bộ
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200) {
                    JSONObject jsonResponse = new JSONObject(response.body());
                    return jsonResponse
                            .getString("translation");
                } else {
                    logger.warn("Failed to translate: " + response.statusCode());
                }
            } catch (Exception e) {
                logger.error("Error calling translation API", e);
            }

            return null;
        }

        public void onMessage(String msg) {
            boolean partial = true;
            String result = "";
            JSONObject jsonObject = new JSONObject(msg);
            logger.info("response: " + jsonObject);
            String message = "";
            try {
                message = jsonObject.getString("predict_segment");
            } catch (Exception e) {
            }
            result = message;
            if (!result.isEmpty() && !result.equals(lastResult)) {
                Translation translation = new Translation(result, Language.EN.getLanguage(), Language.VN.getLanguage());
                String translatedText = translateAPI(translation);
                JSONObject jsonRequest = new JSONObject();
                jsonRequest.put("en", translatedText);
                jsonRequest.put("vi", result);
                result = jsonRequest.toString();
            }
            if (!result.isEmpty() && !result.equals(lastResult)) {
                lastResult = result;
                for (TranscriptionListener l : listeners) {
                    l.notify(new TranscriptionResult(
                            null,
                            uuid,
                            // this time needs to be the one when the audio was sent
                            // the results need to be matched with the time when we sent the audio, so we have
                            // the real time when this transcription was started
                            Instant.now(),
                            partial,
                            transcriptionTag,
                            1.0,
                            new TranscriptionAlternative(result)));
                }
            }

            if (!partial) {
                this.uuid = UUID.randomUUID();
            }
        }

        public void sendRequest(TranscriptionRequest request) {
            try {

                if (socket == null || !socket.connected()) {
                    logger.warn("Socket.IO not connected, cannot send audio for " + debugName);
                    return;
                }

                // { blob_data: arr, segment_id: chunkID, session_id: this.ssId }
                byte[] audioData = request.getAudio(); // PCM 16-bit mono 48k
                JSONObject payload = new JSONObject();
                payload.put("blob_data", audioData); // byte[] -> attachment
                payload.put("segment_id", segmentId++);
                payload.put("session_id", sessionId);
                socket.emit("user_uttered", payload);
                logger.info("user_uttered sessionId: " + sessionId);

            } catch (Exception e) {
                logger.error("Error to send Socket.IO request for participant " + debugName, e);
            }
        }

        public void addTranscriptionListener(TranscriptionListener listener) {
            listeners.add(listener);
        }

        public void end() {
            try {
                if (socket != null && socket.connected()) {
                    // Có thể gửi signal kết thúc streaming nếu server yêu cầu
                    // Hiện tại chỉ disconnect, không cần gửi signal đặc biệt
                    socket.disconnect();
                }
            } catch (Exception e) {
                logger.error("Error to finalize Socket.IO connection for participant " + debugName, e);
            }
        }

        public boolean ended() {
            return socket == null || !socket.connected();
        }
    }

    /**
     * Session to send websocket data and recieve results. Non-streaming version
     */
    @WebSocket
    public class VoskWebsocketSession {
        /* Signal for the end of operation */
        private final CountDownLatch closeLatch;

        /* Request we need to process */
        private final TranscriptionRequest request;
        private final CountDownLatch jettyWsConnectLatch = new CountDownLatch(1);
        /* Collect results*/
        private final StringBuilder result;

        VoskWebsocketSession(TranscriptionRequest request) {
            this.closeLatch = new CountDownLatch(1);
            this.request = request;
            this.result = new StringBuilder();
        }

        @OnWebSocketClose
        public void onClose(int statusCode, String reason) {
            this.closeLatch.countDown(); // trigger latch
        }

        @OnWebSocketConnect
        public void onConnect(Session session) {
            try {
                jettyWsConnectLatch.countDown();
                ObjectMapper objectMapper = new ObjectMapper();

                ClientConfig clientConfig = ClientConfig
                        .builder()
                        .type(EventWsAIEnum.EVENT_RECEIVE_CLIENT_CONFIG.getName())
                        .data(DataClientConfig
                                .builder()
                                .is_recording(true)
                                .build())
                        .build();
                String json = objectMapper.writeValueAsString(clientConfig);
                session.getRemote().sendString(json);
            } catch (IOException e) {
                logger.error("Error to transcribe audio", e);
            }
        }

        @OnWebSocketMessage
        public void onMessage(String msg) {
            result.append(msg);
            result.append('\n');
        }

        @OnWebSocketError
        public void onError(Throwable cause) {
            logger.error("Websocket connection error", cause);
        }

        public String getResult() {
            return result.toString();
        }

        void awaitClose()
                throws InterruptedException {
            closeLatch.await();
        }
    }

}
