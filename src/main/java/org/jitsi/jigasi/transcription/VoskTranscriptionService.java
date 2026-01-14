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
import lombok.Value;
import org.bson.*;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.eclipse.jetty.websocket.api.*;
import org.eclipse.jetty.websocket.api.annotations.*;
import org.eclipse.jetty.websocket.client.*;
import org.jitsi.jigasi.constant.EventWsAIEnum;
import org.jitsi.jigasi.transcription.config.ClientConfig;
import org.jitsi.jigasi.transcription.config.DataClientConfig;
import org.jitsi.jigasi.transcription.utils.Language;
import org.json.*;
import org.jitsi.jigasi.*;
import org.jitsi.utils.logging.*;

import javax.media.format.*;
import java.io.*;
import java.net.*;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;


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
    private CountDownLatch latch = new CountDownLatch(1);

    /**
     * The config value of the websocket to the speech-to-text service.
     */
    private String websocketUrlConfig;

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
//        if (!supportsLanguageRouting())
//        {
//            websocketUrl = websocketUrlConfig;
//            return;
//        }
//
//        org.json.simple.parser.JSONParser jsonParser = new org.json.simple.parser.JSONParser();
//        Object obj = jsonParser.parse(websocketUrlConfig);
//        org.json.simple.JSONObject languageMap = (org.json.simple.JSONObject) obj;
//        String language = participant.getSourceLanguage() != null ? participant.getSourceLanguage() : "en";
//        Object urlObject = languageMap.get(language);
//        if (!(urlObject instanceof String))
//        {
//            logger.error("No websocket URL configured for language " + language);
//            websocketUrl = null;
//            return;
//        }
        String voiceWs = JigasiBundleActivator.getConfigurationService().getString(VOICE_WS, "");
        websocketUrl = voiceWs + participant.getRoomId() + participant.getId();
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
     * the lifecycle of websocket
     */
    @WebSocket
    public class VoskWebsocketStreamingSession
            implements StreamingRecognitionSession {
        private Session session;
        /* The name of the participant */
        private final String debugName;
        /* The sample rate of the audio stream we collect from the first request */
        private double sampleRate = -1.0;
        /* Last returned result so we do not return the same string twice */
        private String lastResult = "";
        /* Transcription language requested by the user who requested the transcription */
        private String transcriptionTag = "en-US";

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
            this.debugName = debugName;
            WebSocketClient ws = new WebSocketClient();
            ws.setMaxTextMessageSize(99999);
            ws.start();
            ws.connect(this, new URI(websocketUrl));
        }

        @OnWebSocketClose
        public void onClose(int statusCode, String reason) {
            this.session = null;
        }

        @OnWebSocketConnect
        public void onConnect(Session session) {
            try {
                logger.info("opened connection " + websocketUrl);
                latch.countDown();
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
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            this.session = session;
        }
        private String translateAPI(Translation translation) {
            try {
                HttpClient client = HttpClient.newHttpClient();
                JSONObject jsonRequest = new JSONObject();
//                jsonRequest.put("q", translation.getQ());
//                jsonRequest.put("source", translation.getSource());
//                jsonRequest.put("target", translation.getTarget());
                jsonRequest.put("text", translation.getSource());
                jsonRequest.put("tgt", translation.getTarget());
                String api_key = JigasiBundleActivator.getConfigurationService()
                        .getString(API_KEY, "");
                String end_point = JigasiBundleActivator.getConfigurationService()
                        .getString(END_POINT, "");
                String url = end_point + api_key;
//                logger.info("Translated url: " + url);
                logger.info("request " + jsonRequest.toString());
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(jsonRequest.toString()))
                        .build();

                // Gửi request đồng bộ (hoặc sendAsync nếu muốn không block)
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200) {
                    JSONObject jsonResponse = new JSONObject(response.body());
                    String translatedText = jsonResponse
                            .getString("translation");
                    logger.info("Translated text: " + translatedText);
                    return translatedText;
                } else {
                    logger.warn("Failed to translate: " + response.statusCode());
                }
            } catch (Exception e) {
                logger.error("Error calling translation API", e);
            }

            return null;
        }
        @OnWebSocketMessage
        public void onMessage(String msg) {
            boolean partial = true;
            String result = "";
            if (logger.isDebugEnabled())
                logger.debug(debugName + "Recieved response: " + msg);
            JSONObject jsonObject = new JSONObject(msg);
            logger.info("response: " + jsonObject.toString());
            String message = "";
            try {
                JSONObject dataObject = jsonObject.getJSONObject("data");
                message = dataObject.getString("predict_segment");
                logger.info("active");
                logger.info(username + ": " + message);
            } catch (Exception e) {
            }

//            JSONObject obj = new JSONObject("{\"partial\" : \"" + message + "\"}");
//            if (obj.has("partial")) {
//                result = obj.getString("partial");
//            } else {
//                partial = false;
//                result = obj.getString("text");
//            }
		  result = message;
            if(!result.isEmpty() && !result.equals(lastResult)){
                Translation translation = new Translation(result, Language.EN.getLanguage(), Language.VN.getLanguage());
                String translatedText = translateAPI(translation);
                JSONObject jsonRequest = new JSONObject();
                jsonRequest.put("en", translatedText);
                jsonRequest.put("vi", result);
                result = jsonRequest.toString();
            }
            //if (!result.isEmpty() && (!partial || !result.equals(lastResult))) {
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

        @OnWebSocketError
        public void onError(Throwable cause) {
            logger.error("Error while streaming audio data to transcription service", cause);
        }

        public void sendRequest(TranscriptionRequest request) {
            try {
                BsonDocument document = new BsonDocument();
                document.put("type", new BsonString(EventWsAIEnum.EVENT_RECEIVE_ADMIN_PUSH_AUDIO.getName()));
                BsonDocument data = new BsonDocument();
                data.put("blob_data", new BsonBinary(request.getAudio()));
                data.put("is_end_streaming", new BsonBoolean(false));
                data.put("segment_id", new BsonString(String.valueOf(0)));
                document.put("data", data);
                BasicOutputBuffer buffer = new BasicOutputBuffer();
                BsonDocumentCodec codec = new BsonDocumentCodec();
                codec.encode(new BsonBinaryWriter(buffer), document, EncoderContext.builder().isEncodingCollectibleDocument(true).build());
                byte[] serializedData = buffer.toByteArray();
                session.getRemote().sendBytes(ByteBuffer.wrap(serializedData));

            } catch (Exception e) {
                logger.error("Error to send websocket request for participant " + debugName, e);
            }
        }

        public void addTranscriptionListener(TranscriptionListener listener) {
            listeners.add(listener);
        }

        public void end() {
            try {
                //session.getRemote().sendString(EOF_MESSAGE);
            } catch (Exception e) {
                logger.error("Error to finalize websocket connection for participant " + debugName, e);
            }
        }

        public boolean ended() {
            return session == null;
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

        /* Collect results*/
        private StringBuilder result;

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
                latch.countDown();
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
