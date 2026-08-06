package org.jitsi.jigasi.whip;

import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.simp.stomp.*;
import org.springframework.web.socket.WebSocketHttpHeaders;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;
import org.springframework.web.socket.sockjs.client.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jitsi.utils.logging.Logger;

public class CMeetStompClient {
    private final static Logger logger = Logger.getLogger(CMeetStompClient.class);

    private final String url;
    private final String authHeader;
    private StompSession session;
    private WebSocketStompClient stompClient;

    private volatile boolean autoReconnect = true;
    private final AtomicBoolean isReconnecting = new AtomicBoolean(false);
    private final ScheduledExecutorService reconnectScheduler = Executors.newSingleThreadScheduledExecutor();

    public CMeetStompClient(String url, String authHeader) {
        this.url = url;
        this.authHeader = authHeader;
    }

    public void connect() throws Exception {
        autoReconnect = true;
        connectInternal();
    }

    private void connectInternal() throws Exception {
        if (this.stompClient != null) {
            try {
                this.stompClient.stop();
            } catch (Exception e) {
                // ignore
            }
        }

        boolean isSockJs = url.startsWith("http://") || url.startsWith("https://");

        logger.info("Connecting to cmeet STOMP server: " + url + " (using SockJS=" + isSockJs + ") with AuthHeader=" + (authHeader != null));

        if (isSockJs) {
            List<Transport> transports = new ArrayList<>(2);
            transports.add(new WebSocketTransport(new StandardWebSocketClient()));
            transports.add(new RestTemplateXhrTransport());
            SockJsClient sockJsClient = new SockJsClient(transports);
            this.stompClient = new WebSocketStompClient(sockJsClient);
        } else {
            this.stompClient = new WebSocketStompClient(new StandardWebSocketClient());
        }

        this.stompClient.setMessageConverter(new MappingJackson2MessageConverter());

        WebSocketHttpHeaders handshakeHeaders = new WebSocketHttpHeaders();
        StompHeaders stompConnectHeaders = new StompHeaders();

        if (authHeader != null && !authHeader.trim().isEmpty()) {
            handshakeHeaders.add("Authorization", authHeader);
            stompConnectHeaders.add("Authorization", authHeader);
        }

        CompletableFuture<StompSession> future = stompClient.connectAsync(
                url, handshakeHeaders, stompConnectHeaders, new StompSessionHandlerAdapter() {
            @Override
            public void afterConnected(StompSession session, StompHeaders connectedHeaders) {
                CMeetStompClient.this.session = session;
                logger.info("Successfully connected to cmeet STOMP server.");
            }

            @Override
            public void handleException(StompSession session, StompCommand command, StompHeaders headers, byte[] payload, Throwable exception) {
                logger.error("STOMP client exception: " + exception.getMessage(), exception);
            }

            @Override
            public void handleTransportError(StompSession session, Throwable exception) {
                logger.error("STOMP transport error: " + exception.getMessage(), exception);
                scheduleReconnect();
            }
        });

        try {
            future.get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            Throwable cause = e.getCause();
            String errorMsg = (cause != null) ? cause.getMessage() : e.getMessage();
            logger.error("Failed to connect to cmeet STOMP: " + errorMsg);
            throw new RuntimeException("Failed to connect to STOMP server: " + errorMsg, cause);
        }
    }

    private boolean isAuthenticationError(Throwable e) {
        if (e == null) return false;
        String msg = e.getMessage();
        if (msg != null && (msg.contains("401") || msg.contains("403") || msg.contains("Unauthorized") || msg.contains("Forbidden"))) {
            return true;
        }
        return isAuthenticationError(e.getCause());
    }

    private void scheduleReconnect() {
        if (!autoReconnect) {
            return;
        }
        if (isReconnecting.compareAndSet(false, true)) {
            logger.info("Scheduling cmeet STOMP reconnect in 5 seconds...");
            reconnectScheduler.schedule(() -> {
                try {
                    logger.info("Attempting to reconnect to cmeet STOMP server...");
                    connectInternal();
                    isReconnecting.set(false);
                } catch (Exception e) {
                    isReconnecting.set(false);
                    if (isAuthenticationError(e)) {
                        logger.error("CRITICAL: Permanent connection failure to cmeet STOMP due to expired/invalid token (401/403). Stopping auto-reconnect.");
                        autoReconnect = false;
                    } else {
                        logger.error("Reconnection attempt failed: " + e.getMessage() + ". Will retry.");
                        scheduleReconnect();
                    }
                }
            }, 5, TimeUnit.SECONDS);
        }
    }

    public void publishPrediction(String timeSheetId, int segmentId, String text) {
        if (session == null || !session.isConnected()) {
            logger.error("STOMP Session is not active. Cannot publish prediction.");
            return;
        }

        try {
            Map<String, Object> bodyMap = new HashMap<>();
            bodyMap.put("segment_id", segmentId);
            bodyMap.put("predict_segment", text);
            bodyMap.put("predict_segment_final", text);

            String destination = "/app/speech-to-text/predict-data/" + timeSheetId;

            StompHeaders sendHeaders = new StompHeaders();
            sendHeaders.setDestination(destination);

            session.send(sendHeaders, bodyMap);
        } catch (Exception e) {
            logger.error("Failed to publish STOMP prediction: " + e.getMessage(), e);
        }
    }

    public void disconnect() {
        autoReconnect = false;
        try {
            reconnectScheduler.shutdownNow();
        } catch (Exception e) {
            // ignore
        }
        if (session != null && session.isConnected()) {
            session.disconnect();
            logger.info("Disconnected from cmeet STOMP server.");
        }
        if (stompClient != null) {
            stompClient.stop();
        }
    }
}
