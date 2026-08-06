package org.jitsi.jigasi.whip;

import io.socket.client.IO;
import io.socket.client.Socket;
import org.json.JSONObject;
import org.jitsi.utils.logging.Logger;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class WhipGatewaySession {
    private final static Logger logger = Logger.getLogger(WhipGatewaySession.class);

    private final WhipConnectDto dto;
    private Process gstProcess;
    private OutputStream gstOutputStream;

    private Socket socketIo;
    private CMeetStompClient cMeetStompClient;

    private volatile boolean running = false;
    private String sessionId;
    private int segmentId = 0;

    private final ByteArrayOutputStream pcmBuffer = new ByteArrayOutputStream();
    private final CountDownLatch sessionReadyLatch = new CountDownLatch(1);

    public WhipGatewaySession(WhipConnectDto dto) {
        this.dto = dto;
    }

    public void start() throws Exception {
        this.running = true;

        // 1. Launch GStreamer WHIP pipeline if whipEndpoint is specified
        if (dto.getWhipEndpoint() != null && !dto.getWhipEndpoint().trim().isEmpty()) {
            try {
                ProcessBuilder pb = new ProcessBuilder(
                        "gst-launch-1.0",
                        "fdsrc", "fd=0", "!",
                        "rawaudioparse", "format=pcm", "pcm-format=s16le", "sample-rate=48000", "num-channels=1", "!",
                        "audioconvert", "!",
                        "audioresample", "!",
                        "opusenc", "!",
                        "rtpopuspay", "!",
                        "rtpopusdepay", "!",
                        "opusparse", "!",
                        "whipclientsink", "signaller::whip-endpoint=" + dto.getWhipEndpoint()
                );
                pb.redirectErrorStream(true);
                this.gstProcess = pb.start();
                this.gstOutputStream = gstProcess.getOutputStream();
                logger.info("Started GStreamer WHIP subprocess for room: " + dto.getRoomId());
            } catch (Exception e) {
                logger.error("Failed to start GStreamer WHIP subprocess: " + e.getMessage(), e);
            }
        }

        // 2. Setup Socket.IO and STOMP if useSocketIo is enabled
        if (Boolean.TRUE.equals(dto.getUseSocketIo())) {
            String voiceAiUrl = dto.getVoiceAiUrl();
            if (voiceAiUrl == null || voiceAiUrl.trim().isEmpty()) {
                voiceAiUrl = org.jitsi.jigasi.JigasiBundleActivator.getConfigurationService() != null
                        ? org.jitsi.jigasi.JigasiBundleActivator.getConfigurationService()
                            .getString("org.jitsi.jigasi.whip.voice_ai_url", "https://stream-voices.cmcati.vn")
                        : "https://stream-voices.cmcati.vn";
            }

            String cmeetStompUrl = dto.getCmeetStompUrl();
            if (cmeetStompUrl == null || cmeetStompUrl.trim().isEmpty()) {
                cmeetStompUrl = org.jitsi.jigasi.JigasiBundleActivator.getConfigurationService() != null
                        ? org.jitsi.jigasi.JigasiBundleActivator.getConfigurationService()
                            .getString("org.jitsi.jigasi.whip.cmeet_stomp_url", "https://sec.cmcati.vn/cmeet-server-socket/ws")
                        : "https://sec.cmcati.vn/cmeet-server-socket/ws";
            }

            if (dto.getAuthHeader() != null && !dto.getAuthHeader().trim().isEmpty()) {
                try {
                    this.cMeetStompClient = new CMeetStompClient(cmeetStompUrl, dto.getAuthHeader());
                    this.cMeetStompClient.connect();
                } catch (Exception e) {
                    logger.error("Failed to connect CMeetStompClient: " + e.getMessage(), e);
                }
            }

            IO.Options opts = new IO.Options();
            opts.transports = new String[]{"websocket"};
            opts.path = "/raw";
            opts.reconnection = true;
            opts.reconnectionAttempts = 5;
            opts.reconnectionDelay = 2000;

            this.socketIo = IO.socket(voiceAiUrl, opts);
            setupSocketListeners();
            this.socketIo.connect();
        }
    }

    private void setupSocketListeners() {
        socketIo.on(Socket.EVENT_CONNECT, args -> {
            logger.info("Socket.IO connected for room " + dto.getRoomId() + ". Initializing session...");
            JSONObject payload = new JSONObject();
            payload.put("session_id", "11111111111111");
            socketIo.emit("session_request", payload);

            JSONObject payload1 = new JSONObject();
            socketIo.emit("set_loop_emit_raw", payload1);
        });

        socketIo.on("session_confirm", args -> {
            if (args.length > 0) {
                this.sessionId = args[0].toString();
                logger.info("Socket.IO session confirmed. Session ID: " + sessionId + " for room: " + dto.getRoomId());
                sessionReadyLatch.countDown();
            }
        });

        socketIo.on("asr_uttered", args -> {
            if (args.length > 0 && cMeetStompClient != null) {
                String rawJson = args[0].toString();
                try {
                    JSONObject json = new JSONObject(rawJson);
                    int segId = json.optInt("segment_id", 0);
                    String text = json.optString("predict_segment", "");
                    String timeSheetId = dto.getTimeSheetId() != null ? dto.getTimeSheetId() : dto.getRoomId();
                    cMeetStompClient.publishPrediction(timeSheetId, segId, text);
                } catch (Exception e) {
                    logger.error("Failed to parse prediction json: " + e.getMessage());
                }
            }
        });
    }

    public synchronized void onAudioBuffer(byte[] pcmData, int offset, int length) {
        if (!running) return;

        // Pipe audio to GStreamer WHIP subprocess
        if (gstOutputStream != null) {
            try {
                gstOutputStream.write(pcmData, offset, length);
                gstOutputStream.flush();
            } catch (Exception e) {
                // ignore
            }
        }

        // Accumulate and resample for Socket.IO Voice AI
        if (socketIo != null && socketIo.connected() && sessionId != null) {
            pcmBuffer.write(pcmData, offset, length);

            // 48kHz mono 16-bit = 96000 bytes per second.
            if (pcmBuffer.size() >= 96000) {
                byte[] raw48k = pcmBuffer.toByteArray();
                byte[] chunk16k = downsample48kTo16k(raw48k, 0, 96000);

                JSONObject payload = new JSONObject();
                payload.put("blob_data", chunk16k);
                payload.put("segment_id", segmentId++);
                payload.put("session_id", sessionId);
                socketIo.emit("user_uttered", payload);

                pcmBuffer.reset();
                if (raw48k.length > 96000) {
                    pcmBuffer.write(raw48k, 96000, raw48k.length - 96000);
                }
            }
        }
    }

    private byte[] downsample48kTo16k(byte[] pcm48k, int offset, int length) {
        int sampleCount = length / 2;
        int outSampleCount = sampleCount / 3;
        byte[] out = new byte[outSampleCount * 2];

        int outIdx = 0;
        for (int i = 0; i < outSampleCount; i++) {
            int srcIdx = offset + (i * 3) * 2;
            out[outIdx] = pcm48k[srcIdx];
            out[outIdx + 1] = pcm48k[srcIdx + 1];
            outIdx += 2;
        }
        return out;
    }

    public void stop() {
        logger.info("Stopping WhipGatewaySession for room " + dto.getRoomId() + "...");
        this.running = false;

        if (gstOutputStream != null) {
            try {
                gstOutputStream.close();
            } catch (Exception e) {
                // ignore
            }
        }

        if (gstProcess != null && gstProcess.isAlive()) {
            gstProcess.destroy();
        }

        if (socketIo != null) {
            socketIo.disconnect();
            socketIo.close();
        }

        if (cMeetStompClient != null) {
            cMeetStompClient.disconnect();
        }

        logger.info("WhipGatewaySession stopped for room " + dto.getRoomId());
    }
}
