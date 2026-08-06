package org.jitsi.jigasi.whip;

import org.jitsi.utils.logging.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class WhipGateway {
    private final static Logger logger = Logger.getLogger(WhipGateway.class);

    private static final WhipGateway INSTANCE = new WhipGateway();
    private final Map<String, WhipGatewaySession> sessionMap = new ConcurrentHashMap<>();

    private WhipGateway() {
    }

    public static WhipGateway getInstance() {
        return INSTANCE;
    }

    public synchronized WhipGatewaySession startWhipSession(WhipConnectDto dto) throws Exception {
        if (dto.getRoomId() == null || dto.getRoomId().trim().isEmpty()) {
            throw new IllegalArgumentException("roomId cannot be null or empty");
        }

        String actualRoomId = dto.getRoomId().endsWith("_record") 
                ? dto.getRoomId().substring(0, dto.getRoomId().length() - 7) 
                : dto.getRoomId();

        boolean isRecord = Boolean.TRUE.equals(dto.getIsRecord()) || dto.getRoomId().endsWith("_record");
        String keyProcess = isRecord ? actualRoomId + "_record_whip" : actualRoomId + "_whip";

        WhipGatewaySession existingSession = sessionMap.get(keyProcess);
        if (existingSession != null) {
            logger.info("Stopping existing WhipGatewaySession for key: " + keyProcess);
            existingSession.stop();
            sessionMap.remove(keyProcess);
        }

        dto.setRoomId(actualRoomId);
        dto.setIsRecord(isRecord);

        WhipGatewaySession session = new WhipGatewaySession(dto);
        session.start();
        sessionMap.put(keyProcess, session);

        logger.info("Started new WhipGatewaySession for key: " + keyProcess);
        return session;
    }

    public synchronized boolean stopWhipSession(String roomId, Boolean isRecord) {
        if (roomId == null || roomId.trim().isEmpty()) {
            return false;
        }

        boolean recordMode = Boolean.TRUE.equals(isRecord) || roomId.endsWith("_record");
        String actualRoomId = roomId.endsWith("_record") 
                ? roomId.substring(0, roomId.length() - 7) 
                : roomId;

        String keyProcess = recordMode ? actualRoomId + "_record_whip" : actualRoomId + "_whip";

        WhipGatewaySession session = sessionMap.remove(keyProcess);
        if (session != null) {
            session.stop();
            logger.info("Stopped WhipGatewaySession for key: " + keyProcess);
            return true;
        } else {
            logger.warn("No active WhipGatewaySession found for key: " + keyProcess);
            return false;
        }
    }
}
