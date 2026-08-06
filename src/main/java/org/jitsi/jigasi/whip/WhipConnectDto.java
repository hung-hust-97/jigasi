package org.jitsi.jigasi.whip;

public class WhipConnectDto {
    private String roomId;
    private String domain;
    private String whipEndpoint;
    private String xmppDomain;
    private String nickname = "CMEET-BOT";
    private Boolean isRecord = false;
    private Boolean useSocketIo = true;
    private String timeSheetId;
    private String authHeader;
    private String voiceAiUrl;
    private String cmeetStompUrl;

    public String getRoomId() {
        return roomId;
    }

    public void setRoomId(String roomId) {
        this.roomId = roomId;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getWhipEndpoint() {
        return whipEndpoint;
    }

    public void setWhipEndpoint(String whipEndpoint) {
        this.whipEndpoint = whipEndpoint;
    }

    public String getXmppDomain() {
        return xmppDomain;
    }

    public void setXmppDomain(String xmppDomain) {
        this.xmppDomain = xmppDomain;
    }

    public String getNickname() {
        return nickname;
    }

    public void setNickname(String nickname) {
        this.nickname = nickname;
    }

    public Boolean getIsRecord() {
        return isRecord;
    }

    public void setIsRecord(Boolean isRecord) {
        this.isRecord = isRecord;
    }

    public Boolean getUseSocketIo() {
        return useSocketIo;
    }

    public void setUseSocketIo(Boolean useSocketIo) {
        this.useSocketIo = useSocketIo;
    }

    public String getTimeSheetId() {
        return timeSheetId;
    }

    public void setTimeSheetId(String timeSheetId) {
        this.timeSheetId = timeSheetId;
    }

    public String getAuthHeader() {
        return authHeader;
    }

    public void setAuthHeader(String authHeader) {
        this.authHeader = authHeader;
    }

    public String getVoiceAiUrl() {
        return voiceAiUrl;
    }

    public void setVoiceAiUrl(String voiceAiUrl) {
        this.voiceAiUrl = voiceAiUrl;
    }

    public String getCmeetStompUrl() {
        return cmeetStompUrl;
    }

    public void setCmeetStompUrl(String cmeetStompUrl) {
        this.cmeetStompUrl = cmeetStompUrl;
    }
}
