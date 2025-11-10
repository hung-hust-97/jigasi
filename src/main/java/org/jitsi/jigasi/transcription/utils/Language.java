package org.jitsi.jigasi.transcription.utils;

import lombok.Getter;

@Getter
public enum Language {
    VN("vi") ,
    EN("en") ,
    JA("ja") ;

    private final String language;
    Language(String language) {
        this.language = language;
    }

}
