package org.jitsi.jigasi.transcription.utils;

import lombok.Getter;

@Getter
public enum Language {
    VN("Vietnamese","vi") ,
    EN("English", "en") ,
    JA("Japanese", "ja") ;

    private final String language;
    private final String languageCode;
    Language(String language, String languageCode) {
        this.language = language;
        this.languageCode = languageCode;
    }

}
