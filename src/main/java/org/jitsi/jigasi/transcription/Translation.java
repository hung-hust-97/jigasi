package org.jitsi.jigasi.transcription;

import lombok.Getter;
import lombok.Setter;
import org.jitsi.jigasi.transcription.utils.Language;

@Setter
@Getter
public class Translation {
    private String q;
    private String target;
    private String source;

    public Translation(String q, String target, String source) {
        this.q = q;
        this.target = target;
        this.source = source;
    }

}
