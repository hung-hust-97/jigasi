package org.jitsi.jigasi.transcription;

import lombok.Getter;
import lombok.Setter;
import org.jitsi.jigasi.transcription.utils.Language;

@Setter
@Getter
public class Translation {
    private String q;
    private Language target;
    private Language source;

    public Translation(String q, Language target, Language source) {
        this.q = q;
        this.target = target;
        this.source = source;
    }

}
