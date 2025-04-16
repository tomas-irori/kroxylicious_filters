package se.irori.kroxylicious.filter;

import lombok.Getter;

@Getter
public class OversizeReference {

    public static final String HEADER_KEY = "oversize-reference";

    private final String ref;

    private OversizeReference(String ref) {
        this.ref = ref;
    }

    public static OversizeReference of(String ref) {
        return new OversizeReference(ref);
    }
}
