package se.irori.kroxylicious.filter.storage;

import lombok.Getter;

@Getter
public class OversizeValueReference {

    public static final String HEADER_KEY = "oversize-reference";

    private final String ref;

    private OversizeValueReference(String ref) {
        this.ref = ref;
    }

    public static OversizeValueReference of(String ref) {
        return new OversizeValueReference(ref);
    }
}
