package com.microsoft.azure.eventprocessorhost;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;

public class JsonDecoder implements Decoder {

    @Override
    public Object decode(final byte[] bytes) throws Exception {
        return new ObjectMapper().readValue(bytes, Object.class);
    }
}
