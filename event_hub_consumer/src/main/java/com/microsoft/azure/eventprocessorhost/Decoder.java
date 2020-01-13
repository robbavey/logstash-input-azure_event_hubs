package com.microsoft.azure.eventprocessorhost;


public interface Decoder {
    public Object decode(byte[] bytes) throws Exception;
}
