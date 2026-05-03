package org.nur.protocol;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class RespWriter {

    private final OutputStream output;

    public RespWriter(OutputStream output) {
        this.output = output;
    }

    public void write(RespValue value) throws IOException {
        output.write(RespSerializer.serialize(value).getBytes(StandardCharsets.UTF_8));
        output.flush();
    }
}
