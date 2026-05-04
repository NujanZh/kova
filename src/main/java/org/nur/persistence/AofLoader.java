package org.nur.persistence;

import org.nur.commands.CommandHandler;
import org.nur.exception.ClientDisconnectedException;
import org.nur.protocol.RespParser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class AofLoader {

    public final CommandHandler handler;

    public AofLoader(CommandHandler handler) {
        this.handler = handler;
    }

    public void load(Path path) throws IOException {
        try (var inputStream = Files.newInputStream(path)) {
            RespParser parser = new RespParser(inputStream);
            while (true) {
                try {
                    handler.handle(parser.parse());
                } catch (ClientDisconnectedException ignored) {
                    break;
                }
            }
        }
    }
}
