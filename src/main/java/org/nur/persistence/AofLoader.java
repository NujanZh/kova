package org.nur.persistence;

import org.nur.commands.CommandHandler;
import org.nur.exception.ClientDisconnectedException;
import org.nur.exception.RespProtocolException;
import org.nur.protocol.RespParser;
import org.nur.protocol.RespValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class AofLoader {

    private static final Logger log = LoggerFactory.getLogger(AofLoader.class);

    private final CommandHandler handler;

    public AofLoader(CommandHandler handler) {
        this.handler = handler;
    }

    public void load(Path path) throws IOException {
        log.info("Starting AOF replay from '{}'", path);
        long startTime = System.currentTimeMillis();
        int commandCount = 0;
        int errorCount = 0;

        try (var inputStream = Files.newInputStream(path)) {
            RespParser parser = new RespParser(inputStream);

            while (true) {
                try {
                    RespValue command = parser.parse();
                    RespValue result = handler.handle(command);

                    if (result instanceof RespValue.Error error) {
                        log.warn(
                                "Command during replay returned error — {}: {}",
                                error.code(),
                                error.message());
                        errorCount++;
                    } else {
                        commandCount++;
                        log.debug("Replayed command #{}: {}", commandCount, command);
                    }

                } catch (ClientDisconnectedException e) {
                    break;
                } catch (RespProtocolException e) {
                    log.error(
                            "AOF file is corrupted at command #{}, replay stopped: {}",
                            commandCount + 1,
                            e.getMessage());
                    break;
                }
            }
        }

        long elapsed = System.currentTimeMillis() - startTime;
        log.info(
                "AOF replay complete — {} command(s) replayed, {} error(s), took {}ms",
                commandCount,
                errorCount,
                elapsed);

        if (errorCount > 0) {
            log.warn(
                    "{} command(s) failed during replay — storage state may be incomplete",
                    errorCount);
        }
    }
}
