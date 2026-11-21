package com.example.jobs;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ContextConfigurationAttributes;
import org.springframework.test.context.ContextCustomizer;
import org.springframework.test.context.ContextCustomizerFactory;
import org.springframework.test.context.MergedContextConfiguration;
import redis.embedded.RedisServer;

/** Starts one real Redis per test JVM on a free port and points every Spring test context at it. */
public class EmbeddedRedisContextCustomizerFactory implements ContextCustomizerFactory {

    private static int port;

    @Override
    public ContextCustomizer createContextCustomizer(
            Class<?> testClass, List<ContextConfigurationAttributes> configAttributes) {
        return new RedisCustomizer();
    }

    static synchronized int startOnce() {
        if (port == 0) {
            try (ServerSocket socket = new ServerSocket(0)) {
                port = socket.getLocalPort();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            try {
                RedisServer server = RedisServer.newRedisServer()
                        .port(port).bind("127.0.0.1").setting("save \"\"").setting("appendonly no").build();
                server.start();
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    try {
                        server.stop();
                    } catch (IOException ignored) {
                        // JVM is exiting
                    }
                }));
            } catch (IOException e) {
                throw new IllegalStateException("Could not start embedded Redis", e);
            }
        }
        return port;
    }

    /** All instances are equal so the test context cache is unaffected. */
    private static final class RedisCustomizer implements ContextCustomizer {

        @Override
        public void customizeContext(ConfigurableApplicationContext context, MergedContextConfiguration config) {
            TestPropertyValues.of("spring.data.redis.host=127.0.0.1", "spring.data.redis.port=" + startOnce())
                    .applyTo(context);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof RedisCustomizer;
        }

        @Override
        public int hashCode() {
            return RedisCustomizer.class.hashCode();
        }
    }
}
