package io.smallrye.reactive.messaging.providers.impl;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;

class ConfiguredChannelFactoryTest extends WeldTestBaseWithoutTails {
    public static class MyApp {
        @Incoming("way-in")
        public void consume(String topic) {
            // do nothing
        }
    }

    @Test
    void configuredIncomingChannel() {
        installInitializeAndGet(MyApp.class);
    }

    @SetEnvironmentVariable(key = "MP_MESSAGING_INCOMING_WAY_IN_CONNECTOR", value = "test")
    @Test
    void channelNameWithDashesSupported() {
        installInitializeAndGet(MyApp.class);
    }
}
