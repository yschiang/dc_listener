package dc.listener.spec;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class SpecParserTest {

    static final String FULL = """
        sessions:
          tool-a:
            desiredState: RUNNING
            configVersion: v1
            config:
              subject: tool.a.events
              durable: listener-tool-a
              retry:
                interval: 5s
                maxAttempts: 3
              drainTimeout: 10s
        """;

    @Test
    void parsesFullSpec() throws Exception {
        var p = SpecParser.parse(FULL);
        assertTrue(p.invalid().isEmpty());
        SessionSpec s = p.valid().get("tool-a");
        assertEquals(DesiredState.RUNNING, s.desiredState());
        assertEquals("v1", s.configVersion());
        assertEquals("tool.a.events", s.subject());
        assertEquals("listener-tool-a", s.durable());
        assertEquals(Duration.ofSeconds(5), s.retryInterval());
        assertEquals(3, s.maxAttempts());
        assertEquals(Duration.ofSeconds(10), s.drainTimeout());
    }

    @Test
    void appliesDefaults() throws Exception {
        var p = SpecParser.parse("""
            sessions:
              tool-a:
                desiredState: STANDBY
                configVersion: v1
                config:
                  subject: tool.a.events
                  durable: listener-tool-a
            """);
        SessionSpec s = p.valid().get("tool-a");
        assertNull(s.retryInterval());
        assertEquals(10, s.maxAttempts());
        assertEquals(Duration.ofSeconds(30), s.drainTimeout());
    }

    @Test
    void invalidEntryDoesNotPoisonOthers() throws Exception {
        var p = SpecParser.parse("""
            sessions:
              tool-a:
                desiredState: RUNNING
                configVersion: v1
                config:
                  subject: tool.a.events
                  durable: listener-tool-a
              tool-x:
                desiredState: RUNNING
                configVersion: v1
                config:
                  durable: listener-tool-x
            """);
        assertTrue(p.valid().containsKey("tool-a"));
        assertTrue(p.invalid().get("tool-x").contains("subject"));
    }

    @Test
    void badDesiredStateIsInvalidEntry() throws Exception {
        var p = SpecParser.parse("""
            sessions:
              tool-a:
                desiredState: BANANAS
                configVersion: v1
                config:
                  subject: tool.a.events
                  durable: listener-tool-a
            """);
        assertTrue(p.valid().isEmpty());
        assertFalse(p.invalid().get("tool-a").isEmpty());
    }

    @Test
    void brokenYamlThrows() {
        assertThrows(SpecParser.SpecParseException.class,
                () -> SpecParser.parse("sessions: [oops"));
    }

    @Test
    void missingSessionsKeyThrows() {
        assertThrows(SpecParser.SpecParseException.class,
                () -> SpecParser.parse("hello: world"));
    }

    @Test
    void badDurationIsInvalidEntry() throws Exception {
        var p = SpecParser.parse("""
            sessions:
              tool-a:
                desiredState: RUNNING
                configVersion: v1
                config:
                  subject: tool.a.events
                  durable: listener-tool-a
                  drainTimeout: soon
            """);
        assertTrue(p.invalid().get("tool-a").contains("soon"));
    }

    @Test
    void nonStringSessionNameIsParseError() {
        assertThrows(SpecParser.SpecParseException.class, () -> SpecParser.parse("""
            sessions:
              123:
                desiredState: RUNNING
                configVersion: v1
                config:
                  subject: tool.a.events
                  durable: dur-a
            """));
    }
}
