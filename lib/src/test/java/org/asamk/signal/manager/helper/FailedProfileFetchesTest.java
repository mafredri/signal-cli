package org.asamk.signal.manager.helper;

import org.junit.jupiter.api.Test;
import org.signal.core.models.ServiceId.ACI;
import org.whispersystems.signalservice.api.push.exceptions.ProofRequiredException;
import org.whispersystems.signalservice.api.push.exceptions.RateLimitException;
import org.whispersystems.signalservice.internal.push.ProofRequiredResponse;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FailedProfileFetchesTest {

    private static final long CACHE = TimeUnit.HOURS.toMillis(6);
    private static final ACI PEER = ACI.parseOrThrow("11111111-1111-4111-8111-111111111111");
    private static final ACI OTHER = ACI.parseOrThrow("22222222-2222-4222-8222-222222222222");
    private static final IOException NETWORK_ERROR = new IOException("connection reset");

    private final FailedProfileFetches fetches = new FailedProfileFetches(CACHE);

    @Test
    void retryDelayDoublesFromThirtySeconds() {
        assertEquals(TimeUnit.SECONDS.toMillis(30), fetches.retryDelay(1, NETWORK_ERROR));
        assertEquals(TimeUnit.SECONDS.toMillis(60), fetches.retryDelay(2, NETWORK_ERROR));
        assertEquals(TimeUnit.MINUTES.toMillis(4), fetches.retryDelay(4, NETWORK_ERROR));
    }

    @Test
    void retryDelayNeverExceedsTheProfileCache() {
        assertEquals(CACHE, fetches.retryDelay(11, NETWORK_ERROR));
        assertEquals(CACHE, fetches.retryDelay(1000, NETWORK_ERROR));
    }

    @Test
    void retryDelayIsAtLeastWhatTheServerAsks() {
        final var twoHours = TimeUnit.HOURS.toMillis(2);
        assertEquals(twoHours, fetches.retryDelay(1, new RateLimitException(429, "rate limited", Optional.of(twoHours))));
        assertEquals(TimeUnit.MINUTES.toMillis(4),
                fetches.retryDelay(4, new RateLimitException(429, "rate limited", Optional.of(1000L))));
        assertEquals(TimeUnit.SECONDS.toMillis(30), fetches.retryDelay(1, new RateLimitException(429, "rate limited")));
        assertEquals(TimeUnit.HOURS.toMillis(1),
                fetches.retryDelay(1,
                        new ProofRequiredException(new ProofRequiredResponse("token", List.of("captcha")), 3600)));
    }

    @Test
    void aFailedFetchWaitsUntilItsRetryTime() {
        final var retryAt = fetches.failed(PEER, NETWORK_ERROR, 1000);
        assertEquals(1000 + TimeUnit.SECONDS.toMillis(30), retryAt);
        assertTrue(fetches.isWaiting(PEER, retryAt - 1));
        assertFalse(fetches.isWaiting(PEER, retryAt));
        assertFalse(fetches.isWaiting(OTHER, 1000));
    }

    @Test
    void consecutiveFailuresWaitLonger() {
        fetches.failed(PEER, NETWORK_ERROR, 0);
        final var second = fetches.failed(PEER, NETWORK_ERROR, TimeUnit.SECONDS.toMillis(30));
        assertEquals(TimeUnit.SECONDS.toMillis(30 + 60), second);
    }

    @Test
    void aLaterFailureKeepsTheDeadlineTheServerAskedFor() {
        final var twoHours = TimeUnit.HOURS.toMillis(2);
        fetches.failed(PEER, new RateLimitException(429, "rate limited", Optional.of(twoHours)), 0);
        final var retryAt = fetches.failed(PEER, NETWORK_ERROR, 1000);
        assertEquals(twoHours, retryAt);
        assertTrue(fetches.isWaiting(PEER, twoHours - 1));
    }

    @Test
    void successClearsTheFailure() {
        fetches.failed(PEER, NETWORK_ERROR, 0);
        assertTrue(fetches.contains(PEER));
        fetches.succeeded(PEER);
        assertFalse(fetches.contains(PEER));
        assertFalse(fetches.isWaiting(PEER, 0));
        assertTrue(fetches.isEmpty());
        assertEquals(1000 + TimeUnit.SECONDS.toMillis(30), fetches.failed(PEER, NETWORK_ERROR, 1000),
                "the backoff starts over after a success");
    }
}
