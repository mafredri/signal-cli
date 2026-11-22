package org.asamk.signal.manager.helper;

import org.signal.core.models.ServiceId;
import org.whispersystems.signalservice.api.push.exceptions.ProofRequiredException;
import org.whispersystems.signalservice.api.push.exceptions.RateLimitException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * When fetching a profile failed for another reason than an unregistered recipient, the next fetch that is not
 * forced waits with a backoff. Kept in memory only, so a restart fetches such profiles again. Keyed by service id,
 * because a RecipientId changes when recipients are merged.
 */
final class FailedProfileFetches {

    private static final long FIRST_RETRY_MILLIS = TimeUnit.SECONDS.toMillis(30);

    private final long maxRetryMillis;
    private final Map<ServiceId, Failure> failures = new ConcurrentHashMap<>();

    private record Failure(int count, long retryAt) {}

    FailedProfileFetches(final long maxRetryMillis) {
        this.maxRetryMillis = maxRetryMillis;
    }

    boolean isEmpty() {
        return failures.isEmpty();
    }

    boolean contains(final ServiceId serviceId) {
        return failures.containsKey(serviceId);
    }

    /**
     * Whether a fetch of a failed profile has to wait at the given time.
     */
    boolean isWaiting(final ServiceId serviceId, final long now) {
        final var failure = failures.get(serviceId);
        return failure != null && now < failure.retryAt();
    }

    /**
     * Records a failed fetch and returns when to fetch again. A deadline the server asked for earlier is kept, even
     * when a later failure would allow an earlier retry.
     */
    long failed(final ServiceId serviceId, final Throwable e, final long now) {
        return failures.compute(serviceId, (id, previous) -> {
            final var count = previous == null ? 1 : previous.count() + 1;
            final var retryAt = now + retryDelay(count, e);
            return new Failure(count, previous == null ? retryAt : Math.max(previous.retryAt(), retryAt));
        }).retryAt();
    }

    void succeeded(final ServiceId serviceId) {
        failures.remove(serviceId);
    }

    /**
     * Doubles from 30s up to the maximum, and is at least as long as the server asked for.
     */
    long retryDelay(final int count, final Throwable e) {
        final var backoff = FIRST_RETRY_MILLIS << Math.min(Math.max(count - 1, 0), 20);
        var delay = Math.min(backoff, maxRetryMillis);
        if (e instanceof RateLimitException rateLimit && rateLimit.getRetryAfterMilliseconds().isPresent()) {
            delay = Math.max(delay, rateLimit.getRetryAfterMilliseconds().get());
        } else if (e instanceof ProofRequiredException proofRequired) {
            delay = Math.max(delay, TimeUnit.SECONDS.toMillis(proofRequired.getRetryAfterSeconds()));
        }
        return delay;
    }
}
