package org.asamk.signal.manager.internal;

import org.asamk.signal.manager.Settings;
import org.asamk.signal.manager.api.Profile;
import org.asamk.signal.manager.api.ServiceEnvironment;
import org.asamk.signal.manager.helper.Context;
import org.asamk.signal.manager.storage.SignalAccount;
import org.asamk.signal.manager.storage.recipients.RecipientAddress;
import org.asamk.signal.manager.util.KeyUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.signal.core.models.ServiceId.ACI;
import org.signal.core.util.UptimeSleepTimer;
import org.whispersystems.signalservice.api.services.ProfileService;
import org.whispersystems.signalservice.api.websocket.SignalWebSocket;
import org.whispersystems.signalservice.api.websocket.WebSocketUnavailableException;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProfileFetchRetryTest {

    @TempDir
    Path directory;

    @Test
    void failedFetchIsRetriedAfterABackoffAndKeepsTheStoredProfile() throws Exception {
        final var requests = new AtomicInteger();
        final var socket = new SignalWebSocket.AuthenticatedWebSocket(() -> {
            requests.incrementAndGet();
            throw new WebSocketUnavailableException("controlled offline connection failure");
        }, () -> true, new UptimeSleepTimer(), 0);
        final var profileService = new ProfileService(null, socket, null);
        final var dependencies = new SignalDependencies(null, "offline-profile-test", null, null, 2, null, null) {
            @Override
            public ProfileService getProfileService() {
                return profileService;
            }
        };

        try (final var account = SignalAccount.createLinkedAccount(directory.resolve("account").toFile(),
                "offline",
                ServiceEnvironment.STAGING,
                Settings.DEFAULT)) {
            account.setProvisioningData(null,
                    ACI.parseOrThrow("22222222-2222-4222-8222-222222222222"),
                    null,
                    "offline-password",
                    new byte[]{1},
                    KeyUtils.generateIdentityKeyPair(),
                    null,
                    KeyUtils.createProfileKey(),
                    null,
                    new byte[32],
                    null);
            final var address = new RecipientAddress(ACI.parseOrThrow("11111111-1111-4111-8111-111111111111"));
            final var recipient = account.getRecipientStore().resolveRecipient(address);
            account.getProfileStore().storeProfileKey(recipient, KeyUtils.createProfileKey());
            // Older than the six hour cache, so the next lookup fetches it
            final var lastFetched = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(7);
            account.getProfileStore()
                    .storeProfile(recipient,
                            Profile.newBuilder()
                                    .withGivenName("Previously observed name")
                                    .withLastUpdateTimestamp(lastFetched)
                                    .build());
            try (final var context = new Context(account, null, dependencies, null, null, null)) {
                final var helper = context.getProfileHelper();

                final var profile = helper.getRecipientProfile(recipient);
                final var attempts = requests.get();
                assertTrue(attempts > 0, "the stale profile must be fetched");
                assertEquals("Previously observed name", profile.getGivenName());
                assertEquals(lastFetched,
                        profile.getLastUpdateTimestamp(),
                        "a failed fetch is not a profile update");

                helper.getRecipientProfile(recipient);
                assertEquals(attempts, requests.get(), "a lookup within the backoff must not fetch again");

                helper.refreshRecipientProfile(recipient);
                assertTrue(requests.get() > attempts, "a forced refresh ignores the backoff");
            }
        } finally {
            socket.disconnect();
        }
    }
}
