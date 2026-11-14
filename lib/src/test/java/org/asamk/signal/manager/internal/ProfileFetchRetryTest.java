package org.asamk.signal.manager.internal;

import org.asamk.signal.manager.Settings;
import org.asamk.signal.manager.api.Profile;
import org.asamk.signal.manager.api.ServiceEnvironment;
import org.asamk.signal.manager.helper.Context;
import org.asamk.signal.manager.storage.AvatarStore;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProfileFetchRetryTest {

    @TempDir
    Path directory;

    @Test
    void failedRemoteProfileFetchDoesNotBecomeASuccessfulSixHourCacheEntry() throws Exception {
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
            account.getProfileStore().storeProfile(recipient,
                    Profile.newBuilder().withGivenName("Previously observed name").build());
            final var avatars = new AvatarStore(directory.resolve("avatars").toFile());
            try (final var context = new Context(account, null, dependencies, avatars, null, null)) {
                final var helper = context.getProfileHelper();
                final var first = helper.getRecipientProfile(recipient);
                assertEquals("Previously observed name", first.getGivenName(), "keep previous metadata on failure");
                final var firstAttempts = requests.get();
                assertTrue(firstAttempts > 0, "actual ProfileService must reach the controlled websocket boundary");
                helper.getRecipientProfile(recipient);
                assertTrue(requests.get() > firstAttempts,
                        "failed metadata fetch must remain retryable rather than appear fresh for six hours");
                assertEquals(0, account.getProfileStore().getProfile(recipient).getLastUpdateTimestamp(),
                        "a transport failure is not a successful profile update");
            }
        } finally {
            socket.disconnect();
        }
    }
}
