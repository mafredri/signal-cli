package org.asamk.signal.manager.internal;

import org.asamk.signal.manager.Settings;
import org.asamk.signal.manager.api.Profile;
import org.asamk.signal.manager.api.ServiceEnvironment;
import org.asamk.signal.manager.helper.Context;
import org.asamk.signal.manager.helper.ProfileHelper;
import org.asamk.signal.manager.storage.AvatarStore;
import org.asamk.signal.manager.storage.SignalAccount;
import org.asamk.signal.manager.storage.recipients.RecipientAddress;
import org.asamk.signal.manager.storage.recipients.RecipientId;
import org.asamk.signal.manager.util.KeyUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.signal.core.models.ServiceId.ACI;
import org.signal.libsignal.zkgroup.profiles.ProfileKey;
import org.whispersystems.signalservice.api.SignalServiceMessageReceiver;
import org.whispersystems.signalservice.api.crypto.ProfileCipherInputStream;
import org.whispersystems.signalservice.api.crypto.ProfileCipherOutputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProfileAvatarTest {

    private static final ACI PEER = ACI.parseOrThrow("11111111-1111-4111-8111-111111111111");
    private static final String SELF_NUMBER = "+12025550100";

    @TempDir
    Path directory;

    /** Encrypted avatars by CDN path; a missing path fails the download. */
    private final Map<String, byte[]> cdn = new HashMap<>();
    private final AtomicInteger downloads = new AtomicInteger();
    /** If set, a download waits for it after counting down downloadStarted. */
    private CountDownLatch downloadStarted;
    private CountDownLatch releaseDownload;

    private SignalAccount account;
    private AvatarStore avatars;
    private Context context;
    private ProfileHelper helper;
    private RecipientId peer;
    private ProfileKey peerKey;

    @BeforeEach
    void setUp() throws IOException {
        account = SignalAccount.createLinkedAccount(directory.resolve("account").toFile(),
                "offline",
                ServiceEnvironment.STAGING,
                Settings.DEFAULT);
        account.setProvisioningData(SELF_NUMBER,
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
        final var dependencies = new SignalDependencies(null, "offline-avatar-test", null, null, 2, null, null) {
            @Override
            public SignalServiceMessageReceiver getMessageReceiver() {
                return new SignalServiceMessageReceiver(null) {
                    @Override
                    public InputStream retrieveProfileAvatar(
                            String path,
                            File destination,
                            ProfileKey profileKey,
                            long maxSizeBytes
                    ) throws IOException {
                        downloads.incrementAndGet();
                        if (releaseDownload != null) {
                            downloadStarted.countDown();
                            try {
                                if (!releaseDownload.await(10, TimeUnit.SECONDS)) {
                                    throw new IOException("test download was not released");
                                }
                            } catch (InterruptedException e) {
                                throw new IOException(e);
                            }
                        }
                        final var encrypted = cdn.get(path);
                        if (encrypted == null) {
                            throw new IOException("controlled download failure");
                        }
                        return new ProfileCipherInputStream(new ByteArrayInputStream(encrypted), profileKey);
                    }
                };
            }
        };
        avatars = new AvatarStore(directory.resolve("avatars").toFile());
        context = new Context(account, null, dependencies, avatars, null, null);
        helper = context.getProfileHelper();

        // Seen by its ACI only, as before its phone number is known
        peer = account.getRecipientStore().resolveRecipient(new RecipientAddress(PEER));
        peerKey = KeyUtils.createProfileKey();
        account.getProfileStore().storeProfileKey(peer, peerKey);
    }

    @AfterEach
    void tearDown() throws Exception {
        context.close();
        account.close();
    }

    @Test
    void avatarIsFoundAfterThePhoneNumberBecomesKnown() throws Exception {
        publish("avatar-1", "first avatar");
        storeFreshProfile("avatar-1");
        helper.downloadProfileAvatar(peer, "avatar-1", peerKey);

        final var merged = account.getRecipientStore().resolveRecipientTrusted(new RecipientAddress(PEER, "+12025550101"));
        assertEquals(peer, merged);
        assertArrayEquals(bytes("first avatar"), read());
        assertEquals(1, downloads.get(), "the stored avatar is read without downloading it again");
    }

    @Test
    void missingAvatarIsDownloadedWhenRead() throws Exception {
        publish("avatar-1", "first avatar");
        storeFreshProfile("avatar-1");

        assertArrayEquals(bytes("first avatar"), read());
        assertArrayEquals(bytes("first avatar"), read());
        assertEquals(1, downloads.get());
    }

    @Test
    void failedDownloadOnReadIsAnErrorAndRetriedOnTheNextRead() throws Exception {
        storeFreshProfile("avatar-1");

        final var e = assertThrows(IOException.class, this::read);
        assertFalse(e instanceof FileNotFoundException, "a failed download is not a missing avatar");
        assertFalse(avatars.hasProfileAvatar(PEER));

        publish("avatar-1", "first avatar");
        assertArrayEquals(bytes("first avatar"), read());
    }

    @Test
    void changedAvatarWhoseDownloadFailsDoesNotKeepThePreviousOne() throws Exception {
        publish("avatar-1", "first avatar");
        storeFreshProfile("avatar-1");
        helper.downloadProfileAvatar(peer, "avatar-1", peerKey);

        helper.downloadProfileAvatar(peer, "avatar-2", peerKey);
        assertEquals("avatar-2", account.getProfileStore().getProfile(peer).getAvatarUrlPath());
        assertFalse(avatars.hasProfileAvatar(PEER), "the previous avatar is not shown for the new path");

        publish("avatar-2", "second avatar");
        helper.downloadProfileAvatar(peer, "avatar-2", peerKey);
        assertArrayEquals(bytes("second avatar"), read());
    }

    @Test
    void removedAvatarIsDeleted() throws Exception {
        publish("avatar-1", "first avatar");
        storeFreshProfile("avatar-1");
        helper.downloadProfileAvatar(peer, "avatar-1", peerKey);

        helper.downloadProfileAvatar(peer, null, peerKey);
        assertNull(account.getProfileStore().getProfile(peer).getAvatarUrlPath());
        assertNull(helper.retrieveProfileAvatar(peer));
    }

    @Test
    void avatarRemovedWhileAReadDownloadsItStaysRemoved() throws Exception {
        publish("avatar-1", "first avatar");
        storeFreshProfile("avatar-1");
        downloadStarted = new CountDownLatch(1);
        releaseDownload = new CountDownLatch(1);
        try (final var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            final var read = executor.submit(() -> helper.retrieveProfileAvatar(peer));
            assertTrue(downloadStarted.await(10, TimeUnit.SECONDS));
            helper.downloadProfileAvatar(peer, null, peerKey);
            releaseDownload.countDown();
            assertNull(read.get(10, TimeUnit.SECONDS));
        }
        assertFalse(avatars.hasProfileAvatar(PEER));
        assertNull(helper.retrieveProfileAvatar(peer));
    }

    @Test
    void ownAvatarStoredUnderThePhoneNumberIsUsed() throws Exception {
        final var self = account.getSelfRecipientId();
        account.getProfileStore()
                .storeProfile(self,
                        Profile.newBuilder()
                                .withAvatarUrlPath("own-avatar")
                                .withLastUpdateTimestamp(System.currentTimeMillis())
                                .build());
        final var avatarsDir = directory.resolve("avatars");
        Files.createDirectories(avatarsDir);
        Files.write(avatarsDir.resolve("profile-" + SELF_NUMBER), bytes("own avatar"));

        final var details = helper.retrieveProfileAvatar(self);
        assertNotNull(details);
        try (final var stream = details.getStream()) {
            assertArrayEquals(bytes("own avatar"), stream.readAllBytes());
        }
        assertEquals(0, downloads.get());
        assertFalse(Files.exists(avatarsDir.resolve("profile-" + SELF_NUMBER)));
    }

    @Test
    void profileWithoutAvatarIsNotDownloaded() throws Exception {
        storeFreshProfile(null);

        assertNull(helper.retrieveProfileAvatar(peer));
        assertEquals(0, downloads.get());
    }

    private void publish(String path, String avatar) throws IOException {
        final var encrypted = new ByteArrayOutputStream();
        try (final var output = new ProfileCipherOutputStream(encrypted, peerKey)) {
            output.write(bytes(avatar));
        }
        cdn.put(path, encrypted.toByteArray());
    }

    /** A profile fetched just now, so reading it does not fetch it from the server. */
    private void storeFreshProfile(String avatarPath) {
        account.getProfileStore()
                .storeProfile(peer,
                        Profile.newBuilder()
                                .withAvatarUrlPath(avatarPath)
                                .withLastUpdateTimestamp(System.currentTimeMillis())
                                .build());
    }

    private byte[] read() throws IOException {
        final var details = helper.retrieveProfileAvatar(peer);
        assertNotNull(details, "the profile has an avatar");
        try (final var stream = details.getStream()) {
            return stream.readAllBytes();
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
