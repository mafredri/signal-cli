package org.asamk.signal.manager.internal;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Single;
import org.asamk.signal.manager.Settings;
import org.asamk.signal.manager.api.Profile;
import org.asamk.signal.manager.api.RecipientIdentifier;
import org.asamk.signal.manager.api.ServiceEnvironment;
import org.asamk.signal.manager.config.ServiceConfig;
import org.asamk.signal.manager.helper.AccountFileUpdater;
import org.asamk.signal.manager.helper.Context;
import org.asamk.signal.manager.jobs.DownloadProfileAvatarJob;
import org.asamk.signal.manager.storage.AvatarStore;
import org.asamk.signal.manager.storage.SignalAccount;
import org.asamk.signal.manager.storage.recipients.RecipientAddress;
import org.asamk.signal.manager.util.KeyUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.signal.core.models.ServiceId.ACI;
import org.signal.core.models.ServiceId.PNI;
import org.signal.core.util.UptimeSleepTimer;
import org.signal.libsignal.zkgroup.profiles.ProfileKey;
import org.signal.network.websocket.WebsocketResponse;
import org.whispersystems.signalservice.api.SignalServiceMessageReceiver;
import org.whispersystems.signalservice.api.crypto.ProfileCipherInputStream;
import org.whispersystems.signalservice.api.crypto.ProfileCipherOutputStream;
import org.whispersystems.signalservice.api.push.ServiceIdType;
import org.whispersystems.signalservice.api.services.ProfileService;
import org.whispersystems.signalservice.api.websocket.SignalWebSocket;
import org.whispersystems.signalservice.api.websocket.WebSocketConnectionState;
import org.whispersystems.signalservice.internal.websocket.WebSocketConnection;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Security;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

class ProfileAvatarIdentityTest {
    @TempDir Path directory;
    static final ACI PEER = ACI.parseOrThrow("22222222-2222-4222-8222-222222222222");
    static final ACI OTHER = ACI.parseOrThrow("33333333-3333-4333-8333-333333333333");
    static final String NUMBER = "+12025550101";

    static byte[] jpeg() throws Exception {
        var output = new ByteArrayOutputStream();
        assertTrue(ImageIO.write(new BufferedImage(640, 640, BufferedImage.TYPE_INT_RGB), "jpeg", output));
        return output.toByteArray();
    }

    class Fixture implements AutoCloseable {
        final SignalAccount account;
        final ManagerImpl manager;
        final AvatarStore avatars;
        final AtomicInteger downloads = new AtomicInteger();
        final AccountFileUpdater updater = new AccountFileUpdater() {
            public void updateAccountIdentifiers(String number, ACI aci) {}
            public void removeAccount() {}
        };

        Fixture() throws Exception {
            this(new AvatarStore(directory.resolve("avatars").toFile()));
        }

        Fixture(AvatarStore avatars) throws Exception {
            this.avatars = avatars;
            Security.addProvider(new BouncyCastleProvider());
            account = SignalAccount.createLinkedAccount(directory.resolve("account").toFile(),
                    "offline", ServiceEnvironment.STAGING, Settings.DEFAULT);
            account.setProvisioningData("+12025550100", ACI.parseOrThrow("55555555-5555-4555-8555-555555555555"),
                    null, "offline-password", new byte[]{1}, KeyUtils.generateIdentityKeyPair(),
                    null, KeyUtils.createProfileKey(), null, new byte[32], null);
            account.finishLinking(2, KeyUtils.generatePreKeysForType(account.getAccountData(ServiceIdType.ACI)), null);
            account.getProfileStore().storeProfile(account.getSelfRecipientId(), Profile.newBuilder()
                    .withLastUpdateTimestamp(System.currentTimeMillis())
                    .withUnidentifiedAccessMode(Profile.UnidentifiedAccessMode.DISABLED).build());
            manager = new ManagerImpl(account, PathConfig.createDefault(directory.toFile()), updater,
                    ServiceConfig.getServiceEnvironmentConfig(ServiceEnvironment.STAGING, "offline-avatar-identity"),
                    "offline-avatar-identity");
            install(new SignalDependencies(null, "offline-avatar-identity", null, null, 2, null, null) {
                @Override public SignalServiceMessageReceiver getMessageReceiver() {
                    downloads.incrementAndGet();
                    throw new AssertionError("reading an existing avatar must not download it");
                }
            });
        }

        void install(SignalDependencies dependencies) throws Exception {
            var field = ManagerImpl.class.getDeclaredField("context");
            field.setAccessible(true);
            var previous = (Context) field.get(manager);
            field.set(manager, new Context(account, updater, dependencies, avatars, null, null));
            previous.close();
        }

        Context context() throws Exception {
            var field = ManagerImpl.class.getDeclaredField("context");
            field.setAccessible(true);
            return (Context) field.get(manager);
        }

        void peer(ACI aci, String number) {
            var recipient = account.getRecipientStore().resolveRecipientTrusted(new RecipientAddress(aci, number));
            account.getProfileStore().storeProfileKey(recipient, KeyUtils.createProfileKey());
            account.getProfileStore().storeProfile(recipient, Profile.newBuilder()
                    .withAvatarUrlPath("same-avatar-url").withLastUpdateTimestamp(System.currentTimeMillis()).build());
        }

        byte[] read(ACI aci) throws Exception {
            try (var input = manager.retrieveProfileAvatar(new RecipientIdentifier.Uuid(UUID.fromString(aci.toString())))) {
                return input.readAllBytes();
            }
        }

        void receiver(Download download) throws Exception {
            install(new SignalDependencies(null, "offline-avatar-identity", null, null, 2, null, null) {
                @Override public SignalServiceMessageReceiver getMessageReceiver() {
                    return new SignalServiceMessageReceiver(null) {
                        @Override public InputStream retrieveProfileAvatar(String path, File target, ProfileKey key, long limit) throws IOException {
                            downloads.incrementAndGet();
                            return download.fetch(path, key);
                        }
                    };
                }
            });
        }

        @Override public void close() throws Exception { manager.close(); }
    }

    interface Download { InputStream fetch(String path, ProfileKey key) throws IOException; }

    static byte[] encrypt(byte[] bytes, ProfileKey key) throws Exception {
        var buffer = new ByteArrayOutputStream();
        try (var output = new ProfileCipherOutputStream(buffer, key)) { output.write(bytes); }
        return buffer.toByteArray();
    }

    @Test void scheduledDownloadFailureRetainsObservedURLForReadRetry() throws Exception {
        try (var f = new Fixture()) {
            var self = f.account.getSelfRecipientId();
            f.account.getProfileStore().storeProfile(self, Profile.newBuilder()
                    .withAvatarUrlPath("previous-self-url").withLastUpdateTimestamp(System.currentTimeMillis()).build());
            f.avatars.storeProfileAvatar(f.account.getSelfRecipientAddress(), out -> out.write(new byte[]{1, 2}));
            f.receiver((path, key) -> { throw new IOException("controlled scheduled download failure"); });
            new DownloadProfileAvatarJob("new-self-url").run(f.context());
            assertEquals("new-self-url", f.account.getProfileStore().getProfile(self).getAvatarUrlPath());
            assertNull(f.avatars.retrieveProfileAvatar(f.account.getSelfRecipientAddress()));
            var image = jpeg();
            var encrypted = encrypt(image, f.account.getProfileKey());
            f.receiver((path, key) -> {
                assertEquals("new-self-url", path);
                return new ProfileCipherInputStream(new ByteArrayInputStream(encrypted), key);
            });
            assertArrayEquals(image, f.read(f.account.getAci()));
        }
    }

    @Test void nameOnlySelfUpdatePreservesConcurrentlyPublishedAvatar() throws Exception {
        try (var f = new Fixture()) {
            var self = f.account.getSelfRecipientId();
            f.account.getProfileStore().storeProfile(self, Profile.newBuilder()
                    .withGivenName("Before").withAvatarUrlPath("previous-self-url")
                    .withLastUpdateTimestamp(System.currentTimeMillis()).build());
            var image = jpeg();
            var encrypted = encrypt(image, f.account.getProfileKey());
            var downloading = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            f.receiver((path, key) -> {
                downloading.countDown();
                try {
                    if (!release.await(10, TimeUnit.SECONDS)) { throw new IOException("test synchronization timed out"); }
                } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IOException(e); }
                return new ProfileCipherInputStream(new ByteArrayInputStream(encrypted), key);
            });
            var context = f.context();
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                var job = executor.submit(() -> new DownloadProfileAvatarJob("new-self-url").run(context));
                assertTrue(downloading.await(10, TimeUnit.SECONDS));
                var failure = new java.util.concurrent.atomic.AtomicReference<Throwable>();
                var updater = Thread.ofPlatform().start(() -> {
                    try { context.getProfileHelper().setProfile(false, false, "After", null, null, null, null, null); }
                    catch (Throwable e) { failure.set(e); }
                });
                try {
                    var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                    while (updater.getState() != Thread.State.BLOCKED && updater.isAlive() && System.nanoTime() < deadline) {
                        Thread.onSpinWait();
                    }
                    assertEquals(Thread.State.BLOCKED, updater.getState(), "self update must reach the local publication gate");
                } finally { release.countDown(); }
                job.get(10, TimeUnit.SECONDS);
                updater.join(10000);
                assertFalse(updater.isAlive());
                assertNull(failure.get());
                assertEquals("After", f.account.getProfileStore().getProfile(self).getGivenName());
                assertEquals("new-self-url", f.account.getProfileStore().getProfile(self).getAvatarUrlPath());
                assertArrayEquals(image, f.read(f.account.getAci()));
            }
        }
    }

    class MetadataBoundary implements AutoCloseable {
        final AtomicInteger fetches = new AtomicInteger();
        final SignalWebSocket.AuthenticatedWebSocket socket;

        MetadataBoundary(Fixture f, String avatarPath, Download download) throws Exception {
            var identity = Base64.getEncoder().encodeToString(KeyUtils.generateIdentityKeyPair().getPublicKey().serialize());
            var body = "{\"uuid\":\"" + PEER + "\",\"identityKey\":\"" + identity + "\",\"avatar\":\"" + avatarPath + "\"}";
            var wire = (WebSocketConnection) Proxy.newProxyInstance(WebSocketConnection.class.getClassLoader(),
                    new Class<?>[]{WebSocketConnection.class}, (proxy, method, args) -> switch (method.getName()) {
                        case "getName" -> "offline-profile-wire";
                        case "connect" -> Observable.just(WebSocketConnectionState.CONNECTED);
                        case "isDead", "isConnectedDirectly" -> false;
                        case "disconnect", "shutdown" -> null;
                        case "sendRequest" -> {
                            fetches.incrementAndGet();
                            yield Single.just(new WebsocketResponse(200, body, Map.of(), false));
                        }
                        default -> throw new AssertionError("unexpected wire operation " + method.getName());
                    });
            socket = new SignalWebSocket.AuthenticatedWebSocket(() -> wire, () -> true, new UptimeSleepTimer(), 3600000);
            var service = new ProfileService(null, socket, null);
            f.install(new SignalDependencies(null, "offline-avatar-identity", null, null, 2, null, null) {
                @Override public ProfileService getProfileService() { return service; }
                @Override public SignalServiceMessageReceiver getMessageReceiver() {
                    return new SignalServiceMessageReceiver(null) {
                        @Override public InputStream retrieveProfileAvatar(String path, File target, ProfileKey key, long limit) throws IOException {
                            f.downloads.incrementAndGet();
                            return download.fetch(path, key);
                        }
                    };
                }
            });
        }

        void refresh(Fixture f) {
            var selected = new RecipientIdentifier.Uuid(UUID.fromString(PEER.toString()));
            f.manager.getRecipients(false, Optional.empty(), List.of(selected), Optional.empty());
        }

        @Override public void close() { socket.disconnect(); }
    }

    @Test void failedChangedURLRetiresPreviousBytesThenReadRetriesCurrentAvatar() throws Exception {
        try (var f = new Fixture()) {
            f.peer(PEER, NUMBER);
            var recipient = f.account.getRecipientStore().resolveRecipient(PEER);
            var key = f.account.getProfileStore().getProfileKey(recipient);
            var expected = jpeg();
            var encrypted = encrypt(expected, key);
            f.avatars.storeProfileAvatar(new RecipientAddress(PEER), out -> out.write(expected));
            var fail = new AtomicBoolean(true);
            try (var boundary = new MetadataBoundary(f, "new-avatar-url", (path, supplied) -> {
                assertEquals("new-avatar-url", path);
                if (fail.get()) { throw new IOException("controlled new-version failure"); }
                return new ProfileCipherInputStream(new ByteArrayInputStream(encrypted), supplied);
            })) {
                boundary.refresh(f);
                assertEquals("new-avatar-url", f.account.getProfileStore().getProfile(recipient).getAvatarUrlPath());
                assertNull(f.avatars.retrieveProfileAvatar(new RecipientAddress(PEER)));
                fail.set(false);
                assertArrayEquals(expected, f.read(PEER));
            }
        }
    }

    @Test void failedRetirementCannotCommitNewURLAlongsidePreviousBytes() throws Exception {
        var failingStore = new AvatarStore(directory.resolve("avatars").toFile()) {
            @Override public void deleteProfileAvatar(RecipientAddress address) throws IOException {
                throw new IOException("controlled filesystem retirement failure");
            }
        };
        try (var f = new Fixture(failingStore)) {
            f.peer(PEER, NUMBER);
            var expected = jpeg();
            f.avatars.storeProfileAvatar(new RecipientAddress(PEER), out -> out.write(expected));
            try (var boundary = new MetadataBoundary(f, "new-avatar-url", (path, key) -> {
                throw new AssertionError("retirement failure must stop before download");
            })) {
                boundary.refresh(f);
                var recipient = f.account.getRecipientStore().resolveRecipient(PEER);
                assertEquals("same-avatar-url", f.account.getProfileStore().getProfile(recipient).getAvatarUrlPath());
                assertArrayEquals(expected, f.read(PEER));
                assertEquals(0, f.downloads.get());
            }
        }
    }

    @Test void missingCanonicalAvatarIsDownloadedAndAuthenticatedOnRead() throws Exception {
        try (var f = new Fixture()) {
            f.peer(PEER, NUMBER);
            var key = f.account.getProfileStore().getProfileKey(f.account.getRecipientStore().resolveRecipient(PEER));
            var expected = jpeg();
            var encrypted = encrypt(expected, key);
            f.receiver((path, supplied) -> {
                assertEquals("same-avatar-url", path);
                assertEquals(key, supplied);
                return new ProfileCipherInputStream(new ByteArrayInputStream(encrypted), supplied);
            });
            assertArrayEquals(expected, f.read(PEER));
            assertArrayEquals(expected, f.read(PEER));
            assertEquals(1, f.downloads.get());
            assertTrue(Files.exists(directory.resolve("avatars/profile-" + PEER)));
        }
    }

    @Test void receiverFileNotFoundIsAnIOFailureAndLeavesNoPublishedBytes() throws Exception {
        try (var f = new Fixture()) {
            f.peer(PEER, NUMBER);
            var original = new FileNotFoundException("controlled CDN unavailable");
            f.receiver((path, key) -> { throw original; });
            var error = assertThrows(IOException.class, () -> f.read(PEER));
            assertFalse(error instanceof FileNotFoundException);
            assertSame(original, error.getCause());
            try (var files = Files.list(directory.resolve("avatars"))) { assertEquals(0, files.count()); }
        }
    }

    @Test void failedAuthenticationDoesNotPublishBytesAndRemainsRetryable() throws Exception {
        try (var f = new Fixture()) {
            f.peer(PEER, NUMBER);
            var key = f.account.getProfileStore().getProfileKey(f.account.getRecipientStore().resolveRecipient(PEER));
            var expected = jpeg();
            var encrypted = encrypt(expected, key);
            var invalid = encrypted.clone();
            invalid[invalid.length - 1] ^= 1;
            var attempts = new AtomicInteger();
            f.receiver((path, supplied) -> new ProfileCipherInputStream(
                    new ByteArrayInputStream(attempts.incrementAndGet() == 1 ? invalid : encrypted), supplied));
            assertThrows(IOException.class, () -> f.read(PEER));
            try (var files = Files.list(directory.resolve("avatars"))) { assertEquals(0, files.count()); }
            assertArrayEquals(expected, f.read(PEER));
            assertEquals(2, f.downloads.get());
        }
    }

    @Test void failedConcurrentRequestCannotEraseSuccessfulRequest() throws Exception {
        try (var f = new Fixture(); var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            f.peer(PEER, NUMBER);
            var key = f.account.getProfileStore().getProfileKey(f.account.getRecipientStore().resolveRecipient(PEER));
            var expected = jpeg();
            var encrypted = encrypt(expected, key);
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var attempts = new AtomicInteger();
            f.receiver((path, supplied) -> {
                if (attempts.incrementAndGet() == 1) {
                    entered.countDown();
                    try {
                        if (!release.await(10, TimeUnit.SECONDS)) { throw new IOException("test synchronization timed out"); }
                    } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IOException(e); }
                    throw new IOException("controlled first download failure");
                }
                return new ProfileCipherInputStream(new ByteArrayInputStream(encrypted), supplied);
            });
            var first = executor.submit(() -> f.read(PEER));
            assertTrue(entered.await(10, TimeUnit.SECONDS));
            var secondStarted = new CountDownLatch(1);
            var second = executor.submit(() -> { secondStarted.countDown(); return f.read(PEER); });
            assertTrue(secondStarted.await(10, TimeUnit.SECONDS));
            release.countDown();
            assertInstanceOf(IOException.class, assertThrows(ExecutionException.class, () -> first.get(10, TimeUnit.SECONDS)).getCause());
            assertArrayEquals(expected, second.get(10, TimeUnit.SECONDS));
            assertArrayEquals(expected, f.read(PEER));
            assertEquals(2, f.downloads.get());
        }
    }

    @Test void existingACIImageSurvivesLearningAndChangingPhoneWithoutDownloads() throws Exception {
        try (var f = new Fixture()) {
            f.peer(PEER, null);
            var expected = jpeg();
            f.avatars.storeProfileAvatar(new RecipientAddress(PEER), output -> output.write(expected));
            assertArrayEquals(expected, f.read(PEER));
            var recipient = f.account.getRecipientStore().resolveRecipient(PEER);
            assertEquals(recipient, f.account.getRecipientStore().resolveRecipientTrusted(new RecipientAddress(PEER, NUMBER)));
            assertArrayEquals(expected, f.read(PEER));
            assertEquals(recipient, f.account.getRecipientStore().resolveRecipientTrusted(new RecipientAddress(PEER, "+12025550102")));
            assertArrayEquals(expected, f.read(PEER));
            var saved = ImageIO.read(directory.resolve("avatars/profile-" + PEER).toFile());
            assertEquals(640, saved.getWidth());
            assertEquals(640, saved.getHeight());
            assertEquals(0, f.downloads.get());
        }
    }

    @Test void reassignedPhoneCannotExposePreviousRecipientsLegacyAvatar() throws Exception {
        try (var f = new Fixture()) {
            f.peer(PEER, NUMBER);
            var expected = jpeg();
            f.avatars.storeProfileAvatar(new RecipientAddress(PEER), output -> output.write(expected));
            var legacy = directory.resolve("avatars/profile-" + NUMBER);
            Files.write(legacy, expected);
            f.peer(OTHER, NUMBER);
            var other = f.account.getRecipientStore().resolveRecipient(OTHER);
            f.account.getProfileStore().storeProfile(other, Profile.newBuilder()
                    .withLastUpdateTimestamp(System.currentTimeMillis()).build());
            assertThrows(FileNotFoundException.class, () -> f.read(OTHER));
            assertArrayEquals(expected, f.read(PEER));
            assertArrayEquals(expected, Files.readAllBytes(legacy), "ambiguous historical alias is not copied or deleted");
            assertFalse(Files.exists(directory.resolve("avatars/profile-" + OTHER)));
            assertEquals(0, f.downloads.get());
        }
    }

    @Test void profileRemovalUsesCanonicalIdentityAndLeavesContactAndLegacyFilesAlone() throws Exception {
        try (var f = new Fixture()) {
            f.peer(PEER, NUMBER);
            var address = new RecipientAddress(PEER, NUMBER);
            var image = jpeg();
            f.avatars.storeProfileAvatar(new RecipientAddress(PEER), output -> output.write(image));
            f.avatars.storeContactAvatar(address, output -> output.write(image));
            var legacy = directory.resolve("avatars/profile-" + NUMBER);
            Files.write(legacy, image);
            f.avatars.deleteProfileAvatar(address);
            assertNull(f.avatars.retrieveProfileAvatar(address));
            try (var contact = f.avatars.retrieveContactAvatar(address)) {
                assertArrayEquals(image, contact.getStream().readAllBytes());
            }
            assertArrayEquals(image, Files.readAllBytes(legacy));
            assertEquals(0, f.downloads.get());
        }
    }

    @Test void ProfileFilesRequireACIAndContactPhoneNamesRemainSupported() throws Exception {
        var avatars = new AvatarStore(directory.toFile());
        var image = jpeg();
        var pni = PNI.parseOrThrow("PNI:44444444-4444-4444-8444-444444444444");
        assertThrows(FileNotFoundException.class, () -> avatars.retrieveProfileAvatar(new RecipientAddress(pni, NUMBER)));
        assertThrows(FileNotFoundException.class, () -> avatars.storeProfileAvatar(new RecipientAddress(NUMBER), output -> output.write(image)));
        avatars.storeContactAvatar(new RecipientAddress(NUMBER), output -> output.write(image));
        try (var saved = avatars.retrieveContactAvatar(new RecipientAddress(NUMBER))) {
            assertArrayEquals(image, saved.getStream().readAllBytes());
        }
    }

    @Test void explicitProfileRefreshReconstructsCanonicalBytesAtUnchangedURL() throws Exception {
        try (var f = new Fixture()) {
            f.peer(PEER, NUMBER);
            var recipient = f.account.getRecipientStore().resolveRecipient(PEER);
            var key = f.account.getProfileStore().getProfileKey(recipient);
            var image = jpeg();
            var encrypted = new ByteArrayOutputStream();
            try (var output = new ProfileCipherOutputStream(encrypted, key)) { output.write(image); }
            // Controlled wire response; actual ProfileService parses it and the
            // actual ProfileHelper success callback decrypts/stores the profile.
            var identity = Base64.getEncoder().encodeToString(KeyUtils.generateIdentityKeyPair().getPublicKey().serialize());
            var body = "{\"uuid\":\"" + PEER + "\",\"identityKey\":\"" + identity + "\",\"avatar\":\"same-avatar-url\"}";
            var fetches = new AtomicInteger();
            var wire = (WebSocketConnection) Proxy.newProxyInstance(WebSocketConnection.class.getClassLoader(),
                    new Class<?>[]{WebSocketConnection.class}, (proxy, method, args) -> switch (method.getName()) {
                        case "getName" -> "offline-profile-wire";
                        case "connect" -> Observable.just(WebSocketConnectionState.CONNECTED);
                        case "isDead", "isConnectedDirectly" -> false;
                        case "disconnect", "shutdown" -> null;
                        case "sendRequest" -> {
                            fetches.incrementAndGet();
                            yield Single.just(new WebsocketResponse(200, body, Map.of(), false));
                        }
                        default -> throw new AssertionError("unexpected wire operation " + method.getName());
                    });
            var socket = new SignalWebSocket.AuthenticatedWebSocket(() -> wire, () -> true, new UptimeSleepTimer(), 3600000);
            try {
                var service = new ProfileService(null, socket, null);
                f.install(new SignalDependencies(null, "offline-avatar-identity", null, null, 2, null, null) {
                    @Override public ProfileService getProfileService() { return service; }
                    @Override public SignalServiceMessageReceiver getMessageReceiver() {
                        return new SignalServiceMessageReceiver(null) {
                            @Override public InputStream retrieveProfileAvatar(String path, File target, ProfileKey supplied, long limit) throws java.io.IOException {
                                assertEquals("same-avatar-url", path);
                                assertEquals(key, supplied);
                                f.downloads.incrementAndGet();
                                return new ProfileCipherInputStream(new ByteArrayInputStream(encrypted.toByteArray()), supplied);
                            }
                        };
                    }
                });
                // The legacy alias is deliberately not imported. The actual
                // explicit-recipient Manager operation must rebuild via crypto.
                var legacy = directory.resolve("avatars/profile-" + NUMBER);
                Files.createDirectories(legacy.getParent());
                Files.write(legacy, new byte[]{9, 9});
                var selected = new RecipientIdentifier.Uuid(UUID.fromString(PEER.toString()));
                f.manager.getRecipients(false, Optional.empty(), List.of(selected), Optional.empty());
                assertArrayEquals(image, f.read(PEER));
                var firstFetches = fetches.get();
                f.manager.getRecipients(false, Optional.empty(), List.of(selected), Optional.empty());
                assertTrue(firstFetches > 0 && fetches.get() > firstFetches,
                        "both explicit refreshes reach the real ProfileService");
                assertEquals(1, f.downloads.get(), "available canonical bytes skip redundant transfers");
                assertArrayEquals(new byte[]{9, 9}, Files.readAllBytes(legacy));
            } finally { socket.disconnect(); }
        }
    }
}
