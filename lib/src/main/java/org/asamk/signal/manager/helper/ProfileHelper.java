package org.asamk.signal.manager.helper;

import org.asamk.signal.manager.api.GroupNotFoundException;
import org.asamk.signal.manager.api.NotAGroupMemberException;
import org.asamk.signal.manager.api.PhoneNumberSharingMode;
import org.asamk.signal.manager.api.Profile;
import org.asamk.signal.manager.config.ServiceConfig;
import org.asamk.signal.manager.internal.SignalDependencies;
import org.asamk.signal.manager.jobs.SyncStorageJob;
import org.asamk.signal.manager.storage.SignalAccount;
import org.asamk.signal.manager.storage.groups.GroupInfoV2;
import org.asamk.signal.manager.storage.recipients.RecipientId;
import org.asamk.signal.manager.util.IOUtils;
import org.asamk.signal.manager.util.KeyUtils;
import org.asamk.signal.manager.util.PaymentUtils;
import org.asamk.signal.manager.util.ProfileUtils;
import org.asamk.signal.manager.util.Utils;
import org.jetbrains.annotations.Nullable;
import org.signal.core.models.ServiceId.ACI;
import org.signal.core.util.ExpiringProfileCredentialUtil;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.InvalidKeyException;
import org.signal.libsignal.zkgroup.profiles.ExpiringProfileKeyCredential;
import org.signal.libsignal.zkgroup.profiles.ProfileKey;
import org.signal.network.crypto.SealedSenderAccess;
import org.signal.network.exceptions.PushNetworkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.signalservice.api.NetworkResultUtil;
import org.whispersystems.signalservice.api.profiles.AvatarUploadParams;
import org.whispersystems.signalservice.api.profiles.ProfileAndCredential;
import org.whispersystems.signalservice.api.profiles.SignalServiceProfile;
import org.whispersystems.signalservice.api.push.SignalServiceAddress;
import org.whispersystems.signalservice.api.push.exceptions.NotFoundException;
import org.whispersystems.signalservice.api.services.ProfileService;
import org.whispersystems.signalservice.api.util.StreamDetails;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;

public final class ProfileHelper {

    private static final Logger logger = LoggerFactory.getLogger(ProfileHelper.class);

    // Profiles are cached for 6h before retrieving them again, unless forced
    private static final long PROFILE_CACHE_MILLIS = TimeUnit.HOURS.toMillis(6);

    private final SignalAccount account;
    private final SignalDependencies dependencies;
    private final Context context;

    private final FailedProfileFetches failedFetches = new FailedProfileFetches(PROFILE_CACHE_MILLIS);

    /**
     * Serialize, per ACI, changes to a stored profile avatar together with the avatar path recorded for it, so that
     * an avatar downloaded for one path is not stored after the path changed. Downloads run outside the lock.
     */
    private final Object[] avatarLocks = IntStream.range(0, 16).mapToObj(i -> new Object()).toArray();

    public ProfileHelper(final Context context) {
        this.account = context.getAccount();
        this.dependencies = context.getDependencies();
        this.context = context;
    }

    public void rotateProfileKey() throws IOException {
        // refresh our profile, before creating a new profile key
        getSelfProfile();
        // The stored avatar is uploaded again below, encrypted with the new key. Download it while the old key can
        // still decrypt it, in case it is not stored locally yet; without it the avatar stays encrypted with the
        // old key, as before.
        try (final var ignored = retrieveProfileAvatar(account.getSelfRecipientId())) {
        } catch (IOException e) {
            logger.warn("Failed to download own profile avatar before rotating the profile key: {}", e.getMessage());
        }
        var profileKey = KeyUtils.createProfileKey();
        account.setProfileKey(profileKey);
        context.getAccountHelper().updateAccountAttributes();
        setProfile(true, true, null, null, null, null, null, null);
        account.getRecipientStore().rotateSelfStorageId();
        context.getJobExecutor().enqueueJob(new SyncStorageJob());

        final var recipientIds = account.getRecipientStore().getRecipientIdsWithEnabledProfileSharing();
        for (final var recipientId : recipientIds) {
            context.getSendHelper().sendProfileKey(recipientId);
        }

        final var selfRecipientId = account.getSelfRecipientId();
        final var activeGroupIds = account.getGroupStore()
                .getGroups()
                .stream()
                .filter(g -> g instanceof GroupInfoV2 && g.isMember(selfRecipientId) && g.isProfileSharingEnabled())
                .map(g -> (GroupInfoV2) g)
                .map(GroupInfoV2::getGroupId)
                .toList();
        for (final var groupId : activeGroupIds) {
            try {
                context.getGroupHelper().updateGroupProfileKey(groupId);
            } catch (GroupNotFoundException | NotAGroupMemberException | IOException e) {
                logger.warn("Failed to update group profile key: {}", e.getMessage());
            }
        }
    }

    public Profile getRecipientProfile(RecipientId recipientId) {
        return getRecipientProfile(recipientId, false);
    }

    public List<Profile> getRecipientProfiles(Collection<RecipientId> recipientIds) {
        return getRecipientProfiles(recipientIds, false);
    }

    public boolean refreshRecipientProfile(RecipientId recipientId) {
        try {
            blockingGetProfile(retrieveProfile(recipientId, SignalServiceProfile.RequestType.PROFILE, false));
            return true;
        } catch (IOException e) {
            logger.warn("Failed to retrieve profile for {}, ignoring: {}",
                    context.getRecipientHelper().resolveSignalServiceAddress(recipientId).getIdentifier(),
                    e.getMessage());
            return false;
        }
    }

    public void refreshRecipientProfiles(Collection<RecipientId> recipientIds) {
        getRecipientProfiles(recipientIds, true);
    }

    public List<ExpiringProfileKeyCredential> getExpiringProfileKeyCredential(List<RecipientId> recipientIds) {
        final var profileFetches = Flowable.fromIterable(recipientIds)
                .filter(recipientId -> !ExpiringProfileCredentialUtil.isValid(account.getProfileStore()
                        .getExpiringProfileKeyCredential(recipientId)))
                .map(recipientId -> retrieveProfile(recipientId,
                        SignalServiceProfile.RequestType.PROFILE_AND_CREDENTIAL,
                        false).onErrorComplete());
        Maybe.merge(profileFetches, 10).blockingSubscribe();

        return recipientIds.stream().map(r -> account.getProfileStore().getExpiringProfileKeyCredential(r)).toList();
    }

    public ExpiringProfileKeyCredential getExpiringProfileKeyCredential(RecipientId recipientId) {
        var profileKeyCredential = account.getProfileStore().getExpiringProfileKeyCredential(recipientId);
        if (ExpiringProfileCredentialUtil.isValid(profileKeyCredential)) {
            return profileKeyCredential;
        }

        try {
            blockingGetProfile(retrieveProfile(recipientId,
                    SignalServiceProfile.RequestType.PROFILE_AND_CREDENTIAL,
                    false));
        } catch (IOException e) {
            logger.warn("Failed to retrieve profile key credential for {}, ignoring: {}",
                    context.getRecipientHelper().resolveSignalServiceAddress(recipientId).getIdentifier(),
                    e.getMessage());
            return null;
        }

        return account.getProfileStore().getExpiringProfileKeyCredential(recipientId);
    }

    /**
     * @param givenName  if null, the previous givenName will be kept
     * @param familyName if null, the previous familyName will be kept
     * @param about      if null, the previous about text will be kept
     * @param aboutEmoji if null, the previous about emoji will be kept
     * @param avatar     if avatar is null the image from the local avatar store is used (if present),
     */
    public void setProfile(
            String givenName,
            final String familyName,
            String about,
            String aboutEmoji,
            Optional<String> avatar,
            byte[] mobileCoinAddress
    ) throws IOException {
        setProfile(true, false, givenName, familyName, about, aboutEmoji, avatar, mobileCoinAddress);
    }

    public void setProfile(
            boolean uploadProfile,
            boolean forceUploadAvatar,
            String givenName,
            final String familyName,
            String about,
            String aboutEmoji,
            Optional<String> avatar,
            byte[] mobileCoinAddress
    ) throws IOException {
        var profile = getSelfProfile();
        var builder = profile == null ? Profile.newBuilder() : Profile.newBuilder(profile);
        if (givenName != null) {
            builder.withGivenName(givenName);
        }
        if (familyName != null) {
            builder.withFamilyName(familyName);
        }
        if (about != null) {
            builder.withAbout(about);
        }
        if (aboutEmoji != null) {
            builder.withAboutEmoji(aboutEmoji);
        }
        if (mobileCoinAddress != null) {
            builder.withMobileCoinAddress(mobileCoinAddress);
        }
        var newProfile = builder.build();

        if (uploadProfile) {
            final var streamDetails = avatar != null && avatar.isPresent()
                    ? Utils.createStreamDetails(avatar.get())
                    .first()
                    : forceUploadAvatar && avatar == null ? context.getAvatarStore()
                            .retrieveProfileAvatar(account.getAci()) : null;
            try (streamDetails) {
                final var avatarUploadParams = streamDetails != null
                        ? AvatarUploadParams.forAvatar(streamDetails)
                        : avatar == null ? AvatarUploadParams.unchanged(true) : AvatarUploadParams.unchanged(false);
                final var paymentsAddress = Optional.ofNullable(newProfile.getMobileCoinAddress())
                        .map(address -> PaymentUtils.signPaymentsAddress(address,
                                account.getAciIdentityKeyPair().getPrivateKey()))
                        .orElse(null);
                logger.debug("Uploading new profile");
                final var avatarPath = NetworkResultUtil.toSetProfileLegacy(dependencies.getProfileApi()
                        .setVersionedProfile(account.getAci(),
                                account.getProfileKey(),
                                newProfile.getInternalServiceName(),
                                newProfile.getAbout() == null ? "" : newProfile.getAbout(),
                                newProfile.getAboutEmoji() == null ? "" : newProfile.getAboutEmoji(),
                                paymentsAddress,
                                avatarUploadParams,
                                List.of(/* TODO implement support for badges */),
                                account.getConfigurationStore().getPhoneNumberSharingMode()
                                        == PhoneNumberSharingMode.EVERYBODY));
                if (!avatarUploadParams.keepTheSame) {
                    builder.withAvatarUrlPath(avatarPath);
                }
                newProfile = builder.build();
            }
        }

        synchronized (avatarLock(account.getAci())) {
            if (avatar != null) {
                if (avatar.isPresent()) {
                    try (final var streamDetails = Utils.createStreamDetails(avatar.get()).first()) {
                        context.getAvatarStore()
                                .storeProfileAvatar(account.getAci(),
                                        outputStream -> IOUtils.copyStream(streamDetails.getStream(), outputStream));
                    }
                } else {
                    context.getAvatarStore().deleteProfileAvatar(account.getAci());
                }
            }
            account.getProfileStore().storeProfile(account.getSelfRecipientId(), newProfile);
        }
    }

    public Profile getSelfProfile() {
        return getRecipientProfile(account.getSelfRecipientId());
    }

    private List<Profile> getRecipientProfiles(Collection<RecipientId> recipientIds, boolean force) {
        final var profileStore = account.getProfileStore();
        final var profileFetches = Flowable.fromIterable(recipientIds)
                .filter(recipientId -> force || isProfileRefreshRequired(recipientId,
                        profileStore.getProfile(recipientId)))
                .map(recipientId -> retrieveProfile(recipientId,
                        SignalServiceProfile.RequestType.PROFILE,
                        false).onErrorComplete());
        Maybe.merge(profileFetches, 10).blockingSubscribe();

        return recipientIds.stream().map(profileStore::getProfile).toList();
    }

    private Profile getRecipientProfile(RecipientId recipientId, boolean force) {
        var profile = account.getProfileStore().getProfile(recipientId);

        if (!force && !isProfileRefreshRequired(recipientId, profile)) {
            return profile;
        }

        try {
            blockingGetProfile(retrieveProfile(recipientId, SignalServiceProfile.RequestType.PROFILE, false));
        } catch (IOException e) {
            logger.warn("Failed to retrieve profile for {}, ignoring: {}",
                    context.getRecipientHelper().resolveSignalServiceAddress(recipientId).getIdentifier(),
                    e.getMessage());
        }

        return account.getProfileStore().getProfile(recipientId);
    }

    private boolean isProfileRefreshRequired(final RecipientId recipientId, final Profile profile) {
        final var now = System.currentTimeMillis();
        if (!failedFetches.isEmpty()) {
            final var serviceId = account.getRecipientAddressResolver().resolveRecipientAddress(recipientId).serviceId();
            if (serviceId.isPresent() && failedFetches.contains(serviceId.get())) {
                return !failedFetches.isWaiting(serviceId.get(), now);
            }
        }
        return profile == null || now - profile.getLastUpdateTimestamp() >= PROFILE_CACHE_MILLIS;
    }

    public void downloadProfileAvatar(
            final RecipientId recipientId,
            final String avatarPath,
            final ProfileKey profileKey
    ) {
        final var aci = account.getRecipientAddressResolver().resolveRecipientAddress(recipientId).aci();
        final boolean download;
        synchronized (aci.map(this::avatarLock).orElse(avatarLocks[0])) {
            final var profile = account.getProfileStore().getProfile(recipientId);
            final var pathChanged = profile == null || !Objects.equals(avatarPath, profile.getAvatarUrlPath());
            if (pathChanged) {
                if (aci.isPresent()) {
                    // Remove the previous avatar first, so a failed download does not leave it under the new path
                    try {
                        context.getAvatarStore().deleteProfileAvatar(aci.get());
                    } catch (IOException e) {
                        logger.warn("Failed to delete local profile avatar, ignoring: {}", e.getMessage());
                    }
                }
                var builder = profile == null ? Profile.newBuilder() : Profile.newBuilder(profile);
                account.getProfileStore().storeProfile(recipientId, builder.withAvatarUrlPath(avatarPath).build());
            }
            // Also download an unchanged avatar that is not stored, for example after an earlier download failed
            download = aci.isPresent() && avatarPath != null && !context.getAvatarStore().hasProfileAvatar(aci.get());
        }
        if (download) {
            logger.trace("Downloading profile avatar for {}", recipientId);
            try {
                storeDownloadedProfileAvatar(recipientId, aci.get(), avatarPath, profileKey);
            } catch (Throwable e) {
                logger.warn("Failed to download profile avatar, ignoring: {}", e.getMessage());
            }
        }
    }

    /**
     * Returns the stored profile avatar, downloading it first if the profile has one that is not stored yet, or null
     * if the profile has no avatar.
     */
    public StreamDetails retrieveProfileAvatar(final RecipientId recipientId) throws IOException {
        final var profile = getRecipientProfile(recipientId);
        final var aci = account.getRecipientAddressResolver().resolveRecipientAddress(recipientId).aci();
        if (aci.isEmpty()) {
            return null;
        }
        final var avatarStore = context.getAvatarStore();
        if (recipientId.equals(account.getSelfRecipientId())) {
            synchronized (avatarLock(aci.get())) {
                avatarStore.moveLegacySelfProfileAvatar(account.getSelfRecipientAddress(), aci.get());
            }
        }
        final var stored = avatarStore.retrieveProfileAvatar(aci.get());
        if (stored != null || profile == null || profile.getAvatarUrlPath() == null) {
            return stored;
        }
        final var profileKey = account.getProfileStore().getProfileKey(recipientId);
        if (profileKey == null) {
            return null;
        }
        logger.trace("Downloading missing profile avatar for {}", recipientId);
        storeDownloadedProfileAvatar(recipientId, aci.get(), profile.getAvatarUrlPath(), profileKey);
        return avatarStore.retrieveProfileAvatar(aci.get());
    }

    /**
     * Downloads the avatar at avatarPath and stores it, unless the profile's avatar path changed meanwhile.
     */
    private void storeDownloadedProfileAvatar(
            final RecipientId recipientId,
            final ACI aci,
            final String avatarPath,
            final ProfileKey profileKey
    ) throws IOException {
        final var avatar = new ByteArrayOutputStream();
        retrieveProfileAvatar(avatarPath, profileKey, avatar);
        synchronized (avatarLock(aci)) {
            final var profile = account.getProfileStore().getProfile(recipientId);
            if (profile == null || !avatarPath.equals(profile.getAvatarUrlPath())) {
                logger.debug("Profile avatar of {} changed while it was downloaded, not storing it", recipientId);
                return;
            }
            context.getAvatarStore().storeProfileAvatar(aci, avatar::writeTo);
        }
    }

    private Object avatarLock(final ACI aci) {
        return avatarLocks[Math.floorMod(aci.hashCode(), avatarLocks.length)];
    }

    private ProfileAndCredential blockingGetProfile(Single<ProfileAndCredential> profile) throws IOException {
        try {
            return profile.blockingGet();
        } catch (RuntimeException e) {
            if (e.getCause() instanceof PushNetworkException) {
                throw (PushNetworkException) e.getCause();
            } else if (e.getCause() instanceof NotFoundException) {
                throw (NotFoundException) e.getCause();
            } else {
                throw new IOException(e);
            }
        }
    }

    private Single<ProfileAndCredential> retrieveProfile(
            RecipientId recipientId,
            SignalServiceProfile.RequestType requestType,
            final boolean ignoreAvatars
    ) {
        var unidentifiedAccess = getUnidentifiedAccess(recipientId);
        var profileKey = Optional.ofNullable(account.getProfileStore().getProfileKey(recipientId));

        logger.trace("Retrieving profile for {} {}",
                recipientId,
                profileKey.isPresent() ? "with profile key" : "without profile key");
        final var address = context.getRecipientHelper().resolveSignalServiceAddress(recipientId);
        return retrieveProfile(address, profileKey, unidentifiedAccess, requestType).doOnSuccess(p -> {
            logger.trace("Got new profile for {}", recipientId);
            final var encryptedProfile = p.getProfile();

            if (requestType == SignalServiceProfile.RequestType.PROFILE_AND_CREDENTIAL
                    || !ExpiringProfileCredentialUtil.isValid(account.getProfileStore()
                    .getExpiringProfileKeyCredential(recipientId))) {
                logger.trace("Storing profile credential");
                final var profileKeyCredential = p.getExpiringProfileKeyCredential().orElse(null);
                account.getProfileStore().storeExpiringProfileKeyCredential(recipientId, profileKeyCredential);
            }

            final var profile = account.getProfileStore().getProfile(recipientId);

            Profile newProfile = null;
            if (profileKey.isPresent()) {
                logger.trace("Decrypting profile");
                final var avatarPath = encryptedProfile.getAvatar();
                if (!ignoreAvatars) {
                    downloadProfileAvatar(recipientId, avatarPath, profileKey.get());
                }

                newProfile = ProfileUtils.decryptProfile(profileKey.get(), encryptedProfile);
            }

            if (newProfile == null) {
                newProfile = (
                        profile == null ? Profile.newBuilder() : Profile.newBuilder(profile)
                ).withLastUpdateTimestamp(System.currentTimeMillis())
                        .withUnidentifiedAccessMode(ProfileUtils.getUnidentifiedAccessMode(encryptedProfile, null))
                        .withCapabilities(ProfileUtils.getCapabilities(encryptedProfile))
                        .build();
            }

            if (recipientId.equals(account.getSelfRecipientId())) {
                final var isUnrestricted = encryptedProfile.getUnrestrictedUnidentifiedAccess();
                if (account.isUnrestrictedUnidentifiedAccess() != isUnrestricted) {
                    account.setUnrestrictedUnidentifiedAccess(isUnrestricted);
                }
                if (account.isPrimaryDevice() && profile != null && newProfile.getCapabilities()
                        .contains(Profile.Capability.storageServiceEncryptionV2Capability) && !profile.getCapabilities()
                        .contains(Profile.Capability.storageServiceEncryptionV2Capability)) {
                    context.getJobExecutor().enqueueJob(new SyncStorageJob(true));
                }
            }

            try {
                logger.trace("Storing identity");
                final var identityKey = new IdentityKey(Base64.getDecoder().decode(encryptedProfile.getIdentityKey()));
                account.getIdentityKeyStore().saveIdentity(p.getProfile().getServiceId(), identityKey);
            } catch (InvalidKeyException ignored) {
                logger.warn("Got invalid identity key in profile for {}",
                        context.getRecipientHelper().resolveSignalServiceAddress(recipientId).getIdentifier());
            }

            logger.trace("Storing profile");
            account.getProfileStore().storeProfile(recipientId, newProfile);
            account.getRecipientStore().markRegistered(recipientId, true);
            failedFetches.succeeded(address.getServiceId());

            logger.trace("Done handling retrieved profile");
        }).doOnError(e -> {
            logger.warn("Failed to retrieve profile for {}, ignoring: {}",
                    context.getRecipientHelper().resolveSignalServiceAddress(recipientId).getIdentifier(),
                    e.getMessage());
            final var profile = account.getProfileStore().getProfile(recipientId);
            final var builder = (profile == null ? Profile.newBuilder() : Profile.newBuilder(profile))
                    .withUnidentifiedAccessMode(Profile.UnidentifiedAccessMode.UNKNOWN)
                    .withCapabilities(Set.of());
            if (e instanceof NotFoundException) {
                logger.debug("Marking recipient {} as unregistered after 404 profile fetch.", recipientId);
                account.getRecipientStore().markRegistered(recipientId, false);
                // An unregistered recipient is cached like a fetched profile
                builder.withLastUpdateTimestamp(System.currentTimeMillis());
                failedFetches.succeeded(address.getServiceId());
            } else if (!address.getServiceId().isUnknown()) {
                // The last update stays that of the last fetched profile; the backoff decides when to retry
                final var now = System.currentTimeMillis();
                final var retryAt = failedFetches.failed(address.getServiceId(), e, now);
                logger.debug("Retrying profile fetch for {} in {}s",
                        recipientId,
                        TimeUnit.MILLISECONDS.toSeconds(retryAt - now));
            }

            account.getProfileStore().storeProfile(recipientId, builder.build());
        });
    }

    private Single<ProfileAndCredential> retrieveProfile(
            SignalServiceAddress address,
            Optional<ProfileKey> profileKey,
            @Nullable SealedSenderAccess unidentifiedAccess,
            SignalServiceProfile.RequestType requestType
    ) {
        final var profileService = dependencies.getProfileService();
        final var locale = Utils.getDefaultLocale(Locale.US);

        return profileService.getProfile(address, profileKey, unidentifiedAccess, requestType, locale).map(pair -> {
            var processor = new ProfileService.ProfileResponseProcessor(pair);
            if (processor.hasResult()) {
                return processor.getResult();
            } else if (processor.notFound()) {
                throw new NotFoundException("Profile not found");
            } else {
                throw pair.getExecutionError()
                        .or(pair::getApplicationError)
                        .orElseThrow(() -> new IOException("Unknown error while retrieving profile"));
            }
        });
    }

    private void retrieveProfileAvatar(
            String avatarPath,
            ProfileKey profileKey,
            OutputStream outputStream
    ) throws IOException {
        var tmpFile = IOUtils.createTempFile();
        try (var input = dependencies.getMessageReceiver()
                .retrieveProfileAvatar(avatarPath,
                        tmpFile,
                        profileKey,
                        ServiceConfig.AVATAR_DOWNLOAD_FAILSAFE_MAX_SIZE)) {
            // Use larger buffer size to prevent AssertionError: Need: 12272 but only have: 8192 ...
            IOUtils.copyStream(input, outputStream, (int) ServiceConfig.AVATAR_DOWNLOAD_FAILSAFE_MAX_SIZE);
        } finally {
            try {
                Files.delete(tmpFile.toPath());
            } catch (IOException e) {
                logger.warn("Failed to delete received profile avatar temp file “{}”, ignoring: {}",
                        tmpFile,
                        e.getMessage());
            }
        }
    }

    private @Nullable SealedSenderAccess getUnidentifiedAccess(RecipientId recipientId) {
        return context.getUnidentifiedAccessHelper().getSealedSenderAccessFor(recipientId, true);
    }
}
