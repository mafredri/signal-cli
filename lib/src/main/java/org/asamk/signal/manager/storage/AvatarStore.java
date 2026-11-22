package org.asamk.signal.manager.storage;

import org.asamk.signal.manager.api.GroupId;
import org.asamk.signal.manager.storage.recipients.RecipientAddress;
import org.asamk.signal.manager.util.IOUtils;
import org.asamk.signal.manager.util.Utils;
import org.signal.core.models.ServiceId.ACI;
import org.whispersystems.signalservice.api.util.StreamDetails;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

public class AvatarStore {

    private final File avatarsPath;

    public AvatarStore(final File avatarsPath) {
        this.avatarsPath = avatarsPath;
    }

    public StreamDetails retrieveContactAvatar(RecipientAddress address) throws IOException {
        return retrieveAvatar(getContactAvatarFile(address));
    }

    public StreamDetails retrieveProfileAvatar(ACI aci) throws IOException {
        return retrieveAvatar(getProfileAvatarFile(aci));
    }

    public boolean hasProfileAvatar(ACI aci) {
        return getProfileAvatarFile(aci).exists();
    }

    public StreamDetails retrieveGroupAvatar(GroupId groupId) throws IOException {
        final var groupAvatarFile = getGroupAvatarFile(groupId);
        return retrieveAvatar(groupAvatarFile);
    }

    public void storeContactAvatar(RecipientAddress address, AvatarStorer storer) throws IOException {
        storeAvatar(getContactAvatarFile(address), storer);
    }

    public void storeProfileAvatar(ACI aci, AvatarStorer storer) throws IOException {
        storeAvatar(getProfileAvatarFile(aci), storer);
    }

    public void storeGroupAvatar(GroupId groupId, AvatarStorer storer) throws IOException {
        storeAvatar(getGroupAvatarFile(groupId), storer);
    }

    public void deleteProfileAvatar(ACI aci) throws IOException {
        deleteAvatar(getProfileAvatarFile(aci));
    }

    /**
     * Moves the own profile avatar from its name before profile avatars were stored under the ACI. Only the own
     * avatar is moved: another recipient's phone number may since belong to a different account.
     */
    public void moveLegacySelfProfileAvatar(RecipientAddress self, ACI aci) throws IOException {
        final var legacyFile = new File(avatarsPath, "profile-" + self.getLegacyIdentifier());
        final var file = getProfileAvatarFile(aci);
        if (legacyFile.exists() && !file.exists() && !legacyFile.equals(file)) {
            Files.move(legacyFile.toPath(), file.toPath());
        }
    }

    public void deleteGroupAvatar(GroupId groupId) throws IOException {
        deleteAvatar(getGroupAvatarFile(groupId));
    }

    private StreamDetails retrieveAvatar(final File avatarFile) throws IOException {
        if (!avatarFile.exists()) {
            return null;
        }
        return Utils.createStreamDetailsFromFile(avatarFile);
    }

    private void storeAvatar(final File avatarFile, final AvatarStorer storer) throws IOException {
        createAvatarsDir();
        // A storer can fail after writing part of the avatar, for example a profile avatar download whose
        // authentication tag is only checked at the end of the stream, so write to a temporary file first.
        final var temporaryFile = Files.createTempFile(avatarsPath.toPath(), avatarFile.getName() + ".", ".tmp");
        try {
            try (OutputStream output = Files.newOutputStream(temporaryFile)) {
                storer.store(output);
            }
            try {
                Files.move(temporaryFile,
                        avatarFile.toPath(),
                        StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(temporaryFile, avatarFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            Files.deleteIfExists(temporaryFile);
        }
    }

    private void deleteAvatar(final File avatarFile) throws IOException {
        if (avatarFile.exists()) {
            Files.delete(avatarFile.toPath());
        }
    }

    private File getGroupAvatarFile(GroupId groupId) {
        return new File(avatarsPath, "group-" + groupId.toBase64().replace("/", "_"));
    }

    private File getContactAvatarFile(RecipientAddress address) {
        return new File(avatarsPath, "contact-" + address.getLegacyIdentifier());
    }

    /**
     * Profiles are encrypted for an ACI, so their avatars are stored under it. A phone number can move to another
     * account, and whether it is known for a recipient changes over time.
     */
    private File getProfileAvatarFile(ACI aci) {
        return new File(avatarsPath, "profile-" + aci);
    }

    private void createAvatarsDir() throws IOException {
        IOUtils.createPrivateDirectories(avatarsPath);
    }

    @FunctionalInterface
    public interface AvatarStorer {

        void store(OutputStream outputStream) throws IOException;
    }
}
