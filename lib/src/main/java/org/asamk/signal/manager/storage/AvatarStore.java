package org.asamk.signal.manager.storage;

import org.asamk.signal.manager.api.GroupId;
import org.asamk.signal.manager.storage.recipients.RecipientAddress;
import org.asamk.signal.manager.util.IOUtils;
import org.asamk.signal.manager.util.Utils;
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

    public StreamDetails retrieveProfileAvatar(RecipientAddress address) throws IOException {
        return retrieveAvatar(getProfileAvatarFile(address));
    }

    public StreamDetails retrieveGroupAvatar(GroupId groupId) throws IOException {
        final var groupAvatarFile = getGroupAvatarFile(groupId);
        return retrieveAvatar(groupAvatarFile);
    }

    public void storeContactAvatar(RecipientAddress address, AvatarStorer storer) throws IOException {
        storeAvatar(getContactAvatarFile(address), storer);
    }

    public void storeProfileAvatar(RecipientAddress address, AvatarStorer storer) throws IOException {
        storeAvatar(getProfileAvatarFile(address), storer);
    }

    public void storeGroupAvatar(GroupId groupId, AvatarStorer storer) throws IOException {
        storeAvatar(getGroupAvatarFile(groupId), storer);
    }

    public void deleteProfileAvatar(RecipientAddress address) throws IOException {
        deleteAvatar(getProfileAvatarFile(address));
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

    private File getProfileAvatarFile(RecipientAddress address) {
        return new File(avatarsPath, "profile-" + address.getLegacyIdentifier());
    }

    private void createAvatarsDir() throws IOException {
        IOUtils.createPrivateDirectories(avatarsPath);
    }

    @FunctionalInterface
    public interface AvatarStorer {

        void store(OutputStream outputStream) throws IOException;
    }
}
