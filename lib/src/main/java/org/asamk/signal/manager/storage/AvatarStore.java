package org.asamk.signal.manager.storage;

import org.asamk.signal.manager.api.GroupId;
import org.asamk.signal.manager.storage.recipients.RecipientAddress;
import org.asamk.signal.manager.util.IOUtils;
import org.asamk.signal.manager.util.Utils;
import org.whispersystems.signalservice.api.util.StreamDetails;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
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
        if (!hasProfileAvatar(address)) {
            return null;
        }
        return retrieveAvatar(getProfileAvatarFile(address));
    }

    public boolean hasProfileAvatar(RecipientAddress address) throws IOException {
        final var avatar = getProfileAvatarFile(address);
        return avatar.isFile() && avatar.length() > 0;
    }

    public StreamDetails retrieveGroupAvatar(GroupId groupId) throws IOException {
        final var groupAvatarFile = getGroupAvatarFile(groupId);
        return retrieveAvatar(groupAvatarFile);
    }

    public void storeContactAvatar(RecipientAddress address, AvatarStorer storer) throws IOException {
        storeAvatar(getContactAvatarFile(address), storer);
    }

    public void storeProfileAvatar(RecipientAddress address, AvatarStorer storer) throws IOException {
        final var destination = getProfileAvatarFile(address);
        createAvatarsDir();
        final var temporary = Files.createTempFile(avatarsPath.toPath(), ".profile-avatar-", ".tmp");
        try {
            try (OutputStream output = Files.newOutputStream(temporary)) {
                storer.store(output);
            }
            if (Files.size(temporary) == 0) {
                throw new IOException("Profile avatar data is empty");
            }
            Files.move(temporary, destination.toPath(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(temporary);
        }
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
        try (OutputStream output = new FileOutputStream(avatarFile)) {
            storer.store(output);
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

    private File getProfileAvatarFile(RecipientAddress address) throws FileNotFoundException {
        // Encrypted profiles belong to the ACI. Phone/PNI aliases do not identify
        // their owner and historical plaintext aliases cannot authenticate it.
        final var aci = address.aci().orElseThrow(() -> new FileNotFoundException("Profile avatar requires an ACI"));
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
