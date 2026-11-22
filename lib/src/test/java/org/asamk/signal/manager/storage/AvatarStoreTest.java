package org.asamk.signal.manager.storage;

import org.asamk.signal.manager.api.GroupId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AvatarStoreTest {

    private static final GroupId GROUP = GroupId.v2(new byte[32]);

    @TempDir
    Path directory;

    @Test
    void storedAvatarIsReadBack() throws IOException {
        final var store = new AvatarStore(directory.resolve("avatars").toFile());
        store.storeGroupAvatar(GROUP, output -> output.write(bytes("first")));
        store.storeGroupAvatar(GROUP, output -> output.write(bytes("second")));

        assertArrayEquals(bytes("second"), read(store));
        assertEquals(1, fileCount(), "only the avatar remains in the avatars directory");
    }

    @Test
    void failedStoreKeepsThePreviousAvatar() throws IOException {
        final var store = new AvatarStore(directory.resolve("avatars").toFile());
        store.storeGroupAvatar(GROUP, output -> output.write(bytes("previous")));

        assertThrows(IOException.class, () -> store.storeGroupAvatar(GROUP, output -> {
            output.write(bytes("partial"));
            throw new IOException("stream failed after some data");
        }));

        assertArrayEquals(bytes("previous"), read(store));
        assertEquals(1, fileCount(), "the partial avatar is removed");
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] read(AvatarStore store) throws IOException {
        try (final var stream = store.retrieveGroupAvatar(GROUP).getStream()) {
            return stream.readAllBytes();
        }
    }

    private long fileCount() throws IOException {
        try (final var files = Files.list(directory.resolve("avatars"))) {
            return files.count();
        }
    }
}
