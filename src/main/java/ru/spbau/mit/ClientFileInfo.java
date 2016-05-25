package ru.spbau.mit;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;

public class ClientFileInfo {
    public static final int BLOCK_SIZE = 10 * 1024 * 1024;
    public static final String DOWNLOADS_FOLDER = "downloads";

    private int id;
    private String name = "";
    private Path path = Paths.get(".");
    private long size = -1;
    private HashSet<Integer> parts = new HashSet<>();
    private int count;

    public ClientFileInfo(DataInputStream dataInputStream) throws IOException {
        id = dataInputStream.readInt();
        final String pathString = dataInputStream.readUTF();
        path = Paths.get(pathString);
        name = path.getFileName().toString();
        size = dataInputStream.readLong();
        final int partsCount = dataInputStream.readInt();
        for (int i = 0; i < partsCount; i++) {
            final int partId = dataInputStream.readInt();
            parts.add(partId);
        }
    }

    public ClientFileInfo(int id) {
        this.id = id;
    }

    public void update(String name, long size) {
        path = Paths.get(DOWNLOADS_FOLDER, String.valueOf(id), name);
        this.name = path.getFileName().toString();
        this.size = size;
        parts = new HashSet<>();
        count = (int) ((size + BLOCK_SIZE - 1) / BLOCK_SIZE);
    }

    public ClientFileInfo(String pathString) {
        path = Paths.get(pathString);
        name = path.getFileName().toString();
        size = path.toFile().length();
        parts = new HashSet<>();
        count = (int) ((size + BLOCK_SIZE - 1) / BLOCK_SIZE);
        for (int i = 0; i < count; i++) {
            parts.add(i);
        }
    }

    public String getName() {
        return name;
    }

    public long getSize() {
        return size;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public HashSet<Integer> getParts() {
        return parts;
    }

    public byte[] getPartContent(int partNumber) throws IOException {
        if (!parts.contains(partNumber)) {
            return null;
        }
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(path.toFile(), "r")) {
            long start = (long) partNumber * BLOCK_SIZE,
                    end = Math.min(size, (long) (partNumber + 1) * BLOCK_SIZE);
            int len = (int) (end - start);
            randomAccessFile.seek(start);
            byte[] partContent = new byte[len];
            randomAccessFile.read(partContent, 0, len);
            return partContent;
        }
    }

    public void writePartContent(int partNumber, byte[] partContent) throws IOException {
        path.toFile().getParentFile().mkdirs();
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(path.toFile(), "rw")) {
            long start = (long) partNumber * BLOCK_SIZE,
                    end = Math.min(size, (long) (partNumber + 1) * BLOCK_SIZE);
            int len = (int) (end - start);
            if (partContent.length != len) {
                throw new IllegalStateException();
            }
            randomAccessFile.seek(start);
            randomAccessFile.write(partContent, 0, len);
        }
    }

    public int partSize(int partNumber) {
        long start = (long) partNumber * BLOCK_SIZE, end = Math.min(size, (long) (partNumber + 1) * BLOCK_SIZE);
        return (int) (end - start);
    }

    public int getCount() {
        return count;
    }

    public void serialize(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeInt(id);
        dataOutputStream.writeUTF(path.toString());
        dataOutputStream.writeLong(size);
        dataOutputStream.writeInt(parts.size());
        for (final int partId : parts) {
            dataOutputStream.writeInt(partId);
        }
    }
}
