package ru.spbau.mit;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;

public class ClientFileInfo {
    public static final int BLOCK_SIZE = 10 * 1024 * 1024;

    private int id;
    private String name;
    private Path path;
    private long size;
    private HashSet<Integer> parts;
    private int count;

    public ClientFileInfo(int id) {
        this.id = id;
    }

    public void update(String name, long size) {
        this.name = name;
        this.path = Paths.get(name);
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
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(path.toFile(), "w")) {
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
}
