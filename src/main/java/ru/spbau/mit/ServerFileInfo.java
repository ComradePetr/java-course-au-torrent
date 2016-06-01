package ru.spbau.mit;

public class ServerFileInfo {
    private int id;
    private String name;
    private Long size;

    public ServerFileInfo(int id, String name, Long size) {
        this.id = id;
        this.name = name;
        this.size = size;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public Long getSize() {
        return size;
    }
}
