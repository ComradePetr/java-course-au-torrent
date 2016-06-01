package ru.spbau.mit;

import java.util.HashSet;

public class ClientInfo {
    public static final long LIVE_TIME = 60 * 1000;
    private long lastTime = System.currentTimeMillis();
    private HashSet<Integer> files = new HashSet<>();
    private IpPort ipPort;

    public ClientInfo(IpPort ipPort) {
        this.ipPort = ipPort;
    }

    public boolean isOutdated() {
        return System.currentTimeMillis() - lastTime > LIVE_TIME;
    }

    public boolean has(int id) {
        return files.contains(id);
    }

    public IpPort getIpPort() {
        return ipPort;
    }

    public void addFileId(int id) {
        files.add(id);
    }
}
