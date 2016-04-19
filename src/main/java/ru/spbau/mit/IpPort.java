package ru.spbau.mit;

import java.net.Inet4Address;
import java.net.UnknownHostException;

public class IpPort {
    public static final int IP_BYTES = 4;

    private byte[] ip;
    private short port;

    public IpPort(byte[] ip, short port) {
        this.ip = ip;
        this.port = port;
    }

    public IpPort(String host, short port) throws UnknownHostException {
        ip = Inet4Address.getByName(host).getAddress();
        this.port = port;
    }

    public byte[] getIp() {
        return ip;
    }

    public short getPort() {
        return port;
    }
}
