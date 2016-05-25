package ru.spbau.mit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class TorrentTrackerMain {
    public static final byte SERVER_REQUEST_LIST = 1;
    public static final byte SERVER_REQUEST_UPLOAD = 2;
    public static final byte SERVER_REQUEST_SOURCES = 3;
    public static final byte SERVER_REQUEST_UPDATE = 4;
    public static final short SERVER_PORT = 8081;

    private static final Logger LOG = LogManager.getLogger(TorrentClientMain.class);
    private static ArrayList<ServerFileInfo> serverFileInfos = new ArrayList<>();
    private static HashMap<IpPort, ClientInfo> clients = new HashMap<>();

    private static synchronized HashMap<IpPort, ClientInfo> getClients() {
        return clients;
    }

    private static synchronized ArrayList<ServerFileInfo> getServerFileInfos() {
        return serverFileInfos;
    }

    private static int maxId = 0;

    private TorrentTrackerMain() {

    }

    public static void main(String[] args) throws IOException {
        ExecutorService taskExecutor = Executors.newCachedThreadPool();
        try (ServerSocket serverSocket = new ServerSocket(SERVER_PORT)) {
            while (true) {
                Socket socket = serverSocket.accept();
                LOG.info("New client's connection");
                taskExecutor.execute(() -> {
                    try (DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                         DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream())) {
                        byte type = dataInputStream.readByte();
                        switch (type) {
                            case SERVER_REQUEST_LIST:
                                dataOutputStream.writeInt(getServerFileInfos().size());
                                for (ServerFileInfo serverFileInfo : getServerFileInfos()) {
                                    dataOutputStream.writeInt(serverFileInfo.getId());
                                    dataOutputStream.writeUTF(serverFileInfo.getName());
                                    dataOutputStream.writeLong(serverFileInfo.getSize());
                                }
                                break;

                            case SERVER_REQUEST_UPLOAD:
                                do {
                                    String name = dataInputStream.readUTF();
                                    long size = dataInputStream.readLong();
                                    int id = getNewId();
                                    getServerFileInfos().add(new ServerFileInfo(id, name, size));
                                    dataOutputStream.writeInt(id);
                                } while (false);
                                break;

                            case SERVER_REQUEST_SOURCES:
                                do {
                                    int id = dataInputStream.readInt();
                                    ArrayList<ClientInfo> clientInfoList = new ArrayList<ClientInfo>();
                                    Iterator<Map.Entry<IpPort, ClientInfo>> iterator =
                                            getClients().entrySet().iterator();
                                    while (iterator.hasNext()) {
                                        Map.Entry<IpPort, ClientInfo> entry = iterator.next();
                                        ClientInfo clientInfo = entry.getValue();
                                        if (clientInfo.isOutdated()) {
                                            iterator.remove();
                                        } else {
                                            if (clientInfo.has(id)) {
                                                clientInfoList.add(clientInfo);
                                            }
                                        }
                                    }
                                    dataOutputStream.writeInt(clientInfoList.size());
                                    for (ClientInfo clientInfo : clientInfoList) {
                                        IpPort ipPort = clientInfo.getIpPort();
                                        byte[] ip = ipPort.getIp();
                                        for (int i = 0; i < ip.length; i++) {
                                            dataOutputStream.writeByte(ip[i]);
                                        }
                                        dataOutputStream.writeShort(ipPort.getPort());
                                    }
                                } while (false);
                                break;

                            case SERVER_REQUEST_UPDATE:
                                do {
                                    short port = dataInputStream.readShort();
                                    IpPort ipPort = new IpPort(socket.getInetAddress().getAddress(), port);
                                    ClientInfo clientInfo = new ClientInfo(ipPort);
                                    int count = dataInputStream.readInt();
                                    for (int i = 0; i < count; i++) {
                                        int id = dataInputStream.readInt();
                                        clientInfo.addFileId(id);
                                    }
                                    getClients().put(ipPort, clientInfo);
                                    dataOutputStream.writeBoolean(true);
                                    LOG.info("Server: I got {}'s update!", port);
                                } while (false);
                                break;

                            default:
                                throw new UnsupportedOperationException();
                        }
                        dataOutputStream.flush();
                        dataInputStream.close();
                        dataOutputStream.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            socket.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                });
            }
        }
    }

    private static synchronized int getNewId() {
        return maxId++;
    }
}
