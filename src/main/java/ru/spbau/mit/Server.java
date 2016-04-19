package ru.spbau.mit;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Server implements Closeable {
    public static final byte SERVER_REQUEST_LIST = 1;
    public static final byte SERVER_REQUEST_UPLOAD = 2;
    public static final byte SERVER_REQUEST_SOURCES = 3;
    public static final byte SERVER_REQUEST_UPDATE = 4;
    public static final short SERVER_PORT = 8081;
    public static final String SERVER_STOP_WORD = "stop";

    private ExecutorService taskExecutor;
    private ArrayList<ServerFileInfo> serverFileInfos = new ArrayList<>();
    private HashMap<IpPort, ClientInfo> clients = new HashMap<>();
    private ServerSocket serverSocket;
    private int maxId = 0;
    private boolean timeToFinish = false;

    public static void main(String[] args) throws IOException {
        try (Server server = new Server()) {
            Scanner scanner = new Scanner(System.in);
            while (true) {
                String s = scanner.nextLine();
                if (s.equals(SERVER_STOP_WORD)) {
                    break;
                }
            }
        }
    }

    public Server() throws IOException {
        taskExecutor = Executors.newCachedThreadPool();
        taskExecutor.execute(() -> {
            try (ServerSocket serverSocket = new ServerSocket(SERVER_PORT)) {
                this.serverSocket = serverSocket;
                while (!timeToFinish) {
                    try {
                        Socket socket = serverSocket.accept();
                        System.out.printf("New client\n");
                        taskExecutor.execute(() -> {
                            try {
                                DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                                DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                                byte type = dataInputStream.readByte();
                                switch (type) {
                                    case SERVER_REQUEST_LIST:
                                        dataOutputStream.writeInt(serverFileInfos.size());
                                        for (ServerFileInfo serverFileInfo : serverFileInfos) {
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
                                            serverFileInfos.add(new ServerFileInfo(id, name, size));
                                            dataOutputStream.writeInt(id);
                                        } while (false);
                                        break;

                                    case SERVER_REQUEST_SOURCES:
                                        do {
                                            int id = dataInputStream.readInt();
                                            ArrayList<ClientInfo> clientInfoList = new ArrayList<ClientInfo>();
                                            Iterator<Map.Entry<IpPort, ClientInfo>> iterator =
                                                    clients.entrySet().iterator();
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
                                            clients.put(ipPort, clientInfo);
                                            dataOutputStream.writeBoolean(true);
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
                    } catch (SocketException e) {
                        break;
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }


    @Override
    public void close() throws IOException {
        timeToFinish = true;
        serverSocket.close();
        taskExecutor.shutdown();
    }

    private synchronized int getNewId() {
        return ++maxId;
    }
}
