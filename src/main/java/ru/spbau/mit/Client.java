package ru.spbau.mit;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class Client {
    public static final String CLIENT_COMMAND_LIST = "list";
    public static final String CLIENT_COMMAND_GET = "get";
    public static final String CLIENT_COMMAND_NEWFILE = "newfile";
    public static final String CLIENT_COMMAND_RUN = "run";
    public static final int CLIENT_REQUEST_STAT = 1;
    public static final int CLIENT_REQUEST_GET = 2;

    private static HashMap<Integer, ClientFileInfo> files = new HashMap<>();
    private static ServerSocket serverSocket;

    private Client() {

    }

    public static void main(String[] args) throws UnknownHostException {
        if (args.length < 1) {
            System.err.println("One argument: port number");
            return;
        }

        final short serverPort = Server.SERVER_PORT;
        Scanner scanner = new Scanner(System.in);
        String command = scanner.next();
        final String host = scanner.next();

        if (command.equals(CLIENT_COMMAND_RUN)) {
            final short port = Short.decode(args[0]);
            ExecutorService taskExecutor = Executors.newCachedThreadPool();
            taskExecutor.execute(() -> {
                while (true) {
                    try (ServerSocket serverSocket = new ServerSocket(port)) {
                        Client.serverSocket = serverSocket;
                        Socket socket = serverSocket.accept();
                        taskExecutor.execute(() -> {
                            try {
                                DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                                DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                                byte type = dataInputStream.readByte();
                                int id = dataInputStream.readInt();
                                ClientFileInfo clientFileInfo = files.get(id);
                                switch (type) {
                                    case CLIENT_REQUEST_STAT:
                                        if (!files.containsKey(id)) {
                                            dataOutputStream.writeInt(0);
                                        } else {
                                            HashSet<Integer> parts = clientFileInfo.getParts();
                                            dataOutputStream.writeInt(parts.size());
                                            for (int i : parts) {
                                                dataOutputStream.writeInt(i);
                                            }
                                        }
                                        break;
                                    case CLIENT_REQUEST_GET:
                                        int partNumber = dataInputStream.readInt();
                                        dataOutputStream.write(clientFileInfo.getPartContent(partNumber));
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
            });

            Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    try (Socket socket = new Socket(host, serverPort);
                         DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                         DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());) {
                        dataOutputStream.writeByte(Server.SERVER_REQUEST_UPDATE);
                        dataOutputStream.writeShort(port);
                        dataOutputStream.writeInt(files.size());
                        for (Integer id : files.keySet()) {
                            dataOutputStream.writeInt(id);
                        }
                        dataOutputStream.flush();
                        boolean success = dataInputStream.readBoolean();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }, 0, ClientInfo.LIVE_TIME / 2);

            try (Socket socket = new Socket(host, serverPort);
                 DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                 DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());) {
                for (ServerFileInfo serverFileInfo : getList(dataInputStream, dataOutputStream)) {
                    int id = serverFileInfo.getId();
                    if (files.containsKey(id)) {
                        ClientFileInfo clientFileInfo = files.get(id);
                        if (clientFileInfo.getParts() == null) {
                            clientFileInfo.update(serverFileInfo.getName(), serverFileInfo.getSize());
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            getFiles(host, serverPort, taskExecutor);
        } else {
            try (Socket socket = new Socket(host, serverPort);
                 DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                 DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());) {
                switch (command) {
                    case CLIENT_COMMAND_LIST:
                        for (ServerFileInfo serverFileInfo : getList(dataInputStream, dataOutputStream)) {
                            System.out.printf("%s: %d bytes, id = %d\n",
                                    serverFileInfo.getName(),
                                    serverFileInfo.getSize(),
                                    serverFileInfo.getId()
                            );
                        }
                        break;
                    case CLIENT_COMMAND_GET:
                        int fileId = scanner.nextInt();
                        if (!files.containsKey(fileId)) {
                            files.put(fileId, new ClientFileInfo(fileId));
                        }
                        break;
                    case CLIENT_COMMAND_NEWFILE:
                        ClientFileInfo newFile = new ClientFileInfo(scanner.next());
                        upload(dataInputStream, dataOutputStream, newFile);
                        files.put(newFile.getId(), newFile);
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static class Task {
        private IpPort ipPort;
        private int partNumber;

        Task(IpPort ipPort, int partNumber) {
            this.ipPort = ipPort;
            this.partNumber = partNumber;
        }
    }

    private static ArrayList<ServerFileInfo> getList(DataInputStream dataInputStream,
                                                     DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeByte(Server.SERVER_REQUEST_LIST);
        dataOutputStream.flush();
        int count = dataInputStream.readInt();
        ArrayList<ServerFileInfo> serverFileInfos = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            serverFileInfos.add(new ServerFileInfo(
                    dataInputStream.readInt(),
                    dataInputStream.readUTF(),
                    dataInputStream.readLong())
            );
        }
        return serverFileInfos;
    }

    private static void upload(DataInputStream dataInputStream, DataOutputStream dataOutputStream,
                               ClientFileInfo clientFileInfo) throws IOException {
        dataOutputStream.writeByte(Server.SERVER_REQUEST_UPLOAD);
        dataOutputStream.writeUTF(clientFileInfo.getName());
        dataOutputStream.writeLong(clientFileInfo.getSize());
        dataOutputStream.flush();
        clientFileInfo.setId(dataInputStream.readInt());
    }

    private static void getFiles(String host, int serverPort, ExecutorService taskExecutor) {
        for (Map.Entry<Integer, ClientFileInfo> entry : files.entrySet()) {
            int id = entry.getKey();
            ClientFileInfo value = entry.getValue();
            if (value.getParts().size() < value.getCount()) {
                taskExecutor.execute(() -> {
                    ArrayList<IpPort> seeds = new ArrayList<>();
                    try (Socket socket = new Socket(host, serverPort);
                         DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                         DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());) {
                        dataOutputStream.writeByte(Server.SERVER_REQUEST_SOURCES);
                        dataOutputStream.writeInt(id);
                        dataOutputStream.flush();
                        int count = dataInputStream.readInt();
                        for (int i = 0; i < count; i++) {
                            byte[] ip = new byte[IpPort.IP_BYTES];
                            dataInputStream.read(ip, 0, IpPort.IP_BYTES);
                            seeds.add(new IpPort(ip, dataInputStream.readShort()));
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    HashSet<Integer> found = (HashSet<Integer>) value.getParts().clone();

                    ArrayList<Task> tasks = new ArrayList<>();
                    for (IpPort ipPort : seeds) {
                        try (Socket socket = new Socket(Inet4Address.getByAddress(ipPort.getIp()),
                                ipPort.getPort());
                             DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                             DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());) {
                            dataOutputStream.write(CLIENT_REQUEST_STAT);
                            dataOutputStream.write(id);
                            dataOutputStream.flush();
                            int count = dataInputStream.read();
                            for (int i = 0; i < count; i++) {
                                int partNumber = dataInputStream.readInt();
                                if (!found.contains(partNumber)) {
                                    tasks.add(new Task(ipPort, partNumber));
                                }
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }

                    for (Task task : tasks) {
                        taskExecutor.execute(() -> {
                            try (Socket socket = new Socket(Inet4Address.getByAddress(task.ipPort.getIp()),
                                    task.ipPort.getPort());
                                 DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                                 DataOutputStream dataOutputStream = new DataOutputStream(socket.
                                         getOutputStream());) {
                                dataOutputStream.write(CLIENT_REQUEST_GET);
                                dataOutputStream.write(id);
                                dataOutputStream.write(task.partNumber);
                                dataOutputStream.flush();
                                byte[] part = new byte[value.partSize(task.partNumber)];
                                dataInputStream.read(part);
                                value.writePartContent(task.partNumber, part);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        });
                    }

                });
            }
        }
    }
}
