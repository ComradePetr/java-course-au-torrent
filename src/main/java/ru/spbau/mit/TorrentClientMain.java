package ru.spbau.mit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.Inet4Address;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class TorrentClientMain {
    public static final String CLIENT_COMMAND_LIST = "list";
    public static final String CLIENT_COMMAND_GET = "get";
    public static final String CLIENT_COMMAND_NEWFILE = "newfile";
    public static final String CLIENT_COMMAND_RUN = "run";
    public static final byte CLIENT_REQUEST_STAT = 1;
    public static final byte CLIENT_REQUEST_GET = 2;
    public static final short MIN_PORT = 1024, MAX_PORT = 4096;

    private static final Logger LOG = LogManager.getLogger(TorrentClientMain.class);
    private static final String SAVED_STATE_FILE = "settings";

    private static final Random RANDOM = new Random();
    private static HashMap<Integer, ClientFileInfo> files = new HashMap<>();
    private static short port;

    private static synchronized HashMap<Integer, ClientFileInfo> getFiles() {
        return files;
    }

    private TorrentClientMain() {

    }

    public static void main(String[] args) {
        loadState();
        final short serverPort = TorrentTrackerMain.SERVER_PORT;
        int currentArg = 0;
        final String command = args[currentArg++], host = args[currentArg++];

        if (command.equals(CLIENT_COMMAND_RUN)) {
            port = (short) (RANDOM.nextInt(MAX_PORT - MIN_PORT) + MIN_PORT);
            LOG.info("I have {} files and port = {}", getFiles().size(), port);
            ExecutorService taskExecutor = Executors.newCachedThreadPool();
            taskExecutor.execute(() -> {
                try (ServerSocket serverSocket = new ServerSocket(port)) {
                    while (true) {
                        try {
                            Socket socket = serverSocket.accept();
                            taskExecutor.execute(() -> {
                                try (DataInputStream dataInputStream =
                                             new DataInputStream(socket.getInputStream());
                                     DataOutputStream dataOutputStream =
                                             new DataOutputStream(socket.getOutputStream())) {
                                    byte type = dataInputStream.readByte();
                                    int id = dataInputStream.readInt();
                                    ClientFileInfo clientFileInfo = getFiles().get(id);
                                    switch (type) {
                                        case CLIENT_REQUEST_STAT:
                                            LOG.info("{} got 'stat'", port);
                                            if (clientFileInfo == null) {
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
                                            LOG.info("{} got 'get {}'", port, partNumber);
                                            dataOutputStream.write(clientFileInfo.getPartContent(partNumber), 0,
                                                    clientFileInfo.partSize(partNumber));
                                            break;
                                        default:
                                            throw new UnsupportedOperationException();
                                    }
                                    dataOutputStream.flush();
                                } catch (IOException e) {
                                    e.printStackTrace();
                                } finally {
                                    try {
                                        socket.close();
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                }
                                LOG.info("{} finished job", port);
                            });
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

            Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    boolean success = false;
                    do {
                        try (Socket socket = new Socket(host, serverPort);
                             DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                             DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream())) {
                            dataOutputStream.writeByte(TorrentTrackerMain.SERVER_REQUEST_UPDATE);
                            dataOutputStream.writeShort(port);
                            dataOutputStream.writeInt(getFiles().size());
                            for (Integer id : getFiles().keySet()) {
                                dataOutputStream.writeInt(id);
                            }
                            dataOutputStream.flush();
                            success = dataInputStream.readBoolean();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    } while (!success);
                    LOG.info("{} send server update!", port);
                }
            }, 0, ClientInfo.LIVE_TIME / 2);

            try (Socket socket = new Socket(host, serverPort);
                 DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                 DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream())) {
                for (ServerFileInfo serverFileInfo : getList(dataInputStream, dataOutputStream)) {
                    int id = serverFileInfo.getId();
                    if (getFiles().containsKey(id)) {
                        ClientFileInfo clientFileInfo = getFiles().get(id);
                        if (clientFileInfo.getSize() == -1) {
                            clientFileInfo.update(serverFileInfo.getName(), serverFileInfo.getSize());
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            saveState();
            downloadFiles(host, serverPort, taskExecutor);
        } else if (command.equals(CLIENT_COMMAND_GET)) {
            int fileId = Integer.parseInt(args[currentArg++]);
            if (!getFiles().containsKey(fileId)) {
                getFiles().put(fileId, new ClientFileInfo(fileId));
            }
        } else {
            try (Socket socket = new Socket(host, serverPort);
                 DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                 DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream())) {
                LOG.info("Open connection {} {}", host, serverPort);
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
                    case CLIENT_COMMAND_NEWFILE:
                        ClientFileInfo newFile = new ClientFileInfo(args[currentArg++]);
                        upload(dataInputStream, dataOutputStream, newFile);
                        getFiles().put(newFile.getId(), newFile);
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        saveState();
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
        dataOutputStream.writeByte(TorrentTrackerMain.SERVER_REQUEST_LIST);
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
        dataOutputStream.writeByte(TorrentTrackerMain.SERVER_REQUEST_UPLOAD);
        dataOutputStream.writeUTF(clientFileInfo.getName());
        dataOutputStream.writeLong(clientFileInfo.getSize());
        dataOutputStream.flush();
        clientFileInfo.setId(dataInputStream.readInt());
    }

    private static void downloadFiles(String host, int serverPort, ExecutorService taskExecutor) {
        for (Map.Entry<Integer, ClientFileInfo> entry : getFiles().entrySet()) {
            LOG.info("{}: file {} (id={})", port, entry.getValue().getName(), entry.getKey());
            final int id = entry.getKey();
            ClientFileInfo value = entry.getValue();
            if (value.getParts().size() < value.getCount()) {
                LOG.info("need to download");
                taskExecutor.execute(() -> {
                    ArrayList<IpPort> seeds = new ArrayList<>();
                    try (Socket socket = new Socket(host, serverPort);
                         DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                         DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream())) {
                        dataOutputStream.writeByte(TorrentTrackerMain.SERVER_REQUEST_SOURCES);
                        dataOutputStream.writeInt(id);
                        dataOutputStream.flush();
                        int count = dataInputStream.readInt();
                        LOG.info("{}: file {} (id={}) --- got {} seeds",
                                port, entry.getValue().getName(), entry.getKey(), count);
                        for (int i = 0; i < count; i++) {
                            byte[] ip = new byte[IpPort.IP_BYTES];
                            dataInputStream.readFully(ip, 0, IpPort.IP_BYTES);
                            IpPort ipPort = new IpPort(ip, dataInputStream.readShort());
                            seeds.add(ipPort);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    HashSet<Integer> alreadyHave = value.getParts();

                    ArrayList<Task> tasks = new ArrayList<>();
                    for (IpPort ipPort : seeds) {
                        LOG.info("{} -> {} stat", port, ipPort.getPort());
                        try (Socket socket = new Socket(Inet4Address.getByAddress(ipPort.getIp()),
                                ipPort.getPort());
                             DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                             DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream())) {
                            dataOutputStream.writeByte(CLIENT_REQUEST_STAT);
                            dataOutputStream.writeInt(id);
                            dataOutputStream.flush();
                            int count = dataInputStream.readInt();
                            for (int i = 0; i < count; i++) {
                                int partNumber = dataInputStream.readInt();
                                if (!alreadyHave.contains(partNumber)) {
                                    tasks.add(new Task(ipPort, partNumber));
                                    LOG.info("I want to download from {} part #{}", ipPort.getPort(), i);
                                }
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }

                    for (Task task : tasks) {
                        taskExecutor.execute(() -> {
                            try (Socket socket = new Socket(
                                    Inet4Address.getByAddress(task.ipPort.getIp()),
                                    task.ipPort.getPort());
                                 DataInputStream dataInputStream =
                                         new DataInputStream(socket.getInputStream());
                                 DataOutputStream dataOutputStream =
                                         new DataOutputStream(socket.getOutputStream())) {
                                dataOutputStream.writeByte(CLIENT_REQUEST_GET);
                                dataOutputStream.writeInt(id);
                                dataOutputStream.writeInt(task.partNumber);
                                dataOutputStream.flush();
                                LOG.info("{} wait for {}", port, value.partSize(task.partNumber));
                                int partSize = value.partSize(task.partNumber);
                                byte[] part = new byte[partSize];
                                dataInputStream.readFully(part, 0, partSize);
                                value.writePartContent(task.partNumber, part);
                                value.getParts().add(task.partNumber);
                                saveState();
                                LOG.info("{}: part #{} is read!", port, task.partNumber);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        });
                    }
                    LOG.info("File {} has been downloaded", value.getName());
                });
            }
        }
    }

    private static void loadState() {
        try (DataInputStream dataInputStream = new DataInputStream(new FileInputStream(SAVED_STATE_FILE))) {
            while (dataInputStream.available() > 0) {
                ClientFileInfo clientFileInfo = new ClientFileInfo(dataInputStream);
                files.put(clientFileInfo.getId(), clientFileInfo);
            }
        } catch (FileNotFoundException e) {
            return;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void saveState() {
        try (DataOutputStream dataOutputStream = new DataOutputStream(new FileOutputStream(SAVED_STATE_FILE))) {
            for (Map.Entry<Integer, ClientFileInfo> entry : getFiles().entrySet()) {
                entry.getValue().serialize(dataOutputStream);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
