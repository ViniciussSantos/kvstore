import com.google.gson.Gson;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Servidor {
    ConcurrentHashMap<String, StoredValue> store = new ConcurrentHashMap<String, StoredValue>();

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage if leader: java Servidor <ipAddress> <port> --followers <follower1> <follower2> ...");
            System.out.println("Example: java Servidor 127.0.0.1 10097 --followers 127.0.0.1:10098 127.0.0.1:10099");
            System.out.println("Usage if follower: java Servidor <ipAddress> <port> --leader <leader>");
            System.out.println("Example: java Servidor 127.0.0.1 10097 --leader 127.0.0.1:10098");
            return;
        }

        String ipAddress = args[0];
        int port = Integer.parseInt(args[1]);

        if (args[2].equals("--leader")) {
            String leaderAddress = args[3];
            System.out.println("Starting follower on " + ipAddress + ":" + port + " with leader " + leaderAddress);
            new Thread(new ServerThread(ipAddress, port, false, leaderAddress)).start();
        } else if (args[2].equals("--followers")) {
            Vector<String> followers = new Vector<String>(Arrays.asList(args).subList(3, args.length));
            new Thread(new ServerThread(ipAddress, port, true, followers)).start();
            System.out.println("Starting leader on " + ipAddress + ":" + port + " with followers " + followers);
        }

    }


    /**
     * This class represents the server thread that will listen for incoming connections from other peers.
     * When a connection is accepted, a new thread is created to handle the client.
     */
    private static class ServerThread extends Thread {

        private final String ipAddress;
        private final int port;

        private final boolean isLeader;
        private final ConcurrentHashMap<String, StoredValue> store = new ConcurrentHashMap<String, StoredValue>();
        private Vector<String> followers;
        private String leaderAddress;


        public ServerThread(String ipAddress, int port, boolean isLeader, Vector<String> followers) {
            this.ipAddress = ipAddress;
            this.port = port;
            this.isLeader = isLeader;
            this.followers = followers;
        }

        public ServerThread(String ipAddress, int port, boolean isLeader, String leaderAddress) {
            this.ipAddress = ipAddress;
            this.port = port;
            this.isLeader = isLeader;
            this.leaderAddress = leaderAddress;
        }

        @Override
        public void run() {
            try {
                ServerSocket serverSocket = new ServerSocket(port);


                while (!Thread.currentThread().isInterrupted()) {
                    Socket clientSocket = serverSocket.accept();
                    if (isLeader)
                        new Thread(new ServerHandler(clientSocket, true, store, followers)).start();
                    else
                        new Thread(new ServerHandler(clientSocket, false, store, leaderAddress)).start();

                }

                serverSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * This class handles the connection accepted by the server thread.
     */
    private static class ServerHandler implements Runnable {
        private final Socket socket;
        private final boolean isLeader;
        private final ConcurrentHashMap<String, StoredValue> store;
        private String leaderAddress;
        private Vector<String> followers;

        public ServerHandler(Socket socket, boolean isLeader, ConcurrentHashMap<String, StoredValue> store, Vector<String> followers) {
            this.socket = socket;
            this.isLeader = isLeader;
            this.store = store;
            this.followers = followers;

        }

        public ServerHandler(Socket socket, boolean isLeader, ConcurrentHashMap<String, StoredValue> store, String leaderAddress) {
            this.socket = socket;
            this.isLeader = isLeader;
            this.store = store;
            this.leaderAddress = leaderAddress;
        }

        @Override
        public void run() {
            try {
                InputStream is = this.socket.getInputStream();
                DataInputStream dis = new DataInputStream(is);

                OutputStream os = this.socket.getOutputStream();
                DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(os));

                String response = dis.readUTF();
                Mensagem msg = new Gson().fromJson(response, Mensagem.class);
                switch (msg.operation) {
                    case "PUT":
                        //If it's the leader, store the value and send it to the followers
                        if (isLeader && followers.size() > 0) {
                            long timestamp = System.currentTimeMillis();
                            System.out.println("Cliente" + (socket.getInetAddress()).getHostAddress() + ":" + socket.getPort() + " PUT " + "key: " + msg.key + " value: " + msg.value);
                            store.put(msg.key, new StoredValue(msg.value, timestamp));

                            ExecutorService executor = Executors.newFixedThreadPool(2);
                            for (String follower : followers) {
                                String[] followerAddress = follower.split(":");
                                executor.execute(new ReplicationService(followerAddress[0], Integer.parseInt(followerAddress[1]), msg.key, msg.value, timestamp));
                            }

                            executor.shutdown();
                            while (!executor.isTerminated()) {
                            }
                            System.out.println("Enviando PUT_OK ao cliente " + (socket.getInetAddress()).getHostAddress() + ":" + socket.getPort() + " key: " + msg.key + " value: " + msg.value + " ts: " + timestamp);

                            dos.writeUTF(new Gson().toJson(new Mensagem("PUT_OK", msg.key, msg.value, timestamp)));
                            dos.flush();
                            socket.close();
                            break;
                        }

                        //If it's a follower, forward the message to the leader
                        System.out.println("Encaminhando PUT key:" + msg.key + " value: " + msg.value);
                        new Thread(new ForwarderService(leaderAddress, msg.key, msg.value, msg.clientAddress, msg.clientPort)).start();
                        dos.writeUTF(new Gson().toJson(new Mensagem("FORWARD")));
                        dos.flush();
                        socket.close();
                        break;

                    case "FORWARD":
                        if (!isLeader) {
                            dos.writeUTF("ERROR: Follower can't receive forward messages");
                            dos.flush();
                            socket.close();
                            break;
                        }
                        System.out.println("Cliente " + msg.clientAddress + ":" + msg.clientPort + " PUT " + "key: " + msg.key + " value: " + msg.value);
                        //store the value and send it to the followers
                        long timestamp = System.currentTimeMillis();
                        store.put(msg.key, new StoredValue(msg.value, timestamp));
                        ExecutorService executor = Executors.newFixedThreadPool(2);
                        for (String follower : followers) {
                            String[] followerAddress = follower.split(":");
                            executor.execute(new ReplicationService(followerAddress[0], Integer.parseInt(followerAddress[1]), msg.key, msg.value, timestamp));
                        }

                        executor.shutdown();
                        while (!executor.isTerminated()) {
                        }
                        System.out.println("Enviando PUT_OK ao cliente " + msg.clientAddress + ":" + msg.clientPort + " key: " + msg.key + " value: " + msg.value + " ts: " + timestamp);


                        Socket clientSocket = new Socket(msg.clientAddress, msg.clientPort);
                        OutputStream clientOutputStream = clientSocket.getOutputStream();
                        DataOutputStream clientDataOutputStream = new DataOutputStream(new BufferedOutputStream(clientOutputStream));
                        //This will always return the port of the leader server, because the leader is the one who sends the message back to the client
                        //TODO: Find a way to pass the port of the server (aka follower) that received the message
                        clientDataOutputStream.writeUTF(new Gson().toJson(new Mensagem("PUT_OK", msg.key, null, timestamp, (socket.getInetAddress()).getHostAddress(), socket.getLocalPort())));

                        clientDataOutputStream.flush();
                        dos.flush();
                        clientSocket.close();
                        socket.close();
                        break;

                    case "GET":
                        StoredValue storedValue = store.get(msg.key);
                        if (storedValue != null) {

                            //TODO: Check if this is correct
                            //Wouldn't this always be true if because the client timestamp is always the current time when the client sends the message?
                            // client timestamp = System.currentTimeMillis() of the client ?
                            // making this check useless?
                            if (storedValue.timestamp < msg.timestamp) {
                                System.out.println("Cliente " + (socket.getInetAddress()).getHostAddress() + ":" + socket.getPort() + " GET " + "key: " + msg.key + " ts: " + msg.timestamp
                                        + ". Meu ts: " + storedValue.timestamp + " portando devolvendo " + "TRY_OTHER_SERVER_OR_LATER" + ". Diferença: " + (msg.timestamp - storedValue.timestamp));
                                dos.writeUTF(new Gson().toJson(new Mensagem("TRY_OTHER_SERVER_OR_LATER")));
                                dos.flush();
                                break;
                            }

                            System.out.println("Cliente " + (socket.getInetAddress()).getHostAddress() + ":" + socket.getPort() + " GET " + "key: " + msg.key + " ts: " + msg.timestamp
                                    + ". Meu ts: " + storedValue.timestamp + " portando devolvendo " + storedValue.value + ". Diferença: " + (msg.timestamp - storedValue.timestamp));

                            dos.writeUTF(new Gson().toJson(new Mensagem("GET_OK", msg.key, storedValue.value, storedValue.timestamp)));
                            dos.flush();
                            break;
                        }
                        System.out.println("Cliente " + (socket.getInetAddress()).getHostAddress() + ":" + socket.getPort() + " GET " + "key: " + msg.key + " ts: " + msg.timestamp
                                + ". Meu ts: " + "NULL" + " portando devolvendo " + "NULL" + " Diferença: " + "NULL");
                        dos.writeUTF(new Gson().toJson(new Mensagem("GET_OK", msg.key, null, 0)));
                        dos.flush();
                        break;
                    case "REPLICATION":
                        if (isLeader) {
                            dos.writeUTF("ERROR: Leader can't receive replication messsages");
                            dos.flush();
                            socket.close();
                            break;
                        }

                        System.out.println("REPLICATION key: " + msg.key + " value: " + msg.value);

                        store.put(msg.key, new StoredValue(msg.value, msg.timestamp));
                        dos.writeUTF(new Gson().toJson(new Mensagem("REPLICATION_OK")));
                        dos.flush();
                        socket.close();
                        break;

                    default:
                        dos.writeUTF("ERROR: Invalid operation");
                        dos.flush();
                        socket.close();
                        break;

                }


            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    /**
     * This class represents the thread that will be responsible for forwarding the message to the leader.
     */
    private static class ForwarderService implements Runnable {

        String leader;
        String key;
        String value;
        String clientAddress;
        Integer clientPort;

        public ForwarderService(String leader, String key, String value, String clientAddress, Integer clientPort) {
            this.leader = leader;
            this.key = key;
            this.value = value;
            this.clientAddress = clientAddress;
            this.clientPort = clientPort;
        }


        @Override
        public void run() {
            try {
                String[] leader = this.leader.split(":");
                Socket socket = new Socket(leader[0], Integer.parseInt(leader[1]));
                OutputStream os = socket.getOutputStream();
                DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(os));
                InputStream is = socket.getInputStream();
                DataInputStream dis = new DataInputStream(is);
                dos.writeUTF(new Gson().toJson(new Mensagem("FORWARD", key, value, clientAddress, clientPort)));
                dos.flush();
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    /**
     * This class handles the replication of the data to the followers
     */
    private static class ReplicationService implements Runnable {
        String followerIP;
        Integer followerPort;
        String key;
        String value;
        long timestamp;

        public ReplicationService(String followerIP, Integer followerPort, String key, String value, long timestamp) {
            this.followerIP = followerIP;
            this.followerPort = followerPort;
            this.key = key;
            this.value = value;
            this.timestamp = timestamp;
        }

        @Override
        public void run() {

            try {
                Socket socket = new Socket(followerIP, followerPort);
                OutputStream os = socket.getOutputStream();
                DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(os));
                InputStream is = socket.getInputStream();
                DataInputStream dis = new DataInputStream(is);
                dos.writeUTF(new Gson().toJson(new Mensagem("REPLICATION", key, value, timestamp)));
                dos.flush();
                String response = dis.readUTF();
                Mensagem msg = new Gson().fromJson(response, Mensagem.class);
                if (msg.operation.equals("REPLICATION_OK")) {
                    System.out.println("REPLICATION_OK");
                } else {
                    System.out.println("REPLICATION_ERROR");
                }

                dos.flush();
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    /**
     * This class stores the value and the timestamp of the value
     */
    private static class StoredValue {
        String value;
        long timestamp;

        public StoredValue(String value, long timestamp) {
            this.value = value;
            this.timestamp = timestamp;
        }
    }

}


