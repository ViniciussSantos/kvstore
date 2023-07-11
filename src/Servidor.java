package src;

import com.google.gson.Gson;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

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
                //TODO: Close the connection on every switch case
                switch (msg.operation) {
                    case "PUT":
                        if (isLeader) {
                            store.put(msg.key, new StoredValue(msg.value, System.currentTimeMillis()));
                            //TODO: replicate to followers
                            //TODO: create two threads, one for each follower and wait for them to finish
                            //Should I use a thread pool?
                            //https://www.javatpoint.com/java-thread-pool
                        }
                        //TODO: If it's a follower, forward the message to the leader
                        //TODO: If it's a follower, close the connection and let the leader handle the response

                        break;

                    case "GET":
                        StoredValue storedValue = store.get(msg.key);
                        if (storedValue != null) {
                            //TODO: Check the timestamp to see if the stored value is the latest one
                            break;
                        }
                        //TODO: If it's null, return a message saying that the key doesn't exist
                        break;
                    case "REPLICATION":
                        if (isLeader) {
                            dos.writeUTF("ERROR: Leader can't receive replication messages");
                            dos.flush();
                            socket.close();
                            break;
                        }

                        store.put(msg.key, new StoredValue(msg.value, msg.timestamp));
                        //
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


    private static class Forwarder implements Runnable {

        @Override
        public void run() {

        }
    }

    private static class ReplicationService implements Runnable {

        @Override
        public void run() {

        }
    }

    private static class StoredValue {
        String value;
        long timestamp;

        public StoredValue(String value, long timestamp) {
            this.value = value;
            this.timestamp = timestamp;
        }
    }

}


