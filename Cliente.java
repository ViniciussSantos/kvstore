import com.google.gson.Gson;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.Random;
import java.util.Scanner;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;


public class Cliente {

    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.out.println("Usage: java Cliente <server1> <server2> ...");
            System.out.println("Example: java Cliente 127.0.0.1:10097 127.0.0.1:10098 127.0.0.1:10099");
            System.exit(1);
        }


        Vector<String> servers = new Vector<>(Arrays.asList(args));
        ConcurrentHashMap<String, Long> recentWrites = new ConcurrentHashMap<>();
        ServerSocket serverSocket = new ServerSocket(0);

        Scanner scanner = new Scanner(System.in);
        boolean initialized = false;
        while (true) {
            System.out.println("Enter command - INIT, PUT OR GET: ");
            String command = scanner.nextLine();
            String[] commandParts = command.split(" ");

            switch (commandParts[0].trim()) {
                case "INIT":
                    if (initialized) {
                        System.out.println("Client already initialized");
                        break;
                    }

                    System.out.println("Initializing client");
                    initialized = true;
                    new Thread(new ServerThread(serverSocket, recentWrites)).start();
                    break;

                case "PUT":
                    if (!initialized) {
                        System.out.println("Client not initialized");
                        break;
                    }

                    if (commandParts.length < 3) {
                        System.out.println("Usage: PUT <key> <value>");
                        break;
                    }

                    String putKey = commandParts[1];
                    String putValue = commandParts[2];

                    String putServer = servers.get(new Random().nextInt(servers.size()));
                    String[] putServerParts = putServer.split(":");
                    String putServerAddress = putServerParts[0];
                    int putServerPort = Integer.parseInt(putServerParts[1]);


                    new Thread(new ClientThread(putServerAddress, putServerPort, recentWrites, "PUT", putKey, putValue, (serverSocket.getInetAddress()).getHostAddress(), serverSocket.getLocalPort())).start();
                    break;

                case "GET":
                    if (!initialized) {
                        System.out.println("Client not initialized");
                        break;
                    }

                    if (commandParts.length < 2) {
                        System.out.println("Usage: GET <key>");
                        break;
                    }

                    String key = commandParts[1];

                    if (commandParts.length == 4) {
                        String serverAddress = commandParts[2];
                        int serverPort = Integer.parseInt(commandParts[3]);
                        new Thread(new ClientThread(serverAddress, serverPort, recentWrites, "GET", key, "", (serverSocket.getInetAddress()).getHostAddress(), serverSocket.getLocalPort())).start();
                        break;

                    }

                    String server = servers.get(new Random().nextInt(servers.size()));
                    String[] serverParts = server.split(":");
                    String serverAddress = serverParts[0];
                    int serverPort = Integer.parseInt(serverParts[1]);

                    new Thread(new ClientThread(serverAddress, serverPort, recentWrites, "GET", key, "", (serverSocket.getInetAddress()).getHostAddress(), serverSocket.getLocalPort())).start();
                    break;


            }


        }
    }

    /**
     * This class represents the server thread that will listen for incoming connections.
     * When a connection is accepted, a new thread is created to handle the client.
     */
    private static class ServerThread extends Thread {
        private final ServerSocket serverSocket;
        private final ConcurrentHashMap<String, Long> recentWrites;

        public ServerThread(ServerSocket serverSocket, ConcurrentHashMap<String, Long> recentWrites) {
            this.serverSocket = serverSocket;
            this.recentWrites = recentWrites;
        }

        @Override
        public void run() {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    Socket clientSocket = serverSocket.accept();
                    new Thread(new ServerHandler(clientSocket, recentWrites)).start();
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
        private final Socket clientSocket;

        private final ConcurrentHashMap<String, Long> recentWrites;

        public ServerHandler(Socket clientSocket, ConcurrentHashMap<String, Long> recentWrites) {
            this.clientSocket = clientSocket;
            this.recentWrites = recentWrites;
        }

        @Override
        public void run() {
            try {
                InputStream is = clientSocket.getInputStream();
                DataInputStream dis = new DataInputStream(is);

                String response = dis.readUTF();
                Mensagem receivedMessage = new Gson().fromJson(response, Mensagem.class);

                if (receivedMessage.operation.equals("PUT_OK")) {
                    recentWrites.put(receivedMessage.key, receivedMessage.timestamp);

                    System.out.println("PUT_OK key: " + receivedMessage.key + " value: " + receivedMessage.value + " timestamp: " + receivedMessage.timestamp
                            + " realizada no servidor " + receivedMessage.serverAddress + ":" + receivedMessage.serverPort);
                }

            } catch (IOException e) {
                e.printStackTrace();
            }


        }
    }

    private static class ClientThread extends Thread {

        private final String ipAddress;
        private final int port;
        private final ConcurrentHashMap<String, Long> recentWrites;
        private final String operation;
        private final String key;
        private final String value;
        private final String clientAddress;
        private final int clientPort;


        public ClientThread(String ipAddress, int port, ConcurrentHashMap<String, Long> recentWrites, String operation, String key, String value, String clientAddress, int clientPort) {
            this.ipAddress = ipAddress;
            this.port = port;
            this.recentWrites = recentWrites;
            this.operation = operation;
            this.key = key;
            this.value = value;
            this.clientAddress = clientAddress;
            this.clientPort = clientPort;

        }


        @Override
        public void run() {
            try {
                Socket socket = new Socket(ipAddress, port);
                OutputStream os = socket.getOutputStream();
                DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(os));
                InputStream is = socket.getInputStream();
                DataInputStream dis = new DataInputStream(is);

                Mensagem sentMessage;
                if (operation.equals("GET")) {
                    if (recentWrites.containsKey(key)) {
                        sentMessage = new Mensagem(operation, key, value, recentWrites.get(key));
                    } else {
                        sentMessage = new Mensagem(operation, key, value, 0);
                    }
                } else {
                    sentMessage = new Mensagem(operation, key, value, clientAddress, clientPort);
                }


                String json = new Gson().toJson(sentMessage);
                dos.writeUTF(json);
                dos.flush();

                String response = dis.readUTF();
                Mensagem receivedMessage = new Gson().fromJson(response, Mensagem.class);

                switch (receivedMessage.operation) {
                    case "FORWARD":
                        dos.flush();
                        socket.close();
                        break;
                    case "PUT_OK":
                        recentWrites.put(key, receivedMessage.timestamp);
                        System.out.println("PUT_OK key: " + key + " value: " + value + " timestamp: " + receivedMessage.timestamp
                                + " realizada no servidor " + ipAddress + ":" + port);

                        break;
                    case "GET_OK":
                        long clientTimestamp;
                        if (recentWrites.containsKey(key)) {
                            recentWrites.put(key, receivedMessage.timestamp);
                            clientTimestamp = recentWrites.get(key);
                        } else {
                            recentWrites.put(key, receivedMessage.timestamp);
                            clientTimestamp = receivedMessage.timestamp;
                        }
                        System.out.println("GET key: " + receivedMessage.key + " value: " + receivedMessage.value
                                + " obtido do servidor " + ipAddress + ":" + port + " meu timestamp : " + clientTimestamp + " e do servidor " + receivedMessage.timestamp);

                        break;
                    default:
                        System.out.println(receivedMessage.operation);
                        System.out.println("Invalid operation or error");
                        break;
                }


            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }
}