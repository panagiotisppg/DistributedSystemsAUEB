package com.fooddelivery.core;

import com.fooddelivery.common.Protocol;
import java.io.*;
import java.net.*;
import java.util.*;

// Master Node: Coordinates Workers, handles Client/Manager requests, orchestrates MapReduce.
// Uses a separate port for receiving asynchronous MAP_RESULTs from workers.
public class Master {

    private final int clientPort;
    private final int resultPort; // Port for workers to send results back
    private final List<WorkerConnection> workers = new ArrayList<>();
    private final Object workersLock = new Object(); // Lock for accessing workers list

    // Search state tracking needs to be thread-safe for access by ClientHandler and
    // MapResultHandler
    private final Map<String, SearchState> activeSearches = new HashMap<>();
    private final Object searchesLock = new Object(); // Lock for the map

    // --- WorkerConnection Inner Class ---
    // Handles primary communication (sync requests, async broadcasts) with a worker
    static class WorkerConnection {
        final String id;
        final Socket socket;
        PrintWriter out;
        BufferedReader in;
        volatile boolean active = true;
        private final Object commsLock = new Object(); // Lock specific to this connection's I/O

        WorkerConnection(String id, Socket socket) throws IOException {
            this.id = id;
            this.socket = socket;
            this.out = new PrintWriter(socket.getOutputStream(), true);
            this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            System.out.println("Master: WorkerConnection created for " + id);
        }

        // Send fire-and-forget (used for MAP_TASK broadcast)
        boolean sendMessage(String message) {
            if (!isActive())
                return false;
            synchronized (commsLock) { // Brief lock for atomicity of check and send
                if (!isActive())
                    return false;
                try {
                    // System.out.println("Master sending async to Worker " + id + ": " +
                    // message.split("\\|")[0]+"..."); // Verbose
                    out.println(message);
                    if (out.checkError()) {
                        System.err.println("Master: PrintWriter error sending async to worker " + id);
                        markInactive();
                        return false;
                    }
                    return true;
                } catch (Exception e) {
                    System.err
                            .println("Master: Exception sending async message to worker " + id + ": " + e.getMessage());
                    markInactive();
                    return false;
                }
            }
        }

        // Send request AND block waiting for sync response (used for ADD, BUY, RATE
        // etc.)
        String sendRequestAndWait(String message, long timeoutMillis) {
            synchronized (commsLock) { // Ensure exclusive access for sync request/reply
                if (!isActive()) {
                    System.err.println("Master: Cannot send sync req to inactive worker " + id);
                    return null;
                }
                System.out.println("Master sending sync to Worker " + id + ": " + message.split("\\|")[0] + "..."); // Log
                                                                                                                    // command
                                                                                                                    // only
                out.println(message);
                if (out.checkError()) {
                    System.err.println("Master: PrintWriter error on sync send to worker " + id);
                    markInactive();
                    return null;
                }

                try { // Wait for response
                    socket.setSoTimeout((int) timeoutMillis);
                    String response = in.readLine(); // BLOCKING READ
                    socket.setSoTimeout(0); // Reset timeout
                    if (response == null) {
                        System.err.println("Master: Worker " + id + " closed connection on sync req.");
                        markInactive();
                        return null;
                    } // Worker closed connection
                    System.out.println("Master received sync from Worker " + id + ": "
                            + response.substring(0, Math.min(100, response.length())) + "...");
                    return response;
                } catch (SocketTimeoutException e) {
                    System.err.println(
                            "Master: Timeout (" + timeoutMillis + "ms) waiting for sync response from worker " + id);
                    return null;
                } catch (IOException e) {
                    System.err.println(
                            "Master: IOException reading sync response from worker " + id + ": " + e.getMessage());
                    markInactive();
                    return null;
                } catch (Exception e) {
                    System.err.println(
                            "Master: Unexpected error reading sync response from worker " + id + ": " + e.getMessage());
                    markInactive();
                    return null;
                } // Catch other errors
            } // End synchronized block
        }

        void close() {
            if (!active)
                return;
            markInactive();
            System.out.println("Master: Closing connection to worker " + id);
            try {
                if (in != null)
                    in.close();
            } catch (IOException e) {
            }
            try {
                if (out != null)
                    out.close();
            } catch (Exception e) {
            }
            try {
                if (socket != null && !socket.isClosed())
                    socket.close();
            } catch (IOException e) {
            }
        }

        void markInactive() {
            this.active = false;
            /* System.out.println("Master: Marked worker " + id + " as inactive."); */ }

        boolean isActive() {
            return active && socket != null && socket.isConnected() && !socket.isClosed();
        }
    } // End WorkerConnection

    // --- SearchState Inner Class (Handles aggregation and notification) ---
    static class SearchState {
        final String mapId;
        final PrintWriter clientWriter;
        final int expectedResults;
        final List<String> results = new ArrayList<>(); // Needs sync
        volatile int receivedResults = 0; // Needs sync for increment
        final Object lock = new Object(); // Used for sync and wait/notify

        SearchState(String mapId, PrintWriter clientWriter, int expectedResults) {
            this.mapId = mapId;
            this.clientWriter = clientWriter;
            this.expectedResults = expectedResults;
        }

        // Add result - synchronized on 'lock'
        void addResult(String workerResultPayload) {
            synchronized (lock) {
                if (workerResultPayload != null && !workerResultPayload.trim().isEmpty()) {
                    String[] storeJsons = workerResultPayload.split(Protocol.MAP_RESULT_STORE_DELIMITER);
                    for (String storeJson : storeJsons) {
                        if (storeJson != null && !storeJson.trim().isEmpty()) {
                            results.add(storeJson);
                        }
                    }
                }
                receivedResults++;
                System.out.println("Search " + mapId + ": Received result " + receivedResults + "/" + expectedResults);
                // Notify if all expected results are in
                if (receivedResults >= expectedResults) {
                    System.out.println("Search " + mapId + ": All results received. Notifying handler.");
                    lock.notifyAll();
                }
            }
        }

        boolean isComplete() {
            synchronized (lock) {
                return receivedResults >= expectedResults;
            }
        } // Check completion under lock

        String getFinalResultPayload() {
            synchronized (lock) {
                if (results.isEmpty())
                    return "";
                return String.join(Protocol.MAP_RESULT_STORE_DELIMITER, results);
            }
        } // Access results under lock

        int getResultCount() {
            synchronized (lock) {
                return results.size();
            }
        } // Access results under lock

        Object getLock() {
            return lock;
        } // Return lock for waiting

        public int getReceivedResults() {
            return receivedResults;
        } // Volatile read is safe
    } // End SearchState

    // Master Constructor - Starts result listener
    public Master(int clientPort, int resultPort, String[] workerAddresses) {
        this.clientPort = clientPort;
        this.resultPort = resultPort; // Store result port
        connectToWorkers(workerAddresses);
        startResultListener(); // Start the *separate* listener for MAP_RESULT
        System.out.println("Master initialization complete. Ready to accept clients on " + clientPort + ", results on "
                + resultPort);
    }

    // connectToWorkers (No sync needed during initialization)
    private void connectToWorkers(String[] workerAddresses) {
        System.out.println("Master: Attempting to connect to workers...");
        for (String address : workerAddresses) {
            try {
                String[] parts = address.split(":");
                if (parts.length != 2)
                    continue;
                String host = parts[0];
                int port = Integer.parseInt(parts[1]);
                Socket socket = new Socket();
                socket.connect(new InetSocketAddress(host, port), 5000);
                WorkerConnection wc = new WorkerConnection(address, socket);
                workers.add(wc);
                System.out.println("Master: Successfully connected to worker " + address);
            } catch (Exception e) {
                System.err.println("Master: Failed to connect to worker " + address + ": " + e.getMessage());
            }
        }
        if (workers.isEmpty()) {
            System.err.println("Master: FATAL - No workers connected. Exiting.");
            System.exit(1);
        }
        System.out.println("Master: Worker connection phase complete. Connected to " + workers.size() + " workers.");
    }

    // startServer (For Client connections - No changes needed)
    public void startServer() {
        try (ServerSocket serverSocket = new ServerSocket(clientPort)) {
            System.out.println("Master listening for clients on port " + clientPort);
            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    System.out.println("Master accepted client connection: " + clientSocket.getRemoteSocketAddress());
                    new ClientHandler(clientSocket, this).start();
                } catch (IOException e) {
                    System.err.println("Master: Error accepting client connection: " + e.getMessage());
                    if (serverSocket.isClosed()) {
                        System.err.println("Master Client ServerSocket closed.");
                        break;
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Master error: Could not listen on client port " + clientPort + ". " + e.getMessage());
            e.printStackTrace();
        } finally {
            System.out.println("Master client server shutting down. Closing worker connections.");
            synchronized (workersLock) {
                workers.forEach(WorkerConnection::close);
                workers.clear();
            }
            System.out.println("Master client server shutdown complete.");
        }
    }

    // --- NEW: Listener for MAP_RESULT messages from Workers on separate port ---
    private void startResultListener() {
        Thread resultThread = new Thread(() -> {
            ServerSocket resultServerSocket = null; // Declare outside try for finally block
            try {
                resultServerSocket = new ServerSocket(resultPort);
                System.out.println("Master Result Listener started on port " + resultPort);
                while (!Thread.currentThread().isInterrupted()) { // Check interrupt flag
                    try {
                        Socket workerResultSocket = resultServerSocket.accept(); // Wait for worker connection
                        new MapResultHandler(workerResultSocket, this).start(); // Handle in new thread
                    } catch (SocketException e) {
                        if (resultServerSocket != null && resultServerSocket.isClosed()) {
                            System.out.println("Master Result Listener: Socket closed, exiting.");
                            break;
                        } // Exit loop if socket closed
                        System.err.println(
                                "Master Result Listener: SocketException accepting connection: " + e.getMessage());
                    } catch (IOException e) {
                        System.err.println(
                                "Master Result Listener: Error accepting worker result connection: " + e.getMessage());
                    }
                }
            } catch (IOException e) {
                System.err.println("Master Result Listener: CRITICAL - Could not listen on result port " + resultPort
                        + ". " + e.getMessage());
            } finally {
                System.out.println("Master Result Listener stopping.");
                if (resultServerSocket != null && !resultServerSocket.isClosed()) {
                    try {
                        resultServerSocket.close();
                    } catch (IOException e) {
                        /* ignore close exception */ }
                }
            }
        }, "MasterResultListener");
        resultThread.setDaemon(true); // Allow clean exit if main thread ends
        resultThread.start();
    }

    // --- NEW: Handler for a single MAP_RESULT submission ---
    private static class MapResultHandler extends Thread {
        private final Socket workerResultSocket;
        private final Master master;

        MapResultHandler(Socket socket, Master masterInstance) {
            this.workerResultSocket = socket;
            this.master = masterInstance;
            setName("MapResultHandler-" + socket.getRemoteSocketAddress());
        }

        @Override
        public void run() {
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(workerResultSocket.getInputStream()))) {
                String message = reader.readLine();
                if (message != null) {
                    master.processMapResult(message);
                } // Pass to Master
                else {
                    System.err.println("MapResultHandler: Received null message from " + getName());
                }
            } catch (IOException e) {
                System.err.println(
                        "MapResultHandler: IOException reading result from " + getName() + ": " + e.getMessage());
            } catch (Exception e) {
                System.err
                        .println("MapResultHandler: Error processing result from " + getName() + ": " + e.getMessage());
                e.printStackTrace();
            } finally {
                try {
                    workerResultSocket.close();
                } catch (IOException e) {
                }
            }
        }
    }

    // --- NEW: Method to process MAP_RESULT received by MapResultHandler ---
    // Needs synchronized access to 'activeSearches' map
    private void processMapResult(String mapResultMessage) {
        System.out.println("Master processMapResult received: "
                + mapResultMessage.substring(0, Math.min(100, mapResultMessage.length())) + "...");
        String[] parts = mapResultMessage.split("\\" + Protocol.DELIMITER, 3);
        if (parts.length < 2 || !parts[0].equals(Protocol.MAP_RESULT)) {
            System.err.println("Master: Received malformed MAP_RESULT: " + mapResultMessage);
            return;
        }
        String mapId = parts[1];
        String payload = (parts.length > 2) ? parts[2] : "";
        SearchState state = getSearchState(mapId); // Uses searchesLock
        if (state != null) {
            state.addResult(payload);
        } // Add result (handles internal sync and notify)
        else {
            System.err.println("Master: Received MAP_RESULT for unknown/expired mapId: " + mapId);
        }
    }

    // getWorkerForStore - Needs synchronized access to 'workers' list via getter
    public WorkerConnection getWorkerForStore(String storeName) {
        if (storeName == null || storeName.trim().isEmpty())
            return null;
        List<WorkerConnection> activeWorkersSnapshot = getActiveWorkersSnapshot();
        if (activeWorkersSnapshot.isEmpty()) {
            System.err.println("Master: No active workers for store: " + storeName);
            return null;
        }
        int workerIndex = Math.abs(storeName.hashCode()) % activeWorkersSnapshot.size();
        WorkerConnection chosenWorker = activeWorkersSnapshot.get(workerIndex);
        return chosenWorker;
    }

    // getActiveWorkersSnapshot - Public method for ClientHandler, needs sync
    public List<WorkerConnection> getActiveWorkersSnapshot() {
        List<WorkerConnection> activeWorkersSnapshot = new ArrayList<>();
        synchronized (workersLock) {
            workers.removeIf(wc -> !wc.isActive());
            activeWorkersSnapshot.addAll(workers);
        }
        return activeWorkersSnapshot;
    }

    // broadcastToWorkers - Needs synchronized access to 'workers' list via getter
    public int broadcastToWorkers(String message) {
        int sentCount = 0;
        List<WorkerConnection> activeWorkersSnapshot = getActiveWorkersSnapshot();
        for (WorkerConnection wc : activeWorkersSnapshot) {
            if (wc.sendMessage(message)) {
                sentCount++;
            }
        }
        return sentCount;
    }

    // --- Search State Registration Methods ---
    // Need synchronized access to 'activeSearches' map
    public void registerSearch(String mapId, SearchState state) {
        synchronized (searchesLock) {
            activeSearches.put(mapId, state);
        }
        System.out.println("Master: Registered search state for mapId: " + mapId);
    }

    public SearchState getSearchState(String mapId) {
        synchronized (searchesLock) {
            return activeSearches.get(mapId);
        }
    }

    public void removeSearch(String mapId) {
        SearchState removedState;
        synchronized (searchesLock) {
            removedState = activeSearches.remove(mapId);
        }
        if (removedState != null) {
            System.out.println("Master: Removed search state for mapId: " + mapId);
        }
    }

    // --- Master Main Method (Updated Usage) ---
    public static void main(String[] args) {
        if (args.length < 3) {
            System.err
                    .println("Usage: java com.fooddelivery.core.Master <client-port> <result-port> <worker1:port> ...");
            System.exit(1);
        }
        int clientPort = 0, resultPort = 0;
        try {
            clientPort = Integer.parseInt(args[0]);
            if (clientPort <= 0 || clientPort > 65535)
                throw new NumberFormatException("Client port");
        } catch (NumberFormatException e) {
            System.err.println("Invalid client port: " + args[0]);
            System.exit(1);
        }
        try {
            resultPort = Integer.parseInt(args[1]);
            if (resultPort <= 0 || resultPort > 65535 || resultPort == clientPort)
                throw new NumberFormatException("Result port");
        } catch (NumberFormatException e) {
            System.err.println("Invalid result port: " + args[1]);
            System.exit(1);
        }
        String[] workerAddresses = Arrays.copyOfRange(args, 2, args.length);
        if (workerAddresses.length == 0) {
            System.err.println("Need worker addresses");
            System.exit(1);
        }
        Master master = new Master(clientPort, resultPort, workerAddresses);
        master.startServer(); // Start accepting client connections (main thread blocks here)
    }
} // End Master class
