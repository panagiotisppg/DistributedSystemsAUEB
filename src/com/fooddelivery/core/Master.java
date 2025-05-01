package com.fooddelivery.core;

import com.fooddelivery.common.*; // Import the new utility
import java.io.*;
import java.net.*;
import java.util.*;

// Master Node: Coordinates Workers, handles Client/Manager requests, orchestrates MapReduce.
public class Master {

    private final int clientPort;
    private final int resultPort;
    private final List<WorkerConnection> workers = new ArrayList<>();
    private final Object workersLock = new Object();

    private final Map<String, SearchState> activeSearches = new HashMap<>();
    private final Object searchesLock = new Object();

    private static boolean verboseMode = false; // Master verbose flag

    // --- WorkerConnection Inner Class (Updated Logging) ---
    static class WorkerConnection {
        final String id;
        final Socket socket;
        PrintWriter out;
        BufferedReader in;
        volatile boolean active = true;
        private final Object commsLock = new Object();

        WorkerConnection(String id, Socket socket) throws IOException {
            this.id = id;
            this.socket = socket;
            this.out = new PrintWriter(socket.getOutputStream(), true);
            this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            ColorfulStatusPrinter.printLog("Master: WorkerConnection created for " + id);
        }

        boolean sendMessage(String message) { // Async fire-and-forget
            if (!isActive())
                return false;
            synchronized (commsLock) {
                if (!isActive())
                    return false;
                try {
                    ColorfulStatusPrinter.printDebug(Master.verboseMode,
                            "Master SEND ASYNC to " + id + ": " + message.split("\\|")[0] + "...");
                    out.println(message);
                    if (out.checkError()) {
                        ColorfulStatusPrinter.printError("Master: PrintWriter error sending async to worker " + id);
                        markInactive();
                        return false;
                    }
                    return true;
                } catch (Exception e) {
                    ColorfulStatusPrinter.printError(
                            "Master: Exception sending async message to worker " + id + ": " + e.getMessage());
                    markInactive();
                    return false;
                }
            }
        }

        String sendRequestAndWait(String message, long timeoutMillis) { // Sync request/reply
            synchronized (commsLock) {
                if (!isActive()) {
                    ColorfulStatusPrinter.printError("Master: Cannot send sync req to inactive worker " + id);
                    return null;
                }
                ColorfulStatusPrinter
                        .printLog("Master sending sync to Worker " + id + ": " + message.split("\\|")[0] + "...");
                ColorfulStatusPrinter.printDebug(Master.verboseMode, "Master SEND SYNC Full: " + message);
                out.println(message);
                if (out.checkError()) {
                    ColorfulStatusPrinter.printError("Master: PrintWriter error on sync send to worker " + id);
                    markInactive();
                    return null;
                }
                try {
                    socket.setSoTimeout((int) timeoutMillis);
                    String response = in.readLine();
                    socket.setSoTimeout(0);
                    if (response == null) {
                        ColorfulStatusPrinter.printError("Master: Worker " + id + " closed connection on sync req.");
                        markInactive();
                        return null;
                    }
                    ColorfulStatusPrinter.printLog("Master received sync from Worker " + id + ": "
                            + response.substring(0, Math.min(100, response.length())) + "...");
                    ColorfulStatusPrinter.printDebug(Master.verboseMode, "Master RECV SYNC Full: " + response);
                    return response;
                } catch (SocketTimeoutException e) {
                    ColorfulStatusPrinter.printError("Master: Timeout waiting sync from " + id);
                    return null;
                } catch (IOException e) {
                    ColorfulStatusPrinter.printError(
                            "Master: IOException reading sync response from worker " + id + ": " + e.getMessage());
                    markInactive();
                    return null;
                } catch (Exception e) {
                    ColorfulStatusPrinter.printError(
                            "Master: Unexpected error reading sync response from worker " + id + ": " + e.getMessage());
                    markInactive();
                    return null;
                }
            }
        }

        void close() {
            if (!active)
                return;
            markInactive();
            ColorfulStatusPrinter.printInfo("Master: Closing WC for " + id);
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
            if (active)
                ColorfulStatusPrinter.printDebug(Master.verboseMode, "Master: Marking worker " + id + " as inactive.");
            this.active = false;
        }

        boolean isActive() {
            return active && socket != null && socket.isConnected() && !socket.isClosed();
        }
    }

    // --- SearchState Inner Class (Updated Logging) ---
    static class SearchState {
        final String mapId;
        final PrintWriter clientWriter;
        final int expectedResults;
        final List<String> results = new ArrayList<>();
        volatile int receivedResults = 0;
        final Object lock = new Object();

        SearchState(String mapId, PrintWriter clientWriter, int expectedResults) {
            this.mapId = mapId;
            this.clientWriter = clientWriter;
            this.expectedResults = expectedResults;
        }

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
                ColorfulStatusPrinter
                        .printLog("Search " + mapId + ": Received result " + receivedResults + "/" + expectedResults);
                if (receivedResults >= expectedResults) {
                    ColorfulStatusPrinter.printDebug(Master.verboseMode, "Search " + mapId + ": Notifying handler.");
                    lock.notifyAll();
                }
            }
        }

        boolean isComplete() {
            synchronized (lock) {
                return receivedResults >= expectedResults;
            }
        }

        String getFinalResultPayload() {
            synchronized (lock) {
                if (results.isEmpty())
                    return "";
                return String.join(Protocol.MAP_RESULT_STORE_DELIMITER, results);
            }
        }

        int getResultCount() {
            synchronized (lock) {
                return results.size();
            }
        }

        Object getLock() {
            return lock;
        }

        public int getReceivedResults() {
            return receivedResults;
        }
    }

    // Master Constructor - Sets verbose mode, starts result listener
    public Master(int clientPort, int resultPort, String[] workerAddresses, boolean verbose) {
        this.clientPort = clientPort;
        this.resultPort = resultPort;
        Master.verboseMode = verbose;
        Product.setVerboseMode(verbose);
        Store.setVerboseMode(verbose); // Pass setting
        connectToWorkers(workerAddresses);
        startResultListener();
        ColorfulStatusPrinter.printSuccess("Master initialization complete." + (verboseMode ? " [VERBOSE MODE]" : ""));
    }

    // connectToWorkers
    private void connectToWorkers(String[] workerAddresses) {
        ColorfulStatusPrinter.printInfo("Master: Attempting to connect to workers...");
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
                ColorfulStatusPrinter.printSuccess("Master: Successfully connected to worker " + address);
            } catch (Exception e) {
                ColorfulStatusPrinter
                        .printError("Master: Failed to connect to worker " + address + ": " + e.getMessage());
            }
        }
        if (workers.isEmpty()) {
            ColorfulStatusPrinter.printError("Master: FATAL - No workers connected. Exiting.");
            System.exit(1);
        }
        ColorfulStatusPrinter
                .printInfo("Master: Worker connection phase complete. Connected to " + workers.size() + " workers.");
    }

    // startServer
    public void startServer() {
        try (ServerSocket serverSocket = new ServerSocket(clientPort)) {
            ColorfulStatusPrinter.printInfo("Master listening for clients on port " + clientPort);
            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    ColorfulStatusPrinter
                            .printLog("Master accepted client connection: " + clientSocket.getRemoteSocketAddress());
                    new ClientHandler(clientSocket, this).start();
                } catch (IOException e) {
                    ColorfulStatusPrinter.printError("Master: Error accepting client connection: " + e.getMessage());
                    if (serverSocket.isClosed()) {
                        ColorfulStatusPrinter.printError("Master Client ServerSocket closed.");
                        break;
                    }
                }
            }
        } catch (IOException e) {
            ColorfulStatusPrinter
                    .printError("Master error: Could not listen on client port " + clientPort + ". " + e.getMessage());
            e.printStackTrace();
        } finally {
            ColorfulStatusPrinter.printInfo("Master client server shutting down. Closing worker connections.");
            synchronized (workersLock) {
                workers.forEach(WorkerConnection::close);
                workers.clear();
            }
            ColorfulStatusPrinter.printInfo("Master client server shutdown complete.");
        }
    }

    // startResultListener
    private void startResultListener() {
        Thread resultThread = new Thread(() -> {
            ServerSocket resultServerSocket = null;
            try {
                resultServerSocket = new ServerSocket(resultPort);
                ColorfulStatusPrinter.printInfo("Master Result Listener started on port " + resultPort);
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        Socket workerResultSocket = resultServerSocket.accept();
                        ColorfulStatusPrinter.printDebug(Master.verboseMode,
                                "DEBUG Master Result Listener: Accepted connection from "
                                        + workerResultSocket.getRemoteSocketAddress());
                        new MapResultHandler(workerResultSocket, this).start();
                    } catch (SocketException e) {
                        if (resultServerSocket != null && resultServerSocket.isClosed()) {
                            ColorfulStatusPrinter.printInfo("Master Result Listener: Socket closed, exiting.");
                            break;
                        }
                        ColorfulStatusPrinter
                                .printError("Master Result Listener: SocketException accepting: " + e.getMessage());
                    } catch (IOException e) {
                        ColorfulStatusPrinter.printError(
                                "Master Result Listener: Error accepting worker result connection: " + e.getMessage());
                    }
                }
            } catch (IOException e) {
                ColorfulStatusPrinter.printError("Master Result Listener: CRITICAL - Could not listen on result port "
                        + resultPort + ". " + e.getMessage());
            } finally {
                ColorfulStatusPrinter.printInfo("Master Result Listener stopping.");
                if (resultServerSocket != null && !resultServerSocket.isClosed()) {
                    try {
                        resultServerSocket.close();
                    } catch (IOException e) {
                    }
                }
            }
        }, "MasterResultListener");
        resultThread.setDaemon(true);
        resultThread.start();
    }

    // MapResultHandler Inner Class
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
                    ColorfulStatusPrinter.printDebug(Master.verboseMode, "DEBUG MapResultHandler received: "
                            + message.substring(0, Math.min(100, message.length())) + "...");
                    master.processMapResult(message);
                } else {
                    ColorfulStatusPrinter.printError("MapResultHandler: Received null message from " + getName());
                }
            } catch (IOException e) {
                ColorfulStatusPrinter.printError(
                        "MapResultHandler: IOException reading result from " + getName() + ": " + e.getMessage());
            } catch (Exception e) {
                ColorfulStatusPrinter.printError(
                        "MapResultHandler: Error processing result from " + getName() + ": " + e.getMessage());
                e.printStackTrace();
            } finally {
                try {
                    workerResultSocket.close();
                } catch (IOException e) {
                }
            }
        }
    }

    // processMapResult Method
    private void processMapResult(String mapResultMessage) {
        ColorfulStatusPrinter.printDebug(verboseMode, "DEBUG Master processMapResult received: "
                + mapResultMessage.substring(0, Math.min(100, mapResultMessage.length())) + "...");
        String[] parts = mapResultMessage.split("\\" + Protocol.DELIMITER, 3);
        if (parts.length < 2 || !parts[0].equals(Protocol.MAP_RESULT)) {
            ColorfulStatusPrinter.printError("Master: Received malformed MAP_RESULT: " + mapResultMessage);
            return;
        }
        String mapId = parts[1];
        String payload = (parts.length > 2) ? parts[2] : "";
        SearchState state = getSearchState(mapId);
        if (state != null) {
            state.addResult(payload);
        } else {
            ColorfulStatusPrinter.printError("Master: Received MAP_RESULT for unknown/expired mapId: " + mapId);
        }
    }

    // getWorkerForStore
    public WorkerConnection getWorkerForStore(String storeName) {
        if (storeName == null || storeName.trim().isEmpty())
            return null;
        List<WorkerConnection> activeWorkersSnapshot = getActiveWorkersSnapshot();
        if (activeWorkersSnapshot.isEmpty()) {
            ColorfulStatusPrinter.printError("Master: No active workers for store: " + storeName);
            return null;
        }
        int workerIndex = Math.abs(storeName.hashCode()) % activeWorkersSnapshot.size();
        WorkerConnection chosenWorker = activeWorkersSnapshot.get(workerIndex);
        ColorfulStatusPrinter.printDebug(verboseMode,
                "DEBUG Master: Routing store '" + storeName + "' to worker " + chosenWorker.id);
        return chosenWorker;
    }

    // getActiveWorkersSnapshot
    public List<WorkerConnection> getActiveWorkersSnapshot() {
        List<WorkerConnection> activeWorkersSnapshot = new ArrayList<>();
        synchronized (workersLock) {
            workers.removeIf(wc -> !wc.isActive());
            activeWorkersSnapshot.addAll(workers);
        }
        return activeWorkersSnapshot;
    }

    // broadcastToWorkers
    public int broadcastToWorkers(String message) {
        int sentCount = 0;
        List<WorkerConnection> activeWorkersSnapshot = getActiveWorkersSnapshot();
        ColorfulStatusPrinter.printDebug(verboseMode, "DEBUG Master: Broadcasting '" + message.split("\\|")[0]
                + "...' to " + activeWorkersSnapshot.size() + " workers.");
        for (WorkerConnection wc : activeWorkersSnapshot) {
            if (wc.sendMessage(message)) {
                sentCount++;
            } else {
                ColorfulStatusPrinter.printError("Master: Broadcast failed to worker " + wc.id);
            }
        }
        return sentCount;
    }

    // Search State Methods
    public void registerSearch(String mapId, SearchState state) {
        synchronized (searchesLock) {
            activeSearches.put(mapId, state);
        }
        ColorfulStatusPrinter.printDebug(verboseMode, "DEBUG Master: Registered search state for mapId: " + mapId);
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
        if (removedState != null && verboseMode) {
            ColorfulStatusPrinter.printDebug(Master.verboseMode,
                    "DEBUG Master: Removed search state for mapId: " + mapId);
        }
    }

    // --- UPDATED: Master Main Method to handle -v flag ---
    public static void main(String[] args) {
        List<String> argList = new ArrayList<>(Arrays.asList(args));
        boolean verbose = argList.remove("-v"); // Check for verbose flag

        if (argList.size() < 3) {
            System.err.println(
                    "Usage: java com.fooddelivery.core.Master [-v] <client-port> <result-port> <worker1:port> ...");
            System.exit(1);
        }
        int clientPort = 0, resultPort = 0;
        try {
            clientPort = Integer.parseInt(argList.get(0));
            if (clientPort <= 0 || clientPort > 65535)
                throw new NumberFormatException("Client port");
        } catch (Exception e) {
            System.err.println("Invalid client port: " + argList.get(0));
            System.exit(1);
        }
        try {
            resultPort = Integer.parseInt(argList.get(1));
            if (resultPort <= 0 || resultPort > 65535 || resultPort == clientPort)
                throw new NumberFormatException("Result port");
        } catch (Exception e) {
            System.err.println("Invalid result port: " + argList.get(1));
            System.exit(1);
        }
        String[] workerAddresses = argList.subList(2, argList.size()).toArray(new String[0]);
        if (workerAddresses.length == 0) {
            System.err.println("Need worker addresses");
            System.exit(1);
        }

        Master master = new Master(clientPort, resultPort, workerAddresses, verbose); // Pass verbose flag
        master.startServer();
    }
}
