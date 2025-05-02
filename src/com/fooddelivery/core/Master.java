package com.fooddelivery.core;

import com.fooddelivery.common.*;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*; // Import concurrency utilities

// Master Node: Coordinates Workers, handles Client/Manager requests, orchestrates MapReduce.
public class Master {

    private final int clientPort;
    private final int resultPort;
    private final List<WorkerConnection> workers = Collections.synchronizedList(new ArrayList<>()); // Use synchronized list
    private final Object workersLock = new Object(); // Still potentially useful for complex operations like snapshotting

    private final Map<String, SearchState> activeSearches = new ConcurrentHashMap<>(); // Use ConcurrentHashMap for searches
    // No separate searchesLock needed with ConcurrentHashMap

    // *** Make verboseMode public static ***
    public static boolean verboseMode = false; // Master verbose flag (NOW PUBLIC)
    private final ExecutorService workerRequestExecutor = Executors.newCachedThreadPool(); // Thread pool for parallel worker requests

    // --- WorkerConnection Inner Class ---
    static class WorkerConnection {
        final String id;
        final Socket socket;
        PrintWriter out;
        BufferedReader in;
        volatile boolean active = true;
        private final Object commsLock = new Object(); // Lock for individual worker send/receive

        WorkerConnection(String id, Socket socket) throws IOException {
            this.id = id;
            this.socket = socket;
            this.out = new PrintWriter(socket.getOutputStream(), true);
            this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            ColorfulStatusPrinter.printLog("Master: WorkerConnection created for " + id);
        }

        // Async fire-and-forget send
        boolean sendMessage(String message) {
            if (!isActive()) return false;
            // No need to synchronize if PrintWriter is thread-safe (it is),
            // but locking ensures atomicity of check-and-send if needed elsewhere.
            // Keep commsLock for sendRequestAndWait where read follows write.
            synchronized (commsLock) {
                if (!isActive()) return false;
                try {
                    ColorfulStatusPrinter.printDebug(Master.verboseMode, // Access fixed
                            "Master SEND ASYNC to " + id + ": " + message.split("\\|")[0] + "...");
                    out.println(message);
                    if (out.checkError()) {
                        ColorfulStatusPrinter.printError("Master: PrintWriter error sending async to worker " + id + ". Marking inactive.");
                        markInactive();
                        return false;
                    }
                    return true;
                } catch (Exception e) { // Catch broader exceptions during send
                    ColorfulStatusPrinter.printError(
                            "Master: Exception sending async message to worker " + id + ": " + e.getMessage());
                    markInactive();
                    return false;
                }
            }
        }


        // Sync request/reply send
        String sendRequestAndWait(String message, long timeoutMillis) {
             synchronized (commsLock) { // Essential to sync send and receive on the same socket
                 if (!isActive()) {
                    ColorfulStatusPrinter.printError("Master: Cannot send sync req to inactive worker " + id);
                    return null;
                 }
                 String command = message.split("\\|")[0]; // For logging
                 ColorfulStatusPrinter.printLog("Master sending sync to Worker " + id + ": " + command + "...");
                 ColorfulStatusPrinter.printDebug(Master.verboseMode, "Master SEND SYNC Full: " + message); // Access fixed

                try {
                    out.println(message);
                    if (out.checkError()) {
                         ColorfulStatusPrinter.printError("Master: PrintWriter error on sync send to worker " + id + ". Marking inactive.");
                         markInactive();
                         return null;
                    }

                    // Set socket timeout for reading the reply
                    socket.setSoTimeout((int) timeoutMillis);
                    String response = in.readLine(); // BLOCKING READ
                    socket.setSoTimeout(0); // Reset timeout

                    if (response == null) {
                         ColorfulStatusPrinter.printError("Master: Worker " + id + " closed connection (read null) on sync req for " + command + ". Marking inactive.");
                         markInactive();
                         return null; // Indicate connection closed
                    }

                    // Log limited response, full if verbose
                    String logResponse = response.length() > 100 ? response.substring(0, 100) + "..." : response;
                    ColorfulStatusPrinter.printLog("Master received sync from Worker " + id + ": " + logResponse);
                    ColorfulStatusPrinter.printDebug(Master.verboseMode, "Master RECV SYNC Full: " + response); // Access fixed
                    return response;

                 } catch (SocketTimeoutException e) {
                    ColorfulStatusPrinter.printError("Master: Timeout (" + timeoutMillis + "ms) waiting sync reply from " + id + " for " + command);
                     // Don't mark inactive on timeout, worker might just be slow
                    return null; // Indicate timeout
                 } catch (IOException e) {
                    ColorfulStatusPrinter.printError(
                         "Master: IOException reading sync response from worker " + id + " for " + command + ": " + e.getMessage() + ". Marking inactive.");
                    markInactive();
                    return null; // Indicate IO error
                 } catch (Exception e) {
                     ColorfulStatusPrinter.printError(
                         "Master: Unexpected error during sync communication with worker " + id + " for " + command + ": " + e.getMessage());
                      markInactive(); // Mark inactive on unexpected errors
                      e.printStackTrace();
                      return null;
                 }
             }
        }

        void close() {
             if (!active) return;
             markInactive(); // Set flag first
             ColorfulStatusPrinter.printInfo("Master: Closing WC for " + id);
             // Close streams and socket, handling potential errors
             try { if (in != null) in.close(); } catch (IOException e) { /* ignore */ }
             if (out != null) out.close(); // PrintWriter close doesn't throw IOException
             try { if (socket != null && !socket.isClosed()) socket.close(); } catch (IOException e) { /* ignore */ }
        }


        void markInactive() {
            // Check active flag first to avoid redundant logging/operations
            if (this.active) {
                ColorfulStatusPrinter.printInfo("Master: Marking worker " + id + " as inactive.");
                this.active = false;
                // Optionally, you could try a clean close here as well,
                // but often markInactive is called *because* of an error.
                // close(); // Be cautious if calling from within send/receive methods.
            }
        }


        boolean isActive() {
            // Check flag first, then socket status
            return active && socket != null && socket.isConnected() && !socket.isClosed() && !socket.isInputShutdown() && !socket.isOutputShutdown();
        }
    }


    // --- SearchState Inner Class (No changes needed here for this issue) ---
    static class SearchState {
         final String mapId;
         // clientWriter is potentially problematic if client disconnects.
         // Consider storing client identifier and looking up connection if needed,
         // or handle potential errors when writing. For now, keep as is.
         final PrintWriter clientWriter;
         final int expectedResults;
         final List<String> results = Collections.synchronizedList(new ArrayList<>()); // Use synchronized list
         volatile int receivedResults = 0;
         final Object lock = new Object(); // For wait/notify coordination

         SearchState(String mapId, PrintWriter clientWriter, int expectedResults) {
             this.mapId = mapId;
             this.clientWriter = clientWriter;
             this.expectedResults = expectedResults;
         }

         // Add results from a single worker (payload is ;; separated store JSONs)
         void addResult(String workerResultPayload) {
             int count = 0;
              if (workerResultPayload != null && !workerResultPayload.trim().isEmpty()) {
                 String[] storeJsons = workerResultPayload.split(Protocol.MAP_RESULT_STORE_DELIMITER);
                 for (String storeJson : storeJsons) {
                     if (storeJson != null && !storeJson.trim().isEmpty()) {
                         results.add(storeJson); // synchronizedList handles concurrency
                         count++;
                     }
                 }
             }
             // Atomic increment might be slightly better, but volatile read/write is usually okay here.
             int currentReceived = ++receivedResults; // Increment and get current value
             ColorfulStatusPrinter.printLog("Search " + mapId + ": Received result " + currentReceived + "/" + expectedResults + " (+" + count + " stores)");

             // Notify waiting handler ONLY if all results are in
             if (currentReceived >= expectedResults) {
                 synchronized (lock) {
                     ColorfulStatusPrinter.printDebug(Master.verboseMode, "Search " + mapId + ": All results received. Notifying handler."); // Access fixed
                     lock.notifyAll(); // Wake up the ClientHandler waiting on this lock
                 }
             }
         }


         boolean isComplete() {
             // Volatile read is sufficient here
             return receivedResults >= expectedResults;
         }

         // Get final ;; separated payload
         String getFinalResultPayload() {
            // synchronizedList allows safe iteration if needed, but join is simpler
             return String.join(Protocol.MAP_RESULT_STORE_DELIMITER, results);
         }

         int getResultCount() {
             return results.size(); // Size on synchronizedList is thread-safe
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
        Master.verboseMode = verbose; // Set the now public static field
        // Set verbose mode in shared classes
        Product.setVerboseMode(verbose);
        Store.setVerboseMode(verbose);
        ColorfulStatusPrinter.printInfo("Master starting... Verbose mode: " + (verboseMode ? "ON" : "OFF"));

        connectToWorkers(workerAddresses);
        startResultListener(); // Listens for async MAP_RESULTS
        ColorfulStatusPrinter.printSuccess("Master initialization complete. Ready for clients.");
    }

    // connectToWorkers
    private void connectToWorkers(String[] workerAddresses) {
        ColorfulStatusPrinter.printInfo("Master: Attempting to connect to workers...");
        List<WorkerConnection> successfullyConnected = new ArrayList<>();
        for (String address : workerAddresses) {
            try {
                String[] parts = address.split(":");
                if (parts.length != 2) {
                     ColorfulStatusPrinter.printError("Master: Invalid worker address format: " + address);
                    continue;
                }
                String host = parts[0];
                int port = Integer.parseInt(parts[1]);
                Socket socket = new Socket();
                 // Set a reasonable connection timeout
                socket.connect(new InetSocketAddress(host, port), 5000); // 5 seconds connect timeout
                WorkerConnection wc = new WorkerConnection(address, socket);
                successfullyConnected.add(wc); // Add to temp list first
                ColorfulStatusPrinter.printSuccess("Master: Successfully connected to worker " + address);
            } catch (NumberFormatException e) {
                 ColorfulStatusPrinter.printError("Master: Invalid port in worker address: " + address);
            } catch (UnknownHostException e) {
                 ColorfulStatusPrinter.printError("Master: Unknown host for worker: " + address);
            } catch (SocketTimeoutException e) {
                 ColorfulStatusPrinter.printError("Master: Timeout connecting to worker: " + address);
            } catch (IOException e) {
                ColorfulStatusPrinter.printError("Master: Failed to connect to worker " + address + ": " + e.getMessage());
            } catch (Exception e) { // Catch any other unexpected errors
                 ColorfulStatusPrinter.printError("Master: Unexpected error connecting to worker " + address + ": " + e.getMessage());
                 e.printStackTrace();
            }
        }

        // Add all successfully connected workers to the main synchronized list
         workers.addAll(successfullyConnected);

        if (workers.isEmpty()) {
            ColorfulStatusPrinter.printError("Master: FATAL - No workers connected. Exiting.");
            System.exit(1); // Exit if no workers are available
        }
        ColorfulStatusPrinter.printInfo("Master: Worker connection phase complete. Connected to " + workers.size() + " workers.");
    }

    // startServer (Listens for Client connections)
    public void startServer() {
        ExecutorService clientExecutor = Executors.newCachedThreadPool(); // Pool for ClientHandlers
        try (ServerSocket serverSocket = new ServerSocket(clientPort)) {
            ColorfulStatusPrinter.printInfo("Master listening for clients on port " + clientPort);
            while (!Thread.currentThread().isInterrupted()) { // Allow interrupting the loop
                try {
                    Socket clientSocket = serverSocket.accept();
                    ColorfulStatusPrinter.printLog("Master accepted client connection: " + clientSocket.getRemoteSocketAddress());
                    // Create and submit ClientHandler to the thread pool
                    clientExecutor.submit(new ClientHandler(clientSocket, this));
                } catch (IOException e) {
                    if (serverSocket.isClosed()) {
                        ColorfulStatusPrinter.printInfo("Master Client ServerSocket closed, stopping accepting clients.");
                        break; // Exit loop if socket is closed
                    }
                    ColorfulStatusPrinter.printError("Master: Error accepting client connection: " + e.getMessage());
                    // Consider adding a small delay here to prevent rapid spinning on persistent errors
                    try { Thread.sleep(100); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                }
            }
        } catch (IOException e) {
            ColorfulStatusPrinter.printError("Master CRITICAL error: Could not listen on client port " + clientPort + ". " + e.getMessage());
            e.printStackTrace();
        } finally {
            ColorfulStatusPrinter.printInfo("Master client server shutting down...");
            clientExecutor.shutdown(); // Disable new tasks from being submitted
             try {
                 // Wait a while for existing tasks to terminate
                 if (!clientExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                     clientExecutor.shutdownNow(); // Cancel currently executing tasks
                     ColorfulStatusPrinter.printWarn("Master: Client handler pool did not terminate cleanly."); // Use printWarn
                 }
             } catch (InterruptedException ie) {
                 clientExecutor.shutdownNow();
                 Thread.currentThread().interrupt();
             }

            // Close worker connections
             ColorfulStatusPrinter.printInfo("Closing worker connections...");
             // Iterate over a snapshot to avoid ConcurrentModificationException if list is modified
             new ArrayList<>(workers).forEach(WorkerConnection::close);
             workers.clear();

            // Shutdown the worker request executor
            shutdownWorkerRequestExecutor();

            ColorfulStatusPrinter.printInfo("Master client server shutdown complete.");
        }
    }


    // startResultListener (Listens for Worker async MAP_RESULT connections)
    private void startResultListener() {
         // Use a thread pool for MapResultHandlers as well
         ExecutorService resultHandlerExecutor = Executors.newCachedThreadPool();

        Thread resultThread = new Thread(() -> {
            ServerSocket resultServerSocket = null;
            try {
                resultServerSocket = new ServerSocket(resultPort);
                ColorfulStatusPrinter.printInfo("Master Result Listener started on port " + resultPort);
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        Socket workerResultSocket = resultServerSocket.accept();
                        // Set TCP_NODELAY might improve latency slightly for small messages
                        workerResultSocket.setTcpNoDelay(true);
                        ColorfulStatusPrinter.printDebug(Master.verboseMode, // Access fixed
                                "DEBUG Master Result Listener: Accepted connection from "
                                        + workerResultSocket.getRemoteSocketAddress());
                        // Submit MapResultHandler to its pool
                         resultHandlerExecutor.submit(new MapResultHandler(workerResultSocket, this));
                    } catch (SocketException e) {
                        if (resultServerSocket != null && resultServerSocket.isClosed()) {
                            ColorfulStatusPrinter.printInfo("Master Result Listener: Socket closed, exiting listener thread.");
                            break; // Exit loop normally
                        }
                        ColorfulStatusPrinter.printError("Master Result Listener: SocketException accepting: " + e.getMessage());
                         // Optional: Add delay?
                    } catch (IOException e) {
                        ColorfulStatusPrinter.printError(
                                "Master Result Listener: Error accepting worker result connection: " + e.getMessage());
                         // Optional: Add delay?
                    }
                }
            } catch (IOException e) {
                ColorfulStatusPrinter.printError("Master Result Listener: CRITICAL - Could not listen on result port "
                        + resultPort + ". " + e.getMessage());
                 // This likely means the Master cannot function correctly
                 // Consider shutting down the Master?
            } finally {
                ColorfulStatusPrinter.printInfo("Master Result Listener stopping.");
                 resultHandlerExecutor.shutdown(); // Shutdown result handler pool
                 try {
                     if (!resultHandlerExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                        resultHandlerExecutor.shutdownNow();
                     }
                 } catch (InterruptedException ie) {
                     resultHandlerExecutor.shutdownNow();
                     Thread.currentThread().interrupt();
                 }

                if (resultServerSocket != null && !resultServerSocket.isClosed()) {
                    try {
                        resultServerSocket.close();
                    } catch (IOException e) { /* ignore */ }
                }
            }
        }, "MasterResultListener");
        resultThread.setDaemon(true); // Allow JVM exit even if this thread is running
        resultThread.start();
    }

    // MapResultHandler Inner Class (Handles a single MAP_RESULT message) - No changes needed
    private static class MapResultHandler implements Runnable { // Implement Runnable for ExecutorService
        private final Socket workerResultSocket;
        private final Master master;

        MapResultHandler(Socket socket, Master masterInstance) {
            this.workerResultSocket = socket;
            this.master = masterInstance;
             // Set name in constructor if needed, though pool manages threads
             // Thread.currentThread().setName("MapResultHandler-" + socket.getRemoteSocketAddress());
        }

        @Override
        public void run() {
            // Set thread name here if desired
            Thread.currentThread().setName("MapResultHandler-" + workerResultSocket.getRemoteSocketAddress());
             // Use try-with-resources for BufferedReader
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(workerResultSocket.getInputStream()))) {

                String message = reader.readLine(); // Read the single line result

                if (message != null) {
                     // Limit logging verbosity
                    String logMsg = message.length() > 150 ? message.substring(0, 150) + "..." : message;
                    ColorfulStatusPrinter.printDebug(Master.verboseMode, "DEBUG MapResultHandler received: " + logMsg); // Access fixed
                    master.processMapResult(message); // Process the received map result
                } else {
                    ColorfulStatusPrinter.printError("MapResultHandler: Received null message from " + Thread.currentThread().getName());
                }
            } catch (IOException e) {
                 // Avoid logging error if socket was closed intentionally from the other side
                 if (!workerResultSocket.isClosed()) {
                    ColorfulStatusPrinter.printError(
                        "MapResultHandler: IOException reading result from " + Thread.currentThread().getName() + ": " + e.getMessage());
                 }
            } catch (Exception e) { // Catch unexpected errors during processing
                ColorfulStatusPrinter.printError(
                        "MapResultHandler: Error processing result from " + Thread.currentThread().getName() + ": " + e.getMessage());
                e.printStackTrace();
            } finally {
                // Ensure the socket is closed
                try {
                    if (!workerResultSocket.isClosed()) {
                        workerResultSocket.close();
                    }
                } catch (IOException e) { /* ignore closing errors */ }
            }
        }
    }


    // processMapResult Method - No changes needed
    private void processMapResult(String mapResultMessage) {
         String logMsg = mapResultMessage.length() > 150 ? mapResultMessage.substring(0, 150) + "..." : mapResultMessage;
         ColorfulStatusPrinter.printDebug(Master.verboseMode, "DEBUG Master processMapResult received: " + logMsg); // Access fixed
        String[] parts = mapResultMessage.split("\\" + Protocol.DELIMITER, 3);
        if (parts.length < 2 || !parts[0].equals(Protocol.MAP_RESULT)) {
            ColorfulStatusPrinter.printError("Master: Received malformed MAP_RESULT: " + mapResultMessage);
            return;
        }
        String mapId = parts[1];
        String payload = (parts.length > 2) ? parts[2] : "";

        // Use computeIfPresent for safer concurrent access if needed, but get is fine with ConcurrentHashMap
        SearchState state = activeSearches.get(mapId);

        if (state != null) {
            state.addResult(payload); // Let SearchState handle synchronization and notification
        } else {
            // This can happen if the result arrives after the search timed out and was removed
            ColorfulStatusPrinter.printWarn("Master: Received MAP_RESULT for unknown or expired mapId: " + mapId); // Use printWarn
        }
    }

    // getWorkerForStore (Routes based on store name hash) - No changes needed
    public WorkerConnection getWorkerForStore(String storeName) {
        if (storeName == null || storeName.trim().isEmpty()){
            ColorfulStatusPrinter.printError("Master: Attempted to route null or empty store name.");
             return null;
        }

        List<WorkerConnection> activeWorkersSnapshot = getActiveWorkersSnapshot(); // Get current active workers
        if (activeWorkersSnapshot.isEmpty()) {
            ColorfulStatusPrinter.printError("Master: No active workers available to route store: " + storeName);
            return null; // No workers to route to
        }

        // Hash-based routing
        int workerIndex = Math.abs(storeName.hashCode()) % activeWorkersSnapshot.size();
        WorkerConnection chosenWorker = activeWorkersSnapshot.get(workerIndex);

        ColorfulStatusPrinter.printDebug(Master.verboseMode, // Access fixed
                "DEBUG Master: Routing store '" + storeName + "' (hash " + storeName.hashCode() + " % "
                + activeWorkersSnapshot.size() + " -> index " + workerIndex + ") to worker " + chosenWorker.id);
        return chosenWorker;
    }

    // getActiveWorkersSnapshot - Checks health and returns a copy
    public List<WorkerConnection> getActiveWorkersSnapshot() {
        List<WorkerConnection> activeSnapshot = new ArrayList<>();
        // Use synchronized block when iterating/modifying the synchronized list
        synchronized (workers) {
             // Remove inactive workers during snapshot creation
             // Use iterator for safe removal while iterating
             Iterator<WorkerConnection> iterator = workers.iterator();
             while(iterator.hasNext()){
                 WorkerConnection wc = iterator.next();
                 if(!wc.isActive()){
                     ColorfulStatusPrinter.printInfo("Master: Removing inactive worker " + wc.id + " from list.");
                     iterator.remove(); // Safely remove using iterator
                     wc.close(); // Ensure resources are closed if not already
                 } else {
                     activeSnapshot.add(wc); // Add active worker to snapshot
                 }
             }
        }
         // Optionally ping workers here periodically if isActive() isn't reliable enough
        return activeSnapshot; // Return the snapshot of active workers
    }

    // broadcastToWorkers (Async send to all) - No changes needed
    public int broadcastToWorkers(String message) {
        int sentCount = 0;
        List<WorkerConnection> activeWorkersSnapshot = getActiveWorkersSnapshot();
        if (activeWorkersSnapshot.isEmpty()) {
             ColorfulStatusPrinter.printWarn("Master: No active workers to broadcast message: " + message.split("\\|")[0]); // Use printWarn
             return 0;
        }

        String command = message.split("\\|")[0]; // For logging
        ColorfulStatusPrinter.printDebug(Master.verboseMode, "DEBUG Master: Broadcasting '" + command // Access fixed
                + "...' to " + activeWorkersSnapshot.size() + " active workers.");

        for (WorkerConnection wc : activeWorkersSnapshot) {
            if (wc.sendMessage(message)) { // Use async send
                sentCount++;
            } else {
                // sendMessage already logs errors and marks inactive
                ColorfulStatusPrinter.printError("Master: Broadcast of " + command + " failed to worker " + wc.id + " (Worker likely marked inactive).");
                // Consider removing immediately or rely on next getActiveWorkersSnapshot call
            }
        }
         ColorfulStatusPrinter.printDebug(Master.verboseMode,"DEBUG Master: Broadcast of " + command + " sent to " + sentCount + "/" + activeWorkersSnapshot.size() + " workers."); // Access fixed
        return sentCount;
    }

    // --- NEW: Method to broadcast a request and wait for ALL synchronous replies ---
    /**
     * Sends a request message to all active workers and waits for their synchronous replies.
     * Uses a thread pool to send requests in parallel.
     *
     * @param message The message to send (e.g., "M_GET_CATEGORY_SALES_TASK|reqId|filter")
     * @param timeoutPerWorkerMillis Timeout for each worker's reply.
     * @return A List of non-null, non-error response strings from the workers. Returns an empty list if no workers respond or all fail/timeout.
     */
    public List<String> broadcastRequestAndWaitForAll(String message, long timeoutPerWorkerMillis) {
        List<WorkerConnection> activeWorkers = getActiveWorkersSnapshot();
        if (activeWorkers.isEmpty()) {
            ColorfulStatusPrinter.printWarn("Master: No active workers to broadcast sync request: " + message.split("\\|")[0]); // Use printWarn
            return Collections.emptyList();
        }

        List<Future<String>> futures = new ArrayList<>();
        String command = message.split("\\|")[0]; // For logging
        ColorfulStatusPrinter.printInfo("Master: Broadcasting sync request '" + command + "' to " + activeWorkers.size() + " workers...");

        // Submit tasks to the pool for each worker
        for (WorkerConnection worker : activeWorkers) {
            Callable<String> task = () -> {
                // This code runs in a separate thread from the pool
                // Use sendRequestAndWait on the specific worker connection
                return worker.sendRequestAndWait(message, timeoutPerWorkerMillis);
            };
            futures.add(workerRequestExecutor.submit(task)); // Submit and store the Future
        }

        List<String> results = new ArrayList<>();
        // Collect results from Futures
        for (int i = 0; i < futures.size(); i++) {
            Future<String> future = futures.get(i);
            WorkerConnection worker = activeWorkers.get(i); // Get corresponding worker for logging
            try {
                // Get the result from the future, waiting if necessary
                // Use a slightly longer timeout here than the worker timeout to account for scheduling delays etc.
                String result = future.get(timeoutPerWorkerMillis + 2000, TimeUnit.MILLISECONDS);

                if (result != null) {
                     // Basic validation: Check if it's an expected result type or ACK/NACK, not ERROR
                     if (!result.startsWith(Protocol.ERROR + Protocol.DELIMITER)) {
                         results.add(result);
                         ColorfulStatusPrinter.printDebug(Master.verboseMode,"DEBUG Master: Received valid sync reply from " + worker.id + " for " + command); // Access fixed
                     } else {
                          ColorfulStatusPrinter.printError("Master: Worker " + worker.id + " returned an ERROR for sync request " + command + ": " + result);
                          // Optionally mark worker inactive based on ERROR response? Maybe not.
                     }
                } else {
                     // sendRequestAndWait returns null on timeout or connection error
                     ColorfulStatusPrinter.printWarn("Master: No valid response (null) received from worker " + worker.id + " for sync request " + command + " (likely timeout or connection issue)."); // Use printWarn
                     // Worker should have been marked inactive by sendRequestAndWait if connection died.
                }
            } catch (TimeoutException e) {
                ColorfulStatusPrinter.printError("Master: Timeout waiting for Future result from worker " + worker.id + " for " + command + ". Cancelling task.");
                 future.cancel(true); // Attempt to interrupt the task
                 // Mark worker potentially inactive or slow? Requires careful consideration.
            } catch (ExecutionException e) {
                ColorfulStatusPrinter.printError("Master: ExecutionException getting result from worker " + worker.id + " for " + command + ": " + e.getCause());
                 e.getCause().printStackTrace();
                 // Worker likely had an internal error. Mark inactive?
                 // worker.markInactive(); // Consider this
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // Preserve interrupt status
                ColorfulStatusPrinter.printError("Master: Interrupted while waiting for worker results for " + command + ".");
                // Cancel remaining futures?
                 futures.forEach(f -> f.cancel(true));
                 return Collections.emptyList(); // Stop collecting results
            } catch (CancellationException e){
                 ColorfulStatusPrinter.printWarn("Master: Task for worker " + worker.id + " for " + command + " was cancelled."); // Use printWarn
            }
        }

        ColorfulStatusPrinter.printInfo("Master: Finished collecting sync replies for " + command + ". Received " + results.size() + " valid responses from " + activeWorkers.size() + " workers.");
        return results;
    }

    // Search State Methods - Using ConcurrentHashMap now
    public void registerSearch(String mapId, SearchState state) {
        activeSearches.put(mapId, state);
        ColorfulStatusPrinter.printDebug(Master.verboseMode, "DEBUG Master: Registered search state for mapId: " + mapId); // Access fixed
    }

    public SearchState getSearchState(String mapId) {
        return activeSearches.get(mapId);
    }

    public void removeSearch(String mapId) {
        SearchState removedState = activeSearches.remove(mapId);
        if (removedState != null && Master.verboseMode) { // Access fixed
            ColorfulStatusPrinter.printDebug(Master.verboseMode, // Access fixed
                    "DEBUG Master: Removed search state for mapId: " + mapId);
        } else if (removedState == null) {
             ColorfulStatusPrinter.printDebug(Master.verboseMode, "DEBUG Master: Attempted to remove non-existent search state for mapId: " + mapId); // Access fixed
        }
    }

    // Shutdown worker request executor pool
    private void shutdownWorkerRequestExecutor() {
         ColorfulStatusPrinter.printInfo("Master: Shutting down worker request executor pool...");
         workerRequestExecutor.shutdown();
         try {
             if (!workerRequestExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                 workerRequestExecutor.shutdownNow();
                 ColorfulStatusPrinter.printWarn("Master: Worker request pool did not terminate cleanly."); // Use printWarn
             }
         } catch (InterruptedException ie) {
             workerRequestExecutor.shutdownNow();
             Thread.currentThread().interrupt();
         }
         ColorfulStatusPrinter.printInfo("Master: Worker request pool shutdown complete.");
    }


    // --- Master Main Method ---
    public static void main(String[] args) {
        List<String> argList = new ArrayList<>(Arrays.asList(args));
        boolean verbose = argList.remove("-v"); // Check for verbose flag

        if (argList.size() < 3) {
            System.err.println(
                    "Usage: java com.fooddelivery.core.Master [-v] <client-port> <result-port> <worker1-host:port> [worker2-host:port] ...");
            System.exit(1);
        }
        int clientPort = 0, resultPort = 0;
        try {
            clientPort = Integer.parseInt(argList.get(0));
            resultPort = Integer.parseInt(argList.get(1));
            if (clientPort <= 0 || clientPort > 65535 || resultPort <= 0 || resultPort > 65535)
                throw new NumberFormatException("Port number out of range (1-65535)");
             if (clientPort == resultPort)
                 throw new NumberFormatException("Client port and Result port cannot be the same");
        } catch (NumberFormatException e) {
            System.err.println("Invalid port configuration: " + e.getMessage());
            System.err.println("Client Port: " + argList.get(0) + ", Result Port: " + argList.get(1));
            System.exit(1);
        } catch (IndexOutOfBoundsException e){
             System.err.println("Missing port arguments.");
             System.exit(1);
        }


        String[] workerAddresses = argList.subList(2, argList.size()).toArray(new String[0]);
        if (workerAddresses.length == 0) {
            System.err.println("Error: At least one worker address (<worker-host:port>) must be provided.");
            System.exit(1);
        }
         // Validate worker address format basic check
         for (String addr : workerAddresses) {
             if (!addr.contains(":") || addr.split(":").length != 2) {
                 System.err.println("Invalid worker address format: " + addr + ". Expected host:port");
                 System.exit(1);
             }
         }


        Master master = null;
        try {
             master = new Master(clientPort, resultPort, workerAddresses, verbose); // Pass verbose flag
             master.startServer(); // Start the main client listening loop (blocking call)
        } catch (Exception e) {
             // Catch unexpected errors during Master initialization or server start
             ColorfulStatusPrinter.printError("Master encountered a fatal error: " + e.getMessage());
             e.printStackTrace();
             // Ensure worker connections are closed if master partially started
             if (master != null) {
                new ArrayList<>(master.workers).forEach(WorkerConnection::close);
                master.shutdownWorkerRequestExecutor();
             }
             System.exit(1); // Exit if master cannot start
        }

        // Code here will likely only be reached if startServer loop is interrupted or exits
        ColorfulStatusPrinter.printInfo("Master main method finished.");
         // JVM will exit if all non-daemon threads (like main) finish.
    }
}