package com.fooddelivery.core;

import com.fooddelivery.common.Product;
import com.fooddelivery.common.Protocol;
import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

// Handles communication with a single connected client.
class ClientHandler extends Thread {
    private final Socket clientSocket;
    private final Master master;
    private PrintWriter out;
    private BufferedReader in;

    public ClientHandler(Socket socket, Master master) {
        this.clientSocket = socket;
        this.master = master;
        setName("ClientHandler-" + socket.getRemoteSocketAddress());
    }

    @Override
    public void run() {
        try (PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true);
                BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))) {
            out = writer;
            in = reader;
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                processClientRequest(inputLine);
            }
        } catch (SocketException e) {
            System.err.println("Client connection closed/reset: " + getName());
        } catch (IOException e) {
            System.err.println("IO Error client " + getName() + ": " + e.getMessage());
        } finally {
            try {
                if (clientSocket != null && !clientSocket.isClosed())
                    clientSocket.close();
            } catch (IOException e) {
            }
            System.out.println("Client handler finished: " + getName());
        }
    }

    // Process client request, calling sync or async handlers
    private void processClientRequest(String request) {
        String[] parts = request.split("\\" + Protocol.DELIMITER, 2);
        if (parts.length == 0) {
            sendError("Empty cmd");
            return;
        }
        String command = parts[0];
        String payload = (parts.length > 1) ? parts[1] : "";
        String syncResponse = null;
        try {
            switch (command) {
                case Protocol.ADD_STORE:
                    syncResponse = handleAddStore(payload);
                    break;
                case Protocol.UPDATE_STOCK:
                    syncResponse = handleUpdateStock(payload);
                    break;
                case Protocol.ADD_PRODUCT:
                    syncResponse = handleAddProduct(payload);
                    break;
                case Protocol.REMOVE_PRODUCT:
                    syncResponse = handleRemoveProduct(payload);
                    break;
                case Protocol.SEARCH:
                    handleSearch(payload);
                    return; // ASYNC: handleSearch waits and sends final response
                case Protocol.BUY:
                    syncResponse = handleBuy(payload);
                    break;
                case Protocol.RATE:
                    syncResponse = handleRate(payload);
                    break;
                case Protocol.GET_SALES:
                    syncResponse = handleGetSales(payload);
                    break; // Handle NEW command
                default:
                    syncResponse = Protocol.ERROR + Protocol.DELIMITER + "Unknown command";
                    break;
            }
        } catch (Exception e) {
            syncResponse = Protocol.ERROR + Protocol.DELIMITER + "Master internal error";
            e.printStackTrace();
        }
        if (syncResponse != null) {
            sendMessage(syncResponse);
        } // Send synchronous response
    }

    // Send message/error to client
    private void sendMessage(String message) {
        if (out != null && !clientSocket.isClosed()) {
            out.println(message);
        } else {
            System.err.println("Master: Cannot send message to likely closed client " + getName());
        }
    }

    private void sendError(String errorMessage) {
        sendMessage(Protocol.ERROR + Protocol.DELIMITER + errorMessage);
    }

    // Forward SYNC request and wait
    private String forwardToWorkerAndWait(String storeName, String workerCommand, String payload) {
        Master.WorkerConnection worker = master.getWorkerForStore(storeName);
        if (worker == null)
            return Protocol.ERROR + "|No worker for store";
        String logPayload = payload.length() > 80 ? payload.substring(0, 80) + "..." : payload; // Prevent huge JSON
                                                                                                // logs
        String messageToWorker = workerCommand + Protocol.DELIMITER + payload;
        String workerResponse = worker.sendRequestAndWait(messageToWorker, 10000); // 10 sec timeout
        return (workerResponse != null) ? workerResponse : Protocol.ERROR + "|No worker response";
    }

    // --- Synchronous Handlers ---
    private String handleAddStore(String jsonPayload) {
        String storeName = extractStoreNameFromJson(jsonPayload);
        if (storeName == null)
            return Protocol.ERROR + "|Bad StoreName";
        return forwardToWorkerAndWait(storeName, Protocol.M_ADD_STORE, jsonPayload);
    }

    private String extractStoreNameFromJson(String json) {
        String keyPattern = "\"StoreName\"";
        int keyIndex = json.indexOf(keyPattern);
        if (keyIndex == -1)
            return null;
        int colonIndex = -1;
        for (int i = keyIndex + keyPattern.length(); i < json.length(); i++) {
            char c = json.charAt(i);
            if (c == ':') {
                colonIndex = i;
                break;
            }
            if (!Character.isWhitespace(c))
                return null;
        }
        if (colonIndex == -1)
            return null;
        int valueStartIndex = -1;
        for (int i = colonIndex + 1; i < json.length(); i++) {
            char c = json.charAt(i);
            if (c == '"') {
                valueStartIndex = i + 1;
                break;
            }
            if (!Character.isWhitespace(c))
                return null;
        }
        if (valueStartIndex == -1)
            return null;
        int valueEndIndex = json.indexOf('"', valueStartIndex);
        if (valueEndIndex == -1)
            return null;
        return json.substring(valueStartIndex, valueEndIndex);
    }

    private String handleUpdateStock(String payload) {
        String[] parts = payload.split("\\|", 2);
        if (parts.length != 2 || parts[0].isEmpty())
            return Protocol.ERROR + "|Format";
        return forwardToWorkerAndWait(parts[0], Protocol.M_UPDATE_STOCK, payload);
    }

    private String handleAddProduct(String payload) {
        String[] parts = payload.split("\\|", 2);
        if (parts.length != 2 || parts[0].isEmpty())
            return Protocol.ERROR + "|Format";
        return forwardToWorkerAndWait(parts[0], Protocol.M_ADD_PRODUCT, payload);
    }

    private String handleRemoveProduct(String payload) {
        String[] parts = payload.split("\\|", 2);
        if (parts.length != 2 || parts[0].isEmpty())
            return Protocol.ERROR + "|Format";
        return forwardToWorkerAndWait(parts[0], Protocol.M_REMOVE_PRODUCT, payload);
    }

    private String handleRate(String payload) {
        String[] parts = payload.split("\\|", 2);
        if (parts.length != 2 || parts[0].isEmpty())
            return Protocol.ERROR + "|Format";
        return forwardToWorkerAndWait(parts[0], Protocol.M_RATE, payload);
    }

    private String handleBuy(String payload) {
        String[] parts = payload.split("\\|", 2);
        if (parts.length != 2 || parts[0].isEmpty())
            return Protocol.ERROR + "|Format";
        String storeName = parts[0];
        Master.WorkerConnection worker = master.getWorkerForStore(storeName);
        if (worker == null)
            return Protocol.ERROR + "|No worker";
        String buyId = UUID.randomUUID().toString().substring(0, 8);
        String messageToWorker = Protocol.M_BUY + "|" + buyId + "|" + payload;
        String workerResponse = worker.sendRequestAndWait(messageToWorker, 15000);
        return (workerResponse != null) ? workerResponse : Protocol.ERROR + "|No BUY response";
    }

    // --- UPDATED: Handle GET_SALES (Synchronous) ---
    private String handleGetSales(String payload) {
        // Payload should now ONLY be storeName
        String storeName = payload;
        if (storeName == null || storeName.trim().isEmpty()) {
            return Protocol.ERROR + "|Invalid GET_SALES format: storeName missing";
        }

        // Need a request ID for worker to echo back in response
        String requestId = UUID.randomUUID().toString().substring(0, 8);
        // Worker payload is now simpler: requestId|storeName
        String workerPayload = requestId + Protocol.DELIMITER + storeName;

        // Send request to worker and wait for SALES_RESULT
        String workerResponse = forwardToWorkerAndWait(storeName, Protocol.M_GET_SALES, workerPayload);

        // Forward the response (which should start with SALES_RESULT|requestId|)
        // Or handle error if workerResponse is null or ERROR
        if (workerResponse == null) {
            return Protocol.ERROR + "|No response from worker for GET_SALES";
        } else if (!workerResponse.startsWith(Protocol.SALES_RESULT + "|" + requestId)) {
            // If worker sent back an error or wrong format, forward it but log it
            System.err.println(
                    "Master: Unexpected response format for GET_SALES (reqId " + requestId + "): " + workerResponse);
            return workerResponse; // Forward original error/NACK from worker
        } else {
            // Forward the actual sales data part
            return workerResponse;
        }
    }

    // --- ASYNCHRONOUS Search Handler with Wait/Notify ---
    private void handleSearch(String payload) {
        String mapId = UUID.randomUUID().toString();
        String[] filters = payload.split("\\" + Protocol.DELIMITER);
        if (filters.length != 6) {
            sendError("Invalid SEARCH format");
            return;
        }

        List<Master.WorkerConnection> targetWorkers = master.getActiveWorkersSnapshot();
        int workersQueried = targetWorkers.size();
        if (workersQueried == 0) {
            System.err.println("Master: No active workers for search " + mapId);
            sendMessage(Protocol.SEARCH_RESULT + "|" + mapId + "|");
            return;
        }

        Master.SearchState state = new Master.SearchState(mapId, this.out, workersQueried);
        master.registerSearch(mapId, state); // Handles sync

        String mapTaskPayload = mapId + Protocol.DELIMITER + payload;
        String messageToWorkers = Protocol.MAP_TASK + Protocol.DELIMITER + mapTaskPayload;
        int sentCount = 0;
        for (Master.WorkerConnection wc : targetWorkers) {
            if (wc.sendMessage(messageToWorkers))
                sentCount++;
        } // Send async to workers
        System.out.println("Master: Broadcasted search " + mapId + " to " + sentCount + "/" + workersQueried
                + " workers for " + getName());

        // --- Wait for results using wait/notify on SearchState lock ---
        System.out.println("ClientHandler " + getName() + ": Waiting for search results for mapId " + mapId);
        synchronized (state.getLock()) {
            long timeoutMillis = 30000;
            long startTime = System.currentTimeMillis();
            while (!state.isComplete() && (System.currentTimeMillis() - startTime) < timeoutMillis) {
                try {
                    long waitTime = timeoutMillis - (System.currentTimeMillis() - startTime);
                    if (waitTime <= 0)
                        break;
                    state.getLock().wait(waitTime);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    master.removeSearch(mapId);
                    sendError("Search interrupted.");
                    return;
                }
            }
        } // Release lock
        System.out.println("ClientHandler " + getName() + ": Wait finished for mapId " + mapId);

        master.removeSearch(mapId); // Clean up state map
        if (!state.isComplete()) {
            System.err.println("Master: Search timed out for mapId: " + mapId + ". Got " + state.getReceivedResults()
                    + "/" + state.expectedResults);
        }
        String finalPayload = state.getFinalResultPayload();
        String finalResponse = Protocol.SEARCH_RESULT + Protocol.DELIMITER + mapId + Protocol.DELIMITER + finalPayload;
        System.out.println("Master: Sending search results for " + mapId + " (" + state.getResultCount() + " stores).");
        sendMessage(finalResponse); // Send final results to client
    }

} // End ClientHandler
