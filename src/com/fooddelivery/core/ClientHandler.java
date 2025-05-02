package com.fooddelivery.core;

import com.fooddelivery.common.*; // Include Protocol etc.
import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

// Handles communication with a single connected client (Dummy or Manager).
class ClientHandler implements Runnable { // Implement Runnable for ExecutorService
    private final Socket clientSocket;
    private final Master master;
    private PrintWriter out;
    private BufferedReader in;
    private String clientDesc; // Description for logging

    public ClientHandler(Socket socket, Master master) {
        this.clientSocket = socket;
        this.master = master;
        // Set a descriptive name for logging
        this.clientDesc = "ClientHandler-" + socket.getRemoteSocketAddress();
    }

    @Override
    public void run() {
         // Set thread name for better debugging/logging
         Thread.currentThread().setName(clientDesc);
         ColorfulStatusPrinter.printLog("Master: Client handler started for " + clientSocket.getRemoteSocketAddress());

        // Use try-with-resources for automatic closing of streams
        try (PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true);
             BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream())))
        {
            out = writer;
            in = reader;
            String inputLine;

            // Loop reading commands from the client
            while (!Thread.currentThread().isInterrupted() && (inputLine = in.readLine()) != null) {
                String command = inputLine.split("\\|")[0]; // Get command for logging
                 ColorfulStatusPrinter.printLog(clientDesc + ": Received Cmd: " + command + "...");
                 ColorfulStatusPrinter.printDebug(Master.verboseMode, clientDesc + ": Rcvd Full: " + inputLine); // Access fixed
                 processClientRequest(inputLine); // Process the request
            }
             // If loop exits because readLine returned null, client disconnected gracefully
             ColorfulStatusPrinter.printInfo(clientDesc + ": Client disconnected (readLine returned null).");

        } catch (SocketException e) {
            // More specific handling for connection resets
             if ("Connection reset".equalsIgnoreCase(e.getMessage()) || "Socket closed".equalsIgnoreCase(e.getMessage()) || "Broken pipe".equalsIgnoreCase(e.getMessage())) {
                 ColorfulStatusPrinter.printWarn(clientDesc + ": Connection reset or closed by client."); // Use printWarn
             } else {
                  ColorfulStatusPrinter.printError(clientDesc + ": SocketException: " + e.getMessage());
                  if (Master.verboseMode) e.printStackTrace(); // Access fixed
             }
        } catch (IOException e) {
            ColorfulStatusPrinter.printError(clientDesc + ": IO Error: " + e.getMessage());
             if (Master.verboseMode) e.printStackTrace(); // Access fixed
        } catch (Exception e) { // Catch any other unexpected errors
             ColorfulStatusPrinter.printError(clientDesc + ": Unexpected error: " + e.getMessage());
             e.printStackTrace(); // Log stack trace for unexpected errors
        } finally {
            // Cleanup resources (socket is closed by try-with-resources for PrintWriter/Reader)
            // Ensure streams are nullified
            out = null;
            in = null;
            try {
                if (clientSocket != null && !clientSocket.isClosed()) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                 ColorfulStatusPrinter.printError(clientDesc + ": Error closing client socket: " + e.getMessage());
            }
            ColorfulStatusPrinter.printLog(clientDesc + ": Client handler finished.");
        }
    }


    // Process client request, calling sync or async handlers
    private void processClientRequest(String request) {
        String[] parts = request.split("\\" + Protocol.DELIMITER, 2);
        if (parts.length == 0 || parts[0].isEmpty()) {
            sendError("Empty command received.");
            return;
        }

        String command = parts[0];
        String payload = (parts.length > 1) ? parts[1] : "";
        String syncResponse = null; // For synchronous operations

        try {
            switch (command) {
                // --- Manager Commands (Mostly Synchronous) ---
                case Protocol.ADD_STORE:
                    syncResponse = handleAddStore(payload); // Returns ACK/NACK
                    break;
                case Protocol.ADD_PRODUCT:
                    syncResponse = handleAddProduct(payload); // Returns ACK/NACK
                    break;
                case Protocol.REMOVE_PRODUCT:
                    syncResponse = handleRemoveProduct(payload); // Returns ACK/NACK
                    break;
                case Protocol.UPDATE_STOCK:
                    syncResponse = handleUpdateStock(payload); // Returns ACK/NACK/Stock info
                    break;
                case Protocol.GET_SALES: // Single store sales
                    syncResponse = handleGetSales(payload); // Returns SALES_RESULT/ERROR
                    break;
                case Protocol.GET_SALES_CATEGORY: // NEW Category Sales
                    syncResponse = handleGetSalesCategory(payload); // Returns SALES_REPORT/ERROR
                    break;
                 case Protocol.GET_SALES_PRODTYPE: // NEW Product Type Sales
                    syncResponse = handleGetSalesProductType(payload); // Returns SALES_REPORT/ERROR
                    break;

                // --- Client Commands ---
                case Protocol.SEARCH: // Async Search
                    handleSearch(payload); // Handles response sending internally via SearchState
                    return; // Don't send a syncResponse here
                case Protocol.BUY: // Sync Buy
                    syncResponse = handleBuy(payload); // Returns BUY_SUCCESS/BUY_FAIL/ERROR
                    break;
                case Protocol.RATE: // Sync Rate
                    syncResponse = handleRate(payload); // Returns RATE_SUCCESS/NACK/ERROR
                    break;

                default:
                    ColorfulStatusPrinter.printError(clientDesc + ": Received unknown command: " + command);
                    syncResponse = Protocol.ERROR + Protocol.DELIMITER + "Unknown command: " + command;
                    break;
            }
        } catch (IllegalArgumentException e) { // Catch validation errors within handlers
             ColorfulStatusPrinter.printError(clientDesc + ": Invalid arguments for command " + command + ": " + e.getMessage());
             syncResponse = Protocol.ERROR + Protocol.DELIMITER + "Invalid arguments: " + e.getMessage();
        } catch (Exception e) { // Catch unexpected errors during processing
             ColorfulStatusPrinter.printError(clientDesc + ": Internal error processing command " + command + ": " + e.getMessage());
             e.printStackTrace(); // Log stack trace for debugging
             syncResponse = Protocol.ERROR + Protocol.DELIMITER + "Master internal error processing command. Please check Master logs.";
        }

        // Send the synchronous response if one was generated
        if (syncResponse != null) {
            sendMessage(syncResponse);
        }
    }


    // Send message/error back to the connected client
    private void sendMessage(String message) {
        if (out != null && !clientSocket.isClosed() && !out.checkError()) {
            String logMsg = message.length() > 150 ? message.substring(0, 150) + "..." : message;
            ColorfulStatusPrinter.printLog(clientDesc + ": Sending Resp: " + logMsg.split("\\|")[0] + "...");
            ColorfulStatusPrinter.printDebug(Master.verboseMode, clientDesc + ": Send Full: " + message); // Access fixed

            out.println(message);
             // Re-check error after sending
             if(out.checkError()){
                  ColorfulStatusPrinter.printError(clientDesc + ": Error detected on PrintWriter AFTER sending. Client connection likely lost.");
                  // Consider closing socket or marking handler for cleanup
             }
        } else {
            ColorfulStatusPrinter.printError(clientDesc + ": Cannot send message - client connection closed or PrintWriter error detected.");
            // Attempt to close the socket if it seems open but writer failed
             try {
                if (!clientSocket.isClosed()) clientSocket.close();
             } catch (IOException e) { /* ignore */ }
        }
    }

    // Helper to send a formatted error message
    private void sendError(String errorMessage) {
        sendMessage(Protocol.ERROR + Protocol.DELIMITER + errorMessage);
    }

    // --- Forwarding Logic ---

    // Forward SYNC request to a SINGLE worker (determined by storeName hash) and wait for reply
    private String forwardToWorkerAndWait(String storeName, String workerCommand, String payload) {
        Master.WorkerConnection worker = master.getWorkerForStore(storeName);
        if (worker == null) {
            ColorfulStatusPrinter.printError(clientDesc + ": No active worker found for store '" + storeName + "' to handle command " + workerCommand);
            return Protocol.ERROR + Protocol.DELIMITER + "No active worker available for store: " + storeName;
        }

        // Construct the message for the worker (e.g., M_ADD_STORE|payload)
        String messageToWorker = workerCommand + Protocol.DELIMITER + payload;

        // Use the worker's synchronous send-and-wait method
        String workerResponse = worker.sendRequestAndWait(messageToWorker, 15000); // 15 sec timeout

        if (workerResponse == null) {
            // Timeout or connection error occurred (logged within sendRequestAndWait)
            return Protocol.ERROR + Protocol.DELIMITER + "No response or error communicating with worker for store: " + storeName;
        } else {
            return workerResponse; // Return the worker's actual response (ACK/NACK/DATA/ERROR)
        }
    }

    // --- Synchronous Handlers ---

    private String handleAddStore(String jsonPayload) throws IllegalArgumentException {
        // Extract store name to determine target worker
        String storeName = extractStoreNameFromJson(jsonPayload);
        if (storeName == null || storeName.trim().isEmpty()) {
             throw new IllegalArgumentException("Could not extract valid StoreName from JSON payload for routing.");
        }
        // Forward M_ADD_STORE | {storeJson}
        return forwardToWorkerAndWait(storeName, Protocol.M_ADD_STORE, jsonPayload); // Returns ACK/NACK from worker
    }

    // Helper to extract StoreName (could be moved to a common JSON util if needed)
     private String extractStoreNameFromJson(String json) {
         // Basic JSON string value parsing (assuming simple structure)
         try {
            String keyPattern = "\"StoreName\"";
            int keyIndex = json.indexOf(keyPattern);
            if (keyIndex == -1) return null;

            int colonIndex = json.indexOf(':', keyIndex + keyPattern.length());
            if (colonIndex == -1) return null;

            int valueStartIndex = json.indexOf('"', colonIndex + 1);
            if (valueStartIndex == -1) return null;

            int valueEndIndex = json.indexOf('"', valueStartIndex + 1);
             if (valueEndIndex == -1) return null;

             return json.substring(valueStartIndex + 1, valueEndIndex).replace("\\\"", "\""); // Handle escaped quotes
         } catch (Exception e) {
             ColorfulStatusPrinter.printError(clientDesc + ": Error parsing StoreName from JSON: " + e.getMessage());
             return null; // Return null on any parsing error
         }
     }


    private String handleUpdateStock(String payload) throws IllegalArgumentException {
        // Payload: storeName|productName|amountChange
        String[] parts = payload.split("\\" + Protocol.DELIMITER); // Use \\ for literal |
        if (parts.length != 3 || parts[0].isEmpty() || parts[1].isEmpty()) {
             throw new IllegalArgumentException("Invalid UPDATE_STOCK format. Expected: storeName|productName|amountChange");
        }
        String storeName = parts[0];
        // Forward M_UPDATE_STOCK | storeName|productName|amountChange
        return forwardToWorkerAndWait(storeName, Protocol.M_UPDATE_STOCK, payload); // Returns ACK/NACK from worker
    }

    private String handleAddProduct(String payload) throws IllegalArgumentException {
        // Payload: storeName|{productJson}
        String[] parts = payload.split("\\" + Protocol.DELIMITER, 2); // Split into 2 max
        if (parts.length != 2 || parts[0].isEmpty()) {
            throw new IllegalArgumentException("Invalid ADD_PRODUCT format. Expected: storeName|{productJson}");
        }
        String storeName = parts[0];
         // Forward M_ADD_PRODUCT | storeName|{productJson}
        return forwardToWorkerAndWait(storeName, Protocol.M_ADD_PRODUCT, payload); // Returns ACK/NACK from worker
    }

    private String handleRemoveProduct(String payload) throws IllegalArgumentException {
        // Payload: storeName|productName
        String[] parts = payload.split("\\" + Protocol.DELIMITER);
        if (parts.length != 2 || parts[0].isEmpty() || parts[1].isEmpty()) {
            throw new IllegalArgumentException("Invalid REMOVE_PRODUCT format. Expected: storeName|productName");
        }
        String storeName = parts[0];
         // Forward M_REMOVE_PRODUCT | storeName|productName
        return forwardToWorkerAndWait(storeName, Protocol.M_REMOVE_PRODUCT, payload); // Returns ACK/NACK from worker
    }

    private String handleRate(String payload) throws IllegalArgumentException {
         // Payload: storeName|ratingValue
        String[] parts = payload.split("\\" + Protocol.DELIMITER);
        if (parts.length != 2 || parts[0].isEmpty()) {
             throw new IllegalArgumentException("Invalid RATE format. Expected: storeName|ratingValue");
        }
        String storeName = parts[0];
         // Forward M_RATE | storeName|ratingValue
        return forwardToWorkerAndWait(storeName, Protocol.M_RATE, payload); // Returns RATE_SUCCESS/NACK from worker
    }

    private String handleBuy(String payload) throws IllegalArgumentException {
        // Payload: storeName|prod1:q1,prod2:q2,...
        String[] parts = payload.split("\\" + Protocol.DELIMITER, 2); // Split into 2 max
        if (parts.length != 2 || parts[0].isEmpty()) {
             throw new IllegalArgumentException("Invalid BUY format. Expected: storeName|productList");
        }
        String storeName = parts[0];
        String productList = parts[1];

        Master.WorkerConnection worker = master.getWorkerForStore(storeName);
        if (worker == null) {
             ColorfulStatusPrinter.printError(clientDesc + ": No active worker found for store '" + storeName + "' to handle BUY command.");
             return Protocol.ERROR + Protocol.DELIMITER + "No active worker available for store: " + storeName;
        }

        // Generate a unique ID for this buy transaction for potential logging/tracking
        String buyId = UUID.randomUUID().toString().substring(0, 8);

        // Format message for worker: M_BUY | buyId | storeName | productList
        String messageToWorker = Protocol.M_BUY + Protocol.DELIMITER + buyId + Protocol.DELIMITER + payload; // Re-use original payload here

        // Send synchronously and wait for BUY_SUCCESS or BUY_FAIL
        String workerResponse = worker.sendRequestAndWait(messageToWorker, 20000); // Slightly longer timeout for buy?

        if (workerResponse == null) {
            return Protocol.ERROR + Protocol.DELIMITER + "No response or error communicating with worker for BUY operation on store: " + storeName;
        } else if (!workerResponse.startsWith(Protocol.BUY_SUCCESS + Protocol.DELIMITER + buyId) &&
                   !workerResponse.startsWith(Protocol.BUY_FAIL + Protocol.DELIMITER + buyId)) {
             // Worker responded, but not with expected success/fail format for this buyId
             ColorfulStatusPrinter.printError(clientDesc + ": Received unexpected response format for BUY " + buyId + ": " + workerResponse);
             return Protocol.ERROR + Protocol.DELIMITER + "Unexpected worker response format for BUY.";
        } else {
             // Return the success or failure message from the worker directly
             return workerResponse;
        }
    }

    // --- Single Store Sales Handler (Existing) ---
    private String handleGetSales(String payload) throws IllegalArgumentException {
        // Payload should be just the storeName
        String storeName = payload.trim();
        if (storeName.isEmpty()) {
            throw new IllegalArgumentException("Invalid GET_SALES format: storeName missing");
        }

        // Need a request ID for worker to echo back in response
        String requestId = UUID.randomUUID().toString().substring(0, 8);
        // Worker payload: requestId|storeName
        String workerPayload = requestId + Protocol.DELIMITER + storeName;

        // Send M_GET_SALES | requestId | storeName
        String workerResponse = forwardToWorkerAndWait(storeName, Protocol.M_GET_SALES, workerPayload);

        // Forward the response (should start with SALES_RESULT|requestId|...) or handle error
        if (workerResponse == null) {
            return Protocol.ERROR + Protocol.DELIMITER + "No response from worker for GET_SALES";
        } else if (!workerResponse.startsWith(Protocol.SALES_RESULT + Protocol.DELIMITER + requestId)) {
            // If worker sent back an error or wrong format, forward it but log it
            ColorfulStatusPrinter.printWarn(clientDesc + ": Unexpected response format for GET_SALES (reqId " + requestId + "): " + workerResponse); // Use printWarn
            // Try to return a sensible error to the client
             if (workerResponse.startsWith(Protocol.ERROR)) return workerResponse;
             if (workerResponse.startsWith(Protocol.GENERIC_NACK)) return Protocol.ERROR + Protocol.DELIMITER + "Worker denied request: " + workerResponse;
             return Protocol.ERROR + Protocol.DELIMITER + "Unexpected worker response for GET_SALES.";
        } else {
            // Forward the actual sales data part (worker already formatted it)
            return workerResponse;
        }
    }


    // --- NEW: Category Sales Handler ---
    private String handleGetSalesCategory(String category) throws IllegalArgumentException {
        if (category == null || category.trim().isEmpty()) {
            throw new IllegalArgumentException("Food Category cannot be empty for GET_SALES_CATEGORY");
        }
        category = category.trim(); // Use trimmed version

        String requestId = UUID.randomUUID().toString().substring(0, 8);
        // Message to broadcast: M_GET_CATEGORY_SALES_TASK | requestId | category
        String messageToWorkers = Protocol.M_GET_CATEGORY_SALES_TASK + Protocol.DELIMITER + requestId + Protocol.DELIMITER + category;

        // Broadcast and wait for ALL workers to reply
        List<String> workerResponses = master.broadcastRequestAndWaitForAll(messageToWorkers, 20000); // 20 sec timeout per worker

        // --- Aggregate results ---
        long totalUnits = 0;
        double totalRevenue = 0.0;
        Map<String, StoreSaleSummary> storeSummaries = new HashMap<>(); // StoreName -> Summary(Units, Revenue)

        for (String response : workerResponses) {
             // Expected format: CATEGORY_SALES_RESULT | reqId | store1:units:rev;;store2:units:rev...
             String[] parts = response.split("\\" + Protocol.DELIMITER, 3);
             if (parts.length == 3 && parts[0].equals(Protocol.CATEGORY_SALES_RESULT) && parts[1].equals(requestId)) {
                 String data = parts[2];
                 if (!data.isEmpty()) {
                     String[] storeEntries = data.split(Protocol.SALES_REPORT_DELIMITER);
                     for (String entry : storeEntries) {
                         String[] fields = entry.split(Protocol.SALES_REPORT_FIELD_DELIMITER);
                         if (fields.length == 3) {
                             try {
                                 String storeName = fields[0];
                                 long units = Long.parseLong(fields[1]);
                                 double revenue = Double.parseDouble(fields[2]);

                                 // Add or update the summary for this store
                                 storeSummaries.compute(storeName, (key, current) -> {
                                      if (current == null) return new StoreSaleSummary(units, revenue);
                                      else {
                                          current.add(units, revenue);
                                          return current;
                                      }
                                  });

                             } catch (NumberFormatException e) {
                                  ColorfulStatusPrinter.printError(clientDesc + ": Error parsing sales data from worker response: " + entry);
                             }
                         }
                     }
                 }
             } else {
                 ColorfulStatusPrinter.printWarn(clientDesc + ": Received unexpected or non-matching worker response for CATEGORY_SALES " + requestId + ": " + response); // Use printWarn
             }
        }

        // --- Format the final report ---
        StringBuilder reportBuilder = new StringBuilder();
        // Sort stores by name for consistent output
         List<String> sortedStoreNames = new ArrayList<>(storeSummaries.keySet());
         Collections.sort(sortedStoreNames);

         for (String storeName : sortedStoreNames) {
             StoreSaleSummary summary = storeSummaries.get(storeName);
             reportBuilder.append(storeName) // Store Name
                          .append(Protocol.SALES_REPORT_FIELD_DELIMITER)
                          .append(summary.totalUnits) // Units
                          .append(Protocol.SALES_REPORT_FIELD_DELIMITER)
                          .append(String.format("%.2f", summary.totalRevenue)) // Revenue
                          .append(Protocol.SALES_REPORT_DELIMITER);
             totalUnits += summary.totalUnits;
             totalRevenue += summary.totalRevenue;
         }


        // Add total line
        reportBuilder.append("TOTAL")
                     .append(Protocol.SALES_REPORT_FIELD_DELIMITER)
                     .append(totalUnits)
                     .append(Protocol.SALES_REPORT_FIELD_DELIMITER)
                     .append(String.format("%.2f", totalRevenue));

        // Final message: SALES_REPORT | CATEGORY | categoryValue | aggregatedData
        return Protocol.SALES_REPORT + Protocol.DELIMITER + "CATEGORY" + Protocol.DELIMITER + category + Protocol.DELIMITER + reportBuilder.toString();
    }

    // --- NEW: Product Type Sales Handler ---
     private String handleGetSalesProductType(String productType) throws IllegalArgumentException {
         if (productType == null || productType.trim().isEmpty()) {
             throw new IllegalArgumentException("Product Type cannot be empty for GET_SALES_PRODTYPE");
         }
         productType = productType.trim();

         String requestId = UUID.randomUUID().toString().substring(0, 8);
         // Message: M_GET_PRODTYPE_SALES_TASK | requestId | productType
         String messageToWorkers = Protocol.M_GET_PRODTYPE_SALES_TASK + Protocol.DELIMITER + requestId + Protocol.DELIMITER + productType;

         List<String> workerResponses = master.broadcastRequestAndWaitForAll(messageToWorkers, 20000);

         // --- Aggregate results ---
         long totalUnits = 0;
         double totalRevenue = 0.0;
         Map<String, StoreSaleSummary> storeSummaries = new HashMap<>();

         for (String response : workerResponses) {
             // Expected format: PRODTYPE_SALES_RESULT | reqId | store1:units:rev;;store2:units:rev...
              String[] parts = response.split("\\" + Protocol.DELIMITER, 3);
              if (parts.length == 3 && parts[0].equals(Protocol.PRODTYPE_SALES_RESULT) && parts[1].equals(requestId)) {
                 String data = parts[2];
                  if (!data.isEmpty()) {
                      String[] storeEntries = data.split(Protocol.SALES_REPORT_DELIMITER);
                      for (String entry : storeEntries) {
                         String[] fields = entry.split(Protocol.SALES_REPORT_FIELD_DELIMITER);
                          if (fields.length == 3) {
                             try {
                                 String storeName = fields[0];
                                 long units = Long.parseLong(fields[1]);
                                 double revenue = Double.parseDouble(fields[2]);

                                  storeSummaries.compute(storeName, (key, current) -> {
                                      if (current == null) return new StoreSaleSummary(units, revenue);
                                      else {
                                          current.add(units, revenue);
                                          return current;
                                      }
                                  });

                              } catch (NumberFormatException e) {
                                   ColorfulStatusPrinter.printError(clientDesc + ": Error parsing product type sales data from worker response: " + entry);
                              }
                          }
                      }
                  }
              } else {
                  ColorfulStatusPrinter.printWarn(clientDesc + ": Received unexpected or non-matching worker response for PRODTYPE_SALES " + requestId + ": " + response); // Use printWarn
              }
         }

         // --- Format the final report ---
         StringBuilder reportBuilder = new StringBuilder();
         // Sort stores by name
          List<String> sortedStoreNames = new ArrayList<>(storeSummaries.keySet());
          Collections.sort(sortedStoreNames);

          for (String storeName : sortedStoreNames) {
              StoreSaleSummary summary = storeSummaries.get(storeName);
              reportBuilder.append(storeName)
                           .append(Protocol.SALES_REPORT_FIELD_DELIMITER)
                           .append(summary.totalUnits)
                           .append(Protocol.SALES_REPORT_FIELD_DELIMITER)
                           .append(String.format("%.2f", summary.totalRevenue))
                           .append(Protocol.SALES_REPORT_DELIMITER);
              totalUnits += summary.totalUnits;
              totalRevenue += summary.totalRevenue;
          }


         reportBuilder.append("TOTAL")
                      .append(Protocol.SALES_REPORT_FIELD_DELIMITER)
                      .append(totalUnits)
                      .append(Protocol.SALES_REPORT_FIELD_DELIMITER)
                      .append(String.format("%.2f", totalRevenue));

         // Final message: SALES_REPORT | PRODTYPE | productTypeValue | aggregatedData
         return Protocol.SALES_REPORT + Protocol.DELIMITER + "PRODTYPE" + Protocol.DELIMITER + productType + Protocol.DELIMITER + reportBuilder.toString();
     }

    // Helper class for aggregating sales
     private static class StoreSaleSummary {
         long totalUnits;
         double totalRevenue;

         StoreSaleSummary(long units, double revenue) {
             this.totalUnits = units;
             this.totalRevenue = revenue;
         }

         void add(long units, double revenue) {
             this.totalUnits += units;
             this.totalRevenue += revenue;
         }
     }


    // --- ASYNCHRONOUS Search Handler (MapReduce) ---
    private void handleSearch(String payload) throws IllegalArgumentException {
        String mapId = UUID.randomUUID().toString(); // Unique ID for this search operation
        String[] filters = payload.split("\\" + Protocol.DELIMITER);
        // SEARCH | lat | lon | radius | category | stars | price
        if (filters.length != 6) {
            // Send error back to client immediately for bad format
            sendError("Invalid SEARCH format: Expected 6 filter arguments separated by |");
             // throw new IllegalArgumentException("Invalid SEARCH format: Expected 6 filter arguments separated by |");
            return; // Don't proceed
        }
        // Basic validation of filters could be added here if desired before broadcasting

        List<Master.WorkerConnection> targetWorkers = master.getActiveWorkersSnapshot();
        int workersQueried = targetWorkers.size();
        if (workersQueried == 0) {
            ColorfulStatusPrinter.printWarn(clientDesc + ": No active workers available for search " + mapId); // Use printWarn
            // Send an empty result back immediately
            sendMessage(Protocol.SEARCH_RESULT + Protocol.DELIMITER + mapId + Protocol.DELIMITER + ""); // Empty payload
            return;
        }

        // Create and register the state object to track this search
        // Ensure this.out (the client's writer) is valid before passing it
         if (this.out == null || this.out.checkError()) {
            ColorfulStatusPrinter.printError(clientDesc + ": Cannot initiate search " + mapId + ", client connection seems closed.");
            return; // Don't proceed if client writer is bad
         }
        Master.SearchState state = new Master.SearchState(mapId, this.out, workersQueried);
        master.registerSearch(mapId, state); // Handles synchronization internally

        // Prepare the message for workers: MAP_TASK | mapId | original_payload
        String mapTaskPayload = mapId + Protocol.DELIMITER + payload;
        String messageToWorkers = Protocol.MAP_TASK + Protocol.DELIMITER + mapTaskPayload;

        // Broadcast the MAP_TASK asynchronously to all active workers
        int sentCount = master.broadcastToWorkers(messageToWorkers);
        ColorfulStatusPrinter.printInfo(clientDesc + ": Broadcasted search " + mapId + " (MAP_TASK) to " + sentCount + "/" + workersQueried + " workers.");

        // --- Wait for results using wait/notify on the SearchState lock ---
        ColorfulStatusPrinter.printLog(clientDesc + ": Waiting for search results (MapReduce) for mapId " + mapId + " (timeout 30s)...");
        long timeoutMillis = 30000; // 30 seconds timeout for the entire search operation
        long startTime = System.currentTimeMillis();
        boolean timedOut = false;

        synchronized (state.getLock()) { // Acquire the lock specific to this search state
            while (!state.isComplete() && (System.currentTimeMillis() - startTime) < timeoutMillis) {
                try {
                    long waitTime = timeoutMillis - (System.currentTimeMillis() - startTime);
                    if (waitTime <= 0) {
                        timedOut = true; // Mark as timed out
                        break; // Exit loop if timeout reached
                    }
                    state.getLock().wait(waitTime); // Wait for notification from SearchState.addResult()
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); // Re-interrupt this handler thread
                    ColorfulStatusPrinter.printError(clientDesc + ": Search " + mapId + " interrupted while waiting.");
                    master.removeSearch(mapId); // Clean up state
                    sendError("Search operation was interrupted."); // Inform client
                    return; // Stop processing this request
                }
            }
            // Check if timed out after the loop finishes (wait could finish just as timeout occurs)
            if (!state.isComplete() && !timedOut && (System.currentTimeMillis() - startTime) >= timeoutMillis) {
                 timedOut = true;
            }
        } // Release lock

        ColorfulStatusPrinter.printLog(clientDesc + ": Wait finished for mapId " + mapId + (timedOut ? " (TIMED OUT)" : ""));

        // --- Reduce Phase: Aggregate and Send Final Result ---
        String finalPayload = state.getFinalResultPayload(); // Get aggregated results first

         // Clean up the search state from the master's map AFTER getting payload
        master.removeSearch(mapId);

        if (timedOut) {
            ColorfulStatusPrinter.printError("Master: Search timed out for mapId: " + mapId + ". Got " + state.getReceivedResults() + "/" + state.expectedResults + " results.");
            // Optionally send partial results or an error message
             // Send partial results collected so far
             ColorfulStatusPrinter.printWarn(clientDesc + ": Search " + mapId + " timed out. Sending partial results (" + state.getResultCount() + " stores found)."); // Use printWarn
             // Fall through to send whatever results were collected in finalPayload
        }


        // Format the final response: SEARCH_RESULT | mapId | finalPayload
        String finalResponse = Protocol.SEARCH_RESULT + Protocol.DELIMITER + mapId + Protocol.DELIMITER + finalPayload;

        ColorfulStatusPrinter.printInfo(clientDesc + ": Sending final search results for " + mapId + " (" + state.getResultCount() + " stores found). Timed Out: " + timedOut);
        sendMessage(finalResponse); // Send the final aggregated results back to the client
    }


} // End ClientHandler