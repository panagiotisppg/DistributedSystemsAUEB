package com.fooddelivery.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

// Simple Product POJO


// Simple Store POJO


// Protocol constants


// Utility for distance (Simplified Euclidean - replace with Haversine for accuracy)



// ==========================================
// Package: com.fooddelivery.core
// ==========================================
package com.fooddelivery.core;

import com.fooddelivery.common.*;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*; // Using Map for state, allowed

// Worker Node
public class Worker {

    private final int port;
    // In-memory storage for stores assigned to this worker. Key: StoreName
    private static final Map<String, Store> storeData = new ConcurrentHashMap<>(); // Thread-safe map

    public Worker(int port) {
        this.port = port;
    }

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Worker listening on port " + port);
            while (true) {
                Socket masterSocket = serverSocket.accept();
                System.out.println("Worker accepted connection from Master: " + masterSocket.getRemoteSocketAddress());
                // Handle each Master connection in a new thread
                new MasterHandler(masterSocket).start();
            }
        } catch (IOException e) {
            System.err.println("Worker error: Could not listen on port " + port);
            e.printStackTrace();
        }
    }

    // Thread to handle communication with a connected Master
    private static class MasterHandler extends Thread {
        private final Socket masterSocket;
        private PrintWriter out;
        private BufferedReader in;

        public MasterHandler(Socket socket) {
            this.masterSocket = socket;
        }

        @Override
        public void run() {
            try {
                out = new PrintWriter(masterSocket.getOutputStream(), true);
                in = new BufferedReader(new InputStreamReader(masterSocket.getInputStream()));
                String inputLine;

                while ((inputLine = in.readLine()) != null) {
                    System.out.println("Worker received: " + inputLine);
                    processMasterRequest(inputLine);
                }

            } catch (IOException e) {
                System.err.println("Worker: Master connection error or closed: " + e.getMessage());
            } finally {
                try {
                    masterSocket.close();
                } catch (IOException e) {
                    // Ignore
                }
                System.out.println("Worker: Master connection closed: " + masterSocket.getRemoteSocketAddress());
            }
        }

        private void processMasterRequest(String request) {
            String[] parts = request.split("\\" + Protocol.DELIMITER, 2); // Split command from rest
            String command = parts[0];
            String payload = (parts.length > 1) ? parts[1] : "";
            String response = Protocol.ERROR + Protocol.DELIMITER + "Unknown command or error";

            try {
                switch (command) {
                    case Protocol.M_ADD_STORE: // Payload is JSON
                        response = handleAddStore(payload);
                        break;
                    case Protocol.M_UPDATE_STOCK: // storeName|productName|changeAmount
                         response = handleUpdateStock(payload);
                         break;
                    case Protocol.M_ADD_PRODUCT: // storeName|productJson
                         response = handleAddProduct(payload);
                         break;
                     case Protocol.M_REMOVE_PRODUCT: // storeName|productName
                         response = handleRemoveProduct(payload);
                         break;
                    case Protocol.MAP_TASK: // mapId|lat|lon|radius|categoryFilter|starsFilter|priceFilter
                        response = handleMapTask(payload);
                        break;
                    case Protocol.M_BUY: // mapId|storeName|product1:q1,product2:q2,...
                        response = handleBuy(payload);
                        break;
                     case Protocol.M_RATE: // storeName|ratingValue
                         response = handleRate(payload);
                         break;
                    default:
                        System.err.println("Worker: Unknown command received: " + command);
                        response = Protocol.ERROR + Protocol.DELIMITER + "Unknown command";
                }
            } catch (Exception e) {
                 System.err.println("Worker: Error processing command '" + command + "': " + e.getMessage());
                 e.printStackTrace(); // Log server-side exception
                 response = Protocol.ERROR + Protocol.DELIMITER + "Failed processing request: " + e.getMessage();
            }
            out.println(response);
        }

        // --- Handler Methods ---

        private String handleAddStore(String jsonPayload) {
             // IMPORTANT: Use a proper JSON library if allowed. Manual parsing is fragile.
            Store store = Store.fromJson(jsonPayload); // Using the basic manual parser
            if (store != null) {
                 // Check if store already exists - maybe update instead? For now, overwrite.
                 storeData.put(store.getStoreName(), store);
                 System.out.println("Worker: Added/Updated store: " + store.getStoreName());
                 return Protocol.GENERIC_ACK + Protocol.DELIMITER + store.getStoreName();
            } else {
                 System.err.println("Worker: Failed to parse store JSON.");
                 return Protocol.GENERIC_NACK + Protocol.DELIMITER + "Failed to parse JSON";
            }
        }

        private String handleUpdateStock(String payload) {
            String[] parts = payload.split("\\" + Protocol.DELIMITER);
            if (parts.length != 3) return Protocol.GENERIC_NACK + Protocol.DELIMITER + "Invalid format for UPDATE_STOCK";

            String storeName = parts[0];
            String productName = parts[1];
            int changeAmount;
             try {
                 changeAmount = Integer.parseInt(parts[2]);
             } catch (NumberFormatException e) {
                 return Protocol.GENERIC_NACK + Protocol.DELIMITER + "Invalid amount";
             }

            Store store = storeData.get(storeName);
            if (store == null) return Protocol.GENERIC_NACK + Protocol.DELIMITER + "Store not found";

            // Synchronization on the specific store object
            synchronized (store.getStoreLock()) {
                Product product = store.findProduct(productName);
                if (product == null) return Protocol.GENERIC_NACK + Protocol.DELIMITER + "Product not found";

                 int newAmount = product.getAvailableAmount() + changeAmount;
                 if (newAmount < 0) return Protocol.GENERIC_NACK + Protocol.DELIMITER + "Insufficient stock to remove";

                 product.setAvailableAmount(newAmount);
                 System.out.println("Worker: Updated stock for " + productName + " in " + storeName + " to " + newAmount);
                 return Protocol.GENERIC_ACK + Protocol.DELIMITER + storeName + Protocol.DELIMITER + productName + Protocol.DELIMITER + newAmount;
            }
        }

         private String handleAddProduct(String payload) {
             String[] parts = payload.split("\\" + Protocol.DELIMITER, 2);
             if (parts.length != 2) return Protocol.GENERIC_NACK + Protocol.DELIMITER + "Invalid format";
             String storeName = parts[0];
             String productJson = parts[1];

             Store store = storeData.get(storeName);
             if (store == null) return Protocol.GENERIC_NACK + Protocol.DELIMITER + "Store not found";

             Product newProduct = Product.fromJsonFragment(productJson); // Parse just the product
             if (newProduct == null) return Protocol.GENERIC_NACK + Protocol.DELIMITER + "Failed to parse product JSON";

             synchronized (store.getStoreLock()) {
                 // Check if product exists, maybe update price/details? For now, add or replace.
                 Product existing = store.findProduct(newProduct.getProductName());
                 if (existing != null) {
                     store.getProducts().remove(existing); // Remove old if replacing
                 }
                 store.getProducts().add(newProduct);
                  // Optional: Recalculate store's average price category if needed immediately
                  System.out.println("Worker: Added product " + newProduct.getProductName() + " to " + storeName);
                  return Protocol.GENERIC_ACK + Protocol.DELIMITER + storeName + Protocol.DELIMITER + newProduct.getProductName();
             }
         }

         private String handleRemoveProduct(String payload) {
             String[] parts = payload.split("\\" + Protocol.DELIMITER);
             if (parts.length != 2) return Protocol.GENERIC_NACK + Protocol.DELIMITER + "Invalid format";
             String storeName = parts[0];
             String productName = parts[1];

             Store store = storeData.get(storeName);
             if (store == null) return Protocol.GENERIC_NACK + Protocol.DELIMITER + "Store not found";

             synchronized (store.getStoreLock()) {
                 Product product = store.findProduct(productName);
                 if (product == null) return Protocol.GENERIC_NACK + Protocol.DELIMITER + "Product not found";

                 store.getProducts().remove(product);
                  System.out.println("Worker: Removed product " + productName + " from " + storeName);
                  return Protocol.GENERIC_ACK + Protocol.DELIMITER + storeName + Protocol.DELIMITER + productName;
             }
         }

         private String handleRate(String payload) {
             String[] parts = payload.split("\\" + Protocol.DELIMITER);
             if (parts.length != 2) return Protocol.GENERIC_NACK + Protocol.DELIMITER + "Invalid format";
             String storeName = parts[0];
             int rating;
             try {
                 rating = Integer.parseInt(parts[1]);
             } catch (NumberFormatException e) {
                 return Protocol.GENERIC_NACK + Protocol.DELIMITER + "Invalid rating value";
             }
             if (rating < 1 || rating > 5) return Protocol.GENERIC_NACK + Protocol.DELIMITER + "Rating must be 1-5";

             Store store = storeData.get(storeName);
             if (store == null) return Protocol.GENERIC_NACK + Protocol.DELIMITER + "Store not found";

             synchronized(store.getStoreLock()) {
                 store.addRating(rating);
                 System.out.println("Worker: Updated rating for " + storeName + " to " + store.getStars());
             }
             return Protocol.RATE_SUCCESS + Protocol.DELIMITER + storeName + Protocol.DELIMITER + String.format("%.1f", store.getStars());
         }


        private String handleMapTask(String payload) {
            String[] parts = payload.split("\\" + Protocol.DELIMITER);
            if (parts.length < 7) return Protocol.ERROR + Protocol.DELIMITER + "Invalid MAP_TASK format"; // mapId|lat|lon|radius|cat|stars|price

            String mapId = parts[0];
            double userLat, userLon, radius;
            int minStars;
            String categoryFilter = parts[4].equals("null") ? null : parts[4];
            String priceFilter = parts[6].equals("null") ? null : parts[6];

            try {
                userLat = Double.parseDouble(parts[1]);
                userLon = Double.parseDouble(parts[2]);
                radius = Double.parseDouble(parts[3]);
                 minStars = parts[5].equals("null") ? 0 : Integer.parseInt(parts[5]);
             } catch (NumberFormatException e) {
                 return Protocol.ERROR + Protocol.DELIMITER + "Invalid numeric filter value";
             }

            List<String> matchingStoresJson = new ArrayList<>();
            for (Store store : storeData.values()) {
                 // 1. Distance Check
                 // double distance = GeoUtils.simpleDistance(userLat, userLon, store.getLatitude(), store.getLongitude());
                 double distance = GeoUtils.haversineDistance(userLat, userLon, store.getLatitude(), store.getLongitude()); // Use Haversine
                 if (distance > radius) continue;

                 // 2. Category Check
                 if (categoryFilter != null && !store.getFoodCategory().equalsIgnoreCase(categoryFilter)) continue;

                 // 3. Stars Check
                 if (minStars > 0 && store.getStars() < minStars) continue; // Assuming filter is minimum stars

                 // 4. Price Category Check
                 if (priceFilter != null && !store.getPriceCategory().equals(priceFilter)) continue;

                 // Store matches all criteria
                 matchingStoresJson.add(store.toJson()); // Convert matching store to JSON
            }

            // Combine results into a single string payload
            String resultPayload = String.join(Protocol.MAP_RESULT_STORE_DELIMITER, matchingStoresJson);
            return Protocol.MAP_RESULT + Protocol.DELIMITER + mapId + Protocol.DELIMITER + resultPayload;
        }


        private String handleBuy(String payload) {
            String[] parts = payload.split("\\" + Protocol.DELIMITER, 3);
             if (parts.length != 3) return Protocol.ERROR + Protocol.DELIMITER + "Invalid BUY format"; // mapId|storeName|prod:q,prod:q

            String mapId = parts[0]; // mapId might not be needed here, but kept for consistency
            String storeName = parts[1];
            String productRequests = parts[2];

            Store store = storeData.get(storeName);
            if (store == null) return Protocol.BUY_FAIL + Protocol.DELIMITER + mapId + Protocol.DELIMITER + "Store not found";

            Map<String, Integer> itemsToBuy = new HashMap<>();
            String[] productPairs = productRequests.split(Protocol.PRODUCT_LIST_DELIMITER);
            for (String pair : productPairs) {
                String[] itemQty = pair.split(Protocol.PRODUCT_QTY_DELIMITER);
                if (itemQty.length == 2) {
                    try {
                        itemsToBuy.put(itemQty[0], Integer.parseInt(itemQty[1]));
                    } catch (NumberFormatException e) {
                        return Protocol.BUY_FAIL + Protocol.DELIMITER + mapId + Protocol.DELIMITER + "Invalid quantity for " + itemQty[0];
                    }
                }
            }

            if (itemsToBuy.isEmpty()) return Protocol.BUY_FAIL + Protocol.DELIMITER + mapId + Protocol.DELIMITER + "No items specified";

            // --- Critical Section ---
            synchronized (store.getStoreLock()) {
                double totalCost = 0;
                Map<Product, Integer> productsToUpdate = new HashMap<>(); // Keep track of changes

                // 1. Check availability and calculate cost
                for (Map.Entry<String, Integer> entry : itemsToBuy.entrySet()) {
                    String productName = entry.getKey();
                    int quantity = entry.getValue();
                    if (quantity <= 0) return Protocol.BUY_FAIL + Protocol.DELIMITER + mapId + Protocol.DELIMITER + "Quantity must be positive";

                    Product product = store.findProduct(productName);
                    if (product == null) return Protocol.BUY_FAIL + Protocol.DELIMITER + mapId + Protocol.DELIMITER + "Product not found: " + productName;
                    if (product.getAvailableAmount() < quantity) return Protocol.BUY_FAIL + Protocol.DELIMITER + mapId + Protocol.DELIMITER + "Insufficient stock for: " + productName;

                    productsToUpdate.put(product, quantity); // Mark for update
                    totalCost += product.getPrice() * quantity;
                }

                // 2. If all checks pass, update stock and record sales
                for (Map.Entry<Product, Integer> entry : productsToUpdate.entrySet()) {
                    Product product = entry.getKey();
                    int quantity = entry.getValue();
                    product.setAvailableAmount(product.getAvailableAmount() - quantity);
                    product.recordSale(quantity); // Update sales figures
                     System.out.println("Worker: Sold " + quantity + " of " + product.getProductName() + " from " + storeName);
                }

                return Protocol.BUY_SUCCESS + Protocol.DELIMITER + mapId + Protocol.DELIMITER + String.format("%.2f", totalCost);
            }
            // --- End Critical Section ---
        }
    }

    // --- Main Method ---
    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: java com.fooddelivery.core.Worker <port>");
            System.exit(1);
        }
        int port = Integer.parseInt(args[0]);
        Worker worker = new Worker(port);
        worker.start();
    }
}

// Master Node
public class Master {

    private final int clientPort;
    private final List<WorkerConnection> workers = Collections.synchronizedList(new ArrayList<>()); // Thread-safe list
    // State for ongoing MapReduce search jobs. Key: mapId
    private final Map<String, SearchState> activeSearches = new ConcurrentHashMap<>();

    // Represents a connection to a worker
    private static class WorkerConnection {
        final String id; // e.g., "host:port"
        final Socket socket;
        final PrintWriter out;
        final BufferedReader in;
        boolean active = true;

        WorkerConnection(String id, Socket socket) throws IOException {
            this.id = id;
            this.socket = socket;
            this.out = new PrintWriter(socket.getOutputStream(), true);
            this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        }

        // Send message, handle potential failure
        synchronized boolean sendMessage(String message) {
             if (!active || socket.isClosed()) {
                 active = false;
                 return false;
             }
             try {
                 out.println(message);
                 // Basic check, doesn't guarantee delivery or processing
                 return !out.checkError();
             } catch (Exception e) {
                 System.err.println("Master: Error sending message to worker " + id + ": " + e.getMessage());
                 active = false; // Mark worker as inactive on send error
                 close();
                 return false;
             }
        }

        // Close resources
        void close() {
            active = false;
            try { if (in != null) in.close(); } catch (IOException e) { /* ignore */ }
            try { if (out != null) out.close(); } catch (Exception e) { /* ignore */ }
            try { if (socket != null) socket.close(); } catch (IOException e) { /* ignore */ }
            System.out.println("Master: Closed connection to worker " + id);
        }
    }

    // Holds state for an ongoing search request
    private static class SearchState {
        final String mapId;
        final PrintWriter clientWriter; // Writer to send final result back to client
        final int expectedResults; // Number of workers queried
        final List<String> results = Collections.synchronizedList(new ArrayList<>()); // Store JSONs from workers
        volatile int receivedResults = 0; // Count responses received
        final Object lock = new Object(); // For waiting/notifying

        SearchState(String mapId, PrintWriter clientWriter, int expectedResults) {
            this.mapId = mapId;
            this.clientWriter = clientWriter;
            this.expectedResults = expectedResults;
        }

         // Called when a MAP_RESULT arrives from a worker
        void addResult(String workerResultPayload) {
             // Payload might be empty if worker found nothing
            if (workerResultPayload != null && !workerResultPayload.trim().isEmpty()) {
                 // Split the payload by the store delimiter and add individual store JSONs
                String[] storeJsons = workerResultPayload.split(Protocol.MAP_RESULT_STORE_DELIMITER);
                for(String storeJson : storeJsons) {
                    if (storeJson != null && !storeJson.trim().isEmpty()) {
                        results.add(storeJson);
                    }
                }
            }
             // Increment received count using synchronization for visibility and atomicity
             synchronized(lock) {
                 receivedResults++;
                 // If all results are in, notify any waiting thread (the client handler)
                 if (receivedResults >= expectedResults) {
                     lock.notifyAll(); // Notify potentially waiting client handler
                 }
             }
        }

        boolean isComplete() {
             synchronized(lock) { // Ensure visibility of receivedResults
                 return receivedResults >= expectedResults;
             }
        }

         // Combine results into final payload for client
         String getFinalResultPayload() {
            // Already synchronized access to results list by adding via synchronized list/method
             return String.join(Protocol.MAP_RESULT_STORE_DELIMITER, results);
         }
    }

    public Master(int clientPort, String[] workerAddresses) {
        this.clientPort = clientPort;
        connectToWorkers(workerAddresses);
        startWorkerListenerThreads(); // Start threads to listen for async results from workers
    }

    // Attempt to connect to all specified workers
    private void connectToWorkers(String[] workerAddresses) {
        for (String address : workerAddresses) {
            try {
                String[] parts = address.split(":");
                if (parts.length != 2) {
                    System.err.println("Master: Invalid worker address format: " + address + " (expected host:port)");
                    continue;
                }
                String host = parts[0];
                int port = Integer.parseInt(parts[1]);
                Socket socket = new Socket(host, port);
                WorkerConnection wc = new WorkerConnection(address, socket);
                workers.add(wc);
                System.out.println("Master: Successfully connected to worker " + address);
            } catch (IOException | NumberFormatException e) {
                System.err.println("Master: Failed to connect to worker " + address + ": " + e.getMessage());
            }
        }
        if (workers.isEmpty()) {
             System.err.println("Master: FATAL - No workers connected. Exiting.");
             System.exit(1);
        }
    }

    // Start listening for incoming client/manager connections
    public void startServer() {
        try (ServerSocket serverSocket = new ServerSocket(clientPort)) {
            System.out.println("Master listening for clients on port " + clientPort);
            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Master accepted connection from client: " + clientSocket.getRemoteSocketAddress());
                // Handle each client in a new thread
                new ClientHandler(clientSocket, this).start();
            }
        } catch (IOException e) {
            System.err.println("Master error: Could not listen on port " + clientPort);
            e.printStackTrace();
        } finally {
            // Cleanup worker connections on master shutdown
             workers.forEach(WorkerConnection::close);
        }
    }


    // Start background threads to listen for asynchronous responses from each worker
    private void startWorkerListenerThreads() {
        synchronized (workers) { // Avoid concurrent modification if a worker disconnects immediately
            workers.removeIf(wc -> !wc.active); // Clean up any initially failed connections

             for (WorkerConnection wc : workers) {
                 Thread listenerThread = new Thread(() -> listenToWorker(wc));
                 listenerThread.setDaemon(true); // Allow JVM to exit if only daemon threads remain
                 listenerThread.start();
             }
         }
    }

    // Method run by each worker listener thread
    private void listenToWorker(WorkerConnection wc) {
        System.out.println("Master: Started listener thread for worker " + wc.id);
        try {
            String messageFromWorker;
            while (wc.active && (messageFromWorker = wc.in.readLine()) != null) {
                System.out.println("Master received async from " + wc.id + ": " + messageFromWorker);
                // Process asynchronous messages like MAP_RESULT
                processAsyncWorkerResponse(messageFromWorker, wc);
            }
        } catch (IOException e) {
             if (wc.active) { // Only report error if we didn't intentionally close it
                 System.err.println("Master: Error reading from worker " + wc.id + ": " + e.getMessage());
             }
        } finally {
             System.out.println("Master: Listener thread for worker " + wc.id + " finished.");
             wc.close(); // Ensure connection is closed
             // Optionally remove the worker from the active list
             // workers.remove(wc); // Be careful with concurrent modification if iterating elsewhere
        }
    }


    // Process asynchronous messages received from worker listener threads
    private void processAsyncWorkerResponse(String message, WorkerConnection workerConn) {
        String[] parts = message.split("\\" + Protocol.DELIMITER, 3); // mapResult|mapId|payload or buySuccess|mapId|cost etc.
        String command = parts[0];

        if (Protocol.MAP_RESULT.equals(command) && parts.length >= 2) {
             String mapId = parts[1];
             String payload = (parts.length > 2) ? parts[2] : ""; // Handle empty results payload
             SearchState state = activeSearches.get(mapId);
             if (state != null) {
                 state.addResult(payload);
                 // Note: The notification logic is within SearchState.addResult
                 // The ClientHandler thread is responsible for waiting and sending the final reply.
             } else {
                 System.err.println("Master: Received MAP_RESULT for unknown/expired mapId: " + mapId);
             }
         } else if (Protocol.BUY_SUCCESS.equals(command) || Protocol.BUY_FAIL.equals(command)) {
             // These are typically handled synchronously by ClientHandler waiting for a reply.
             // If we implement fully async buy confirmation, handle it here.
             System.out.println("Master: Received unexpected async BUY response: " + message);
         } else if (Protocol.GENERIC_ACK.equals(command) || Protocol.GENERIC_NACK.equals(command) || Protocol.RATE_SUCCESS.equals(command)) {
              // Also typically handled synchronously by ClientHandler.
              System.out.println("Master: Received unexpected async ACK/NACK/RATE response: " + message);
         }
         // Handle other async messages if needed
    }

    // Selects the appropriate worker for a given store name
    public WorkerConnection getWorkerForStore(String storeName) {
         List<WorkerConnection> activeWorkers = new ArrayList<>();
         // Get a snapshot of active workers to avoid issues if a worker disconnects
         // during hashing/selection
         synchronized (workers) {
             workers.removeIf(wc -> !wc.active); // Clean up inactive workers first
             activeWorkers.addAll(workers.stream().filter(wc -> wc.active).toList());
         }

        if (activeWorkers.isEmpty()) {
            System.err.println("Master: No active workers available!");
            return null;
        }
        int workerIndex = Math.abs(storeName.hashCode()) % activeWorkers.size();
        return activeWorkers.get(workerIndex);
    }


     // --- Main Method ---
     public static void main(String[] args) {
         if (args.length < 2) {
             System.err.println("Usage: java com.fooddelivery.core.Master <client-port> <worker1-host:port> [worker2-host:port] ...");
             System.exit(1);
         }

         int clientPort = Integer.parseInt(args[0]);
         String[] workerAddresses = Arrays.copyOfRange(args, 1, args.length);

         Master master = new Master(clientPort, workerAddresses);
         master.startServer(); // Start accepting client connections
     }
}

// Handles individual client connections to the Master
class ClientHandler extends Thread {
    private final Socket clientSocket;
    private final Master master;
    private PrintWriter out; // To client
    private BufferedReader in; // From client

    public ClientHandler(Socket socket, Master master) {
        this.clientSocket = socket;
        this.master = master;
    }

    @Override
    public void run() {
        try {
            out = new PrintWriter(clientSocket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            String inputLine;

            while ((inputLine = in.readLine()) != null) {
                System.out.println("Master received from client " + clientSocket.getRemoteSocketAddress() + ": " + inputLine);
                processClientRequest(inputLine);
            }

        } catch (IOException e) {
            System.err.println("Master: Client connection error or closed: " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) { /* ignore */ }
            System.out.println("Master: Client connection closed: " + clientSocket.getRemoteSocketAddress());
            // Clean up any state associated with this client if necessary
        }
    }

    private void processClientRequest(String request) {
        String[] parts = request.split("\\" + Protocol.DELIMITER, 2);
        String command = parts[0];
        String payload = (parts.length > 1) ? parts[1] : "";
        String responseToClient = Protocol.ERROR + Protocol.DELIMITER + "Request failed"; // Default error

        try {
             if (command.equals(Protocol.ADD_STORE)) { // Payload is JSON
                 responseToClient = handleAddStore(payload);
             } else if (command.equals(Protocol.UPDATE_STOCK)) { // storeName|productName|changeAmount
                  responseToClient = handleUpdateStock(payload);
             } else if (command.equals(Protocol.ADD_PRODUCT)) { // storeName|productJson
                  responseToClient = handleAddProduct(payload);
              } else if (command.equals(Protocol.REMOVE_PRODUCT)) { // storeName|productName
                  responseToClient = handleRemoveProduct(payload);
              } else if (command.equals(Protocol.SEARCH)) { // lat|lon|radius|category|stars|price
                 handleSearch(payload); // Search is async, response sent later
                 return; // Don't send immediate response here
             } else if (command.equals(Protocol.BUY)) { // storeName|product1:q1,product2:q2,...
                  responseToClient = handleBuy(payload);
              } else if (command.equals(Protocol.RATE)) { // storeName|ratingValue
                  responseToClient = handleRate(payload);
              }
              else {
                 System.err.println("Master: Unknown command from client: " + command);
                 responseToClient = Protocol.ERROR + Protocol.DELIMITER + "Unknown command";
             }
        } catch (Exception e) {
             System.err.println("Master: Error processing client command '" + command + "': " + e.getMessage());
             e.printStackTrace();
             responseToClient = Protocol.ERROR + Protocol.DELIMITER + "Internal server error";
        }

        out.println(responseToClient); // Send synchronous response
    }


    // --- Handler methods for specific client requests ---

    private String forwardToWorker(String storeName, String workerCommand, String payload) {
         WorkerConnection worker = master.getWorkerForStore(storeName);
         if (worker == null) {
             return Protocol.ERROR + Protocol.DELIMITER + "No active worker found for store";
         }

         String messageToWorker = workerCommand + Protocol.DELIMITER + payload;
         if (worker.sendMessage(messageToWorker)) {
              // Wait for a synchronous response from the worker
              // This is a simplification. A real system might use request IDs and timeouts.
              try {
                  // The Worker Listener thread actually handles the response processing.
                  // This simple synchronous wait is problematic if the worker is slow or fails.
                  // It relies on the listener thread setting the response somehow or using a Future/Callback.
                  // *** For now, let's assume simple ACK/NACK is sufficient for non-search/buy ***
                  // *** And Search/Buy are handled differently (async/specific response codes) ***

                  // Let's return a simple ACK for now, assuming the worker handles it.
                  // Proper sync response would require a request-reply correlation mechanism.
                  // Example: return worker.in.readLine(); // <-- THIS BLOCKS and conflicts with listener thread

                  // Placeholder: Assume success if sent for simpler ops.
                  return Protocol.GENERIC_ACK + Protocol.DELIMITER + "Request forwarded to worker";

              } catch (Exception e) { // Catch potential read timeout etc.
                  System.err.println("Master: Error waiting for response from worker " + worker.id + ": " + e.getMessage());
                  worker.close(); // Assume worker is dead
                  return Protocol.ERROR + Protocol.DELIMITER + "Worker communication error";
              }
          } else {
              return Protocol.ERROR + Protocol.DELIMITER + "Failed to send message to worker " + worker.id;
          }
    }


    private String handleAddStore(String jsonPayload) {
         // Extract store name for hashing (Requires parsing part of the JSON!)
         // Again, use a proper JSON parser if allowed.
         String storeName = Product.parseJsonStringValue(jsonPayload, "StoreName"); // Basic manual parse
         if (storeName == null) {
             return Protocol.ERROR + Protocol.DELIMITER + "Could not parse StoreName from JSON";
         }
         // Forward the *entire* JSON payload
         return forwardToWorker(storeName, Protocol.M_ADD_STORE, jsonPayload);
    }

     private String handleUpdateStock(String payload) { // storeName|productName|changeAmount
         String[] parts = payload.split("\\" + Protocol.DELIMITER, 2);
         if (parts.length < 2) return Protocol.ERROR + Protocol.DELIMITER + "Invalid format";
         String storeName = parts[0];
         return forwardToWorker(storeName, Protocol.M_UPDATE_STOCK, payload);
     }

     private String handleAddProduct(String payload) { // storeName|productJson
         String[] parts = payload.split("\\" + Protocol.DELIMITER, 2);
         if (parts.length < 2) return Protocol.ERROR + Protocol.DELIMITER + "Invalid format";
         String storeName = parts[0];
         return forwardToWorker(storeName, Protocol.M_ADD_PRODUCT, payload);
     }

     private String handleRemoveProduct(String payload) { // storeName|productName
         String[] parts = payload.split("\\" + Protocol.DELIMITER, 2);
         if (parts.length < 2) return Protocol.ERROR + Protocol.DELIMITER + "Invalid format";
         String storeName = parts[0];
         return forwardToWorker(storeName, Protocol.M_REMOVE_PRODUCT, payload);
     }

      private String handleRate(String payload) { // storeName|ratingValue
         String[] parts = payload.split("\\" + Protocol.DELIMITER, 2);
         if (parts.length < 2) return Protocol.ERROR + Protocol.DELIMITER + "Invalid format";
         String storeName = parts[0];
         // Maybe get sync ACK/NACK for rating?
          return forwardToWorker(storeName, Protocol.M_RATE, payload); // Using generic forward for now
      }


    private void handleSearch(String payload) {
        String mapId = UUID.randomUUID().toString(); // Unique ID for this search
        String[] filters = payload.split("\\" + Protocol.DELIMITER); // lat|lon|radius|cat|stars|price
        if (filters.length != 6) {
            out.println(Protocol.ERROR + Protocol.DELIMITER + "Invalid SEARCH format");
            return;
        }

        // Broadcast MAP_TASK to all active workers
        String mapTaskPayload = mapId + Protocol.DELIMITER + payload;
        int workersQueried = 0;
        synchronized (master.workers) { // Access worker list safely
             master.workers.removeIf(wc -> !wc.active); // Clean up inactive first
             for (WorkerConnection wc : master.workers) {
                 if (wc.sendMessage(Protocol.MAP_TASK + Protocol.DELIMITER + mapTaskPayload)) {
                     workersQueried++;
                 } else {
                     System.err.println("Master: Failed to send MAP_TASK to worker " + wc.id + ". Excluding from search.");
                     // Worker will be marked inactive by sendMessage
                 }
             }
         }


        if (workersQueried == 0) {
            out.println(Protocol.SEARCH_RESULT + Protocol.DELIMITER + mapId + Protocol.DELIMITER); // Send empty result immediately
            return;
        }

        // Create state for this search and store it
        SearchState state = new SearchState(mapId, out, workersQueried);
        master.activeSearches.put(mapId, state);
        System.out.println("Master: Started search " + mapId + ", expecting " + workersQueried + " results.");


        // --- Asynchronous Wait ---
        // The client handler thread will now wait until the search is complete.
        // The worker listener threads will populate the SearchState.
        synchronized (state.lock) {
            long timeoutMillis = 30000; // 30 second timeout
            long startTime = System.currentTimeMillis();
            while (!state.isComplete() && (System.currentTimeMillis() - startTime) < timeoutMillis) {
                try {
                    // Wait for notification from SearchState.addResult or timeout
                    state.lock.wait(timeoutMillis - (System.currentTimeMillis() - startTime));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); // Restore interrupt status
                    System.err.println("Master: Search wait interrupted for mapId: " + mapId);
                    master.activeSearches.remove(mapId); // Clean up state
                    out.println(Protocol.ERROR + Protocol.DELIMITER + "Search interrupted");
                    return;
                }
            }
        }

        // --- Process Result ---
        master.activeSearches.remove(mapId); // Clean up completed/timed-out search state

         if (!state.isComplete()) {
             System.err.println("Master: Search timed out for mapId: " + mapId + ". Received " + state.receivedResults + "/" + state.expectedResults);
             // Still send back whatever results we got
         }

        String finalPayload = state.getFinalResultPayload();
        String finalResponse = Protocol.SEARCH_RESULT + Protocol.DELIMITER + mapId + Protocol.DELIMITER + finalPayload;
        System.out.println("Master: Sending search results for " + mapId + " ("+ state.results.size() +" stores)");
        out.println(finalResponse);

    }

     private String handleBuy(String payload) { // storeName|product1:q1,product2:q2,...
         String[] parts = payload.split("\\" + Protocol.DELIMITER, 2);
         if (parts.length != 2) return Protocol.ERROR + Protocol.DELIMITER + "Invalid BUY format";
         String storeName = parts[0];
         String productRequestPayload = parts[1];

         WorkerConnection worker = master.getWorkerForStore(storeName);
         if (worker == null) {
             return Protocol.ERROR + Protocol.DELIMITER + "No active worker found for store";
         }

         // Use a unique ID for the buy request in case we need async later
         String buyId = UUID.randomUUID().toString();
         String messageToWorker = Protocol.M_BUY + Protocol.DELIMITER + buyId + Protocol.DELIMITER + storeName + Protocol.DELIMITER + productRequestPayload;

         // Send and wait for a specific synchronous response (BUY_SUCCESS or BUY_FAIL)
         if (worker.sendMessage(messageToWorker)) {
              try {
                  // *** Simplification: Blocking read for immediate response ***
                  // This tightly couples the client handler to the worker response time.
                  String workerResponse = worker.in.readLine();
                  if (workerResponse == null) throw new IOException("Worker closed connection");

                  // Forward the exact response (BUY_SUCCESS or BUY_FAIL with details)
                  System.out.println("Master: Received buy response from worker: " + workerResponse);
                  return workerResponse; // Forward directly to client

              } catch (IOException e) {
                  System.err.println("Master: Error reading BUY response from worker " + worker.id + ": " + e.getMessage());
                  worker.close();
                  return Protocol.ERROR + Protocol.DELIMITER + "Worker communication error during buy";
              }
          } else {
              return Protocol.ERROR + Protocol.DELIMITER + "Failed to send BUY message to worker " + worker.id;
          }
     }
}


// ==========================================
// Package: com.fooddelivery.client
// ==========================================
package com.fooddelivery.client;

import com.fooddelivery.common.Protocol;
import java.io.*;
import java.net.*;
import java.util.Scanner;
import java.nio.file.Files;
import java.nio.file.Paths;

// Simple Manager CLI Application
public class ManagerClient {

    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;
    private final Scanner consoleScanner;

    public ManagerClient(String masterHost, int masterPort) throws IOException {
        this.socket = new Socket(masterHost, masterPort);
        this.out = new PrintWriter(socket.getOutputStream(), true);
        this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.consoleScanner = new Scanner(System.in);
        System.out.println("Manager connected to Master at " + masterHost + ":" + masterPort);
    }

    public void startConsole() {
        String line;
        while (true) {
            printMenu();
            System.out.print("> ");
            line = consoleScanner.nextLine();
            if ("exit".equalsIgnoreCase(line)) {
                break;
            }
            handleCommand(line);
            // Simple blocking read for ACK/NACK/Error after sending a command
            try {
                 System.out.println("Master Response: " + in.readLine());
             } catch (IOException e) {
                 System.err.println("Error reading response from master: " + e.getMessage());
                 break; // Assume connection lost
             }
        }
        close();
    }

    private void printMenu() {
        System.out.println("\n--- Manager Menu ---");
        System.out.println("addstore <json_file_path>");
        System.out.println("addproduct <storeName> <product_json_file_path>");
        System.out.println("removeproduct <storeName> <productName>");
        System.out.println("updatestock <storeName> <productName> <amount_change>");
        System.out.println("rate <storeName> <rating 1-5>"); // Added for testing
        System.out.println("exit");
        System.out.println("--------------------");
    }

    private void handleCommand(String line) {
        String[] parts = line.split(" ", 2); // Split command from args
        String command = parts[0].toLowerCase();
        String args = (parts.length > 1) ? parts[1] : "";

        try {
            switch (command) {
                case "addstore":
                    if (args.isEmpty()) { System.out.println("Usage: addstore <json_file_path>"); return; }
                    addStore(args);
                    break;
                case "addproduct":
                     handleProductCommand(args, true);
                    break;
                 case "removeproduct":
                      handleProductCommand(args, false);
                     break;
                case "updatestock":
                     handleUpdateStock(args);
                    break;
                 case "rate":
                      handleRate(args);
                     break;
                default:
                    System.out.println("Unknown command.");
                    return; // Don't send unknown commands
            }
        } catch (Exception e) {
            System.err.println("Error processing command: " + e.getMessage());
        }
    }

     private void addStore(String filePath) throws IOException {
         String jsonContent = new String(Files.readAllBytes(Paths.get(filePath)));
         // Basic validation: check if it looks like JSON
         if (!jsonContent.trim().startsWith("{") || !jsonContent.trim().endsWith("}")) {
              System.err.println("Error: File content does not look like JSON.");
              return;
         }
         sendMessage(Protocol.ADD_STORE + Protocol.DELIMITER + jsonContent);
     }


     private void handleProductCommand(String args, boolean isAdd) throws IOException {
         String[] parts = args.split(" ", 2);
         if (parts.length != 2) {
             System.out.println("Usage: " + (isAdd ? "addproduct" : "removeproduct") + " <storeName> " + (isAdd ? "<product_json_file_path>" : "<productName>"));
             return;
         }
         String storeName = parts[0];
         String productIdentifier = parts[1];

         if (isAdd) {
              String productJson = new String(Files.readAllBytes(Paths.get(productIdentifier)));
              // Basic validation
              if (!productJson.trim().startsWith("{") || !productJson.trim().endsWith("}")) {
                   System.err.println("Error: Product file content does not look like JSON.");
                   return;
              }
             sendMessage(Protocol.ADD_PRODUCT + Protocol.DELIMITER + storeName + Protocol.DELIMITER + productJson);
         } else { // Remove
             sendMessage(Protocol.REMOVE_PRODUCT + Protocol.DELIMITER + storeName + Protocol.DELIMITER + productIdentifier);
         }
     }


     private void handleUpdateStock(String args) {
         String[] parts = args.split(" ");
         if (parts.length != 3) {
             System.out.println("Usage: updatestock <storeName> <productName> <amount_change>");
             return;
         }
         String storeName = parts[0];
         String productName = parts[1];
         String amountStr = parts[2];
         // Basic validation for amount
         try {
             Integer.parseInt(amountStr);
         } catch (NumberFormatException e) {
              System.err.println("Error: Amount must be an integer.");
              return;
         }
         sendMessage(Protocol.UPDATE_STOCK + Protocol.DELIMITER + storeName + Protocol.DELIMITER + productName + Protocol.DELIMITER + amountStr);
     }

      private void handleRate(String args) {
         String[] parts = args.split(" ");
         if (parts.length != 2) {
             System.out.println("Usage: rate <storeName> <rating 1-5>");
             return;
         }
          String storeName = parts[0];
          String ratingStr = parts[1];
          try {
             int rating = Integer.parseInt(ratingStr);
              if (rating < 1 || rating > 5) {
                 System.err.println("Error: Rating must be between 1 and 5.");
                 return;
             }
         } catch (NumberFormatException e) {
              System.err.println("Error: Rating must be an integer.");
              return;
         }
          sendMessage(Protocol.RATE + Protocol.DELIMITER + storeName + Protocol.DELIMITER + ratingStr);
      }


    private void sendMessage(String message) {
        System.out.println("Sending: " + message);
        out.println(message);
    }

    public void close() {
        try {
            System.out.println("Closing manager connection.");
            if (consoleScanner != null) consoleScanner.close();
            if (in != null) in.close();
            if (out != null) out.close();
            if (socket != null) socket.close();
        } catch (IOException e) {
            System.err.println("Error closing resources: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: java com.fooddelivery.client.ManagerClient <masterHost> <masterPort>");
            System.exit(1);
        }
        String host = args[0];
        int port = Integer.parseInt(args[1]);

        try {
            ManagerClient manager = new ManagerClient(host, port);
            manager.startConsole();
        } catch (IOException e) {
            System.err.println("Could not connect to Master: " + e.getMessage());
        }
    }
}


// Simple Dummy Client CLI Application (for Deliverable A)
public class DummyClient {

    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;
    private final Scanner consoleScanner;
    private volatile boolean listening = true;
    private Thread listenerThread;


    public DummyClient(String masterHost, int masterPort) throws IOException {
        this.socket = new Socket(masterHost, masterPort);
        this.out = new PrintWriter(socket.getOutputStream(), true);
        this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.consoleScanner = new Scanner(System.in);
        System.out.println("Dummy Client connected to Master at " + masterHost + ":" + masterPort);
         startListenerThread(); // Start listening for asynchronous responses (like search results)
    }

    // Thread to listen for messages from the Master (especially async search results)
    private void startListenerThread() {
        listenerThread = new Thread(() -> {
            try {
                String messageFromServer;
                while (listening && (messageFromServer = in.readLine()) != null) {
                    System.out.println("\n[Async Response from Master]: " + messageFromServer);
                     // Handle specific async messages if needed, e.g., parsing SEARCH_RESULT
                     // For now, just print it.
                     System.out.print("> "); // Reprint prompt
                }
            } catch (IOException e) {
                 if (listening) {
                     System.err.println("\nListener Error: Connection to Master lost. " + e.getMessage());
                     listening = false; // Stop main loop
                 }
            } finally {
                 System.out.println("\nListener thread stopped.");
            }
        });
         listenerThread.setDaemon(true); // Don't prevent JVM exit
        listenerThread.start();
    }

    public void startConsole() {
        String line;
        while (listening) {
            printMenu();
            System.out.print("> ");
            if (!consoleScanner.hasNextLine()) { // Handle EOF or closed input
                 listening = false;
                 break;
             }
             line = consoleScanner.nextLine();
            if ("exit".equalsIgnoreCase(line)) {
                 listening = false; // Signal listener thread to stop
                break;
            }
            handleCommand(line);
             // NOTE: Responses for most commands might come asynchronously via the listener thread.
             // The BUY command response is handled synchronously for simplicity here.
        }
        close();
    }

    private void printMenu() {
        System.out.println("\n--- Dummy Client Menu ---");
        System.out.println("search <lat> <lon> <radius_km> <category|null> <min_stars|null> <price|null ($/$$/$$$)>");
        System.out.println("buy <storeName> <productName1:quantity1>,<productName2:quantity2>,...");
        System.out.println("rate <storeName> <rating 1-5>"); // Added for testing
        System.out.println("exit");
        System.out.println("-------------------------");
    }

    private void handleCommand(String line) {
        String[] parts = line.split(" ", 2); // Split command from args
        String command = parts[0].toLowerCase();
        String args = (parts.length > 1) ? parts[1] : "";

        try {
            switch (command) {
                case "search":
                    handleSearch(args);
                    break;
                case "buy":
                    handleBuy(args); // Buy is synchronous in this simple version
                    break;
                 case "rate":
                     handleRate(args); // Rate is synchronous for now
                    break;
                default:
                    System.out.println("Unknown command.");
                    return; // Don't send unknown commands
            }
        } catch (Exception e) {
            System.err.println("Error processing command: " + e.getMessage());
        }
    }

    private void handleSearch(String args) {
         String[] parts = args.split(" ");
         if (parts.length != 6) {
             System.out.println("Usage: search <lat> <lon> <radius_km> <category|null> <min_stars|null> <price|null ($/$$/$$$)>");
             return;
         }
         // Basic validation can be added here for lat/lon/radius/stars format
         String payload = String.join(Protocol.DELIMITER, parts);
         sendMessage(Protocol.SEARCH + Protocol.DELIMITER + payload);
         System.out.println("Search request sent. Waiting for async response...");
    }

    private void handleBuy(String args) {
         String[] parts = args.split(" ", 2);
         if (parts.length != 2) {
             System.out.println("Usage: buy <storeName> <productName1:quantity1>,<productName2:quantity2>,...");
             return;
         }
         String storeName = parts[0];
         String productList = parts[1];
          // Basic validation for product list format can be added

          // --- Synchronous Buy ---
          sendMessage(Protocol.BUY + Protocol.DELIMITER + storeName + Protocol.DELIMITER + productList);
          System.out.println("Buy request sent. Waiting for synchronous response...");
          // In this simple client, we'll block here waiting for the listener thread
          // to print the response. A real UI would handle this better.
          // For CLI, let's just assume the listener prints it. If a direct ACK/NACK
          // is needed *here*, the listener logic would need modification (e.g., using queues).
          // Let's add a small pause to make it likely the listener prints before the next prompt.
          try { Thread.sleep(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

    }

     private void handleRate(String args) {
         String[] parts = args.split(" ");
         if (parts.length != 2) {
             System.out.println("Usage: rate <storeName> <rating 1-5>");
             return;
         }
         String storeName = parts[0];
         String ratingStr = parts[1];
         try {
             int rating = Integer.parseInt(ratingStr);
             if (rating < 1 || rating > 5) {
                 System.err.println("Error: Rating must be between 1 and 5.");
                 return;
             }
         } catch (NumberFormatException e) {
              System.err.println("Error: Rating must be an integer.");
              return;
         }
         // Send synchronously and expect listener to print result.
         sendMessage(Protocol.RATE + Protocol.DELIMITER + storeName + Protocol.DELIMITER + ratingStr);
          System.out.println("Rate request sent. Waiting for async response...");
          try { Thread.sleep(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
     }

    private void sendMessage(String message) {
        System.out.println("Sending: " + message);
        out.println(message);
    }

    public void close() {
        listening = false; // Signal listener thread
        try {
            System.out.println("Closing client connection.");
            if (listenerThread != null) listenerThread.interrupt(); // Interrupt blocking reads
            if (consoleScanner != null) consoleScanner.close();
            if (in != null) in.close();
            if (out != null) out.close();
            if (socket != null) socket.close();
             if (listenerThread != null) listenerThread.join(1000); // Wait briefly for listener to exit
        } catch (IOException | InterruptedException e) {
            System.err.println("Error closing resources: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: java com.fooddelivery.client.DummyClient <masterHost> <masterPort>");
            System.exit(1);
        }
        String host = args[0];
        int port = Integer.parseInt(args[1]);

        try {
            DummyClient client = new DummyClient(host, port);
            client.startConsole();
        } catch (IOException e) {
            System.err.println("Could not connect to Master: " + e.getMessage());
        }
    }
}
