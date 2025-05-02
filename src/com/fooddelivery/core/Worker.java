package com.fooddelivery.core;

import com.fooddelivery.common.*;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap; // Use ConcurrentHashMap for storeData
import java.util.stream.Collectors;

// Worker Node: Stores data and executes tasks assigned by the Master.
public class Worker {

    private final int port;
    // Use ConcurrentHashMap for thread-safe store data access
    // Key: StoreName, Value: Store object
    private static final Map<String, Store> storeData = new ConcurrentHashMap<>();
    // No explicit storeDataLock needed with ConcurrentHashMap for basic get/put/remove
    // However, operations requiring consistency across multiple Store fields or Product list
    // still need synchronization on the *individual Store object's lock*.

    private static String masterHost = "localhost"; // Master host for sending async results
    private static int masterResultPort = -1;    // Master result listener port
    private static boolean verboseMode = false;     // Worker verbose flag

    public Worker(int port) {
        this.port = port;
    }

    // UPDATED: Main method to handle -v flag and initialize common classes
    public static void main(String[] args) {
        List<String> argList = new ArrayList<>(Arrays.asList(args));
        verboseMode = argList.remove("-v"); // Check and remove -v flag

        // Set verbose mode in common classes if worker is verbose
        Product.setVerboseMode(verboseMode);
        Store.setVerboseMode(verboseMode);

        if (argList.size() != 3) {
            System.err.println(
                    "Usage: java com.fooddelivery.core.Worker [-v] <worker-listen-port> <master-host> <master-result-port>");
            System.exit(1);
        }

        int port = 0;
        try {
            port = Integer.parseInt(argList.get(0));
             if (port <= 0 || port > 65535) throw new NumberFormatException("Worker port out of range");
        } catch (NumberFormatException e) {
            System.err.println("Invalid worker port: " + argList.get(0) + " (" + e.getMessage() + ")");
            System.exit(1);
        }

        masterHost = argList.get(1); // Host where Master's result listener is running

        try {
            masterResultPort = Integer.parseInt(argList.get(2));
            if (masterResultPort <= 0 || masterResultPort > 65535) throw new NumberFormatException("Master result port out of range");
             if (masterResultPort == port) throw new NumberFormatException("Worker port and Master result port cannot be the same");
        } catch (NumberFormatException e) {
            System.err.println("Invalid master result port: " + argList.get(2) + " (" + e.getMessage() + ")");
            System.exit(1);
        }

        // Use colorful printer for startup message
        ColorfulStatusPrinter.printSuccess("Worker starting on port " + port + ". Master results target: " + masterHost + ":"
                + masterResultPort + (verboseMode ? " [VERBOSE MODE]" : ""));
        Worker worker = new Worker(port);
        worker.start(); // Start listening for Master connections
    }

    // Starts the worker server socket to listen for Master connections
    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            ColorfulStatusPrinter.printInfo("Worker-" + port + " listening for Master on port " + port);
            while (true) { // Loop indefinitely accepting Master connections
                try {
                    Socket masterSocket = serverSocket.accept();
                    masterSocket.setTcpNoDelay(true); // Optimize for small command/response latency
                    ColorfulStatusPrinter.printLog("Worker-" + port + ": Accepted connection from Master: " + masterSocket.getRemoteSocketAddress());
                    // Handle each Master connection in a new thread
                    new MasterHandler(masterSocket, port).start();
                } catch (IOException e) {
                    // Avoid spamming logs if the server socket itself is closed
                    if (serverSocket.isClosed()) {
                         ColorfulStatusPrinter.printInfo("Worker-" + port + ": Server socket closed, stopping accepting connections.");
                         break; // Exit loop
                    }
                    ColorfulStatusPrinter.printError("Worker-" + port + ": Error accepting Master connection: " + e.getMessage());
                    // Consider adding a small delay here if errors persist
                     try { Thread.sleep(100); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break;}
                }
            }
        } catch (IOException e) {
            ColorfulStatusPrinter.printError("Worker-" + port + ": CRITICAL - Could not listen on port " + port + ". " + e.getMessage());
            e.printStackTrace();
             // Exit if unable to listen
             System.exit(1);
        } finally {
            ColorfulStatusPrinter.printInfo("Worker-" + port + " server loop finished.");
        }
    }

    // Inner class: Handles communication with ONE connected Master instance
    private static class MasterHandler extends Thread {
        private final Socket masterSocket;
        private PrintWriter out = null;
        private BufferedReader in = null;
        private final int workerPort; // Worker's own port for logging identity
        private final String handlerDesc; // Description for logging

        public MasterHandler(Socket socket, int port) {
            this.masterSocket = socket;
            this.workerPort = port;
            this.handlerDesc = "Worker-" + workerPort + "-MH-" + socket.getRemoteSocketAddress();
            setName(handlerDesc); // Set thread name
        }

        @Override
        public void run() {
            ColorfulStatusPrinter.printLog(handlerDesc + ": MasterHandler thread started.");
             try ( // Use try-with-resources for reader and writer
                  BufferedReader reader = new BufferedReader(new InputStreamReader(masterSocket.getInputStream()));
                  PrintWriter writer = new PrintWriter(masterSocket.getOutputStream(), true))
             {
                 in = reader; // Assign for use in methods
                 out = writer; // Assign for use in methods

                 String inputLine;
                 // Loop reading commands from the Master connection
                 while (!Thread.currentThread().isInterrupted() && (inputLine = in.readLine()) != null) {
                     String command = inputLine.split("\\|")[0];
                     ColorfulStatusPrinter.printLog(handlerDesc + ": Received Cmd: " + command + "...");
                     ColorfulStatusPrinter.printDebug(verboseMode, handlerDesc + ": Rcvd Full: " + inputLine);

                     String syncResponse = processMasterRequest(inputLine); // Process command

                     // If a synchronous response is needed, send it back
                     if (syncResponse != null) {
                         String logResp = syncResponse.length() > 100 ? syncResponse.substring(0, 100) + "..." : syncResponse;
                         ColorfulStatusPrinter.printLog(handlerDesc + ": Sending Resp: " + logResp.split("\\|")[0] + "...");
                         ColorfulStatusPrinter.printDebug(verboseMode, handlerDesc + ": Send Full: " + syncResponse);
                         out.println(syncResponse); // Send response
                          if(out.checkError()){
                               ColorfulStatusPrinter.printError(handlerDesc + ": PrintWriter error detected after sending response. Master connection likely lost.");
                               break; // Stop reading from this connection
                          }
                     }
                     // If syncResponse is null, it means the task was handled asynchronously (like MAP_TASK)
                 }
                  // Loop exited, likely because Master closed the connection (readLine returned null)
                  ColorfulStatusPrinter.printInfo(handlerDesc + ": Master disconnected or command loop interrupted.");

             } catch (SocketException e) {
                 // Handle connection resets gracefully
                  if ("Connection reset".equalsIgnoreCase(e.getMessage()) || "Socket closed".equalsIgnoreCase(e.getMessage()) || "Broken pipe".equalsIgnoreCase(e.getMessage())) {
                     ColorfulStatusPrinter.printWarn(handlerDesc + ": Connection reset or closed by Master."); // Use printWarn
                 } else {
                      ColorfulStatusPrinter.printError(handlerDesc + ": SocketException: " + e.getMessage());
                      if (verboseMode) e.printStackTrace();
                 }
             } catch (IOException e) {
                 ColorfulStatusPrinter.printError(handlerDesc + ": IO Error communicating with Master: " + e.getMessage());
                 if (verboseMode) e.printStackTrace();
             } catch (Exception e) { // Catch any unexpected errors during processing
                 ColorfulStatusPrinter.printError(handlerDesc + ": Unexpected error: " + e.getMessage());
                 e.printStackTrace();
                 // Try to send an error back to Master if possible
                  if (out != null && !out.checkError()) {
                      out.println(Protocol.ERROR + Protocol.DELIMITER + "Worker internal error: " + e.getMessage());
                  }
             } finally {
                 // Cleanup: Close socket (streams are closed by try-with-resources)
                 try {
                     if (masterSocket != null && !masterSocket.isClosed()) {
                         masterSocket.close();
                     }
                 } catch (IOException e) {
                     ColorfulStatusPrinter.printError(handlerDesc + ": Error closing master socket: " + e.getMessage());
                 }
                 ColorfulStatusPrinter.printLog(handlerDesc + ": MasterHandler thread finished.");
             }
        }


        // Process request from Master, return sync response string, or null if response is async (MAP_TASK)
        private String processMasterRequest(String request) {
            String[] parts = request.split("\\" + Protocol.DELIMITER, 2); // Split into command and payload
            if (parts.length == 0 || parts[0].isEmpty()) {
                ColorfulStatusPrinter.printError(handlerDesc + ": Received empty command from Master.");
                return Protocol.ERROR + Protocol.DELIMITER + "Empty command received";
            }

            String command = parts[0];
            String payload = (parts.length > 1) ? parts[1] : ""; // Payload is empty if no delimiter found
            String response = null; // Default to null (no sync response)

            try {
                switch (command) {
                    // --- Store/Product Management (Synchronous) ---
                    case Protocol.M_ADD_STORE:
                        response = handleAddStore(payload);
                        break;
                    case Protocol.M_ADD_PRODUCT:
                        response = handleAddProduct(payload);
                        break;
                    case Protocol.M_REMOVE_PRODUCT:
                         response = handleRemoveProduct(payload);
                         break;
                    case Protocol.M_UPDATE_STOCK:
                        response = handleUpdateStock(payload);
                        break;

                    // --- Client Actions (Synchronous within Worker) ---
                     case Protocol.M_BUY:
                        response = handleBuy(payload);
                        break;
                    case Protocol.M_RATE:
                        response = handleRate(payload);
                        break;

                    // --- Search Task (Asynchronous Result) ---
                    case Protocol.MAP_TASK:
                        handleMapTaskAndSendResult(payload); // Sends result via separate connection
                        response = null; // No synchronous response needed back to Master on this channel
                        break;

                    // --- Sales Reporting Tasks (Synchronous Result) ---
                    case Protocol.M_GET_SALES: // Single store detailed sales
                         response = handleGetSales(payload); // Returns SALES_RESULT | reqId | data
                         break;
                    case Protocol.M_GET_CATEGORY_SALES_TASK: // NEW category summary task
                         response = handleGetCategorySales(payload); // Returns CATEGORY_SALES_RESULT | reqId | aggregated_data
                         break;
                     case Protocol.M_GET_PRODTYPE_SALES_TASK: // NEW product type summary task
                         response = handleGetProductTypeSales(payload); // Returns PRODTYPE_SALES_RESULT | reqId | aggregated_data
                         break;

                    default:
                        ColorfulStatusPrinter.printError(handlerDesc + ": Unknown command received from Master: " + command);
                        response = Protocol.ERROR + Protocol.DELIMITER + "Unknown command: " + command;
                        break;
                }
            } catch (IllegalArgumentException e) { // Catch validation errors
                 ColorfulStatusPrinter.printError(handlerDesc + ": Invalid arguments processing command " + command + ": " + e.getMessage());
                 response = Protocol.GENERIC_NACK + Protocol.DELIMITER + "Invalid arguments: " + e.getMessage(); // Use NACK for bad args
            }
            catch (Exception e) { // Catch unexpected internal errors
                response = Protocol.ERROR + Protocol.DELIMITER + "Worker internal error processing " + command;
                 ColorfulStatusPrinter.printError(handlerDesc + ": Unhandled Exception processing " + command + ": " + e.getMessage());
                e.printStackTrace(); // Log stack trace
            }
            return response; // Return sync response (or null if async)
        }


        // --- Handlers ---

        // Uses ConcurrentHashMap.computeIfAbsent for atomic check-and-add
        private String handleAddStore(String jsonPayload) throws IllegalArgumentException {
            Store store = Store.fromJson(jsonPayload);
            if (store == null) {
                throw new IllegalArgumentException("Failed to parse Store JSON.");
            }
            final String storeName = store.getStoreName(); // Final variable for lambda

             // Atomically add if absent
            Store existingStore = storeData.computeIfAbsent(storeName, key -> {
                 // This lambda only executes if the key is absent
                 synchronized (store.getStoreLock()) { // Ensure price category is calculated before adding
                     store.updatePriceCategory();
                 }
                 ColorfulStatusPrinter.printSuccess(handlerDesc + ": Added new store: " + storeName + " (Price Cat: " + store.getPriceCategory() + ")");
                 return store; // Return the new store to be put in the map
             });


            if (existingStore == store) { // Check if the store returned by computeIfAbsent is the one we created
                return Protocol.GENERIC_ACK + Protocol.DELIMITER + storeName; // Successfully added
             } else {
                 ColorfulStatusPrinter.printWarn(handlerDesc + ": Denied adding existing store: " + storeName); // Use printWarn
                 return Protocol.GENERIC_NACK + Protocol.DELIMITER + "Store already exists: " + storeName; // Store existed
             }
        }


        // Updates stock, requires locking the specific Store object
        private String handleUpdateStock(String payload) throws IllegalArgumentException {
            // Payload: storeName|productName|amountChange
            String[] parts = payload.split("\\|");
            if (parts.length != 3) throw new IllegalArgumentException("Invalid format for UPDATE_STOCK");

            String storeName = parts[0];
            String productName = parts[1];
            int changeAmount;
            try {
                changeAmount = Integer.parseInt(parts[2]);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid amount format for UPDATE_STOCK.");
            }

            Store store = storeData.get(storeName); // Get from ConcurrentHashMap (thread-safe read)
            if (store == null) {
                 ColorfulStatusPrinter.printError(handlerDesc + ": Store not found for UPDATE_STOCK: " + storeName);
                 return Protocol.GENERIC_NACK + Protocol.DELIMITER + "Store not found: " + storeName;
            }

            // *** Need to lock the specific Store object for consistency ***
            synchronized (store.getStoreLock()) {
                Product product = store.findProduct(productName); // Assumes findProduct is safe within lock
                if (product == null || !product.isActive()) { // Check if active as well
                    ColorfulStatusPrinter.printError(handlerDesc + ": Product '" + productName + "' not found or inactive in store '" + storeName + "' for UPDATE_STOCK.");
                    return Protocol.GENERIC_NACK + Protocol.DELIMITER + "Product not found or inactive: " + productName;
                }

                int currentAmount = product.getAvailableAmount();
                int newAmount = currentAmount + changeAmount;

                if (newAmount < 0) {
                    ColorfulStatusPrinter.printError(handlerDesc + ": Insufficient stock for UPDATE_STOCK on '" + productName + "' in '" + storeName + "' (needs " + (-changeAmount) + ", has " + currentAmount + ")");
                    return Protocol.GENERIC_NACK + Protocol.DELIMITER + "Insufficient stock for " + productName;
                }

                product.setAvailableAmount(newAmount); // Update amount within lock
                ColorfulStatusPrinter.printSuccess(handlerDesc + ": Stock updated for '" + productName + "' in '" + storeName + "' -> " + newAmount);
                return Protocol.GENERIC_ACK + Protocol.DELIMITER + storeName + Protocol.DELIMITER + productName + Protocol.DELIMITER + newAmount;
            } // Release store lock
        }

        // Adds or updates a product within a store, requires Store lock
         private String handleAddProduct(String payload) throws IllegalArgumentException {
            // Payload: storeName|{productJson}
            String[] parts = payload.split("\\|", 2);
            if (parts.length != 2) throw new IllegalArgumentException("Invalid format for ADD_PRODUCT");

            String storeName = parts[0];
            String productJson = parts[1];

            Store store = storeData.get(storeName);
            if (store == null) {
                 ColorfulStatusPrinter.printError(handlerDesc + ": Store not found for ADD_PRODUCT: " + storeName);
                 return Protocol.GENERIC_NACK + Protocol.DELIMITER + "Store not found: " + storeName;
            }

            Product newProductData = Product.fromJsonFragment(productJson); // Assuming this is thread-safe or doesn't rely on shared state
            if (newProductData == null) {
                 throw new IllegalArgumentException("Failed to parse product JSON for ADD_PRODUCT.");
            }

            // *** Lock the specific Store object ***
             synchronized (store.getStoreLock()) {
                 Product existingProduct = store.findProduct(newProductData.getProductName());

                 if (existingProduct != null) {
                     // Update existing product's details (reactivate if necessary)
                     existingProduct.setProductType(newProductData.getProductType());
                     existingProduct.setAvailableAmount(newProductData.getAvailableAmount());
                     existingProduct.setPrice(newProductData.getPrice());
                     if (!existingProduct.isActive()) {
                         existingProduct.setActive(true); // Reactivate if it was marked inactive
                         ColorfulStatusPrinter.printInfo(handlerDesc + ": Reactivated product '" + existingProduct.getProductName() + "' in store '" + storeName + "'");
                     }
                     store.updatePriceCategory(); // Recalculate price category after update
                     ColorfulStatusPrinter.printSuccess(handlerDesc + ": Updated product '" + existingProduct.getProductName() + "' in store '" + storeName + "'");
                 } else {
                     // Add the new product
                     store.addProduct(newProductData); // addProduct calls updatePriceCategory internally
                     ColorfulStatusPrinter.printSuccess(handlerDesc + ": Added new product '" + newProductData.getProductName() + "' to store '" + storeName + "'");
                 }
             } // Release store lock

            return Protocol.GENERIC_ACK + Protocol.DELIMITER + storeName + Protocol.DELIMITER + newProductData.getProductName();
         }


         // Marks a product as inactive, requires Store lock
         private String handleRemoveProduct(String payload) throws IllegalArgumentException {
             // Payload: storeName|productName
             String[] parts = payload.split("\\|");
             if (parts.length != 2) throw new IllegalArgumentException("Invalid format for REMOVE_PRODUCT");

             String storeName = parts[0];
             String productName = parts[1];

             Store store = storeData.get(storeName);
             if (store == null) {
                 ColorfulStatusPrinter.printError(handlerDesc + ": Store not found for REMOVE_PRODUCT: " + storeName);
                 return Protocol.GENERIC_NACK + Protocol.DELIMITER + "Store not found: " + storeName;
             }

             boolean markedInactive;
             // *** Lock the specific Store object ***
             synchronized (store.getStoreLock()) {
                 markedInactive = store.removeProduct(productName); // removeProduct marks inactive and updates price category
             } // Release store lock

             if (markedInactive) {
                 ColorfulStatusPrinter.printSuccess(handlerDesc + ": Marked product inactive: '" + productName + "' in store '" + storeName + "'");
                 return Protocol.GENERIC_ACK + Protocol.DELIMITER + storeName + Protocol.DELIMITER + productName;
             } else {
                 ColorfulStatusPrinter.printWarn(handlerDesc + ": Product not found to mark inactive: '" + productName + "' in store '" + storeName + "'"); // Use printWarn
                 return Protocol.GENERIC_NACK + Protocol.DELIMITER + "Product not found to remove: " + productName;
             }
         }

         // Handles buy request, needs Store lock for consistency check and update
         private String handleBuy(String payload) throws IllegalArgumentException {
             // Payload: buyId|storeName|prod1:q1,prod2:q2,...
             String[] parts = payload.split("\\|", 3);
             if (parts.length != 3) throw new IllegalArgumentException("Invalid BUY format from Master");

             String buyId = parts[0];
             String storeName = parts[1];
             String productRequests = parts[2];

             Store store = storeData.get(storeName);
             if (store == null) {
                  ColorfulStatusPrinter.printError(handlerDesc + ": Store '" + storeName + "' not found for BUY " + buyId);
                  return Protocol.BUY_FAIL + Protocol.DELIMITER + buyId + Protocol.DELIMITER + "Store not found: " + storeName;
             }

             // --- Parse requested items ---
             Map<String, Integer> itemsToBuy = new HashMap<>();
             try {
                 String[] productPairs = productRequests.split(Protocol.PRODUCT_LIST_DELIMITER);
                 for (String pair : productPairs) {
                     String[] itemQty = pair.trim().split(Protocol.PRODUCT_QTY_DELIMITER);
                     if (itemQty.length == 2) {
                         String pName = itemQty[0].trim();
                         int quantity = Integer.parseInt(itemQty[1].trim());
                         if (pName.isEmpty() || quantity <= 0) {
                             throw new IllegalArgumentException("Invalid product name or quantity in pair: " + pair);
                         }
                         // Aggregate quantity if product appears multiple times (e.g., buy X chips:1,chips:2)
                         itemsToBuy.put(pName, itemsToBuy.getOrDefault(pName, 0) + quantity);
                     } else {
                         throw new IllegalArgumentException("Invalid product format: " + pair);
                     }
                 }
             } catch (NumberFormatException e) {
                  throw new IllegalArgumentException("Invalid quantity format in product list.");
             } catch (IllegalArgumentException e) { // Catch specific parsing errors
                 throw new IllegalArgumentException("Error parsing product list: " + e.getMessage());
             }

             if (itemsToBuy.isEmpty()) {
                  return Protocol.BUY_FAIL + Protocol.DELIMITER + buyId + Protocol.DELIMITER + "No valid items specified in buy request";
             }

             // --- Check stock and calculate cost (within Store lock) ---
             double totalCost = 0;
             // Store products to update and quantities AFTER checking all items
             Map<Product, Integer> productsToUpdate = new HashMap<>();

             // *** Lock the Store object for consistent check/update ***
             synchronized (store.getStoreLock()) {
                 // 1. Check availability and calculate cost
                 for (Map.Entry<String, Integer> entry : itemsToBuy.entrySet()) {
                     String requestedProductName = entry.getKey();
                     int requestedQuantity = entry.getValue();

                     Product product = store.findProduct(requestedProductName);

                     if (product == null || !product.isActive()) {
                         ColorfulStatusPrinter.printError(handlerDesc + ": Buy " + buyId + " failed for '" + requestedProductName + "' - not active/found in '" + storeName + "'.");
                         return Protocol.BUY_FAIL + Protocol.DELIMITER + buyId + Protocol.DELIMITER + "Product not available or not found: " + requestedProductName;
                     }

                     if (product.getAvailableAmount() < requestedQuantity) {
                         ColorfulStatusPrinter.printError(handlerDesc + ": Buy " + buyId + " failed for '" + requestedProductName + "' - stock low in '" + storeName + "' (requested " + requestedQuantity + ", have " + product.getAvailableAmount() + ").");
                         return Protocol.BUY_FAIL + Protocol.DELIMITER + buyId + Protocol.DELIMITER + "Insufficient stock for " + requestedProductName;
                     }

                     // If checks pass, add to update list and accumulate cost
                     productsToUpdate.put(product, requestedQuantity);
                     totalCost += product.getPrice() * requestedQuantity;
                 }

                 // 2. If all checks passed, update stock and record sales
                 if (productsToUpdate.size() == itemsToBuy.size()) { // Ensure all requested items were found and valid
                     for (Map.Entry<Product, Integer> updateEntry : productsToUpdate.entrySet()) {
                         Product product = updateEntry.getKey();
                         int quantitySold = updateEntry.getValue();
                         product.setAvailableAmount(product.getAvailableAmount() - quantitySold);
                         product.recordSale(quantitySold); // Record sale within lock
                     }
                     ColorfulStatusPrinter.printSuccess(handlerDesc + ": Sale " + buyId + " completed successfully in '" + storeName + "' for " + String.format("%.2f", totalCost) + " EUR.");
                     return Protocol.BUY_SUCCESS + Protocol.DELIMITER + buyId + Protocol.DELIMITER + String.format("%.2f", totalCost);
                 } else {
                     // Should not happen if logic above is correct, but as a safeguard
                     ColorfulStatusPrinter.printError(handlerDesc + ": Buy " + buyId + " consistency error - not all products validated but reached update stage.");
                     return Protocol.BUY_FAIL + Protocol.DELIMITER + buyId + Protocol.DELIMITER + "Internal consistency error during purchase.";
                 }

             } // Release store lock
         }

         // Handles rating, needs Store lock to update average
         private String handleRate(String payload) throws IllegalArgumentException {
             // Payload: storeName|ratingValue
             String[] parts = payload.split("\\|");
             if (parts.length != 2) throw new IllegalArgumentException("Invalid format for RATE");

             String storeName = parts[0];
             int rating;
             try {
                 rating = Integer.parseInt(parts[1]);
                 if (rating < 1 || rating > 5) throw new IllegalArgumentException("Rating out of range (1-5)");
             } catch (NumberFormatException e) {
                 throw new IllegalArgumentException("Invalid rating format.");
             }

             Store store = storeData.get(storeName);
             if (store == null) {
                 ColorfulStatusPrinter.printError(handlerDesc + ": Store not found for RATE: " + storeName);
                 return Protocol.GENERIC_NACK + Protocol.DELIMITER + "Store not found: " + storeName;
             }

             double newAverageStars;
             // *** Lock the store to update rating atomically ***
             synchronized (store.getStoreLock()) {
                 store.addRating(rating);
                 newAverageStars = store.getStars(); // Get the updated average
             } // Release lock

             ColorfulStatusPrinter.printInfo(handlerDesc + ": Rating updated for '" + storeName + "' to " + String.format("%.1f", newAverageStars) + " stars.");
             return Protocol.RATE_SUCCESS + Protocol.DELIMITER + storeName + Protocol.DELIMITER + String.format("%.1f", newAverageStars);
         }


        // --- Sales Report Handlers ---

        // Handles single store sales request (Existing)
         private String handleGetSales(String payload) throws IllegalArgumentException {
             // Payload: requestId|storeName
             String[] parts = payload.split("\\|", 2);
             if (parts.length != 2) throw new IllegalArgumentException("Invalid format for M_GET_SALES");

             String requestId = parts[0];
             String storeName = parts[1];

             Store store = storeData.get(storeName);
             if (store == null) {
                 ColorfulStatusPrinter.printWarn(handlerDesc + ": Store '" + storeName + "' not found for GET_SALES request " + requestId); // Use printWarn
                 // Return SALES_RESULT indicating not found, using the specified format
                 return Protocol.SALES_RESULT + Protocol.DELIMITER + requestId + Protocol.DELIMITER + "Store not found: " + storeName;
             }

             StringBuilder salesData = new StringBuilder();
             double totalStoreRevenue = 0.0;
             List<Product> productsCopy = new ArrayList<>(); // Copy products to avoid holding lock too long

             // *** Lock store briefly to get product list safely ***
             synchronized (store.getStoreLock()) {
                 List<Product> currentProducts = store.getProducts();
                 if (currentProducts != null) {
                     // Shallow copy is usually sufficient if Product fields used below are immutable or primitive after creation
                     productsCopy.addAll(currentProducts);
                 }
             } // Release lock

             if (productsCopy.isEmpty()) {
                 salesData.append("No products found for store: ").append(storeName);
             } else {
                 salesData.append("Sales for store: ").append(storeName).append(Protocol.SALES_DATA_SEPARATOR);
                 for (Product product : productsCopy) {
                     // Accessing product fields - assuming getTotalUnitsSold/Revenue are reasonably thread-safe
                     // If recordSale modifies these without synchronization, this might not be perfectly accurate
                     // without locking the Product itself, but often acceptable for reporting.
                     if (product != null) {
                         salesData.append(String.format("  %s -> Units Sold: %d, Revenue: %.2f EUR (%s)%s",
                                 product.getProductName(), product.getTotalUnitsSold(), product.getTotalRevenue(),
                                 product.isActive() ? "Active" : "Inactive", Protocol.SALES_DATA_SEPARATOR));
                         totalStoreRevenue += product.getTotalRevenue();
                     }
                 }
                 salesData.append(String.format("Total Store Revenue (All Products): %.2f EUR", totalStoreRevenue));
             }

             // Format: SALES_RESULT | requestId | formattedDataString
             return Protocol.SALES_RESULT + Protocol.DELIMITER + requestId + Protocol.DELIMITER + salesData.toString();
         }


        // NEW: Handle request for sales aggregated by category
         private String handleGetCategorySales(String payload) throws IllegalArgumentException {
             // Payload: requestId|foodCategory
             String[] parts = payload.split("\\|", 2);
             if (parts.length != 2) throw new IllegalArgumentException("Invalid format for M_GET_CATEGORY_SALES_TASK");
             String requestId = parts[0];
             String targetCategory = parts[1];

             StringBuilder resultData = new StringBuilder();
             int storesProcessed = 0;

             // Iterate over the entries in the ConcurrentHashMap (thread-safe iteration)
             for (Map.Entry<String, Store> entry : storeData.entrySet()) {
                 Store store = entry.getValue();
                 long storeTotalUnits = 0;
                 double storeTotalRevenue = 0.0;
                 boolean categoryMatch = false;

                 // Check category first (read is thread-safe for final/primitive fields)
                 if (store.getFoodCategory().equalsIgnoreCase(targetCategory)) {
                     categoryMatch = true;
                     List<Product> productsCopy = new ArrayList<>();
                     // Lock store briefly to get product list
                     synchronized (store.getStoreLock()) {
                         if (store.getProducts() != null) {
                             productsCopy.addAll(store.getProducts());
                         }
                     } // Release lock

                     // Aggregate sales for this store
                     for (Product p : productsCopy) {
                          if (p != null) { // Assuming getTotalUnitsSold/Revenue are safe to read
                             storeTotalUnits += p.getTotalUnitsSold();
                             storeTotalRevenue += p.getTotalRevenue();
                          }
                     }
                 }

                 // If category matched and there were sales (or even if 0 sales), add to result
                 if (categoryMatch) {
                      // Only add store if it actually sold something (or exists in category)
                      // Or decide to always include stores of the category even with 0 sales?
                      // Current logic includes stores with 0 sales if category matches.
                     if (resultData.length() > 0) {
                         resultData.append(Protocol.SALES_REPORT_DELIMITER); // Add separator before next entry
                     }
                     resultData.append(store.getStoreName())
                               .append(Protocol.SALES_REPORT_FIELD_DELIMITER)
                               .append(storeTotalUnits)
                               .append(Protocol.SALES_REPORT_FIELD_DELIMITER)
                               .append(String.format("%.2f", storeTotalRevenue));
                     storesProcessed++;
                 }
             } // End loop through stores

             ColorfulStatusPrinter.printInfo(handlerDesc + ": Processed category sales task " + requestId + " for '" + targetCategory + "'. Found " + storesProcessed + " matching stores.");

             // Format: CATEGORY_SALES_RESULT | requestId | store1:units:rev;;store2:units:rev...
             return Protocol.CATEGORY_SALES_RESULT + Protocol.DELIMITER + requestId + Protocol.DELIMITER + resultData.toString();
         }


         // NEW: Handle request for sales aggregated by product type
         private String handleGetProductTypeSales(String payload) throws IllegalArgumentException {
             // Payload: requestId|productType
             String[] parts = payload.split("\\|", 2);
             if (parts.length != 2) throw new IllegalArgumentException("Invalid format for M_GET_PRODTYPE_SALES_TASK");
             String requestId = parts[0];
             String targetProductType = parts[1];

             StringBuilder resultData = new StringBuilder();
             int storesProcessed = 0;

             for (Map.Entry<String, Store> entry : storeData.entrySet()) {
                 Store store = entry.getValue();
                 long storeTypeUnits = 0;
                 double storeTypeRevenue = 0.0;
                 boolean typeMatchFound = false; // Did this store sell *any* product of this type?

                 List<Product> productsCopy = new ArrayList<>();
                 // Lock store briefly
                 synchronized (store.getStoreLock()) {
                     if (store.getProducts() != null) {
                         productsCopy.addAll(store.getProducts());
                     }
                 } // Release lock

                 for (Product p : productsCopy) {
                     if (p != null && p.getProductType().equalsIgnoreCase(targetProductType)) {
                         // Accumulate sales ONLY for products matching the type
                         storeTypeUnits += p.getTotalUnitsSold();
                         storeTypeRevenue += p.getTotalRevenue();
                         if (p.getTotalUnitsSold() > 0) { // Only set flag if units were actually sold
                            typeMatchFound = true;
                         }
                     }
                 }

                 // Only include the store in the result if it sold products of the target type
                 if (typeMatchFound) {
                     if (resultData.length() > 0) {
                         resultData.append(Protocol.SALES_REPORT_DELIMITER);
                     }
                     resultData.append(store.getStoreName())
                               .append(Protocol.SALES_REPORT_FIELD_DELIMITER)
                               .append(storeTypeUnits) // Units of this type sold by this store
                               .append(Protocol.SALES_REPORT_FIELD_DELIMITER)
                               .append(String.format("%.2f", storeTypeRevenue)); // Revenue from this type by this store
                     storesProcessed++;
                 }
             } // End loop through stores

             ColorfulStatusPrinter.printInfo(handlerDesc + ": Processed product type sales task " + requestId + " for '" + targetProductType + "'. Found " + storesProcessed + " matching stores.");

             // Format: PRODTYPE_SALES_RESULT | requestId | store1:units:rev;;store2:units:rev...
             return Protocol.PRODTYPE_SALES_RESULT + Protocol.DELIMITER + requestId + Protocol.DELIMITER + resultData.toString();
         }

         // --- Map Task (Search) Handler ---

        // Performs the Map phase of search and sends result asynchronously
        private void handleMapTaskAndSendResult(String payload) {
             // Payload: mapId|lat|lon|radius|category|stars|price
             String[] parts = payload.split("\\|"); // Split by literal '|'
            // Expecting 7 parts: MAP_TASK is removed, so payload starts with mapId (index 0) + 6 filters
            if (parts.length < 7) { // Check for at least mapId + 6 filters
                 ColorfulStatusPrinter.printError(handlerDesc + ": Invalid MAP_TASK format received: " + payload);
                 // Cannot easily send error back async, just log it.
                 return;
             }

             String mapId = parts[0];
             double userLat = 0, userLon = 0, radius = 0;
             String categoryFilter = null; // Default to null if "null" string or parsing fails
             int minStars = 0; // Default to 0 (no filter)
             String priceFilter = null; // Default to null

             try {
                 userLat = Double.parseDouble(parts[1]);
                 userLon = Double.parseDouble(parts[2]);
                 radius = Double.parseDouble(parts[3]);
                 // Category: Keep as null if "null" string
                 if (!parts[4].equalsIgnoreCase("null")) {
                      categoryFilter = parts[4];
                 }
                 // Stars: Keep as 0 if "null" string or parsing fails
                 if (!parts[5].equalsIgnoreCase("null")) {
                     minStars = Integer.parseInt(parts[5]);
                     if (minStars < 0 || minStars > 5) minStars = 0; // Reset if invalid range
                 }
                 // Price: Keep as null if "null" string
                 if (!parts[6].equalsIgnoreCase("null")) {
                      // Validate price filter format ($ , $$, $$$)
                      if (parts[6].matches("^\\${1,3}$")) {
                           priceFilter = parts[6];
                      } else {
                            ColorfulStatusPrinter.printWarn(handlerDesc + ": Invalid price filter '" + parts[6] + "' in MAP_TASK " + mapId + ". Ignoring filter."); // Use printWarn
                      }
                 }
             } catch (NumberFormatException | IndexOutOfBoundsException e) {
                 ColorfulStatusPrinter.printError(handlerDesc + ": Error parsing numeric filter in MAP_TASK " + mapId + ": " + e.getMessage());
                 // Potentially return empty result? For now, just log and continue with defaults.
                 // Or: sendResultToMaster(Protocol.MAP_RESULT + "|" + mapId + "|"); return;
                 return; // Stop processing this task if filters are bad
             }


             List<String> matchingStoresJson = new ArrayList<>();

             // Iterate safely over ConcurrentHashMap's values
             for (Store store : storeData.values()) {
                 try {
                     // --- Filtering Logic ---
                     // 1. Distance Check
                     double dist = GeoUtils.haversineDistance(userLat, userLon, store.getLatitude(), store.getLongitude());
                     if (dist > radius) continue; // Skip if too far

                     // Get potentially dynamic store properties (need lock?)
                     // For simple filtering, reading category/stars might be ok without lock if set at creation.
                     // Price category *needs* lock if it can change.
                     String sCat = store.getFoodCategory(); // Assuming immutable after creation
                     double sStars = store.getStars();      // Assuming only changes via rating (atomic enough?)
                     String sPriceCategory;
                     synchronized(store.getStoreLock()){ // Lock needed for price category
                         sPriceCategory = store.getPriceCategory();
                     }

                     // 2. Category Filter
                     if (categoryFilter != null && !sCat.equalsIgnoreCase(categoryFilter)) continue;

                     // 3. Stars Filter
                     if (minStars > 0 && sStars < minStars) continue;

                     // 4. Price Filter
                     if (priceFilter != null && !sPriceCategory.equals(priceFilter)) continue;

                     // --- Store Matches All Filters ---
                     String storeJson;
                     synchronized (store.getStoreLock()) { // Lock to get consistent JSON state
                         storeJson = store.toJson(); // Generate JSON representation
                     }
                     matchingStoresJson.add(storeJson);

                 } catch (Exception filterEx) {
                     // Catch errors during filtering for a specific store
                     ColorfulStatusPrinter.printError(handlerDesc + ": Error filtering store '"
                             + store.getStoreName() + "' for mapId " + mapId + ": " + filterEx.getMessage());
                     // Continue with the next store
                 }
             } // End loop through stores


             // Combine results and send back asynchronously
             String resultPayload = String.join(Protocol.MAP_RESULT_STORE_DELIMITER, matchingStoresJson);
             String mapResultMessage = Protocol.MAP_RESULT + Protocol.DELIMITER + mapId + Protocol.DELIMITER + resultPayload;

             ColorfulStatusPrinter.printInfo(handlerDesc + ": Map task " + mapId + " complete. Found " + matchingStoresJson.size() + " matching stores.");
             sendResultToMaster(mapResultMessage); // Send async result
         }

        // Sends result of MAP_TASK back to Master's result port
        private void sendResultToMaster(String message) {
            if (masterResultPort <= 0) { // Check if port is valid
                ColorfulStatusPrinter.printError(handlerDesc + ": Invalid Master Result Port (" + masterResultPort + "). Cannot send MAP_RESULT.");
                return;
            }

            // Use try-with-resources for socket and writer
            try (Socket resultSocket = new Socket()) {
                // Connect to Master's dedicated result listener port
                 resultSocket.connect(new InetSocketAddress(masterHost, masterResultPort), 3000); // 3 sec connect timeout
                 resultSocket.setSoTimeout(5000); // 5 sec write timeout
                 resultSocket.setTcpNoDelay(true);

                 try (PrintWriter resultWriter = new PrintWriter(resultSocket.getOutputStream(), true)) {
                     String logMsg = message.length() > 150 ? message.substring(0, 150) + "..." : message;
                     ColorfulStatusPrinter.printDebug(verboseMode, handlerDesc + ": Sending MAP_RESULT to " + masterHost + ":" + masterResultPort + " -> " + logMsg.split("\\|")[0] +"...");
                     resultWriter.println(message); // Send the result message
                     if(resultWriter.checkError()){
                          ColorfulStatusPrinter.printError(handlerDesc + ": PrintWriter error sending MAP_RESULT to Master.");
                     } else {
                          ColorfulStatusPrinter.printDebug(verboseMode, handlerDesc + ": MAP_RESULT sent successfully for mapId " + message.split("\\|")[1]);
                     }
                 }

            } catch (SocketTimeoutException e) {
                 ColorfulStatusPrinter.printError(handlerDesc + ": Timeout connecting or sending MAP_RESULT to Master at " + masterHost + ":" + masterResultPort + ": " + e.getMessage());
            } catch (IOException e) {
                ColorfulStatusPrinter.printError(handlerDesc + ": Failed to send MAP_RESULT to Master at " + masterHost + ":" + masterResultPort + ": " + e.getMessage());
                // Common causes: Master listener not running, network issue, firewall
            } catch (Exception e) { // Catch unexpected errors
                 ColorfulStatusPrinter.printError(handlerDesc + ": Unexpected error sending MAP_RESULT: " + e.getMessage());
                 e.printStackTrace();
            }
        } // End sendResultToMaster


    } // End MasterHandler Inner Class
} // End Worker Class
