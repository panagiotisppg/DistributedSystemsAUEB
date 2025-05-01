package com.fooddelivery.core;

import com.fooddelivery.common.*; // Import the new utility
import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Arrays;

// Worker Node: Stores data and executes tasks assigned by the Master.
public class Worker {

    private final int port;
    private static final Map<String, Store> storeData = new HashMap<>();
    private static final Object storeDataLock = new Object();
    private static String masterHost = "localhost";
    private static int masterResultPort = -1;
    private static boolean verboseMode = false; // Worker verbose flag

    public Worker(int port) {
        this.port = port;
    }

    // UPDATED: Main method to handle -v flag
    public static void main(String[] args) {
        List<String> argList = new ArrayList<>(Arrays.asList(args));
        verboseMode = argList.remove("-v"); // Check and remove -v flag

        // Set verbose mode in common classes if worker is verbose
        Product.setVerboseMode(verboseMode);
        Store.setVerboseMode(verboseMode);

        if (argList.size() != 3) {
            System.err.println(
                    "Usage: java com.fooddelivery.core.Worker [-v] <worker-port> <master-host> <master-result-port>");
            System.exit(1);
        }
        int port = 0;
        try {
            port = Integer.parseInt(argList.get(0));
            if (port <= 0 || port > 65535)
                throw new NumberFormatException("Worker port");
        } catch (NumberFormatException e) {
            System.err.println("Invalid worker port: " + argList.get(0));
            System.exit(1);
        }
        masterHost = argList.get(1);
        try {
            masterResultPort = Integer.parseInt(argList.get(2));
            if (masterResultPort <= 0 || masterResultPort > 65535)
                throw new NumberFormatException("Master result port");
        } catch (NumberFormatException e) {
            System.err.println("Invalid master result port: " + argList.get(2));
            System.exit(1);
        }

        // Use colorful printer for startup message
        ColorfulStatusPrinter.printInfo("Worker starting on port " + port + ", results to " + masterHost + ":"
                + masterResultPort + (verboseMode ? " [VERBOSE MODE]" : ""));
        Worker worker = new Worker(port);
        worker.start();
    }

    // Starts the worker server socket
    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            ColorfulStatusPrinter.printInfo("Worker listening on port " + port); // Use info color
            while (true) {
                Socket masterSocket = serverSocket.accept();
                ColorfulStatusPrinter
                        .printLog("Worker accepted connection from Master: " + masterSocket.getRemoteSocketAddress()); // Use
                                                                                                                       // log
                                                                                                                       // color
                new MasterHandler(masterSocket, port).start();
            }
        } catch (IOException e) {
            ColorfulStatusPrinter.printError("Worker error: Could not listen on port " + port + ". " + e.getMessage()); // Use
                                                                                                                        // error
                                                                                                                        // color
            e.printStackTrace();
        }
    }

    // Inner class: Handles one Master connection
    private static class MasterHandler extends Thread {
        private final Socket masterSocket;
        private PrintWriter out;
        private BufferedReader in;
        private final int workerPort;

        public MasterHandler(Socket socket, int port) {
            this.masterSocket = socket;
            this.workerPort = port;
            setName("MasterHandler-" + workerPort + "-" + socket.getRemoteSocketAddress());
        }

        @Override
        public void run() {
            try (PrintWriter writer = new PrintWriter(masterSocket.getOutputStream(), true);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(masterSocket.getInputStream()))) {
                out = writer;
                in = reader;
                String inputLine;
                while ((inputLine = in.readLine()) != null) {
                    // Basic log always shown
                    ColorfulStatusPrinter
                            .printLog("Worker-" + workerPort + ": Received Cmd: " + inputLine.split("\\|")[0] + "...");
                    // Full log only if verbose
                    ColorfulStatusPrinter.printDebug(verboseMode, "Worker Rcvd Full: " + inputLine);

                    String syncResponse = processMasterRequest(inputLine);
                    if (syncResponse != null) {
                        // Basic log always shown
                        ColorfulStatusPrinter.printLog(
                                "Worker-" + workerPort + ": Sending Resp: " + syncResponse.split("\\|")[0] + "...");
                        // Full log only if verbose
                        ColorfulStatusPrinter.printDebug(verboseMode, "Worker Send Full: " + syncResponse);
                        out.println(syncResponse);
                    }
                }
            } catch (SocketException e) {
                ColorfulStatusPrinter
                        .printError("Worker-" + workerPort + ": Master connection closed/reset: " + e.getMessage());
            } catch (IOException e) {
                ColorfulStatusPrinter.printError("Worker-" + workerPort + ": IO Error: " + e.getMessage());
            } catch (Exception e) {
                ColorfulStatusPrinter.printError(
                        "Worker-" + workerPort + ": Unhandled Exception in MasterHandler: " + e.getMessage());
                e.printStackTrace();
            } finally {
                try {
                    if (masterSocket != null && !masterSocket.isClosed())
                        masterSocket.close();
                } catch (IOException e) {
                }
                ColorfulStatusPrinter.printInfo("Worker-" + workerPort + ": Master handler finished for " + getName());
            }
        }

        // Process request, return sync response string, or null if response is async
        // (MAP_RESULT)
        private String processMasterRequest(String request) {
            String[] parts = request.split("\\" + Protocol.DELIMITER, 2);
            if (parts.length == 0 || parts[0].isEmpty())
                return Protocol.ERROR + "|Empty command";
            String command = parts[0];
            String payload = (parts.length > 1) ? parts[1] : "";
            String response = Protocol.ERROR + "|Unknown command";
            try {
                switch (command) {
                    case Protocol.M_ADD_STORE:
                        response = handleAddStore(payload);
                        break;
                    case Protocol.M_UPDATE_STOCK:
                        response = handleUpdateStock(payload);
                        break;
                    case Protocol.M_ADD_PRODUCT:
                        response = handleAddProduct(payload);
                        break;
                    case Protocol.M_REMOVE_PRODUCT:
                        response = handleRemoveProduct(payload);
                        break;
                    case Protocol.MAP_TASK:
                        handleMapTaskAndSendResult(payload);
                        response = null;
                        break;
                    case Protocol.M_BUY:
                        response = handleBuy(payload);
                        break;
                    case Protocol.M_RATE:
                        response = handleRate(payload);
                        break;
                    case Protocol.M_GET_SALES:
                        response = handleGetSales(payload);
                        break;
                    default:
                        ColorfulStatusPrinter
                                .printError("Worker-" + workerPort + ": Unknown command received: " + command);
                        break;
                }
            } catch (Exception e) {
                response = Protocol.ERROR + "|Worker internal error processing " + command;
                e.printStackTrace();
            }
            return response;
        }

        // --- Handlers with colorful logs ---
        private String handleAddStore(String jsonPayload) {
            Store store = Store.fromJson(jsonPayload);
            if (store == null) {
                ColorfulStatusPrinter.printError("Worker-" + workerPort + ": Denied adding existing store: " + store.getStoreName());
                return Protocol.GENERIC_NACK + "|Store already exists: " + store.getStoreName();
            }
            synchronized (storeDataLock) {
                if (storeData.containsKey(store.getStoreName())) {
                    ColorfulStatusPrinter.printStatus("X",
                            "Worker-" + workerPort + ": Denied adding existing store: " + store.getStoreName(),
                            ColorfulStatusPrinter.RED);
                    return Protocol.GENERIC_NACK + "|Store already exists: " + store.getStoreName();
                } else {
                    synchronized (store.getStoreLock()) {
                        store.updatePriceCategory();
                    }
                    storeData.put(store.getStoreName(), store);
                    ColorfulStatusPrinter
                            .printStatus("✓",
                                    "Worker-" + workerPort + ": Added new store: " + store.getStoreName()
                                            + " (Price Cat: " + store.getPriceCategory() + ")",
                                    ColorfulStatusPrinter.GREEN);
                    return Protocol.GENERIC_ACK + "|" + store.getStoreName();
                }
            }
        }

        private String handleUpdateStock(String payload) {
            String[] parts = payload.split("\\|");
            if (parts.length != 3){
                ColorfulStatusPrinter.printError("Worker-" + workerPort + ": Invalid format for UPDATE_STOCK payload.");
                return Protocol.GENERIC_NACK + "|Invalid format";
            }
            String storeName = parts[0], productName = parts[1];
            int changeAmount;
            try {
                changeAmount = Integer.parseInt(parts[2]);
            } catch (NumberFormatException e) {
                ColorfulStatusPrinter.printError("Worker-" + workerPort + ": Invalid amount format for UPDATE_STOCK.");
                return Protocol.GENERIC_NACK + "|Amount format";
            }
            Store store;
            synchronized (storeDataLock) {
                store = storeData.get(storeName);
            }
            if (store == null){
                ColorfulStatusPrinter.printError("Worker-" + workerPort + ": Store not found for UPDATE_STOCK: " + storeName);
                return Protocol.GENERIC_NACK + "|Store nf";
            }
            synchronized (store.getStoreLock()) {
                Product product = store.findProduct(productName);
                if (product == null){
                    ColorfulStatusPrinter.printError("Worker-" + workerPort + ": Product not found for UPDATE_STOCK: " + productName);
                    return Protocol.GENERIC_NACK + "|Product nf";
                }
                int newAmount = product.getAvailableAmount() + changeAmount;
                if (newAmount < 0){
                    ColorfulStatusPrinter.printError("Worker-" + workerPort + ": Insufficient stock for UPDATE_STOCK on " + productName);
                    return Protocol.GENERIC_NACK + "|Insufficient stock";
                }
                product.setAvailableAmount(newAmount);
                ColorfulStatusPrinter.printStatus("✓",
                        "Worker-" + workerPort + ": Stock updated " + productName + "->" + newAmount,
                        ColorfulStatusPrinter.GREEN);
                return Protocol.GENERIC_ACK + "|" + storeName + "|" + productName + "|" + newAmount;
            }
        }

        private String handleAddProduct(String payload) {
            String[] parts = payload.split("\\|", 2);
            if (parts.length != 2){
                ColorfulStatusPrinter.printError("Worker-" + workerPort + ": Invalid format for ADD_PRODUCT payload.");
                return Protocol.GENERIC_NACK + "|Invalid format";
            }
            String storeName = parts[0];
            String productJson = parts[1];
            Store store;
            synchronized (storeDataLock) {
                store = storeData.get(storeName);
            }
            if (store == null){
                ColorfulStatusPrinter.printError("Worker-" + workerPort + ": Store not found for ADD_PRODUCT: " + storeName);
                return Protocol.GENERIC_NACK + "|Store nf";
            }
            Product newProductData = Product.fromJsonFragment(productJson);
            if (newProductData == null){
                ColorfulStatusPrinter.printError("Worker-" + workerPort + ": Failed to parse product JSON for ADD_PRODUCT.");
                return Protocol.GENERIC_NACK + "|Bad prod JSON";
            }
            synchronized (store.getStoreLock()) {
                Product existingProduct = store.findProduct(newProductData.getProductName());
                if (existingProduct != null) {
                    existingProduct.setProductType(newProductData.getProductType());
                    existingProduct.setAvailableAmount(newProductData.getAvailableAmount());
                    existingProduct.setPrice(newProductData.getPrice());
                    existingProduct.setActive(true);
                    store.updatePriceCategory();
                    ColorfulStatusPrinter.printStatus("✓", "Worker-" + workerPort + ": Updated/Reactivated product "
                            + existingProduct.getProductName(), ColorfulStatusPrinter.GREEN);
                } else {
                    store.addProduct(newProductData);
                    ColorfulStatusPrinter.printStatus("+",
                            "Worker-" + workerPort + ": Added new product " + newProductData.getProductName(),
                            ColorfulStatusPrinter.GREEN);
                }
            }
            return Protocol.GENERIC_ACK + "|" + storeName + "|" + newProductData.getProductName();
        }

        private String handleRemoveProduct(String payload) {
            String[] parts = payload.split("\\|");
            if (parts.length != 2){
                ColorfulStatusPrinter.printError("Worker-" + workerPort + ": Invalid format for REMOVE_PRODUCT payload.");
                return Protocol.GENERIC_NACK + "|Invalid format";
            }
            String storeName = parts[0];
            String productName = parts[1];
            Store store;
            synchronized (storeDataLock) {
                store = storeData.get(storeName);
            }
            if (store == null){
                ColorfulStatusPrinter.printError("Worker-" + workerPort + ": Store not found for REMOVE_PRODUCT: " + storeName);
                return Protocol.GENERIC_NACK + "|Store nf";
            }
            boolean markedInactive = false;
            synchronized (store.getStoreLock()) {
                markedInactive = store.removeProduct(productName);
            }
            if (markedInactive) {
                ColorfulStatusPrinter.printStatus("-",
                        "Worker-" + workerPort + ": Marked product inactive: " + productName + " in " + storeName,
                        ColorfulStatusPrinter.ORANGE);
                return Protocol.GENERIC_ACK + "|" + storeName + "|" + productName;
            } else {
                ColorfulStatusPrinter.printStatus("?",
                        "Worker-" + workerPort + ": Product not found to mark inactive: " + productName,
                        ColorfulStatusPrinter.ORANGE);
                return Protocol.GENERIC_NACK + "|Prod nf remove";
            }
        }

        private String handleBuy(String payload) {
            String[] parts = payload.split("\\|", 3);
            if (parts.length != 3)
                return Protocol.ERROR + "|BUY Format";
            String buyId = parts[0], storeName = parts[1], productRequests = parts[2];
            Store store;
            synchronized (storeDataLock) {
                store = storeData.get(storeName);
            }
            if (store == null)
                return Protocol.BUY_FAIL + "|" + buyId + "|Store nf";
            Map<String, Integer> itemsToBuy = new HashMap<>();
            String[] productPairs = productRequests.split(",");
            for (String pair : productPairs) {
                String[] itemQty = pair.split(":");
                if (itemQty.length == 2) {
                    try {
                        int q = Integer.parseInt(itemQty[1]);
                        if (q <= 0)
                            return Protocol.BUY_FAIL + "|" + buyId + "|Bad qty";
                        itemsToBuy.put(itemQty[0], q);
                    } catch (NumberFormatException e) {
                        return Protocol.BUY_FAIL + "|" + buyId + "|Bad qty format";
                    }
                } else
                    return Protocol.BUY_FAIL + "|" + buyId + "|Bad pair format";
            }
            if (itemsToBuy.isEmpty())
                return Protocol.BUY_FAIL + "|" + buyId + "|No valid items specified";
            synchronized (store.getStoreLock()) {
                double totalCost = 0;
                Map<Product, Integer> productsToUpdate = new HashMap<>();
                for (Map.Entry<String, Integer> entry : itemsToBuy.entrySet()) {
                    String pName = entry.getKey();
                    int q = entry.getValue();
                    Product p = store.findProduct(pName);
                    if (p == null || !p.isActive()) {
                        ColorfulStatusPrinter.printError(
                                "Worker-" + workerPort + ": Buy failed for " + pName + " - not active/found.");
                        return Protocol.BUY_FAIL + "|" + buyId + "|Product not available/found: " + pName;
                    }
                    if (p.getAvailableAmount() < q) {
                        ColorfulStatusPrinter
                                .printError("Worker-" + workerPort + ": Buy failed for " + pName + " - stock low.");
                        return Protocol.BUY_FAIL + "|" + buyId + "|Stock low: " + pName;
                    }
                    productsToUpdate.put(p, q);
                    totalCost += p.getPrice() * q;
                }
                for (Map.Entry<Product, Integer> entry : productsToUpdate.entrySet()) {
                    entry.getKey().setAvailableAmount(entry.getKey().getAvailableAmount() - entry.getValue());
                    entry.getKey().recordSale(entry.getValue());
                }
                ColorfulStatusPrinter.printStatus("✓", "Worker-" + workerPort + ": Sale OK " + buyId,
                        ColorfulStatusPrinter.GREEN);
                return Protocol.BUY_SUCCESS + "|" + buyId + "|" + String.format("%.2f", totalCost);
            }
        }

        private String handleRate(String payload) {
            String[] parts = payload.split("\\|");
            if (parts.length != 2){
                ColorfulStatusPrinter.printError("Worker-" + workerPort + ": Invalid format for RATE payload.");
                return Protocol.GENERIC_NACK + "|Format";
            }
            String storeName = parts[0];
            int rating;
            try {
                rating = Integer.parseInt(parts[1]);
            } catch (NumberFormatException e) {
                ColorfulStatusPrinter.printError("Worker-" + workerPort + ": Invalid rating format for RATE.");
                return Protocol.GENERIC_NACK + "|Rating format";
            }
            if (rating < 1 || rating > 5){
                ColorfulStatusPrinter.printError("Worker-" + workerPort + ": Rating out of range for RATE.");
                return Protocol.GENERIC_NACK + "|Rating range";
            }
            Store store;
            synchronized (storeDataLock) {
                store = storeData.get(storeName);
            }
            if (store == null){
                ColorfulStatusPrinter.printError("Worker-" + workerPort + ": Store not found for RATE: " + storeName);
                return Protocol.GENERIC_NACK + "|Store nf";
            }
            double newAverageStars;
            synchronized (store.getStoreLock()) {
                store.addRating(rating);
                newAverageStars = store.getStars();
            }
            ColorfulStatusPrinter
                    .printInfo("Worker-" + workerPort + ": Rating updated " + storeName + "->" + newAverageStars);
            return Protocol.RATE_SUCCESS + "|" + storeName + "|" + String.format("%.1f", newAverageStars);
        }

        private String handleGetSales(String payload) {
            String[] parts = payload.split("\\|");
            if (parts.length != 2)
                return Protocol.ERROR + "|Format";
            String requestId = parts[0];
            String storeName = parts[1];
            Store store;
            synchronized (storeDataLock) {
                store = storeData.get(storeName);
            }
            if (store == null) {
                return Protocol.SALES_RESULT + "|" + requestId + "|Store nf: " + storeName;
            }
            StringBuilder salesData = new StringBuilder();
            double totalStoreRevenue = 0.0;
            List<Product> productsCopy = new ArrayList<>();
            synchronized (store.getStoreLock()) {
                List<Product> currentProducts = store.getProducts();
                if (currentProducts != null) {
                    productsCopy.addAll(currentProducts);
                }
            }
            if (productsCopy.isEmpty()) {
                salesData.append("No products for store: ").append(storeName);
            } else {
                salesData.append("Sales for store: ").append(storeName).append(Protocol.SALES_DATA_SEPARATOR);
                for (Product product : productsCopy) {
                    if (product != null) {
                        salesData.append(String.format("  %s -> Units Sold: %d, Revenue: %.2f EUR (%s)%s",
                                product.getProductName(), product.getTotalUnitsSold(), product.getTotalRevenue(),
                                product.isActive() ? "Active" : "Inactive", Protocol.SALES_DATA_SEPARATOR));
                        totalStoreRevenue += product.getTotalRevenue();
                    }
                }
                salesData.append(String.format("Total Store Revenue (All Products): %.2f EUR", totalStoreRevenue));
            }
            return Protocol.SALES_RESULT + "|" + requestId + "|" + salesData.toString();
        }

        private void handleMapTaskAndSendResult(String payload) {
            String[] parts = payload.split("\\|");
            if (parts.length < 7) {
                ColorfulStatusPrinter.printError("Worker-" + workerPort + ": Invalid MAP_TASK format");
                return;
            }
            String mapId = parts[0];
            double userLat = 0, userLon = 0, radius = 0;
            int minStars = 0;
            String categoryFilter = parts[4].equals("null") ? null : parts[4];
            String priceFilter = parts[6].equals("null") ? null : parts[6];
            try {
                userLat = Double.parseDouble(parts[1]);
                userLon = Double.parseDouble(parts[2]);
                radius = Double.parseDouble(parts[3]);
                minStars = parts[5].equals("null") ? 0 : Integer.parseInt(parts[5]);
            } catch (NumberFormatException e) {
                ColorfulStatusPrinter.printError("Worker-" + workerPort + ": Invalid numeric filter in MAP_TASK");
                return;
            }
            List<String> matchingStoresJson = new ArrayList<>();
            List<Store> storesToCheck = new ArrayList<>();
            synchronized (storeDataLock) {
                storesToCheck.addAll(storeData.values());
            }
            for (Store store : storesToCheck) {
                try {
                    double dist = GeoUtils.haversineDistance(userLat, userLon, store.getLatitude(),
                            store.getLongitude());
                    if (dist > radius)
                        continue;
                    String sCat = store.getFoodCategory();
                    double sStars;
                    String sPriceCategory;
                    synchronized (store.getStoreLock()) {
                        sStars = store.getStars();
                        sPriceCategory = store.getPriceCategory();
                    }
                    if (categoryFilter != null && !sCat.equalsIgnoreCase(categoryFilter))
                        continue;
                    if (minStars > 0 && sStars < minStars)
                        continue;
                    if (priceFilter != null && !sPriceCategory.equals(priceFilter))
                        continue;
                    String storeJson;
                    synchronized (store.getStoreLock()) {
                        storeJson = store.toJson();
                    }
                    matchingStoresJson.add(storeJson);
                } catch (Exception filterEx) {
                    ColorfulStatusPrinter.printError("Worker-" + workerPort + ": Error filtering "
                            + store.getStoreName() + ": " + filterEx.getMessage());
                }
            }
            String resultPayload = String.join(Protocol.MAP_RESULT_STORE_DELIMITER, matchingStoresJson);
            String mapResultMessage = Protocol.MAP_RESULT + "|" + mapId + "|" + resultPayload;
            ColorfulStatusPrinter.printInfo(
                    "Worker-" + workerPort + ": Found " + matchingStoresJson.size() + " stores for mapId " + mapId);
            sendResultToMaster(mapResultMessage);
        }

        private void sendResultToMaster(String message) {
            if (masterResultPort == -1) {
                ColorfulStatusPrinter.printError("Worker-" + workerPort + ": Master Result Port invalid.");
                return;
            }
            try (Socket resultSocket = new Socket(masterHost, masterResultPort);
                    PrintWriter resultWriter = new PrintWriter(resultSocket.getOutputStream(), true)) {
                ColorfulStatusPrinter.printDebug(verboseMode,
                        "Worker-" + workerPort + ": Sending MAP_RESULT to " + masterHost + ":" + masterResultPort);
                resultWriter.println(message);
            } catch (IOException e) {
                ColorfulStatusPrinter.printError(
                        "Worker-" + workerPort + ": Failed to send MAP_RESULT to Master: " + e.getMessage());
            }
        }

    } // End MasterHandler
} // End Worker
