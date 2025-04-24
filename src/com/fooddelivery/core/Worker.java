package com.fooddelivery.core;

import com.fooddelivery.common.*;
import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Worker Node: Stores data and executes tasks assigned by the Master.
public class Worker {

    private final int port;
    private static final Map<String, Store> storeData = new HashMap<>();
    private static final Object storeDataLock = new Object();
    private static String masterHost = "localhost";
    private static int masterResultPort = -1;

    public Worker(int port) { this.port = port; }

    // Main method needs to accept Master result port info
    public static void main(String[] args) {
        if (args.length != 3) { System.err.println("Usage: java com.fooddelivery.core.Worker <worker-port> <master-host> <master-result-port>"); System.exit(1); }
        int port = 0; try { port = Integer.parseInt(args[0]); if (port <= 0 || port > 65535) throw new NumberFormatException("Worker port"); } catch (NumberFormatException e) { System.err.println("Invalid worker port: " + args[0]); System.exit(1); }
        masterHost = args[1];
        try { masterResultPort = Integer.parseInt(args[2]); if (masterResultPort <= 0 || masterResultPort > 65535) throw new NumberFormatException("Master result port"); } catch (NumberFormatException e) { System.err.println("Invalid master result port: " + args[2]); System.exit(1); }
        System.out.println("Worker starting on port " + port + ", results to " + masterHost + ":" + masterResultPort);
        Worker worker = new Worker(port); worker.start();
    }

    // Starts the worker server socket
    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Worker listening on port " + port);
            while (true) { Socket masterSocket = serverSocket.accept(); System.out.println("Worker accepted connection from Master: " + masterSocket.getRemoteSocketAddress()); new MasterHandler(masterSocket, port).start(); }
        } catch (IOException e) { System.err.println("Worker error: Could not listen on port " + port + ". " + e.getMessage()); e.printStackTrace(); }
    }

    // Inner class: Handles one Master connection
    private static class MasterHandler extends Thread {
        private final Socket masterSocket;
        private PrintWriter out;
        private BufferedReader in;
        private final int workerPort;

        public MasterHandler(Socket socket, int port) { this.masterSocket = socket; this.workerPort = port; setName("MasterHandler-" + workerPort + "-" + socket.getRemoteSocketAddress()); }

        @Override
        public void run() {
            try ( PrintWriter writer = new PrintWriter(masterSocket.getOutputStream(), true); BufferedReader reader = new BufferedReader(new InputStreamReader(masterSocket.getInputStream())) ) {
                out = writer; in = reader; String inputLine;
                while ((inputLine = in.readLine()) != null) {
                    System.out.println("Worker-"+workerPort+": Received: " + inputLine.substring(0, Math.min(100, inputLine.length())) + "...");
                    String syncResponse = processMasterRequest(inputLine);
                    if (syncResponse != null) { System.out.println("Worker-"+workerPort+": Sending sync response: " + syncResponse.substring(0, Math.min(100, syncResponse.length())) + "..."); out.println(syncResponse); }
                }
            } catch (SocketException e) { System.err.println("Worker-"+workerPort+": Master connection closed/reset: " + e.getMessage());
            } catch (IOException e) { System.err.println("Worker-"+workerPort+": IO Error: " + e.getMessage());
            } catch (Exception e) { System.err.println("Worker-"+workerPort+": Unhandled Exception in MasterHandler: " + e.getMessage()); e.printStackTrace();
            } finally { try { if (masterSocket != null && !masterSocket.isClosed()) masterSocket.close(); } catch (IOException e) {} System.out.println("Worker-"+workerPort+": Master handler finished."); }
        }

        // Process request, return sync response string, or null if response is async (MAP_RESULT)
        private String processMasterRequest(String request) {
             String[] parts = request.split("\\" + Protocol.DELIMITER, 2); if (parts.length == 0 || parts[0].isEmpty()) return Protocol.ERROR + "|Empty command";
             String command = parts[0]; String payload = (parts.length > 1) ? parts[1] : "";
             String response = Protocol.ERROR + "|Unknown command";
             try {
                 switch (command) {
                     case Protocol.M_ADD_STORE: response = handleAddStore(payload); break;
                     case Protocol.M_UPDATE_STOCK: response = handleUpdateStock(payload); break;
                     case Protocol.M_ADD_PRODUCT: response = handleAddProduct(payload); break;
                     case Protocol.M_REMOVE_PRODUCT: response = handleRemoveProduct(payload); break; // UPDATED logic
                     case Protocol.MAP_TASK: handleMapTaskAndSendResult(payload); response = null; break; // Uses Store.toJson which filters
                     case Protocol.M_BUY: response = handleBuy(payload); break; // UPDATED logic
                     case Protocol.M_RATE: response = handleRate(payload); break;
                     case Protocol.M_GET_SALES: response = handleGetSales(payload); break; // Already includes all
                     default: System.err.println("Worker-"+workerPort+": Unknown command: " + command); break;
                 }
             } catch (Exception e) { response = Protocol.ERROR + "|Worker internal error processing " + command; e.printStackTrace();}
             return response;
        }

        // --- Handlers ---
        private String handleAddStore(String jsonPayload) { Store store = Store.fromJson(jsonPayload); if (store == null) { return Protocol.GENERIC_NACK + "|Failed Store JSON"; } synchronized (storeDataLock) { storeData.put(store.getStoreName(), store); } System.out.println("Worker-"+workerPort+": Added/Updated store: " + store.getStoreName()); return Protocol.GENERIC_ACK + "|" + store.getStoreName(); }
        private String handleUpdateStock(String payload) { String[] parts = payload.split("\\|"); if (parts.length != 3) return Protocol.GENERIC_NACK + "|Format"; String storeName = parts[0], productName = parts[1]; int changeAmount; try { changeAmount = Integer.parseInt(parts[2]); } catch (NumberFormatException e) { return Protocol.GENERIC_NACK + "|Amount format"; } Store store; synchronized (storeDataLock) { store = storeData.get(storeName); } if (store == null) return Protocol.GENERIC_NACK + "|Store nf"; synchronized (store.getStoreLock()) { Product product = store.findProduct(productName); if (product == null) return Protocol.GENERIC_NACK + "|Product nf"; /* OK to update stock of inactive product */ int newAmount = product.getAvailableAmount() + changeAmount; if (newAmount < 0) return Protocol.GENERIC_NACK + "|Insufficient stock"; product.setAvailableAmount(newAmount); System.out.println("Worker-"+workerPort+": Stock updated "+ productName+"->"+newAmount); return Protocol.GENERIC_ACK + "|" + storeName + "|" + productName + "|" + newAmount; } }
        private String handleAddProduct(String payload) { String[] parts = payload.split("\\|", 2); if (parts.length != 2) return Protocol.GENERIC_NACK + "|Format"; String storeName = parts[0]; String productJson = parts[1]; Store store; synchronized (storeDataLock) { store = storeData.get(storeName); } if (store == null) return Protocol.GENERIC_NACK + "|Store nf"; Product newProductData = Product.fromJsonFragment(productJson); if (newProductData == null) return Protocol.GENERIC_NACK + "|Bad prod JSON"; synchronized (store.getStoreLock()) { Product existingProduct = store.findProduct(newProductData.getProductName()); if (existingProduct != null) { existingProduct.setProductType(newProductData.getProductType()); existingProduct.setAvailableAmount(newProductData.getAvailableAmount()); existingProduct.setPrice(newProductData.getPrice()); existingProduct.setActive(true); System.out.println("Worker-"+workerPort+": Updated/Reactivated product " + existingProduct.getProductName()); } else { store.addProduct(newProductData); System.out.println("Worker-"+workerPort+": Added new product " + newProductData.getProductName()); } } return Protocol.GENERIC_ACK + "|" + storeName + "|" + newProductData.getProductName(); }
        private String handleRemoveProduct(String payload) { String[] parts = payload.split("\\|"); if (parts.length != 2) return Protocol.GENERIC_NACK + "|Format"; String storeName = parts[0]; String productName = parts[1]; Store store; synchronized (storeDataLock) { store = storeData.get(storeName); } if (store == null) return Protocol.GENERIC_NACK + "|Store nf"; boolean markedInactive = false; synchronized (store.getStoreLock()) { Product product = store.findProduct(productName); if (product != null) { product.setActive(false); markedInactive = true; } } if(markedInactive) { System.out.println("Worker-"+workerPort+": Marked product inactive: " + productName + " in " + storeName); return Protocol.GENERIC_ACK + "|" + storeName + "|" + productName;} else { System.out.println("Worker-"+workerPort+": Product not found to mark inactive: " + productName); return Protocol.GENERIC_NACK + "|Prod nf remove"; } }
        private String handleRate(String payload) { String[] parts = payload.split("\\|"); if (parts.length != 2) return Protocol.GENERIC_NACK + "|Format"; String storeName = parts[0]; int rating; try { rating = Integer.parseInt(parts[1]); } catch (NumberFormatException e) { return Protocol.GENERIC_NACK + "|Rating format"; } if (rating < 1 || rating > 5) return Protocol.GENERIC_NACK + "|Rating range"; Store store; synchronized (storeDataLock) { store = storeData.get(storeName); } if (store == null) return Protocol.GENERIC_NACK + "|Store nf"; double newAverageStars; synchronized(store.getStoreLock()) { store.addRating(rating); newAverageStars = store.getStars(); } System.out.println("Worker-"+workerPort+": Rating updated "+storeName+"->"+newAverageStars); return Protocol.RATE_SUCCESS + "|" + storeName + "|" + String.format("%.1f", newAverageStars); }
        private String handleGetSales(String payload) { String[] parts = payload.split("\\|"); if (parts.length != 2) return Protocol.ERROR + "|Format"; String requestId = parts[0]; String storeName = parts[1]; Store store; synchronized(storeDataLock) { store = storeData.get(storeName); } if (store == null) { return Protocol.SALES_RESULT + "|" + requestId + "|Store nf: " + storeName; } StringBuilder salesData = new StringBuilder(); double totalStoreRevenue = 0.0; List<Product> productsCopy = new ArrayList<>(); synchronized(store.getStoreLock()) { List<Product> currentProducts = store.getProducts(); if (currentProducts != null) { productsCopy.addAll(currentProducts); } } if (productsCopy.isEmpty()) { salesData.append("No products for store: ").append(storeName); } else { salesData.append("Sales for store: ").append(storeName).append(Protocol.SALES_DATA_SEPARATOR); for (Product product : productsCopy) { if (product != null) { salesData.append(String.format("  %s -> Units Sold: %d, Revenue: %.2f EUR (%s)%s", product.getProductName(), product.getTotalUnitsSold(), product.getTotalRevenue(), product.isActive() ? "Active" : "Inactive", Protocol.SALES_DATA_SEPARATOR)); totalStoreRevenue += product.getTotalRevenue(); } } salesData.append(String.format("Total Store Revenue (All Products): %.2f EUR", totalStoreRevenue)); } return Protocol.SALES_RESULT + "|" + requestId + "|" + salesData.toString(); }

        // --- CORRECTED: handleBuy ---
        private String handleBuy(String payload) {
            String[] parts = payload.split("\\" + Protocol.DELIMITER, 3); if (parts.length != 3) return Protocol.ERROR + "|BUY Format";
            String buyId = parts[0], storeName = parts[1], productRequests = parts[2];
            Store store; synchronized (storeDataLock) { store = storeData.get(storeName); } // SYNC 1
            if (store == null) return Protocol.BUY_FAIL + "|" + buyId + "|Store nf";

            Map<String, Integer> itemsToBuy = new HashMap<>(); // Parse items
            String[] productPairs = productRequests.split(Protocol.PRODUCT_LIST_DELIMITER);
            for (String pair : productPairs) {
                String[] itemQty = pair.split(Protocol.PRODUCT_QTY_DELIMITER);
                if (itemQty.length == 2) {
                    try {
                        int q = Integer.parseInt(itemQty[1]);
                        if(q<=0) return Protocol.BUY_FAIL+"|"+buyId+"|Bad qty";
                        itemsToBuy.put(itemQty[0], q);
                    } catch (NumberFormatException e) { return Protocol.BUY_FAIL+"|"+buyId+"|Bad qty format"; }
                } else return Protocol.BUY_FAIL+"|"+buyId+"|Bad pair format";
            }
            // *** This check was missing in the erroneous version ***
            if (itemsToBuy.isEmpty()) return Protocol.BUY_FAIL + "|" + buyId + "|No items";

            // *** Critical section starts here ***
            synchronized (store.getStoreLock()) { // SYNC 2
                double totalCost = 0;
                Map<Product, Integer> productsToUpdate = new HashMap<>();
                // Check availability AND active status
                for (Map.Entry<String, Integer> entry : itemsToBuy.entrySet()) {
                    String pName = entry.getKey(); int q = entry.getValue();
                    Product p = store.findProduct(pName); // Find first
                    if (p == null || !p.isActive()) { System.out.println("Worker-"+workerPort+": Buy failed for "+pName+" - not active/found."); return Protocol.BUY_FAIL + "|" + buyId + "|Product not available/found: " + pName; }
                    if (p.getAvailableAmount() < q) { System.out.println("Worker-"+workerPort+": Buy failed for "+pName+" - stock low."); return Protocol.BUY_FAIL + "|" + buyId + "|Stock low: " + pName; }
                    productsToUpdate.put(p, q); totalCost += p.getPrice() * q;
                }
                // Commit changes
                for (Map.Entry<Product, Integer> entry : productsToUpdate.entrySet()) {
                    entry.getKey().setAvailableAmount(entry.getKey().getAvailableAmount() - entry.getValue());
                    entry.getKey().recordSale(entry.getValue());
                }
                System.out.println("Worker-"+workerPort+": Sale OK "+buyId);
                return Protocol.BUY_SUCCESS + "|" + buyId + "|" + String.format("%.2f", totalCost);
            } // *** End critical section ***
        } // End handleBuy


        // handleMapTask processes AND sends result on new connection
        private void handleMapTaskAndSendResult(String payload) { String[] parts = payload.split("\\|"); if (parts.length < 7) { System.err.println("Worker-"+workerPort+": Invalid MAP_TASK format"); return; } String mapId = parts[0]; double userLat=0, userLon=0, radius=0; int minStars=0; String categoryFilter = parts[4].equals("null") ? null : parts[4]; String priceFilter = parts[6].equals("null") ? null : parts[6]; try { userLat = Double.parseDouble(parts[1]); userLon = Double.parseDouble(parts[2]); radius = Double.parseDouble(parts[3]); minStars = parts[5].equals("null") ? 0 : Integer.parseInt(parts[5]); } catch (NumberFormatException e) { System.err.println("Worker-"+workerPort+": Invalid numeric filter in MAP_TASK"); return; } List<String> matchingStoresJson = new ArrayList<>(); List<Store> storesToCheck = new ArrayList<>(); synchronized(storeDataLock) { storesToCheck.addAll(storeData.values()); } for (Store store : storesToCheck) { try { double dist = GeoUtils.haversineDistance(userLat, userLon, store.getLatitude(), store.getLongitude()); if (dist > radius) continue; String sCat = store.getFoodCategory(); double sStars; String sPrice; synchronized(store.getStoreLock()){ sStars = store.getStars(); sPrice = store.getPriceCategory(); } if (categoryFilter != null && !sCat.equalsIgnoreCase(categoryFilter)) continue; if (minStars > 0 && sStars < minStars) continue; if (priceFilter != null && !sPrice.equals(priceFilter)) continue; String storeJson; synchronized(store.getStoreLock()){ storeJson = store.toJson(); } matchingStoresJson.add(storeJson); } catch (Exception filterEx) { System.err.println("Worker-"+workerPort+": Error filtering "+ store.getStoreName() + ": " + filterEx.getMessage()); } } String resultPayload = String.join(Protocol.MAP_RESULT_STORE_DELIMITER, matchingStoresJson); String mapResultMessage = Protocol.MAP_RESULT + "|" + mapId + "|" + resultPayload; System.out.println("Worker-"+workerPort+": Found " + matchingStoresJson.size() + " stores for mapId " + mapId + " (active products only)"); sendResultToMaster(mapResultMessage); }
        // Helper method to send result on a temporary connection
        private void sendResultToMaster(String message) { if (masterResultPort == -1) { System.err.println("Worker-"+workerPort+": Master Result Port invalid."); return; } try ( Socket resultSocket = new Socket(masterHost, masterResultPort); PrintWriter resultWriter = new PrintWriter(resultSocket.getOutputStream(), true) ) { System.out.println("Worker-"+workerPort+": Sending MAP_RESULT to " + masterHost + ":" + masterResultPort); resultWriter.println(message); } catch (IOException e) { System.err.println("Worker-"+workerPort+": Failed to send MAP_RESULT to Master: " + e.getMessage()); } }

    } // End MasterHandler
} // End Worker (This closing brace was missing)