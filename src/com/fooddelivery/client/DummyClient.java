// File: src/com/fooddelivery/client/DummyClient.java
package com.fooddelivery.client;

import com.fooddelivery.common.Protocol;
import java.io.*;
import java.net.*;
import java.util.Scanner;

// Simple Dummy Client CLI Application (for Deliverable A user interactions).
public class DummyClient {

    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;
    private final Scanner consoleScanner;
    private volatile boolean running = true;
    private Thread listenerThread;

    // Shared object for synchronous responses
    private final Object syncResponseLock = new Object();
    private String lastSyncResponse = null;

    // Constructor
    public DummyClient(String masterHost, int masterPort) throws IOException {
        try {
            this.socket = new Socket();
            this.socket.connect(new InetSocketAddress(masterHost, masterPort), 5000);
            this.out = new PrintWriter(socket.getOutputStream(), true);
            this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            this.consoleScanner = new Scanner(System.in);
            System.out.println("Dummy Client connected to Master at " + masterHost + ":" + masterPort);
            startListenerThread();
        } catch (IOException e) {
            System.err.println("Dummy Client Error: Could not connect: " + e.getMessage());
            throw e;
        }
    }

    // Listener thread reads ALL messages and dispatches appropriately
    private void startListenerThread() {
        listenerThread = new Thread(() -> {
            try {
                String messageFromServer;
                while (running && !Thread.currentThread().isInterrupted()
                        && (messageFromServer = in.readLine()) != null) {
                    if (messageFromServer.startsWith(Protocol.SEARCH_RESULT + Protocol.DELIMITER)) {
                        System.out.println("\n[Search Result]: " + messageFromServer);
                        System.out.print("Client > ");
                    } else { // Assume sync reply
                        synchronized (syncResponseLock) {
                            System.out.println("\n[Listener Received Sync]: " + messageFromServer);
                            lastSyncResponse = messageFromServer;
                            syncResponseLock.notifyAll();
                        }
                    }
                }
            } catch (SocketException e) {
                if (running)
                    System.err.println("\n[Listener Error]: Connection lost/reset.");
            } catch (IOException e) {
                if (running)
                    System.err.println("\n[Listener Error]: IO Error reading from Master.");
            } catch (Exception e) {
                System.err.println("\n[Listener Error]: Unexpected error: " + e.getMessage());
                e.printStackTrace();
            } finally {
                System.out.println("\n[Listener thread stopped]");
                running = false;
                synchronized (syncResponseLock) {
                    syncResponseLock.notifyAll();
                }
            }
        }, "ClientListenerThread");
        listenerThread.setDaemon(true);
        listenerThread.start();
    }

    // Starts interactive console loop
    public void startConsole() {
        String line;
        while (running) {
            printMenu();
            System.out.print("Client > ");
            if (!consoleScanner.hasNextLine()) {
                System.out.println("Console input closed.");
                running = false;
                break;
            }
            line = consoleScanner.nextLine().trim();
            if (line.isEmpty())
                continue;
            if ("exit".equalsIgnoreCase(line)) {
                running = false;
                break;
            }
            handleCommand(line); // Handle command (sends & potentially waits)
        }
        closeClient();
    }

    // Prints menu
    private void printMenu() {
        System.out.println(
                "\n--- Dummy Client Menu ---\n  search <lat> <lon> <radius> <cat|null> <stars|null> <price|null>\n  buy <Store Name> <prod1:q1,prod2:q2>\n  rate <Store Name> <1-5>\n  exit\n-------------------------");
    }

    // Handles user command input, sends, and waits for SYNC replies
    private void handleCommand(String line) {
        String[] parts = line.split(" ", 2); // Split command from the rest
        String command = parts[0].toLowerCase();
        String args = (parts.length > 1) ? parts[1] : "";
        String messageToSend = null;
        boolean expectSyncResponse = false;

        try {
            switch (command) {
                case "search":
                    messageToSend = prepareSearchMessage(args);
                    expectSyncResponse = false;
                    break;
                case "buy":
                    messageToSend = prepareBuyMessage(args);
                    expectSyncResponse = true;
                    break;
                case "rate":
                    messageToSend = prepareRateMessage(args);
                    expectSyncResponse = true;
                    break;
                default:
                    System.out.println("Unknown command.");
                    return;
            }
        } catch (Exception e) {
            System.err.println("Error processing args: " + e.getMessage());
            return;
        }

        if (messageToSend != null) {
            String response = null;
            synchronized (syncResponseLock) {
                lastSyncResponse = null;
            } // Clear previous response
            if (!sendMessage(messageToSend))
                return; // Exit if send fails

            if (expectSyncResponse) {
                System.out.println("Waiting for synchronous response...");
                long timeoutMillis = 15000;
                long startTime = System.currentTimeMillis();
                synchronized (syncResponseLock) { // Acquire lock
                    while (lastSyncResponse == null && (System.currentTimeMillis() - startTime) < timeoutMillis) {
                        try {
                            long waitTime = timeoutMillis - (System.currentTimeMillis() - startTime);
                            if (waitTime <= 0)
                                break;
                            syncResponseLock.wait(waitTime);
                        } // Wait on the lock
                        catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            System.err.println("[Error]: Interrupted.");
                            return;
                        }
                    }
                    response = lastSyncResponse; // Get response potentially set by listener
                } // Release lock
                if (response != null) {
                    System.out.println("[Sync Response]: " + response);
                } else {
                    System.err.println("[Error]: Timeout waiting for synchronous response.");
                }
            } else {
                System.out.println("Search request sent. Results will arrive asynchronously via listener.");
            }
        }
    }

    // --- Message Preparation Methods ---
    private String prepareSearchMessage(String args) {
        String[] parts = args.split(" ");
        if (parts.length != 6) {
            System.out.println("Usage: search <lat> <lon> <radius> <cat|null> <stars|null> <price|null>");
            return null;
        }
        try {
            Double.parseDouble(parts[0]);
            Double.parseDouble(parts[1]);
            Double.parseDouble(parts[2]);
            if (!parts[4].equals("null"))
                Integer.parseInt(parts[4]);
        } catch (NumberFormatException e) {
            System.err.println("Invalid number format.");
            return null;
        }
        String payload = String.join("|", parts);
        return Protocol.SEARCH + "|" + payload;
    }

    // --- UPDATED: prepareBuyMessage ---
    private String prepareBuyMessage(String args) {
        // Find the last argument that looks like a product list (contains ":")
        String[] parts = args.split(" ");
        if (parts.length < 2) { // Need at least store name and one product item
            System.out.println("Usage: buy <Store Name> <prod1:q1,prod2:q2>");
            return null;
        }

        int productListIndex = -1;
        for (int i = parts.length - 1; i >= 1; i--) { // Start from end, look for colon
            if (parts[i].contains(Protocol.PRODUCT_QTY_DELIMITER)) {
                productListIndex = i;
                break;
            }
        }

        if (productListIndex == -1) { // No colon found in later args
            System.out.println("Usage: buy <Store Name> <prod1:q1,prod2:q2> (Product list format invalid?)");
            return null;
        }

        // Reconstruct store name (parts before product list)
        StringBuilder storeNameBuilder = new StringBuilder();
        for (int i = 0; i < productListIndex; i++) {
            storeNameBuilder.append(parts[i]);
            if (i < productListIndex - 1) {
                storeNameBuilder.append(" "); // Add space back if needed
            }
        }
        String storeName = storeNameBuilder.toString();

        // Get product list (the last part containing ':')
        String productList = parts[productListIndex];
        // Basic validation
        if (storeName.isEmpty() || productList.isEmpty()) {
            System.out.println("Store name or product list cannot be empty.");
            return null;
        }

        return Protocol.BUY + "|" + storeName + "|" + productList;
    }

    // --- UPDATED: prepareRateMessage ---
    private String prepareRateMessage(String args) {
        // Assume rating is the VERY last word and must be a number 1-5
        String[] parts = args.split(" ");
        if (parts.length < 2) {
            System.out.println("Usage: rate <Store Name> <1-5>");
            return null;
        }

        String ratingStr = parts[parts.length - 1]; // Get last part as potential rating
        int rating;
        try {
            rating = Integer.parseInt(ratingStr);
            if (rating < 1 || rating > 5) {
                System.err.println("Rating 1-5.");
                return null;
            }
        } catch (NumberFormatException e) {
            System.err.println("Rating not integer.");
            return null;
        }

        // Reconstruct store name (everything before the last part)
        StringBuilder storeNameBuilder = new StringBuilder();
        for (int i = 0; i < parts.length - 1; i++) {
            storeNameBuilder.append(parts[i]);
            if (i < parts.length - 2) {
                storeNameBuilder.append(" "); // Add space back
            }
        }
        String storeName = storeNameBuilder.toString();

        if (storeName.isEmpty()) {
            System.out.println("Store name empty.");
            return null;
        }

        return Protocol.RATE + "|" + storeName + "|" + ratingStr;
    }

    // Sends message to master, returns true on success, false on error
    private boolean sendMessage(String message) {
        if (out != null && !socket.isClosed() && running) {
            System.out.println("Sending to Master: " + message.substring(0, Math.min(100, message.length())) + "...");
            out.println(message);
            if (out.checkError()) {
                System.err.println("Error sending message.");
                running = false;
                return false;
            }
            return true; // Send successful
        } else {
            System.err.println("Cannot send. Connection closed?");
            running = false;
            return false;
        }
    }

    // Closes client resources
    public void closeClient() {
        if (!running && listenerThread == null)
            return;
        running = false;
        System.out.println("Closing dummy client.");
        try {
            if (listenerThread != null)
                listenerThread.interrupt();
            if (consoleScanner != null)
                consoleScanner.close();
            if (socket != null && !socket.isClosed())
                socket.close();
            if (listenerThread != null)
                listenerThread.join(500);
        } catch (IOException | InterruptedException e) {
            System.err.println("Error closing: " + e.getMessage());
            if (e instanceof InterruptedException)
                Thread.currentThread().interrupt();
        } finally {
            in = null;
            out = null;
            socket = null;
            listenerThread = null;
            System.out.println("Dummy Client closed.");
        }
    }

    // Main method
    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: java com.fooddelivery.client.DummyClient <host> <port>");
            System.exit(1);
        }
        String host = args[0];
        int port = 0;
        try {
            port = Integer.parseInt(args[1]);
            if (port <= 0 || port > 65535)
                throw new NumberFormatException();
        } catch (NumberFormatException e) {
            System.err.println("Invalid port");
            System.exit(1);
        }
        DummyClient client = null;
        try {
            client = new DummyClient(host, port);
            client.startConsole();
        } catch (IOException e) {
            System.err.println("Exiting dummy client due to connection failure.");
        } catch (Exception e) {
            System.err.println("Unexpected error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (client != null && client.running)
                client.closeClient();
            System.out.println("Dummy client finished.");
        }
    }
}
