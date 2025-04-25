package com.fooddelivery.client;

import com.fooddelivery.common.Protocol;
import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.util.Scanner;

// Simple Manager CLI Application for administrative tasks.
public class ManagerClient {

    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;
    private final Scanner consoleScanner;

    // Constructor
    public ManagerClient(String masterHost, int masterPort) throws IOException {
        try {
            this.socket = new Socket();
            this.socket.connect(new InetSocketAddress(masterHost, masterPort), 5000);
            this.out = new PrintWriter(socket.getOutputStream(), true);
            this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            this.consoleScanner = new Scanner(System.in);
            System.out.println("Manager connected to Master at " + masterHost + ":" + masterPort);
        } catch (IOException e) {
            System.err.println("Manager Error: Could not connect to Master: " + e.getMessage());
            throw e;
        }
    }

    // Starts the interactive console loop.
    public void startConsole() {
        String line;
        boolean running = true;
        while (running) {
            printMenu();
            System.out.print("Manager > ");
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
            String response = handleCommand(line); // Process command and get sync response
            if (response != null) {
                // Special formatting for multi-line sales results
                if (response.startsWith(Protocol.SALES_RESULT + "|")) {
                    displaySalesResult(response);
                } else {
                    System.out.println("[Master Response]: " + response);
                }
            } else {
                System.err.println("No response or client-side error.");
            }
        }
        close();
    }

    // Prints the menu - UPDATED (removed rate)
    private void printMenu() {
        System.out.println(
                "\n--- Manager Menu ---\n  addstore <json_file_path>\n  addproduct <storeName> <prod_json_path>\n  removeproduct <storeName> <productName>\n  updatestock <storeName> <productName> <amount>\n  getsales <storeName>\n  exit\n--------------------");
    }

    // Handles command input, prepares message, sends and waits for reply.
    private String handleCommand(String line) {
        String[] parts = line.split(" ", 2);
        String command = parts[0].toLowerCase();
        String args = (parts.length > 1) ? parts[1] : "";
        String messageToSend = null;
        try {
            switch (command) {
                case "addstore":
                    messageToSend = prepareAddStoreMessage(args);
                    break;
                case "addproduct":
                    messageToSend = prepareAddProductMessage(args);
                    break;
                case "removeproduct":
                    messageToSend = prepareRemoveProductMessage(args);
                    break;
                case "updatestock":
                    messageToSend = prepareUpdateStockMessage(args);
                    break;
                // case "rate": messageToSend = prepareRateMessage(args); break; // <-- REMOVED
                // CASE
                case "getsales":
                    messageToSend = prepareGetSalesMessage(args);
                    break;
                default:
                    System.out.println("Unknown command: " + command);
                    return "[Client Info]: Unknown command.";
            }
        } catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
            return "[Client Error]: File read error.";
        } catch (InvalidPathException e) {
            System.err.println("Invalid file path: " + e.getMessage());
            return "[Client Error]: Invalid path.";
        } catch (Exception e) {
            System.err.println("Error processing args: " + e.getMessage());
            return "[Client Error]: Invalid arguments.";
        }

        if (messageToSend != null) {
            return sendMessageAndWait(messageToSend);
        } else {
            return "[Client Info]: Command failed validation.";
        }
    }

    // --- Message Preparation Methods ---
    private String prepareAddStoreMessage(String filePath) throws IOException, InvalidPathException {
        if (filePath.isEmpty()) {
            System.out.println("Usage: addstore <path>");
            return null;
        }
        String jsonContent = new String(Files.readAllBytes(Paths.get(filePath))).trim().replace("\n", "").replace("\r",
                "");
        if (!jsonContent.startsWith("{") || !jsonContent.endsWith("}")) {
            System.err.println("Error: Not JSON object.");
            return null;
        }
        return Protocol.ADD_STORE + "|" + jsonContent;
    }

    private String prepareAddProductMessage(String args) throws IOException, InvalidPathException {
        String[] parts = args.split(" ", 2);
        if (parts.length != 2) {
            System.out.println("Usage: addproduct <store> <path>");
            return null;
        }
        String storeName = parts[0];
        String filePath = parts[1];
        if (storeName.isEmpty() || filePath.isEmpty()) {
            System.out.println("Names empty.");
            return null;
        }
        String productJson = new String(Files.readAllBytes(Paths.get(filePath))).trim().replace("\n", "").replace("\r",
                "");
        if (!productJson.startsWith("{") || !productJson.endsWith("}")) {
            System.err.println("Error: Not JSON object.");
            return null;
        }
        return Protocol.ADD_PRODUCT + "|" + storeName + "|" + productJson;
    }

    private String prepareRemoveProductMessage(String args) {
        String[] parts = args.split(" ", 2);
        if (parts.length != 2) {
            System.out.println("Usage: removeproduct <store> <product>");
            return null;
        }
        String storeName = parts[0];
        String productName = parts[1];
        if (storeName.isEmpty() || productName.isEmpty()) {
            System.out.println("Names empty.");
            return null;
        }
        return Protocol.REMOVE_PRODUCT + "|" + storeName + "|" + productName;
    }

    private String prepareUpdateStockMessage(String args) {
        String[] parts = args.split(" ");
        if (parts.length != 3) {
            System.out.println("Usage: updatestock <store> <product> <amount>");
            return null;
        }
        String storeName = parts[0];
        String productName = parts[1];
        String amountStr = parts[2];
        try {
            Integer.parseInt(amountStr);
        } catch (NumberFormatException e) {
            System.err.println("Error: Amount not integer.");
            return null;
        }
        if (storeName.isEmpty() || productName.isEmpty()) {
            System.out.println("Names empty.");
            return null;
        }
        return Protocol.UPDATE_STOCK + "|" + storeName + "|" + productName + "|" + amountStr;
    }

    // --- prepareRateMessage REMOVED ---
    // private String prepareRateMessage(String args) { ... }
    private String prepareGetSalesMessage(String args) {
        String storeName = args.trim();
        if (storeName.isEmpty() || storeName.contains(" ")) {
            System.out.println("Usage: getsales <storeName>");
            return null;
        }
        return Protocol.GET_SALES + Protocol.DELIMITER + storeName;
    }

    // Sends message and waits for reply.
    private String sendMessageAndWait(String message) {
        if (out == null || in == null || socket == null || socket.isClosed())
            return "[Client Error]: Connection closed.";
        try {
            System.out.println("Sending to Master: " + message.substring(0, Math.min(100, message.length())) + "...");
            out.println(message);
            socket.setSoTimeout(10000);
            String response = in.readLine();
            socket.setSoTimeout(0);
            if (response == null) {
                close();
                return "[Client Error]: Connection closed by Master.";
            }
            return response;
        } catch (SocketTimeoutException e) {
            return "[Client Error]: Timeout waiting for Master response.";
        } catch (IOException e) {
            close();
            return "[Client Error]: Communication error.";
        }
    }

    // Display formatted sales results
    private void displaySalesResult(String rawResponse) {
        String[] parts = rawResponse.split("\\|", 3); // SALES_RESULT | requestId | data
        if (parts.length < 3) {
            System.out.println("[Master Response Error]: Malformed sales result -> " + rawResponse);
            return;
        }
        String salesData = parts[2];
        System.out.println("--- Sales Report ---");
        System.out.println(salesData.replace(Protocol.SALES_DATA_SEPARATOR, "\n")); // Replace separator with newline
        System.out.println("--------------------");
    }

    // Closes resources.
    public void close() {
        System.out.println("Closing manager client connection.");
        try {
            if (consoleScanner != null)
                consoleScanner.close();
            if (socket != null && !socket.isClosed())
                socket.close();
        } catch (IOException e) {
            System.err.println("Error closing: " + e.getMessage());
        } finally {
            in = null;
            out = null;
            socket = null;
        }
    }

    // Main method.
    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: java com.fooddelivery.client.ManagerClient <host> <port>");
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
        ManagerClient manager = null;
        try {
            manager = new ManagerClient(host, port);
            manager.startConsole();
        } catch (IOException e) {
            System.err.println("Exiting manager client due to connection failure.");
        } catch (Exception e) {
            System.err.println("Unexpected error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (manager != null && manager.socket != null && !manager.socket.isClosed())
                manager.close();
            System.out.println("Manager client finished.");
        }
    }
}
