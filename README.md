# README - Distributed Food Delivery System

This document provides instructions on how to compile and run the simple distributed food delivery backend system implemented in Java using basic synchronization primitives.

## 1. Prerequisites

*   **Java Development Kit (JDK):** JDK 8 or later recommended. Verify with `java -version` and `javac -version`.

## 2. Directory Structure

Your project should look like this:
```
DistributedSystemsAUEB/
  └── src/
    └── com/
      └── fooddelivery/
        ├── common/ 
        │ ├── GeoUtils.java
        │ ├── Product.java
        │ ├── Protocol.java
        │ └── Store.java
        ├── core/ 
        │ ├── ClientHandler.java
        │ ├── Master.java
        │ └── Worker.java
        └── client/ 
          ├── DummyClient.java
          └── ManagerClient.java
```

## 3. Compilation

1.  Open a terminal/command prompt.
2.  Navigate to your `project-root` directory (the one containing `src`).
3.  Compile all `.java` files:

    *   **Linux/macOS:**
        ```bash
        javac -Xlint:unchecked src/com/fooddelivery/common/*.java src/com/fooddelivery/core/*.java src/com/fooddelivery/client/*.java
        ```
    *   **Windows:**
        ```bash
        javac -Xlint:unchecked src\com\fooddelivery\common\*.java src\com\fooddelivery\core\*.java src\com\fooddelivery\client\*.java
        ```
    *(This creates `.class` files within the `src` directory structure).*

## 4. Running the Application

Start components **in order**: Workers -> Master -> Clients. Run all `java` commands from the `project-root` directory.

**4.1. Run Worker(s)** (Separate terminal for each)

*   **Command:** `java com.fooddelivery.core.Worker <worker-listen-port> <master-host> <master-result-port>`
*   **Example Worker 1:**
    ```bash
    java com.fooddelivery.core.Worker 5001 localhost 6001
    ```
*   **Example Worker 2:**
    ```bash
    java com.fooddelivery.core.Worker 5002 localhost 6001
    ```
*   *Output:* `Worker listening on port <port>`

**4.2. Run Master** (Separate terminal)

*   **Command:** `java com.fooddelivery.core.Master <client-port> <result-port> <worker1-host:port> <worker2-host:port> ...`
*   **Example Master:**
    ```bash
    java com.fooddelivery.core.Master 6000 6001 localhost:5001 localhost:5002
    ```
*   *Output:* Confirmation of worker connections and listening ports.

**4.3. Run Client(s)** (Separate terminal for each)

*   Connect to the Master's `<client-port>`.

*   **Manager Client:**
    *   **Command:** `java com.fooddelivery.client.ManagerClient <master-host> <master-client-port>`
    *   **Example:**
        ```bash
        java com.fooddelivery.client.ManagerClient localhost 6000
        ```

*   **Dummy Client (User):**
    *   **Command:** `java com.fooddelivery.client.DummyClient <master-host> <master-client-port>`
    *   **Example:**
        ```bash
        java com.fooddelivery.client.DummyClient localhost 6000
        ```
*   *Output:* Connection message and command menu.

## 5. Example Commands

**(Remember to create sample `.json` files for `addstore` and `addproduct`)**

*   **Manager Client (`Manager >`)**
    *   `addstore path/to/store.json`
    *   `addproduct StoreName path/to/product.json`
    *   `removeproduct StoreName ProductName` (Marks inactive)
    *   `updatestock StoreName ProductName +/-Amount`
    *   `getsales StoreName` (Shows report for all products)
    *   `exit`

*   **Dummy Client (`Client >`)**
    *   `search <lat> <lon> <radius> <category|null> <stars|null> <price|null ($/$$/$$$)>`
        *   Example: `search 37.99 23.73 5.0 pizzeria 4 $$`
        *   Example: `search 37.98 23.72 10 null null null`
        *   *(Results appear asynchronously)*
    *   `buy <StoreName> <product1:qty1>,<product2:qty2>,...`
        *   Example: `buy PizzaPlace margarita:1,coke:2`
        *   *(Response appears asynchronously)*
    *   `rate <StoreName> <1-5>`
        *   Example: `rate PizzaPlace 5`
        *   *(Response appears asynchronously)*
    *   `exit`

## 6. Stopping the Application

1.  Type `exit` in all client terminals.
2.  Press `Ctrl + C` in the Master terminal.
3.  Press `Ctrl + C` in each Worker terminal.

## 7. Notes

*   Ensure ports are available.
*   Use IPs/hostnames instead of `localhost` if running on different machines.
*   Check firewalls if running across machines.
*   Client responses (especially for Dummy Client) arrive asynchronously and might appear while typing.
