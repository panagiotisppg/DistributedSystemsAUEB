import java.io.*;
import java.nio.file.*;
import java.util.*;
import org.json.*;

public class StoreManagerCLI {
    static Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) {
        while (true) {
            System.out.println("---------------------------------------");
            System.out.println("* Option 1) Add a new shop");
            System.out.println("* Option 2) Update product availability");
            System.out.println("* Option 3) Add/Remove products");
            System.out.println("* Option 4) Exit");
            System.out.println("---------------------------------------");
            int option = getIntInput("Choose an option (1-4): ", 1, 4);

            switch (option) {
                case 1 -> addNewShopInteractive();
                case 2 -> updateProductAmountInteractive();
                case 3 -> manageProductsInteractive();
                case 4 -> System.exit(0);
            }
        }
    }

    static void addNewShop(String name, double lat, double lon, String category, double stars, int votes, String logo, JSONArray products) throws IOException {
        JSONObject store = new JSONObject();
        store.put("StoreName", name);
        store.put("Latitude", lat);
        store.put("Longitude", lon);
        store.put("FoodCategory", category);
        store.put("Stars", stars);
        store.put("NoOfVotes", votes);
        store.put("StoreLogo", logo);
        store.put("Products", products);

        Files.write(Paths.get(name + ".json"), store.toString(2).getBytes());
    }

    static int updateProductAmount(String storeName, String productName, String changeOfAmount) throws IOException {
        String fileName = storeName + ".json";
        JSONObject store = new JSONObject(Files.readString(Paths.get(fileName)));
        JSONArray products = store.getJSONArray("Products");

        for (int i = 0; i < products.length(); i++) {
            JSONObject product = products.getJSONObject(i);
            if (product.getString("ProductName").equals(productName)) {
                int oldAmount = product.getInt("AvailableAmount");
                int delta = Integer.parseInt(changeOfAmount);
                if (oldAmount + delta < 0) return -1;
                int newAmount = oldAmount + delta;
                product.put("AvailableAmount", newAmount);
                Files.write(Paths.get(fileName), store.toString(2).getBytes());
                return 1;
            }
        }
        return -1;
    }

    static void addNewShopInteractive() {
        try {
            System.out.print(" Store Name: ");
            String name = scanner.nextLine();
            double lat = getDoubleInput(" Latitude: ");
            double lon = getDoubleInput(" Longitude: ");
            System.out.print(" Food Category: ");
            String category = scanner.nextLine();
            double stars = getDoubleInput(" Stars: ");
            int votes = getIntInput(" No. of Votes: ");
            System.out.print(" Store Logo: ");
            String logo = scanner.nextLine();
            int n = getIntInput(" Number of Products: ", 0, Integer.MAX_VALUE);

            JSONArray products = new JSONArray();
            for (int i = 0; i < n; i++) {
                System.out.println("  Product " + (i + 1));
                System.out.print("   Name: ");
                String pname = scanner.nextLine();
                System.out.print("   Type: ");
                String ptype = scanner.nextLine();
                int amount = getIntInput("   Available Amount: ", 0, Integer.MAX_VALUE);
                double price = getDoubleInput("   Price: ");

                JSONObject product = new JSONObject();
                product.put("ProductName", pname);
                product.put("ProductType", ptype);
                product.put("AvailableAmount", amount);
                product.put("Price", price);
                products.put(product);
            }

            addNewShop(name, lat, lon, category, stars, votes, logo, products);
            System.out.println("Store saved as " + name + ".json");
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    static void updateProductAmountInteractive() {
        try {
            List<String> files = listJsonFiles();
            if (files.isEmpty()) return;

            System.out.println("Choose a store:");
            for (int i = 0; i < files.size(); i++) {
                System.out.println("   " + (i + 1) + ". " + files.get(i));
            }
            int storeIndex = getIntInput("Store number: ", 1, files.size()) - 1;
            String fileName = files.get(storeIndex);

            JSONObject store = new JSONObject(Files.readString(Paths.get(fileName)));
            JSONArray products = store.getJSONArray("Products");

            System.out.println("Choose a product to update:");
            for (int i = 0; i < products.length(); i++) {
                JSONObject p = products.getJSONObject(i);
                System.out.println("   " + (i + 1) + ". " + p.getString("ProductName"));
            }
            int productIndex = getIntInput("Product number: ", 1, products.length()) - 1;
            JSONObject product = products.getJSONObject(productIndex);

            int oldAmount = product.getInt("AvailableAmount");
            System.out.println("Current available amount: " + oldAmount);

            int delta;
            while (true) {
                System.out.print("Enter change (e.g. +2, -1): ");
                try {
                    delta = Integer.parseInt(scanner.nextLine());
                    if (oldAmount + delta >= 0) break;
                    else System.out.println("Resulting amount cannot be negative. Try again.");
                } catch (NumberFormatException e) {
                    System.out.println("Invalid input. Enter a number like +2 or -1.");
                }
            }

            int newAmount = oldAmount + delta;
            product.put("AvailableAmount", newAmount);
            Files.write(Paths.get(fileName), store.toString(2).getBytes());
            System.out.println("Updated product amount.");

        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    static void manageProductsInteractive() {
        try {
            int choice = getIntInput("1) Add new products\n2) Remove existing products\nChoose an option: ", 1, 2);

            List<String> files = listJsonFiles();
            if (files.isEmpty()) return;

            System.out.println("Choose a store:");
            for (int i = 0; i < files.size(); i++) {
                System.out.println("   " + (i + 1) + ". " + files.get(i));
            }
            int storeIndex = getIntInput("Store number: ", 1, files.size()) - 1;
            String fileName = files.get(storeIndex);
            JSONObject store = new JSONObject(Files.readString(Paths.get(fileName)));
            JSONArray products = store.getJSONArray("Products");

            if (choice == 1) {
                int count = getIntInput("How many new products to add? ", 0, Integer.MAX_VALUE);
                for (int i = 0; i < count; i++) {
                    System.out.println("Product " + (i + 1));
                    System.out.print("  Name: ");
                    String pname = scanner.nextLine();
                    System.out.print("  Type: ");
                    String ptype = scanner.nextLine();
                    int amount = getIntInput("  Available Amount: ", 0, Integer.MAX_VALUE);
                    double price = getDoubleInput("  Price: ");

                    JSONObject product = new JSONObject();
                    product.put("ProductName", pname);
                    product.put("ProductType", ptype);
                    product.put("AvailableAmount", amount);
                    product.put("Price", price);
                    products.put(product);
                }
                Files.write(Paths.get(fileName), store.toString(2).getBytes());
                System.out.println("Products added.");
            } else {
                System.out.println("Choose products to remove (e.g. 1 4 5):");
                for (int i = 0; i < products.length(); i++) {
                    JSONObject p = products.getJSONObject(i);
                    System.out.println((i + 1) + ". " + p.getString("ProductName"));
                }
                String[] indexes = scanner.nextLine().split(" ");
                List<Integer> toRemove = new ArrayList<>();
                for (String idx : indexes) {
                    try {
                        int val = Integer.parseInt(idx.trim()) - 1;
                        if (val >= 0 && val < products.length()) toRemove.add(val);
                    } catch (NumberFormatException ignored) {}
                }
                toRemove.sort(Collections.reverseOrder());
                for (int idx : toRemove) {
                    products.remove(idx);
                }
                Files.write(Paths.get(fileName), store.toString(2).getBytes());
                System.out.println("Selected products removed.");
            }

        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    static List<String> listJsonFiles() throws IOException {
        File dir = new File(".");
        File[] files = dir.listFiles((d, name) -> name.endsWith(".json"));
        if (files == null || files.length == 0) {
            System.out.println("No store files found.");
            return Collections.emptyList();
        }
        List<String> fileNames = new ArrayList<>();
        for (File f : files) fileNames.add(f.getName());
        return fileNames;
    }

    static int getIntInput(String prompt) {
        while (true) {
            System.out.print(prompt);
            try {
                return Integer.parseInt(scanner.nextLine());
            } catch (NumberFormatException e) {
                System.out.println("Please enter a valid integer.");
            }
        }
    }

    static int getIntInput(String prompt, int min, int max) {
        while (true) {
            int value = getIntInput(prompt);
            if (value >= min && value <= max) return value;
            System.out.println("Please enter a number between " + min + " and " + max);
        }
    }

    static double getDoubleInput(String prompt) {
        while (true) {
            System.out.print(prompt);
            try {
                return Double.parseDouble(scanner.nextLine());
            } catch (NumberFormatException e) {
                System.out.println("Please enter a valid number.");
            }
        }
    }
}
