package com.fooddelivery.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Store {
    private String storeName;
    private double latitude;
    private double longitude;
    private String foodCategory;
    private double stars;
    private int noOfVotes;
    private String storeLogo;
    private List<Product> products; // Contains ACTIVE and INACTIVE products
    private final Object storeLock = new Object();
    // --- NEW: Field to store calculated price category ---
    private String priceCategory = "N/A"; // Default if no products or not calculated yet

    private static final boolean PARSER_DEBUG = true;

    public Store(String storeName, double latitude, double longitude, String foodCategory, double stars, int noOfVotes,
            String storeLogo) {
        this.storeName = storeName;
        this.latitude = latitude;
        this.longitude = longitude;
        this.foodCategory = foodCategory;
        this.stars = stars;
        this.noOfVotes = noOfVotes;
        this.storeLogo = storeLogo;
        this.products = new ArrayList<>();
        // Initial calculation is done by the worker after adding
    }

    // Getters & Other Methods
    public String getStoreName() {
        return storeName;
    }

    public double getLatitude() {
        return latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public String getFoodCategory() {
        return foodCategory;
    }

    public double getStars() {
        return stars;
    }

    public int getNoOfVotes() {
        return noOfVotes;
    }

    public String getStoreLogo() {
        return storeLogo;
    }

    public List<Product> getProducts() {
        return products;
    } // Returns ALL products

    public Object getStoreLock() {
        return storeLock;
    }

    // --- NEW: Getter for the stored price category ---
    // This getter should be called within synchronized(storeLock) by the Worker for
    // consistency
    public String getPriceCategory() {
        return priceCategory;
    }

    public void setProducts(List<Product> products) {
        this.products = products;
    } // Needs external sync

    // Calculates avg price based on ACTIVE products.
    // This method itself doesn't need sync if called from within a synchronized
    // block.
    private double calculateAveragePriceOfActiveProducts() {
        List<Product> currentProducts = this.products; // Assumes called under lock
        if (currentProducts == null || currentProducts.isEmpty())
            return 0.0;
        double sum = 0;
        int count = 0;
        for (Product p : currentProducts) {
            if (p != null && p.isActive()) { // Only consider ACTIVE products
                sum += p.getPrice();
                count++;
            }
        }
        return (count > 0) ? (sum / count) : 0.0;
    }

    // --- NEW: Updates the stored price category ---
    // MUST be called inside synchronized(storeLock) block by the Worker.
    public void updatePriceCategory() {
        double avgPrice = calculateAveragePriceOfActiveProducts();
        if (avgPrice <= 0) {
            this.priceCategory = "N/A"; // Or perhaps "$" if 0 price counts as cheap? Let's use N/A.
        } else if (avgPrice <= 5.0) {
            this.priceCategory = "$";
        } else if (avgPrice <= 15.0) {
            this.priceCategory = "$$";
        } else {
            this.priceCategory = "$$$";
        }
        if (PARSER_DEBUG)
            System.out.println("DEBUG (Store): Updated price category for " + storeName + " to " + this.priceCategory
                    + " (Avg Price: " + String.format("%.2f", avgPrice) + ")");
    }

    // Adds rating. MUST be called inside synchronized(storeLock) block.
    public void addRating(int ratingValue) {
        if (ratingValue < 1 || ratingValue > 5)
            return;
        double currentTotalStars = this.stars * this.noOfVotes;
        this.noOfVotes++;
        this.stars = (this.noOfVotes > 0) ? ((currentTotalStars + ratingValue) / this.noOfVotes) : ratingValue;
    }

    // Finds product REGARDLESS of active status. Needs external sync if products
    // list can change.
    public Product findProduct(String productName) {
        List<Product> currentProducts = this.products;
        if (currentProducts == null)
            return null;
        for (Product p : currentProducts) {
            if (p != null && p.getProductName().equalsIgnoreCase(productName))
                return p;
        }
        return null;
    }

    // Adds product. MUST be called inside synchronized(storeLock) block. Also
    // update price category.
    public boolean addProduct(Product product) {
        if (product == null)
            return false;
        boolean added = this.products.add(product);
        updatePriceCategory();
        return added;
    } // Update category after adding
    // Removes product BY MARKING INACTIVE. MUST be called inside
    // synchronized(storeLock) block. Also update price category.

    public boolean removeProduct(String productName) {
        Product productToMark = findProduct(productName);
        if (productToMark != null) {
            productToMark.setActive(false); // Mark as inactive
            updatePriceCategory(); // Update category after marking inactive
            return true;
        }
        return false;
    }

    // toString - Added active count for debugging
    @Override
    public String toString() {
        int productCount = 0;
        int activeCount = 0;
        List<Product> currentProducts = this.products;
        if (currentProducts != null) {
            productCount = currentProducts.size();
            for (Product p : currentProducts) {
                if (p != null && p.isActive())
                    activeCount++;
            }
        }
        return String.format(
                "Store{name='%s', lat=%.5f, lon=%.5f, category='%s', stars=%.1f, votes=%d, priceCat='%s', products=%d (%d active)}",
                storeName, latitude, longitude, foodCategory, stars, noOfVotes, priceCategory, productCount,
                activeCount);
    }

    // --- UPDATED toJson() ---
    // Creates a JSON representation of the store, including ONLY ACTIVE products.
    // Needs external synchronization on storeLock.
    public String toJson() {
        StringBuilder productsJson = new StringBuilder("[");
        List<Product> currentProducts = this.products; // Assumes called under external lock
        boolean firstProductAdded = false;
        if (currentProducts != null) {
            for (Product p : currentProducts) {
                if (p != null && p.isActive()) { // Only include ACTIVE products
                    if (firstProductAdded) {
                        productsJson.append(",");
                    }
                    productsJson.append(p.toJson());
                    firstProductAdded = true;
                }
            }
        }
        productsJson.append("]");
        // Note: We don't include the calculated priceCategory in the JSON sent to the
        // client.
        return String.format(
                "{\"StoreName\":\"%s\",\"Latitude\":%.5f,\"Longitude\":%.5f,\"FoodCategory\":\"%s\",\"Stars\":%.1f,\"NoOfVotes\":%d,\"StoreLogo\":\"%s\",\"Products\":%s}",
                storeName.replace("\"", "\\\""), latitude, longitude, foodCategory.replace("\"", "\\\""), stars,
                noOfVotes, storeLogo.replace("\"", "\\\""), productsJson.toString());
    }

    // Parses a JSON string into a Store object. Price category calculated later by
    // Worker.
    public static Store fromJson(String json) {
        if (json == null || json.trim().isEmpty()) {
            System.err.println("Store.fromJson: Input JSON null or empty.");
            return null;
        }
        json = json.replace("\n", "").replace("\r", "").trim();
        if (PARSER_DEBUG)
            System.out.println("DEBUG (Store.fromJson): Parsing JSON: " + json);
        String name = Product.parseJsonStringValue(json, "StoreName");
        double lat = Product.parseJsonDoubleValue(json, "Latitude");
        double lon = Product.parseJsonDoubleValue(json, "Longitude");
        String category = Product.parseJsonStringValue(json, "FoodCategory");
        double stars = Product.parseJsonDoubleValue(json, "Stars");
        int votes = Product.parseJsonIntValue(json, "NoOfVotes");
        String logo = Product.parseJsonStringValue(json, "StoreLogo");
        if (name == null || category == null || lat == -1.0 || lon == -1.0) {
            System.err.println("Store.fromJson Error: Failed mandatory fields.");
            return null;
        }
        if (stars == -1.0)
            stars = 0.0;
        if (votes == -1)
            votes = 0;
        if (logo == null)
            logo = "";
        Store store = new Store(name, lat, lon, category, stars, votes, logo);
        if (PARSER_DEBUG)
            System.out.println("DEBUG (Store.fromJson): Basic fields parsed.");
        String productsKey = "\"Products\":";
        int productsArrayStartIndex = json.indexOf(productsKey);
        if (productsArrayStartIndex != -1) {
            productsArrayStartIndex += productsKey.length();
            if (PARSER_DEBUG)
                System.out.println("DEBUG (Store.fromJson): Found 'Products' key.");
            int arrayOpenBracketIndex = -1;
            for (int i = productsArrayStartIndex; i < json.length(); i++) {
                char c = json.charAt(i);
                if (c == '[') {
                    arrayOpenBracketIndex = i;
                    break;
                }
                if (!Character.isWhitespace(c))
                    break;
            }
            if (arrayOpenBracketIndex != -1) {
                if (PARSER_DEBUG)
                    System.out.println("DEBUG (Store.fromJson): Found '[' at " + arrayOpenBracketIndex);
                int currentPos = arrayOpenBracketIndex + 1;
                int braceLevel = 0;
                int productStartIndex = -1;
                while (currentPos < json.length()) {
                    char currentChar = json.charAt(currentPos);
                    if (currentChar == '{' && braceLevel == 0) {
                        if (PARSER_DEBUG)
                            System.out.println("DEBUG: Prod start '{' at " + currentPos);
                        productStartIndex = currentPos;
                        braceLevel++;
                    } else if (currentChar == '{' && productStartIndex != -1) {
                        braceLevel++;
                    } else if (currentChar == '}' && productStartIndex != -1) {
                        braceLevel--;
                        if (braceLevel == 0) {
                            String productJsonFragment = json.substring(productStartIndex, currentPos + 1);
                            if (PARSER_DEBUG)
                                System.out.println("DEBUG: Extracted prod frag: " + productJsonFragment);
                            Product product = Product.fromJsonFragment(productJsonFragment);
                            if (product != null) {
                                store.getProducts().add(product);
                                /* Price category not calculated here */ if (PARSER_DEBUG)
                                    System.out.println("DEBUG: Added product " + product.getProductName());
                            } else {
                                System.err.println("Store.fromJson: Failed product frag: " + productJsonFragment);
                            }
                            productStartIndex = -1;
                        } else if (braceLevel < 0) {
                            System.err.println("Store.fromJson Error: Mismatched braces near " + currentPos);
                            break;
                        }
                    } else if (currentChar == ']' && braceLevel == 0) {
                        if (PARSER_DEBUG)
                            System.out.println("DEBUG: Found array end ']' at " + currentPos);
                        break;
                    }
                    currentPos++; // Move this outside the specific char checks
                }
                if (braceLevel != 0) {
                    System.err.println("Store.fromJson Error: Mismatched braces at end.");
                }
            } else {
                System.err.println("Store.fromJson: Malformed JSON - Couldn't find '[' after 'Products:'.");
            }
        } else {
            System.out.println("Store.fromJson: 'Products' key not found or array missing.");
        }
        if (PARSER_DEBUG)
            System.out.println("DEBUG (Store.fromJson): Finished parsing.");
        return store;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Store store = (Store) o;
        return Objects.equals(storeName, store.storeName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(storeName);
    }
}
