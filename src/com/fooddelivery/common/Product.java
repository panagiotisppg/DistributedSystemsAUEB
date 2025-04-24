package com.fooddelivery.common;

import java.util.Objects;

public class Product {
    private String productName;
    private String productType;
    private int availableAmount;
    private double price;
    private int totalUnitsSold = 0;
    private double totalRevenue = 0.0;
    // --- NEW: Flag for soft delete ---
    private boolean isActive = true; // Default to active when created

    // --- Static flag for debug logging ---
    private static final boolean PARSER_DEBUG = true; // Set true for detailed parsing logs

    public Product(String productName, String productType, int availableAmount, double price) {
        this.productName = productName;
        this.productType = productType;
        this.availableAmount = availableAmount;
        this.price = price;
        this.isActive = true; // Ensure new products are active
    }

    // Getters
    public String getProductName() { return productName; }
    public String getProductType() { return productType; }
    public int getAvailableAmount() { return availableAmount; }
    public double getPrice() { return price; }
    public int getTotalUnitsSold() { return totalUnitsSold; }
    public double getTotalRevenue() { return totalRevenue; }
    public boolean isActive() { return isActive; } // Getter for active status

    // Setters - **MUST be called within appropriate synchronized blocks in Worker**
    public void setAvailableAmount(int availableAmount) { this.availableAmount = availableAmount; }
    public void setProductName(String productName) { this.productName = productName; }
    public void setProductType(String productType) { this.productType = productType; }
    public void setPrice(double price) { this.price = price; }
    public void setActive(boolean active) { isActive = active; } // Setter for active status

    // recordSale - No changes needed
    public void recordSale(int quantity) { if (quantity > 0) { this.totalUnitsSold += quantity; this.totalRevenue += quantity * this.price; } }

    // toString - Added active status for debugging
    @Override public String toString() { return "Product{name='" + productName + "', type='" + productType + "', amount=" + availableAmount + ", price=" + price + ", active=" + isActive + '}'; }

    // toJson - No change needed here, filtering happens in Store.toJson
    public String toJson() { return String.format("{\"ProductName\":\"%s\",\"ProductType\":\"%s\",\"AvailableAmount\":%d,\"Price\":%.2f}", productName.replace("\"", "\\\""), productType.replace("\"", "\\\""), availableAmount, price); }

    // fromJsonFragment - No changes needed (new products default to active)
    public static Product fromJsonFragment(String jsonFragment) {
        if (PARSER_DEBUG) System.out.println("DEBUG (Product.fromJsonFragment): Parsing fragment: " + jsonFragment);
        String name = parseJsonStringValue(jsonFragment, "ProductName");
        String type = parseJsonStringValue(jsonFragment, "ProductType");
        int amount = parseJsonIntValue(jsonFragment, "AvailableAmount");
        double price = parseJsonDoubleValue(jsonFragment, "Price");

        if (name != null && type != null && amount != -1 && price != -1.0) {
            if (PARSER_DEBUG) System.out.println("DEBUG (Product.fromJsonFragment): Success -> name=" + name + ", type=" + type + ", amount=" + amount + ", price=" + price);
            return new Product(name, type, amount, price); // isActive defaults to true
        }
        System.err.println("Error parsing Product fragment (one or more fields failed): " + jsonFragment);
        if (PARSER_DEBUG) System.out.println("DEBUG (Product.fromJsonFragment): FAILED.");
        return null;
    }

    // --- Helper methods for manual parsing (More robust to whitespace + DEBUG) ---

    public static String parseJsonStringValue(String json, String key) {
        if (PARSER_DEBUG) System.out.println("DEBUG (parseJsonStringValue): Searching for key '" + key + "'");
        String keyPattern = "\"" + key + "\"";
        int keyIndex = json.indexOf(keyPattern);
        if (keyIndex == -1) { if (PARSER_DEBUG) System.out.println("DEBUG: Key '" + key + "' not found."); return null; }

        int colonIndex = -1;
        for (int i = keyIndex + keyPattern.length(); i < json.length(); i++) {
            char c = json.charAt(i);
            if (c == ':') { colonIndex = i; break; }
            if (!Character.isWhitespace(c)) { if (PARSER_DEBUG) System.out.println("DEBUG: Unexpected char '" + c + "' before colon for key '" + key + "'."); return null; }
        }
        if (colonIndex == -1) { if (PARSER_DEBUG) System.out.println("DEBUG: Colon not found for key '" + key + "'."); return null; }

        int valueStartIndex = -1;
        for (int i = colonIndex + 1; i < json.length(); i++) {
            char c = json.charAt(i);
            if (c == '"') { valueStartIndex = i + 1; break; }
            if (!Character.isWhitespace(c)) { if (PARSER_DEBUG) System.out.println("DEBUG: Unexpected char '" + c + "' before value quote for key '" + key + "'."); return null; }
        }
        if (valueStartIndex == -1) { if (PARSER_DEBUG) System.out.println("DEBUG: Opening value quote not found for key '" + key + "'."); return null; }

        int valueEndIndex = json.indexOf('"', valueStartIndex);
        if (valueEndIndex == -1) { if (PARSER_DEBUG) System.out.println("DEBUG: Closing value quote not found for key '" + key + "'."); return null; }

        String value = json.substring(valueStartIndex, valueEndIndex).replace("\\\"", "\"");
        if (PARSER_DEBUG) System.out.println("DEBUG: Found value '" + value + "' for key '" + key + "'.");
        return value;
    }

     public static int parseJsonIntValue(String json, String key) {
         if (PARSER_DEBUG) System.out.println("DEBUG (parseJsonIntValue): Searching for key '" + key + "'");
         String keyPattern = "\"" + key + "\"";
         int keyIndex = json.indexOf(keyPattern);
         if (keyIndex == -1) { if (PARSER_DEBUG) System.out.println("DEBUG: Key '" + key + "' not found."); return -1; }

         int colonIndex = -1;
         for (int i = keyIndex + keyPattern.length(); i < json.length(); i++) { char c = json.charAt(i); if (c == ':') { colonIndex = i; break; } if (!Character.isWhitespace(c)) return -1; }
         if (colonIndex == -1) { if (PARSER_DEBUG) System.out.println("DEBUG: Colon not found for key '" + key + "'."); return -1; }

        int valueStartIndex = -1;
        for (int i = colonIndex + 1; i < json.length(); i++) { char c = json.charAt(i); if (!Character.isWhitespace(c)) { valueStartIndex = i; break; } }
        if (valueStartIndex == -1) { if (PARSER_DEBUG) System.out.println("DEBUG: Value start not found for key '" + key + "'."); return -1; }

        int valueEndIndex = -1;
        for (int i = valueStartIndex; i < json.length(); i++) { char c = json.charAt(i); boolean isDigitOrLeadingMinus = Character.isDigit(c) || (c == '-' && i == valueStartIndex); if (!isDigitOrLeadingMinus) { valueEndIndex = i; break; } }
        if (valueEndIndex == -1) { valueEndIndex = json.length(); }

        if (valueStartIndex >= valueEndIndex) { if (PARSER_DEBUG) System.out.println("DEBUG: Invalid number range for key '" + key + "'."); return -1; }

         try {
             String numStr = json.substring(valueStartIndex, valueEndIndex).trim();
             if (numStr.isEmpty() || numStr.equals("-")) { if (PARSER_DEBUG) System.out.println("DEBUG: Empty number string for key '" + key + "'."); return -1; }
             int value = Integer.parseInt(numStr);
             if (PARSER_DEBUG) System.out.println("DEBUG: Found value '" + value + "' for key '" + key + "'.");
             return value;
         } catch (NumberFormatException | StringIndexOutOfBoundsException e) {
             System.err.println("Error parsing int for key '" + key + "' from substring '" + json.substring(valueStartIndex, Math.min(valueEndIndex, json.length())) +"': " + e.getMessage());
             return -1;
         }
     }

     public static double parseJsonDoubleValue(String json, String key) {
         if (PARSER_DEBUG) System.out.println("DEBUG (parseJsonDoubleValue): Searching for key '" + key + "'");
         String keyPattern = "\"" + key + "\"";
         int keyIndex = json.indexOf(keyPattern);
         if (keyIndex == -1) { if (PARSER_DEBUG) System.out.println("DEBUG: Key '" + key + "' not found."); return -1.0; }

         int colonIndex = -1;
         for (int i = keyIndex + keyPattern.length(); i < json.length(); i++) { char c = json.charAt(i); if (c == ':') { colonIndex = i; break; } if (!Character.isWhitespace(c)) return -1.0; }
         if (colonIndex == -1) { if (PARSER_DEBUG) System.out.println("DEBUG: Colon not found for key '" + key + "'."); return -1.0; }

        int valueStartIndex = -1;
        for (int i = colonIndex + 1; i < json.length(); i++) { char c = json.charAt(i); if (!Character.isWhitespace(c)) { valueStartIndex = i; break; } }
        if (valueStartIndex == -1) { if (PARSER_DEBUG) System.out.println("DEBUG: Value start not found for key '" + key + "'."); return -1.0; }

        int valueEndIndex = -1;
         for (int i = valueStartIndex; i < json.length(); i++) { char c = json.charAt(i); boolean isPartOfDouble = Character.isDigit(c) || c == '.' || (c == '-' && i == valueStartIndex); if (!isPartOfDouble) { valueEndIndex = i; break; } }
         if (valueEndIndex == -1) { valueEndIndex = json.length(); }

        if (valueStartIndex >= valueEndIndex) { if (PARSER_DEBUG) System.out.println("DEBUG: Invalid number range for key '" + key + "'."); return -1.0; }

         try {
             String numStr = json.substring(valueStartIndex, valueEndIndex).trim();
             if (numStr.isEmpty() || numStr.equals("-") || numStr.equals(".")) { if (PARSER_DEBUG) System.out.println("DEBUG: Empty number string for key '" + key + "'."); return -1.0; }
             double value = Double.parseDouble(numStr);
             if (PARSER_DEBUG) System.out.println("DEBUG: Found value '" + value + "' for key '" + key + "'.");
             return value;
         } catch (NumberFormatException | StringIndexOutOfBoundsException e) {
             System.err.println("Error parsing double for key '" + key + "' from substring '" + json.substring(valueStartIndex, Math.min(valueEndIndex, json.length())) +"': " + e.getMessage());
             return -1.0;
         }
     }

     @Override public boolean equals(Object o) { if (this == o) return true; if (o == null || getClass() != o.getClass()) return false; Product product = (Product) o; return Objects.equals(productName, product.productName) && Objects.equals(productType, product.productType); }
     @Override public int hashCode() { return Objects.hash(productName, productType); }
}