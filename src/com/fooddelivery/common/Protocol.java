package com.fooddelivery.common;

public class Protocol {
    // Delimiters
    public static final String DELIMITER = "|"; // Main command parts separator
    public static final String PRODUCT_LIST_DELIMITER = ","; // Separator for items in buy list
    public static final String PRODUCT_QTY_DELIMITER = ":"; // Separator for product:quantity in buy list
    public static final String MAP_RESULT_STORE_DELIMITER = ";;"; // Separator for store JSONs in MAP_RESULT
    public static final String SALES_DATA_SEPARATOR = "##NL##"; // Separator for multi-line sales data in SALES_RESULT
    public static final String SALES_REPORT_DELIMITER = ";;"; // Separator for store entries in CATEGORY/PRODTYPE results
    public static final String SALES_REPORT_FIELD_DELIMITER = ":"; // Separator for fields within a store entry (name:units:revenue)

    // Commands from Client/Manager to Master
    public static final String ADD_STORE = "ADD_STORE";         // Payload: storeJson
    public static final String ADD_PRODUCT = "ADD_PRODUCT";     // Payload: storeName|productJson
    public static final String REMOVE_PRODUCT = "REMOVE_PRODUCT"; // Payload: storeName|productName
    public static final String UPDATE_STOCK = "UPDATE_STOCK";   // Payload: storeName|productName|amountChange
    public static final String SEARCH = "SEARCH";             // Payload: lat|lon|radius|category|stars|price
    public static final String BUY = "BUY";                   // Payload: storeName|prod1:q1,prod2:q2,...
    public static final String RATE = "RATE";                 // Payload: storeName|ratingValue
    public static final String GET_SALES = "GET_SALES";         // Payload: storeName (Existing single store sales)
    public static final String GET_SALES_CATEGORY = "GET_SALES_CATEGORY"; // NEW Payload: foodCategory
    public static final String GET_SALES_PRODTYPE = "GET_SALES_PRODTYPE"; // NEW Payload: productType

    // Commands from Master to Worker
    public static final String M_ADD_STORE = "M_ADD_STORE";         // Payload: storeJson
    public static final String M_ADD_PRODUCT = "M_ADD_PRODUCT";     // Payload: storeName|productJson
    public static final String M_REMOVE_PRODUCT = "M_REMOVE_PRODUCT"; // Payload: storeName|productName
    public static final String M_UPDATE_STOCK = "M_UPDATE_STOCK";   // Payload: storeName|productName|amountChange
    public static final String M_BUY = "M_BUY";                   // Payload: buyId|storeName|prod1:q1,...
    public static final String M_RATE = "M_RATE";                 // Payload: storeName|ratingValue
    public static final String MAP_TASK = "MAP_TASK";             // Payload: mapId|lat|lon|radius|category|stars|price
    public static final String M_GET_SALES = "M_GET_SALES";         // Payload: requestId|storeName (Existing single store)
    public static final String M_GET_CATEGORY_SALES_TASK = "M_GET_CATEGORY_SALES_TASK"; // NEW Payload: requestId|foodCategory
    public static final String M_GET_PRODTYPE_SALES_TASK = "M_GET_PRODTYPE_SALES_TASK"; // NEW Payload: requestId|productType

    // Commands from Worker to Master
    public static final String MAP_RESULT = "MAP_RESULT";           // Payload: mapId|storeJson1;;storeJson2...
    public static final String BUY_SUCCESS = "BUY_SUCCESS";         // Payload: buyId|totalCost
    public static final String BUY_FAIL = "BUY_FAIL";             // Payload: buyId|reason
    public static final String RATE_SUCCESS = "RATE_SUCCESS";       // Payload: storeName|newAverageStars
    public static final String SALES_RESULT = "SALES_RESULT";       // Payload: requestId|formattedSalesDataString (For single store M_GET_SALES)
    public static final String CATEGORY_SALES_RESULT = "CATEGORY_SALES_RESULT"; // NEW Payload: requestId|store1:units:rev;;store2:units:rev...
    public static final String PRODTYPE_SALES_RESULT = "PRODTYPE_SALES_RESULT"; // NEW Payload: requestId|store1:units:rev;;store2:units:rev...
    public static final String GENERIC_ACK = "ACK";                 // Payload: [optional context]
    public static final String GENERIC_NACK = "NACK";               // Payload: [optional reason]

    // Commands from Master to Client/Manager
    public static final String SEARCH_RESULT = "SEARCH_RESULT";     // Payload: mapId|storeJson1;;storeJson2...
    public static final String SALES_REPORT = "SALES_REPORT";       // NEW Payload: type|filterValue|reportData (reportData uses SALES_REPORT_DELIMITER/FIELD_DELIMITER)
    public static final String ERROR = "ERROR";                     // Payload: errorMessage
}
