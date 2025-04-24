package com.fooddelivery.common;

public class Protocol {
    public static final String DELIMITER = "|";
    public static final String PRODUCT_LIST_DELIMITER = ",";
    public static final String PRODUCT_QTY_DELIMITER = ":";
    public static final String MAP_RESULT_STORE_DELIMITER = ";;";
    // *** CHANGE SEPARATOR VALUE ***
    public static final String SALES_DATA_SEPARATOR = "##NL##"; // Custom separator for multi-line sales data

    // ... (Rest of the constants remain the same) ...
    // Commands from Client/Manager to Master
    public static final String ADD_STORE = "ADD_STORE";
    public static final String ADD_PRODUCT = "ADD_PRODUCT";
    public static final String REMOVE_PRODUCT = "REMOVE_PRODUCT";
    public static final String UPDATE_STOCK = "UPDATE_STOCK";
    public static final String SEARCH = "SEARCH";
    public static final String BUY = "BUY";
    public static final String RATE = "RATE";
    public static final String GET_SALES = "GET_SALES"; // Payload: storeName

    // Commands from Master to Worker
    public static final String M_ADD_STORE = "M_ADD_STORE";
    public static final String M_ADD_PRODUCT = "M_ADD_PRODUCT";
    public static final String M_REMOVE_PRODUCT = "M_REMOVE_PRODUCT";
    public static final String M_UPDATE_STOCK = "M_UPDATE_STOCK";
    public static final String M_BUY = "M_BUY";
    public static final String M_RATE = "M_RATE";
    public static final String MAP_TASK = "MAP_TASK";
    public static final String M_GET_SALES = "M_GET_SALES"; // Payload: requestId|storeName

    // Commands from Worker to Master
    public static final String MAP_RESULT = "MAP_RESULT";
    public static final String BUY_SUCCESS = "BUY_SUCCESS";
    public static final String BUY_FAIL = "BUY_FAIL";
    public static final String RATE_SUCCESS = "RATE_SUCCESS";
    public static final String SALES_RESULT = "SALES_RESULT"; // Payload: requestId|formattedSalesDataString
    public static final String GENERIC_ACK = "ACK";
    public static final String GENERIC_NACK = "NACK";

    // Commands from Master to Client/Manager
    public static final String SEARCH_RESULT = "SEARCH_RESULT";
    public static final String ERROR = "ERROR";
}