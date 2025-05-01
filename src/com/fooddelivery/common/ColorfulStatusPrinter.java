package com.fooddelivery.common;

// Utility for printing colorful status messages to the console using ANSI escape codes.
public class ColorfulStatusPrinter {

    // ANSI escape codes for colors
    public static final String RESET = "\u001B[0m";     // Resets text formatting
    public static final String GREEN = "\u001B[32m";    // Green text
    public static final String RED = "\u001B[31m";      // Red text
    public static final String ORANGE = "\u001B[38;5;208m"; // 256-color mode orange
    public static final String WHITE = "\u001B[37m";     // White text
    public static final String BLUE = "\u001B[34m";     // Blue text
    public static final String YELLOW = "\u001B[33m";   // Yellow text (Alternative for Orange if 256 color doesn't work)

    // Simple test method
    public static void main(String[] args) {
        printStatus("+", "Success message example.", GREEN);
        printStatus("-", "Error message example.", RED);
        printStatus("?", "Warning or query example.", ORANGE); // Use ORANGE for warnings/debug
        printStatus("✓", "Operation successful.", GREEN);
        printStatus("X", "Operation failed.", RED);
        printStatus("i", "Informational message.", WHITE); // Use WHITE for standard info
        printStatus("...", "Processing step.", BLUE);     // Use BLUE for process steps
    }

    /**
     * Prints a status message with a colored symbol.
     * @param symbol The symbol to display (e.g., "+", "-", "?", "✓", "X", "i", "...").
     * @param message The message text.
     * @param color The ANSI color code (e.g., GREEN, RED, ORANGE, WHITE, BLUE).
     */
    public static void printStatus(String symbol, String message, String color) {
        // Using System.out for all status messages for consistent ordering,
        // even for errors, but coloring them red.
        System.out.println("[" + color + symbol + RESET + "] " + message);
    }

    /**
     * Prints a debug message only if verbose mode is enabled. Uses ORANGE color.
     * @param verbose Flag indicating if verbose mode is on.
     * @param message The debug message.
     */
    public static void printDebug(boolean verbose, String message) {
        if (verbose) {
            printStatus("DEBUG", message, ORANGE); // Use ORANGE for debug
        }
    }

     /**
     * Prints an error message using RED color.
     * @param message The error message.
     */
    public static void printError(String message) {
        printStatus("!", message, RED); // Using "!" symbol for errors
    }

     /**
     * Prints a success message using GREEN color.
     * @param message The success message.
     */
    public static void printSuccess(String message) {
        printStatus("✓", message, GREEN);
    }

    /**
     * Prints an informational message using WHITE color.
     * @param message The info message.
     */
    public static void printInfo(String message) {
        printStatus("i", message, WHITE);
    }

     /**
     * Prints a general log message (e.g., connection accepted) using BLUE color.
     * @param message The log message.
     */
    public static void printLog(String message) {
        printStatus(">", message, BLUE);
    }


}
