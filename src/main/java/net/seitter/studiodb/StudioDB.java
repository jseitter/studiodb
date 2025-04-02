package net.seitter.studiodb;

import net.seitter.studiodb.sql.SQLEngine;
import net.seitter.studiodb.web.DatabaseSystemInstrumenter;
import net.seitter.studiodb.web.WebServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;

/**
 * Main class for StudioDB - an educational database system.
 */
public class StudioDB {
    private static final Logger logger = LoggerFactory.getLogger(StudioDB.class);
    private final DatabaseSystem dbSystem;
    private final SQLEngine sqlEngine;
    private WebServer webServer;
    private boolean visualizationEnabled = false;

    public StudioDB(boolean enableVisualization) {
        logger.info("Initializing StudioDB...");
        this.dbSystem = new DatabaseSystem();
        this.sqlEngine = new SQLEngine(dbSystem);
        this.visualizationEnabled = enableVisualization;
        
        if (visualizationEnabled) {
            initializeVisualization();
        }
        
        logger.info("StudioDB initialized successfully");
    }

    /**
     * Initializes the visualization components.
     */
    private void initializeVisualization() {
        try {
            // Initialize web server
            this.webServer = new WebServer(dbSystem, 8080);
            logger.info("Visualization web server initialized");
            
            // Instrument database system components
            DatabaseSystemInstrumenter instrumenter = new DatabaseSystemInstrumenter(dbSystem, webServer);
            instrumenter.instrumentComponents();
            logger.info("Database components instrumented for visualization");
        } catch (Exception e) {
            logger.error("Failed to initialize visualization components", e);
        }
    }

    public void start() {
        logger.info("Starting StudioDB...");
        
        // Start the web server if visualization is enabled
        if (visualizationEnabled && webServer != null) {
            webServer.start();
            logger.info("Visualization interface started at http://localhost:8080");
        }
        
        // Start the interactive SQL shell
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
            System.out.println("StudioDB SQL Shell");
            System.out.println("Type SQL commands, or 'help' for assistance, or 'exit' to quit");
            
            String line;
            StringBuilder queryBuilder = new StringBuilder();
            
            while (true) {
                System.out.print("SQL> ");
                line = reader.readLine().trim();
                
                if (line.equalsIgnoreCase("exit")) {
                    break;
                }
                
                if (line.equalsIgnoreCase("help")) {
                    displayHelp();
                    continue;
                }
                
                queryBuilder.append(line);
                
                if (line.endsWith(";")) {
                    try {
                        String result = sqlEngine.executeQuery(queryBuilder.toString());
                        System.out.println(result);
                    } catch (Exception e) {
                        System.out.println("Error: " + e.getMessage());
                        logger.error("Query execution error", e);
                    }
                    queryBuilder = new StringBuilder();
                } else {
                    queryBuilder.append(" ");
                }
            }
        } catch (Exception e) {
            logger.error("Error in SQL shell", e);
        }
        
        shutdown();
    }

    /**
     * Displays help information about available commands.
     */
    private void displayHelp() {
        System.out.println("\n=== StudioDB Help ===");
        System.out.println("Available commands:");
        
        // Special Commands
        System.out.println("\n-- Special Commands --");
        System.out.println("help                           Display this help message");
        System.out.println("exit                           Exit the SQL shell");
        
        // Tablespace Management
        System.out.println("\n-- Tablespace Management --");
        System.out.println("CREATE TABLESPACE name DATAFILE 'path' SIZE n PAGES;");
        System.out.println("                                Create a new tablespace with specified size");
        System.out.println("SHOW TABLESPACES;              List all tablespaces and their properties");
        
        // Table Management
        System.out.println("\n-- Table Management --");
        System.out.println("CREATE TABLE name (            Create a new table");
        System.out.println("    column1 type1 [NOT NULL],");
        System.out.println("    column2 type2 [NOT NULL],");
        System.out.println("    ...");
        System.out.println("    PRIMARY KEY (column1, ...)");
        System.out.println(") IN TABLESPACE tablespace_name;");
        System.out.println();
        System.out.println("DROP TABLE name;               Drop an existing table");
        System.out.println("SHOW TABLES;                   List all tables");
        
        // Index Management
        System.out.println("\n-- Index Management --");
        System.out.println("CREATE [UNIQUE] INDEX name ON table (column1, ...);");
        System.out.println("                                Create a new index");
        System.out.println("DROP INDEX name;               Drop an existing index");
        System.out.println("SHOW INDEXES;                  List all indexes");
        
        // Data Manipulation
        System.out.println("\n-- Data Manipulation --");
        System.out.println("INSERT INTO table (col1, ...) VALUES (val1, ...);");
        System.out.println("                                Insert data into a table");
        System.out.println("SELECT col1, col2, ... FROM table [WHERE condition];");
        System.out.println("                                Query data from a table");
        System.out.println("UPDATE table SET col1 = val1, ... [WHERE condition];");
        System.out.println("                                Update data in a table");
        System.out.println("DELETE FROM table [WHERE condition];");
        System.out.println("                                Delete data from a table");
        
        // System Catalog
        System.out.println("\n-- System Catalog --");
        System.out.println("SELECT * FROM SYS_TABLES;      View all tables in the system catalog");
        System.out.println("SELECT * FROM SYS_COLUMNS;     View all columns in the system catalog");
        System.out.println("SELECT * FROM SYS_INDEXES;     View all indexes in the system catalog");
        System.out.println("SELECT * FROM SYS_INDEX_COLUMNS; View all index columns in the system catalog");
        
        // System Monitoring
        System.out.println("\n-- System Monitoring --");
        System.out.println("SHOW BUFFERPOOLS;              Display buffer pool usage information");
        System.out.println("SHOW STATISTICS;               Show detailed page allocation and buffer pool stats");
        
        // Data Types
        System.out.println("\n-- Supported Data Types --");
        System.out.println("INTEGER                        32-bit integer");
        System.out.println("FLOAT                          64-bit floating point");
        System.out.println("VARCHAR(n)                     Variable-length string (max n characters)");
        System.out.println("BOOLEAN                        True/false value");
        System.out.println("DATE                           Date value (YYYY-MM-DD)");
        
        // Visualization
        if (visualizationEnabled) {
            System.out.println("\n-- Visualization --");
            System.out.println("Visualization interface available at: http://localhost:8080");
        }
        
        System.out.println("\nNote: All SQL commands must end with a semicolon (;)");
        System.out.println("Note: The system automatically persists all database objects in the system catalog");
        System.out.println("=====================\n");
    }

    public void shutdown() {
        logger.info("Shutting down StudioDB...");
        
        // Stop the web server if it's running
        if (webServer != null) {
            webServer.stop();
            logger.info("Visualization interface stopped");
        }
        
        dbSystem.shutdown();
        logger.info("StudioDB shutdown complete");
    }

    public static void main(String[] args) {
        // Check if we should enable visualization
        boolean enableVisualization = Arrays.asList(args).contains("--viz");
        
        StudioDB studioDB = new StudioDB(enableVisualization);
        studioDB.start();
    }
} 