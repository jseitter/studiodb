package net.seitter.studiodb;

import net.seitter.studiodb.sql.SQLEngine;
import net.seitter.studiodb.web.DatabaseSystemInstrumenter;
import net.seitter.studiodb.web.WebServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

// JLine imports
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.reader.History;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

/**
 * Main class for StudioDB - an educational database system.
 */
public class StudioDB {
    private static final Logger logger = LoggerFactory.getLogger(StudioDB.class);
    private final DatabaseSystem dbSystem;
    private final SQLEngine sqlEngine;
    private WebServer webServer;
    private boolean visualizationEnabled = false;
    private static final String HISTORY_FILE = ".studiodb_history";
    private Terminal terminal;

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
        
        // Start the interactive SQL shell with JLine
        try {
            // Setup terminal
            terminal = TerminalBuilder.builder()
                    .name("StudioDB Terminal")
                    .system(true)
                    .build();
            
            // Set up history file in user home directory
            File historyFile = new File(System.getProperty("user.home"), HISTORY_FILE);
            if (!historyFile.exists()) {
                historyFile.createNewFile();
                logger.info("Created history file at: {}", historyFile.getAbsolutePath());
            }
            
            // Create history object
            History history = new DefaultHistory();
            
            // Configure line reader with history
            LineReader lineReader = LineReaderBuilder.builder()
                    .terminal(terminal)
                    .history(history)
                    .variable(LineReader.HISTORY_FILE, historyFile.getPath())
                    .build();
            
            // Welcome message
            terminal.writer().println("StudioDB SQL Shell");
            terminal.writer().println("Type SQL commands, or 'help' for assistance, or 'exit' to quit");
            terminal.writer().flush();
            
            String line;
            StringBuilder queryBuilder = new StringBuilder();
            String prompt = "SQL> ";
            
            while (true) {
                try {
                    // Read input with JLine (with history)
                    line = lineReader.readLine(prompt).trim();
                    
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
                            terminal.writer().println(result);
                            terminal.writer().flush();
                        } catch (Exception e) {
                            terminal.writer().println("Error: " + e.getMessage());
                            terminal.writer().flush();
                            logger.error("Query execution error", e);
                        }
                        queryBuilder = new StringBuilder();
                    } else {
                        queryBuilder.append(" ");
                    }
                } catch (UserInterruptException e) {
                    // Ctrl-C
                    queryBuilder = new StringBuilder();
                    terminal.writer().println("Query cancelled.");
                    terminal.writer().flush();
                } catch (EndOfFileException e) {
                    // Ctrl-D
                    break;
                }
            }
            
            // Save history
            try {
                ((DefaultHistory)history).save();
            } catch (IOException e) {
                logger.warn("Failed to save command history: {}", e.getMessage());
            }
            
        } catch (IOException e) {
            logger.error("Error in SQL shell", e);
        }
        
        shutdown();
    }

    /**
     * Displays help information about available commands.
     */
    private void displayHelp() {
        if (terminal == null) {
            logger.warn("Terminal not initialized when displaying help");
            return;
        }
        
        terminal.writer().println("\n=== StudioDB Help ===");
        terminal.writer().println("Available commands:");
        
        // Special Commands
        terminal.writer().println("\n-- Special Commands --");
        terminal.writer().println("help                           Display this help message");
        terminal.writer().println("exit                           Exit the SQL shell");
        
        // Tablespace Management
        terminal.writer().println("\n-- Tablespace Management --");
        terminal.writer().println("CREATE TABLESPACE name DATAFILE 'path' SIZE n PAGES;");
        terminal.writer().println("                                Create a new tablespace with specified size");
        terminal.writer().println("SHOW TABLESPACES;              List all tablespaces and their properties");
        
        // Table Management
        terminal.writer().println("\n-- Table Management --");
        terminal.writer().println("CREATE TABLE name (            Create a new table");
        terminal.writer().println("    column1 type1 [NOT NULL],");
        terminal.writer().println("    column2 type2 [NOT NULL],");
        terminal.writer().println("    ...");
        terminal.writer().println("    PRIMARY KEY (column1, ...)");
        terminal.writer().println(") IN TABLESPACE tablespace_name;");
        terminal.writer().println();
        terminal.writer().println("DROP TABLE name;               Drop an existing table");
        terminal.writer().println("SHOW TABLES;                   List all tables");
        
        // Index Management
        terminal.writer().println("\n-- Index Management --");
        terminal.writer().println("CREATE [UNIQUE] INDEX name ON table (column1, ...);");
        terminal.writer().println("                                Create a new index");
        terminal.writer().println("DROP INDEX name;               Drop an existing index");
        terminal.writer().println("SHOW INDEXES;                  List all indexes");
        
        // Data Manipulation
        terminal.writer().println("\n-- Data Manipulation --");
        terminal.writer().println("INSERT INTO table (col1, ...) VALUES (val1, ...);");
        terminal.writer().println("                                Insert data into a table");
        terminal.writer().println("SELECT col1, col2, ... FROM table [WHERE condition];");
        terminal.writer().println("                                Query data from a table");
        terminal.writer().println("UPDATE table SET col1 = val1, ... [WHERE condition];");
        terminal.writer().println("                                Update data in a table");
        terminal.writer().println("DELETE FROM table [WHERE condition];");
        terminal.writer().println("                                Delete data from a table");
        
        // System Catalog
        terminal.writer().println("\n-- System Catalog --");
        terminal.writer().println("SELECT * FROM SYS_TABLES;      View all tables in the system catalog");
        terminal.writer().println("SELECT * FROM SYS_COLUMNS;     View all columns in the system catalog");
        terminal.writer().println("SELECT * FROM SYS_INDEXES;     View all indexes in the system catalog");
        terminal.writer().println("SELECT * FROM SYS_INDEX_COLUMNS; View all index columns in the system catalog");
        
        // System Monitoring
        terminal.writer().println("\n-- System Monitoring --");
        terminal.writer().println("SHOW BUFFERPOOLS;              Display buffer pool usage information");
        terminal.writer().println("SHOW STATISTICS;               Show detailed page allocation and buffer pool stats");
        
        // Data Types
        terminal.writer().println("\n-- Supported Data Types --");
        terminal.writer().println("INTEGER                        32-bit integer");
        terminal.writer().println("FLOAT                          64-bit floating point");
        terminal.writer().println("VARCHAR(n)                     Variable-length string (max n characters)");
        terminal.writer().println("BOOLEAN                        True/false value");
        terminal.writer().println("DATE                           Date value (YYYY-MM-DD)");
        
        // Visualization
        if (visualizationEnabled) {
            terminal.writer().println("\n-- Visualization --");
            terminal.writer().println("Visualization interface available at: http://localhost:8080");
        }
        
        terminal.writer().println("\nNote: All SQL commands must end with a semicolon (;)");
        terminal.writer().println("Note: The system automatically persists all database objects in the system catalog");
        terminal.writer().println("Note: Command history is automatically saved to ~/.studiodb_history");
        terminal.writer().println("=====================\n");
        terminal.writer().flush();
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