package net.seitter.studiodb;

import net.seitter.studiodb.sql.SQLEngine;
import net.seitter.studiodb.web.DatabaseSystemInstrumenter;
import net.seitter.studiodb.web.WebServer;
import net.seitter.studiodb.schema.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

// JLine imports
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.reader.History;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

// Add these imports for autocompletion
import org.jline.reader.Completer;
import org.jline.reader.impl.completer.StringsCompleter;
import org.jline.reader.impl.completer.ArgumentCompleter;
import org.jline.reader.impl.completer.NullCompleter;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

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
    
    // SQL keywords for autocompletion
    private static final List<String> SQL_KEYWORDS = Arrays.asList(
        "select", "from", "where", "and", "or", "insert", "into", "values",
        "update", "set", "delete", "create", "drop", "table", "index", "tablespace",
        "primary", "key", "integer", "varchar", "boolean", "float", "date", "not", "null",
        "unique", "on", "show", "tables", "tablespaces", "indexes", "datafile", "size", "pages",
        "in"
    );
    
    // Special commands
    private static final List<String> SPECIAL_COMMANDS = Arrays.asList(
        "help", "exit"
    );

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
            
            // Set up autocompletion
            Completer completer = createCompleter();
            
            // Configure line reader with history and autocompletion
            LineReader lineReader = LineReaderBuilder.builder()
                    .terminal(terminal)
                    .history(history)
                    .variable(LineReader.HISTORY_FILE, historyFile.getPath())
                    .completer(completer)
                    .option(LineReader.Option.CASE_INSENSITIVE, true) // Make completion case insensitive
                    .option(LineReader.Option.AUTO_MENU, true) // Show completions automatically
                    .option(LineReader.Option.AUTO_LIST, true) // Automatically list options for tab
                    .build();
            
            // Welcome message
            terminal.writer().println("StudioDB SQL Shell");
            terminal.writer().println("Type SQL commands, or 'help' for assistance, or 'exit' to quit");
            terminal.writer().println("Use TAB for autocompletion");
            terminal.writer().flush();
            
            String line;
            StringBuilder queryBuilder = new StringBuilder();
            String prompt = "SQL> ";
            
            while (true) {
                try {
                    // Read input with JLine (with history and autocompletion)
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
        terminal.writer().println("SELECT * FROM SYS_TABLESPACES; View all tablespaces in the system catalog");
        terminal.writer().println("SELECT * FROM SYS_TABLES;      View all tables in the system catalog");
        terminal.writer().println("SELECT * FROM SYS_COLUMNS;     View all columns in the system catalog");
        terminal.writer().println("SELECT * FROM SYS_INDEXES;     View all indexes in the system catalog");
        terminal.writer().println("SELECT * FROM SYS_INDEX_COLUMNS; View all index columns in the system catalog");
        
        // System Monitoring
        terminal.writer().println("\n-- System Monitoring --");
        terminal.writer().println("SHOW BUFFERPOOLS;              Display buffer pool usage information");
        terminal.writer().println("SHOW STATISTICS;               Show detailed page allocation and buffer pool stats");
        terminal.writer().println("SHOW PAGES IN TABLESPACE name; Display detailed page layout for a specific tablespace");
        terminal.writer().println("SHOW PINCOUNT;                 Show pin count for each page in the buffer pool");
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

    /**
     * Creates a case-insensitive completer for SQL commands and database objects.
     *
     * @return The configured completer
     */
    private Completer createCompleter() {
        List<Completer> completers = new ArrayList<>();
        
        // Add database object completers (these will update dynamically)
        completers.add(createDatabaseObjectsCompleter());
        
        // Add SQL syntax completers
        completers.add(createSqlSyntaxCompleter());
        
        // Add special commands completer
        completers.add(new StringsCompleter(SPECIAL_COMMANDS));
        
        // Combine all completers
        return new ArgumentCompleter(completers);
    }
    
    /**
     * Creates a completer for database objects (tables, columns, etc.).
     *
     * @return The database objects completer
     */
    private Completer createDatabaseObjectsCompleter() {
        // This implements a dynamic approach to completion that will be updated whenever tables are accessed
        return new Completer() {
            @Override
            public void complete(org.jline.reader.LineReader reader, org.jline.reader.ParsedLine line, List<org.jline.reader.Candidate> candidates) {
                String buffer = line.line();
                String word = line.word().toLowerCase();
                
                // Get updated list of database objects each time
                List<String> databaseObjects = getDatabaseObjects(buffer);
                
                // Add candidates that match the current word (case insensitive)
                for (String obj : databaseObjects) {
                    if (obj.toLowerCase().startsWith(word)) {
                        candidates.add(new org.jline.reader.Candidate(obj, obj, null, null, null, null, true));
                    }
                }
            }
        };
    }
    
    /**
     * Gets all database objects that might be relevant in the current context.
     * 
     * @param buffer The current input line
     * @return A list of database objects
     */
    private List<String> getDatabaseObjects(String buffer) {
        List<String> objects = new ArrayList<>();
        
        // Get all tablespace names
        // TODO: Implement once getDatabaseObjects is available
        
        // Get all table names
        if (dbSystem.getSchemaManager() != null) {
            dbSystem.getSchemaManager().getAllTables().forEach(table -> {
                // Add simple table name
                objects.add(table.getName());
                
                // If we're in a context where columns make sense (e.g., after SELECT or WHERE)
                if (isColumnContextRelevant(buffer)) {
                    // Add column names for this table
                    table.getColumns().forEach(column -> {
                        // Add both simple column name and qualified column name
                        objects.add(column.getName());
                        objects.add(table.getName() + "." + column.getName());
                    });
                }
            });
        }
        
        // Add system catalog tables
        objects.add("SYS_TABLESPACES");
        objects.add("SYS_TABLES");
        objects.add("SYS_COLUMNS");
        objects.add("SYS_INDEXES");
        objects.add("SYS_INDEX_COLUMNS");
        
        return objects;
    }
    
    /**
     * Determines if the current context suggests column names would be relevant.
     * 
     * @param buffer The current input line
     * @return true if columns would be relevant in this context
     */
    private boolean isColumnContextRelevant(String buffer) {
        String lowerBuffer = buffer.toLowerCase();
        
        // Columns are relevant after SELECT, WHERE, ORDER BY, GROUP BY, etc.
        return lowerBuffer.contains("select") || 
               lowerBuffer.contains("where") ||
               lowerBuffer.contains("order by") ||
               lowerBuffer.contains("group by") ||
               lowerBuffer.contains("having") ||
               lowerBuffer.contains("set") ||
               lowerBuffer.contains("on") ||
               (lowerBuffer.contains("insert") && lowerBuffer.contains("into"));
    }
    
    /**
     * Gets column suggestions for a specific table.
     * 
     * @param tableName The name of the table
     * @return A list of column names for the table
     */
    private List<String> getColumnSuggestionsForTable(String tableName) {
        List<String> columns = new ArrayList<>();
        
        if (dbSystem.getSchemaManager() != null) {
            Table table = dbSystem.getSchemaManager().getTable(tableName);
            if (table != null) {
                table.getColumns().forEach(column -> columns.add(column.getName()));
            }
        }
        
        return columns;
    }
    
    /**
     * Creates a completer for SQL syntax.
     *
     * @return The SQL syntax completer
     */
    private Completer createSqlSyntaxCompleter() {
        // Use a dynamic completer instead of static patterns for SQL syntax
        return new Completer() {
            @Override
            public void complete(org.jline.reader.LineReader reader, org.jline.reader.ParsedLine line, List<org.jline.reader.Candidate> candidates) {
                String buffer = line.line().toLowerCase();
                String word = line.word().toLowerCase();
                
                // Add customized SQL completions based on context
                List<String> suggestions = getSqlSuggestions(buffer);
                
                // Filter suggestions by current word (case insensitive)
                for (String suggestion : suggestions) {
                    if (suggestion.toLowerCase().startsWith(word)) {
                        candidates.add(new org.jline.reader.Candidate(suggestion, suggestion, null, null, null, null, true));
                    }
                }
            }
        };
    }
    
    /**
     * Gets SQL suggestions based on the current input context.
     * 
     * @param buffer The current input line
     * @return A list of SQL suggestions
     */
    private List<String> getSqlSuggestions(String buffer) {
        List<String> suggestions = new ArrayList<>();
        
        // For an empty or new input, suggest top-level commands
        if (buffer.trim().isEmpty()) {
            suggestions.addAll(Arrays.asList(
                "SELECT", "INSERT", "UPDATE", "DELETE", 
                "CREATE TABLE", "CREATE INDEX", "CREATE TABLESPACE",
                "DROP TABLE", "DROP INDEX",
                "SHOW TABLES", "SHOW INDEXES", "SHOW TABLESPACES", "SHOW BUFFERPOOLS",
                "SHOW STATISTICS", "SHOW PAGES"
            ));
            return suggestions;
        }
        
        // Context: SHOW PAGES - suggest IN TABLESPACE
        if (buffer.matches("(?i).*show\\s+pages\\s*$")) {
            suggestions.add("IN TABLESPACE");
            return suggestions;
        }
        
        // Context: SHOW PAGES IN TABLESPACE - need tablespace name
        if (buffer.matches("(?i).*show\\s+pages\\s+in\\s+tablespace\\s*$")) {
            // After SHOW PAGES IN TABLESPACE, expecting a tablespace name
            // Get tablespace names from the system
            if (dbSystem.getStorageManager() != null) {
                try {
                    java.lang.reflect.Field tablespacesField = dbSystem.getStorageManager().getClass().getDeclaredField("tablespaces");
                    tablespacesField.setAccessible(true);
                    Map<String, Object> tablespaces = (Map<String, Object>) tablespacesField.get(dbSystem.getStorageManager());
                    suggestions.addAll(tablespaces.keySet());
                } catch (Exception e) {
                    logger.warn("Failed to get tablespace names for autocompletion", e);
                }
            }
            return suggestions;
        }
        
        // Context: CREATE TABLE - first part
        if (buffer.contains("create") && !buffer.contains("table") && 
            !buffer.contains("index") && !buffer.contains("tablespace")) {
            suggestions.addAll(Arrays.asList("TABLE", "INDEX", "TABLESPACE", "UNIQUE INDEX"));
            return suggestions;
        }
        
        // Context: CREATE TABLE - need table name
        if (buffer.matches("(?i).*create\\s+table\\s*$")) {
            // After CREATE TABLE, expecting a table name
            return suggestions; // Return empty, as table name is user-provided
        }
        
        // Context: CREATE TABLE - need opening parenthesis
        if (buffer.matches("(?i).*create\\s+table\\s+\\w+\\s*$")) {
            suggestions.add("(");
            return suggestions;
        }
        
        // Context: CREATE TABLE - need opening parenthesis
        if (buffer.matches("(?i).*create\\s+table\\s+\\w+\\s*\\(.*") && !buffer.contains(")")) {
            // Inside column definitions
            if (buffer.matches("(?i).*,\\s*$") || buffer.matches("(?i).*\\(\\s*$")) {
                // After comma or just after opening bracket, expecting column name
                return suggestions; // Return empty, as column name is user-provided
            }
            
            // After column name, expecting data type
            if (buffer.matches("(?i).*\\w+\\s+$")) {
                suggestions.addAll(Arrays.asList(
                    "INTEGER", "VARCHAR(", "BOOLEAN", "FLOAT", "DATE"
                ));
                return suggestions;
            }
            
            // After data type, expecting constraint or comma
            if (buffer.matches("(?i).*(integer|boolean|float|date)\\s*$") || 
                buffer.matches("(?i).*varchar\\s*\\(\\d+\\)\\s*$")) {
                suggestions.addAll(Arrays.asList(
                    "NOT NULL", "PRIMARY KEY", ","
                ));
                return suggestions;
            }
            
            // After NOT NULL, expecting comma or other constraint
            if (buffer.matches("(?i).*not\\s+null\\s*$")) {
                suggestions.addAll(Arrays.asList(
                    "PRIMARY KEY", ","
                ));
                return suggestions;
            }
            
            // For PRIMARY KEY definition
            if (buffer.matches("(?i).*,\\s*primary\\s+key\\s*$")) {
                suggestions.add("(");
                return suggestions;
            }
            
            // After last column or PRIMARY KEY definition
            if (buffer.matches("(?i).*primary\\s+key\\s*\\([^)]*\\)\\s*$") || 
                buffer.matches("(?i).*,\\s*[^,]*\\s*$")) {
                suggestions.add(")");
                return suggestions;
            }
        }
        
        // Context: After CREATE TABLE definition
        if (buffer.matches("(?i).*create\\s+table\\s+\\w+\\s*\\(.*\\)\\s*$")) {
            suggestions.addAll(Arrays.asList(
                "IN TABLESPACE", ";"
            ));
            return suggestions;
        }
        
        // Context: After IN TABLESPACE
        if (buffer.matches("(?i).*\\)\\s*in\\s+tablespace\\s*$")) {
            // Return empty, as tablespace name is user-provided
            return suggestions;
        }
        
        // Context: After tablespace name in CREATE TABLE
        if (buffer.matches("(?i).*\\)\\s*in\\s+tablespace\\s+\\w+\\s*$")) {
            suggestions.add(";");
            return suggestions;
        }
        
        // Context: CREATE INDEX - first part
        if (buffer.matches("(?i).*create\\s+(unique\\s+)?index\\s*$")) {
            // After CREATE INDEX, expecting an index name
            return suggestions; // Return empty, as index name is user-provided
        }
        
        // Context: CREATE INDEX - need ON
        if (buffer.matches("(?i).*create\\s+(unique\\s+)?index\\s+\\w+\\s*$")) {
            suggestions.add("ON");
            return suggestions;
        }
        
        // Context: CREATE INDEX - need table name
        if (buffer.matches("(?i).*create\\s+(unique\\s+)?index\\s+\\w+\\s+on\\s*$")) {
            // After ON, expecting a table name
            return suggestions; // Return empty, as table name is user-provided
        }
        
        // Context: CREATE INDEX - need opening parenthesis
        if (buffer.matches("(?i).*create\\s+(unique\\s+)?index\\s+\\w+\\s+on\\s+\\w+\\s*$")) {
            suggestions.add("(");
            return suggestions;
        }
        
        // Context: Inside CREATE INDEX column list
        if (buffer.matches("(?i).*create\\s+(unique\\s+)?index\\s+\\w+\\s+on\\s+\\w+\\s*\\(.*") && !buffer.endsWith(")")) {
            if (buffer.matches("(?i).*\\(\\s*$") || buffer.matches("(?i).*,\\s*$")) {
                // Column names will be provided by database objects completer
                return suggestions;
            }
            
            if (buffer.matches("(?i).*\\(\\s*\\w+\\s*$")) {
                suggestions.addAll(Arrays.asList(",", ")"));
                return suggestions;
            }
        }
        
        // Context: After CREATE INDEX column list
        if (buffer.matches("(?i).*create\\s+(unique\\s+)?index\\s+\\w+\\s+on\\s+\\w+\\s*\\(.*\\)\\s*$")) {
            suggestions.add(";");
            return suggestions;
        }
        
        // Context: CREATE TABLESPACE - first part
        if (buffer.matches("(?i).*create\\s+tablespace\\s*$")) {
            // After CREATE TABLESPACE, expecting a tablespace name
            return suggestions; // Return empty, as tablespace name is user-provided
        }
        
        // Context: CREATE TABLESPACE - need DATAFILE
        if (buffer.matches("(?i).*create\\s+tablespace\\s+\\w+\\s*$")) {
            suggestions.add("DATAFILE");
            return suggestions;
        }
        
        // Context: CREATE TABLESPACE - need file path
        if (buffer.matches("(?i).*create\\s+tablespace\\s+\\w+\\s+datafile\\s*$")) {
            // Suggest using quotes for file path
            suggestions.add("'");
            return suggestions;
        }
        
        // Context: CREATE TABLESPACE - after file path, need SIZE
        if (buffer.matches("(?i).*datafile\\s+'[^']*'\\s*$")) {
            suggestions.add("SIZE");
            return suggestions;
        }
        
        // Context: CREATE TABLESPACE - after SIZE, need number
        if (buffer.matches("(?i).*datafile\\s+'[^']*'\\s+size\\s*$")) {
            // Return empty, as size is user-provided
            return suggestions;
        }
        
        // Context: CREATE TABLESPACE - after size number, need PAGES
        if (buffer.matches("(?i).*size\\s+\\d+\\s*$")) {
            suggestions.add("PAGES");
            return suggestions;
        }
        
        // Context: CREATE TABLESPACE - after PAGES, need semicolon
        if (buffer.matches("(?i).*size\\s+\\d+\\s+pages\\s*$")) {
            suggestions.add(";");
            return suggestions;
        }
        
        // Context: After SELECT
        if (buffer.contains("select") && !buffer.contains("from")) {
            suggestions.add("*");
            suggestions.add("FROM");
            // Also add column names (handled by database objects completer)
            return suggestions;
        }
        
        // Context: After FROM
        if (buffer.contains("from") && !buffer.contains("where") && !buffer.contains(";")) {
            suggestions.add("WHERE");
            // Table names are handled by database objects completer
            return suggestions;
        }
        
        // Context: After WHERE
        if (buffer.contains("where") && !buffer.contains(";")) {
            suggestions.addAll(Arrays.asList("AND", "OR", "=", ">", "<", ">=", "<=", "<>"));
            // Column names are handled by database objects completer
            return suggestions;
        }
        
        // Context: INSERT INTO
        if (buffer.contains("insert into") && !buffer.contains("values")) {
            if (!buffer.contains("(")) {
                suggestions.add("(");
            } else if (buffer.contains("(") && !buffer.contains(")")) {
                suggestions.add(")");
            } else if (buffer.contains(")")) {
                suggestions.add("VALUES");
            }
            // Table and column names are handled by database objects completer
            return suggestions;
        }
        
        // Context: After VALUES
        if (buffer.contains("values") && buffer.contains("insert")) {
            if (!buffer.substring(buffer.lastIndexOf("values")).contains("(")) {
                suggestions.add("(");
            } else if (buffer.lastIndexOf("(") > buffer.lastIndexOf(")")) {
                suggestions.add(")");
            } else {
                suggestions.add(";");
            }
            return suggestions;
        }
        
        // Default: Add all SQL keywords
        suggestions.addAll(SQL_KEYWORDS);
        return suggestions;
    }

    public static void main(String[] args) {
        // Check if we should enable visualization
        boolean enableVisualization = Arrays.asList(args).contains("--viz");
        
        StudioDB studioDB = new StudioDB(enableVisualization);
        studioDB.start();
    }
} 