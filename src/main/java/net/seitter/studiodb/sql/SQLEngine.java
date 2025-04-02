package net.seitter.studiodb.sql;

import net.seitter.studiodb.DatabaseSystem;
import net.seitter.studiodb.buffer.BufferPoolManager;
import net.seitter.studiodb.schema.Column;
import net.seitter.studiodb.schema.DataType;
import net.seitter.studiodb.schema.Index;
import net.seitter.studiodb.schema.SchemaManager;
import net.seitter.studiodb.schema.Table;
import net.seitter.studiodb.storage.Page;
import net.seitter.studiodb.storage.PageId;
import net.seitter.studiodb.storage.StorageManager;
import net.seitter.studiodb.storage.Tablespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses and executes SQL statements.
 */
public class SQLEngine {
    private static final Logger logger = LoggerFactory.getLogger(SQLEngine.class);
    
    private final DatabaseSystem dbSystem;
    
    /**
     * Creates a new SQL engine.
     *
     * @param dbSystem The database system
     */
    public SQLEngine(DatabaseSystem dbSystem) {
        this.dbSystem = dbSystem;
    }
    
    /**
     * Executes an SQL query.
     *
     * @param query The SQL query
     * @return The result of the query
     */
    public String executeQuery(String query) {
        logger.debug("Executing query: {}", query);
        
        // Trim whitespace and remove trailing semicolon
        query = query.trim();
        if (query.endsWith(";")) {
            query = query.substring(0, query.length() - 1).trim();
        }
        
        // Split into command and rest
        String[] parts = query.split("\\s+", 2);
        String command = parts[0].toUpperCase();
        String rest = parts.length > 1 ? parts[1] : "";
        
        try {
            switch (command) {
                case "CREATE":
                    return executeCreate(rest);
                case "DROP":
                    return executeDrop(rest);
                case "INSERT":
                    return executeInsert(rest);
                case "SELECT":
                    return executeSelect(rest);
                case "UPDATE":
                    return executeUpdate(rest);
                case "DELETE":
                    return executeDelete(rest);
                case "SHOW":
                    return executeShow(rest);
                default:
                    return "Unknown command: " + command;
            }
        } catch (Exception e) {
            logger.error("Error executing query", e);
            return "Error: " + e.getMessage();
        }
    }
    
    /**
     * Executes a CREATE statement.
     *
     * @param rest The rest of the statement
     * @return The result of the statement
     */
    private String executeCreate(String rest) {
        // Split into type and rest
        String[] parts = rest.split("\\s+", 2);
        String type = parts[0].toUpperCase();
        String restOfCreate = parts.length > 1 ? parts[1] : "";
        
        switch (type) {
            case "TABLESPACE":
                return executeCreateTablespace(restOfCreate);
            case "TABLE":
                return executeCreateTable(restOfCreate);
            case "INDEX":
                return executeCreateIndex(restOfCreate);
            default:
                return "Unknown CREATE type: " + type;
        }
    }
    
    /**
     * Executes a CREATE TABLESPACE statement.
     *
     * @param rest The rest of the statement
     * @return The result of the statement
     */
    private String executeCreateTablespace(String rest) {
        // Parse: CREATE TABLESPACE name DATAFILE 'file_path' SIZE number pages
        Pattern pattern = Pattern.compile("(\\w+)\\s+DATAFILE\\s+'([^']+)'\\s+SIZE\\s+(\\d+)\\s+PAGES", 
                                        Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(rest);
        
        if (!matcher.matches()) {
            return "Invalid CREATE TABLESPACE syntax. Expected: CREATE TABLESPACE name DATAFILE 'file_path' SIZE number PAGES";
        }
        
        String tablespaceName = matcher.group(1);
        String filePath = matcher.group(2);
        int size = Integer.parseInt(matcher.group(3));
        
        boolean created = dbSystem.createTablespace(tablespaceName, filePath, size);
        
        if (created) {
            return "Tablespace '" + tablespaceName + "' created successfully";
        } else {
            return "Failed to create tablespace '" + tablespaceName + "'";
        }
    }
    
    /**
     * Executes a CREATE TABLE statement.
     *
     * @param rest The rest of the statement
     * @return The result of the statement
     */
    private String executeCreateTable(String rest) {
        // Parse: CREATE TABLE name (col1 type1, col2 type2, ...) IN TABLESPACE ts_name
        Pattern tablePattern = Pattern.compile("(\\w+)\\s+\\((.+)\\)(?:\\s+IN\\s+TABLESPACE\\s+(\\w+))?", 
                                             Pattern.CASE_INSENSITIVE);
        Matcher tableMatcher = tablePattern.matcher(rest);
        
        if (!tableMatcher.matches()) {
            return "Invalid CREATE TABLE syntax. Expected: CREATE TABLE name (col1 type1, col2 type2, ...) IN TABLESPACE ts_name";
        }
        
        String tableName = tableMatcher.group(1);
        String columnsStr = tableMatcher.group(2);
        String tablespaceName = tableMatcher.group(3);
        
        if (tablespaceName == null) {
            tablespaceName = "DEFAULT"; // Use default tablespace if not specified
        }
        
        // Parse columns
        List<Column> columns = new ArrayList<>();
        List<String> primaryKeyColumns = new ArrayList<>();
        
        String[] columnDefs = columnsStr.split(",(?=(?:[^']*'[^']*')*[^']*$)");
        
        for (String columnDef : columnDefs) {
            columnDef = columnDef.trim();
            
            // Check for PRIMARY KEY constraint
            if (columnDef.toUpperCase().startsWith("PRIMARY KEY")) {
                Pattern pkPattern = Pattern.compile("PRIMARY\\s+KEY\\s+\\((.+)\\)", 
                                                  Pattern.CASE_INSENSITIVE);
                Matcher pkMatcher = pkPattern.matcher(columnDef);
                
                if (pkMatcher.find()) {
                    String pkColumnsStr = pkMatcher.group(1);
                    String[] pkColumns = pkColumnsStr.split(",");
                    
                    for (String pkColumn : pkColumns) {
                        primaryKeyColumns.add(pkColumn.trim());
                    }
                }
                
                continue;
            }
            
            // Parse column definition
            Pattern colPattern = Pattern.compile("(\\w+)\\s+(\\w+(?:\\(\\d+\\))?)(?:\\s+(NOT\\s+NULL))?", 
                                               Pattern.CASE_INSENSITIVE);
            Matcher colMatcher = colPattern.matcher(columnDef);
            
            if (!colMatcher.matches()) {
                return "Invalid column definition: " + columnDef;
            }
            
            String columnName = colMatcher.group(1);
            String dataTypeStr = colMatcher.group(2);
            boolean notNull = colMatcher.group(3) != null;
            
            // Parse data type and length
            DataType dataType;
            int maxLength = 0;
            
            if (dataTypeStr.toUpperCase().startsWith("VARCHAR")) {
                dataType = DataType.VARCHAR;
                Pattern lengthPattern = Pattern.compile("VARCHAR\\(?(\\d+)\\)?", 
                                                      Pattern.CASE_INSENSITIVE);
                Matcher lengthMatcher = lengthPattern.matcher(dataTypeStr);
                
                if (lengthMatcher.matches()) {
                    maxLength = Integer.parseInt(lengthMatcher.group(1));
                } else {
                    maxLength = 255; // Default
                }
            } else {
                dataType = DataType.fromSqlType(dataTypeStr);
                
                if (dataType == null) {
                    return "Unknown data type: " + dataTypeStr;
                }
            }
            
            columns.add(new Column(columnName, dataType, !notNull, maxLength));
        }
        
        // Create the table
        SchemaManager schemaManager = dbSystem.getSchemaManager();
        Table table = schemaManager.createTable(tableName, tablespaceName, columns, primaryKeyColumns);
        
        if (table != null) {
            return "Table '" + tableName + "' created successfully";
        } else {
            return "Failed to create table '" + tableName + "'";
        }
    }
    
    /**
     * Executes a CREATE INDEX statement.
     *
     * @param rest The rest of the statement
     * @return The result of the statement
     */
    private String executeCreateIndex(String rest) {
        // Parse: CREATE [UNIQUE] INDEX name ON table (col1, col2, ...)
        Pattern pattern = Pattern.compile("(UNIQUE\\s+)?INDEX\\s+(\\w+)\\s+ON\\s+(\\w+)\\s+\\((.+)\\)", 
                                        Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(rest);
        
        if (!matcher.matches()) {
            return "Invalid CREATE INDEX syntax. Expected: CREATE [UNIQUE] INDEX name ON table (col1, col2, ...)";
        }
        
        boolean unique = matcher.group(1) != null;
        String indexName = matcher.group(2);
        String tableName = matcher.group(3);
        String columnsStr = matcher.group(4);
        
        // Parse columns
        List<String> columns = new ArrayList<>();
        String[] columnNames = columnsStr.split(",");
        
        for (String columnName : columnNames) {
            columns.add(columnName.trim());
        }
        
        // Create the index
        SchemaManager schemaManager = dbSystem.getSchemaManager();
        Index index = schemaManager.createIndex(indexName, tableName, columns, unique);
        
        if (index != null) {
            return "Index '" + indexName + "' created successfully";
        } else {
            return "Failed to create index '" + indexName + "'";
        }
    }
    
    /**
     * Executes a DROP statement.
     *
     * @param rest The rest of the statement
     * @return The result of the statement
     */
    private String executeDrop(String rest) {
        // Split into type and rest
        String[] parts = rest.split("\\s+", 2);
        String type = parts[0].toUpperCase();
        String name = parts.length > 1 ? parts[1].trim() : "";
        
        switch (type) {
            case "TABLE":
                return executeDropTable(name);
            case "INDEX":
                return executeDropIndex(name);
            default:
                return "Unknown DROP type: " + type;
        }
    }
    
    /**
     * Executes a DROP TABLE statement.
     *
     * @param tableName The name of the table to drop
     * @return The result of the statement
     */
    private String executeDropTable(String tableName) {
        SchemaManager schemaManager = dbSystem.getSchemaManager();
        boolean dropped = schemaManager.dropTable(tableName);
        
        if (dropped) {
            return "Table '" + tableName + "' dropped successfully";
        } else {
            return "Failed to drop table '" + tableName + "'";
        }
    }
    
    /**
     * Executes a DROP INDEX statement.
     *
     * @param indexName The name of the index to drop
     * @return The result of the statement
     */
    private String executeDropIndex(String indexName) {
        SchemaManager schemaManager = dbSystem.getSchemaManager();
        boolean dropped = schemaManager.dropIndex(indexName);
        
        if (dropped) {
            return "Index '" + indexName + "' dropped successfully";
        } else {
            return "Failed to drop index '" + indexName + "'";
        }
    }
    
    /**
     * Executes an INSERT statement.
     *
     * @param rest The rest of the statement
     * @return The result of the statement
     */
    private String executeInsert(String rest) {
        // Parse: INSERT INTO table (col1, col2, ...) VALUES (val1, val2, ...)
        Pattern pattern = Pattern.compile("INTO\\s+(\\w+)(?:\\s+\\((.+)\\))?\\s+VALUES\\s+\\((.+)\\)", 
                                        Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(rest);
        
        if (!matcher.matches()) {
            return "Invalid INSERT syntax. Expected: INSERT INTO table (col1, col2, ...) VALUES (val1, val2, ...)";
        }
        
        String tableName = matcher.group(1);
        String columnsStr = matcher.group(2);
        String valuesStr = matcher.group(3);
        
        // For educational purposes, we'll just pretend to insert the data
        SchemaManager schemaManager = dbSystem.getSchemaManager();
        Table table = schemaManager.getTable(tableName);
        
        if (table == null) {
            return "Table '" + tableName + "' does not exist";
        }
        
        // Parse columns
        List<String> columnNames;
        if (columnsStr != null) {
            columnNames = new ArrayList<>();
            String[] columns = columnsStr.split(",");
            
            for (String column : columns) {
                columnNames.add(column.trim());
            }
        } else {
            // Use all columns
            columnNames = new ArrayList<>();
            for (Column column : table.getColumns()) {
                columnNames.add(column.getName());
            }
        }
        
        // Parse values
        List<String> values = new ArrayList<>();
        String[] valueParts = valuesStr.split(",(?=(?:[^']*'[^']*')*[^']*$)");
        
        for (String value : valueParts) {
            value = value.trim();
            
            // Remove quotes from string values
            if (value.startsWith("'") && value.endsWith("'")) {
                value = value.substring(1, value.length() - 1);
            }
            
            values.add(value);
        }
        
        // Validate number of values
        if (columnNames.size() != values.size()) {
            return "Number of columns (" + columnNames.size() + 
                  ") does not match number of values (" + values.size() + ")";
        }
        
        // Create a row
        Map<String, Object> row = new HashMap<>();
        
        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            String value = values.get(i);
            
            Column column = table.getColumn(columnName);
            
            if (column == null) {
                return "Column '" + columnName + "' does not exist in table '" + tableName + "'";
            }
            
            // Convert value to the appropriate type
            Object convertedValue = column.getDataType().parseValue(value);
            
            if (convertedValue == null && !column.isNullable()) {
                return "Cannot insert null into non-nullable column '" + columnName + "'";
            }
            
            row.put(columnName, convertedValue);
        }
        
        // Validate the row
        if (!table.validateRow(row)) {
            return "Invalid row data";
        }
        
        // For educational purposes, we'll just pretend to insert the data
        return "Inserted 1 row into table '" + tableName + "'";
    }
    
    /**
     * Executes a SELECT statement.
     *
     * @param rest The rest of the statement
     * @return The result of the statement
     */
    private String executeSelect(String rest) {
        // Parse: SELECT col1, col2, ... FROM table [WHERE condition]
        Pattern pattern = Pattern.compile("(.+)\\s+FROM\\s+(\\w+)(?:\\s+WHERE\\s+(.+))?", 
                                        Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(rest);
        
        if (!matcher.matches()) {
            return "Invalid SELECT syntax. Expected: SELECT col1, col2, ... FROM table [WHERE condition]";
        }
        
        String columnsStr = matcher.group(1).trim();
        String tableName = matcher.group(2);
        String whereClause = matcher.group(3);
        
        // For educational purposes, we'll just pretend to select the data
        SchemaManager schemaManager = dbSystem.getSchemaManager();
        Table table = schemaManager.getTable(tableName);
        
        if (table == null) {
            return "Table '" + tableName + "' does not exist";
        }
        
        // Parse columns
        List<String> columnNames;
        if (columnsStr.equals("*")) {
            // Select all columns
            columnNames = new ArrayList<>();
            for (Column column : table.getColumns()) {
                columnNames.add(column.getName());
            }
        } else {
            columnNames = new ArrayList<>();
            String[] columns = columnsStr.split(",");
            
            for (String column : columns) {
                columnNames.add(column.trim());
            }
        }
        
        // Validate columns
        for (String columnName : columnNames) {
            if (table.getColumn(columnName) == null) {
                return "Column '" + columnName + "' does not exist in table '" + tableName + "'";
            }
        }
        
        // For educational purposes, we'll just return a mock result
        StringBuilder sb = new StringBuilder();
        sb.append("Selected columns: ").append(String.join(", ", columnNames));
        sb.append(" from table '").append(tableName).append("'");
        
        if (whereClause != null) {
            sb.append(" where ").append(whereClause);
        }
        
        return sb.toString();
    }
    
    /**
     * Executes an UPDATE statement.
     *
     * @param rest The rest of the statement
     * @return The result of the statement
     */
    private String executeUpdate(String rest) {
        // Parse: UPDATE table SET col1 = val1, col2 = val2, ... [WHERE condition]
        Pattern pattern = Pattern.compile("(\\w+)\\s+SET\\s+(.+?)(?:\\s+WHERE\\s+(.+))?", 
                                        Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(rest);
        
        if (!matcher.matches()) {
            return "Invalid UPDATE syntax. Expected: UPDATE table SET col1 = val1, col2 = val2, ... [WHERE condition]";
        }
        
        String tableName = matcher.group(1);
        String setClause = matcher.group(2);
        String whereClause = matcher.group(3);
        
        // For educational purposes, we'll just pretend to update the data
        SchemaManager schemaManager = dbSystem.getSchemaManager();
        Table table = schemaManager.getTable(tableName);
        
        if (table == null) {
            return "Table '" + tableName + "' does not exist";
        }
        
        // Parse SET clause
        Map<String, Object> updates = new HashMap<>();
        String[] setParts = setClause.split(",(?=(?:[^']*'[^']*')*[^']*$)");
        
        for (String setPart : setParts) {
            setPart = setPart.trim();
            String[] keyValue = setPart.split("=", 2);
            
            if (keyValue.length != 2) {
                return "Invalid SET clause: " + setPart;
            }
            
            String columnName = keyValue[0].trim();
            String value = keyValue[1].trim();
            
            // Remove quotes from string values
            if (value.startsWith("'") && value.endsWith("'")) {
                value = value.substring(1, value.length() - 1);
            }
            
            Column column = table.getColumn(columnName);
            
            if (column == null) {
                return "Column '" + columnName + "' does not exist in table '" + tableName + "'";
            }
            
            // Convert value to the appropriate type
            Object convertedValue = column.getDataType().parseValue(value);
            
            if (convertedValue == null && !column.isNullable()) {
                return "Cannot update non-nullable column '" + columnName + "' to null";
            }
            
            updates.put(columnName, convertedValue);
        }
        
        // For educational purposes, we'll just return a mock result
        StringBuilder sb = new StringBuilder();
        sb.append("Updated columns: ");
        
        boolean first = true;
        for (Map.Entry<String, Object> entry : updates.entrySet()) {
            if (!first) {
                sb.append(", ");
            }
            sb.append(entry.getKey()).append(" = ").append(entry.getValue());
            first = false;
        }
        
        sb.append(" in table '").append(tableName).append("'");
        
        if (whereClause != null) {
            sb.append(" where ").append(whereClause);
        }
        
        return sb.toString();
    }
    
    /**
     * Executes a DELETE statement.
     *
     * @param rest The rest of the statement
     * @return The result of the statement
     */
    private String executeDelete(String rest) {
        // Parse: DELETE FROM table [WHERE condition]
        Pattern pattern = Pattern.compile("FROM\\s+(\\w+)(?:\\s+WHERE\\s+(.+))?", 
                                        Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(rest);
        
        if (!matcher.matches()) {
            return "Invalid DELETE syntax. Expected: DELETE FROM table [WHERE condition]";
        }
        
        String tableName = matcher.group(1);
        String whereClause = matcher.group(2);
        
        // For educational purposes, we'll just pretend to delete the data
        SchemaManager schemaManager = dbSystem.getSchemaManager();
        Table table = schemaManager.getTable(tableName);
        
        if (table == null) {
            return "Table '" + tableName + "' does not exist";
        }
        
        // For educational purposes, we'll just return a mock result
        StringBuilder sb = new StringBuilder();
        sb.append("Deleted from table '").append(tableName).append("'");
        
        if (whereClause != null) {
            sb.append(" where ").append(whereClause);
        }
        
        return sb.toString();
    }
    
    /**
     * Executes a SHOW statement.
     *
     * @param rest The rest of the statement
     * @return The result of the statement
     */
    private String executeShow(String rest) {
        // Split into type and rest
        String[] parts = rest.split("\\s+", 2);
        String type = parts[0].toUpperCase();
        
        switch (type) {
            case "TABLES":
                return executeShowTables();
            case "INDEXES":
                return executeShowIndexes();
            case "TABLESPACES":
                return executeShowTablespaces();
            case "BUFFERPOOLS":
                return executeShowBufferPools();
            case "STATISTICS":
                return executeShowStatistics();
            default:
                return "Unknown SHOW type: " + type;
        }
    }
    
    /**
     * Executes a SHOW TABLES statement.
     *
     * @return The result of the statement
     */
    private String executeShowTables() {
        SchemaManager schemaManager = dbSystem.getSchemaManager();
        List<Table> tables = schemaManager.getAllTables();
        
        if (tables.isEmpty()) {
            return "No tables found";
        }
        
        StringBuilder sb = new StringBuilder();
        sb.append("Tables:\n");
        
        for (Table table : tables) {
            sb.append("  ").append(table.getName()).append(" (").append(table.getTablespaceName()).append(")\n");
        }
        
        return sb.toString();
    }
    
    /**
     * Executes a SHOW INDEXES statement.
     *
     * @return The result of the statement
     */
    private String executeShowIndexes() {
        SchemaManager schemaManager = dbSystem.getSchemaManager();
        List<Index> indexes = schemaManager.getAllIndexes();
        
        if (indexes.isEmpty()) {
            return "No indexes found";
        }
        
        StringBuilder sb = new StringBuilder();
        sb.append("Indexes:\n");
        
        for (Index index : indexes) {
            sb.append("  ").append(index.toString()).append("\n");
        }
        
        return sb.toString();
    }
    
    /**
     * Executes a SHOW TABLESPACES statement.
     *
     * @return The result of the statement
     */
    private String executeShowTablespaces() {
        StringBuilder sb = new StringBuilder();
        sb.append("TABLESPACES:\n");
        sb.append(String.format("%-20s %-30s %-15s %-15s\n", 
                "NAME", "LOCATION", "SIZE (PAGES)", "PAGE SIZE"));
        sb.append(String.format("%-20s %-30s %-15s %-15s\n", 
                "--------------------", "------------------------------", "---------------", "---------------"));
        
        StorageManager storageManager = dbSystem.getStorageManager();
        Map<String, Tablespace> tablespaces = new HashMap<>();
        
        // Get all tablespaces through reflection since it's private in StorageManager
        try {
            java.lang.reflect.Field tablespacesField = StorageManager.class.getDeclaredField("tablespaces");
            tablespacesField.setAccessible(true);
            tablespaces = (Map<String, Tablespace>) tablespacesField.get(storageManager);
        } catch (Exception e) {
            logger.error("Failed to get tablespaces", e);
            return "Failed to retrieve tablespace information: " + e.getMessage();
        }
        
        if (tablespaces.isEmpty()) {
            return "No tablespaces found.";
        }
        
        for (Tablespace tablespace : tablespaces.values()) {
            String name = tablespace.getName();
            String location = tablespace.getStorageContainer().getContainerPath();
            int pageSize = tablespace.getPageSize();
            int totalPages;
            
            try {
                totalPages = tablespace.getTotalPages();
            } catch (IOException e) {
                totalPages = -1;
            }
            
            sb.append(String.format("%-20s %-30s %-15d %-15d\n", 
                    name, location, totalPages, pageSize));
        }
        
        return sb.toString();
    }
    
    /**
     * Executes a SHOW BUFFERPOOLS statement.
     *
     * @return The result of the statement
     */
    private String executeShowBufferPools() {
        StringBuilder sb = new StringBuilder();
        sb.append("BUFFER POOLS:\n");
        sb.append(String.format("%-20s %-15s %-15s %-15s\n", 
                "TABLESPACE", "CAPACITY", "USED", "USAGE %"));
        sb.append(String.format("%-20s %-15s %-15s %-15s\n", 
                "--------------------", "---------------", "---------------", "---------------"));
        
        Map<String, BufferPoolManager> bufferPools = new HashMap<>();
        
        // Get all buffer pools through reflection since it's private in DatabaseSystem
        try {
            java.lang.reflect.Field bpmsField = DatabaseSystem.class.getDeclaredField("bufferPoolManagers");
            bpmsField.setAccessible(true);
            bufferPools = (Map<String, BufferPoolManager>) bpmsField.get(dbSystem);
        } catch (Exception e) {
            logger.error("Failed to get buffer pool managers", e);
            return "Failed to retrieve buffer pool information: " + e.getMessage();
        }
        
        if (bufferPools.isEmpty()) {
            return "No buffer pools found.";
        }
        
        for (BufferPoolManager bpm : bufferPools.values()) {
            String tablespaceName = bpm.getTablespaceName();
            int capacity = bpm.getCapacity();
            int used = bpm.getSize();
            double usagePercent = (double) used / capacity * 100.0;
            
            sb.append(String.format("%-20s %-15d %-15d %-15.2f\n", 
                    tablespaceName, capacity, used, usagePercent));
        }
        
        return sb.toString();
    }
    
    /**
     * Executes a SHOW STATISTICS statement to show detailed buffer pool and page stats.
     *
     * @return The result of the statement showing buffer pool statistics
     */
    private String executeShowStatistics() {
        StringBuilder sb = new StringBuilder();
        
        // Get buffer pools through reflection
        Map<String, BufferPoolManager> bufferPools = new HashMap<>();
        try {
            java.lang.reflect.Field bpmsField = DatabaseSystem.class.getDeclaredField("bufferPoolManagers");
            bpmsField.setAccessible(true);
            bufferPools = (Map<String, BufferPoolManager>) bpmsField.get(dbSystem);
        } catch (Exception e) {
            logger.error("Failed to get buffer pool managers", e);
            return "Failed to retrieve buffer pool information: " + e.getMessage();
        }
        
        if (bufferPools.isEmpty()) {
            return "No buffer pools found.";
        }
        
        // Display detailed buffer pool statistics
        sb.append("BUFFER POOL STATISTICS:\n\n");
        
        for (BufferPoolManager bpm : bufferPools.values()) {
            String tablespaceName = bpm.getTablespaceName();
            sb.append("Tablespace: ").append(tablespaceName).append("\n");
            sb.append(String.format("  Capacity: %d pages\n", bpm.getCapacity()));
            sb.append(String.format("  Current Size: %d pages\n", bpm.getSize()));
            sb.append(String.format("  Usage: %.2f%%\n", (double) bpm.getSize() / bpm.getCapacity() * 100.0));
            
            // Get and display detailed page information using reflection
            Map<PageId, Page> pageTable = new HashMap<>();
            try {
                java.lang.reflect.Field pageTableField = BufferPoolManager.class.getDeclaredField("pageTable");
                pageTableField.setAccessible(true);
                pageTable = (Map<PageId, Page>) pageTableField.get(bpm);
                
                if (!pageTable.isEmpty()) {
                    sb.append("\n  LOADED PAGES:\n");
                    sb.append(String.format("  %-10s %-10s %-10s %-10s\n", 
                            "PAGE_NO", "PIN COUNT", "DIRTY", "SIZE (B)"));
                    sb.append(String.format("  %-10s %-10s %-10s %-10s\n", 
                            "----------", "----------", "----------", "----------"));
                    
                    for (Map.Entry<PageId, Page> entry : pageTable.entrySet()) {
                        PageId pageId = entry.getKey();
                        Page page = entry.getValue();
                        
                        sb.append(String.format("  %-10d %-10d %-10s %-10d\n", 
                                pageId.getPageNumber(), 
                                page.getPinCount(), 
                                page.isDirty() ? "YES" : "NO", 
                                page.getPageSize()));
                    }
                } else {
                    sb.append("\n  No pages currently loaded.\n");
                }
            } catch (Exception e) {
                logger.error("Failed to get page table", e);
                sb.append("\n  Failed to retrieve page information: ").append(e.getMessage()).append("\n");
            }
            
            sb.append("\n");
        }
        
        // Get storage manager and tablespaces
        StorageManager storageManager = dbSystem.getStorageManager();
        Map<String, Tablespace> tablespaces = new HashMap<>();
        try {
            java.lang.reflect.Field tablespacesField = StorageManager.class.getDeclaredField("tablespaces");
            tablespacesField.setAccessible(true);
            tablespaces = (Map<String, Tablespace>) tablespacesField.get(storageManager);
            
            sb.append("STORAGE STATISTICS:\n\n");
            
            for (Tablespace tablespace : tablespaces.values()) {
                String name = tablespace.getName();
                int pageSize = tablespace.getPageSize();
                int totalPages;
                
                try {
                    totalPages = tablespace.getTotalPages();
                    long totalSizeBytes = (long) totalPages * pageSize;
                    double totalSizeMB = totalSizeBytes / (1024.0 * 1024.0);
                    
                    sb.append("Tablespace: ").append(name).append("\n");
                    sb.append(String.format("  Container: %s\n", tablespace.getStorageContainer().getContainerPath()));
                    sb.append(String.format("  Page Size: %d bytes\n", pageSize));
                    sb.append(String.format("  Total Pages: %d\n", totalPages));
                    sb.append(String.format("  Total Size: %.2f MB\n", totalSizeMB));
                    sb.append("\n");
                } catch (IOException e) {
                    sb.append("Tablespace: ").append(name).append("\n");
                    sb.append("  Error reading tablespace statistics: ").append(e.getMessage()).append("\n\n");
                }
            }
        } catch (Exception e) {
            logger.error("Failed to get tablespaces", e);
            sb.append("Failed to retrieve tablespace information: ").append(e.getMessage()).append("\n");
        }
        
        return sb.toString();
    }
} 