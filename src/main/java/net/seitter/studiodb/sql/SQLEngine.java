package net.seitter.studiodb.sql;

import net.seitter.studiodb.DatabaseSystem;
import net.seitter.studiodb.buffer.BufferPoolManager;
import net.seitter.studiodb.buffer.IBufferPoolManager;
import net.seitter.studiodb.schema.Column;
import net.seitter.studiodb.schema.DataType;
import net.seitter.studiodb.schema.Index;
import net.seitter.studiodb.schema.SchemaManager;
import net.seitter.studiodb.schema.Table;
import net.seitter.studiodb.storage.Page;
import net.seitter.studiodb.storage.PageId;
import net.seitter.studiodb.storage.StorageManager;
import net.seitter.studiodb.storage.Tablespace;
import net.seitter.studiodb.storage.PageLayout;
import net.seitter.studiodb.storage.PageType;
import net.seitter.studiodb.storage.layout.PageLayoutFactory;
import net.seitter.studiodb.storage.layout.TableHeaderPageLayout;
import net.seitter.studiodb.storage.layout.TableDataPageLayout;
import net.seitter.studiodb.storage.layout.IndexPageLayout;
import net.seitter.studiodb.storage.layout.ContainerMetadataPageLayout;
import net.seitter.studiodb.storage.layout.FreeSpaceMapPageLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.HashSet;
import java.util.Set;

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
     * @throws IOException If there's an error accessing the storage
     */
    private String executeInsert(String rest) throws IOException {
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
        
        // Use the new insertRow method to handle the insertion
        boolean success = schemaManager.insertRow(tableName, row);
        
        if (success) {
            return "Inserted 1 row into table '" + tableName + "'";
        } else {
            return "Failed to insert row into table '" + tableName + "'";
        }
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
        
        try {
            // Fetch data from the table
            List<Map<String, Object>> rows = fetchTableData(table, columnNames, whereClause);
            return formatSelectResults(columnNames, rows);
        } catch (Exception e) {
            logger.error("Error executing SELECT query", e);
            return "Error executing query: " + e.getMessage();
        }
    }
    
    /**
     * Fetches data from a table based on selected columns and where clause.
     *
     * @param table The table to fetch from
     * @param columnNames The columns to fetch
     * @param whereClause The where clause for filtering rows, or null if no filtering
     * @return A list of rows as maps of column name to value
     * @throws IOException If there's an error reading from storage
     */
    private List<Map<String, Object>> fetchTableData(Table table, List<String> columnNames, 
                                                   String whereClause) throws IOException {
        List<Map<String, Object>> result = new ArrayList<>();
        
        // Get the buffer pool for the table's tablespace
        IBufferPoolManager bufferPool = dbSystem.getBufferPoolManager(table.getTablespaceName());
        if (bufferPool == null) {
            throw new IOException("Buffer pool not found for tablespace: " + table.getTablespaceName());
        }
        
        // Get the header page to find the first data page
        PageId headerPageId = new PageId(table.getTablespaceName(), table.getHeaderPageId());
        Page headerPage = null;
        
        try {
            headerPage = bufferPool.fetchPage(headerPageId);
            
            if (headerPage == null) {
                throw new IOException("Header page not found for table: " + table.getName());
            }
            
            // Create header page layout
            TableHeaderPageLayout headerLayout = new TableHeaderPageLayout(headerPage);
            
            // Get the first data page ID
            int firstDataPageId = headerLayout.getFirstDataPageId();
            
            // If there are no data pages, return empty result
            if (firstDataPageId == -1) {
                return result;
            }
            
            // Traverse through the chain of data pages
            int currentPageId = firstDataPageId;
            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            mapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
            
            while (currentPageId != -1) {
                PageId dataPageId = new PageId(table.getTablespaceName(), currentPageId);
                Page dataPage = null;
                int nextPageId = -1;
                
                try {
                    dataPage = bufferPool.fetchPage(dataPageId);
                    
                    if (dataPage == null) {
                        logger.error("Data page not found: {}", dataPageId);
                        break;
                    }
                    
                    // Create data page layout
                    TableDataPageLayout dataLayout = new TableDataPageLayout(dataPage);
                    
                    // Get next page ID before processing rows (in case processing throws an exception)
                    nextPageId = dataLayout.getNextPageId();
                    
                    // Get number of rows in this page
                    int rowCount = dataLayout.getRowCount();
                    
                    // Process each row
                    for (int i = 0; i < rowCount; i++) {
                        byte[] rowData = dataLayout.getRow(i);
                        
                        if (rowData != null) {
                            try {
                                // Deserialize the row
                                Map<String, Object> row = mapper.readValue(rowData, 
                                    new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {});
                                
                                // Filter row based on where clause if present
                                if (whereClause == null || evaluateWhereClause(row, whereClause)) {
                                    // Project only requested columns
                                    Map<String, Object> projectedRow = new HashMap<>();
                                    for (String columnName : columnNames) {
                                        projectedRow.put(columnName, row.get(columnName));
                                    }
                                    
                                    result.add(projectedRow);
                                }
                            } catch (Exception e) {
                                logger.error("Error deserializing row: {}", e.getMessage());
                                // Continue with next row
                            }
                        }
                    }
                    
                    // Update currentPageId for next iteration
                    currentPageId = nextPageId;
                    
                } finally {
                    // Always unpin the data page, even if an exception occurred
                    if (dataPage != null) {
                        bufferPool.unpinPage(dataPageId, false);
                    }
                }
            }
        } finally {
            // Always unpin the header page, even if an exception occurred
            if (headerPage != null) {
                bufferPool.unpinPage(headerPageId, false);
            }
        }
        
        return result;
    }
    
    /**
     * Evaluates the where clause for a single row.
     * This is a simple implementation that only supports basic conditions.
     *
     * @param row The row to evaluate
     * @param whereClause The where clause to evaluate
     * @return true if the row matches the where clause, false otherwise
     */
    private boolean evaluateWhereClause(Map<String, Object> row, String whereClause) {
        // Simple equality condition: column = value
        Pattern equalsPattern = Pattern.compile("(\\w+)\\s*=\\s*([^\\s]+)");
        Matcher matcher = equalsPattern.matcher(whereClause);
        
        if (matcher.matches()) {
            String columnName = matcher.group(1);
            String value = matcher.group(2);
            
            // Remove quotes from string values
            if (value.startsWith("'") && value.endsWith("'")) {
                value = value.substring(1, value.length() - 1);
            }
            
            Object rowValue = row.get(columnName);
            
            // If value in row is null, it doesn't match
            if (rowValue == null) {
                return false;
            }
            
            // Convert both to strings for comparison
            return rowValue.toString().equals(value);
        }
        
        // For more complex conditions, return true for now
        // In a full implementation, we would need to parse and evaluate complex expressions
        logger.warn("Complex WHERE clause not fully supported: {}", whereClause);
        return true;
    }
    
    /**
     * Formats the results of a SELECT query into a human-readable string.
     *
     * @param columnNames The column names
     * @param rows The rows of data
     * @return A formatted string representing the results
     */
    private String formatSelectResults(List<String> columnNames, List<Map<String, Object>> rows) {
        if (rows.isEmpty()) {
            return "No rows found.";
        }
        
        StringBuilder sb = new StringBuilder();
        
        // Calculate column widths
        Map<String, Integer> columnWidths = new HashMap<>();
        for (String columnName : columnNames) {
            columnWidths.put(columnName, columnName.length());
        }
        
        // Find the maximum width for each column
        for (Map<String, Object> row : rows) {
            for (String columnName : columnNames) {
                Object value = row.get(columnName);
                String displayValue = value != null ? value.toString() : "NULL";
                columnWidths.put(columnName, Math.max(columnWidths.get(columnName), displayValue.length()));
            }
        }
        
        // Format header
        for (String columnName : columnNames) {
            sb.append(String.format("%-" + (columnWidths.get(columnName) + 2) + "s", columnName));
        }
        sb.append("\n");
        
        // Format separator
        for (String columnName : columnNames) {
            for (int i = 0; i < columnWidths.get(columnName) + 2; i++) {
                sb.append("-");
            }
        }
        sb.append("\n");
        
        // Format rows
        for (Map<String, Object> row : rows) {
            for (String columnName : columnNames) {
                Object value = row.get(columnName);
                String displayValue = value != null ? value.toString() : "NULL";
                sb.append(String.format("%-" + (columnWidths.get(columnName) + 2) + "s", displayValue));
            }
            sb.append("\n");
        }
        
        sb.append("\n").append(rows.size()).append(" row(s) returned.");
        
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
     * Executes a SHOW command.
     *
     * @param rest The rest of the statement
     * @return The result of the statement
     */
    private String executeShow(String rest) {
        // Split into type and rest
        String[] parts = rest.split("\\s+", 2);
        String type = parts[0].toUpperCase();
        String restOfShow = parts.length > 1 ? parts[1] : "";
        
        switch (type) {
            case "TABLES":
                return executeShowTables();
            case "INDEXES":
                return executeShowIndexes();
            case "TABLESPACES":
                return executeShowTablespaces();
            case "BUFFERPOOLS":
                return executeShowBufferPools();
            case "PAGES":
                return executeShowPages(restOfShow);
            case "STATISTICS":
                return executeShowStatistics();
            case "PINCOUNT":
                return executeShowPinCount();
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
        
        // Get tablespaces from the system catalog
        SchemaManager schemaManager = dbSystem.getSchemaManager();
        StorageManager storageManager = dbSystem.getStorageManager();
        
        if (schemaManager == null) {
            return "Schema manager not initialized.";
        }
        
        // Get the SYS_TABLESPACES table
        String sysTablespaceName = SchemaManager.SYS_TABLESPACES;
        Table tablespaceTable = schemaManager.getTable(sysTablespaceName);
        
        if (tablespaceTable == null) {
            return "System catalog table not found: " + sysTablespaceName;
        }
        
        try {
            // Execute a simplified SELECT-like operation on SYS_TABLESPACES
            String query = "SELECT * FROM " + sysTablespaceName;
            List<Map<String, Object>> rows = new ArrayList<>();
            
            // This is a simplified approach - in a real implementation, 
            // we would use the query engine to do this
            IBufferPoolManager systemBpm = dbSystem.getBufferPoolManager(SchemaManager.SYSTEM_TABLESPACE);
            if (systemBpm == null) {
                return "System buffer pool not found.";
            }
            
            // Get all rows from SYS_TABLESPACES
            rows = executeSelectInternal(tablespaceTable, systemBpm);
            
            if (rows.isEmpty()) {
                return "No tablespaces found in system catalog.";
            }
            
            // Format the rows
            for (Map<String, Object> row : rows) {
                String name = (String) row.get("TABLESPACE_NAME");
                String location = (String) row.get("CONTAINER_PATH");
                int pageSize = ((Number) row.get("PAGE_SIZE")).intValue();
                
                // Get totalPages from StorageManager
                int totalPages = -1;
                Tablespace tablespace = storageManager.getTablespace(name);
                if (tablespace != null) {
                    try {
                        totalPages = tablespace.getTotalPages();
                    } catch (IOException e) {
                        logger.warn("Error getting total pages for tablespace '{}'", name, e);
                    }
                }
                
                sb.append(String.format("%-20s %-30s %-15d %-15d\n", 
                        name, location, totalPages, pageSize));
            }
            
            return sb.toString();
        } catch (Exception e) {
            logger.error("Error retrieving tablespace information", e);
            return "Error retrieving tablespace information: " + e.getMessage();
        }
    }
    
    /**
     * Executes a simplified internal SELECT operation on a system table.
     * 
     * @param table The table to select from
     * @param bufferPool The buffer pool to use
     * @return A list of rows from the table
     * @throws IOException If there's an error reading from the table
     */
    private List<Map<String, Object>> executeSelectInternal(Table table, IBufferPoolManager bufferPool) 
            throws IOException {
        List<Map<String, Object>> results = new ArrayList<>();
        
        // Read the header page to get the first data page
        PageId headerPageId = new PageId(table.getTablespaceName(), table.getHeaderPageId());
        Page headerPage = bufferPool.fetchPage(headerPageId);
        
        if (headerPage == null) {
            logger.error("Header page for table '{}' not found", table.getName());
            bufferPool.unpinPage(headerPageId, false);
            return results;
        }
        
        try {
            TableHeaderPageLayout headerLayout = new TableHeaderPageLayout(headerPage);
            int firstDataPageId = headerLayout.getFirstDataPageId();
            
            if (firstDataPageId == -1) {
                // No data pages
                return results;
            }
            
            // Track visited page IDs to avoid circular references
            Set<Integer> visitedPageIds = new HashSet<>();
            int currentPageId = firstDataPageId;
            
            // Read all data pages
            while (currentPageId != -1 && !visitedPageIds.contains(currentPageId)) {
                visitedPageIds.add(currentPageId);
                
                PageId dataPageId = new PageId(table.getTablespaceName(), currentPageId);
                Page dataPage = bufferPool.fetchPage(dataPageId);
                
                if (dataPage == null) {
                    logger.error("Data page {} for table '{}' not found", currentPageId, table.getName());
                    break;
                }
                
                try {
                    TableDataPageLayout dataLayout = new TableDataPageLayout(dataPage);
                    
                    // Read all rows in this page
                    for (int i = 0; i < dataLayout.getRowCount(); i++) {
                        byte[] rowData = dataLayout.getRow(i);
                        if (rowData != null) {
                            Map<String, Object> row = deserializeRow(rowData);
                            results.add(row);
                        }
                    }
                    
                    // Get next page ID
                    currentPageId = dataLayout.getNextPageId();
                } finally {
                    bufferPool.unpinPage(dataPageId, false);
                }
            }
        } finally {
            bufferPool.unpinPage(headerPageId, false);
        }
        
        return results;
    }
    
    /**
     * Deserializes a row from a byte array.
     * 
     * @param rowData The serialized row data
     * @return The deserialized row as a map
     * @throws IOException If there's an error deserializing the row
     */
    private Map<String, Object> deserializeRow(byte[] rowData) throws IOException {
        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        return mapper.readValue(rowData, Map.class);
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
     * Executes a SHOW PAGES statement to display detailed page information for a tablespace.
     *
     * @param rest The rest of the statement
     * @return The result of the statement showing page details
     */
    private String executeShowPages(String rest) {
        // Parse: SHOW PAGES [IN TABLESPACE tablespace_name]
        String tablespaceName = rest;
        
        if (tablespaceName == null || tablespaceName.isEmpty()) {
            return "Please specify a tablespace name. Syntax: SHOW PAGES IN TABLESPACE tablespace_name";
        }
        
        // Remove leading/trailing whitespace and optional "IN TABLESPACE" prefix
        tablespaceName = tablespaceName.trim();
        if (tablespaceName.toUpperCase().startsWith("IN TABLESPACE")) {
            tablespaceName = tablespaceName.substring("IN TABLESPACE".length()).trim();
        }
        
        Tablespace tablespace = dbSystem.getStorageManager().getTablespace(tablespaceName);
        if (tablespace == null) {
            return "Tablespace '" + tablespaceName + "' not found";
        }
        
        // Get the buffer pool for the tablespace
        IBufferPoolManager bufferPool = dbSystem.getBufferPoolManager(tablespaceName);
        if (bufferPool == null) {
            return "Buffer pool for tablespace '" + tablespaceName + "' not found";
        }
        
        StringBuilder sb = new StringBuilder();
        sb.append("Pages in tablespace '").append(tablespaceName).append("':\n\n");
        
        int totalPages;
        try {
            totalPages = tablespace.getTotalPages();
        } catch (IOException e) {
            return "Error getting total pages: " + e.getMessage();
        }
        
        // Format for the header
        sb.append(String.format("%-6s %-14s %-10s %-10s %s\n", 
                "PAGE", "TYPE", "SIZE", "DIRTY", "ADDITIONAL INFO"));
        sb.append(String.format("%-6s %-14s %-10s %-10s %s\n", 
                "------", "--------------", "----------", "----------", "--------------------"));
        
        // Iterate through all pages in the tablespace
        for (int i = 0; i < totalPages; i++) {
            PageId pageId = new PageId(tablespaceName, i);
            Page page = null;
            try {
                page = bufferPool.fetchPage(pageId);
                
                if (page == null) {
                    continue;
                }
                
                // Access page information
                PageLayout layout = PageLayoutFactory.createLayout(page);
                if (layout == null) {
                    // If the layout is null, it's likely a free or uninitialized page
                    sb.append(String.format("%-6d %-14s %-10d %-10s %s\n", 
                            i, "UNKNOWN", page.getPageSize(), page.isDirty() ? "YES" : "NO", ""));
                    continue;
                }
                
                // Get page type
                PageType pageType = layout.getPageType();
                
                // Get additional information based on page type
                String additionalInfo = getAdditionalPageInfoFromLayout(layout);
                
                // Format the output
                sb.append(String.format("%-6d %-14s %-10d %-10s %s\n", 
                        i, pageType, page.getPageSize(), page.isDirty() ? "YES" : "NO", additionalInfo));
            } catch (Exception e) {
                // Log any errors but continue processing other pages
                logger.error("Error processing page {}: {}", pageId, e.getMessage());
                sb.append(String.format("%-6d %-14s %-10s %-10s %s\n", 
                        i, "ERROR", "?", "?", "Error: " + e.getMessage()));
            } finally {
                // Ensure page is always unpinned even if an exception occurs
                if (page != null) {
                    bufferPool.unpinPage(pageId, false);
                }
            }
        }
        
        return sb.toString();
    }
    
    /**
     * Get additional information about a page based on its layout.
     * 
     * @param layout The page layout
     * @return Additional information as a string
     */
    private String getAdditionalPageInfoFromLayout(PageLayout layout) {
        StringBuilder info = new StringBuilder();
        
        if (layout instanceof TableHeaderPageLayout) {
            TableHeaderPageLayout headerLayout = (TableHeaderPageLayout) layout;
            int firstDataPageId = headerLayout.getFirstDataPageId();
            info.append("Table: ").append(headerLayout.getTableName())
                .append(", FirstDataPage: ").append(firstDataPageId);
                
            // Add column count info
            try {
                int colCount = headerLayout.getColumnCount();
                info.append(", Columns: ").append(colCount);
            } catch (Exception e) {
                info.append(", Columns: Error");
            }
        }
        else if (layout instanceof TableDataPageLayout) {
            TableDataPageLayout dataLayout = (TableDataPageLayout) layout;
            // Use proper API method to get row count
            int rowCount = dataLayout.getRowCount();
            int freeSpace = dataLayout.getFreeSpace();
            
            info.append("Records: ").append(rowCount)
                .append(", FreeSpace: ").append(freeSpace).append(" bytes")
                .append(", Buffer: ").append(dataLayout.getPage().getBuffer().capacity()).append(" bytes");
            
            // Show some row information if rows exist
            if (rowCount > 0) {
                info.append(", Rows: [");
                int maxRowsToShow = Math.min(3, rowCount); // Show at most 3 rows
                List<byte[]> rows = dataLayout.getAllRows();
                for (int i = 0; i < maxRowsToShow && i < rows.size(); i++) {
                    if (i > 0) info.append(", ");
                    info.append("(").append(rows.get(i).length).append("b)");
                }
                if (rowCount > maxRowsToShow) info.append("...");
                info.append("]");
            }
        }
        else if (layout instanceof IndexPageLayout) {
            IndexPageLayout indexLayout = (IndexPageLayout) layout;
            info.append(indexLayout.isLeaf() ? "Leaf" : "Internal")
                .append(", Keys: ").append(indexLayout.getKeyCount())
                .append(", KeyType: ").append(indexLayout.getKeyType());
        }
        else if (layout instanceof ContainerMetadataPageLayout) {
            ContainerMetadataPageLayout metadataLayout = (ContainerMetadataPageLayout) layout;
            info.append("Tablespace: ").append(metadataLayout.getTablespaceName())
                .append(", PageSize: ").append(metadataLayout.getPageSize())
                .append(", TotalPages: ").append(metadataLayout.getTotalPages())
                .append(", FSM Page: ").append(metadataLayout.getFreeSpaceMapPageId())
                .append(", Created: ").append(metadataLayout.getCreationDate().toString());
        }
        else if (layout instanceof FreeSpaceMapPageLayout) {
            FreeSpaceMapPageLayout fsmLayout = (FreeSpaceMapPageLayout) layout;
            int capacity = fsmLayout.getBitmapCapacity();
            int freePages = fsmLayout.countFreePages();
            double freePercentage = (double) freePages / capacity * 100.0;
            info.append("Capacity: ").append(capacity).append(" pages")
                .append(", Free: ").append(freePages).append(" pages")
                .append(String.format(", %.2f%% available", freePercentage))
                .append(", LastChecked: ").append(fsmLayout.getLastCheckedPage());
        }
        else {
            info.append("No details available");
        }
        
        return info.toString();
    }
    
    /**
     * Executes a SHOW STATISTICS statement to show detailed buffer pool and page stats.
     *
     * @return The result of the statement showing buffer pool statistics
     */
    private String executeShowStatistics() {
        StringBuilder sb = new StringBuilder();
        
        // Get all buffer pool managers
        List<String> tablespacesNames = dbSystem.getStorageManager().getAllTablespaceNames();
        if (tablespacesNames.isEmpty()) {
            return "No tablespaces found.";
        }
        
        // Display detailed buffer pool statistics for each tablespace
        sb.append("BUFFER POOL STATISTICS:\n\n");
        
        for (String tablespaceName : tablespacesNames) {
            IBufferPoolManager bpm = dbSystem.getBufferPoolManager(tablespaceName);
            if (bpm == null) {
                continue;
            }
            
            // Get statistics using the new API
            Map<String, Object> stats = bpm.getStatistics();
            
            sb.append("Tablespace: ").append(tablespaceName).append("\n");
            sb.append(String.format("  Capacity: %d pages\n", stats.get("capacity")));
            sb.append(String.format("  Current Size: %d pages\n", stats.get("size")));
            sb.append(String.format("  Usage: %.2f%%\n", stats.get("usagePercentage")));
            sb.append(String.format("  Dirty Pages: %d\n", stats.get("dirtyPages")));
            
            // Performance statistics
            sb.append("\n  PERFORMANCE METRICS:\n");
            sb.append(String.format("  Page Hits: %d\n", stats.get("pageHits")));
            sb.append(String.format("  Page Misses: %d\n", stats.get("pageMisses")));
            sb.append(String.format("  Hit Ratio: %.2f%%\n", stats.get("hitRatio")));
            sb.append(String.format("  Page Evictions: %d\n", stats.get("pageEvictions")));
            sb.append(String.format("  Page Allocations: %d\n", stats.get("pageAllocations")));
            sb.append(String.format("  Page Flushes: %d\n", stats.get("pageFlushes")));
            
            // Page Cleaner statistics if available
            if (stats.containsKey("cleanerEnabled")) {
                sb.append("\n  PAGE CLEANER:\n");
                sb.append(String.format("  Enabled: %s\n", stats.get("cleanerEnabled")));
                sb.append(String.format("  Check Interval: %d ms\n", stats.get("cleanerInterval")));
                sb.append(String.format("  Dirty Page Threshold: %d\n", stats.get("dirtyPageThreshold")));
                
                long lastCleanTime = (long) stats.get("lastCleanTime");
                String lastCleanTimeStr = lastCleanTime > 0 ? 
                    new java.util.Date(lastCleanTime).toString() : "Never";
                sb.append(String.format("  Last Clean Time: %s\n", lastCleanTimeStr));
                sb.append(String.format("  Total Cleanings: %d\n", stats.get("totalCleanings")));
            }
            
            // Get page details using the new API
            Map<PageId, Map<String, Object>> pageDetails = bpm.getPageDetails();
            
            if (!pageDetails.isEmpty()) {
                sb.append("\n  LOADED PAGES:\n");
                sb.append(String.format("  %-10s %-10s %-10s %-10s\n", 
                        "PAGE_NO", "PIN COUNT", "DIRTY", "SIZE (B)"));
                sb.append(String.format("  %-10s %-10s %-10s %-10s\n", 
                        "----------", "----------", "----------", "----------"));
                
                for (Map.Entry<PageId, Map<String, Object>> entry : pageDetails.entrySet()) {
                    PageId pageId = entry.getKey();
                    Map<String, Object> details = entry.getValue();
                    
                    sb.append(String.format("  %-10d %-10d %-10s %-10d\n", 
                            details.get("pageNumber"), 
                            details.get("pinCount"), 
                            (Boolean)details.get("isDirty") ? "YES" : "NO", 
                            details.get("size")));
                }
            } else {
                sb.append("\n  No pages currently loaded.\n");
            }
            
            sb.append("\n");
        }
        
        // Storage statistics section
        sb.append("STORAGE STATISTICS:\n\n");
        
        StorageManager storageManager = dbSystem.getStorageManager();
        for (String tablespaceName : tablespacesNames) {
            Tablespace tablespace = storageManager.getTablespace(tablespaceName);
            if (tablespace == null) {
                continue;
            }
            
            int pageSize = tablespace.getPageSize();
            int totalPages;
            
            try {
                totalPages = tablespace.getTotalPages();
                long totalSizeBytes = (long) totalPages * pageSize;
                double totalSizeMB = totalSizeBytes / (1024.0 * 1024.0);
                
                sb.append("Tablespace: ").append(tablespaceName).append("\n");
                sb.append(String.format("  Container: %s\n", tablespace.getStorageContainer().getContainerPath()));
                sb.append(String.format("  Page Size: %d bytes\n", pageSize));
                sb.append(String.format("  Total Pages: %d\n", totalPages));
                sb.append(String.format("  Total Size: %.2f MB\n", totalSizeMB));
                sb.append("\n");
            } catch (IOException e) {
                sb.append("Tablespace: ").append(tablespaceName).append("\n");
                sb.append("  Error reading tablespace statistics: ").append(e.getMessage()).append("\n\n");
            }
        }
        
        return sb.toString();
    }
    
    /**
     * Executes a SHOW PINCOUNT statement to display pages with high pin counts.
     * This is useful for debugging page leaks in the buffer pool.
     *
     * @return Information about pages with high pin counts
     */
    private String executeShowPinCount() {
        return net.seitter.studiodb.buffer.PinTracker.debugHighPinCounts();
    }
    
    /**
     * Serializes a row into a byte array.
     *
     * @param row The row to serialize
     * @return The serialized row as a byte array
     * @throws IOException If there's an error serializing the row
     */
    private byte[] serializeRow(Map<String, Object> row) throws IOException {
        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        // Register JSR310 module for LocalDate support
        mapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
        return mapper.writeValueAsBytes(row);
    }
} 