package net.seitter.studiodb.schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a database table with its schema and storage information.
 */
public class Table {
    private static final Logger logger = LoggerFactory.getLogger(Table.class);
    
    private final String name;
    private final String tablespaceName;
    private final List<Column> columns;
    private final Map<String, Integer> columnIndexMap;
    private final List<String> primaryKey;
    private int rowSize;
    private int headerPageId;
    private int firstDataPageId;
    
    /**
     * Creates a new table with the specified parameters.
     *
     * @param name The name of the table
     * @param tablespaceName The name of the tablespace to store the table in
     */
    public Table(String name, String tablespaceName) {
        this.name = name;
        this.tablespaceName = tablespaceName;
        this.columns = new ArrayList<>();
        this.columnIndexMap = new HashMap<>();
        this.primaryKey = new ArrayList<>();
        this.rowSize = 0;
        this.headerPageId = -1;
        this.firstDataPageId = -1;
        
        logger.info("Created table '{}' in tablespace '{}'", name, tablespaceName);
    }
    
    /**
     * Adds a column to the table.
     *
     * @param column The column to add
     * @return true if the column was added successfully
     */
    public boolean addColumn(Column column) {
        if (columnIndexMap.containsKey(column.getName())) {
            logger.warn("Column '{}' already exists in table '{}'", column.getName(), name);
            return false;
        }
        
        columns.add(column);
        columnIndexMap.put(column.getName(), columns.size() - 1);
        rowSize += column.getSizeInBytes();
        
        logger.info("Added column '{}' to table '{}'", column.getName(), name);
        return true;
    }
    
    /**
     * Adds multiple columns to the table.
     *
     * @param columnsToAdd The columns to add
     * @return true if all columns were added successfully
     */
    public boolean addColumns(List<Column> columnsToAdd) {
        boolean allAdded = true;
        
        for (Column column : columnsToAdd) {
            boolean added = addColumn(column);
            allAdded = allAdded && added;
        }
        
        return allAdded;
    }
    
    /**
     * Sets the primary key for the table.
     *
     * @param columnNames The names of the columns that form the primary key
     * @return true if the primary key was set successfully
     */
    public boolean setPrimaryKey(List<String> columnNames) {
        // Verify all columns exist
        for (String columnName : columnNames) {
            if (!columnIndexMap.containsKey(columnName)) {
                logger.warn("Cannot set primary key: Column '{}' does not exist in table '{}'", 
                        columnName, name);
                return false;
            }
        }
        
        this.primaryKey.clear();
        this.primaryKey.addAll(columnNames);
        
        logger.info("Set primary key for table '{}': {}", name, primaryKey);
        return true;
    }
    
    /**
     * Gets the name of the table.
     *
     * @return The table name
     */
    public String getName() {
        return name;
    }
    
    /**
     * Gets the name of the tablespace where the table is stored.
     *
     * @return The tablespace name
     */
    public String getTablespaceName() {
        return tablespaceName;
    }
    
    /**
     * Gets the columns of the table.
     *
     * @return The list of columns
     */
    public List<Column> getColumns() {
        return new ArrayList<>(columns);
    }
    
    /**
     * Gets a column by its name.
     *
     * @param columnName The name of the column
     * @return The column, or null if not found
     */
    public Column getColumn(String columnName) {
        Integer index = columnIndexMap.get(columnName);
        
        if (index == null) {
            return null;
        }
        
        return columns.get(index);
    }
    
    /**
     * Gets the index of a column by its name.
     *
     * @param columnName The name of the column
     * @return The index of the column, or -1 if not found
     */
    public int getColumnIndex(String columnName) {
        Integer index = columnIndexMap.get(columnName);
        return index != null ? index : -1;
    }
    
    /**
     * Gets the primary key columns.
     *
     * @return The list of primary key column names
     */
    public List<String> getPrimaryKey() {
        return new ArrayList<>(primaryKey);
    }
    
    /**
     * Gets the size of a row in bytes.
     *
     * @return The row size
     */
    public int getRowSize() {
        return rowSize;
    }
    
    /**
     * Gets the ID of the header page.
     *
     * @return The header page ID
     */
    public int getHeaderPageId() {
        return headerPageId;
    }
    
    /**
     * Sets the ID of the header page.
     *
     * @param headerPageId The header page ID
     */
    public void setHeaderPageId(int headerPageId) {
        this.headerPageId = headerPageId;
        logger.debug("Set header page ID for table '{}' to {}", name, headerPageId);
    }
    
    /**
     * Gets the ID of the first data page.
     *
     * @return The first data page ID
     */
    public int getFirstDataPageId() {
        return firstDataPageId;
    }
    
    /**
     * Sets the ID of the first data page.
     *
     * @param firstDataPageId The first data page ID
     */
    public void setFirstDataPageId(int firstDataPageId) {
        this.firstDataPageId = firstDataPageId;
        logger.debug("Set first data page ID for table '{}' to {}", name, firstDataPageId);
    }
    
    /**
     * Validates a row of data against the table schema.
     *
     * @param row The map of column names to values
     * @return true if the row is valid for this table schema
     */
    public boolean validateRow(Map<String, Object> row) {
        // Check that all non-nullable columns have values
        for (Column column : columns) {
            if (!column.isNullable() && !row.containsKey(column.getName())) {
                logger.warn("Validation failed: No value provided for non-nullable column '{}'", 
                        column.getName());
                return false;
            }
        }
        
        // Check that all values have valid types
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            String columnName = entry.getKey();
            Object value = entry.getValue();
            
            Column column = getColumn(columnName);
            
            if (column == null) {
                logger.warn("Validation failed: Column '{}' does not exist in table '{}'", 
                        columnName, name);
                return false;
            }
            
            if (!column.getDataType().validateValue(value)) {
                logger.warn("Validation failed: Value '{}' is not valid for column '{}' with type '{}'", 
                        value, columnName, column.getDataType());
                return false;
            }
        }
        
        return true;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Table: ").append(name).append(" (").append(tablespaceName).append(")\n");
        sb.append("Columns:\n");
        
        for (Column column : columns) {
            sb.append("  ").append(column.toString()).append("\n");
        }
        
        if (!primaryKey.isEmpty()) {
            sb.append("Primary Key: ").append(String.join(", ", primaryKey)).append("\n");
        }
        
        return sb.toString();
    }
} 