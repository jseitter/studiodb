package net.seitter.studiodb.schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a B-Tree index on a database table.
 */
public class Index {
    private static final Logger logger = LoggerFactory.getLogger(Index.class);
    
    private final String name;
    private final String tableName;
    private final String tablespaceName;
    private final List<String> columnNames;
    private final boolean unique;
    private int rootPageId;
    
    /**
     * Creates a new index with the specified parameters.
     *
     * @param name The name of the index
     * @param tableName The name of the table this index belongs to
     * @param tablespaceName The name of the tablespace to store the index in
     * @param columnNames The names of the columns to index
     * @param unique Whether the index enforces uniqueness
     */
    public Index(String name, String tableName, String tablespaceName, List<String> columnNames, boolean unique) {
        this.name = name;
        this.tableName = tableName;
        this.tablespaceName = tablespaceName;
        this.columnNames = new ArrayList<>(columnNames);
        this.unique = unique;
        this.rootPageId = -1;
        
        logger.info("Created {} index '{}' on table '{}' for columns: {}", 
                unique ? "unique" : "non-unique", name, tableName, columnNames);
    }
    
    /**
     * Gets the name of the index.
     *
     * @return The index name
     */
    public String getName() {
        return name;
    }
    
    /**
     * Gets the name of the table this index belongs to.
     *
     * @return The table name
     */
    public String getTableName() {
        return tableName;
    }
    
    /**
     * Gets the name of the tablespace where the index is stored.
     *
     * @return The tablespace name
     */
    public String getTablespaceName() {
        return tablespaceName;
    }
    
    /**
     * Gets the names of the columns this index indexes.
     *
     * @return The list of column names
     */
    public List<String> getColumnNames() {
        return new ArrayList<>(columnNames);
    }
    
    /**
     * Checks if the index enforces uniqueness.
     *
     * @return true if the index is unique
     */
    public boolean isUnique() {
        return unique;
    }
    
    /**
     * Gets the ID of the root page of the B-Tree.
     *
     * @return The root page ID
     */
    public int getRootPageId() {
        return rootPageId;
    }
    
    /**
     * Sets the ID of the root page of the B-Tree.
     *
     * @param rootPageId The root page ID
     */
    public void setRootPageId(int rootPageId) {
        this.rootPageId = rootPageId;
        logger.debug("Set root page ID for index '{}' to {}", name, rootPageId);
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(unique ? "UNIQUE " : "").append("INDEX: ").append(name);
        sb.append(" ON ").append(tableName).append(" (");
        sb.append(String.join(", ", columnNames)).append(")");
        return sb.toString();
    }
} 