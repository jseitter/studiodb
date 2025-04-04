package net.seitter.studiodb.schema;

import net.seitter.studiodb.DatabaseSystem;
import net.seitter.studiodb.buffer.BufferPoolManager;
import net.seitter.studiodb.storage.Page;
import net.seitter.studiodb.storage.PageId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Manages database objects like tables and indexes, and their schema information.
 */
public class SchemaManager {
    private static final Logger logger = LoggerFactory.getLogger(SchemaManager.class);
    
    private final DatabaseSystem dbSystem;
    private final Map<String, Table> tables;
    private final Map<String, Index> indexes;
    private final Map<String, List<Index>> tableIndexes;
    
    // System catalog table names
    private static final String SYSTEM_TABLESPACE = "SYSTEM";
    private static final String SYS_TABLESPACES = "SYS_TABLESPACES";
    private static final String SYS_TABLES = "SYS_TABLES";
    private static final String SYS_COLUMNS = "SYS_COLUMNS";
    private static final String SYS_INDEXES = "SYS_INDEXES";
    private static final String SYS_INDEX_COLUMNS = "SYS_INDEX_COLUMNS";
    
    /**
     * Creates a new schema manager.
     *
     * @param dbSystem The database system
     */
    public SchemaManager(DatabaseSystem dbSystem) {
        this.dbSystem = dbSystem;
        this.tables = new HashMap<>();
        this.indexes = new HashMap<>();
        this.tableIndexes = new HashMap<>();
        
        // Check if system tablespace exists
        if (dbSystem.getBufferPoolManager(SYSTEM_TABLESPACE) == null) {
            logger.error("System tablespace does not exist, system catalog initialization will fail");
            return;
        }
        
        // Initialize system catalog - this creates the system tables if they don't exist
        boolean catalogInitialized = initializeSystemCatalog();
        
        // Load existing schema from system catalog if initialization was successful
        if (catalogInitialized) {
            loadSchemaFromCatalog();
        } else {
            logger.warn("System catalog initialization failed, schema loading skipped");
        }
        
        logger.info("Schema manager initialized");
    }
    
    /**
     * Initializes the system catalog tables if they don't exist.
     * 
     * @return true if initialization was successful
     */
    private boolean initializeSystemCatalog() {
        try {
            // Verify the system tablespace exists
            BufferPoolManager systemBufferPool = dbSystem.getBufferPoolManager(SYSTEM_TABLESPACE);
            if (systemBufferPool == null) {
                logger.error("Cannot initialize system catalog: system tablespace not found");
                return false;
            }
            
            boolean allTablesCreated = true;
            
            // Create SYS_TABLESPACES if it doesn't exist
            if (!tableExists(SYS_TABLESPACES)) {
                List<Column> tablespaceColumns = new ArrayList<>();
                tablespaceColumns.add(new Column("TABLESPACE_NAME", DataType.VARCHAR, false, 128));
                tablespaceColumns.add(new Column("CONTAINER_PATH", DataType.VARCHAR, false, 512));
                tablespaceColumns.add(new Column("PAGE_SIZE", DataType.INTEGER, false));
                tablespaceColumns.add(new Column("CREATION_TIME", DataType.INTEGER, false));
                
                List<String> tablespacePK = new ArrayList<>();
                tablespacePK.add("TABLESPACE_NAME");
                
                Table sysTablespacesTable = createSystemTable(SYS_TABLESPACES, tablespaceColumns, tablespacePK);
                if (sysTablespacesTable != null) {
                    logger.info("Created system catalog table: {}", SYS_TABLESPACES);
                } else {
                    logger.error("Failed to create system catalog table: {}", SYS_TABLESPACES);
                    allTablesCreated = false;
                }
            }
            
            // Create SYS_TABLES if it doesn't exist
            if (!tableExists(SYS_TABLES)) {
                List<Column> tableColumns = new ArrayList<>();
                tableColumns.add(new Column("TABLE_NAME", DataType.VARCHAR, false, 128));
                tableColumns.add(new Column("TABLESPACE_NAME", DataType.VARCHAR, false, 128));
                tableColumns.add(new Column("HEADER_PAGE_ID", DataType.INTEGER, false));
                tableColumns.add(new Column("FIRST_DATA_PAGE_ID", DataType.INTEGER, false));
                
                List<String> tablePK = new ArrayList<>();
                tablePK.add("TABLE_NAME");
                
                Table sysTable = createSystemTable(SYS_TABLES, tableColumns, tablePK);
                if (sysTable != null) {
                    logger.info("Created system catalog table: {}", SYS_TABLES);
                } else {
                    logger.error("Failed to create system catalog table: {}", SYS_TABLES);
                    allTablesCreated = false;
                }
            }
            
            // Only continue if SYS_TABLES was created successfully
            if (!tableExists(SYS_TABLES)) {
                logger.error("SYS_TABLES does not exist, cannot create other system tables");
                return false;
            }
            
            // Create SYS_COLUMNS if it doesn't exist
            if (!tableExists(SYS_COLUMNS)) {
                List<Column> columnColumns = new ArrayList<>();
                columnColumns.add(new Column("TABLE_NAME", DataType.VARCHAR, false, 128));
                columnColumns.add(new Column("COLUMN_NAME", DataType.VARCHAR, false, 128));
                columnColumns.add(new Column("COLUMN_POSITION", DataType.INTEGER, false));
                columnColumns.add(new Column("DATA_TYPE", DataType.INTEGER, false));
                columnColumns.add(new Column("NULLABLE", DataType.BOOLEAN, false));
                columnColumns.add(new Column("MAX_LENGTH", DataType.INTEGER, true));
                columnColumns.add(new Column("IS_PRIMARY_KEY", DataType.BOOLEAN, false));
                
                List<String> columnPK = new ArrayList<>();
                columnPK.add("TABLE_NAME");
                columnPK.add("COLUMN_NAME");
                
                Table sysColumnsTable = createSystemTable(SYS_COLUMNS, columnColumns, columnPK);
                if (sysColumnsTable != null) {
                    logger.info("Created system catalog table: {}", SYS_COLUMNS);
                } else {
                    logger.error("Failed to create system catalog table: {}", SYS_COLUMNS);
                    allTablesCreated = false;
                }
            }
            
            // Create SYS_INDEXES if it doesn't exist
            if (!tableExists(SYS_INDEXES)) {
                List<Column> indexColumns = new ArrayList<>();
                indexColumns.add(new Column("INDEX_NAME", DataType.VARCHAR, false, 128));
                indexColumns.add(new Column("TABLE_NAME", DataType.VARCHAR, false, 128));
                indexColumns.add(new Column("TABLESPACE_NAME", DataType.VARCHAR, false, 128));
                indexColumns.add(new Column("UNIQUE_FLAG", DataType.BOOLEAN, false));
                indexColumns.add(new Column("ROOT_PAGE_ID", DataType.INTEGER, false));
                
                List<String> indexPK = new ArrayList<>();
                indexPK.add("INDEX_NAME");
                
                Table sysIndexesTable = createSystemTable(SYS_INDEXES, indexColumns, indexPK);
                if (sysIndexesTable != null) {
                    logger.info("Created system catalog table: {}", SYS_INDEXES);
                } else {
                    logger.error("Failed to create system catalog table: {}", SYS_INDEXES);
                    allTablesCreated = false;
                }
            }
            
            // Create SYS_INDEX_COLUMNS if it doesn't exist
            if (!tableExists(SYS_INDEX_COLUMNS)) {
                List<Column> indexColColumns = new ArrayList<>();
                indexColColumns.add(new Column("INDEX_NAME", DataType.VARCHAR, false, 128));
                indexColColumns.add(new Column("COLUMN_NAME", DataType.VARCHAR, false, 128));
                indexColColumns.add(new Column("COLUMN_POSITION", DataType.INTEGER, false));
                
                List<String> indexColPK = new ArrayList<>();
                indexColPK.add("INDEX_NAME");
                indexColPK.add("COLUMN_NAME");
                
                Table sysIndexColumnsTable = createSystemTable(SYS_INDEX_COLUMNS, indexColColumns, indexColPK);
                if (sysIndexColumnsTable != null) {
                    logger.info("Created system catalog table: {}", SYS_INDEX_COLUMNS);
                } else {
                    logger.error("Failed to create system catalog table: {}", SYS_INDEX_COLUMNS);
                    allTablesCreated = false;
                }
            }
            
            return allTablesCreated;
        } catch (Exception e) {
            logger.error("Failed to initialize system catalog", e);
            return false;
        }
    }
    
    /**
     * Checks if a table exists in the internal cache.
     * Used during initialization to check if system tables need to be created.
     *
     * @param tableName The table name
     * @return true if the table exists
     */
    private boolean tableExists(String tableName) {
        return tables.containsKey(tableName);
    }
    
    /**
     * Creates a system table bypassing the normal table creation process.
     * This is used for creating the system catalog tables themselves.
     *
     * @param tableName The name of the table
     * @param columns The columns of the table
     * @param primaryKeyColumns The names of the columns that form the primary key
     * @return The created table
     */
    private Table createSystemTable(String tableName, List<Column> columns, List<String> primaryKeyColumns) {
        try {
            // Create the table object
            Table table = new Table(tableName, SYSTEM_TABLESPACE);
            table.addColumns(columns);
            
            if (primaryKeyColumns != null && !primaryKeyColumns.isEmpty()) {
                table.setPrimaryKey(primaryKeyColumns);
            }
            
            // Allocate header page
            BufferPoolManager bufferPool = dbSystem.getBufferPoolManager(SYSTEM_TABLESPACE);
            
            if (bufferPool == null) {
                logger.error("System tablespace does not exist");
                return null;
            }
            
            Page headerPage = bufferPool.allocatePage();
            table.setHeaderPageId(headerPage.getPageId().getPageNumber());
            
            // Initialize header page with table info
            initializeTableHeaderPage(headerPage, table);
            
            // Release the page
            bufferPool.unpinPage(headerPage.getPageId(), true);
            
            // Allocate first data page
            Page firstDataPage = bufferPool.allocatePage();
            table.setFirstDataPageId(firstDataPage.getPageId().getPageNumber());
            
            // Initialize the data page
            initializeTableDataPage(firstDataPage);
            
            // Release the page
            bufferPool.unpinPage(firstDataPage.getPageId(), true);
            
            // Update the header page with the first data page ID
            updateHeaderPageWithFirstDataPageId(headerPage.getPageId(), table.getFirstDataPageId(), bufferPool);
            
            // Store the table in our schema
            tables.put(tableName, table);
            tableIndexes.put(tableName, new ArrayList<>());
            
            logger.info("Created system table '{}' in tablespace '{}'", 
                    tableName, SYSTEM_TABLESPACE);
            return table;
        } catch (IOException e) {
            logger.error("Failed to create system table '{}'", tableName, e);
            return null;
        }
    }
    
    /**
     * Loads the database schema from the system catalog.
     */
    private void loadSchemaFromCatalog() {
        try {
            // First check if the system catalog tables exist
            if (!tables.containsKey(SYS_TABLESPACES) ||
                !tables.containsKey(SYS_TABLES) || 
                !tables.containsKey(SYS_COLUMNS) ||
                !tables.containsKey(SYS_INDEXES) ||
                !tables.containsKey(SYS_INDEX_COLUMNS)) {
                logger.warn("Some system catalog tables not found, schema not loaded");
                logger.debug("Available tables: {}", tables.keySet());
                return;
            }
            
            // Verify the system buffer pool is available
            BufferPoolManager systemBufferPool = dbSystem.getBufferPoolManager(SYSTEM_TABLESPACE);
            if (systemBufferPool == null) {
                logger.error("Cannot load schema from catalog: system buffer pool not available");
                return;
            }
            
            // Load tablespaces
            boolean tablespacesLoaded = loadTablespacesFromCatalog();
            if (!tablespacesLoaded) {
                logger.warn("Failed to load tablespaces from catalog");
            }
            
            // Load tables
            boolean tablesLoaded = loadTablesFromCatalog();
            
            // Only load indexes if tables were loaded successfully
            if (tablesLoaded) {
                boolean indexesLoaded = loadIndexesFromCatalog();
                if (!indexesLoaded) {
                    logger.warn("Failed to load indexes from catalog");
                }
                
                logger.info("Schema loaded from system catalog");
            } else {
                logger.warn("Failed to load tables from catalog");
            }
        } catch (Exception e) {
            logger.error("Failed to load schema from system catalog", e);
        }
    }
    
    /**
     * Loads tables from the system catalog.
     * 
     * @return true if loading was successful
     */
    private boolean loadTablesFromCatalog() {
        // For now, we're just defining the interface
        // The actual implementation would involve reading from SYS_TABLES and SYS_COLUMNS
        // and reconstructing the Table objects
        
        // This would be completed in a future implementation
        logger.info("Table loading from catalog not fully implemented yet");
        return true; // For now, pretend it succeeded
    }
    
    /**
     * Loads indexes from the system catalog.
     * 
     * @return true if loading was successful
     */
    private boolean loadIndexesFromCatalog() {
        // For now, we're just defining the interface
        // The actual implementation would involve reading from SYS_INDEXES and SYS_INDEX_COLUMNS
        // and reconstructing the Index objects
        
        // This would be completed in a future implementation
        logger.info("Index loading from catalog not fully implemented yet");
        return true; // For now, pretend it succeeded
    }
    
    /**
     * Loads tablespaces from the system catalog.
     * 
     * @return true if loading was successful
     */
    private boolean loadTablespacesFromCatalog() {
        // For now, we're just defining the interface
        // The actual implementation would involve reading from SYS_TABLESPACES
        // and reconstruct existing tablespaces
        
        // This would be completed in a future implementation
        logger.info("Tablespace loading from catalog not fully implemented yet");
        return true;
    }
    
    /**
     * Persists a table's metadata to the system catalog.
     *
     * @param table The table to persist
     */
    private void persistTableToCatalog(Table table) {
        try {
            // Insert into SYS_TABLES
            Map<String, Object> tableRow = new HashMap<>();
            tableRow.put("TABLE_NAME", table.getName());
            tableRow.put("TABLESPACE_NAME", table.getTablespaceName());
            tableRow.put("HEADER_PAGE_ID", table.getHeaderPageId());
            tableRow.put("FIRST_DATA_PAGE_ID", table.getFirstDataPageId());
            
            insertIntoSystemTable(SYS_TABLES, tableRow);
            
            // Insert each column into SYS_COLUMNS
            List<Column> columns = table.getColumns();
            List<String> primaryKeyColumns = table.getPrimaryKey();
            
            for (int i = 0; i < columns.size(); i++) {
                Column column = columns.get(i);
                boolean isPrimaryKey = primaryKeyColumns.contains(column.getName());
                
                Map<String, Object> columnRow = new HashMap<>();
                columnRow.put("TABLE_NAME", table.getName());
                columnRow.put("COLUMN_NAME", column.getName());
                columnRow.put("COLUMN_POSITION", i);
                columnRow.put("DATA_TYPE", column.getDataType().ordinal());
                columnRow.put("NULLABLE", column.isNullable());
                columnRow.put("MAX_LENGTH", column.getMaxLength());
                columnRow.put("IS_PRIMARY_KEY", isPrimaryKey);
                
                insertIntoSystemTable(SYS_COLUMNS, columnRow);
            }
            
            logger.debug("Persisted table '{}' to system catalog", table.getName());
        } catch (Exception e) {
            logger.error("Failed to persist table '{}' to system catalog", table.getName(), e);
        }
    }
    
    /**
     * Persists an index's metadata to the system catalog.
     *
     * @param index The index to persist
     */
    private void persistIndexToCatalog(Index index) {
        try {
            // Insert into SYS_INDEXES
            Map<String, Object> indexRow = new HashMap<>();
            indexRow.put("INDEX_NAME", index.getName());
            indexRow.put("TABLE_NAME", index.getTableName());
            indexRow.put("TABLESPACE_NAME", index.getTablespaceName());
            indexRow.put("UNIQUE_FLAG", index.isUnique());
            indexRow.put("ROOT_PAGE_ID", index.getRootPageId());
            
            insertIntoSystemTable(SYS_INDEXES, indexRow);
            
            // Insert each column into SYS_INDEX_COLUMNS
            List<String> columns = index.getColumnNames();
            
            for (int i = 0; i < columns.size(); i++) {
                String columnName = columns.get(i);
                
                Map<String, Object> columnRow = new HashMap<>();
                columnRow.put("INDEX_NAME", index.getName());
                columnRow.put("COLUMN_NAME", columnName);
                columnRow.put("COLUMN_POSITION", i);
                
                insertIntoSystemTable(SYS_INDEX_COLUMNS, columnRow);
            }
            
            logger.debug("Persisted index '{}' to system catalog", index.getName());
        } catch (Exception e) {
            logger.error("Failed to persist index '{}' to system catalog", index.getName(), e);
        }
    }
    
    /**
     * Inserts a row into a system table.
     * This is a simplified implementation for educational purposes.
     *
     * @param tableName The name of the system table
     * @param row The row to insert
     */
    private void insertIntoSystemTable(String tableName, Map<String, Object> row) {
        // For now, we're just defining the interface
        // The actual implementation would involve writing to the system tables
        
        // This would be completed in a future implementation
        logger.debug("Inserting into system table {} not fully implemented yet", tableName);
    }
    
    /**
     * Removes a table's metadata from the system catalog.
     *
     * @param tableName The name of the table to remove
     */
    private void removeTableFromCatalog(String tableName) {
        // For now, we're just defining the interface
        // The actual implementation would involve deleting from SYS_TABLES and SYS_COLUMNS
        
        // This would be completed in a future implementation
        logger.debug("Removing table {} from catalog not fully implemented yet", tableName);
    }
    
    /**
     * Removes an index's metadata from the system catalog.
     *
     * @param indexName The name of the index to remove
     */
    private void removeIndexFromCatalog(String indexName) {
        // For now, we're just defining the interface
        // The actual implementation would involve deleting from SYS_INDEXES and SYS_INDEX_COLUMNS
        
        // This would be completed in a future implementation
        logger.debug("Removing index {} from catalog not fully implemented yet", indexName);
    }

    /**
     * Creates a new table with the specified columns.
     *
     * @param tableName The name of the table
     * @param tablespaceName The name of the tablespace to store the table in
     * @param columns The columns of the table
     * @param primaryKeyColumns The names of the columns that form the primary key
     * @return The created table, or null if creation failed
     */
    public Table createTable(String tableName, String tablespaceName, List<Column> columns, 
                            List<String> primaryKeyColumns) {
        if (tables.containsKey(tableName)) {
            logger.warn("Table '{}' already exists", tableName);
            return null;
        }
        
        try {
            // Create the table object
            Table table = new Table(tableName, tablespaceName);
            table.addColumns(columns);
            
            if (primaryKeyColumns != null && !primaryKeyColumns.isEmpty()) {
                table.setPrimaryKey(primaryKeyColumns);
            }
            
            // Allocate header page
            BufferPoolManager bufferPool = dbSystem.getBufferPoolManager(tablespaceName);
            
            if (bufferPool == null) {
                logger.error("Tablespace '{}' does not exist", tablespaceName);
                return null;
            }
            
            Page headerPage = bufferPool.allocatePage();
            table.setHeaderPageId(headerPage.getPageId().getPageNumber());
            
            // Initialize header page with table info
            initializeTableHeaderPage(headerPage, table);
            
            // Release the page
            bufferPool.unpinPage(headerPage.getPageId(), true);
            
            // Allocate first data page
            Page firstDataPage = bufferPool.allocatePage();
            table.setFirstDataPageId(firstDataPage.getPageId().getPageNumber());
            
            // Initialize the data page
            initializeTableDataPage(firstDataPage);
            
            // Release the page
            bufferPool.unpinPage(firstDataPage.getPageId(), true);
            
            // Update the header page with the first data page ID
            updateHeaderPageWithFirstDataPageId(headerPage.getPageId(), table.getFirstDataPageId(), bufferPool);
            
            // Store the table in our schema
            tables.put(tableName, table);
            tableIndexes.put(tableName, new ArrayList<>());
            
            // Persist to system catalog
            persistTableToCatalog(table);
            
            logger.info("Created table '{}' in tablespace '{}' with {} columns", 
                    tableName, tablespaceName, columns.size());
            return table;
        } catch (IOException e) {
            logger.error("Failed to create table '{}'", tableName, e);
            return null;
        }
    }
    
    /**
     * Initializes a table header page with the table schema information.
     *
     * @param headerPage The header page to initialize
     * @param table The table
     */
    private void initializeTableHeaderPage(Page headerPage, Table table) {
        ByteBuffer buffer = headerPage.getBuffer();
        
        // Basic format of the header page:
        // [Magic Number (4 bytes)] [Table Name Length (4 bytes)] [Table Name (variable)]
        // [Number of Columns (4 bytes)] [Column Definitions...] [First Data Page ID (4 bytes)]
        
        // Write magic number
        buffer.putInt(0xDADA0101); // Magic number for table header page
        
        // Write table name
        String tableName = table.getName();
        buffer.putInt(tableName.length());
        for (int i = 0; i < tableName.length(); i++) {
            buffer.putChar(tableName.charAt(i));
        }
        
        // Write number of columns
        List<Column> columns = table.getColumns();
        buffer.putInt(columns.size());
        
        // Write column definitions
        for (Column column : columns) {
            // Column format: [Name Length (4 bytes)] [Name (variable)] [Data Type (4 bytes)]
            // [Nullable (1 byte)] [Max Length (4 bytes)]
            String columnName = column.getName();
            buffer.putInt(columnName.length());
            for (int i = 0; i < columnName.length(); i++) {
                buffer.putChar(columnName.charAt(i));
            }
            
            buffer.putInt(column.getDataType().ordinal());
            buffer.put(column.isNullable() ? (byte) 1 : (byte) 0);
            buffer.putInt(column.getMaxLength());
        }
        
        // Space for first data page ID (will be updated later)
        buffer.putInt(-1);
        
        headerPage.markDirty();
    }
    
    /**
     * Updates the header page with the first data page ID.
     *
     * @param headerPageId The ID of the header page
     * @param firstDataPageId The ID of the first data page
     * @param bufferPool The buffer pool manager
     * @throws IOException If there's an error accessing the page
     */
    private void updateHeaderPageWithFirstDataPageId(PageId headerPageId, int firstDataPageId, 
                                                    BufferPoolManager bufferPool) throws IOException {
        Page headerPage = bufferPool.fetchPage(headerPageId);
        ByteBuffer buffer = headerPage.getBuffer();
        
        // Skip to the first data page ID field (at the end of the header page)
        int magic = buffer.getInt(); // Magic number
        int tableNameLength = buffer.getInt();
        buffer.position(buffer.position() + tableNameLength * 2); // Skip table name (2 bytes per char)
        
        int numColumns = buffer.getInt();
        for (int i = 0; i < numColumns; i++) {
            int columnNameLength = buffer.getInt();
            buffer.position(buffer.position() + columnNameLength * 2); // Skip column name
            buffer.getInt(); // Data type
            buffer.get(); // Nullable
            buffer.getInt(); // Max length
        }
        
        // Now at the first data page ID field
        buffer.putInt(firstDataPageId);
        
        headerPage.markDirty();
        bufferPool.unpinPage(headerPageId, true);
    }
    
    /**
     * Initializes a table data page.
     *
     * @param dataPage The data page to initialize
     */
    private void initializeTableDataPage(Page dataPage) {
        ByteBuffer buffer = dataPage.getBuffer();
        
        // Basic format of a data page:
        // [Magic Number (4 bytes)] [Next Page ID (4 bytes)] [Number of Rows (4 bytes)]
        // [Free Space Offset (4 bytes)] [Row Directory...] [Row Data...]
        
        // Write magic number
        buffer.putInt(0xDADA0201); // Magic number for table data page
        
        // No next page yet
        buffer.putInt(-1);
        
        // No rows yet
        buffer.putInt(0);
        
        // Free space starts after the header
        buffer.putInt(16); // 4 (magic) + 4 (next page) + 4 (num rows) + 4 (free space offset)
        
        dataPage.markDirty();
    }
    
    /**
     * Creates a new index on a table.
     *
     * @param indexName The name of the index
     * @param tableName The name of the table to index
     * @param columnNames The names of the columns to index
     * @param unique Whether the index should enforce uniqueness
     * @return The created index, or null if creation failed
     */
    public Index createIndex(String indexName, String tableName, List<String> columnNames, boolean unique) {
        if (indexes.containsKey(indexName)) {
            logger.warn("Index '{}' already exists", indexName);
            return null;
        }
        
        Table table = tables.get(tableName);
        
        if (table == null) {
            logger.warn("Table '{}' does not exist", tableName);
            return null;
        }
        
        // Verify all columns exist in the table
        for (String columnName : columnNames) {
            if (table.getColumnIndex(columnName) == -1) {
                logger.warn("Column '{}' does not exist in table '{}'", columnName, tableName);
                return null;
            }
        }
        
        try {
            String tablespaceName = table.getTablespaceName();
            
            // Create the index object
            Index index = new Index(indexName, tableName, tablespaceName, columnNames, unique);
            
            // Allocate root page for the B-Tree
            BufferPoolManager bufferPool = dbSystem.getBufferPoolManager(tablespaceName);
            
            if (bufferPool == null) {
                logger.error("Tablespace '{}' does not exist", tablespaceName);
                return null;
            }
            
            Page rootPage = bufferPool.allocatePage();
            index.setRootPageId(rootPage.getPageId().getPageNumber());
            
            // Initialize B-Tree root page
            initializeBTreeRootPage(rootPage, index);
            
            // Release the page
            bufferPool.unpinPage(rootPage.getPageId(), true);
            
            // Store the index in our schema
            indexes.put(indexName, index);
            
            // Add to table's index list
            List<Index> tableIndexList = tableIndexes.get(tableName);
            tableIndexList.add(index);
            
            // Persist to system catalog
            persistIndexToCatalog(index);
            
            logger.info("Created {} index '{}' on table '{}' for columns: {}", 
                    unique ? "unique" : "non-unique", indexName, tableName, columnNames);
            return index;
        } catch (IOException e) {
            logger.error("Failed to create index '{}'", indexName, e);
            return null;
        }
    }
    
    /**
     * Initializes a B-Tree root page.
     *
     * @param rootPage The root page to initialize
     * @param index The index
     */
    private void initializeBTreeRootPage(Page rootPage, Index index) {
        ByteBuffer buffer = rootPage.getBuffer();
        
        // Basic format of a B-Tree page:
        // [Magic Number (4 bytes)] [Is Leaf (1 byte)] [Number of Keys (4 bytes)]
        // [Key Type (4 bytes)] [Key Entries...] [Child Pointers...]
        
        // Write magic number
        buffer.putInt(0xDADA0301); // Magic number for B-Tree page
        
        // This is a leaf node (initially)
        buffer.put((byte) 1);
        
        // No keys yet
        buffer.putInt(0);
        
        // Store the key type based on the first indexed column
        Table table = tables.get(index.getTableName());
        String firstColumnName = index.getColumnNames().get(0);
        Column firstColumn = table.getColumn(firstColumnName);
        buffer.putInt(firstColumn.getDataType().ordinal());
        
        rootPage.markDirty();
    }
    
    /**
     * Gets a table by name.
     *
     * @param tableName The name of the table
     * @return The table, or null if not found
     */
    public Table getTable(String tableName) {
        return tables.get(tableName);
    }
    
    /**
     * Gets all tables.
     *
     * @return A list of all tables
     */
    public List<Table> getAllTables() {
        return new ArrayList<>(tables.values());
    }
    
    /**
     * Gets an index by name.
     *
     * @param indexName The name of the index
     * @return The index, or null if not found
     */
    public Index getIndex(String indexName) {
        return indexes.get(indexName);
    }
    
    /**
     * Gets all indexes for a table.
     *
     * @param tableName The name of the table
     * @return A list of indexes for the table, or an empty list if the table has no indexes
     */
    public List<Index> getIndexesForTable(String tableName) {
        List<Index> result = tableIndexes.get(tableName);
        return result != null ? new ArrayList<>(result) : new ArrayList<>();
    }
    
    /**
     * Gets all indexes.
     *
     * @return A list of all indexes
     */
    public List<Index> getAllIndexes() {
        return new ArrayList<>(indexes.values());
    }
    
    /**
     * Drops a table.
     *
     * @param tableName The name of the table to drop
     * @return true if the table was dropped successfully
     */
    public boolean dropTable(String tableName) {
        // Don't allow dropping system tables
        if (tableName.startsWith("SYS_")) {
            logger.warn("Cannot drop system table '{}'", tableName);
            return false;
        }
        
        Table table = tables.remove(tableName);
        
        if (table == null) {
            logger.warn("Table '{}' does not exist", tableName);
            return false;
        }
        
        // Drop all indexes on this table
        List<Index> indexList = tableIndexes.remove(tableName);
        
        if (indexList != null) {
            for (Index index : indexList) {
                indexes.remove(index.getName());
                
                // Remove from system catalog
                removeIndexFromCatalog(index.getName());
                
                logger.info("Dropped index '{}' as part of dropping table '{}'", 
                        index.getName(), tableName);
            }
        }
        
        // Remove from system catalog
        removeTableFromCatalog(tableName);
        
        logger.info("Dropped table '{}'", tableName);
        return true;
    }
    
    /**
     * Drops an index.
     *
     * @param indexName The name of the index to drop
     * @return true if the index was dropped successfully
     */
    public boolean dropIndex(String indexName) {
        Index index = indexes.remove(indexName);
        
        if (index == null) {
            logger.warn("Index '{}' does not exist", indexName);
            return false;
        }
        
        // Remove from table's index list
        List<Index> tableIndexList = tableIndexes.get(index.getTableName());
        
        if (tableIndexList != null) {
            tableIndexList.remove(index);
        }
        
        // Remove from system catalog
        removeIndexFromCatalog(indexName);
        
        logger.info("Dropped index '{}'", indexName);
        return true;
    }
    
    /**
     * Persists a tablespace's metadata to the system catalog.
     *
     * @param tablespaceName The name of the tablespace
     * @param containerPath The container path
     * @param pageSize The page size
     */
    public void persistTablespaceToCatalog(String tablespaceName, String containerPath, int pageSize) {
        try {
            // Insert into SYS_TABLESPACES
            Map<String, Object> tablespaceRow = new HashMap<>();
            tablespaceRow.put("TABLESPACE_NAME", tablespaceName);
            tablespaceRow.put("CONTAINER_PATH", containerPath);
            tablespaceRow.put("PAGE_SIZE", pageSize);
            tablespaceRow.put("CREATION_TIME", System.currentTimeMillis());
            
            insertIntoSystemTable(SYS_TABLESPACES, tablespaceRow);
            
            logger.debug("Persisted tablespace '{}' to system catalog", tablespaceName);
        } catch (Exception e) {
            logger.error("Failed to persist tablespace '{}' to system catalog", tablespaceName, e);
        }
    }
} 