package net.seitter.studiodb.schema;

import net.seitter.studiodb.DatabaseSystem;
import net.seitter.studiodb.buffer.IBufferPoolManager;
import net.seitter.studiodb.storage.Page;
import net.seitter.studiodb.storage.PageId;
import net.seitter.studiodb.storage.PageLayout;
import net.seitter.studiodb.storage.PageType;
import net.seitter.studiodb.storage.layout.PageLayoutFactory;
import net.seitter.studiodb.storage.layout.IndexPageLayout;
import net.seitter.studiodb.storage.layout.TableHeaderPageLayout;
import net.seitter.studiodb.storage.layout.TableDataPageLayout;
import net.seitter.studiodb.storage.Tablespace;
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
    public static final String SYSTEM_TABLESPACE = "SYSTEM";
    public static final String SYS_TABLESPACES = "SYS_TABLESPACES";
    public static final String SYS_TABLES = "SYS_TABLES";
    public static final String SYS_COLUMNS = "SYS_COLUMNS";
    public static final String SYS_INDEXES = "SYS_INDEXES";
    public static final String SYS_INDEX_COLUMNS = "SYS_INDEX_COLUMNS";
    
    // System catalog type identifiers
    public static final int CATALOG_TYPE_TABLESPACES = 1;
    public static final int CATALOG_TYPE_TABLES = 2;
    public static final int CATALOG_TYPE_COLUMNS = 3;
    public static final int CATALOG_TYPE_INDEXES = 4;
    public static final int CATALOG_TYPE_INDEX_COLUMNS = 5;
    
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
            IBufferPoolManager systemBufferPool = dbSystem.getBufferPoolManager(SYSTEM_TABLESPACE);
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
                    
                    // Persist the system tablespace information since we just created the catalog
                    persistTablespaceToCatalog(SYSTEM_TABLESPACE, 
                            dbSystem.getStorageManager().getTablespace(SYSTEM_TABLESPACE).getStorageContainer().getContainerPath(),
                            dbSystem.getStorageManager().getPageSize());
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
            IBufferPoolManager bufferPool = dbSystem.getBufferPoolManager(SYSTEM_TABLESPACE);
            if (bufferPool == null) {
                logger.error("System tablespace does not exist");
                return null;
            }
            
            Page headerPage = bufferPool.allocatePage();
            if (headerPage == null) {
                logger.error("Failed to allocate header page for system table '{}'", tableName);
                return null;
            }
            
            table.setHeaderPageId(headerPage.getPageId().getPageNumber());
            
            // Initialize header page with table info
            initializeTableHeaderPage(headerPage, table);
            
            // Allocate first data page
            Page firstDataPage = bufferPool.allocatePage();
            if (firstDataPage == null) {
                logger.error("Failed to allocate first data page for system table '{}'", tableName);
                bufferPool.unpinPage(headerPage.getPageId(), true); // Make sure to unpin the header page
                return null;
            }
            
            // Initialize the data page
            initializeTableDataPage(firstDataPage);
            
            // Update the header page with the first data page ID
            updateHeaderPageWithFirstDataPageId(headerPage.getPageId(), firstDataPage.getPageId().getPageNumber(), bufferPool);
            
            // Store data page ID in the table object for redundancy
            table.setFirstDataPageId(firstDataPage.getPageId().getPageNumber());
            
            // Unpin the data page, marking it as dirty to flush changes
            bufferPool.unpinPage(firstDataPage.getPageId(), true);
            
            // Store the table in our schema
            tables.put(tableName, table);
            tableIndexes.put(tableName, new ArrayList<>());
            
            // Force a flush to make sure all changes are written to disk
            try {
                bufferPool.flushAll();
                logger.debug("Flushed all changes after creating system table '{}'", tableName);
            } catch (IOException e) {
                logger.warn("Failed to flush buffer pool after creating system table '{}': {}", tableName, e.getMessage());
            }
            
            logger.info("Created system table '{}' in tablespace '{}'", tableName, SYSTEM_TABLESPACE);
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
            IBufferPoolManager systemBufferPool = dbSystem.getBufferPoolManager(SYSTEM_TABLESPACE);
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
     * Verifies and fixes page header values to ensure they are within valid ranges.
     * This is a recovery mechanism for potentially corrupted pages.
     * 
     * @param pageId The page ID to check
     * @param bufferPool The buffer pool to use
     * @return true if verification was successful, false if page could not be repaired
     * @throws IOException if there is an error accessing the tablespace
     */
    private boolean verifyAndFixPageHeader(PageId pageId, IBufferPoolManager bufferPool) throws IOException {
        Page page = bufferPool.fetchPage(pageId);
        if (page == null) {
            logger.error("Failed to fetch page {} for verification", pageId);
            return false;
        }
        
        try {
            ByteBuffer buffer = page.getBuffer();
            
            // Check the page type
            int typeId = buffer.get(0) & 0xFF;
            
            // If this is a TableHeaderPage (typeId = 1), check specific fields
            if (typeId == 1) { // 1 = TABLE_HEADER
                // Check magic number
                int magic = buffer.getInt(1);
                if (magic != PageLayout.MAGIC_NUMBER) {
                    logger.error("Invalid magic number in page {}: 0x{} (should be 0x{})", 
                        pageId, Integer.toHexString(magic), Integer.toHexString(PageLayout.MAGIC_NUMBER));
                    
                    // Fix the magic number
                    buffer.putInt(1, PageLayout.MAGIC_NUMBER);
                    page.markDirty();
                }
                
                // Check first data page ID
                int headerSize = PageLayout.HEADER_SIZE;
                int firstDataPageOffset = headerSize;
                int firstDataPageId = buffer.getInt(firstDataPageOffset);
                
                try {
                    Tablespace tablespace = dbSystem.getStorageManager().getTablespace(pageId.getTablespaceName());
                    int totalPages = tablespace.getTotalPages();
                    
                    // If the first data page ID is invalid, reset it
                    if (firstDataPageId < -1 || firstDataPageId >= totalPages) {
                        logger.error("Invalid first data page ID in page {}: {} (valid range is -1 to {})", 
                            pageId, firstDataPageId, totalPages - 1);
                        
                        // Reset to invalid (-1)
                        buffer.putInt(firstDataPageOffset, -1);
                        page.markDirty();
                    }
                } catch (IOException e) {
                    logger.error("Failed to get tablespace information: {}", e.getMessage());
                }
                
                // Check table name length
                int tableNameLengthOffset = firstDataPageOffset + 4;
                int tableNameLength = buffer.getShort(tableNameLengthOffset) & 0xFFFF;
                
                if (tableNameLength < 0 || tableNameLength > 255) {
                    logger.error("Invalid table name length in page {}: {} (valid range is 0 to 255)", 
                        pageId, tableNameLength);
                    
                    // Reset to empty name
                    buffer.putShort(tableNameLengthOffset, (short) 0);
                    page.markDirty();
                }
            }
            
            // If this is a TableDataPage (typeId = 2), check specific fields
            if (typeId == 2) { // 2 = TABLE_DATA
                // Check magic number
                int magic = buffer.getInt(1);
                if (magic != PageLayout.MAGIC_NUMBER) {
                    logger.error("Invalid magic number in page {}: 0x{} (should be 0x{})", 
                        pageId, Integer.toHexString(magic), Integer.toHexString(PageLayout.MAGIC_NUMBER));
                    
                    // Fix the magic number
                    buffer.putInt(1, PageLayout.MAGIC_NUMBER);
                    page.markDirty();
                }
                
                // Check next page ID
                int nextPageId = buffer.getInt(5);
                try {
                    Tablespace tablespace = dbSystem.getStorageManager().getTablespace(pageId.getTablespaceName());
                    int totalPages = tablespace.getTotalPages();
                    
                    if (nextPageId != -1 && (nextPageId < 0 || nextPageId >= totalPages)) {
                        logger.error("Invalid next page ID in page {}: {} (valid range is -1 or 0 to {})", 
                            pageId, nextPageId, totalPages - 1);
                        
                        // Reset to no next page
                        buffer.putInt(5, -1);
                        page.markDirty();
                    }
                } catch (IOException e) {
                    logger.error("Failed to get tablespace information: {}", e.getMessage());
                }
            }
            
            bufferPool.unpinPage(pageId, page.isDirty());
            return true;
        } catch (Exception e) {
            logger.error("Exception during page verification: {}", e.getMessage());
            bufferPool.unpinPage(pageId, false);
            return false;
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
        // Get the system table object
        Table table = tables.get(tableName);
        if (table == null) {
            logger.error("Cannot insert into system table '{}': table not found", tableName);
            return;
        }
        
        try {
            // Get a buffer pool for the SYSTEM tablespace
            IBufferPoolManager bufferPool = dbSystem.getBufferPoolManager(SYSTEM_TABLESPACE);
            if (bufferPool == null) {
                logger.error("Cannot insert into system table '{}': buffer pool not found for SYSTEM tablespace", tableName);
                return;
            }
            
            // Validate the row data
            if (!table.validateRow(row)) {
                logger.error("Cannot insert into system table '{}': invalid row data", tableName);
                return;
            }
            
            // Get the header page ID to read the first data page ID
            int headerPageNumber = table.getHeaderPageId();
            PageId headerPageId = new PageId(SYSTEM_TABLESPACE, headerPageNumber);
            
            // Verify and fix page header if necessary
            if (!verifyAndFixPageHeader(headerPageId, bufferPool)) {
                logger.error("Cannot insert into system table '{}': header page verification failed", tableName);
                return;
            }
            
            // Fetch the header page
            Page headerPage = bufferPool.fetchPage(headerPageId);
            if (headerPage == null) {
                logger.error("Cannot insert into system table '{}': header page not found", tableName);
                return;
            }
            
            // Create a TableHeaderPageLayout to access the header data
            TableHeaderPageLayout headerLayout = new TableHeaderPageLayout(headerPage);
            
            // Log debug information
            logger.debug("Header page for table '{}': ID={}, TableName={}, FirstDataPageId={}", 
                    tableName, headerPageId.getPageNumber(), headerLayout.getTableName(), headerLayout.getFirstDataPageId());
            
            // Get the first data page ID
            int firstDataPageId = headerLayout.getFirstDataPageId();
            
            // Verify the first data page ID is valid
            try {
                Tablespace tablespace = dbSystem.getStorageManager().getTablespace(SYSTEM_TABLESPACE);
                int totalPages = tablespace.getTotalPages();
                
                if (firstDataPageId < 0 || firstDataPageId >= totalPages) {
                    // Invalid first data page ID - this could be due to page corruption or inconsistency
                    logger.error("Invalid first data page ID {} for table '{}' (valid range: 0-{})", 
                        firstDataPageId, tableName, totalPages - 1);
                    
                    // Use a fallback approach - try to find the first data page ID from the table object
                    if (table.getFirstDataPageId() > 0 && table.getFirstDataPageId() < totalPages) {
                        firstDataPageId = table.getFirstDataPageId();
                        logger.info("Using first data page ID {} from table object instead", firstDataPageId);
                        
                        // Update the header page with the correct first data page ID
                        headerLayout.setFirstDataPageId(firstDataPageId);
                        headerPage.markDirty();
                    } else {
                        // If we can't find a valid page, we'll need to allocate a new one
                        logger.info("Allocating a new data page for table '{}'", tableName);
                        Page newDataPage = bufferPool.allocatePage();
                        if (newDataPage == null) {
                            logger.error("Failed to allocate new data page for table '{}'", tableName);
                            bufferPool.unpinPage(headerPageId, false);
                            return;
                        }
                        
                        // Initialize the data page
                        TableDataPageLayout newDataLayout = new TableDataPageLayout(newDataPage);
                        newDataLayout.initialize();
                        newDataPage.markDirty();
                        
                        // Update the header with this new page ID
                        firstDataPageId = newDataPage.getPageId().getPageNumber();
                        headerLayout.setFirstDataPageId(firstDataPageId);
                        headerPage.markDirty();
                        
                        // Also update the table object
                        table.setFirstDataPageId(firstDataPageId);
                        
                        // Unpin the new page
                        bufferPool.unpinPage(newDataPage.getPageId(), true);
                        
                        logger.info("Allocated new first data page {} for table '{}'", firstDataPageId, tableName);
                    }
                }
            } catch (IOException e) {
                logger.error("Error checking tablespace size: {}", e.getMessage());
                bufferPool.unpinPage(headerPageId, false);
                return;
            }
            
            // Serialize the row data first to know its size
            byte[] rowData = serializeRow(row);
            logger.debug("Attempting to insert row of {} bytes into table '{}'", rowData.length, tableName);
            
            // Try to insert into the first data page or chain of data pages
            boolean inserted = false;
            PageId currentDataPageId = new PageId(SYSTEM_TABLESPACE, firstDataPageId);
            PageId previousDataPageId = null;
            TableDataPageLayout previousDataLayout = null;
            
            // Add extra debugging
            logger.info("Attempting to insert into first data page {} for table '{}'", firstDataPageId, tableName);
            
            try {
                Tablespace tablespace = dbSystem.getStorageManager().getTablespace(SYSTEM_TABLESPACE);
                int totalPages = tablespace.getTotalPages();
                
                // Try to find a page with enough space for the row
                while (!inserted) {
                    // Verify page ID is valid before fetching
                    int pageNumber = currentDataPageId.getPageNumber();
                    if (pageNumber < 0 || pageNumber >= totalPages) {
                        logger.error("Invalid page number {} (valid range: 0-{}), allocating new page", 
                            pageNumber, totalPages - 1);
                        
                        // Allocate a new page
                        Page newDataPage = bufferPool.allocatePage();
                        if (newDataPage == null) {
                            logger.error("Failed to allocate new data page for table '{}'", tableName);
                            return;
                        }
                        
                        // Initialize the new page
                        TableDataPageLayout newDataLayout = new TableDataPageLayout(newDataPage);
                        newDataLayout.initialize();
                        
                        // If we have a previous page, link it to this new one
                        if (previousDataPageId != null) {
                            Page prevPage = bufferPool.fetchPage(previousDataPageId);
                            if (prevPage != null) {
                                TableDataPageLayout prevLayout = new TableDataPageLayout(prevPage);
                                prevLayout.setNextPageId(newDataPage.getPageId().getPageNumber());
                                prevPage.markDirty();
                                bufferPool.unpinPage(previousDataPageId, true);
                            }
                        } else {
                            // This is the first data page, update the table header
                            PageId tableHeaderPageId = new PageId(SYSTEM_TABLESPACE, table.getHeaderPageId());
                            Page tableHeaderPage = bufferPool.fetchPage(tableHeaderPageId);
                            if (tableHeaderPage != null) {
                                TableHeaderPageLayout tableHeaderLayout = new TableHeaderPageLayout(tableHeaderPage);
                                tableHeaderLayout.setFirstDataPageId(newDataPage.getPageId().getPageNumber());
                                tableHeaderPage.markDirty();
                                bufferPool.unpinPage(tableHeaderPageId, true);
                                
                                // Also update the table object
                                table.setFirstDataPageId(newDataPage.getPageId().getPageNumber());
                            }
                        }
                        
                        // Try to add the row to the new page
                        if (newDataLayout.addRow(rowData)) {
                            inserted = true;
                            newDataPage.markDirty();
                            logger.info("Inserted row into table '{}' on new page {}", 
                                tableName, newDataPage.getPageId());
                        } else {
                            logger.error("Failed to add row to new page {} - this should not happen", 
                                newDataPage.getPageId());
                        }
                        
                        // Unpin the new page
                        bufferPool.unpinPage(newDataPage.getPageId(), true);
                        break;
                    }
                    
                    Page dataPage = bufferPool.fetchPage(currentDataPageId);
                    if (dataPage == null) {
                        logger.error("Cannot insert into table '{}': data page {} not found", tableName, currentDataPageId);
                        return;
                    }
                    
                    TableDataPageLayout dataLayout = new TableDataPageLayout(dataPage);
                    
                    // Check if there's enough space in this page
                    if (dataLayout.getFreeSpace() >= rowData.length + TableDataPageLayout.ROW_DIRECTORY_ENTRY_SIZE) {
                        // Add the row to this page
                        if (dataLayout.addRow(rowData)) {
                            inserted = true;
                            dataPage.markDirty();
                            bufferPool.unpinPage(currentDataPageId, true);
                            logger.debug("Inserted row into table '{}' on page {}", tableName, currentDataPageId);
                        } else {
                            // If addRow returns false despite our space check, something went wrong
                            logger.error("Failed to add row to page {} despite space check", currentDataPageId);
                            bufferPool.unpinPage(currentDataPageId, false);
                        }
                    } else {
                        // Not enough space - check for next page
                        int nextPageId = dataLayout.getNextPageId();
                        
                        if (nextPageId != -1) {
                            // Move to the next page in the chain
                            previousDataPageId = currentDataPageId;
                            previousDataLayout = dataLayout;
                            currentDataPageId = new PageId(SYSTEM_TABLESPACE, nextPageId);
                            bufferPool.unpinPage(previousDataPageId, false);
                            
                            // Add debugging
                            logger.debug("Moving to next page {} in chain for table '{}'", nextPageId, tableName);
                        } else {
                            // This is the last page and it doesn't have enough space
                            // We need to allocate a new page and link it
                            logger.debug("Allocating new data page for table '{}' (current page {} is full)", 
                                tableName, currentDataPageId);
                            
                            // Allocate a new page
                            Page newDataPage = bufferPool.allocatePage();
                            if (newDataPage == null) {
                                logger.error("Failed to allocate new data page for table '{}'", tableName);
                                bufferPool.unpinPage(currentDataPageId, false);
                                return;
                            }
                            
                            // Initialize the new page
                            TableDataPageLayout newDataLayout = new TableDataPageLayout(newDataPage);
                            newDataLayout.initialize();
                            
                            // Link the pages
                            dataLayout.setNextPageId(newDataPage.getPageId().getPageNumber());
                            dataPage.markDirty();
                            
                            // Try to add the row to the new page
                            if (newDataLayout.addRow(rowData)) {
                                inserted = true;
                                newDataPage.markDirty();
                                logger.debug("Inserted row into table '{}' on new page {}", 
                                    tableName, newDataPage.getPageId());
                            } else {
                                logger.error("Failed to add row to new page {} - this should not happen", 
                                    newDataPage.getPageId());
                            }
                            
                            // Unpin both pages
                            bufferPool.unpinPage(currentDataPageId, true);
                            bufferPool.unpinPage(newDataPage.getPageId(), true);
                        }
                    }
                    
                    // If we've visited too many pages, break to avoid infinite loop
                    if (!inserted && previousDataPageId != null && previousDataPageId.equals(currentDataPageId)) {
                        logger.error("Circular reference detected in data pages for table '{}'", tableName);
                        break;
                    }
                }
            } catch (IOException e) {
                logger.error("Error accessing tablespace: {}", e.getMessage());
            }
            
            // Flush all changes to disk
            try {
                bufferPool.flushAll();
                logger.debug("Flushed all changes after inserting into table '{}'", tableName);
            } catch (IOException e) {
                logger.error("Error flushing buffer pool after inserting into table '{}'", tableName, e);
            }
        } catch (Exception e) {
            logger.error("Failed to insert into table '{}'", tableName, e);
        }
    }
    
    /**
     * Serializes a row into a byte array.
     *
     * @param row The row to serialize
     * @return The serialized row as a byte array
     */
    private byte[] serializeRow(Map<String, Object> row) throws IOException {
        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        return mapper.writeValueAsBytes(row);
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
     * Creates a new table in the specified tablespace.
     *
     * @param tableName The name of the table
     * @param tablespaceName The name of the tablespace
     * @param columns The columns of the table
     * @param primaryKeyColumns The names of the columns that form the primary key
     * @return The created table
     */
    public Table createTable(String tableName, String tablespaceName, List<Column> columns, List<String> primaryKeyColumns) {
        try {
            // Get the buffer pool for the tablespace
            IBufferPoolManager bufferPool = dbSystem.getBufferPoolManager(tablespaceName);
            if (bufferPool == null) {
                logger.error("Tablespace '{}' not found", tablespaceName);
                return null;
            }
            
            // Allocate a header page
            Page headerPage = bufferPool.allocatePage();
            if (headerPage == null) {
                logger.error("Failed to allocate header page for table '{}'", tableName);
                return null;
            }
            
            // Create the table object
            Table table = new Table(tableName, tablespaceName);
            table.addColumns(columns);
            if (primaryKeyColumns != null && !primaryKeyColumns.isEmpty()) {
                table.setPrimaryKey(primaryKeyColumns);
            }
            table.setHeaderPageId(headerPage.getPageId().getPageNumber());
            
            // Initialize the header page
            initializeTableHeaderPage(headerPage, table);
            
            // Allocate and initialize the first data page
            Page dataPage = bufferPool.allocatePage();
            if (dataPage == null) {
                logger.error("Failed to allocate data page for table '{}'", tableName);
                return null;
            }
            
            // Initialize the data page
            initializeTableDataPage(dataPage);
            
            // Update the header page with the first data page ID
            updateHeaderPageWithFirstDataPageId(headerPage.getPageId(), dataPage.getPageId().getPageNumber(), bufferPool);
            
            // Add the table to our cache
            tables.put(tableName, table);
            
            // Initialize the table's index list
            tableIndexes.put(tableName, new ArrayList<>());
            
            // Persist the table information to the system catalog
            persistTableToCatalog(table);
            
            logger.info("Created table '{}' in tablespace '{}'", tableName, tablespaceName);
            return table;
        } catch (IOException e) {
            logger.error("Failed to create table '{}'", tableName, e);
            return null;
        }
    }
    
    /**
     * Initializes a header page for a table.
     *
     * @param headerPage The header page to initialize
     * @param table The table information
     * @throws IOException If there's an error writing the page
     */
    private void initializeTableHeaderPage(Page headerPage, Table table) throws IOException {
        // Create a table header page layout
        TableHeaderPageLayout headerLayout = new TableHeaderPageLayout(headerPage);
        headerLayout.initialize();
        
        // Set table name
        headerLayout.setTableName(table.getName());
        
        // Add all columns to the header layout
        for (Column column : table.getColumns()) {
            headerLayout.addColumn(
                column.getName(),
                column.getDataType(),
                column.getMaxLength(),
                column.isNullable()
            );
        }
        
        // Mark the page as dirty to ensure changes are persisted
        headerPage.markDirty();
        
        logger.debug("Initialized table header page for table '{}' with {} columns", 
                table.getName(), table.getColumns().size());
    }
    
    /**
     * Updates the header page with the first data page ID.
     *
     * @param headerPageId The header page ID
     * @param firstDataPageId The first data page ID
     * @param bufferPool The buffer pool manager
     * @throws IOException If there's an error writing the page
     */
    private void updateHeaderPageWithFirstDataPageId(PageId headerPageId, int firstDataPageId, 
                                                    IBufferPoolManager bufferPool) throws IOException {
        Page headerPage = bufferPool.fetchPage(headerPageId);
        if (headerPage == null) {
            throw new IOException("Failed to fetch header page");
        }
        
        try {
            // Create the header page layout for proper object-oriented access
            TableHeaderPageLayout headerLayout = new TableHeaderPageLayout(headerPage);
            
            // Update the first data page ID through the layout class
            headerLayout.setFirstDataPageId(firstDataPageId);
            
            // The page is marked dirty by the layout method
            logger.debug("Updated first data page ID to {} for header page {}", 
                    firstDataPageId, headerPageId);
        } finally {
            bufferPool.unpinPage(headerPageId, true);
        }
    }
    
    /**
     * Initializes a table data page.
     *
     * @param dataPage The data page to initialize
     * @throws IOException If there's an error writing the page
     */
    private void initializeTableDataPage(Page dataPage) throws IOException {
        // Create a table data page layout directly
        TableDataPageLayout dataLayout = new TableDataPageLayout(dataPage);
        
        // Initialize the page layout
        dataLayout.initialize();
        
        // Mark the page as dirty to ensure changes are persisted
        dataPage.markDirty();
        
        // Set the first data page ID in the table object if applicable
        if (dataPage.getPageId() != null) {
            logger.debug("Initialized table data page {} with type {}", 
                    dataPage.getPageId().getPageNumber(), PageType.TABLE_DATA);
        }
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
            IBufferPoolManager bufferPool = dbSystem.getBufferPoolManager(tablespaceName);
            
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
     * Initializes a B-Tree root page for an index.
     *
     * @param rootPage The root page to initialize
     * @param index The index information
     * @throws IOException If there's an error writing the page
     */
    private void initializeBTreeRootPage(Page rootPage, Index index) throws IOException {
        // Get the table and first indexed column
        Table table = tables.get(index.getTableName());
        if (table == null) {
            throw new IOException("Table '" + index.getTableName() + "' not found");
        }
        
        String firstColumnName = index.getColumnNames().get(0);
        Column firstColumn = table.getColumn(firstColumnName);
        if (firstColumn == null) {
            throw new IOException("Column '" + firstColumnName + "' not found in table '" + index.getTableName() + "'");
        }
        
        // Create and initialize the index page layout
        IndexPageLayout indexLayout = new IndexPageLayout(rootPage);
        indexLayout.initialize(PageType.INDEX_LEAF, firstColumn.getDataType());
        
        // Mark the page as dirty
        rootPage.markDirty();
        
        logger.debug("Initialized B-Tree root page for index '{}' with key type {}", 
                index.getName(), firstColumn.getDataType());
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
     * Persists tablespace information to the system catalog.
     *
     * @param tablespaceName The name of the tablespace
     * @param containerPath The path to the tablespace file
     * @param pageSize The page size in bytes
     */
    public void persistTablespaceToCatalog(String tablespaceName, String containerPath, int pageSize) {
        try {
            Map<String, Object> tablespaceRow = new HashMap<>();
            tablespaceRow.put("TABLESPACE_NAME", tablespaceName);
            tablespaceRow.put("CONTAINER_PATH", containerPath);
            tablespaceRow.put("PAGE_SIZE", pageSize);
            tablespaceRow.put("CREATION_TIME", System.currentTimeMillis() / 1000);
            
            insertIntoSystemTable(SYS_TABLESPACES, tablespaceRow);
            logger.info("Persisted tablespace '{}' to system catalog", tablespaceName);
        } catch (Exception e) {
            logger.error("Failed to persist tablespace '{}' to system catalog", tablespaceName, e);
        }
    }

    /**
     * Allocates and initializes storage for a new table.
     *
     * @param tableName The name of the table
     * @param tablespaceName The name of the tablespace to store the table in
     * @param columnCount The number of columns in the table
     * @return The created table
     */
    public Table createTableStorage(String tableName, String tablespaceName, int columnCount) {
        try {
            IBufferPoolManager bufferPool = dbSystem.getBufferPoolManager(tablespaceName);
            
            if (bufferPool == null) {
                logger.error("Tablespace '{}' does not exist", tablespaceName);
                return null;
            }
            
            // Implementation of createTableStorage method
            // This method should return the created table
            return null; // Placeholder return, actual implementation needed
        } catch (Exception e) {
            logger.error("Failed to create table storage for '{}'", tableName, e);
            return null;
        }
    }
} 