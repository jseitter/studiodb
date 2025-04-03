package net.seitter.studiodb;

import net.seitter.studiodb.buffer.BufferPoolManager;
import net.seitter.studiodb.buffer.IBufferPoolManager;
import net.seitter.studiodb.schema.SchemaManager;
import net.seitter.studiodb.storage.StorageContainer;
import net.seitter.studiodb.storage.StorageManager;
import net.seitter.studiodb.storage.Tablespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Main class responsible for managing the database system components.
 */
public class DatabaseSystem {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseSystem.class);
    
    private final StorageManager storageManager;
    private final Map<String, IBufferPoolManager> bufferPoolManagers;
    private final SchemaManager schemaManager;
    
    // Default configuration
    private static final int DEFAULT_PAGE_SIZE = 4096; // 4KB pages
    private static final int DEFAULT_BUFFER_POOL_SIZE = 100; // 100 pages per buffer pool
    
    public DatabaseSystem() {
        // Ensure data directory exists
        ensureDataDirectoryExists();
        
        this.storageManager = new StorageManager(DEFAULT_PAGE_SIZE);
        this.bufferPoolManagers = new HashMap<>();
        
        // Create system tablespace first
        boolean systemTablespaceCreated = false;
        String systemTablespacePath = "./data/system_tablespace.dat";
        try {
            boolean created = createSystemTablespace();
            if (!created) {
                logger.error("Failed to create or open system tablespace");
            } else {
                systemTablespaceCreated = true;
            }
        } catch (Exception e) {
            logger.error("Error initializing system tablespace", e);
        }
        
        this.schemaManager = new SchemaManager(this);
        
        // Now that schema manager is initialized, persist system tablespace info if it was newly created
        if (systemTablespaceCreated) {
            try {
                schemaManager.persistTablespaceToCatalog("SYSTEM", systemTablespacePath, DEFAULT_PAGE_SIZE);
                logger.info("Persisted system tablespace information to system catalog");
            } catch (Exception e) {
                logger.error("Failed to persist system tablespace information", e);
            }
        }
        
        logger.info("Database system initialized with page size: {} bytes", DEFAULT_PAGE_SIZE);
    }
    
    /**
     * Ensures that the data directory exists.
     */
    private void ensureDataDirectoryExists() {
        // Create a data directory for storing tablespace files
        File dataDir = new File("./data");
        if (!dataDir.exists()) {
            boolean created = dataDir.mkdirs();
            if (created) {
                logger.info("Created data directory at {}", dataDir.getAbsolutePath());
            } else {
                logger.warn("Failed to create data directory at {}", dataDir.getAbsolutePath());
            }
        }
    }
    
    /**
     * Creates a new tablespace.
     * 
     * @param tablespaceName The name of the tablespace
     * @param containerPath The path to the tablespace container
     * @param initialSizePages The initial size of the tablespace in pages
     * @return true if creation was successful
     */
    public boolean createTablespace(String tablespaceName, String containerPath, int initialSizePages) {
        try {
            boolean created = storageManager.createTablespace(tablespaceName, containerPath, initialSizePages);
            
            if (created) {
                // Create a buffer pool for this tablespace
                IBufferPoolManager bufferPoolManager = new BufferPoolManager(
                        tablespaceName, 
                        storageManager, 
                        DEFAULT_BUFFER_POOL_SIZE);
                
                bufferPoolManagers.put(tablespaceName, bufferPoolManager);
                logger.info("Created tablespace '{}' with buffer pool of {} pages", 
                        tablespaceName, DEFAULT_BUFFER_POOL_SIZE);
                
                // Persist tablespace information to system catalog
                if (schemaManager != null) {
                    schemaManager.persistTablespaceToCatalog(tablespaceName, containerPath, DEFAULT_PAGE_SIZE);
                    logger.info("Persisted tablespace '{}' information to system catalog", tablespaceName);
                } else {
                    logger.warn("Schema manager is not initialized, cannot persist tablespace information");
                }
            }
            
            return created;
        } catch (Exception e) {
            logger.error("Failed to create tablespace '{}'", tablespaceName, e);
            return false;
        }
    }
    
    /**
     * Creates or opens the system tablespace where catalog tables are stored.
     * 
     * @return true if creation was successful or if it already exists
     */
    private boolean createSystemTablespace() {
        String systemTablespaceName = "SYSTEM";
        String containerPath = "./data/system_tablespace.dat";
        int initialSizePages = 100;
        
        try {
            // Check if the tablespace already exists in the StorageManager
            if (storageManager.getTablespace(systemTablespaceName) != null) {
                logger.info("System tablespace already loaded in storage manager");
                
                // Make sure we have a buffer pool for it
                if (!bufferPoolManagers.containsKey(systemTablespaceName)) {
                    IBufferPoolManager bufferPoolManager = new BufferPoolManager(
                            systemTablespaceName, 
                            storageManager, 
                            DEFAULT_BUFFER_POOL_SIZE);
                    
                    bufferPoolManagers.put(systemTablespaceName, bufferPoolManager);
                    logger.info("Created buffer pool for existing system tablespace");
                }
                
                return true;
            }
            
            // Check if the tablespace file already exists
            File containerFile = new File(containerPath);
            boolean exists = containerFile.exists();
            
            // Create the tablespace
            boolean created = createTablespace(systemTablespaceName, containerPath, initialSizePages);
            
            if (created) {
                logger.info("{} system tablespace '{}'", exists ? "Opened" : "Created", systemTablespaceName);
            } else {
                logger.error("Failed to {} system tablespace '{}'", exists ? "open" : "create", systemTablespaceName);
            }
            
            return created;
        } catch (Exception e) {
            logger.error("Error creating system tablespace", e);
            return false;
        }
    }
    
    /**
     * Gets the buffer pool manager for a specific tablespace.
     * 
     * @param tablespaceName The name of the tablespace
     * @return The buffer pool manager, or null if not found
     */
    public IBufferPoolManager getBufferPoolManager(String tablespaceName) {
        return bufferPoolManagers.get(tablespaceName);
    }
    
    /**
     * Gets the storage manager.
     * 
     * @return The storage manager
     */
    public StorageManager getStorageManager() {
        return storageManager;
    }
    
    /**
     * Gets the schema manager.
     * 
     * @return The schema manager
     */
    public SchemaManager getSchemaManager() {
        return schemaManager;
    }
    
    /**
     * Gets a list of all tablespace names in the system.
     * 
     * @return A list of all tablespace names
     */
    public List<String> getAllTablespaceNames() {
        return storageManager.getAllTablespaceNames();
    }
    
    /**
     * Shutdowns the database system, ensuring all data is properly persisted.
     */
    public void shutdown() {
        logger.info("Shutting down database system...");
        
        // Flush all buffer pools
        for (IBufferPoolManager bpm : bufferPoolManagers.values()) {
            try {
                bpm.shutdown();
                logger.info("Shut down buffer pool for tablespace '{}'", bpm.getTablespaceName());
            } catch (Exception e) {
                logger.error("Error shutting down buffer pool for tablespace '{}'", bpm.getTablespaceName(), e);
            }
        }
        
        // Close storage manager
        storageManager.shutdown();
        
        logger.info("Database system shutdown complete");
    }
} 