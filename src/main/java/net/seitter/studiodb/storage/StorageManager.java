package net.seitter.studiodb.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Manages all tablespaces and storage-related operations in the database system.
 * This class is agnostic about page layouts and only handles raw page storage operations.
 */
public class StorageManager {
    private static final Logger logger = LoggerFactory.getLogger(StorageManager.class);
    
    private final int pageSize;
    private final Map<String, Tablespace> tablespaces;
    
    /**
     * Creates a new storage manager with a specific page size.
     *
     * @param pageSize The page size in bytes
     */
    public StorageManager(int pageSize) {
        this.pageSize = pageSize;
        this.tablespaces = new HashMap<>();
        
        logger.info("Storage manager initialized with page size: {} bytes", pageSize);
    }
    
    /**
     * Creates a new tablespace with a single storage container.
     *
     * @param tablespaceName The name of the tablespace
     * @param containerPath The file path for the storage container
     * @param initialSizePages The initial size in pages
     * @return true if the tablespace was created successfully
     */
    public boolean createTablespace(String tablespaceName, String containerPath, int initialSizePages) {
        if (tablespaces.containsKey(tablespaceName)) {
            logger.warn("Tablespace '{}' already exists", tablespaceName);
            return false;
        }
        
        try {
            // Ensure the directory exists
            File containerFile = new File(containerPath);
            File parentDir = containerFile.getParentFile();
            
            // Only attempt to create parent directories if the path has them
            if (parentDir != null) {
                if (!parentDir.exists()) {
                    boolean dirCreated = parentDir.mkdirs();
                    if (!dirCreated) {
                        logger.error("Failed to create parent directory for tablespace container: {}", 
                                parentDir.getAbsolutePath());
                        return false;
                    } else {
                        logger.info("Created parent directory: {}", parentDir.getAbsolutePath());
                    }
                }
            }
            
            // Create the storage container
            StorageContainer container = new StorageContainer(tablespaceName, containerPath, pageSize, initialSizePages);
            
            // Create the tablespace
            Tablespace tablespace = new Tablespace(tablespaceName, container);
            tablespaces.put(tablespaceName, tablespace);
            
            logger.info("Created tablespace '{}' with initial size of {} pages", 
                    tablespaceName, initialSizePages);
            return true;
        } catch (IOException e) {
            logger.error("Failed to create tablespace '{}'", tablespaceName, e);
            return false;
        }
    }
    
    /**
     * Gets a tablespace by name.
     *
     * @param tablespaceName The name of the tablespace
     * @return The tablespace, or null if not found
     */
    public Tablespace getTablespace(String tablespaceName) {
        return tablespaces.get(tablespaceName);
    }
    
    /**
     * Reads a page from a tablespace.
     *
     * @param pageId The ID of the page to read
     * @return The page, or null if the tablespace doesn't exist or the page is out of bounds
     * @throws IOException If there's an error reading the page
     */
    public Page readPage(PageId pageId) throws IOException {
        Tablespace tablespace = tablespaces.get(pageId.getTablespaceName());
        
        if (tablespace == null) {
            logger.warn("Attempt to read page from non-existent tablespace '{}'", 
                    pageId.getTablespaceName());
            return null;
        }
        
        return tablespace.readPage(pageId.getPageNumber());
    }
    
    /**
     * Writes a page to a tablespace.
     *
     * @param page The page to write
     * @throws IOException If there's an error writing the page
     */
    public void writePage(Page page) throws IOException {
        if (page == null) {
            throw new IllegalArgumentException("Page cannot be null");
        }
        
        Tablespace tablespace = tablespaces.get(page.getPageId().getTablespaceName());
        
        if (tablespace == null) {
            throw new IllegalArgumentException("Tablespace '" + 
                    page.getPageId().getTablespaceName() + "' does not exist");
        }
        
        tablespace.writePage(page);
    }
    
    /**
     * Allocates a new page in a tablespace.
     *
     * @param tablespaceName The name of the tablespace
     * @return The newly allocated page, or null if the tablespace doesn't exist
     * @throws IOException If there's an error allocating the page
     */
    public Page allocatePage(String tablespaceName) throws IOException {
        Tablespace tablespace = tablespaces.get(tablespaceName);
        
        if (tablespace == null) {
            logger.warn("Attempt to allocate page in non-existent tablespace '{}'", tablespaceName);
            return null;
        }
        
        return tablespace.allocatePage();
    }
    
    /**
     * Gets the page size used by the storage manager.
     *
     * @return The page size in bytes
     */
    public int getPageSize() {
        return pageSize;
    }
    
    /**
     * Gets all tablespace names.
     *
     * @return A list of all tablespace names
     */
    public List<String> getAllTablespaceNames() {
        return new ArrayList<>(tablespaces.keySet());
    }
    
    /**
     * Shuts down the storage manager and all tablespaces.
     */
    public void shutdown() {
        logger.info("Shutting down storage manager...");
        
        for (Tablespace tablespace : tablespaces.values()) {
            tablespace.close();
        }
        
        tablespaces.clear();
        logger.info("Storage manager shutdown complete");
    }
    
    /**
     * Adds an existing tablespace to the storage manager.
     *
     * @param tablespace The tablespace to add
     * @return true if the tablespace was added successfully
     */
    public boolean addTablespace(Tablespace tablespace) {
        if (tablespaces.containsKey(tablespace.getName())) {
            logger.warn("Tablespace '{}' already exists", tablespace.getName());
            return false;
        }
        
        tablespaces.put(tablespace.getName(), tablespace);
        logger.info("Added existing tablespace '{}'", tablespace.getName());
        return true;
    }
} 