package net.seitter.studiodb.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Represents a logical division of the database storage.
 * In this simplified implementation, a tablespace contains only a single storage container.
 */
public class Tablespace {
    private static final Logger logger = LoggerFactory.getLogger(Tablespace.class);
   
    // name of the tablespace
    private final String name;
    // for now we only support a single storage container per tablespace
    private final StorageContainer storageContainer;
    
    /**
     * Creates a new tablespace with a single storage container.
     *
     * @param name The name of the tablespace
     * @param storageContainer The storage container for this tablespace
     */
    public Tablespace(String name, StorageContainer storageContainer) {
        this.name = name;
        this.storageContainer = storageContainer;
        
        logger.info("Created tablespace '{}' with storage container at {}", 
                name, storageContainer.getContainerPath());
    }
    
    /**
     * Gets the name of the tablespace.
     *
     * @return The tablespace name
     */
    public String getName() {
        return name;
    }
    
    /**
     * Gets the storage container for this tablespace.
     *
     * @return The storage container
     */
    public StorageContainer getStorageContainer() {
        return storageContainer;
    }
    
    /**
     * Reads a page from the tablespace.
     *
     * @param pageNumber The page number to read
     * @return The page
     * @throws IOException If there's an error reading the page
     */
    public Page readPage(int pageNumber) throws IOException {
        return storageContainer.readPage(pageNumber);
    }
    
    /**
     * Writes a page to the tablespace.
     *
     * @param page The page to write
     * @throws IOException If there's an error writing the page
     */
    public void writePage(Page page) throws IOException {
        if (!page.getPageId().getTablespaceName().equals(name)) {
            throw new IllegalArgumentException("Page belongs to tablespace '" +
                    page.getPageId().getTablespaceName() + "', not '" + name + "'");
        }
        
        storageContainer.writePage(page);
    }
    
    /**
     * Allocates a new page in the tablespace.
     *
     * @return The newly allocated page
     * @throws IOException If there's an error allocating the page
     */
    public Page allocatePage() throws IOException {
        return storageContainer.allocatePage();
    }
    
    /**
     * Gets the total number of pages in the tablespace.
     *
     * @return The total number of pages
     * @throws IOException If there's an error accessing the storage
     */
    public int getTotalPages() throws IOException {
        return storageContainer.getTotalPages();
    }
    
    /**
     * Gets the page size used by this tablespace.
     *
     * @return The page size in bytes
     */
    public int getPageSize() {
        return storageContainer.getPageSize();
    }
    
    /**
     * Closes the tablespace and its storage container.
     */
    public void close() {
        storageContainer.close();
        logger.info("Closed tablespace '{}'", name);
    }
} 