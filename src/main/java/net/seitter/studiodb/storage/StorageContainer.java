package net.seitter.studiodb.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Represents a physical storage container for a tablespace.
 * Handles low-level read and write operations to the underlying file.
 */
public class StorageContainer {
    private static final Logger logger = LoggerFactory.getLogger(StorageContainer.class);
    
    private final String containerPath;
    private final String tablespaceName;
    private final int pageSize;
    private RandomAccessFile file;
    private FileChannel channel;
    
    /**
     * Creates a new storage container.
     *
     * @param tablespaceName The name of the tablespace this container belongs to
     * @param containerPath The file path for the container
     * @param pageSize The size of each page in bytes
     * @throws IOException If there's an error accessing the file
     */
    public StorageContainer(String tablespaceName, String containerPath, int pageSize) throws IOException {
        this.tablespaceName = tablespaceName;
        this.containerPath = containerPath;
        this.pageSize = pageSize;
        
        File containerFile = new File(containerPath);
        boolean fileExists = containerFile.exists();
        
        this.file = new RandomAccessFile(containerFile, "rw");
        this.channel = file.getChannel();
        
        if (!fileExists) {
            logger.info("Created new storage container for tablespace '{}' at {}", 
                    tablespaceName, containerPath);
        } else {
            logger.info("Opened existing storage container for tablespace '{}' at {}", 
                    tablespaceName, containerPath);
        }
    }
    
    /**
     * Creates a new storage container with an initial size.
     *
     * @param tablespaceName The name of the tablespace this container belongs to
     * @param containerPath The file path for the container
     * @param pageSize The size of each page in bytes
     * @param initialSizePages The initial size in pages
     * @throws IOException If there's an error accessing the file
     */
    public StorageContainer(String tablespaceName, String containerPath, int pageSize, int initialSizePages) 
            throws IOException {
        this(tablespaceName, containerPath, pageSize);
        
        // Preallocate space
        if (initialSizePages > 0) {
            long initialSizeBytes = (long) initialSizePages * pageSize;
            file.setLength(initialSizeBytes);
            logger.info("Preallocated {} pages ({} bytes) for tablespace '{}'", 
                    initialSizePages, initialSizeBytes, tablespaceName);
        }
    }
    
    /**
     * Reads a page from the storage container.
     *
     * @param pageNumber The page number to read
     * @return The page read from disk, or null if the page is out of bounds
     * @throws IOException If there's an error reading the page
     */
    public Page readPage(int pageNumber) throws IOException {
        if (pageNumber < 0 || pageNumber >= getTotalPages()) {
            logger.warn("Attempt to read page {} which is out of bounds (0-{})", 
                    pageNumber, getTotalPages() - 1);
            return null;
        }
        
        long offset = (long) pageNumber * pageSize;
        ByteBuffer buffer = ByteBuffer.allocate(pageSize);
        
        channel.position(offset);
        int bytesRead = channel.read(buffer);
        
        if (bytesRead != pageSize) {
            logger.warn("Read only {} bytes instead of {} for page {}", 
                    bytesRead, pageSize, pageNumber);
        }
        
        buffer.flip();
        PageId pageId = new PageId(tablespaceName, pageNumber);
        return new Page(pageId, buffer.array());
    }
    
    /**
     * Writes a page to the storage container.
     *
     * @param page The page to write
     * @throws IOException If there's an error writing the page
     */
    public void writePage(Page page) throws IOException {
        PageId pageId = page.getPageId();
        
        if (!pageId.getTablespaceName().equals(tablespaceName)) {
            throw new IllegalArgumentException("Page belongs to tablespace '" + 
                    pageId.getTablespaceName() + "', not '" + tablespaceName + "'");
        }
        
        int pageNumber = pageId.getPageNumber();
        long offset = (long) pageNumber * pageSize;
        
        // Ensure file is large enough
        if (offset + pageSize > file.length()) {
            file.setLength(offset + pageSize);
        }
        
        ByteBuffer buffer = ByteBuffer.wrap(page.getData());
        channel.position(offset);
        channel.write(buffer);
        
        // Force data to disk immediately for educational clarity
        // In a real system, this would be controlled by a write-ahead log and checkpointing
        channel.force(true);
    }
    
    /**
     * Allocates a new page in the storage container.
     *
     * @return The newly allocated page
     * @throws IOException If there's an error allocating the page
     */
    public Page allocatePage() throws IOException {
        int pageNumber = (int) (file.length() / pageSize);
        PageId pageId = new PageId(tablespaceName, pageNumber);
        Page page = new Page(pageId, pageSize);
        
        // Write the blank page to disk to allocate the space
        writePage(page);
        
        logger.debug("Allocated new page {} in tablespace '{}'", pageNumber, tablespaceName);
        return page;
    }
    
    /**
     * Gets the total number of pages in the storage container.
     *
     * @return The total number of pages
     * @throws IOException If there's an error accessing the file
     */
    public int getTotalPages() throws IOException {
        return (int) (file.length() / pageSize);
    }
    
    /**
     * Gets the name of the tablespace this container belongs to.
     *
     * @return The tablespace name
     */
    public String getTablespaceName() {
        return tablespaceName;
    }
    
    /**
     * Gets the file path of the container.
     *
     * @return The container path
     */
    public String getContainerPath() {
        return containerPath;
    }
    
    /**
     * Gets the page size of the container.
     *
     * @return The page size in bytes
     */
    public int getPageSize() {
        return pageSize;
    }
    
    /**
     * Closes the storage container.
     */
    public void close() {
        try {
            if (channel != null && channel.isOpen()) {
                channel.force(true);
                channel.close();
            }
            
            if (file != null) {
                file.close();
            }
            
            logger.info("Closed storage container for tablespace '{}'", tablespaceName);
        } catch (IOException e) {
            logger.error("Error closing storage container for tablespace '{}'", 
                    tablespaceName, e);
        }
    }
} 