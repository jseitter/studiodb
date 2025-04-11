package net.seitter.studiodb.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import net.seitter.studiodb.schema.SchemaManager;
import net.seitter.studiodb.storage.layout.ContainerMetadataPageLayout;
import net.seitter.studiodb.storage.layout.FreeSpaceMapPageLayout;

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
        
        // Check if this is a new file
        boolean isNewFile = file.length() == 0;
        
        // For a new file, always allocate exactly 3 pages initially:
        // Page 0: Container metadata
        // Page 1: Free space map
        // Page 2: First available page for user data
        if (isNewFile) {
            // Allocate the minimum required space (3 pages)
            long initialMinBytes = 3L * pageSize;
            file.setLength(initialMinBytes);
            
            // Initialize page 0 as container metadata
            initializeContainerMetadataPage();
            logger.info("Initialized container metadata page for tablespace '{}'", tablespaceName);
            
            // Initialize page 1 as free space map
            initializeFreeSpaceMapPage();
            logger.info("Initialized free space map page for tablespace '{}'", tablespaceName);
            
            // Only extend if requested more than 3 pages
            if (initialSizePages > 3) {
                long fullSizeBytes = (long) initialSizePages * pageSize;
                file.setLength(fullSizeBytes);
                
                // Mark all pages beyond page 2 as free
                markPagesAsFree(3, initialSizePages - 1);
            }
            
            logger.info("Preallocated {} pages ({} bytes) for tablespace '{}'", 
                    Math.max(3, initialSizePages), file.length(), tablespaceName);
        } else {
            // For existing files, check if size needs to be increased
            long currentBytes = file.length();
            long requestedBytes = (long) initialSizePages * pageSize;
            
            if (currentBytes < requestedBytes) {
                int oldTotalPages = (int)(currentBytes / pageSize);
                file.setLength(requestedBytes);
                logger.info("Extended tablespace '{}' to {} pages ({} bytes)", 
                        tablespaceName, initialSizePages, requestedBytes);
                
                // Mark the newly added pages as free
                markPagesAsFree(oldTotalPages, initialSizePages - 1);
            }
        }
    }
    
    /**
     * Initializes page 0 as a container metadata page.
     * This page contains information about the container itself.
     *
     * @throws IOException If there's an error writing the page
     */
    private void initializeContainerMetadataPage() throws IOException {
        PageId metadataPageId = new PageId(tablespaceName, 0);
        Page metadataPage = new Page(metadataPageId, pageSize);
        
        // Use proper ContainerMetadataPageLayout instead of direct buffer access
        ContainerMetadataPageLayout layout = new ContainerMetadataPageLayout(metadataPage);
        layout.initialize();
        
        // Set tablespace properties
        layout.setTablespaceName(tablespaceName);
        layout.setPageSize(pageSize);
        layout.setCreationTime(System.currentTimeMillis());
        layout.setLastOpenedTime(System.currentTimeMillis());
        layout.setTotalPages(getTotalPages());
        layout.setFreeSpaceMapPageId(1); // Page 1 is typically the free space map
        
        // Write the page to disk
        writePage(metadataPage);
    }
    
    /**
     * Initializes page 1 as a free space map page.
     * This page tracks which pages are free (available for allocation) in the container.
     *
     * @throws IOException If there's an error writing the page
     */
    private void initializeFreeSpaceMapPage() throws IOException {
        PageId freeSpaceMapPageId = new PageId(tablespaceName, 1);
        Page freeSpaceMapPage = new Page(freeSpaceMapPageId, pageSize);
        
        // Use proper FreeSpaceMapPageLayout instead of direct buffer access
        FreeSpaceMapPageLayout layout = new FreeSpaceMapPageLayout(freeSpaceMapPage);
        layout.initialize();
        
        // Mark page 2 as free (since pages 0 and 1 are used for metadata and free space map)
        layout.markPageAsFree(2);
        
        // Write the page to disk
        writePage(freeSpaceMapPage);
        
        logger.info("Free space map initialized with page 2 marked as free");
    }
    
    /**
     * Marks a range of pages as free in the free space map.
     *
     * @param startPage The first page number to mark as free
     * @param endPage The last page number to mark as free
     * @throws IOException If there's an error updating the free space map
     */
    private void markPagesAsFree(int startPage, int endPage) throws IOException {
        // if we get a wrong argument we shall abort
        if (startPage < 2) {
            logger.error("Cannot mark page {} as free - it's a reserved page", startPage);
            return;
        }
        
        Page freeSpaceMapPage = readPage(1);
        if (freeSpaceMapPage == null) {
            logger.error("Failed to read free space map page");
            return;
        }
        
        // Use proper FreeSpaceMapPageLayout to manage the free space map
        FreeSpaceMapPageLayout layout = new FreeSpaceMapPageLayout(freeSpaceMapPage);
        
        // Mark each page in the range as free
        for (int pageNum = startPage; pageNum <= endPage; pageNum++) {
            layout.markPageAsFree(pageNum);
        }
        
        // Mark the page as dirty and write it back
        freeSpaceMapPage.markDirty();
        writePage(freeSpaceMapPage);
    }
    
    /**
     * Marks a page as used (not free) in the free space map.
     *
     * @param pageNum The page number to mark as used
     * @throws IOException If there's an error updating the free space map
     */
    private void markPageAsUsed(int pageNum) throws IOException {
        // if we get a wrong argument we shall abort
        if (pageNum < 2) {
            logger.error("Cannot mark page {} as used - it's a reserved page", pageNum);
            return;
        }
       // read the free space map page
        Page freeSpaceMapPage = readPage(1);
        if (freeSpaceMapPage == null) {
            logger.error("Failed to read free space map page");
            return;
        }
        
        // Use proper FreeSpaceMapPageLayout to manage the free space map
        FreeSpaceMapPageLayout layout = new FreeSpaceMapPageLayout(freeSpaceMapPage);
        
        // Mark the page as used
        layout.markPageAsUsed(pageNum);
        
        // Mark the page as dirty and write it back
        freeSpaceMapPage.markDirty();
        writePage(freeSpaceMapPage);
    }
    
    /**
     * Finds a free page in the free space map.
     *
     * @return The page number of a free page, or -1 if no free pages are available
     * @throws IOException If there's an error reading the free space map
     */
    private int findFreePage() throws IOException {
        Page freeSpaceMapPage = readPage(1);
        if (freeSpaceMapPage == null) {
            logger.error("Failed to read free space map page");
            return -1;
        }
        
        // Use proper FreeSpaceMapPageLayout to find a free page
        FreeSpaceMapPageLayout layout = new FreeSpaceMapPageLayout(freeSpaceMapPage);
        
        // Find the next free page using the layout's method
        int freePage = layout.findNextFreePage();
        
        // No need to update the page - the layout's method already did that
        
        if (freePage >= 0) {
            logger.debug("Found free page {} in tablespace '{}'", freePage, tablespaceName);
        } else {
            logger.debug("No free pages found in tablespace '{}'", tablespaceName);
        }
        
        return freePage;
    }
    
    /**
     * Gets a free page from the storage container.
     * First tries to find a free page in the free space map,
     * then falls back to allocating a new page if none is found.
     *
     * @return The page number of the free page
     * @throws IOException If there's an error accessing the storage
     */
    public Page getFreePage() throws IOException {
        // Try to find a free page in the free space map
        int freePageNum = findFreePage();
        
        if (freePageNum != -1) {
            // Found a free page
            logger.debug("Found free page {} in tablespace '{}'", freePageNum, tablespaceName);
            
            // Mark the page as used
            markPageAsUsed(freePageNum);
            
            // Create and return the page
            PageId pageId = new PageId(tablespaceName, freePageNum);
            Page page = new Page(pageId, pageSize);
            
            // Initialize the page with zeros
            byte[] zeroData = new byte[pageSize];
            page.writeData(zeroData);
            
            // Write the blank page to disk
            writePage(page);
            
            return page;
        } else {
            // No free pages, allocate a new one at the end
            return allocateNewPage();
        }
    }
    
    /**
     * Allocates a new page at the end of the storage container.
     *
     * @return The newly allocated page
     * @throws IOException If there's an error allocating the page
     */
    private Page allocateNewPage() throws IOException {
        // Get the total number of pages
        int totalPages = getTotalPages();
        
        // Create a new page at the end
        PageId pageId = new PageId(tablespaceName, totalPages);
        Page page = new Page(pageId, pageSize);
        
        // Write the blank page to disk to allocate the space
        writePage(page);
        
        // Update the container metadata
        updateContainerMetadataPage();
        
        logger.debug("Allocated new page {} at end of tablespace '{}'", totalPages, tablespaceName);
        return page;
    }
    
    /**
     * Allocates a new page in the storage container.
     * This method is now a wrapper around getFreePage().
     *
     * @return The newly allocated page
     * @throws IOException If there's an error allocating the page
     */
    public Page allocatePage() throws IOException {
        return getFreePage();
    }
    
    /**
     * Deallocates a page, marking it as free in the free space map.
     *
     * @param pageNum The page number to deallocate
     * @throws IOException If there's an error updating the free space map
     */
    public void deallocatePage(int pageNum) throws IOException {
        if (pageNum < 2) {
            logger.warn("Cannot deallocate reserved page {}", pageNum);
            return;
        }
        
        // Mark the page as free in the free space map
        markPagesAsFree(pageNum, pageNum);
        
        logger.debug("Deallocated page {} in tablespace '{}'", pageNum, tablespaceName);
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
     * Updates the container metadata page (page 0) with current information.
     *
     * @throws IOException If there's an error updating the page
     */
    private void updateContainerMetadataPage() throws IOException {
        // Only update occasionally to avoid excessive I/O
        // In a real database, this would be more sophisticated
        PageId metadataPageId = new PageId(tablespaceName, 0);
        Page metadataPage = readPage(0);
        
        if (metadataPage != null) {
            // Use proper ContainerMetadataPageLayout to update metadata
            ContainerMetadataPageLayout layout = new ContainerMetadataPageLayout(metadataPage);
            
            // Update total pages
            layout.setTotalPages(getTotalPages());
            layout.setLastOpenedTime(System.currentTimeMillis());
            
            // Mark as dirty and write back
            metadataPage.markDirty();
            writePage(metadataPage);
        }
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
            //
            if (channel != null && channel.isOpen()) {
                channel.force(true);
                channel.close();
            }
            
            // close the file
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