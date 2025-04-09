package net.seitter.studiodb.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import net.seitter.studiodb.schema.SchemaManager;

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
            // Allocate the minimum required space
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
        ByteBuffer buffer = metadataPage.getBuffer();
        
        // Write container metadata
        // [Page Type (1 byte)] [Magic Number (4 bytes)] [Tablespace Name Length (2 bytes)] [Tablespace Name (variable)]
        // [Page Size (4 bytes)] [Creation Time (8 bytes)] [Total Pages (4 bytes)]
        
        // Write page type marker
        buffer.put((byte) SchemaManager.PAGE_TYPE_CONTAINER_METADATA);
        
        // Write magic number for container metadata
        buffer.putInt(SchemaManager.MAGIC_CONTAINER_METADATA);
        
        // Write tablespace name
        buffer.putShort((short) tablespaceName.length());
        for (int i = 0; i < tablespaceName.length(); i++) {
            buffer.putChar(tablespaceName.charAt(i));
        }
        
        // Write page size
        buffer.putInt(pageSize);
        
        // Write creation time (current time in milliseconds)
        buffer.putLong(System.currentTimeMillis());
        
        // Write total pages
        buffer.putInt(getTotalPages());
        
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
        ByteBuffer buffer = freeSpaceMapPage.getBuffer();
        
        // Basic format of a free space map page:
        // [Page Type (1 byte)] [Magic Number (4 bytes)] [Last Page Checked (4 bytes)]
        // [Bitmap Size (4 bytes)] [Bitmap (variable)]
        
        // Write page type marker
        buffer.put((byte) SchemaManager.PAGE_TYPE_FREE_SPACE_MAP);
        
        // Write magic number for free space map
        buffer.putInt(SchemaManager.MAGIC_CONTAINER_METADATA);
        
        // Last page checked (starts at 1 which is the free space map page)
        buffer.putInt(1);
        
        // Calculate how many pages can be tracked in this page
        // Each bit represents one page, so we can track 8 pages per byte
        int bitmapCapacity = (pageSize - 13) * 8; // 13 = header size
        
        // Write bitmap capacity
        buffer.putInt(bitmapCapacity);
        
        // Initialize bitmap (all zeros means all pages are in use)
        // We'll set bits specifically for free pages
        for (int i = 0; i < (pageSize - 13); i++) {
            buffer.put((byte) 0);
        }
        
        // Explicitly mark page 2 as free (set bit 2 in the bitmap)
        // The byte offset for page 2 is 2/8 = 0, and the bit offset is 2%8 = 2
        buffer.position(13); // Position at the start of the bitmap
        byte value = buffer.get(); // Get the first byte of the bitmap
        value |= (1 << 2); // Set the bit at position 2 to 1 (1 means free)
        buffer.position(13);
        buffer.put(value);
        
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
        if (startPage < 2) startPage = 2; // Pages 0 and 1 are never free
        
        Page freeSpaceMapPage = readPage(1);
        if (freeSpaceMapPage == null) {
            logger.error("Failed to read free space map page");
            return;
        }
        
        ByteBuffer buffer = freeSpaceMapPage.getBuffer();
        
        // Skip header to get to bitmap
        int bitmapOffset = 13; // 1 (type) + 4 (magic) + 4 (last checked) + 4 (capacity)
        
        // For each page in the range, set its bit in the bitmap
        for (int pageNum = startPage; pageNum <= endPage; pageNum++) {
            int byteOffset = pageNum / 8;
            int bitOffset = pageNum % 8;
            
            // Skip if beyond the bitmap capacity
            if (byteOffset >= (pageSize - bitmapOffset)) {
                logger.warn("Page {} is beyond the free space map capacity", pageNum);
                break;
            }
            
            // Read current byte
            buffer.position(bitmapOffset + byteOffset);
            byte currentByte = buffer.get();
            
            // Set the bit for this page (1 means free)
            currentByte |= (1 << bitOffset);
            
            // Write back the updated byte
            buffer.position(bitmapOffset + byteOffset);
            buffer.put(currentByte);
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
        if (pageNum < 2) return; // Pages 0 and 1 are always in use
        
        Page freeSpaceMapPage = readPage(1);
        if (freeSpaceMapPage == null) {
            logger.error("Failed to read free space map page");
            return;
        }
        
        ByteBuffer buffer = freeSpaceMapPage.getBuffer();
        
        // Skip header to get to bitmap
        int bitmapOffset = 13; // 1 (type) + 4 (magic) + 4 (last checked) + 4 (capacity)
        
        int byteOffset = pageNum / 8;
        int bitOffset = pageNum % 8;
        
        // Skip if beyond the bitmap capacity
        if (byteOffset >= (pageSize - bitmapOffset)) {
            logger.warn("Page {} is beyond the free space map capacity", pageNum);
            return;
        }
        
        // Read current byte
        byte currentByte = buffer.get(bitmapOffset + byteOffset);
        
        // Clear the bit for this page (0 means used)
        currentByte &= ~(1 << bitOffset);
        
        // Write back the updated byte
        buffer.put(bitmapOffset + byteOffset, currentByte);
        
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
        
        ByteBuffer buffer = freeSpaceMapPage.getBuffer();
        
        // Skip header to get to bitmap
        int bitmapOffset = 13; // 1 (type) + 4 (magic) + 4 (last checked) + 4 (capacity)
        
        // Read last checked page to start search from there
        buffer.position(5); // Skip page type and magic number
        int lastChecked = buffer.getInt();
        int bitmapCapacity = buffer.getInt();
        logger.debug("Last checked page: {}, Bitmap capacity: {}", lastChecked, bitmapCapacity);
        // Start search from the last checked page + 1
        int startPage = lastChecked + 1;
        logger.debug("Start page: {}", startPage);
        if (startPage < 2) startPage = 2; // Pages 0 and 1 are never free
        
        // First pass: search from lastChecked to end
        for (int pageNum = startPage; pageNum < bitmapCapacity; pageNum++) {
            int byteOffset = pageNum / 8;
            int bitOffset = pageNum % 8;
            
            // Skip if beyond the bitmap capacity
            if (byteOffset >= (pageSize - bitmapOffset)) {
                break;
            }
            
            // Read current byte
            buffer.position(bitmapOffset + byteOffset);
            byte currentByte = buffer.get();
            
            // Check if the bit for this page is set (1 means free)
            if ((currentByte & (1 << bitOffset)) != 0) {
                // Update last checked page
                buffer.position(5);
                buffer.putInt(pageNum);
                
                // Mark page as dirty and write it back
                freeSpaceMapPage.markDirty();
                writePage(freeSpaceMapPage);
                
                return pageNum;
            }
        }
        
        // Second pass: search from beginning to lastChecked
        for (int pageNum = 2; pageNum <= lastChecked; pageNum++) {
            int byteOffset = pageNum / 8;
            int bitOffset = pageNum % 8;
            
            // Skip if beyond the bitmap capacity
            if (byteOffset >= (pageSize - bitmapOffset)) {
                break;
            }
            
            // Read current byte
            buffer.position(bitmapOffset + byteOffset);
            byte currentByte = buffer.get();
            
            // Check if the bit for this page is set (1 means free)
            if ((currentByte & (1 << bitOffset)) != 0) {
                // Update last checked page
                buffer.position(5);
                buffer.putInt(pageNum);
                
                // Mark page as dirty and write it back
                freeSpaceMapPage.markDirty();
                writePage(freeSpaceMapPage);
                
                return pageNum;
            }
        }
        
        // No free pages found
        return -1;
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
            ByteBuffer buffer = metadataPage.getBuffer();
            
            // Navigate to the total pages field, which is near the end of the metadata
            // Skip: page type (1), magic (4), name length (2), name (variable), page size (4), creation time (8)
            int tableNameLength = buffer.getShort(5) & 0xFFFF;
            int totalPagesOffset = 7 + (tableNameLength * 2) + 4 + 8;
            
            // Update total pages
            buffer.position(totalPagesOffset);
            buffer.putInt(getTotalPages());
            
            // Write the updated page
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