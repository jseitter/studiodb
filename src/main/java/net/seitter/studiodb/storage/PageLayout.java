package net.seitter.studiodb.storage;

import java.nio.ByteBuffer;

/**
 * Abstract base class for page layouts.
 * Provides common functionality for reading and writing page headers.
 */
public abstract class PageLayout {
    // the header is fixed size for all page types
    // 32 bytes to keep some space for extensions
    protected static final int HEADER_SIZE = 32;
    // Magic number is a constant value used to identify valid pages
    final int MAGIC_NUMBER = 0xDADADADA;
 
   
    // the page is the page that this layout is working on      
    protected final Page page;
    // the buffer is the buffer of the page that this layout is working on
    protected final ByteBuffer buffer;
   
    // constructor takes a page and initializes the buffer
    protected PageLayout(Page page) {
        this.page = page;
        this.buffer = page.getBuffer();
    }
    
    /**
     * Initializes the page layout. This method must be implemented by subclasses
     * to set up the page with the appropriate header and initial state.
     */
    public abstract void initialize();
    
    /**
     * Writes the common page header.
     * Format: 
     * [Page Type (1 byte)] 
     * [Magic Number (4 bytes)] 
     * [Next Page ID (4 bytes)] 
     * [Prev Page ID (4 bytes)]
     * [Free Space Offset (4 bytes)]
     * 
     * @param pageType The type of the page
     */
    protected void writeHeader(PageType pageType) {
        // Clear the buffer and set position to 0
        buffer.clear();
        
        // Write page type marker
        buffer.put((byte) pageType.getTypeId());
       
        buffer.putInt(MAGIC_NUMBER);
        
        // No next page yet
        buffer.putInt(-1);
        
        // No previous page yet
        buffer.putInt(-1);
        
        // Free space starts after the header
        buffer.putInt(HEADER_SIZE);
        
        page.markDirty();
    }
    
    /**
     * Reads the common page header.
     * 
     * @return The page type if valid, null otherwise
     */
    protected PageType readHeader() {
        if (buffer == null) {
            throw new IllegalStateException("Buffer is null");
        }
        if (buffer.limit() < HEADER_SIZE) {
            throw new IllegalStateException("Buffer size " + buffer.limit() + " is too small for header size " + HEADER_SIZE);
        }
        // get the page type
        int typeId = buffer.get(0) & 0xFF;
        int magic = buffer.getInt(1);
       
        // Validate magic number
        if (magic != MAGIC_NUMBER) {
            throw new IllegalStateException("Invalid magic number: expected " + 
                String.format("0x%08X", MAGIC_NUMBER) + ", got " + 
                String.format("0x%08X", magic));
        }
         //create the page type
        PageType pageType = PageType.fromTypeId(typeId);
        
        return pageType;
    }
    
    /**
     * Gets the next page ID from the header.
     * 
     * @return The next page ID, or -1 if none
     */
    public int getNextPageId() {
        return buffer.getInt(5);
    }
    
    /**
     * Gets the previous page ID from the header.
     * 
     * @return The previous page ID, or -1 if none
     */
    protected int getPrevPageId() {
        return buffer.getInt(9);
    }
    
    /**
     * Gets the number of rows from the header.
     * 
     * @return The number of rows
     */
    protected int getRowCount() {
        return buffer.getInt(13);
    }
    
    /**
     * Gets the free space offset from the header.
     * 
     * @return The free space offset
     */
    protected int getFreeSpaceOffset() {
        return buffer.getInt(17);
    }
    
    /**
     * Sets the next page ID in the header.
     * 
     * @param nextPageId The next page ID
     */
    public void setNextPageId(int nextPageId) {
        buffer.putInt(5, nextPageId);
        page.markDirty();
    }
    
    /**
     * Sets the previous page ID in the header.
     * 
     * @param prevPageId The previous page ID
     */
    protected void setPrevPageId(int prevPageId) {
        buffer.putInt(9, prevPageId);
        page.markDirty();
    }
    
   
    /**
     * Sets the free space offset in the header.
     * 
     * @param freeSpaceOffset The free space offset
     */
    protected void setFreeSpaceOffset(int freeSpaceOffset) {
        buffer.putInt(17, freeSpaceOffset);
        page.markDirty();
    }
    
    /**
     * Gets the page associated with this layout.
     * 
     * @return The page
     */
    public Page getPage() {
        return page;
    }

    /**
     * Gets the amount of free space available in the page.
     * 
     * @return The number of bytes of free space available
     */
    public int getFreeSpace() {
        return page.getPageSize() - getFreeSpaceOffset();
    }
} 