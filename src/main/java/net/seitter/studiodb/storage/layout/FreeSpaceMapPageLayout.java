package net.seitter.studiodb.storage.layout;

import java.nio.ByteBuffer;
import net.seitter.studiodb.storage.Page;
import net.seitter.studiodb.storage.PageLayout;
import net.seitter.studiodb.storage.PageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Layout class for free space map pages.
 * This is typically Page 1 of a container and tracks which pages are free or used.
 */
public class FreeSpaceMapPageLayout extends PageLayout {
    private static final Logger logger = LoggerFactory.getLogger(FreeSpaceMapPageLayout.class);
    
    // Constants for field offsets
    private static final int LAST_CHECKED_PAGE_OFFSET = HEADER_SIZE; // 4 bytes
    private static final int BITMAP_CAPACITY_OFFSET = LAST_CHECKED_PAGE_OFFSET + 4; // 4 bytes
    private static final int BITMAP_START_OFFSET = BITMAP_CAPACITY_OFFSET + 4; // Start of bitmap data
    
    public FreeSpaceMapPageLayout(Page page) {
        super(page);
    }
    
    /**
     * Initializes a new free space map page.
     */
    @Override
    public void initialize() {
        writeHeader(PageType.FREE_SPACE_MAP);
        
        // Calculate bitmap capacity (how many pages we can track)
        int bitmapCapacity = calculateBitmapCapacity();
        
        // Set initial values
        setLastCheckedPage(1); // Start at page 1
        setBitmapCapacity(bitmapCapacity);
        
        // Initialize bitmap - all zeros (all pages available)
        clearBitmap();
        
        // Mark first two pages as used (page 0 is container metadata, page 1 is this free space map)
        markPageAsUsed(0);
        markPageAsUsed(1);
        
        logger.debug("Initialized free space map page with capacity for {} pages", bitmapCapacity);
    }
    
    /**
     * Clears the entire bitmap (sets all bytes to 0).
     * This marks all pages as used.
     */
    private void clearBitmap() {
        int bitmapSize = (getBitmapCapacity() + 7) / 8; // Round up to nearest byte
        for (int i = 0; i < bitmapSize; i++) {
            buffer.put(BITMAP_START_OFFSET + i, (byte) 0);
        }
        page.markDirty();
    }
    
    /**
     * Calculates how many pages can be tracked by this free space map.
     * Each bit represents one page, so we can track 8 pages per byte.
     * 
     * @return The bitmap capacity (number of pages that can be tracked)
     */
    private int calculateBitmapCapacity() {
        // Available space for bitmap = page size - header size - last checked - capacity
        int availableSpace = page.getPageSize() - BITMAP_START_OFFSET;
        return availableSpace * 8; // 8 bits per byte
    }
    
    /**
     * Sets the last checked page ID.
     * This is used for linear scans to find free pages.
     * 
     * @param pageId The last checked page ID
     */
    public void setLastCheckedPage(int pageId) {
        buffer.putInt(LAST_CHECKED_PAGE_OFFSET, pageId);
        page.markDirty();
    }
    
    /**
     * Gets the last checked page ID.
     * 
     * @return The last checked page ID
     */
    public int getLastCheckedPage() {
        return buffer.getInt(LAST_CHECKED_PAGE_OFFSET);
    }
    
    /**
     * Sets the bitmap capacity.
     * 
     * @param capacity The number of pages that can be tracked
     */
    public void setBitmapCapacity(int capacity) {
        buffer.putInt(BITMAP_CAPACITY_OFFSET, capacity);
        page.markDirty();
    }
    
    /**
     * Gets the bitmap capacity.
     * 
     * @return The number of pages that can be tracked
     */
    public int getBitmapCapacity() {
        return buffer.getInt(BITMAP_CAPACITY_OFFSET);
    }
    
    /**
     * Marks a page as free in the bitmap.
     * A set bit (1) indicates that the page is free.
     * 
     * @param pageId The page ID to mark as free
     * @return true if the operation was successful
     */
    public boolean markPageAsFree(int pageId) {
        if (pageId < 0 || pageId >= getBitmapCapacity()) {
            return false;
        }
        
        int byteOffset = pageId / 8;
        int bitOffset = pageId % 8;
        
        int bytePos = BITMAP_START_OFFSET + byteOffset;
        byte currentByte = buffer.get(bytePos);
        byte newByte = (byte) (currentByte | (1 << bitOffset));
        buffer.put(bytePos, newByte);
        
        page.markDirty();
        logger.debug("Marked page {} as free", pageId);
        return true;
    }
    
    /**
     * Marks a page as used in the bitmap.
     * A cleared bit (0) indicates that the page is used.
     * 
     * @param pageId The page ID to mark as used
     * @return true if the operation was successful
     */
    public boolean markPageAsUsed(int pageId) {
        if (pageId < 0 || pageId >= getBitmapCapacity()) {
            return false;
        }
        
        int byteOffset = pageId / 8;
        int bitOffset = pageId % 8;
        
        int bytePos = BITMAP_START_OFFSET + byteOffset;
        byte currentByte = buffer.get(bytePos);
        byte newByte = (byte) (currentByte & ~(1 << bitOffset));
        buffer.put(bytePos, newByte);
        
        page.markDirty();
        logger.debug("Marked page {} as used", pageId);
        return true;
    }
    
    /**
     * Checks if a page is free.
     * 
     * @param pageId The page ID to check
     * @return true if the page is free (bit is set), false if it's used (bit is cleared) or outside the range
     */
    public boolean isPageFree(int pageId) {
        if (pageId < 0 || pageId >= getBitmapCapacity()) {
            return false;
        }
        
        int byteOffset = pageId / 8;
        int bitOffset = pageId % 8;
        
        int bytePos = BITMAP_START_OFFSET + byteOffset;
        byte currentByte = buffer.get(bytePos);
        
        return (currentByte & (1 << bitOffset)) != 0;
    }
    
    /**
     * Finds the next free page starting from the last checked page.
     * Updates the last checked page pointer.
     * 
     * @return The page ID of the next free page, or -1 if none found
     */
    public int findNextFreePage() {
        int startPageId = getLastCheckedPage();
        int capacity = getBitmapCapacity();
        
        // First pass: check from last checked page to end
        for (int i = startPageId; i < capacity; i++) {
            if (isPageFree(i)) {
                setLastCheckedPage((i + 1) % capacity); // Move to next page, wrap if needed
                return i;
            }
        }
        
        // Second pass: check from beginning to last checked page
        for (int i = 0; i < startPageId; i++) {
            if (isPageFree(i)) {
                setLastCheckedPage((i + 1) % capacity); // Move to next page, wrap if needed
                return i;
            }
        }
        
        // No free pages found
        return -1;
    }
    
    /**
     * Allocates a page by finding the next free page and marking it as used.
     * 
     * @return The allocated page ID, or -1 if no free pages are available
     */
    public int allocatePage() {
        int pageId = findNextFreePage();
        if (pageId >= 0) {
            markPageAsUsed(pageId);
            logger.debug("Allocated page {}", pageId);
        } else {
            logger.warn("No free pages available for allocation");
        }
        return pageId;
    }
    
    /**
     * Counts the number of free pages in the tablespace.
     * 
     * @return The number of free pages
     */
    public int countFreePages() {
        int count = 0;
        int capacity = getBitmapCapacity();
        
        for (int i = 0; i < capacity; i++) {
            if (isPageFree(i)) {
                count++;
            }
        }
        
        return count;
    }
} 