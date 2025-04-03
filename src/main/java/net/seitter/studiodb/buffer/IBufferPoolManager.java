package net.seitter.studiodb.buffer;

import net.seitter.studiodb.storage.Page;
import net.seitter.studiodb.storage.PageId;

import java.io.IOException;

/**
 * Interface for buffer pool managers that handle caching of database pages in memory.
 */
public interface IBufferPoolManager {
    /**
     * Gets the name of the tablespace this buffer pool manages.
     * 
     * @return The tablespace name
     */
    String getTablespaceName();
    
    /**
     * Gets the number of dirty pages in the buffer pool.
     * 
     * @return The number of dirty pages
     */
    int getDirtyPageCount();
    
    /**
     * Gets the current number of pages in the buffer pool.
     *
     * @return The number of pages in the buffer pool
     */
    int getSize();

    /**
     * Gets the maximum capacity of the buffer pool.
     *
     * @return The maximum capacity in pages
     */
    int getCapacity();
    
    /**
     * Fetches a page from the buffer pool, loading it from disk if necessary.
     * 
     * @param pageId The ID of the page to fetch
     * @return The page, or null if not found
     * @throws IOException If there's an error reading the page
     */
    Page fetchPage(PageId pageId) throws IOException;
    
    /**
     * Unpins a page from the buffer pool.
     * 
     * @param pageId The ID of the page to unpin
     * @param isDirty Whether the page was modified
     */
    void unpinPage(PageId pageId, boolean isDirty);
    
    /**
     * Flushes all dirty pages to disk.
     * 
     * @throws IOException If there's an error writing the pages
     */
    void flushAll() throws IOException;
    
    /**
     * Allocates a new page in the tablespace.
     * 
     * @return The newly allocated page
     * @throws IOException If there's an error allocating the page
     */
    Page allocatePage() throws IOException;
    
    /**
     * Shuts down the buffer pool manager, flushing all dirty pages.
     */
    void shutdown();
} 