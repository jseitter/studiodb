package net.seitter.studiodb.buffer;

import net.seitter.studiodb.storage.Page;
import net.seitter.studiodb.storage.PageId;

import java.io.IOException;
import java.util.Map;

/**
 * Interface for a buffer pool manager that manages a pool of pages in memory.
 */
public interface IBufferPoolManager {
    /**
     * Fetches a page from the buffer pool, loading it from disk if necessary.
     *
     * @param pageId The ID of the page to fetch
     * @return The fetched page, or null if the page doesn't exist
     * @throws IOException If there's an error reading the page from disk
     */
    Page fetchPage(PageId pageId) throws IOException;
    
    /**
     * Unpins a page, allowing it to be evicted if necessary.
     *
     * @param pageId The ID of the page to unpin
     * @param isDirty Whether the page was modified
     */
    void unpinPage(PageId pageId, boolean isDirty);
    
    /**
     * Flushes a dirty page to disk.
     *
     * @param pageId The ID of the page to flush
     * @return true if the page was flushed, false if the page is not in the buffer pool or not dirty
     * @throws IOException If there's an error writing the page to disk
     */
    boolean flushPage(PageId pageId) throws IOException;
    
    /**
     * Allocates a new page in the tablespace and loads it into the buffer pool.
     *
     * @return The newly allocated page
     * @throws IOException If there's an error allocating the page
     */
    Page allocatePage() throws IOException;
    
    /**
     * Flushes all dirty pages in the buffer pool to disk.
     *
     * @throws IOException If there's an error writing pages to disk
     */
    void flushAll() throws IOException;
    
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
     * Gets the name of the tablespace this buffer pool serves.
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
     * Shuts down the buffer pool manager, stopping the page cleaner and flushing all dirty pages.
     */
    void shutdown();
    
    /**
     * Gets statistics about the buffer pool.
     * 
     * @return A map of statistic names to values
     */
    Map<String, Object> getStatistics();
    
    /**
     * Gets details about pages currently in the buffer pool.
     * 
     * @return A map of page IDs to page details
     */
    Map<PageId, Map<String, Object>> getPageDetails();
} 