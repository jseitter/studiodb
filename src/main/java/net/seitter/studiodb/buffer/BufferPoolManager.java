package net.seitter.studiodb.buffer;

import net.seitter.studiodb.storage.Page;
import net.seitter.studiodb.storage.PageId;
import net.seitter.studiodb.storage.StorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

/**
 * Manages a buffer pool for a single tablespace.
 * The buffer pool caches pages in memory to reduce disk I/O.
 */
public class BufferPoolManager {
    private static final Logger logger = LoggerFactory.getLogger(BufferPoolManager.class);

    private final String tablespaceName;
    private final StorageManager storageManager;
    private final int capacity;
    private final Map<PageId, Page> pageTable;
    private final Queue<PageId> replacementQueue;

    /**
     * Creates a new buffer pool manager.
     *
     * @param tablespaceName The name of the tablespace this buffer pool serves
     * @param storageManager The storage manager
     * @param capacity The maximum number of pages this buffer pool can hold
     */
    public BufferPoolManager(String tablespaceName, StorageManager storageManager, int capacity) {
        this.tablespaceName = tablespaceName;
        this.storageManager = storageManager;
        this.capacity = capacity;
        this.pageTable = new HashMap<>(capacity);
        this.replacementQueue = new LinkedList<>();

        logger.info("Buffer pool for tablespace '{}' initialized with capacity: {} pages", 
                tablespaceName, capacity);
    }

    /**
     * Fetches a page from the buffer pool, loading it from disk if necessary.
     *
     * @param pageId The ID of the page to fetch
     * @return The fetched page, or null if the page doesn't exist
     * @throws IOException If there's an error reading the page from disk
     */
    public synchronized Page fetchPage(PageId pageId) throws IOException {
        if (!pageId.getTablespaceName().equals(tablespaceName)) {
            throw new IllegalArgumentException("Page ID belongs to tablespace '" +
                    pageId.getTablespaceName() + "', not '" + tablespaceName + "'");
        }

        // Check if the page is already in the buffer pool
        if (pageTable.containsKey(pageId)) {
            Page page = pageTable.get(pageId);
            
            // Move to the end of the replacement queue (most recently used)
            replacementQueue.remove(pageId);
            replacementQueue.add(pageId);
            
            // Pin the page
            page.pin();
            
            logger.debug("Page {} hit in buffer pool", pageId);
            return page;
        }

        // Page is not in the buffer pool, need to load from disk
        Page page = storageManager.readPage(pageId);
        
        if (page == null) {
            logger.warn("Page {} does not exist on disk", pageId);
            return null;
        }

        // If the buffer pool is full, we need to evict a page
        if (pageTable.size() >= capacity) {
            evictPage();
        }

        // Add the page to the buffer pool
        pageTable.put(pageId, page);
        replacementQueue.add(pageId);
        
        // Pin the page
        page.pin();
        
        logger.debug("Page {} loaded from disk to buffer pool", pageId);
        return page;
    }

    /**
     * Unpins a page, allowing it to be evicted if necessary.
     *
     * @param pageId The ID of the page to unpin
     * @param isDirty Whether the page was modified
     * @return true if the page was successfully unpinned
     */
    public synchronized boolean unpinPage(PageId pageId, boolean isDirty) {
        if (!pageTable.containsKey(pageId)) {
            logger.warn("Attempted to unpin page {} that is not in the buffer pool", pageId);
            return false;
        }

        Page page = pageTable.get(pageId);
        
        if (isDirty) {
            page.markDirty();
        }
        
        boolean unpinned = page.unpin();
        
        if (!unpinned) {
            logger.warn("Attempted to unpin page {} with pin count already at 0", pageId);
        }
        
        return unpinned;
    }

    /**
     * Flushes a dirty page to disk.
     *
     * @param pageId The ID of the page to flush
     * @return true if the page was flushed, false if the page is not in the buffer pool or not dirty
     * @throws IOException If there's an error writing the page to disk
     */
    public synchronized boolean flushPage(PageId pageId) throws IOException {
        if (!pageTable.containsKey(pageId)) {
            logger.warn("Attempted to flush page {} that is not in the buffer pool", pageId);
            return false;
        }

        Page page = pageTable.get(pageId);
        
        if (page.isDirty()) {
            storageManager.writePage(page);
            page.markClean();
            logger.debug("Flushed dirty page {} to disk", pageId);
            return true;
        }
        
        return false;
    }

    /**
     * Allocates a new page in the tablespace and loads it into the buffer pool.
     *
     * @return The newly allocated page
     * @throws IOException If there's an error allocating the page
     */
    public synchronized Page allocatePage() throws IOException {
        Page page = storageManager.allocatePage(tablespaceName);
        
        if (page == null) {
            logger.error("Failed to allocate new page in tablespace '{}'", tablespaceName);
            return null;
        }

        // If the buffer pool is full, we need to evict a page
        if (pageTable.size() >= capacity) {
            evictPage();
        }

        // Add the page to the buffer pool
        PageId pageId = page.getPageId();
        pageTable.put(pageId, page);
        replacementQueue.add(pageId);
        
        // Pin the page
        page.pin();
        
        logger.debug("Allocated new page {} and added to buffer pool", pageId);
        return page;
    }

    /**
     * Evicts a page from the buffer pool to make room for new pages.
     * Uses a simple FIFO replacement policy.
     *
     * @throws IOException If there's an error flushing a dirty page
     */
    private void evictPage() throws IOException {
        // Find the first unpinned page to evict
        PageId victimPageId = null;
        
        for (PageId pageId : replacementQueue) {
            Page page = pageTable.get(pageId);
            
            if (page.getPinCount() == 0) {
                victimPageId = pageId;
                break;
            }
        }

        // If no unpinned pages were found, we cannot evict
        if (victimPageId == null) {
            logger.warn("Cannot evict any pages from buffer pool, all pages are pinned");
            return;
        }

        // Remove the victim page from the buffer pool
        Page victimPage = pageTable.remove(victimPageId);
        replacementQueue.remove(victimPageId);

        // If the page is dirty, write it back to disk
        if (victimPage.isDirty()) {
            storageManager.writePage(victimPage);
            logger.debug("Evicted dirty page {} and flushed to disk", victimPageId);
        } else {
            logger.debug("Evicted clean page {}", victimPageId);
        }
    }

    /**
     * Flushes all dirty pages in the buffer pool to disk.
     *
     * @throws IOException If there's an error writing pages to disk
     */
    public synchronized void flushAll() {
        try {
            for (Page page : pageTable.values()) {
                if (page.isDirty()) {
                    storageManager.writePage(page);
                    page.markClean();
                    logger.debug("Flushed dirty page {} to disk", page.getPageId());
                }
            }
            
            logger.info("Flushed all dirty pages in buffer pool for tablespace '{}'", tablespaceName);
        } catch (IOException e) {
            logger.error("Error flushing dirty pages in buffer pool for tablespace '{}'", 
                    tablespaceName, e);
        }
    }

    /**
     * Gets the current number of pages in the buffer pool.
     *
     * @return The number of pages in the buffer pool
     */
    public synchronized int getSize() {
        return pageTable.size();
    }

    /**
     * Gets the maximum capacity of the buffer pool.
     *
     * @return The maximum capacity in pages
     */
    public int getCapacity() {
        return capacity;
    }

    /**
     * Gets the name of the tablespace this buffer pool serves.
     *
     * @return The tablespace name
     */
    public String getTablespaceName() {
        return tablespaceName;
    }
} 