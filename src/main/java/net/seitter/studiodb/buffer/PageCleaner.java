package net.seitter.studiodb.buffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Background thread that periodically flushes dirty pages to disk.
 */
public class PageCleaner implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(PageCleaner.class);
    
    private final IBufferPoolManager bufferPool;
    private final PageCleanerConfig config;
    private final Thread cleanerThread;
    private final AtomicBoolean running;
    private final AtomicLong lastCleanTime;
    private final AtomicInteger totalCleanings;
    
    /**
     * Creates a new page cleaner.
     *
     * @param bufferPool The buffer pool to clean
     * @param config The cleaner configuration
     */
    public PageCleaner(IBufferPoolManager bufferPool, PageCleanerConfig config) {
        this.bufferPool = bufferPool;
        this.config = config;
        this.cleanerThread = new Thread(this, "PageCleaner-" + bufferPool.getTablespaceName());
        this.cleanerThread.setDaemon(true);
        this.running = new AtomicBoolean(false);
        this.lastCleanTime = new AtomicLong(0);
        this.totalCleanings = new AtomicInteger(0);
        
        logger.info("Page cleaner created for tablespace '{}' with interval {} ms and threshold {} pages",
                bufferPool.getTablespaceName(), config.getCheckIntervalMs(), config.getDirtyPageThreshold());
    }
    
    /**
     * Starts the page cleaner thread.
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            cleanerThread.start();
            logger.info("Started page cleaner for tablespace '{}'", bufferPool.getTablespaceName());
        }
    }
    
    /**
     * Stops the page cleaner thread.
     */
    public void shutdown() {
        if (running.compareAndSet(true, false)) {
            logger.info("Shutting down page cleaner for tablespace '{}'", bufferPool.getTablespaceName());
            cleanerThread.interrupt();
            try {
                cleanerThread.join(1000); // Wait up to 1 second for the thread to die
            } catch (InterruptedException e) {
                logger.warn("Interrupted while waiting for page cleaner to shut down", e);
            }
        }
    }
    
    @Override
    public void run() {
        logger.info("Page cleaner thread started for tablespace '{}'", bufferPool.getTablespaceName());
        
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                // Sleep for the configured interval
                Thread.sleep(config.getCheckIntervalMs());
                
                // Check if we need to clean dirty pages
                int dirtyPages = bufferPool.getDirtyPageCount();
                
                if (dirtyPages >= config.getDirtyPageThreshold()) {
                    logger.debug("Dirty page count ({}) exceeds threshold ({}), flushing pages to disk",
                            dirtyPages, config.getDirtyPageThreshold());
                    
                    // Flush all dirty pages
                    bufferPool.flushAll();
                    
                    // Update statistics
                    lastCleanTime.set(System.currentTimeMillis());
                    totalCleanings.incrementAndGet();
                    
                    logger.debug("Finished flushing dirty pages for tablespace '{}'",
                            bufferPool.getTablespaceName());
                }
            } catch (InterruptedException e) {
                // Thread was interrupted, exit the loop
                Thread.currentThread().interrupt();
                logger.info("Page cleaner thread interrupted, exiting");
                break;
            } catch (IOException e) {
                logger.error("Error flushing dirty pages", e);
                // Continue running, hopefully the error will resolve itself
            } catch (Exception e) {
                logger.error("Unexpected error in page cleaner", e);
                // Continue running, hopefully the error will resolve itself
            }
        }
        
        running.set(false);
        logger.info("Page cleaner thread exited for tablespace '{}'", bufferPool.getTablespaceName());
    }
    
    /**
     * Checks if the cleaner thread is currently running.
     * 
     * @return true if the cleaner is running, false otherwise
     */
    public boolean isRunning() {
        return running.get();
    }
    
    /**
     * Gets the cleaner configuration.
     * 
     * @return The configuration
     */
    public PageCleanerConfig getConfig() {
        return config;
    }
    
    /**
     * Gets the timestamp of the last time pages were cleaned.
     * 
     * @return The timestamp in milliseconds, or 0 if never cleaned
     */
    public long getLastCleanTime() {
        return lastCleanTime.get();
    }
    
    /**
     * Gets the total number of cleaning operations performed.
     * 
     * @return The total number of cleanings
     */
    public int getTotalCleanings() {
        return totalCleanings.get();
    }
} 