package net.seitter.studiodb.buffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A background thread that periodically flushes dirty pages to disk.
 */
public class PageCleaner extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(PageCleaner.class);

    private final IBufferPoolManager bufferPoolManager;
    private final PageCleanerConfig config;
    private volatile boolean running;

    /**
     * Creates a new page cleaner.
     *
     * @param bufferPoolManager The buffer pool manager to clean
     * @param config The page cleaner configuration
     */
    public PageCleaner(IBufferPoolManager bufferPoolManager, PageCleanerConfig config) {
        this.bufferPoolManager = bufferPoolManager;
        this.config = config;
        this.running = false;
        setName("PageCleaner-" + bufferPoolManager.getTablespaceName());
    }

    /**
     * Starts the page cleaner thread.
     */
    public void start() {
        if (!config.isEnabled()) {
            logger.info("Page cleaner is disabled");
            return;
        }

        running = true;
        super.start();
        logger.info("Started page cleaner for tablespace '{}'", bufferPoolManager.getTablespaceName());
    }

    /**
     * Stops the page cleaner thread.
     */
    public void shutdown() {
        running = false;
        interrupt();
        logger.info("Stopped page cleaner for tablespace '{}'", bufferPoolManager.getTablespaceName());
    }

    @Override
    public void run() {
        while (running) {
            try {
                // Check if we need to clean
                int dirtyPages = bufferPoolManager.getDirtyPageCount();
                
                if (dirtyPages > 0) {
                    logger.debug("Cleaning {} dirty pages in tablespace '{}'", 
                            dirtyPages, bufferPoolManager.getTablespaceName());
                    
                    bufferPoolManager.flushAll();
                }
                
                // Sleep until next cleaning interval
                Thread.sleep(config.getCleaningIntervalMs());
            } catch (InterruptedException e) {
                if (running) {
                    logger.warn("Page cleaner interrupted", e);
                }
            } catch (IOException e) {
                logger.error("Error cleaning pages", e);
            }
        }
    }
} 