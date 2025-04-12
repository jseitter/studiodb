package net.seitter.studiodb.buffer;

/**
 * Configuration for the page cleaner.
 */
public class PageCleanerConfig {
    private final boolean enabled;
    private final int cleaningIntervalMs;
    private final int dirtyPageThreshold;
    
    /**
     * Creates a new page cleaner configuration.
     *
     * @param enabled Whether the page cleaner is enabled
     * @param cleaningIntervalMs The interval between cleaning operations in milliseconds
     * @param dirtyPageThreshold The threshold of dirty pages that triggers a cleaning operation
     */
    public PageCleanerConfig(boolean enabled, int cleaningIntervalMs, int dirtyPageThreshold) {
        this.enabled = enabled;
        this.cleaningIntervalMs = cleaningIntervalMs;
        this.dirtyPageThreshold = dirtyPageThreshold;
    }
    
    /**
     * Checks if the page cleaner is enabled.
     *
     * @return true if enabled, false otherwise
     */
    public boolean isEnabled() {
        return enabled;
    }
    
    /**
     * Gets the interval between cleaning operations in milliseconds.
     *
     * @return The cleaning interval in milliseconds
     */
    public int getCleaningIntervalMs() {
        return cleaningIntervalMs;
    }
    
    /**
     * Gets the interval between cleaning operations in milliseconds.
     * This is an alias for getCleaningIntervalMs() for API consistency.
     *
     * @return The check interval in milliseconds
     */
    public int getCheckIntervalMs() {
        return cleaningIntervalMs;
    }
    
    /**
     * Gets the threshold of dirty pages that triggers a cleaning operation.
     *
     * @return The dirty page threshold
     */
    public int getDirtyPageThreshold() {
        return dirtyPageThreshold;
    }
    
    /**
     * Gets the default page cleaner configuration.
     *
     * @return The default configuration
     */
    public static PageCleanerConfig getDefault() {
        return new PageCleanerConfig(true, 5000, 10);
    }
} 