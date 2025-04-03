package net.seitter.studiodb.buffer;

/**
 * Configuration for the page cleaner.
 */
public class PageCleanerConfig {
    private final long cleaningIntervalMs;
    private final int maxDirtyPages;
    private final boolean enabled;

    /**
     * Creates a new page cleaner configuration.
     *
     * @param cleaningIntervalMs The interval in milliseconds between cleaning runs
     * @param maxDirtyPages The maximum number of dirty pages before forcing a clean
     * @param enabled Whether the page cleaner is enabled
     */
    public PageCleanerConfig(long cleaningIntervalMs, int maxDirtyPages, boolean enabled) {
        this.cleaningIntervalMs = cleaningIntervalMs;
        this.maxDirtyPages = maxDirtyPages;
        this.enabled = enabled;
    }

    /**
     * Gets the default page cleaner configuration.
     *
     * @return The default configuration
     */
    public static PageCleanerConfig getDefault() {
        return new PageCleanerConfig(5000, 100, true);
    }

    public long getCleaningIntervalMs() {
        return cleaningIntervalMs;
    }

    public int getMaxDirtyPages() {
        return maxDirtyPages;
    }

    public boolean isEnabled() {
        return enabled;
    }
} 