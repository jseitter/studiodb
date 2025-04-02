package net.seitter.studiodb.storage;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Represents a single page in the database, which is the basic unit of storage.
 * Pages have a unique identifier and contain raw bytes of data.
 */
public class Page {
    private final PageId pageId;
    private final byte[] data;
    private boolean dirty;
    private int pinCount;

    /**
     * Creates a new page with the specified ID and size.
     *
     * @param pageId The unique identifier for the page
     * @param pageSize The size of the page in bytes
     */
    public Page(PageId pageId, int pageSize) {
        this.pageId = pageId;
        this.data = new byte[pageSize];
        this.dirty = false;
        this.pinCount = 0;
    }

    /**
     * Creates a new page with the specified ID and data.
     *
     * @param pageId The unique identifier for the page
     * @param data The data for the page
     */
    public Page(PageId pageId, byte[] data) {
        this.pageId = pageId;
        this.data = Arrays.copyOf(data, data.length);
        this.dirty = false;
        this.pinCount = 0;
    }

    /**
     * Gets the page ID.
     *
     * @return The page ID
     */
    public PageId getPageId() {
        return pageId;
    }

    /**
     * Gets the page data as a byte array.
     *
     * @return The page data
     */
    public byte[] getData() {
        return data;
    }

    /**
     * Gets the page data as a ByteBuffer.
     *
     * @return The page data as a ByteBuffer
     */
    public ByteBuffer getBuffer() {
        return ByteBuffer.wrap(data);
    }

    /**
     * Checks if the page is dirty (has been modified).
     *
     * @return true if the page is dirty
     */
    public boolean isDirty() {
        return dirty;
    }

    /**
     * Marks the page as dirty (modified).
     */
    public void markDirty() {
        this.dirty = true;
    }

    /**
     * Marks the page as clean (not modified or changes persisted).
     */
    public void markClean() {
        this.dirty = false;
    }

    /**
     * Gets the pin count for this page.
     * Pin count represents how many operations are currently using this page.
     *
     * @return The pin count
     */
    public int getPinCount() {
        return pinCount;
    }

    /**
     * Increments the pin count for this page.
     */
    public void pin() {
        pinCount++;
    }

    /**
     * Decrements the pin count for this page.
     *
     * @return true if the page was successfully unpinned, false if already at 0
     */
    public boolean unpin() {
        if (pinCount > 0) {
            pinCount--;
            return true;
        }
        return false;
    }

    /**
     * Copies data from the provided byte array into this page.
     *
     * @param sourceData The source data to copy
     */
    public void writeData(byte[] sourceData) {
        System.arraycopy(sourceData, 0, data, 0, Math.min(sourceData.length, data.length));
        markDirty();
    }

    /**
     * Writes data to the page at the specified offset.
     *
     * @param offset The offset to write at
     * @param sourceData The source data to write
     * @param length The number of bytes to write
     */
    public void writeData(int offset, byte[] sourceData, int length) {
        System.arraycopy(sourceData, 0, data, offset, Math.min(length, data.length - offset));
        markDirty();
    }

    /**
     * Gets the size of the page in bytes.
     *
     * @return The page size
     */
    public int getPageSize() {
        return data.length;
    }
} 