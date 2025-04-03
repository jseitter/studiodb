package net.seitter.studiodb.web.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents the status of a buffer pool.
 */
public class BufferPoolStatus {
    private String name;
    private String tablespaceName;
    private int size;
    private int capacity;
    private int dirtyPageCount;
    private List<PageStatus> pages;

    /**
     * Default constructor for JSON serialization.
     */
    public BufferPoolStatus() {
        this.pages = new ArrayList<>();
    }

    /**
     * Creates a new buffer pool status.
     *
     * @param name The name of the buffer pool
     * @param size The current number of pages in the buffer pool
     * @param capacity The maximum capacity of the buffer pool
     * @param pages The pages in the buffer pool
     */
    public BufferPoolStatus(String name, int size, int capacity, List<PageStatus> pages) {
        this.name = name;
        this.size = size;
        this.capacity = capacity;
        this.pages = pages != null ? pages : new ArrayList<>();
    }

    /**
     * Creates a new buffer pool status with tablespace name and dirty page count.
     *
     * @param name The name of the buffer pool
     * @param tablespaceName The name of the tablespace
     * @param size The current number of pages in the buffer pool
     * @param capacity The maximum capacity of the buffer pool
     * @param dirtyPageCount The number of dirty pages
     * @param pages The pages in the buffer pool
     */
    public BufferPoolStatus(String name, String tablespaceName, int size, int capacity, 
                           int dirtyPageCount, List<PageStatus> pages) {
        this.name = name;
        this.tablespaceName = tablespaceName;
        this.size = size;
        this.capacity = capacity;
        this.dirtyPageCount = dirtyPageCount;
        this.pages = pages != null ? pages : new ArrayList<>();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTablespaceName() {
        return tablespaceName;
    }

    public void setTablespaceName(String tablespaceName) {
        this.tablespaceName = tablespaceName;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int getCapacity() {
        return capacity;
    }

    public void setCapacity(int capacity) {
        this.capacity = capacity;
    }

    public int getDirtyPageCount() {
        return dirtyPageCount;
    }

    public void setDirtyPageCount(int dirtyPageCount) {
        this.dirtyPageCount = dirtyPageCount;
    }

    public List<PageStatus> getPages() {
        return pages;
    }

    public void setPages(List<PageStatus> pages) {
        this.pages = pages;
    }

    @Override
    public String toString() {
        return "BufferPoolStatus{" +
                "name='" + name + '\'' +
                ", tablespaceName='" + tablespaceName + '\'' +
                ", size=" + size +
                ", capacity=" + capacity +
                ", dirtyPageCount=" + dirtyPageCount +
                ", pages=" + pages +
                '}';
    }
} 