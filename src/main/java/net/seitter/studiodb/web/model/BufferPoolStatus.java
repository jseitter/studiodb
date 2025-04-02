package net.seitter.studiodb.web.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents the status of a buffer pool.
 */
public class BufferPoolStatus {
    private String name;
    private int size;
    private int capacity;
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

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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
                ", size=" + size +
                ", capacity=" + capacity +
                ", pages=" + pages +
                '}';
    }
} 