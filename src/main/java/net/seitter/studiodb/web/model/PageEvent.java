package net.seitter.studiodb.web.model;

import java.time.Instant;

/**
 * Represents a page event for real-time visualization.
 */
public class PageEvent {
    
    /**
     * Event types for page operations.
     */
    public enum EventType {
        PAGE_READ,       // Page read from disk to buffer pool
        PAGE_WRITE,      // Page written from buffer pool to disk
        PAGE_ALLOCATE,   // New page allocated 
        PAGE_PIN,        // Page pinned in buffer pool
        PAGE_UNPIN,      // Page unpinned in buffer pool
        PAGE_DIRTY,      // Page marked as dirty
        PAGE_EVICT,      // Page evicted from buffer pool
        BUFFER_FLUSH     // All dirty pages flushed to disk
    }
    
    private String tablespaceName;
    private int pageId;
    private EventType eventType;
    private Instant timestamp;
    private String details;

    /**
     * Default constructor for JSON serialization.
     */
    public PageEvent() {
        this.timestamp = Instant.now();
    }

    /**
     * Creates a new page event.
     *
     * @param tablespaceName The name of the tablespace containing the page
     * @param pageId The ID of the page
     * @param eventType The type of event
     * @param details Additional details about the event
     */
    public PageEvent(String tablespaceName, int pageId, EventType eventType, String details) {
        this.tablespaceName = tablespaceName;
        this.pageId = pageId;
        this.eventType = eventType;
        this.timestamp = Instant.now();
        this.details = details;
    }

    public String getTablespaceName() {
        return tablespaceName;
    }

    public void setTablespaceName(String tablespaceName) {
        this.tablespaceName = tablespaceName;
    }

    public int getPageId() {
        return pageId;
    }

    public void setPageId(int pageId) {
        this.pageId = pageId;
    }

    public EventType getEventType() {
        return eventType;
    }

    public void setEventType(EventType eventType) {
        this.eventType = eventType;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public String getDetails() {
        return details;
    }

    public void setDetails(String details) {
        this.details = details;
    }

    @Override
    public String toString() {
        return "PageEvent{" +
                "tablespaceName='" + tablespaceName + '\'' +
                ", pageId=" + pageId +
                ", eventType=" + eventType +
                ", timestamp=" + timestamp +
                ", details='" + details + '\'' +
                '}';
    }
} 