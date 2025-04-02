package net.seitter.studiodb.web.model;

/**
 * Represents the status of a page in a buffer pool.
 */
public class PageStatus {
    private String tablespaceName;
    private int pageId;
    private boolean dirty;
    private int pinCount;
    private boolean modified;

    /**
     * Default constructor for JSON serialization.
     */
    public PageStatus() {
    }

    /**
     * Creates a new page status.
     *
     * @param tablespaceName The name of the tablespace containing the page
     * @param pageId The ID of the page
     * @param dirty Whether the page is dirty
     * @param pinCount The pin count of the page
     * @param modified Whether the page was modified since last flush
     */
    public PageStatus(String tablespaceName, int pageId, boolean dirty, int pinCount, boolean modified) {
        this.tablespaceName = tablespaceName;
        this.pageId = pageId;
        this.dirty = dirty;
        this.pinCount = pinCount;
        this.modified = modified;
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

    public boolean isDirty() {
        return dirty;
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    public int getPinCount() {
        return pinCount;
    }

    public void setPinCount(int pinCount) {
        this.pinCount = pinCount;
    }

    public boolean isModified() {
        return modified;
    }

    public void setModified(boolean modified) {
        this.modified = modified;
    }

    @Override
    public String toString() {
        return "PageStatus{" +
                "tablespaceName='" + tablespaceName + '\'' +
                ", pageId=" + pageId +
                ", dirty=" + dirty +
                ", pinCount=" + pinCount +
                ", modified=" + modified +
                '}';
    }
} 