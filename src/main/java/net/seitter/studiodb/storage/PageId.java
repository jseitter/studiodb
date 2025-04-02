package net.seitter.studiodb.storage;

import java.util.Objects;

/**
 * Uniquely identifies a page within the database system.
 * A PageId consists of a tablespace name and a page number within that tablespace.
 */
public class PageId {
    private final String tablespaceName;
    private final int pageNumber;

    /**
     * Creates a new PageId.
     *
     * @param tablespaceName The name of the tablespace containing the page
     * @param pageNumber The number of the page within the tablespace
     */
    public PageId(String tablespaceName, int pageNumber) {
        this.tablespaceName = tablespaceName;
        this.pageNumber = pageNumber;
    }

    /**
     * Gets the tablespace name.
     *
     * @return The tablespace name
     */
    public String getTablespaceName() {
        return tablespaceName;
    }

    /**
     * Gets the page number.
     *
     * @return The page number
     */
    public int getPageNumber() {
        return pageNumber;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PageId pageId = (PageId) o;
        return pageNumber == pageId.pageNumber && 
               Objects.equals(tablespaceName, pageId.tablespaceName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tablespaceName, pageNumber);
    }

    @Override
    public String toString() {
        return tablespaceName + ":" + pageNumber;
    }
} 