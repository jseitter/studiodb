package net.seitter.studiodb.web.model;

/**
 * Represents the status of a tablespace.
 */
public class TablespaceStatus {
    private String name;
    private String containerPath;
    private int pageSize;
    private int totalPages;

    /**
     * Default constructor for JSON serialization.
     */
    public TablespaceStatus() {
    }

    /**
     * Creates a new tablespace status.
     *
     * @param name The name of the tablespace
     * @param containerPath The path to the storage container
     * @param pageSize The page size in bytes
     * @param totalPages The total number of pages
     */
    public TablespaceStatus(String name, String containerPath, int pageSize, int totalPages) {
        this.name = name;
        this.containerPath = containerPath;
        this.pageSize = pageSize;
        this.totalPages = totalPages;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getContainerPath() {
        return containerPath;
    }

    public void setContainerPath(String containerPath) {
        this.containerPath = containerPath;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public int getTotalPages() {
        return totalPages;
    }

    public void setTotalPages(int totalPages) {
        this.totalPages = totalPages;
    }

    @Override
    public String toString() {
        return "TablespaceStatus{" +
                "name='" + name + '\'' +
                ", containerPath='" + containerPath + '\'' +
                ", pageSize=" + pageSize +
                ", totalPages=" + totalPages +
                '}';
    }
} 