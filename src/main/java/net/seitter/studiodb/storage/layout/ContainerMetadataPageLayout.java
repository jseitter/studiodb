package net.seitter.studiodb.storage.layout;

import java.nio.ByteBuffer;
import java.util.Date;
import net.seitter.studiodb.storage.Page;
import net.seitter.studiodb.storage.PageLayout;
import net.seitter.studiodb.storage.PageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Layout class for container metadata pages.
 * This is always Page 0 of a container and stores essential metadata about the tablespace.
 */
public class ContainerMetadataPageLayout extends PageLayout {
    private static final Logger logger = LoggerFactory.getLogger(ContainerMetadataPageLayout.class);
    
    // Constants for fixed field offsets after header
    // Page size is 4 bytes
    private static final int PAGE_SIZE_OFFSET = HEADER_SIZE;
    // Creation time is 8 bytes
    private static final int CREATION_TIME_OFFSET = PAGE_SIZE_OFFSET + 4;
    // Last opened time is 8 bytes
    private static final int LAST_OPENED_TIME_OFFSET = CREATION_TIME_OFFSET + 8;
    // Total pages is 4 bytes
    private static final int TOTAL_PAGES_OFFSET = LAST_OPENED_TIME_OFFSET + 8;
    // Free space map page id is 4 bytes
    private static final int FREE_SPACE_MAP_PAGE_ID_OFFSET = TOTAL_PAGES_OFFSET + 4;
    // Tablespace name length is 2 bytes
    private static final int TS_NAME_LENGTH_OFFSET = FREE_SPACE_MAP_PAGE_ID_OFFSET + 4;
    // Tablespace name starts immediately after the length
    private static final int TS_NAME_START_OFFSET = TS_NAME_LENGTH_OFFSET + 2;
    
    public ContainerMetadataPageLayout(Page page) {
        super(page);
    }
    
    /**
     * Initializes a new container metadata page.
     */
    @Override
    public void initialize() {
        writeHeader(PageType.CONTAINER_METADATA);
        
        // Set initial values for fixed-length fields
        setPageSize(page.getPageSize());
        setCreationTime(System.currentTimeMillis());
        setLastOpenedTime(System.currentTimeMillis());
        setTotalPages(0);
        setFreeSpaceMapPageId(1); // Page 1 is typically the free space map
        
        // Set tablespace name last (variable length field)
        setTablespaceName(""); // Empty name initially
        
        logger.debug("Initialized container metadata page");
    }
    
    /**
     * Sets the tablespace name.
     * 
     * @param name The name of the tablespace
     */
    public void setTablespaceName(String name) {
        if (name == null) {
            name = "";
        }
        
        // Limit name length to avoid buffer overflow
        if (name.length() > 64) {
            name = name.substring(0, 64);
            logger.warn("Tablespace name truncated to 64 characters");
        }
        
        // Write name length (2 bytes)
        buffer.putShort(TS_NAME_LENGTH_OFFSET, (short) name.length());
        
        // Write name characters (each character is 2 bytes in Java)
        int pos = TS_NAME_START_OFFSET;
        for (int i = 0; i < name.length(); i++) {
            buffer.putChar(pos, name.charAt(i));
            pos += 2;
        }
        
        page.markDirty();
    }
    
    /**
     * Gets the tablespace name.
     * 
     * @return The name of the tablespace
     */
    public String getTablespaceName() {
        int nameLength = buffer.getShort(TS_NAME_LENGTH_OFFSET) & 0xFFFF;
        if (nameLength == 0) {
            return "";
        }
        
        // Safety check - prevent reading beyond buffer
        if (nameLength > 64) {
            nameLength = 64;
            logger.warn("Tablespace name length exceeds maximum, truncating to 64");
        }
        
        StringBuilder name = new StringBuilder(nameLength);
        int pos = TS_NAME_START_OFFSET;
        for (int i = 0; i < nameLength; i++) {
            name.append(buffer.getChar(pos));
            pos += 2;
        }
        
        return name.toString();
    }
    
    /**
     * Sets the page size for this tablespace.
     * 
     * @param pageSize The page size in bytes
     */
    public void setPageSize(int pageSize) {
        buffer.putInt(PAGE_SIZE_OFFSET, pageSize);
        page.markDirty();
    }
    
    /**
     * Gets the page size for this tablespace.
     * 
     * @return The page size in bytes
     */
    public int getPageSize() {
        return buffer.getInt(PAGE_SIZE_OFFSET);
    }
    
    /**
     * Sets the creation time of this tablespace.
     * 
     * @param creationTime The creation time as milliseconds since epoch
     */
    public void setCreationTime(long creationTime) {
        buffer.putLong(CREATION_TIME_OFFSET, creationTime);
        page.markDirty();
    }
    
    /**
     * Gets the creation time of this tablespace.
     * 
     * @return The creation time as milliseconds since epoch
     */
    public long getCreationTime() {
        return buffer.getLong(CREATION_TIME_OFFSET);
    }
    
    /**
     * Gets the creation date of this tablespace.
     * 
     * @return The creation date
     */
    public Date getCreationDate() {
        return new Date(getCreationTime());
    }
    
    /**
     * Sets the last opened time of this tablespace.
     * 
     * @param lastOpenedTime The last opened time as milliseconds since epoch
     */
    public void setLastOpenedTime(long lastOpenedTime) {
        buffer.putLong(LAST_OPENED_TIME_OFFSET, lastOpenedTime);
        page.markDirty();
    }
    
    /**
     * Gets the last opened time of this tablespace.
     * 
     * @return The last opened time as milliseconds since epoch
     */
    public long getLastOpenedTime() {
        return buffer.getLong(LAST_OPENED_TIME_OFFSET);
    }
    
    /**
     * Gets the last opened date of this tablespace.
     * 
     * @return The last opened date
     */
    public Date getLastOpenedDate() {
        return new Date(getLastOpenedTime());
    }
    
    /**
     * Sets the total number of pages in this tablespace.
     * 
     * @param totalPages The total number of pages
     */
    public void setTotalPages(int totalPages) {
        buffer.putInt(TOTAL_PAGES_OFFSET, totalPages);
        page.markDirty();
    }
    
    /**
     * Gets the total number of pages in this tablespace.
     * 
     * @return The total number of pages
     */
    public int getTotalPages() {
        return buffer.getInt(TOTAL_PAGES_OFFSET);
    }
    
    /**
     * Sets the page ID of the free space map.
     * 
     * @param pageId The page ID
     */
    public void setFreeSpaceMapPageId(int pageId) {
        buffer.putInt(FREE_SPACE_MAP_PAGE_ID_OFFSET, pageId);
        page.markDirty();
    }
    
    /**
     * Gets the page ID of the free space map.
     * 
     * @return The page ID
     */
    public int getFreeSpaceMapPageId() {
        return buffer.getInt(FREE_SPACE_MAP_PAGE_ID_OFFSET);
    }
} 