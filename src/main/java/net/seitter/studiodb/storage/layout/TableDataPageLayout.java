package net.seitter.studiodb.storage.layout;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import net.seitter.studiodb.storage.Page;
import net.seitter.studiodb.storage.PageLayout;
import net.seitter.studiodb.storage.PageType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Layout class for table data pages.
 * Handles the specific structure and operations for table data pages.
 */
public class TableDataPageLayout extends PageLayout {
    private static final Logger logger = LoggerFactory.getLogger(TableDataPageLayout.class);
    // Offset for row count within the header (previously was in PageLayout)
    private static final int ROW_COUNT_OFFSET = HEADER_SIZE;
    public static final int ROW_DIRECTORY_ENTRY_SIZE = 8; // 4 bytes for offset, 4 bytes for length
    public static final int ROW_DIRECTORY_START = HEADER_SIZE+4; // Row directory starts after header
    

    
    public TableDataPageLayout(Page page) {
        super(page);
    }
    
    /**
     * Initializes a new table data page.
     */
    public void initialize() {
        writeHeader(PageType.TABLE_DATA);
        
        // Set the free space to start at the end of the buffer
        // The free space grows downward as rows are added from the end
        setFreeSpaceOffset(buffer.capacity());
        
        // Pre-allocate some space for the row directory to avoid calculation errors
        // with very first row insertion
        setRowCount(0);
        
        logger.debug("Initialized table data page with free space offset at {}", buffer.capacity());
    }
    
    /**
     * Sets the row count in the page header.
     * 
     * @param rowCount The number of rows
     */
    public void setRowCount(int rowCount) {
        buffer.putInt(ROW_COUNT_OFFSET, rowCount);
        page.markDirty();
    }
    
    /**
     * Gets the row count from the page header.
     * 
     * @return The number of rows
     */
    public int getRowCount() {
        return buffer.getInt(ROW_COUNT_OFFSET);
    }
    
    /**
     * Gets the row data at the specified index.
     * 
     * @param index The row index
     * @return The row data, or null if the index is invalid
     */
    public byte[] getRow(int index) {
        if (index < 0 || index >= getRowCount()) {
            return null;
        }
        
        // Calculate position in row directory
        int directoryPos = ROW_DIRECTORY_START + (index * ROW_DIRECTORY_ENTRY_SIZE);
        
        // Get row offset and length
        int rowOffset = buffer.getInt(directoryPos);
        int rowLength = buffer.getInt(directoryPos + 4);
        
        // Read row data
        byte[] rowData = new byte[rowLength];
        buffer.position(rowOffset);
        buffer.get(rowData);
        
        return rowData;
    }
    
    /**
     * Adds a new row to the page.
     * 
     * @param rowData The row data to add
     * @return true if the row was added successfully, false if there wasn't enough space
     */
    public boolean addRow(byte[] rowData) {
        int rowCount = getRowCount();
        int freeSpaceOffset = getFreeSpaceOffset();
        
        // Calculate required space
        int requiredSpace = rowData.length;
        int directoryEndPos = ROW_DIRECTORY_START + ((rowCount + 1) * ROW_DIRECTORY_ENTRY_SIZE);
        
        // Calculate available space - this is the gap between directory end and free space offset
        int availableSpace = freeSpaceOffset - directoryEndPos;
        
        logger.debug("Row space calculation - Free space offset: {}, Directory end: {}, Available: {}, Required: {}", 
            freeSpaceOffset, directoryEndPos, availableSpace, requiredSpace);
        
        if (requiredSpace > availableSpace) {
            logger.debug("Not enough space to add row of size {} to page - available: {}", rowData.length, availableSpace);
            return false;
        }
        
        // Calculate new free space offset
        int newRowOffset = freeSpaceOffset - rowData.length;
        
        // Write row data
        buffer.position(newRowOffset);
        buffer.put(rowData);
        
        // Update row directory
        int directoryPos = ROW_DIRECTORY_START + (rowCount * ROW_DIRECTORY_ENTRY_SIZE);
        buffer.putInt(directoryPos, newRowOffset);
        buffer.putInt(directoryPos + 4, rowData.length);
        
        // Update metadata
        setRowCount(rowCount + 1);
        setFreeSpaceOffset(newRowOffset);
        
        logger.debug("Added row of size {} to page - new free space offset: {}, new available space: {}", 
            rowData.length, newRowOffset, newRowOffset - (ROW_DIRECTORY_START + ((rowCount + 1) * ROW_DIRECTORY_ENTRY_SIZE)));
        
        return true;
    }
    
    /**
     * Gets all rows in the page.
     * 
     * @return A list of row data
     */
    public List<byte[]> getAllRows() {
        List<byte[]> rows = new ArrayList<>();
        int rowCount = getRowCount();
        
        for (int i = 0; i < rowCount; i++) {
            byte[] rowData = getRow(i);
            if (rowData != null) {
                rows.add(rowData);
            }
        }
        
        return rows;
    }
    
    /**
     * Calculates the amount of free space in the page.
     * This takes into account the space needed for the row count field (4 bytes),
     * row directory entries, and actual row data.
     * 
     * @return The amount of free space in bytes
     */
    @Override
    public int getFreeSpace() {
        int rowCount = getRowCount();
        int freeSpaceOffset = getFreeSpaceOffset();
        int rowDirectorySize = rowCount * ROW_DIRECTORY_ENTRY_SIZE;
        int directoryEndPos = ROW_DIRECTORY_START + rowDirectorySize;
        
        // Available space is between directory end and free space offset
        return Math.max(0, freeSpaceOffset - directoryEndPos);
    }
} 