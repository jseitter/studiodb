package net.seitter.studiodb.storage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Layout class for table data pages.
 * Handles the specific structure and operations for table data pages.
 */
public class TableDataPageLayout extends PageLayout {
    private static final int ROW_DIRECTORY_ENTRY_SIZE = 8; // 4 bytes for offset, 4 bytes for length
    
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
        int directoryPos = HEADER_SIZE + (index * ROW_DIRECTORY_ENTRY_SIZE);
        
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
        int requiredSpace = rowData.length + ROW_DIRECTORY_ENTRY_SIZE;
        int directoryEndPos = HEADER_SIZE + ((rowCount + 1) * ROW_DIRECTORY_ENTRY_SIZE);
        int availableSpace = freeSpaceOffset - directoryEndPos;
        
        if (requiredSpace > availableSpace) {
            return false;
        }
        
        // Calculate new free space offset
        int newRowOffset = freeSpaceOffset - rowData.length;
        
        // Write row data
        buffer.position(newRowOffset);
        buffer.put(rowData);
        
        // Update row directory
        int directoryPos = HEADER_SIZE + (rowCount * ROW_DIRECTORY_ENTRY_SIZE);
        buffer.putInt(directoryPos, newRowOffset);
        buffer.putInt(directoryPos + 4, rowData.length);
        
        // Update metadata
        setRowCount(rowCount + 1);
        setFreeSpaceOffset(newRowOffset);
        
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
     * 
     * @return The amount of free space in bytes
     */
    public int getFreeSpace() {
        int rowCount = getRowCount();
        int freeSpaceOffset = getFreeSpaceOffset();
        int rowDirectorySize = rowCount * ROW_DIRECTORY_ENTRY_SIZE;
        
        // Calculate used space
        int usedSpace = freeSpaceOffset + rowDirectorySize;
        return Math.max(0, buffer.limit() - usedSpace);
    }
} 