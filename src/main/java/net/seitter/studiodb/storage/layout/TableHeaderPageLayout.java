package net.seitter.studiodb.storage.layout;

import net.seitter.studiodb.schema.DataType;
import net.seitter.studiodb.storage.Page;
import net.seitter.studiodb.storage.PageLayout;
import net.seitter.studiodb.storage.PageType;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Layout class for table header pages.
 * Handles the specific structure and operations for table header pages.
 */
public class TableHeaderPageLayout extends PageLayout {
    // Offsets for various fields in the header page
    private static final int FIRST_DATA_PAGE_ID_OFFSET = HEADER_SIZE; // Immediately after header
    private static final int TABLE_NAME_LENGTH_OFFSET = FIRST_DATA_PAGE_ID_OFFSET + 4; // After first data page ID (4 bytes)
    private static final int TABLE_NAME_START_OFFSET = TABLE_NAME_LENGTH_OFFSET + 2; // After name length (2 bytes)
    
    public TableHeaderPageLayout(Page page) {
        super(page);
    }
    
    /**
     * Initializes a new table header page.
     */
    public void initialize() {
        try {
            // Clear the entire buffer first to avoid garbage data
            for (int i = 0; i < buffer.capacity(); i++) {
                buffer.put(i, (byte) 0);
            }
            
            writeHeader(PageType.TABLE_HEADER);
            
            // Initialize first data page ID to -1 (invalid)
            setFirstDataPageId(-1);
            
            // Initialize table name to empty string
            setTableName("");
            
            // Initialize column count to 0
            setColumnCount(0);
            
            // Validate initialization
            if (getFirstDataPageId() != -1 || !getTableName().equals("") || getColumnCount() != 0) {
                // Re-initialize if validation fails
                writeHeader(PageType.TABLE_HEADER);
                buffer.putInt(FIRST_DATA_PAGE_ID_OFFSET, -1);
                buffer.putShort(TABLE_NAME_LENGTH_OFFSET, (short) 0);
                buffer.putShort(TABLE_NAME_START_OFFSET, (short) 0);
            }
        } catch (Exception e) {
            // Handle initialization errors
        }
    }
    
    /**
     * Sets the first data page ID.
     * 
     * @param firstDataPageId The ID of the first data page
     */
    public void setFirstDataPageId(int firstDataPageId) {
        try {
            // Validate the firstDataPageId (reasonable range check)
            if (firstDataPageId < -1 || firstDataPageId > 10000) {
                // Log an error or handle invalid value
                firstDataPageId = -1; // Set to invalid as fallback
            }
            
            buffer.putInt(FIRST_DATA_PAGE_ID_OFFSET, firstDataPageId);
            page.markDirty();
        } catch (Exception e) {
            // Handle buffer exceptions
        }
    }
    
    /**
     * Gets the first data page ID.
     * 
     * @return The ID of the first data page
     */
    public int getFirstDataPageId() {
        try {
            int firstDataPageId = buffer.getInt(FIRST_DATA_PAGE_ID_OFFSET);
            
            // Check for unreasonable values (assuming pageId should be within reasonable range)
            // This is a common source of corruption - uninititalized values reading as very large numbers
            if (firstDataPageId < -1 || firstDataPageId > 10000) {
                return -1; // Return invalid page ID as fallback
            }
            
            return firstDataPageId;
        } catch (Exception e) {
            // In case of any error, return an invalid page ID
            return -1;
        }
    }
    
    /**
     * Sets the table name.
     * 
     * @param tableName The name of the table
     */
    public void setTableName(String tableName) {
        try {
            if (tableName == null) {
                tableName = "";
            }
            
            // Validate name length
            if (tableName.length() > 255) {
                tableName = tableName.substring(0, 255); // Truncate to maximum allowed
            }
            
            // Write name length (2 bytes)
            buffer.putShort(TABLE_NAME_LENGTH_OFFSET, (short) tableName.length());
            
            // Write name characters with bounds checking
            int pos = TABLE_NAME_START_OFFSET;
            int maxPos = buffer.capacity() - 2; // Leave room for a char (2 bytes)
            
            for (int i = 0; i < tableName.length() && pos < maxPos; i++) {
                buffer.putChar(pos, tableName.charAt(i));
                pos += 2;
            }
            
            page.markDirty();
        } catch (Exception e) {
            // Handle any buffer exceptions
        }
    }
    
    /**
     * Gets the table name.
     * 
     * @return The name of the table
     */
    public String getTableName() {
        try {
            int nameLength = buffer.getShort(TABLE_NAME_LENGTH_OFFSET) & 0xFFFF;
            if (nameLength == 0) {
                return "";
            }
            
            // Sanity check to avoid extremely large names
            if (nameLength > 1000) {
                return "[INVALID NAME LENGTH: " + nameLength + "]";
            }
            
            StringBuilder name = new StringBuilder(nameLength);
            int pos = TABLE_NAME_START_OFFSET;
            for (int i = 0; i < nameLength; i++) {
                // Check if we're still within the buffer limits
                if (pos + 1 >= buffer.limit()) {
                    return name + "[TRUNCATED]";
                }
                name.append(buffer.getChar(pos));
                pos += 2;
            }
            
            return name.toString();
        } catch (Exception e) {
            return "[ERROR: " + e.getMessage() + "]";
        }
    }
    
    /**
     * Gets the offset where column count is stored.
     * This is calculated based on the table name length.
     * 
     * @return The offset for column count
     */
    private int getColumnCountOffset() {
        try {
            int nameLength = buffer.getShort(TABLE_NAME_LENGTH_OFFSET) & 0xFFFF;
            
            // Sanity check to avoid invalid offsets
            if (nameLength > 1000) {
                return TABLE_NAME_START_OFFSET + 2; // Default to a reasonable position
            }
            
            return TABLE_NAME_START_OFFSET + (nameLength * 2);
        } catch (Exception e) {
            // If there's an error, return a default position
            return TABLE_NAME_START_OFFSET + 2;
        }
    }
    
    /**
     * Sets the number of columns.
     * 
     * @param columnCount The number of columns
     */
    public void setColumnCount(int columnCount) {
        int offset = getColumnCountOffset();
        buffer.putShort(offset, (short) columnCount);
        page.markDirty();
    }
    
    /**
     * Gets the number of columns.
     * 
     * @return The number of columns
     */
    public int getColumnCount() {
        try {
            int offset = getColumnCountOffset();
            
            // Check if the offset is within buffer limits
            if (offset + 1 >= buffer.limit()) {
                return 0;
            }
            
            int count = buffer.getShort(offset) & 0xFFFF;
            
            // Sanity check to avoid unreasonable column counts
            if (count > 1000) {
                return 0;
            }
            
            return count;
        } catch (Exception e) {
            return 0;
        }
    }
    
    /**
     * Gets the offset where column definitions start.
     * This is after the column count.
     * 
     * @return The starting offset for column definitions
     */
    private int getColumnsStartOffset() {
        return getColumnCountOffset() + 2; // After column count (2 bytes)
    }
    
    /**
     * Adds a column definition to the header.
     * 
     * @param name The column name
     * @param dataType The column data type
     * @param maxLength The maximum length for variable-length types
     * @param nullable Whether the column is nullable
     */
    public void addColumn(String name, DataType dataType, int maxLength, boolean nullable) {
        try {
            if (name == null) {
                name = "";
            }
            
            // Validate name length
            if (name.length() > 255) {
                name = name.substring(0, 255); // Truncate name if too long
            }
            
            int columnCount = getColumnCount();
            
            // Calculate size of existing column definitions
            int columnsSize = 0;
            List<ColumnDefinition> existingColumns = getColumns();
            
            for (ColumnDefinition col : existingColumns) {
                // 2 bytes for name length + (name.length * 2) for name chars + 1 byte for type + 2 bytes for maxLength + 1 byte for nullable
                columnsSize += 2 + (col.getName().length() * 2) + 4;
            }
            
            // Calculate offset for new column
            int columnOffset = getColumnsStartOffset() + columnsSize;
            
            // Check if adding this column would exceed buffer capacity
            int requiredSpace = columnOffset + 2 + (name.length() * 2) + 4;
            if (requiredSpace >= buffer.capacity()) {
                // Not enough space, handle appropriately
                return;
            }
            
            // Write column name length (2 bytes)
            buffer.putShort(columnOffset, (short) name.length());
            
            // Write column name
            int pos = columnOffset + 2;
            for (int i = 0; i < name.length(); i++) {
                if (pos + 1 >= buffer.capacity()) {
                    break; // Stop if we're at buffer boundary
                }
                buffer.putChar(pos, name.charAt(i));
                pos += 2;
            }
            
            // Check if we have enough space left for remaining fields
            if (pos + 4 < buffer.capacity()) {
                // Write data type (1 byte)
                buffer.put(pos, (byte) dataType.ordinal());
                pos++;
                
                // Write max length (2 bytes)
                buffer.putShort(pos, (short) maxLength);
                pos += 2;
                
                // Write nullable flag (1 byte)
                buffer.put(pos, (byte) (nullable ? 1 : 0));
                
                // Update column count
                setColumnCount(columnCount + 1);
            }
            
            page.markDirty();
        } catch (Exception e) {
            // Handle any exceptions
        }
    }
    
    /**
     * Gets all column definitions from the header.
     * 
     * @return A list of column definitions
     */
    public List<ColumnDefinition> getColumns() {
        List<ColumnDefinition> columns = new ArrayList<>();
        try {
            int columnCount = getColumnCount();
            if (columnCount <= 0) {
                return columns;
            }
            
            int pos = getColumnsStartOffset();
            
            for (int i = 0; i < columnCount; i++) {
                try {
                    // Check if we have enough buffer left
                    if (pos + 1 >= buffer.limit()) {
                        break;
                    }
                    
                    // Read column name length
                    int colNameLength = buffer.getShort(pos) & 0xFFFF;
                    pos += 2;
                    
                    // Validate column name length
                    if (colNameLength < 0 || colNameLength > 1000 || pos + (colNameLength * 2) > buffer.limit()) {
                        break;
                    }
                    
                    // Read column name
                    StringBuilder name = new StringBuilder(colNameLength);
                    for (int j = 0; j < colNameLength; j++) {
                        if (pos + 1 >= buffer.limit()) {
                            break;
                        }
                        name.append(buffer.getChar(pos));
                        pos += 2;
                    }
                    
                    // Check if we have enough buffer left for datatype
                    if (pos >= buffer.limit()) {
                        break;
                    }
                    
                    // Read data type
                    int dataTypeOrdinal = buffer.get(pos) & 0xFF;
                    pos++;
                    
                    // Validate data type
                    DataType dataType;
                    try {
                        dataType = DataType.values()[dataTypeOrdinal];
                    } catch (ArrayIndexOutOfBoundsException e) {
                        // Invalid data type, use INTEGER as fallback
                        dataType = DataType.INTEGER;
                    }
                    
                    // Check if we have enough buffer left for max length
                    if (pos + 1 >= buffer.limit()) {
                        break;
                    }
                    
                    // Read max length
                    int maxLength = buffer.getShort(pos) & 0xFFFF;
                    pos += 2;
                    
                    // Check if we have enough buffer left for nullable flag
                    if (pos >= buffer.limit()) {
                        break;
                    }
                    
                    // Read nullable flag
                    boolean nullable = buffer.get(pos) == 1;
                    pos++;
                    
                    columns.add(new ColumnDefinition(name.toString(), dataType, maxLength, nullable));
                } catch (Exception e) {
                    // Skip this column and try the next one
                    continue;
                }
            }
        } catch (Exception e) {
            // Return whatever columns we were able to read
        }
        
        return columns;
    }
    
    /**
     * Represents a column definition in a table header.
     */
    public static class ColumnDefinition {
        private final String name;
        private final DataType dataType;
        private final int maxLength;
        private final boolean nullable;
        
        public ColumnDefinition(String name, DataType dataType, int maxLength, boolean nullable) {
            this.name = name;
            this.dataType = dataType;
            this.maxLength = maxLength;
            this.nullable = nullable;
        }
        
        public String getName() {
            return name;
        }
        
        public DataType getDataType() {
            return dataType;
        }
        
        public int getMaxLength() {
            return maxLength;
        }
        
        public boolean isNullable() {
            return nullable;
        }
    }
} 