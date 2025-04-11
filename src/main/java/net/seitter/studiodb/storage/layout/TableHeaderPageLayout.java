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
        writeHeader(PageType.TABLE_HEADER);
        // Initialize first data page ID to -1 (invalid)
        setFirstDataPageId(-1);
        // Initialize table name to empty string
        setTableName("");
        // Initialize column count to 0
        setColumnCount(0);
    }
    
    /**
     * Sets the first data page ID.
     * 
     * @param firstDataPageId The ID of the first data page
     */
    public void setFirstDataPageId(int firstDataPageId) {
        buffer.putInt(FIRST_DATA_PAGE_ID_OFFSET, firstDataPageId);
        page.markDirty();
    }
    
    /**
     * Gets the first data page ID.
     * 
     * @return The ID of the first data page
     */
    public int getFirstDataPageId() {
        return buffer.getInt(FIRST_DATA_PAGE_ID_OFFSET);
    }
    
    /**
     * Sets the table name.
     * 
     * @param tableName The name of the table
     */
    public void setTableName(String tableName) {
        if (tableName == null) {
            tableName = "";
        }
        
        // Write name length (2 bytes)
        buffer.putShort(TABLE_NAME_LENGTH_OFFSET, (short) tableName.length());
        
        // Write name characters
        int pos = TABLE_NAME_START_OFFSET;
        for (int i = 0; i < tableName.length(); i++) {
            buffer.putChar(pos, tableName.charAt(i));
            pos += 2;
        }
        
        page.markDirty();
    }
    
    /**
     * Gets the table name.
     * 
     * @return The name of the table
     */
    public String getTableName() {
        int nameLength = buffer.getShort(TABLE_NAME_LENGTH_OFFSET) & 0xFFFF;
        if (nameLength == 0) {
            return "";
        }
        
        StringBuilder name = new StringBuilder(nameLength);
        int pos = TABLE_NAME_START_OFFSET;
        for (int i = 0; i < nameLength; i++) {
            name.append(buffer.getChar(pos));
            pos += 2;
        }
        
        return name.toString();
    }
    
    /**
     * Gets the offset where column count is stored.
     * This is calculated based on the table name length.
     * 
     * @return The offset for column count
     */
    private int getColumnCountOffset() {
        int nameLength = buffer.getShort(TABLE_NAME_LENGTH_OFFSET) & 0xFFFF;
        return TABLE_NAME_START_OFFSET + (nameLength * 2);
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
        int offset = getColumnCountOffset();
        return buffer.getShort(offset) & 0xFFFF;
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
        if (name == null) {
            name = "";
        }
        
        int columnCount = getColumnCount();
        
        // Calculate size of existing column definitions
        int columnsSize = 0;
        for (ColumnDefinition col : getColumns()) {
            // 2 bytes for name length + (name.length * 2) for name chars + 1 byte for type + 2 bytes for maxLength + 1 byte for nullable
            columnsSize += 2 + (col.getName().length() * 2) + 4;
        }
        
        // Calculate offset for new column
        int columnOffset = getColumnsStartOffset() + columnsSize;
        
        // Write column name length (2 bytes)
        buffer.putShort(columnOffset, (short) name.length());
        
        // Write column name
        int pos = columnOffset + 2;
        for (int i = 0; i < name.length(); i++) {
            buffer.putChar(pos, name.charAt(i));
            pos += 2;
        }
        
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
        
        page.markDirty();
    }
    
    /**
     * Gets all column definitions from the header.
     * 
     * @return A list of column definitions
     */
    public List<ColumnDefinition> getColumns() {
        List<ColumnDefinition> columns = new ArrayList<>();
        int columnCount = getColumnCount();
        
        int pos = getColumnsStartOffset();
        
        for (int i = 0; i < columnCount; i++) {
            // Read column name length
            int colNameLength = buffer.getShort(pos) & 0xFFFF;
            pos += 2;
            
            // Read column name
            StringBuilder name = new StringBuilder(colNameLength);
            for (int j = 0; j < colNameLength; j++) {
                name.append(buffer.getChar(pos));
                pos += 2;
            }
            
            // Read data type
            DataType dataType = DataType.values()[buffer.get(pos) & 0xFF];
            pos++;
            
            // Read max length
            int maxLength = buffer.getShort(pos) & 0xFFFF;
            pos += 2;
            
            // Read nullable flag
            boolean nullable = buffer.get(pos) == 1;
            pos++;
            
            columns.add(new ColumnDefinition(name.toString(), dataType, maxLength, nullable));
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