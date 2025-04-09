package net.seitter.studiodb.storage;

import net.seitter.studiodb.schema.DataType;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Layout class for table header pages.
 * Handles the specific structure and operations for table header pages.
 */
public class TableHeaderPageLayout extends PageLayout {
    private static final int TABLE_NAME_OFFSET = 5; // After page type and magic
    private static final int TABLE_NAME_LENGTH_OFFSET = 9; // After first data page ID
    private static final int TABLE_NAME_START_OFFSET = 11; // After name length
    private static final int COLUMN_COUNT_OFFSET = 11; // After table name
    
    public TableHeaderPageLayout(Page page) {
        super(page);
    }
    
    /**
     * Initializes a new table header page.
     */
    public void initialize() {
        writeHeader(PageType.TABLE_HEADER);
    }
    
    /**
     * Sets the first data page ID.
     * 
     * @param firstDataPageId The ID of the first data page
     */
    public void setFirstDataPageId(int firstDataPageId) {
        buffer.putInt(TABLE_NAME_OFFSET, firstDataPageId);
        page.markDirty();
    }
    
    /**
     * Gets the first data page ID.
     * 
     * @return The ID of the first data page
     */
    public int getFirstDataPageId() {
        return buffer.getInt(TABLE_NAME_OFFSET);
    }
    
    /**
     * Sets the table name.
     * 
     * @param tableName The name of the table
     */
    public void setTableName(String tableName) {
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
        if (nameLength == 0 || nameLength > 128) {
            return null;
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
     * Sets the number of columns.
     * 
     * @param columnCount The number of columns
     */
    public void setColumnCount(int columnCount) {
        int nameLength = buffer.getShort(TABLE_NAME_LENGTH_OFFSET) & 0xFFFF;
        int columnCountOffset = TABLE_NAME_START_OFFSET + (nameLength * 2);
        buffer.putShort(columnCountOffset, (short) columnCount);
        page.markDirty();
    }
    
    /**
     * Gets the number of columns.
     * 
     * @return The number of columns
     */
    public int getColumnCount() {
        int nameLength = buffer.getShort(TABLE_NAME_LENGTH_OFFSET) & 0xFFFF;
        int columnCountOffset = TABLE_NAME_START_OFFSET + (nameLength * 2);
        return buffer.getShort(columnCountOffset) & 0xFFFF;
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
        int nameLength = buffer.getShort(TABLE_NAME_LENGTH_OFFSET) & 0xFFFF;
        int columnCount = getColumnCount();
        
        // Calculate offset for new column
        int columnOffset = TABLE_NAME_START_OFFSET + (nameLength * 2) + 2 + (columnCount * 12);
        
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
        int nameLength = buffer.getShort(TABLE_NAME_LENGTH_OFFSET) & 0xFFFF;
        int columnCount = getColumnCount();
        
        int pos = TABLE_NAME_START_OFFSET + (nameLength * 2) + 2;
        
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