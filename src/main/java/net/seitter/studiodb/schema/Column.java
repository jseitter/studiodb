package net.seitter.studiodb.schema;

/**
 * Represents a column in a database table.
 */
public class Column {
    private final String name;
    private final DataType dataType;
    private final boolean nullable;
    private final int maxLength;
    
    /**
     * Creates a new column with the specified parameters.
     *
     * @param name The name of the column
     * @param dataType The data type of the column
     * @param nullable Whether the column can contain null values
     * @param maxLength The maximum length for string data types (ignored for others)
     */
    public Column(String name, DataType dataType, boolean nullable, int maxLength) {
        this.name = name;
        this.dataType = dataType;
        this.nullable = nullable;
        this.maxLength = dataType == DataType.VARCHAR ? maxLength : 0;
    }
    
    /**
     * Creates a new column with the specified parameters and default maxLength.
     *
     * @param name The name of the column
     * @param dataType The data type of the column
     * @param nullable Whether the column can contain null values
     */
    public Column(String name, DataType dataType, boolean nullable) {
        this(name, dataType, nullable, dataType == DataType.VARCHAR ? 255 : 0);
    }
    
    /**
     * Gets the name of the column.
     *
     * @return The column name
     */
    public String getName() {
        return name;
    }
    
    /**
     * Gets the data type of the column.
     *
     * @return The data type
     */
    public DataType getDataType() {
        return dataType;
    }
    
    /**
     * Checks if the column can contain null values.
     *
     * @return true if nullable, false otherwise
     */
    public boolean isNullable() {
        return nullable;
    }
    
    /**
     * Gets the maximum length for string data types.
     *
     * @return The maximum length, or 0 for non-string types
     */
    public int getMaxLength() {
        return maxLength;
    }
    
    /**
     * Gets the size in bytes that this column's data will occupy in storage.
     *
     * @return The size in bytes
     */
    public int getSizeInBytes() {
        switch (dataType) {
            case INTEGER:
                return 4;
            case FLOAT:
                return 8;
            case BOOLEAN:
                return 1;
            case VARCHAR:
                return maxLength + 2; // 2 bytes for length prefix
            case DATE:
                return 8; // Store as milliseconds since epoch
            default:
                return 0;
        }
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(name).append(" ").append(dataType);
        
        if (dataType == DataType.VARCHAR) {
            sb.append("(").append(maxLength).append(")");
        }
        
        if (!nullable) {
            sb.append(" NOT NULL");
        }
        
        return sb.toString();
    }
} 