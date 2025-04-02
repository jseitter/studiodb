package net.seitter.studiodb.btree;

/**
 * Represents the types of keys that can be stored in a B-Tree index.
 */
public enum KeyType {
    INTEGER,
    FLOAT,
    VARCHAR;
    
    /**
     * Gets the size in bytes of a key of this type.
     *
     * @return The size in bytes
     */
    public int getSize() {
        switch (this) {
            case INTEGER:
                return 4; // int is 4 bytes
            case FLOAT:
                return 8; // double is 8 bytes
            case VARCHAR:
                return -1; // variable size, need to know the string length
            default:
                return 0;
        }
    }
    
    /**
     * Converts a schema DataType to the corresponding KeyType.
     *
     * @param dataType The schema DataType
     * @return The corresponding KeyType
     */
    public static KeyType fromDataType(net.seitter.studiodb.schema.DataType dataType) {
        switch (dataType) {
            case INTEGER:
                return INTEGER;
            case FLOAT:
                return FLOAT;
            case VARCHAR:
                return VARCHAR;
            default:
                throw new IllegalArgumentException("Unsupported data type for index key: " + dataType);
        }
    }
} 