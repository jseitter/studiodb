package net.seitter.studiodb.btree;

/**
 * Represents a key entry in a B-Tree node.
 */
public class KeyEntry {
    private final Object value;
    
    /**
     * Creates a new key entry with the specified value.
     *
     * @param value The key value
     */
    public KeyEntry(Object value) {
        this.value = value;
    }
    
    /**
     * Gets the value of the key.
     *
     * @return The key value
     */
    public Object getValue() {
        return value;
    }
    
    /**
     * Gets the size of the key in bytes.
     *
     * @return The size in bytes
     */
    public int getSize() {
        if (value instanceof Integer) {
            return 4;
        } else if (value instanceof Double || value instanceof Float) {
            return 8;
        } else if (value instanceof String) {
            return 4 + ((String) value).length() * 2; // 4 bytes for length, 2 bytes per char
        } else {
            return 0;
        }
    }
    
    /**
     * Gets the type of the key.
     *
     * @return The key type
     */
    public KeyType getType() {
        if (value instanceof Integer) {
            return KeyType.INTEGER;
        } else if (value instanceof Double || value instanceof Float) {
            return KeyType.FLOAT;
        } else if (value instanceof String) {
            return KeyType.VARCHAR;
        } else {
            throw new IllegalStateException("Unsupported key value type: " + value.getClass());
        }
    }
    
    @Override
    public String toString() {
        return value.toString();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        
        KeyEntry other = (KeyEntry) obj;
        return value.equals(other.value);
    }
    
    @Override
    public int hashCode() {
        return value.hashCode();
    }
} 