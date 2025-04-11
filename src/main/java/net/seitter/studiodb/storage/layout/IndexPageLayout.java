package net.seitter.studiodb.storage.layout;

import net.seitter.studiodb.schema.DataType;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import net.seitter.studiodb.storage.Page;
import net.seitter.studiodb.storage.PageLayout;
import net.seitter.studiodb.storage.PageType;

/**
 * Layout class for B-tree index pages.
 * Handles the specific structure and operations for index pages.
 */
public class IndexPageLayout extends PageLayout {
    private static final int IS_LEAF_OFFSET = 5; // After page type and magic
    private static final int KEY_COUNT_OFFSET = 6; // After is leaf flag
    private static final int KEY_TYPE_OFFSET = 8; // After key count
    private static final int KEYS_START_OFFSET = 9; // After key type
    
    public IndexPageLayout(Page page) {
        super(page);
    }
    
    @Override
    public void initialize() {
        // Default initialization for an index page
        initialize(PageType.INDEX_HEADER, DataType.INTEGER);
    }
    
    /**
     * Initializes a new index page.
     * 
     * @param pageType The type of index page (header, internal, or leaf)
     * @param keyType The type of keys stored in this page
     */
    public void initialize(PageType pageType, DataType keyType) {
        writeHeader(pageType);
        
        // Set is leaf flag based on page type
        buffer.put(IS_LEAF_OFFSET, (byte) (pageType == PageType.INDEX_LEAF ? 1 : 0));
        
        // Initialize key count to 0
        buffer.putShort(KEY_COUNT_OFFSET, (short) 0);
        
        // Set key type
        buffer.put(KEY_TYPE_OFFSET, (byte) keyType.ordinal());
        
        page.markDirty();
    }
    
    /**
     * Checks if this is a leaf page.
     * 
     * @return true if this is a leaf page
     */
    public boolean isLeaf() {
        return buffer.get(IS_LEAF_OFFSET) == 1;
    }
    
    /**
     * Gets the number of keys in this page.
     * 
     * @return The number of keys
     */
    public int getKeyCount() {
        return buffer.getShort(KEY_COUNT_OFFSET) & 0xFFFF;
    }
    
    /**
     * Gets the type of keys stored in this page.
     * 
     * @return The key type
     */
    public DataType getKeyType() {
        return DataType.values()[buffer.get(KEY_TYPE_OFFSET) & 0xFF];
    }
    
    /**
     * Adds a key to the page.
     * 
     * @param key The key value
     * @param childPageId The child page ID (for internal nodes) or record page ID (for leaf nodes)
     * @param recordSlot The record slot (for leaf nodes only)
     * @return true if the key was added successfully
     */
    public boolean addKey(Object key, int childPageId, int recordSlot) {
        int keyCount = getKeyCount();
        DataType keyType = getKeyType();
        
        // Calculate required space
        int keySize = getKeySize(key, keyType);
        int entrySize = keySize + (isLeaf() ? 8 : 4); // 8 bytes for leaf (page ID + slot), 4 for internal
        
        // Check if we have enough space
        if (getFreeSpace() < entrySize) {
            return false;
        }
        
        // Write key
        int keyOffset = KEYS_START_OFFSET + (keyCount * entrySize);
        writeKey(keyOffset, key, keyType);
        
        // Write child/record pointer
        if (isLeaf()) {
            buffer.putInt(keyOffset + keySize, childPageId);
            buffer.putInt(keyOffset + keySize + 4, recordSlot);
        } else {
            buffer.putInt(keyOffset + keySize, childPageId);
        }
        
        // Update key count
        buffer.putShort(KEY_COUNT_OFFSET, (short) (keyCount + 1));
        
        page.markDirty();
        return true;
    }
    
    /**
     * Gets a key at the specified index.
     * 
     * @param index The key index
     * @return The key value, or null if the index is invalid
     */
    public Object getKey(int index) {
        if (index < 0 || index >= getKeyCount()) {
            return null;
        }
        
        DataType keyType = getKeyType();
        int keySize = getKeySize(null, keyType); // Get size for this type
        int entrySize = keySize + (isLeaf() ? 8 : 4);
        
        int keyOffset = KEYS_START_OFFSET + (index * entrySize);
        return readKey(keyOffset, keyType);
    }
    
    /**
     * Gets a child page ID at the specified index.
     * For internal nodes, this is the page ID of a child node.
     * For leaf nodes, this is the page ID of a record.
     * 
     * @param index The key index
     * @return The child page ID, or -1 if the index is invalid
     */
    public int getChildPageId(int index) {
        if (index < 0 || index >= getKeyCount()) {
            return -1;
        }
        
        DataType keyType = getKeyType();
        int keySize = getKeySize(null, keyType);
        int entrySize = keySize + (isLeaf() ? 8 : 4);
        
        int offset = KEYS_START_OFFSET + (index * entrySize) + keySize;
        return buffer.getInt(offset);
    }
    
    /**
     * Gets a record slot at the specified index.
     * Only valid for leaf nodes.
     * 
     * @param index The key index
     * @return The record slot, or -1 if the index is invalid or this is not a leaf node
     */
    public int getRecordSlot(int index) {
        if (!isLeaf() || index < 0 || index >= getKeyCount()) {
            return -1;
        }
        
        DataType keyType = getKeyType();
        int keySize = getKeySize(null, keyType);
        int entrySize = keySize + 8;
        
        int offset = KEYS_START_OFFSET + (index * entrySize) + keySize + 4;
        return buffer.getInt(offset);
    }
    
    /**
     * Gets the size of a key in bytes.
     * 
     * @param key The key value (can be null)
     * @param keyType The type of the key
     * @return The size in bytes
     */
    private int getKeySize(Object key, DataType keyType) {
        switch (keyType) {
            case INTEGER:
                return 4;
            case FLOAT:
                return 8;
            case VARCHAR:
                if (key == null) {
                    return 4; // Just the length field
                }
                return 4 + (((String) key).length() * 2);
            default:
                throw new IllegalArgumentException("Unsupported key type: " + keyType);
        }
    }
    
    /**
     * Writes a key to the buffer at the specified offset.
     * 
     * @param offset The offset to write at
     * @param key The key value
     * @param keyType The type of the key
     */
    private void writeKey(int offset, Object key, DataType keyType) {
        switch (keyType) {
            case INTEGER:
                buffer.putInt(offset, (Integer) key);
                break;
            case FLOAT:
                buffer.putDouble(offset, (Double) key);
                break;
            case VARCHAR:
                String strValue = (String) key;
                buffer.putInt(offset, strValue.length());
                offset += 4;
                for (int i = 0; i < strValue.length(); i++) {
                    buffer.putChar(offset, strValue.charAt(i));
                    offset += 2;
                }
                break;
            default:
                throw new IllegalArgumentException("Unsupported key type: " + keyType);
        }
    }
    
    /**
     * Reads a key from the buffer at the specified offset.
     * 
     * @param offset The offset to read from
     * @param keyType The type of the key
     * @return The key value
     */
    private Object readKey(int offset, DataType keyType) {
        switch (keyType) {
            case INTEGER:
                return buffer.getInt(offset);
            case FLOAT:
                return buffer.getDouble(offset);
            case VARCHAR:
                int length = buffer.getInt(offset);
                offset += 4;
                StringBuilder sb = new StringBuilder(length);
                for (int i = 0; i < length; i++) {
                    sb.append(buffer.getChar(offset));
                    offset += 2;
                }
                return sb.toString();
            default:
                throw new IllegalArgumentException("Unsupported key type: " + keyType);
        }
    }
} 