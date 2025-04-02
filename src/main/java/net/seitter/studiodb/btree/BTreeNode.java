package net.seitter.studiodb.btree;

import net.seitter.studiodb.storage.Page;
import net.seitter.studiodb.storage.PageId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents a node in a B-Tree index.
 * A node can be either an internal node (with keys and child pointers) or a leaf node (with keys and record pointers).
 */
public class BTreeNode {
    private static final Logger logger = LoggerFactory.getLogger(BTreeNode.class);
    
    private static final int MAGIC_NUMBER = 0xDADA0301;
    
    private final Page page;
    private final boolean isLeaf;
    private final List<KeyEntry> keys;
    private final List<Integer> children;
    private final KeyType keyType;
    
    /**
     * Loads a B-Tree node from a page.
     *
     * @param page The page containing the node data
     */
    public BTreeNode(Page page) {
        this.page = page;
        this.keys = new ArrayList<>();
        this.children = new ArrayList<>();
        
        ByteBuffer buffer = page.getBuffer();
        
        // Read header
        int magic = buffer.getInt(0);
        if (magic != MAGIC_NUMBER) {
            throw new IllegalArgumentException("Invalid B-Tree page: wrong magic number");
        }
        
        this.isLeaf = buffer.get(4) == 1;
        int numKeys = buffer.getInt(5);
        int keyTypeOrdinal = buffer.getInt(9);
        this.keyType = KeyType.values()[keyTypeOrdinal];
        
        // Read keys and children
        int position = 13; // After header
        
        for (int i = 0; i < numKeys; i++) {
            KeyEntry key = readKeyEntry(buffer, position, keyType);
            keys.add(key);
            position += key.getSize();
        }
        
        if (!isLeaf) {
            // Read child pointers (one more than keys for internal nodes)
            for (int i = 0; i <= numKeys; i++) {
                int childPageId = buffer.getInt(position);
                children.add(childPageId);
                position += 4;
            }
        } else {
            // For leaf nodes, each key has a corresponding record pointer
            for (int i = 0; i < numKeys; i++) {
                int recordPageId = buffer.getInt(position);
                children.add(recordPageId);
                position += 4;
                
                // Also read record slot
                int recordSlot = buffer.getInt(position);
                // Store the slot with the child pointer (we could use a separate list, 
                // but for simplicity we just store pairs of pageId,slot)
                children.add(recordSlot);
                position += 4;
            }
        }
    }
    
    /**
     * Reads a key entry from the buffer at the specified position.
     *
     * @param buffer The buffer to read from
     * @param position The position to start reading
     * @param keyType The type of the key
     * @return The key entry
     */
    private KeyEntry readKeyEntry(ByteBuffer buffer, int position, KeyType keyType) {
        switch (keyType) {
            case INTEGER:
                int intValue = buffer.getInt(position);
                return new KeyEntry(intValue);
            case FLOAT:
                double doubleValue = buffer.getDouble(position);
                return new KeyEntry(doubleValue);
            case VARCHAR:
                int length = buffer.getInt(position);
                StringBuilder sb = new StringBuilder(length);
                for (int i = 0; i < length; i++) {
                    sb.append(buffer.getChar(position + 4 + i * 2));
                }
                return new KeyEntry(sb.toString());
            default:
                throw new IllegalArgumentException("Unsupported key type: " + keyType);
        }
    }
    
    /**
     * Writes the node data back to the page.
     */
    public void write() {
        ByteBuffer buffer = page.getBuffer();
        buffer.clear();
        
        // Write header
        buffer.putInt(MAGIC_NUMBER);
        buffer.put(isLeaf ? (byte) 1 : (byte) 0);
        buffer.putInt(keys.size());
        buffer.putInt(keyType.ordinal());
        
        // Write keys
        for (KeyEntry key : keys) {
            writeKeyEntry(buffer, key);
        }
        
        // Write children
        if (!isLeaf) {
            // Internal nodes have one more child than keys
            for (int i = 0; i < children.size(); i++) {
                buffer.putInt(children.get(i));
            }
        } else {
            // Leaf nodes have record pointers (page ID and slot number)
            for (int i = 0; i < children.size(); i += 2) {
                buffer.putInt(children.get(i)); // Page ID
                buffer.putInt(children.get(i + 1)); // Slot
            }
        }
        
        page.markDirty();
    }
    
    /**
     * Writes a key entry to the buffer.
     *
     * @param buffer The buffer to write to
     * @param key The key entry
     */
    private void writeKeyEntry(ByteBuffer buffer, KeyEntry key) {
        switch (keyType) {
            case INTEGER:
                buffer.putInt((Integer) key.getValue());
                break;
            case FLOAT:
                buffer.putDouble((Double) key.getValue());
                break;
            case VARCHAR:
                String strValue = (String) key.getValue();
                buffer.putInt(strValue.length());
                for (int i = 0; i < strValue.length(); i++) {
                    buffer.putChar(strValue.charAt(i));
                }
                break;
            default:
                throw new IllegalArgumentException("Unsupported key type: " + keyType);
        }
    }
    
    /**
     * Gets the page containing this node.
     *
     * @return The page
     */
    public Page getPage() {
        return page;
    }
    
    /**
     * Gets the page ID of this node.
     *
     * @return The page ID
     */
    public PageId getPageId() {
        return page.getPageId();
    }
    
    /**
     * Checks if this is a leaf node.
     *
     * @return true if this is a leaf node
     */
    public boolean isLeaf() {
        return isLeaf;
    }
    
    /**
     * Gets the keys in this node.
     *
     * @return The list of keys
     */
    public List<KeyEntry> getKeys() {
        return keys;
    }
    
    /**
     * Gets the child pointers in this node.
     * For internal nodes, these are page IDs of child nodes.
     * For leaf nodes, these are pairs of (page ID, slot) for records.
     *
     * @return The list of child pointers
     */
    public List<Integer> getChildren() {
        return children;
    }
    
    /**
     * Gets the key type of this node.
     *
     * @return The key type
     */
    public KeyType getKeyType() {
        return keyType;
    }
    
    /**
     * Finds the index where a key should be inserted to maintain order.
     *
     * @param key The key to insert
     * @return The index where the key should be inserted
     */
    public int findInsertionPoint(Comparable key) {
        int low = 0;
        int high = keys.size() - 1;
        
        while (low <= high) {
            int mid = (low + high) / 2;
            Comparable midKey = (Comparable) keys.get(mid).getValue();
            int cmp = key.compareTo(midKey);
            
            if (cmp == 0) {
                return mid; // Exact match
            } else if (cmp < 0) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        
        return low; // Insert at this position
    }
    
    /**
     * Finds the child index for a key in an internal node.
     *
     * @param key The key to search for
     * @return The index of the child to follow
     */
    public int findChildIndex(Comparable key) {
        if (isLeaf) {
            throw new IllegalStateException("Cannot find child index in a leaf node");
        }
        
        int insertionPoint = findInsertionPoint(key);
        
        // If the key would be inserted at position i, we need to follow child i
        return insertionPoint;
    }
    
    /**
     * Inserts a key and associated child pointer at the specified index.
     *
     * @param index The index to insert at
     * @param key The key to insert
     * @param childPageId The child page ID (for internal nodes) or record page ID (for leaf nodes)
     * @param recordSlot The record slot (for leaf nodes, ignored for internal nodes)
     */
    public void insertKey(int index, Comparable key, int childPageId, int recordSlot) {
        KeyEntry keyEntry = new KeyEntry(key);
        keys.add(index, keyEntry);
        
        if (isLeaf) {
            // For leaf nodes, add the record pointer (page ID and slot)
            children.add(index * 2, childPageId);
            children.add(index * 2 + 1, recordSlot);
        } else {
            // For internal nodes, add the right child pointer
            // (the left child pointer stays the same)
            children.add(index + 1, childPageId);
        }
        
        write();
    }
    
    /**
     * Initializes a new empty leaf node.
     *
     * @param page The page to use
     * @param keyType The key type
     * @return The new leaf node
     */
    public static BTreeNode createLeafNode(Page page, KeyType keyType) {
        ByteBuffer buffer = page.getBuffer();
        buffer.clear();
        
        // Write header
        buffer.putInt(MAGIC_NUMBER);
        buffer.put((byte) 1); // IsLeaf
        buffer.putInt(0); // NumKeys
        buffer.putInt(keyType.ordinal());
        
        page.markDirty();
        
        return new BTreeNode(page);
    }
    
    /**
     * Initializes a new empty internal node.
     *
     * @param page The page to use
     * @param keyType The key type
     * @param leftChildPageId The page ID of the left child
     * @return The new internal node
     */
    public static BTreeNode createInternalNode(Page page, KeyType keyType, int leftChildPageId) {
        ByteBuffer buffer = page.getBuffer();
        buffer.clear();
        
        // Write header
        buffer.putInt(MAGIC_NUMBER);
        buffer.put((byte) 0); // IsLeaf
        buffer.putInt(0); // NumKeys
        buffer.putInt(keyType.ordinal());
        
        // Write the initial left child pointer
        buffer.putInt(leftChildPageId);
        
        page.markDirty();
        
        BTreeNode node = new BTreeNode(page);
        node.children.add(leftChildPageId);
        return node;
    }
} 