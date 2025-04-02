package net.seitter.studiodb.btree;

import net.seitter.studiodb.buffer.BufferPoolManager;
import net.seitter.studiodb.storage.Page;
import net.seitter.studiodb.storage.PageId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents a B-Tree index structure.
 */
public class BTree {
    private static final Logger logger = LoggerFactory.getLogger(BTree.class);
    
    private static final int MAX_KEYS = 10; // Maximum number of keys per node
    
    private final BufferPoolManager bufferPool;
    private final int rootPageId;
    private final String tablespaceName;
    private final KeyType keyType;
    private final boolean unique;
    
    /**
     * Creates a B-Tree with an existing root page.
     *
     * @param bufferPool The buffer pool manager
     * @param rootPageId The page ID of the root node
     * @param tablespaceName The name of the tablespace
     * @param keyType The type of keys in the tree
     * @param unique Whether the tree enforces unique keys
     */
    public BTree(BufferPoolManager bufferPool, int rootPageId, String tablespaceName, 
                 KeyType keyType, boolean unique) {
        this.bufferPool = bufferPool;
        this.rootPageId = rootPageId;
        this.tablespaceName = tablespaceName;
        this.keyType = keyType;
        this.unique = unique;
    }
    
    /**
     * Creates a new B-Tree.
     *
     * @param bufferPool The buffer pool manager
     * @param tablespaceName The name of the tablespace
     * @param keyType The type of keys in the tree
     * @param unique Whether the tree enforces unique keys
     * @return The new B-Tree
     * @throws IOException If there's an error allocating pages
     */
    public static BTree createBTree(BufferPoolManager bufferPool, String tablespaceName, 
                                   KeyType keyType, boolean unique) throws IOException {
        // Allocate a page for the root node
        Page rootPage = bufferPool.allocatePage();
        
        // Initialize the root as a leaf node
        BTreeNode.createLeafNode(rootPage, keyType);
        
        // Create the B-Tree
        BTree tree = new BTree(bufferPool, rootPage.getPageId().getPageNumber(), 
                              tablespaceName, keyType, unique);
        
        // Unpin the page
        bufferPool.unpinPage(rootPage.getPageId(), true);
        
        logger.info("Created new B-Tree with root page {}", rootPage.getPageId());
        return tree;
    }
    
    /**
     * Gets the root page ID of the tree.
     *
     * @return The root page ID
     */
    public int getRootPageId() {
        return rootPageId;
    }
    
    /**
     * Gets the key type of the tree.
     *
     * @return The key type
     */
    public KeyType getKeyType() {
        return keyType;
    }
    
    /**
     * Inserts a key-value pair into the tree.
     *
     * @param key The key to insert
     * @param recordPageId The page ID of the record
     * @param recordSlot The slot of the record in the page
     * @return true if the key was inserted, false if it's a duplicate in a unique tree
     * @throws IOException If there's an error accessing pages
     */
    public boolean insert(Comparable key, int recordPageId, int recordSlot) throws IOException {
        // Check for duplicates in a unique tree
        if (unique && find(key) != null) {
            logger.warn("Duplicate key {} in unique index", key);
            return false;
        }
        
        // Get the root node
        PageId rootId = new PageId(tablespaceName, rootPageId);
        Page rootPage = bufferPool.fetchPage(rootId);
        BTreeNode root = new BTreeNode(rootPage);
        
        if (root.getKeys().size() >= MAX_KEYS) {
            // Root is full, need to split it
            splitRoot(root);
            bufferPool.unpinPage(rootId, false); // No changes to the original root
            
            // Get the new root and try again
            rootPage = bufferPool.fetchPage(rootId);
            root = new BTreeNode(rootPage);
        }
        
        // Insert into the tree
        boolean inserted = insertIntoNode(root, key, recordPageId, recordSlot);
        
        // Unpin the root page
        bufferPool.unpinPage(rootId, true);
        
        return inserted;
    }
    
    /**
     * Inserts a key-value pair into a node or its children.
     *
     * @param node The node to insert into
     * @param key The key to insert
     * @param recordPageId The page ID of the record
     * @param recordSlot The slot of the record in the page
     * @return true if the key was inserted
     * @throws IOException If there's an error accessing pages
     */
    private boolean insertIntoNode(BTreeNode node, Comparable key, int recordPageId, int recordSlot) 
            throws IOException {
        if (node.isLeaf()) {
            // Insert directly into the leaf node
            int insertionPoint = node.findInsertionPoint(key);
            
            // Check for duplicates in a unique tree
            if (unique && insertionPoint < node.getKeys().size() && 
                ((Comparable) node.getKeys().get(insertionPoint).getValue()).compareTo(key) == 0) {
                logger.warn("Duplicate key {} in unique index", key);
                return false;
            }
            
            node.insertKey(insertionPoint, key, recordPageId, recordSlot);
            return true;
        } else {
            // Find the child to follow
            int childIndex = node.findChildIndex(key);
            int childPageId = node.getChildren().get(childIndex);
            
            // Get the child node
            PageId childId = new PageId(tablespaceName, childPageId);
            Page childPage = bufferPool.fetchPage(childId);
            BTreeNode child = new BTreeNode(childPage);
            
            if (child.getKeys().size() >= MAX_KEYS) {
                // Child is full, need to split it
                splitChild(node, childIndex, child);
                
                // Get the updated child (the original might have been modified)
                bufferPool.unpinPage(childId, true);
                childPage = bufferPool.fetchPage(childId);
                child = new BTreeNode(childPage);
                
                // Re-find the child to follow
                childIndex = node.findChildIndex(key);
                if (childIndex != node.getChildren().size() - 1 && 
                    ((Comparable) node.getKeys().get(childIndex).getValue()).compareTo(key) <= 0) {
                    // Follow the right child after the split
                    bufferPool.unpinPage(childId, false);
                    childPageId = node.getChildren().get(childIndex + 1);
                    childId = new PageId(tablespaceName, childPageId);
                    childPage = bufferPool.fetchPage(childId);
                    child = new BTreeNode(childPage);
                }
            }
            
            // Recursively insert into the child
            boolean inserted = insertIntoNode(child, key, recordPageId, recordSlot);
            
            // Unpin the child page
            bufferPool.unpinPage(childId, true);
            
            return inserted;
        }
    }
    
    /**
     * Splits a full root node.
     *
     * @param root The root node to split
     * @throws IOException If there's an error allocating pages
     */
    private void splitRoot(BTreeNode root) throws IOException {
        // Allocate a new page for the new root
        Page newRootPage = bufferPool.allocatePage();
        
        // Create a new root as an internal node
        BTreeNode newRoot = BTreeNode.createInternalNode(newRootPage, root.getKeyType(), rootPageId);
        
        // Split the old root
        splitChild(newRoot, 0, root);
        
        // Update the root page ID (this is a bit of a hack since we can't actually change 
        // the rootPageId field, but for educational purposes it's fine)
        // In a real system, we would have a way to update the root page ID in some metadata
        logger.warn("Cannot change root page ID in this implementation: {} -> {}", 
                   rootPageId, newRoot.getPageId().getPageNumber());
        
        // Unpin the new root page
        bufferPool.unpinPage(newRoot.getPageId(), true);
    }
    
    /**
     * Splits a full child node.
     *
     * @param parent The parent node
     * @param childIndex The index of the child in the parent
     * @param child The child node to split
     * @throws IOException If there's an error allocating pages
     */
    private void splitChild(BTreeNode parent, int childIndex, BTreeNode child) throws IOException {
        // Allocate a new page for the new child
        Page newChildPage = bufferPool.allocatePage();
        
        // Create a new child node
        BTreeNode newChild;
        if (child.isLeaf()) {
            newChild = BTreeNode.createLeafNode(newChildPage, child.getKeyType());
        } else {
            newChild = BTreeNode.createInternalNode(newChildPage, child.getKeyType(), -1);
            // We'll fill in the children later
            newChild.getChildren().clear();
        }
        
        // Get the middle key
        int midIndex = MAX_KEYS / 2;
        Comparable midKey = (Comparable) child.getKeys().get(midIndex).getValue();
        
        // Move the right half of the keys and children to the new child
        for (int i = midIndex + 1; i < child.getKeys().size(); i++) {
            KeyEntry key = child.getKeys().get(i);
            
            if (child.isLeaf()) {
                // For leaf nodes, move the record pointers too
                int recordPageId = child.getChildren().get(i * 2);
                int recordSlot = child.getChildren().get(i * 2 + 1);
                newChild.getKeys().add(key);
                newChild.getChildren().add(recordPageId);
                newChild.getChildren().add(recordSlot);
            } else {
                // For internal nodes, move the child pointers too
                int rightChildId = child.getChildren().get(i + 1);
                newChild.getKeys().add(key);
                newChild.getChildren().add(rightChildId);
            }
        }
        
        // If it's an internal node, move the last child
        if (!child.isLeaf() && !child.getChildren().isEmpty()) {
            int lastChildId = child.getChildren().get(child.getChildren().size() - 1);
            newChild.getChildren().add(lastChildId);
        }
        
        // Insert the middle key into the parent
        parent.insertKey(childIndex, midKey, newChild.getPageId().getPageNumber(), 0);
        
        // Truncate the original child
        List<KeyEntry> childKeys = child.getKeys();
        List<Integer> childChildren = child.getChildren();
        
        while (childKeys.size() > midIndex) {
            childKeys.remove(childKeys.size() - 1);
        }
        
        if (child.isLeaf()) {
            // For leaf nodes, remove the record pointers too
            while (childChildren.size() > midIndex * 2) {
                childChildren.remove(childChildren.size() - 1);
            }
        } else {
            // For internal nodes, remove the child pointers too
            while (childChildren.size() > midIndex + 1) {
                childChildren.remove(childChildren.size() - 1);
            }
        }
        
        // Write changes back to the pages
        child.write();
        newChild.write();
        
        // Unpin the new child page
        bufferPool.unpinPage(newChild.getPageId(), true);
    }
    
    /**
     * Finds a key in the tree.
     *
     * @param key The key to find
     * @return A RecordLocation with the page ID and slot of the record, or null if not found
     * @throws IOException If there's an error accessing pages
     */
    public RecordLocation find(Comparable key) throws IOException {
        PageId rootId = new PageId(tablespaceName, rootPageId);
        Page rootPage = bufferPool.fetchPage(rootId);
        BTreeNode root = new BTreeNode(rootPage);
        
        RecordLocation location = findInNode(root, key);
        
        bufferPool.unpinPage(rootId, false);
        
        return location;
    }
    
    /**
     * Finds a key in a node or its children.
     *
     * @param node The node to search in
     * @param key The key to find
     * @return A RecordLocation with the page ID and slot of the record, or null if not found
     * @throws IOException If there's an error accessing pages
     */
    private RecordLocation findInNode(BTreeNode node, Comparable key) throws IOException {
        int insertionPoint = node.findInsertionPoint(key);
        
        if (node.isLeaf()) {
            // Check if the key exists in this leaf
            if (insertionPoint < node.getKeys().size() && 
                ((Comparable) node.getKeys().get(insertionPoint).getValue()).compareTo(key) == 0) {
                // Found the key, return the record location
                int recordPageId = node.getChildren().get(insertionPoint * 2);
                int recordSlot = node.getChildren().get(insertionPoint * 2 + 1);
                return new RecordLocation(recordPageId, recordSlot);
            } else {
                // Key not found
                return null;
            }
        } else {
            // Find the child to follow
            int childIndex = Math.min(insertionPoint, node.getChildren().size() - 1);
            int childPageId = node.getChildren().get(childIndex);
            
            // Get the child node
            PageId childId = new PageId(tablespaceName, childPageId);
            Page childPage = bufferPool.fetchPage(childId);
            BTreeNode child = new BTreeNode(childPage);
            
            // Recursively search in the child
            RecordLocation location = findInNode(child, key);
            
            bufferPool.unpinPage(childId, false);
            
            return location;
        }
    }
    
    /**
     * Finds all keys in a specified range.
     *
     * @param start The start of the range (inclusive), or null for unbounded
     * @param end The end of the range (inclusive), or null for unbounded
     * @return A list of record locations
     * @throws IOException If there's an error accessing pages
     */
    public List<RecordLocation> findRange(Comparable start, Comparable end) throws IOException {
        List<RecordLocation> results = new ArrayList<>();
        
        PageId rootId = new PageId(tablespaceName, rootPageId);
        Page rootPage = bufferPool.fetchPage(rootId);
        BTreeNode root = new BTreeNode(rootPage);
        
        findRangeInNode(root, start, end, results);
        
        bufferPool.unpinPage(rootId, false);
        
        return results;
    }
    
    /**
     * Finds all keys in a specified range in a node or its children.
     *
     * @param node The node to search in
     * @param start The start of the range (inclusive), or null for unbounded
     * @param end The end of the range (inclusive), or null for unbounded
     * @param results The list to add results to
     * @throws IOException If there's an error accessing pages
     */
    private void findRangeInNode(BTreeNode node, Comparable start, Comparable end, 
                               List<RecordLocation> results) throws IOException {
        if (node.isLeaf()) {
            // Find all keys in range in this leaf
            for (int i = 0; i < node.getKeys().size(); i++) {
                Comparable key = (Comparable) node.getKeys().get(i).getValue();
                
                if ((start == null || key.compareTo(start) >= 0) && 
                    (end == null || key.compareTo(end) <= 0)) {
                    // Key is in range, add to results
                    int recordPageId = node.getChildren().get(i * 2);
                    int recordSlot = node.getChildren().get(i * 2 + 1);
                    results.add(new RecordLocation(recordPageId, recordSlot));
                }
            }
        } else {
            // Find the starting child
            int startChildIndex = 0;
            if (start != null) {
                startChildIndex = node.findChildIndex(start);
            }
            
            // Search in each relevant child
            for (int i = startChildIndex; i < node.getChildren().size(); i++) {
                // Check if we've gone past the end
                if (end != null && i > 0 && 
                    ((Comparable) node.getKeys().get(i-1).getValue()).compareTo(end) > 0) {
                    break;
                }
                
                int childPageId = node.getChildren().get(i);
                
                // Get the child node
                PageId childId = new PageId(tablespaceName, childPageId);
                Page childPage = bufferPool.fetchPage(childId);
                BTreeNode child = new BTreeNode(childPage);
                
                // Recursively search in the child
                findRangeInNode(child, start, end, results);
                
                bufferPool.unpinPage(childId, false);
            }
        }
    }
    
    /**
     * Represents a record location in the database.
     */
    public static class RecordLocation {
        private final int pageId;
        private final int slot;
        
        /**
         * Creates a new record location.
         *
         * @param pageId The page ID of the record
         * @param slot The slot of the record in the page
         */
        public RecordLocation(int pageId, int slot) {
            this.pageId = pageId;
            this.slot = slot;
        }
        
        /**
         * Gets the page ID of the record.
         *
         * @return The page ID
         */
        public int getPageId() {
            return pageId;
        }
        
        /**
         * Gets the slot of the record in the page.
         *
         * @return The slot
         */
        public int getSlot() {
            return slot;
        }
        
        @Override
        public String toString() {
            return "RecordLocation{pageId=" + pageId + ", slot=" + slot + '}';
        }
    }
} 