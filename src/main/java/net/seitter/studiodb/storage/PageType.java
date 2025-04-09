package net.seitter.studiodb.storage;

/**
 * Enum representing different types of pages in the database system.
 * Each page type has a unique identifier.
 */
public enum PageType {
    UNUSED(0),
    TABLE_HEADER(1),
    TABLE_DATA(2),
    INDEX_HEADER(3),
    INDEX_INTERNAL(4),
    INDEX_LEAF(5),
    FREE_SPACE_MAP(7),
    TRANSACTION_LOG(8),
    CONTAINER_METADATA(9);

    private final int typeId;

    PageType(int typeId) {
        this.typeId = typeId;
    }

    public int getTypeId() {
        return typeId;
    }

    public static PageType fromTypeId(int typeId) {
        for (PageType type : values()) {
            if (type.typeId == typeId) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown page type ID: " + typeId);
    }
} 