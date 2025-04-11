# StudioDB Storage Subsystem Specification

## Table of Contents
1. [Introduction](#introduction)
2. [Key Components](#key-components)
3. [Page Structure](#page-structure)
4. [Page Layout Hierarchy](#page-layout-hierarchy)
5. [Page Types](#page-types)
6. [Tablespace Container Structure](#tablespace-container-structure)
7. [Buffer Pool Management](#buffer-pool-management)
8. [Row Storage Format](#row-storage-format)
9. [Free Space Management](#free-space-management)
10. [Diagnostic Commands](#diagnostic-commands)

## Introduction

The storage subsystem is the foundation of StudioDB, providing persistent storage for database objects. It is responsible for:

- Organizing data into pages
- Managing page allocation and deallocation
- Caching frequently accessed pages in memory
- Providing an interface for higher-level components to read and write data

The storage layer is designed with these key principles:
- Page-oriented storage
- Multiple tablespaces for logical organization
- Efficient buffer management to minimize I/O
- Separation of data organization (layouts) from physical storage (pages)

## Key Components

### StorageManager
Central component that orchestrates storage operations across all tablespaces. It maintains references to all tablespaces and provides a unified interface for page operations.

### Tablespace
A logical container for database objects. Each tablespace maps to a physical storage container file and has its own buffer pool.

### StorageContainer
Represents the physical storage container file for a tablespace. Handles raw read/write operations at the file level.

### Page
The fundamental unit of data storage in the system. Each page has a fixed size (typically 4KB or 8KB) and contains a header and data section.

### PageId
Uniquely identifies a page within the database, consisting of a tablespace name and a page number.

### PageLayout
Defines the structure of data within a page. Different types of pages (table data, index, metadata) use different layout implementations.

### BufferPoolManager
Manages the buffer pool for a tablespace, implementing page replacement policies and coordinating I/O with the storage container.

## Page Structure

Each page in StudioDB has the following general structure:

```
+------------------+
| Page Header      |
+------------------+
| Page-specific    |
| data organization|
+------------------+
```

### Page Header (32 bytes)
- **Page Type** (1 byte): Identifies the type of page (Table Data, Index, etc.)
- **Magic Number** (4 bytes): A constant value (0xDADADADA) to verify page integrity
- **Next Page ID** (4 bytes): Page number of the next page in a chain, or -1
- **Previous Page ID** (4 bytes): Page number of the previous page in a chain, or -1
- **Free Space Offset** (4 bytes): Offset to the start of free space
- **Reserved** (15 bytes): Reserved for future use, ensuring the header is 32 bytes total

Each page type may use the page header fields slightly differently and extend with additional metadata after the header.

## Page Layout Hierarchy

The page layout system is organized as a class hierarchy:

```
                      PageLayout (abstract)
                            |
       +----------+----------+----------+----------+
       |          |          |          |          |
TableHeader  TableData   IndexPage  Container  FreeSpaceMap
  Layout      Layout      Layout    Metadata    PageLayout
                                    PageLayout
```

### PageLayout (Abstract Base Class)
- Defines common header operations
- Provides methods for reading/writing metadata
- Manages page initialization

### TableHeaderPageLayout
- Stores metadata about a table
- Contains column definitions
- Tracks table properties

### TableDataPageLayout
- Stores actual table data (rows)
- Implements a slotted page design with a row directory
- Manages free space and variable-length records

### IndexPageLayout
- Implements B-tree index nodes
- Variants for leaf and internal nodes
- Stores keys and references

### ContainerMetadataPageLayout
- Stores metadata about the tablespace container
- Contains tablespace name, page size, and creation timestamp
- Keeps track of total pages and free space map location

### FreeSpaceMapPageLayout
- Implements bitmap-based free space tracking
- Manages page allocation and deallocation
- Provides methods to find free pages efficiently

## Page Types

Page types are defined in the `PageType` enum:

| Type | ID | Description |
|------|------|-------------|
| UNUSED | 0 | Unused/uninitialized page |
| TABLE_HEADER | 1 | Contains table metadata and schema |
| TABLE_DATA | 2 | Contains table row data |
| INDEX_HEADER | 3 | Contains index metadata |
| INDEX_INTERNAL | 4 | B-tree internal node |
| INDEX_LEAF | 5 | B-tree leaf node |
| FREE_SPACE_MAP | 7 | Tracks free pages in a tablespace |
| TRANSACTION_LOG | 8 | Transaction log entries |
| CONTAINER_METADATA | 9 | Tablespace container metadata |

## Tablespace Container Structure

A tablespace container is a physical file on disk that stores pages. The file has a specific structure:

```
+---------------------+
| Container Metadata  | Page 0
+---------------------+
| Free Space Map      | Page 1
+---------------------+
| User Data           | Page 2+
| ...                 |
+---------------------+
```

### Container Metadata Page (Page 0)
- **Common Page Header**: Including type (CONTAINER_METADATA) and magic number
- **Page Size** (4 bytes): Size of pages in the container
- **Creation Time** (8 bytes): Timestamp when the container was created
- **Last Opened Time** (8 bytes): Timestamp when the container was last opened
- **Total Pages** (4 bytes): Total number of pages in the container
- **Free Space Map Page ID** (4 bytes): Page ID of the free space map (typically 1)
- **Tablespace Name Length** (2 bytes): Length of the tablespace name
- **Tablespace Name** (variable): The name of the tablespace (as UTF-16 characters)

### Free Space Map Page (Page 1)
- **Common Page Header**: Including type (FREE_SPACE_MAP) and magic number
- **Last Checked Page** (4 bytes): Last page number checked for allocation
- **Bitmap Capacity** (4 bytes): Number of pages that can be tracked by this bitmap
- **Bitmap** (variable): A bitmap where each bit represents a page (1 = free, 0 = used)

The free space map uses a bitmap to track page allocation status. Each bit represents one page, with a set bit (1) indicating the page is free and a cleared bit (0) indicating the page is in use. The bitmap is sized to accommodate the maximum number of pages that can fit in the tablespace container.

## Buffer Pool Management

The buffer pool caches pages in memory to reduce disk I/O. The BufferPoolManager:

1. **Page Table**: Maps PageIds to Page instances in memory
2. **Replacement Policy**: Uses a FIFO queue to determine which pages to evict
3. **Page Cleaner**: Background thread that periodically flushes dirty pages

### Key Operations:

- **Fetch Page**: Loads a page into the buffer pool, evicting if necessary
- **Unpin Page**: Marks a page as no longer in use
- **Flush Page**: Writes a dirty page to disk
- **Allocate Page**: Allocates a new page in the tablespace

### Pinning Mechanism
Pages are "pinned" while in use to prevent them from being evicted. A page's pin count indicates how many operations are currently using it.

## Row Storage Format

Table data pages use a slotted page structure:

```
+----------------------+
| Page Header          |
+----------------------+
| Row Count            | (4 bytes)
+----------------------+
| Row Directory        | (grows downward)
| (offset, length)     |
| pairs                |
+----------------------+
|                      |
| Free Space           |
|                      |
+----------------------+
| Row Data             | (grows upward)
+----------------------+
```

### Row Directory
Each entry in the row directory is 8 bytes:
- **Offset** (4 bytes): Offset of the row data from the start of the page
- **Length** (4 bytes): Length of the row data in bytes

### Row Data
Actual row data is stored from the end of the page, growing toward the beginning. This allows for efficient variable-length records.

## Free Space Management

Free space is managed at two levels:

### Page-Level Free Space
- Each TableDataPageLayout tracks free space within the page
- The free space offset indicates where free space begins
- Row data is added from the end of the page toward the beginning
- The row directory grows from the beginning of the page toward the end
- When the row directory and row data would overlap, the page is full

### Tablespace-Level Free Space
- The Free Space Map page (Page 1) tracks which pages are free in the tablespace
- A bitmap represents all pages in the tablespace (1 bit per page)
- When a page is allocated, its bit is set to 0 (used)
- When a page is deallocated, its bit is set to 1 (free)
- The allocation algorithm scans the bitmap to find free pages
- The LastCheckedPage field is used to implement a round-robin allocation policy

## Diagnostic Commands

StudioDB provides several diagnostic commands to inspect the storage layer:

### SHOW PAGES Command

The `SHOW PAGES IN TABLESPACE tablespace_name` command displays detailed information about all pages in a tablespace. For each page, it shows:

- **PAGE_ID**: The page number
- **TYPE**: The page type (TABLE_HEADER, TABLE_DATA, etc.)
- **NEXT_PAGE**: The next page in the chain (-1 if none)
- **PREV_PAGE**: The previous page in the chain (-1 if none)
- **FREE_SPACE**: Free space available in the page
- **ADDITIONAL_INFO**: Page-specific information

The additional information varies by page type:

- **ContainerMetadataPageLayout**: Shows tablespace name, page size, total pages, free space map page ID, and creation date
- **FreeSpaceMapPageLayout**: Shows capacity (total pages tracked), number of free pages, percentage available, and last checked page
- **TableHeaderPageLayout**: Shows table name and first data page ID
- **TableDataPageLayout**: Shows record count, free space, and sample of row sizes
- **IndexPageLayout**: Shows whether the page is a leaf or internal node, key count, and key type 