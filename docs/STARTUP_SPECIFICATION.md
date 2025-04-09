# StudioDB Startup Process Specification

This document describes the startup process for the StudioDB educational database system, detailing how tablespaces, tables, and the system catalog are initialized and loaded.

## Overview

The StudioDB startup process is designed to:

1. Ensure data persistence between restarts
2. Handle existing tablespaces and database objects properly
3. Initialize the system catalog if needed
4. Load existing database schema from the system catalog

## Detailed Startup Sequence

### 1. Initialization of DatabaseSystem

When a `DatabaseSystem` instance is created:

1. The data directory (`./data`) is checked and created if it doesn't exist
2. A `StorageManager` is initialized with the configured page size
3. The system tablespace is created or opened
4. The `SchemaManager` is initialized to manage database objects
5. Buffer pools are created for all tablespaces

### 2. System Tablespace Handling

The system tablespace is a special tablespace named "SYSTEM" that contains all system catalog tables. When the database starts:

1. Check if the system tablespace is already loaded in the `StorageManager`
   - If loaded, ensure a buffer pool exists for it
   - If not loaded, proceed to step 2

2. Check if the system tablespace file exists on disk
   - If it exists, open it without modifying its size or content
   - If it doesn't exist, create a new system tablespace with the default size

3. When opening an existing tablespace:
   - Create a `StorageContainer` without specifying an initial size
   - Create a `Tablespace` object and register it with the `StorageManager`
   - Create a buffer pool for the tablespace

4. When creating a new tablespace:
   - Create the tablespace with an initial size
   - Register it with the system

### 3. Schema Manager Initialization

After the system tablespace is available, the `SchemaManager` initializes:

1. Check if system catalog tables exist
   - If not, create them
   - If they exist, keep them as-is

2. The system catalog tables include:
   - `SYS_TABLESPACES`: Stores tablespace metadata
   - `SYS_TABLES`: Stores table metadata
   - `SYS_COLUMNS`: Stores column metadata
   - `SYS_INDEXES`: Stores index metadata
   - `SYS_INDEX_COLUMNS`: Stores index column metadata

3. Load existing schema from the system catalog
   - Load all tablespaces from `SYS_TABLESPACES`
   - Load all tables from `SYS_TABLES` and `SYS_COLUMNS`
   - Load all indexes from `SYS_INDEXES` and `SYS_INDEX_COLUMNS`

### 4. Loading Existing Tablespaces

When loading tablespaces from the system catalog:

1. Read all entries from the `SYS_TABLESPACES` table
2. For each tablespace (except the SYSTEM tablespace which is already loaded):
   - Check if the tablespace file exists on disk
   - Open the tablespace file as a `StorageContainer`
   - Create a `Tablespace` object and register it with the `StorageManager`
   - Create a buffer pool for this tablespace

### 5. Loading Tables and Indexes

After tablespaces are loaded:

1. Read all entries from the `SYS_TABLES` table
2. For each table (except system tables):
   - Create a `Table` object with the table's metadata
   - Read its columns from the `SYS_COLUMNS` table
   - Set up primary key information
   - Register the table in the schema

3. Read all entries from the `SYS_INDEXES` table
4. For each index:
   - Create an `Index` object with the index's metadata
   - Read its columns from the `SYS_INDEX_COLUMNS` table
   - Register the index with its table

## Important Considerations

1. **Data Preservation:** The system never truncates or resets existing tablespace files during startup, ensuring data persistence.

2. **File Opening:** When opening existing files, the system opens them in read-write mode without changing their size.

3. **Versioning:** The system supports both current and legacy magic numbers for page identification, allowing backward compatibility.

4. **Error Handling:** If a tablespace file is missing but exists in the catalog, a warning is logged but startup continues.

5. **Schema Loading:** The schema loading process is robust against partial schema information, attempting to load as much valid data as possible.

## Implementation Guidelines

1. When checking if files exist, always use `File.exists()` before attempting to open or create them.

2. When opening existing files, use the constructor that doesn't specify an initial size to avoid truncation.

3. When reading from the system catalog, handle exceptions gracefully to prevent startup failures.

4. Log all operations during startup for debugging purposes.

5. Keep track of all resources (file handles, buffer pools) for proper cleanup during shutdown.

By following this specification, the StudioDB system will maintain data persistence between restarts and properly initialize the database system with existing data. 

## Detailed Analysis of Startup Process

### 1. Tablespace Opening Sequence

Looking at the code, I've identified the following critical path:

**DatabaseSystem Constructor**:
```java
<code_block_to_apply_changes_from>
```

**Key Issue #1**: The system creates/opens the tablespace file first, then initializes the SchemaManager.

### 2. System Catalog Initialization

The `SchemaManager` constructor:
```java
// Initialize system catalog - this creates the system tables if they don't exist
boolean catalogInitialized = initializeSystemCatalog();

// Load existing schema from system catalog if initialization was successful
if (catalogInitialized) {
    loadSchemaFromCatalog();
}
```

The `initializeSystemCatalog()` method:
```java
// Create SYS_TABLESPACES if it doesn't exist
if (!tableExists(SYS_TABLESPACES)) {
    // Create table code here
}
```

**Key Issue #2**: The `tableExists()` method only checks the in-memory tables map:
```java
private boolean tableExists(String tableName) {
    return tables.containsKey(tableName);
}
```

Since we're just starting up, this map is empty! So it always thinks tables don't exist and creates new ones, **even if they already exist on disk**.

### 3. Page Allocation for System Tables

When creating system tables:
```java
Page headerPage = bufferPool.allocatePage();
// ... initialize page ...
Page firstDataPage = bufferPool.allocatePage();
// ... initialize page ...
```

**Key Issue #3**: It's allocating new pages for system tables on every startup without checking if they already exist.

## Root Cause Identified

The fundamental problem is that:

1. When we open an existing tablespace file, we don't actually read its content to determine what tables already exist
2. The system blindly uses `tables.containsKey()` to check if tables exist, which always fails on startup
3. As a result, it creates new system tables with each startup, appending new pages to the file rather than reusing existing ones

## Solution Approach

We need to modify the initialization sequence to:

1. First open the tablespace file without modifying it
2. Before creating system tables, check the disk content to see if they already exist
3. If they exist, load their metadata rather than creating new ones

Let me illustrate the fix with specific code changes:

// First create system tablespace
boolean systemTablespaceCreated = false;
String systemTablespacePath = "./data/system_tablespace.dat";
try {
    boolean created = createSystemTablespace();
    if (!created) {
        logger.error("Failed to create or open system tablespace");
    } else {
        systemTablespaceCreated = true;
    }
} catch (Exception e) {
    logger.error("Error initializing system tablespace", e);
}

this.schemaManager = new SchemaManager(this); 