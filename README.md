# StudioDB - Educational Relational Database System

StudioDB is a simplified relational database system designed for educational purposes. It demonstrates key concepts of database management systems including:

- Page-based storage architecture
- Tablespaces and storage containers
- Buffer pool management
- Table and index operations
- B-Tree indexing
- Basic SQL query processing

## Features

- **Storage Layer**: Page-based storage with tablespaces and storage containers
- **Buffer Management**: Configurable buffer pools for each tablespace
- **Database Objects**: Support for Tables and B-Tree Indexes
- **SQL Frontend**: Basic SQL operations (SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, CREATE INDEX)
- **Visualization Interface**: Interactive web-based visualization of database internals

## Building and Running

### Prerequisites
- Java 11 or higher
- Gradle 6.8 or higher
- Node.js 16+ and npm (for the visualization interface)

### Build
```
./gradlew build
```

### Run
```
./gradlew run
```

### Start with visualization interface
```
./gradlew runWithViz
```

## Visualization Interface

The visualization interface provides an interactive way to observe and understand the internal workings of the database system:

- Real-time visualization of tablespaces, pages, and buffer pools
- Animated page transfers between disk and memory
- Visual indicators for page states (clean, dirty, pinned)
- B-Tree structure and operations visualization
- Table and row data visualization
- Query execution visualization with step-by-step animation

Access the visualization interface by visiting `http://localhost:8080` in your web browser when running with the visualization option enabled.

## SQL Command Reference

The following commands are available in the StudioDB SQL shell:

### Special Commands
```
help                           Display this help message
exit                           Exit the SQL shell
```

### Tablespace Management
```
CREATE TABLESPACE name DATAFILE 'path' SIZE n PAGES;
                                Create a new tablespace with specified size
SHOW TABLESPACES;              List all tablespaces and their properties
```

### Table Management
```
CREATE TABLE name (            Create a new table
    column1 type1 [NOT NULL],
    column2 type2 [NOT NULL],
    ...
    PRIMARY KEY (column1, ...)
) IN TABLESPACE tablespace_name;

DROP TABLE name;               Drop an existing table
SHOW TABLES;                   List all tables
```

### Index Management
```
CREATE [UNIQUE] INDEX name ON table (column1, ...);
                                Create a new index
DROP INDEX name;               Drop an existing index
SHOW INDEXES;                  List all indexes
```

### Data Manipulation
```
INSERT INTO table (col1, ...) VALUES (val1, ...);
                                Insert data into a table
SELECT col1, col2, ... FROM table [WHERE condition];
                                Query data from a table
UPDATE table SET col1 = val1, ... [WHERE condition];
                                Update data in a table
DELETE FROM table [WHERE condition];
                                Delete data from a table
```

### System Catalog
```
SELECT * FROM SYS_TABLES;      View all tables in the system catalog
SELECT * FROM SYS_COLUMNS;     View all columns in the system catalog
SELECT * FROM SYS_INDEXES;     View all indexes in the system catalog
SELECT * FROM SYS_INDEX_COLUMNS; View all index columns in the system catalog
```

### System Monitoring
```
SHOW BUFFERPOOLS;              Display buffer pool usage information
SHOW STATISTICS;               Show detailed page allocation and buffer pool stats
```

### Supported Data Types
```
INTEGER                        32-bit integer
FLOAT                          64-bit floating point
VARCHAR(n)                     Variable-length string (max n characters)
BOOLEAN                        True/false value
DATE                           Date value (YYYY-MM-DD)
```

## Project Structure

- `storage`: Page-based storage, tablespaces, storage containers
- `buffer`: Buffer pool implementation and management
- `schema`: Database schema, tables, and indexes
- `btree`: B-Tree index implementation
- `sql`: SQL parser and execution engine
- `utils`: Utility classes

## Educational Purpose

This project is specifically designed to illustrate database internals and is not intended for production use. It prioritizes clarity of concepts over performance or feature completeness. 