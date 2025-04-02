package net.seitter.studiodb.schema;

/**
 * Represents the data types supported by the database system.
 */
public enum DataType {
    INTEGER,
    FLOAT,
    VARCHAR,
    BOOLEAN,
    DATE;
    
    /**
     * Validates if a value can be stored in this data type.
     *
     * @param value The value to validate
     * @return true if the value is compatible with this data type
     */
    public boolean validateValue(Object value) {
        if (value == null) {
            return true; // Null can be stored in any type as long as the column is nullable
        }
        
        switch (this) {
            case INTEGER:
                return value instanceof Integer || value instanceof Long || 
                      (value instanceof String && ((String) value).matches("-?\\d+"));
            case FLOAT:
                return value instanceof Float || value instanceof Double ||
                      (value instanceof String && ((String) value).matches("-?\\d+(\\.\\d+)?"));
            case VARCHAR:
                return value instanceof String;
            case BOOLEAN:
                return value instanceof Boolean ||
                      (value instanceof String && 
                      (((String) value).equalsIgnoreCase("true") || 
                       ((String) value).equalsIgnoreCase("false")));
            case DATE:
                return value instanceof java.util.Date || value instanceof java.time.LocalDate ||
                      (value instanceof String && 
                      ((String) value).matches("\\d{4}-\\d{2}-\\d{2}"));
            default:
                return false;
        }
    }
    
    /**
     * Converts a string value to the appropriate Java type for this data type.
     *
     * @param stringValue The string value to convert
     * @return The converted value, or null if conversion fails
     */
    public Object parseValue(String stringValue) {
        if (stringValue == null) {
            return null;
        }
        
        try {
            switch (this) {
                case INTEGER:
                    return Integer.parseInt(stringValue);
                case FLOAT:
                    return Double.parseDouble(stringValue);
                case VARCHAR:
                    return stringValue;
                case BOOLEAN:
                    return Boolean.parseBoolean(stringValue);
                case DATE:
                    return java.time.LocalDate.parse(stringValue);
                default:
                    return null;
            }
        } catch (Exception e) {
            return null; // Conversion failed
        }
    }
    
    /**
     * Converts an SQL type name to the corresponding DataType.
     *
     * @param sqlType The SQL type name
     * @return The corresponding DataType, or null if not found
     */
    public static DataType fromSqlType(String sqlType) {
        String upperType = sqlType.toUpperCase();
        
        if (upperType.contains("INT")) {
            return INTEGER;
        } else if (upperType.contains("FLOAT") || upperType.contains("DOUBLE") || 
                  upperType.contains("REAL") || upperType.contains("DECIMAL")) {
            return FLOAT;
        } else if (upperType.contains("VARCHAR") || upperType.contains("CHAR") || 
                  upperType.contains("TEXT")) {
            return VARCHAR;
        } else if (upperType.contains("BOOL")) {
            return BOOLEAN;
        } else if (upperType.contains("DATE") || upperType.contains("TIME")) {
            return DATE;
        }
        
        return null;
    }
} 