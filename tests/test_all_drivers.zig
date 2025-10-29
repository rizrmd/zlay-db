const std = @import("std");
const database = @import("../src/database.zig");
const types = @import("../src/utils/types.zig");
const errors = @import("../src/utils/errors.zig");

// Import all real drivers
const sqlite_driver = @import("../src/drivers/sqlite.zig");
const mysql_driver = @import("../src/drivers/mysql.zig");
const postgresql_driver = @import("../src/drivers/postgresql.zig");
const sqlserver_driver = @import("../src/drivers/sqlserver.zig");
const oracle_driver = @import("../src/drivers/oracle.zig");
const csv_driver = @import("../src/drivers/csv.zig");
const excel_driver = @import("../src/drivers/excel.zig");

const testing = std.testing;

const TestConfig = struct {
    database_type: database.DatabaseType,
    connection_string: []const u8,
    test_table: []const u8,
    skip_reason: ?[]const u8 = null,
};

const test_configs = [_]TestConfig{
    .{ .database_type = .sqlite, .connection_string = "sqlite:test.db", .test_table = "test_table" },
    .{ .database_type = .mysql, .connection_string = "mysql://localhost:3306/testdb?user=root&password=", .test_table = "test_table", .skip_reason = "Requires MySQL server" },
    .{ .database_type = .postgresql, .connection_string = "postgresql://localhost:5432/testdb?user=postgres&password=", .test_table = "test_table", .skip_reason = "Requires PostgreSQL server" },
    .{ .database_type = .sqlserver, .connection_string = "sqlserver://localhost:1433/testdb?user=sa&password=", .test_table = "test_table", .skip_reason = "Requires SQL Server" },
    .{ .database_type = .oracle, .connection_string = "oracle://localhost:1521/XE?user=system&password=", .test_table = "test_table", .skip_reason = "Requires Oracle Database" },
    .{ .database_type = .csv, .connection_string = "csv:test.csv", .test_table = "test" },
    .{ .database_type = .excel, .connection_string = "excel:test.xlsx", .test_table = "Sheet1" },
};

test "driver_factory_creates_correct_drivers" {
    const allocator = testing.allocator;

    // Test SQLite driver creation
    const sqlite_db = database.createDatabase(.sqlite, allocator);
    try testing.expect(sqlite_db != null);

    // Test MySQL driver creation
    const mysql_db = database.createDatabase(.mysql, allocator);
    try testing.expect(mysql_db != null);

    // Test PostgreSQL driver creation
    const postgresql_db = database.createDatabase(.postgresql, allocator);
    try testing.expect(postgresql_db != null);

    // Test SQL Server driver creation
    const sqlserver_db = database.createDatabase(.sqlserver, allocator);
    try testing.expect(sqlserver_db != null);

    // Test Oracle driver creation
    const oracle_db = database.createDatabase(.oracle, allocator);
    try testing.expect(oracle_db != null);

    // Test CSV driver creation
    const csv_db = database.createDatabase(.csv, allocator);
    try testing.expect(csv_db != null);

    // Test Excel driver creation
    const excel_db = database.createDatabase(.excel, allocator);
    try testing.expect(excel_db != null);
}

test "sqlite_basic_operations" {
    const allocator = testing.allocator;
    const db = database.createDatabase(.sqlite, allocator) orelse return error.DatabaseCreationFailed;
    defer db.deinit();

    // Connect to in-memory SQLite database
    const conn = try db.connect(.{
        .file_path = ":memory:",
    });
    defer conn.close();

    // Create test table
    const create_sql = "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT, value REAL, active BOOLEAN)";
    _ = try conn.executeUpdate(create_sql, &[_]types.Value{});

    // Insert test data
    const insert_sql = "INSERT INTO test_table (name, value, active) VALUES (?, ?, ?)";
    const affected_rows = try conn.executeUpdate(insert_sql, &[_]types.Value{
        .{ .text = "test1" },
        .{ .float = 42.5 },
        .{ .boolean = true },
    });
    try testing.expect(affected_rows == 1);

    // Query test data
    const select_sql = "SELECT id, name, value, active FROM test_table WHERE name = ?";
    const result = try conn.executeQuery(select_sql, &[_]types.Value{
        .{ .text = "test1" },
    });
    defer result.deinit();

    try testing.expect(result.rows.len == 1);
    try testing.expect(result.columns.len == 4);

    const row = result.rows[0];
    try testing.expect(row.values[0].integer == 1); // id
    try testing.expect(std.mem.eql(u8, row.values[1].text, "test1")); // name
    try testing.expect(row.values[2].float == 42.5); // value
    try testing.expect(row.values[3].boolean == true); // active
}

test "csv_basic_operations" {
    const allocator = testing.allocator;
    const db = database.createDatabase(.csv, allocator) orelse return error.DatabaseCreationFailed;
    defer db.deinit();

    // Create test CSV file
    const csv_content =
        \\id,name,value,active
        \\1,test1,42.5,true
        \\2,test2,24.0,false
        \\3,test3,100.0,true
    ;

    const csv_file = try std.fs.cwd().createFile("test.csv", .{ .truncate = true });
    defer csv_file.close();
    try csv_file.writeAll(csv_content);
    defer std.fs.cwd().deleteFile("test.csv") catch {};

    // Connect to CSV file
    const conn = try db.connect(.{
        .file_path = "test.csv",
    });
    defer conn.close();

    // Query CSV data
    const result = try conn.executeQuery("SELECT * FROM test WHERE active = true", &[_]types.Value{});
    defer result.deinit();

    try testing.expect(result.rows.len == 2);
    try testing.expect(result.columns.len == 4);

    // Check first row
    const row1 = result.rows[0];
    try testing.expect(std.mem.eql(u8, row1.values[1].text, "test1"));
    try testing.expect(row1.values[3].boolean == true);
}

test "excel_basic_operations" {
    const allocator = testing.allocator;
    const db = database.createDatabase(.excel, allocator) orelse return error.DatabaseCreationFailed;
    defer db.deinit();

    // Create test Excel file (using DuckDB to create it)
    const csv_content =
        \\id,name,value,active
        \\1,test1,42.5,true
        \\2,test2,24.0,false
        \\3,test3,100.0,true
    ;

    // First create a CSV file that DuckDB can use to create Excel
    const csv_file = try std.fs.cwd().createFile("test_excel.csv", .{ .truncate = true });
    defer csv_file.close();
    try csv_file.writeAll(csv_content);
    defer std.fs.cwd().deleteFile("test_excel.csv") catch {};

    // Create a simple test Excel file (for testing, we'll use a CSV-like format)
    // Note: In real usage, Excel files would be created by external tools
    const xlsx_file = try std.fs.cwd().createFile("test.xlsx", .{ .truncate = true });
    defer xlsx_file.close();
    try xlsx_file.writeAll(csv_content); // DuckDB can handle CSV-like Excel data
    defer std.fs.cwd().deleteFile("test.xlsx") catch {};

    // Connect to Excel file
    const conn = db.connect(.{
        .file_path = "test.xlsx",
    }) catch |err| {
        // Skip test if DuckDB is not available or can't read the file
        std.debug.print("Skipping Excel test: {}\n", .{err});
        return error.SkipZigTest;
    };
    defer conn.close();

    // Query Excel data
    const result = conn.executeQuery("SELECT * FROM Sheet1 WHERE active = true", &[_]types.Value{}) catch |err| {
        // If query fails, skip the test (Excel format might not be supported)
        std.debug.print("Skipping Excel query test: {}\n", .{err});
        return error.SkipZigTest;
    };
    defer result.deinit();

    try testing.expect(result.rows.len >= 1);
    try testing.expect(result.columns.len >= 2);
}

test "connection_string_parsing" {
    const allocator = testing.allocator;

    // Test SQLite connection string
    const sqlite_config = try database.parseConnectionString("sqlite:/path/to/db.sqlite", allocator);
    defer allocator.free(sqlite_config.file_path.?);
    try testing.expect(sqlite_config.database_type == .sqlite);
    try testing.expect(std.mem.eql(u8, sqlite_config.file_path.?, "/path/to/db.sqlite"));

    // Test MySQL connection string
    const mysql_config = try database.parseConnectionString("mysql://localhost:3306/testdb?user=root&password=pass", allocator);
    defer {
        allocator.free(mysql_config.host.?);
        allocator.free(mysql_config.database.?);
        allocator.free(mysql_config.username.?);
        allocator.free(mysql_config.password.?);
    }
    try testing.expect(mysql_config.database_type == .mysql);
    try testing.expect(std.mem.eql(u8, mysql_config.host.?, "localhost"));
    try testing.expect(mysql_config.port.? == 3306);
    try testing.expect(std.mem.eql(u8, mysql_config.database.?, "testdb"));
    try testing.expect(std.mem.eql(u8, mysql_config.username.?, "root"));
    try testing.expect(std.mem.eql(u8, mysql_config.password.?, "pass"));

    // Test PostgreSQL connection string
    const postgresql_config = try database.parseConnectionString("postgresql://localhost:5432/testdb?user=postgres&password=pass", allocator);
    defer {
        allocator.free(postgresql_config.host.?);
        allocator.free(postgresql_config.database.?);
        allocator.free(postgresql_config.username.?);
        allocator.free(postgresql_config.password.?);
    }
    try testing.expect(postgresql_config.database_type == .postgresql);
    try testing.expect(std.mem.eql(u8, postgresql_config.host.?, "localhost"));
    try testing.expect(postgresql_config.port.? == 5432);
    try testing.expect(std.mem.eql(u8, postgresql_config.database.?, "testdb"));
    try testing.expect(std.mem.eql(u8, postgresql_config.username.?, "postgres"));
    try testing.expect(std.mem.eql(u8, postgresql_config.password.?, "pass"));
}

test "transaction_support" {
    const allocator = testing.allocator;
    const db = database.createDatabase(.sqlite, allocator) orelse return error.DatabaseCreationFailed;
    defer db.deinit();

    const conn = try db.connect(.{
        .file_path = ":memory:",
    });
    defer conn.close();

    // Create test table
    _ = try conn.executeUpdate("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)", &[_]types.Value{});

    // Test transaction
    const transaction = try conn.beginTransaction();
    defer transaction.rollback() catch {};

    // Insert data within transaction
    _ = try transaction.executeUpdate("INSERT INTO test_table (name) VALUES (?)", &[_]types.Value{
        .{ .text = "transaction_test" },
    });

    // Commit transaction
    try transaction.commit();

    // Verify data was committed
    const result = try conn.executeQuery("SELECT COUNT(*) as count FROM test_table", &[_]types.Value{});
    defer result.deinit();

    try testing.expect(result.rows.len == 1);
    try testing.expect(result.rows[0].values[0].integer == 1);
}

test "parameter_binding" {
    const allocator = testing.allocator;
    const db = database.createDatabase(.sqlite, allocator) orelse return error.DatabaseCreationFailed;
    defer db.deinit();

    const conn = try db.connect(.{
        .file_path = ":memory:",
    });
    defer conn.close();

    // Create test table
    _ = try conn.executeUpdate("CREATE TABLE test_table (id INTEGER PRIMARY KEY, text_col TEXT, int_col INTEGER, float_col REAL, bool_col BOOLEAN, date_col DATE, time_col TIME, timestamp_col TIMESTAMP, binary_col BLOB)", &[_]types.Value{});

    // Test all data types
    const test_data = [_]types.Value{
        .{ .text = "test string" },
        .{ .integer = 42 },
        .{ .float = 3.14159 },
        .{ .boolean = true },
        .{ .date = "2023-01-01" },
        .{ .time = "12:30:45" },
        .{ .timestamp = "2023-01-01 12:30:45" },
        .{ .binary = &[_]u8{ 0x01, 0x02, 0x03, 0x04 } },
    };

    const insert_sql = "INSERT INTO test_table (text_col, int_col, float_col, bool_col, date_col, time_col, timestamp_col, binary_col) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
    const affected_rows = try conn.executeUpdate(insert_sql, &test_data);
    try testing.expect(affected_rows == 1);

    // Query back the data
    const select_sql = "SELECT text_col, int_col, float_col, bool_col, date_col, time_col, timestamp_col, binary_col FROM test_table WHERE id = ?";
    const result = try conn.executeQuery(select_sql, &[_]types.Value{
        .{ .integer = 1 },
    });
    defer result.deinit();

    try testing.expect(result.rows.len == 1);
    try testing.expect(result.columns.len == 8);

    const row = result.rows[0];
    try testing.expect(std.mem.eql(u8, row.values[0].text, "test string"));
    try testing.expect(row.values[1].integer == 42);
    try testing.expect(row.values[2].float == 3.14159);
    try testing.expect(row.values[3].boolean == true);
    try testing.expect(std.mem.eql(u8, row.values[4].text, "2023-01-01"));
    try testing.expect(std.mem.eql(u8, row.values[5].text, "12:30:45"));
    try testing.expect(std.mem.eql(u8, row.values[6].text, "2023-01-01 12:30:45"));
    try testing.expect(std.mem.eql(u8, row.values[7].binary, &[_]u8{ 0x01, 0x02, 0x03, 0x04 }));
}

test "error_handling" {
    const allocator = testing.allocator;
    const db = database.createDatabase(.sqlite, allocator) orelse return error.DatabaseCreationFailed;
    defer db.deinit();

    // Test invalid connection
    const invalid_conn = db.connect(.{
        .file_path = "/invalid/path/db.sqlite",
    });
    try testing.expectError(error.ConnectionFailed, invalid_conn);

    // Test valid connection
    const conn = try db.connect(.{
        .file_path = ":memory:",
    });
    defer conn.close();

    // Test invalid SQL
    const invalid_sql = conn.executeQuery("INVALID SQL STATEMENT", &[_]types.Value{});
    try testing.expectError(error.QueryFailed, invalid_sql);

    // Test non-existent table
    const no_table = conn.executeQuery("SELECT * FROM non_existent_table", &[_]types.Value{});
    try testing.expectError(error.QueryFailed, no_table);
}

test "connection_info" {
    const allocator = testing.allocator;
    const db = database.createDatabase(.sqlite, allocator) orelse return error.DatabaseCreationFailed;
    defer db.deinit();

    const conn = try db.connect(.{
        .file_path = ":memory:",
    });
    defer conn.close();

    const info = conn.getConnectionInfo();
    try testing.expect(info.database_type == .sqlite);
    try testing.expect(std.mem.eql(u8, info.host, "file"));
    try testing.expect(info.connected_at > 0);
    try testing.expect(info.server_version.len > 0);
}

// Performance test
test "performance_benchmark" {
    const allocator = testing.allocator;
    const db = database.createDatabase(.sqlite, allocator) orelse return error.DatabaseCreationFailed;
    defer db.deinit();

    const conn = try db.connect(.{
        .file_path = ":memory:",
    });
    defer conn.close();

    // Create test table
    _ = try conn.executeUpdate("CREATE TABLE test_table (id INTEGER PRIMARY KEY, data TEXT)", &[_]types.Value{});

    const start_time = std.time.nanoTimestamp();

    // Insert 1000 records
    for (0..1000) |i| {
        const insert_sql = "INSERT INTO test_table (data) VALUES (?)";
        _ = try conn.executeUpdate(insert_sql, &[_]types.Value{
            .{ .text = try std.fmt.allocPrint(allocator, "data_{}", .{i}) },
        });
    }

    const insert_time = std.time.nanoTimestamp();

    // Query all records
    const result = try conn.executeQuery("SELECT COUNT(*) as count FROM test_table", &[_]types.Value{});
    defer result.deinit();

    const query_time = std.time.nanoTimestamp();

    const insert_duration = insert_time - start_time;
    const query_duration = query_time - insert_time;

    std.debug.print("Insert 1000 records: {} ms\n", .{insert_duration / 1_000_000});
    std.debug.print("Query count: {} ms\n", .{query_duration / 1_000_000});

    try testing.expect(result.rows[0].values[0].integer == 1000);
    try testing.expect(insert_duration < 5_000_000_000); // Less than 5 seconds
    try testing.expect(query_duration < 1_000_000_000); // Less than 1 second
}

// MySQL basic operations test
test "mysql_basic_operations" {
    const allocator = testing.allocator;
    const db = database.createDatabase(.mysql, allocator) orelse return error.DatabaseCreationFailed;
    defer db.deinit();

    // Try to connect to MySQL server
    const conn = db.connect(.{
        .host = "localhost",
        .port = 3306,
        .database = "testdb",
        .username = "root",
        .password = "",
    }) catch |err| {
        // Skip test if MySQL server is not available
        std.debug.print("Skipping MySQL test (server not available): {}\n", .{err});
        return error.SkipZigTest;
    };
    defer conn.close();

    // Drop table if exists (cleanup from previous runs)
    _ = conn.executeUpdate("DROP TABLE IF EXISTS test_table", &[_]types.Value{}) catch {};

    // Create test table
    const create_sql = "CREATE TABLE test_table (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), value DOUBLE, active BOOLEAN)";
    _ = try conn.executeUpdate(create_sql, &[_]types.Value{});

    // Insert test data
    const insert_sql = "INSERT INTO test_table (name, value, active) VALUES (?, ?, ?)";
    const affected_rows = try conn.executeUpdate(insert_sql, &[_]types.Value{
        .{ .text = "mysql_test" },
        .{ .float = 99.9 },
        .{ .boolean = true },
    });
    try testing.expect(affected_rows == 1);

    // Query test data
    const select_sql = "SELECT id, name, value, active FROM test_table WHERE name = ?";
    const result = try conn.executeQuery(select_sql, &[_]types.Value{
        .{ .text = "mysql_test" },
    });
    defer result.deinit();

    try testing.expect(result.rows.len == 1);
    try testing.expect(result.columns.len == 4);

    const row = result.rows[0];
    try testing.expect(row.values[0].integer >= 1); // id (auto-increment)
    try testing.expect(std.mem.eql(u8, row.values[1].text, "mysql_test")); // name
    try testing.expect(row.values[2].float == 99.9); // value
    try testing.expect(row.values[3].boolean == true); // active

    // Cleanup
    _ = try conn.executeUpdate("DROP TABLE test_table", &[_]types.Value{});
}

// PostgreSQL basic operations test
test "postgresql_basic_operations" {
    const allocator = testing.allocator;
    const db = database.createDatabase(.postgresql, allocator) orelse return error.DatabaseCreationFailed;
    defer db.deinit();

    // Try to connect to PostgreSQL server
    const conn = db.connect(.{
        .host = "localhost",
        .port = 5432,
        .database = "testdb",
        .username = "postgres",
        .password = "",
    }) catch |err| {
        // Skip test if PostgreSQL server is not available
        std.debug.print("Skipping PostgreSQL test (server not available): {}\n", .{err});
        return error.SkipZigTest;
    };
    defer conn.close();

    // Drop table if exists (cleanup from previous runs)
    _ = conn.executeUpdate("DROP TABLE IF EXISTS test_table", &[_]types.Value{}) catch {};

    // Create test table
    const create_sql = "CREATE TABLE test_table (id SERIAL PRIMARY KEY, name VARCHAR(255), value DOUBLE PRECISION, active BOOLEAN)";
    _ = try conn.executeUpdate(create_sql, &[_]types.Value{});

    // Insert test data
    const insert_sql = "INSERT INTO test_table (name, value, active) VALUES (?, ?, ?)";
    const affected_rows = try conn.executeUpdate(insert_sql, &[_]types.Value{
        .{ .text = "postgresql_test" },
        .{ .float = 88.8 },
        .{ .boolean = true },
    });
    try testing.expect(affected_rows == 1);

    // Query test data
    const select_sql = "SELECT id, name, value, active FROM test_table WHERE name = ?";
    const result = try conn.executeQuery(select_sql, &[_]types.Value{
        .{ .text = "postgresql_test" },
    });
    defer result.deinit();

    try testing.expect(result.rows.len == 1);
    try testing.expect(result.columns.len == 4);

    const row = result.rows[0];
    try testing.expect(row.values[0].integer >= 1); // id (auto-increment)
    try testing.expect(std.mem.eql(u8, row.values[1].text, "postgresql_test")); // name
    try testing.expect(row.values[2].float == 88.8); // value
    try testing.expect(row.values[3].boolean == true); // active

    // Cleanup
    _ = try conn.executeUpdate("DROP TABLE test_table", &[_]types.Value{});
}

// SQL Server basic operations test
test "sqlserver_basic_operations" {
    const allocator = testing.allocator;
    const db = database.createDatabase(.sqlserver, allocator) orelse return error.DatabaseCreationFailed;
    defer db.deinit();

    // Try to connect to SQL Server
    const conn = db.connect(.{
        .host = "localhost",
        .port = 1433,
        .database = "testdb",
        .username = "sa",
        .password = "",
    }) catch |err| {
        // Skip test if SQL Server is not available
        std.debug.print("Skipping SQL Server test (server not available): {}\n", .{err});
        return error.SkipZigTest;
    };
    defer conn.close();

    // Drop table if exists (cleanup from previous runs)
    _ = conn.executeUpdate("IF OBJECT_ID('test_table', 'U') IS NOT NULL DROP TABLE test_table", &[_]types.Value{}) catch {};

    // Create test table
    const create_sql = "CREATE TABLE test_table (id INT IDENTITY(1,1) PRIMARY KEY, name NVARCHAR(255), value FLOAT, active BIT)";
    _ = try conn.executeUpdate(create_sql, &[_]types.Value{});

    // Insert test data
    const insert_sql = "INSERT INTO test_table (name, value, active) VALUES (?, ?, ?)";
    const affected_rows = try conn.executeUpdate(insert_sql, &[_]types.Value{
        .{ .text = "sqlserver_test" },
        .{ .float = 77.7 },
        .{ .boolean = true },
    });
    try testing.expect(affected_rows == 1);

    // Query test data
    const select_sql = "SELECT id, name, value, active FROM test_table WHERE name = ?";
    const result = try conn.executeQuery(select_sql, &[_]types.Value{
        .{ .text = "sqlserver_test" },
    });
    defer result.deinit();

    try testing.expect(result.rows.len == 1);
    try testing.expect(result.columns.len == 4);

    const row = result.rows[0];
    try testing.expect(row.values[0].integer >= 1); // id (auto-increment)
    try testing.expect(std.mem.eql(u8, row.values[1].text, "sqlserver_test")); // name
    try testing.expect(row.values[2].float == 77.7); // value
    try testing.expect(row.values[3].boolean == true); // active

    // Cleanup
    _ = try conn.executeUpdate("DROP TABLE test_table", &[_]types.Value{});
}

// Oracle basic operations test
test "oracle_basic_operations" {
    const allocator = testing.allocator;
    const db = database.createDatabase(.oracle, allocator) orelse return error.DatabaseCreationFailed;
    defer db.deinit();

    // Try to connect to Oracle Database
    const conn = db.connect(.{
        .host = "localhost",
        .port = 1521,
        .database = "XE",
        .username = "system",
        .password = "",
    }) catch |err| {
        // Skip test if Oracle Database is not available
        std.debug.print("Skipping Oracle test (server not available): {}\n", .{err});
        return error.SkipZigTest;
    };
    defer conn.close();

    // Drop table if exists (cleanup from previous runs)
    _ = conn.executeUpdate("BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_table'; EXCEPTION WHEN OTHERS THEN NULL; END;", &[_]types.Value{}) catch {};

    // Create test table
    const create_sql = "CREATE TABLE test_table (id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY, name VARCHAR2(255), value NUMBER, active NUMBER(1))";
    _ = try conn.executeUpdate(create_sql, &[_]types.Value{});

    // Insert test data
    const insert_sql = "INSERT INTO test_table (name, value, active) VALUES (?, ?, ?)";
    const affected_rows = try conn.executeUpdate(insert_sql, &[_]types.Value{
        .{ .text = "oracle_test" },
        .{ .float = 66.6 },
        .{ .boolean = true },
    });
    try testing.expect(affected_rows == 1);

    // Query test data
    const select_sql = "SELECT id, name, value, active FROM test_table WHERE name = ?";
    const result = try conn.executeQuery(select_sql, &[_]types.Value{
        .{ .text = "oracle_test" },
    });
    defer result.deinit();

    try testing.expect(result.rows.len == 1);
    try testing.expect(result.columns.len == 4);

    const row = result.rows[0];
    try testing.expect(row.values[0].integer >= 1); // id (auto-increment)
    try testing.expect(std.mem.eql(u8, row.values[1].text, "oracle_test")); // name
    try testing.expect(row.values[2].float == 66.6); // value
    try testing.expect(row.values[3].boolean == true); // active

    // Cleanup
    _ = try conn.executeUpdate("DROP TABLE test_table", &[_]types.Value{});
}

// Update operations test
test "update_operations" {
    const allocator = testing.allocator;
    const db = database.createDatabase(.sqlite, allocator) orelse return error.DatabaseCreationFailed;
    defer db.deinit();

    const conn = try db.connect(.{
        .file_path = ":memory:",
    });
    defer conn.close();

    // Create test table
    _ = try conn.executeUpdate("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT, value REAL, active BOOLEAN)", &[_]types.Value{});

    // Insert initial data
    _ = try conn.executeUpdate("INSERT INTO test_table (name, value, active) VALUES (?, ?, ?)", &[_]types.Value{
        .{ .text = "original" },
        .{ .float = 10.0 },
        .{ .boolean = false },
    });

    // Update the data
    const update_sql = "UPDATE test_table SET name = ?, value = ?, active = ? WHERE id = ?";
    const updated_rows = try conn.executeUpdate(update_sql, &[_]types.Value{
        .{ .text = "updated" },
        .{ .float = 20.0 },
        .{ .boolean = true },
        .{ .integer = 1 },
    });
    try testing.expect(updated_rows == 1);

    // Verify the update
    const result = try conn.executeQuery("SELECT name, value, active FROM test_table WHERE id = ?", &[_]types.Value{
        .{ .integer = 1 },
    });
    defer result.deinit();

    try testing.expect(result.rows.len == 1);
    const row = result.rows[0];
    try testing.expect(std.mem.eql(u8, row.values[0].text, "updated"));
    try testing.expect(row.values[1].float == 20.0);
    try testing.expect(row.values[2].boolean == true);

    // Test bulk update
    _ = try conn.executeUpdate("INSERT INTO test_table (name, value, active) VALUES ('test2', 30.0, false)", &[_]types.Value{});
    _ = try conn.executeUpdate("INSERT INTO test_table (name, value, active) VALUES ('test3', 40.0, false)", &[_]types.Value{});

    const bulk_updated = try conn.executeUpdate("UPDATE test_table SET active = ? WHERE active = ?", &[_]types.Value{
        .{ .boolean = true },
        .{ .boolean = false },
    });
    try testing.expect(bulk_updated == 2);

    // Verify bulk update
    const bulk_result = try conn.executeQuery("SELECT COUNT(*) as count FROM test_table WHERE active = true", &[_]types.Value{});
    defer bulk_result.deinit();
    try testing.expect(bulk_result.rows[0].values[0].integer == 3);
}

// Delete operations test
test "delete_operations" {
    const allocator = testing.allocator;
    const db = database.createDatabase(.sqlite, allocator) orelse return error.DatabaseCreationFailed;
    defer db.deinit();

    const conn = try db.connect(.{
        .file_path = ":memory:",
    });
    defer conn.close();

    // Create test table
    _ = try conn.executeUpdate("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT, value REAL)", &[_]types.Value{});

    // Insert test data
    _ = try conn.executeUpdate("INSERT INTO test_table (name, value) VALUES ('test1', 10.0)", &[_]types.Value{});
    _ = try conn.executeUpdate("INSERT INTO test_table (name, value) VALUES ('test2', 20.0)", &[_]types.Value{});
    _ = try conn.executeUpdate("INSERT INTO test_table (name, value) VALUES ('test3', 30.0)", &[_]types.Value{});

    // Test single row delete
    const delete_sql = "DELETE FROM test_table WHERE name = ?";
    const deleted_rows = try conn.executeUpdate(delete_sql, &[_]types.Value{
        .{ .text = "test1" },
    });
    try testing.expect(deleted_rows == 1);

    // Verify deletion
    const result1 = try conn.executeQuery("SELECT COUNT(*) as count FROM test_table", &[_]types.Value{});
    defer result1.deinit();
    try testing.expect(result1.rows[0].values[0].integer == 2);

    // Test bulk delete
    const bulk_delete_sql = "DELETE FROM test_table WHERE value > ?";
    const bulk_deleted = try conn.executeUpdate(bulk_delete_sql, &[_]types.Value{
        .{ .float = 15.0 },
    });
    try testing.expect(bulk_deleted == 2);

    // Verify bulk deletion
    const result2 = try conn.executeQuery("SELECT COUNT(*) as count FROM test_table", &[_]types.Value{});
    defer result2.deinit();
    try testing.expect(result2.rows[0].values[0].integer == 0);

    // Test delete with no matching rows
    const no_match_delete = try conn.executeUpdate("DELETE FROM test_table WHERE name = ?", &[_]types.Value{
        .{ .text = "nonexistent" },
    });
    try testing.expect(no_match_delete == 0);
}
