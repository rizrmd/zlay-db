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
