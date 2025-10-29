const std = @import("std");
const database = @import("../database.zig");
const types = @import("../utils/types.zig");
const errors = @import("../utils/errors.zig");

// DuckDB C API types (same as CSV driver)
const duckdb_database = opaque {};
const duckdb_connection = opaque {};
const duckdb_result = opaque {};

const duckdb_state = enum(c_int) {
    DUCKDB_STATE_SUCCESS = 0,
    DUCKDB_STATE_ERROR = 1,
};

const duckdb_type = enum(c_int) {
    DUCKDB_TYPE_INVALID = 0,
    DUCKDB_TYPE_BOOLEAN = 1,
    DUCKDB_TYPE_TINYINT = 2,
    DUCKDB_TYPE_SMALLINT = 3,
    DUCKDB_TYPE_INTEGER = 4,
    DUCKDB_TYPE_BIGINT = 5,
    DUCKDB_TYPE_UTINYINT = 6,
    DUCKDB_TYPE_USMALLINT = 7,
    DUCKDB_TYPE_UINTEGER = 8,
    DUCKDB_TYPE_UBIGINT = 9,
    DUCKDB_TYPE_FLOAT = 10,
    DUCKDB_TYPE_DOUBLE = 11,
    DUCKDB_TYPE_TIMESTAMP = 12,
    DUCKDB_TYPE_DATE = 13,
    DUCKDB_TYPE_TIME = 14,
    DUCKDB_TYPE_INTERVAL = 15,
    DUCKDB_TYPE_HUGEINT = 16,
    DUCKDB_TYPE_VARCHAR = 17,
    DUCKDB_TYPE_BLOB = 18,
};

// DuckDB C functions (same as CSV driver)
extern fn duckdb_open(path: [*:0]const u8, out_database: *?*duckdb_database) duckdb_state;
extern fn duckdb_close(database: *duckdb_database) void;
extern fn duckdb_connect(database: *duckdb_database, out_connection: *?*duckdb_connection) duckdb_state;
extern fn duckdb_disconnect(connection: *duckdb_connection) void;
extern fn duckdb_query(connection: *duckdb_connection, sql: [*:0]const u8, out_result: *?*duckdb_result) duckdb_state;
extern fn duckdb_destroy_result(result: *duckdb_result) void;
extern fn duckdb_column_count(result: *duckdb_result) c_ulong;
extern fn duckdb_row_count(result: *duckdb_result) c_ulong;
extern fn duckdb_column_name(result: *duckdb_result, col: c_ulong) [*:0]const u8;
extern fn duckdb_column_type(result: *duckdb_result, col: c_ulong) duckdb_type;
extern fn duckdb_value_varchar(result: *duckdb_result, col: c_ulong, row: c_ulong) [*:0]const u8;
extern fn duckdb_value_boolean(result: *duckdb_result, col: c_ulong, row: c_ulong) bool;
extern fn duckdb_value_int64(result: *duckdb_result, col: c_ulong, row: c_ulong) i64;
extern fn duckdb_value_double(result: *duckdb_result, col: c_ulong, row: c_ulong) f64;
extern fn duckdb_value_is_null(result: *duckdb_result, col: c_ulong, row: c_ulong) bool;
extern fn duckdb_error(result: *duckdb_result) [*:0]const u8;

// DuckDB-based Excel driver
pub const ExcelDriver = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    file_path: ?[]const u8 = null,
    db: ?*duckdb_database = null,
    conn: ?*duckdb_connection = null,

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        if (self.conn) |conn| {
            duckdb_disconnect(conn);
            self.conn = null;
        }
        if (self.db) |db| {
            duckdb_close(db);
            self.db = null;
        }
    }

    pub fn connect(self: *Self, config: database.ConnectionConfig) errors.DatabaseError!database.Connection {
        const file_path = config.file_path orelse return errors.DatabaseError.InvalidConfiguration;
        self.file_path = file_path;

        // Open DuckDB in-memory database
        var db: ?*duckdb_database = null;
        if (duckdb_open(":memory:", &db) != .DUCKDB_STATE_SUCCESS) {
            return errors.DatabaseError.ConnectionFailed;
        }
        self.db = db.?;

        // Connect to database
        var conn: ?*duckdb_connection = null;
        if (duckdb_connect(db.?, &conn) != .DUCKDB_STATE_SUCCESS) {
            duckdb_close(db.?);
            return errors.DatabaseError.ConnectionFailed;
        }
        self.conn = conn.?;

        // Install and load spatial extension for Excel support
        var result: ?*duckdb_result = null;
        if (duckdb_query(conn.?, "INSTALL spatial", &result) != .DUCKDB_STATE_SUCCESS) {
            const error_msg = duckdb_error(result orelse return errors.DatabaseError.ConnectionFailed);
            std.debug.print("DuckDB install spatial error: {s}\n", .{error_msg});
            if (result) |r| duckdb_destroy_result(r);
            duckdb_disconnect(conn.?);
            duckdb_close(db.?);
            return errors.DatabaseError.ConnectionFailed;
        }
        if (result) |r| duckdb_destroy_result(r);

        if (duckdb_query(conn.?, "LOAD spatial", &result) != .DUCKDB_STATE_SUCCESS) {
            const error_msg = duckdb_error(result orelse return errors.DatabaseError.ConnectionFailed);
            std.debug.print("DuckDB load spatial error: {s}\n", .{error_msg});
            if (result) |r| duckdb_destroy_result(r);
            duckdb_disconnect(conn.?);
            duckdb_close(db.?);
            return errors.DatabaseError.ConnectionFailed;
        }
        if (result) |r| duckdb_destroy_result(r);

        // Create Excel table using DuckDB's Excel reader
        const create_sql = try std.fmt.allocPrint(self.allocator, "CREATE TABLE excel_data AS SELECT * FROM st_read_auto('{s}')\x00", .{file_path});
        defer self.allocator.free(create_sql);

        if (duckdb_query(conn.?, create_sql[0..create_sql.len :0], &result) != .DUCKDB_STATE_SUCCESS) {
            const error_msg = duckdb_error(result orelse return errors.DatabaseError.ConnectionFailed);
            std.debug.print("DuckDB Excel error: {s}\n", .{error_msg});
            if (result) |r| duckdb_destroy_result(r);
            duckdb_disconnect(conn.?);
            duckdb_close(db.?);
            return errors.DatabaseError.ConnectionFailed;
        }
        if (result) |r| duckdb_destroy_result(r);

        // Create connection object
        const conn_data = try self.allocator.create(ExcelConnectionData);
        conn_data.* = ExcelConnectionData{
            .driver = self,
            .file_path = file_path,
        };

        return database.Connection{
            .driver = createDriverInterface(self),
            .connection_data = conn_data,
        };
    }

    pub fn executeQuery(self: *Self, sql: []const u8, args: []const types.Value) errors.DatabaseError!types.ResultSet {
        if (self.conn == null) return errors.DatabaseError.InvalidConnection;

        // For DuckDB, we need to substitute parameters manually since we don't have prepared statements
        const final_sql = if (args.len > 0) try self.substituteParameters(sql, args) else sql;

        // Convert SQL to work with our Excel table
        const converted_sql = try self.convertSql(final_sql);
        defer self.allocator.free(converted_sql);

        var result: ?*duckdb_result = null;
        if (duckdb_query(self.conn.?, converted_sql, &result) != .DUCKDB_STATE_SUCCESS) {
            const error_msg = duckdb_error(result);
            std.debug.print("DuckDB query error: {s}\n", .{error_msg});
            if (result) |r| duckdb_destroy_result(r);
            return errors.DatabaseError.QueryFailed;
        }
        defer duckdb_destroy_result(result.?);

        return self.convertResult(result.?);
    }

    pub fn executeUpdate(self: *Self, sql: []const u8, args: []const types.Value) errors.DatabaseError!u64 {
        if (self.conn == null) return errors.DatabaseError.InvalidConnection;

        // For DuckDB, we need to substitute parameters manually since we don't have prepared statements
        const final_sql = if (args.len > 0) try self.substituteParameters(sql, args) else sql;

        // Convert SQL to work with our Excel table
        const converted_sql = try self.convertSql(final_sql);
        defer self.allocator.free(converted_sql);

        var result: ?*duckdb_result = null;
        if (duckdb_query(self.conn.?, converted_sql, &result) != .DUCKDB_STATE_SUCCESS) {
            const error_msg = duckdb_error(result);
            std.debug.print("DuckDB update error: {s}\n", .{error_msg});
            if (result) |r| duckdb_destroy_result(r);
            return errors.DatabaseError.QueryFailed;
        }
        defer duckdb_destroy_result(result.?);

        // DuckDB doesn't provide affected rows count for Excel operations
        return 0;
    }

    pub fn beginTransaction(self: *Self) errors.DatabaseError!database.Transaction {
        // DuckDB with Excel files doesn't support transactions in the traditional sense
        // For now, return a stub transaction that does nothing
        const transaction_data = try self.allocator.create(ExcelTransactionData);
        transaction_data.* = ExcelTransactionData{ .driver = self };

        return database.Transaction{
            .driver = createDriverInterface(self),
            .transaction_data = transaction_data,
        };
    }

    pub fn getConnectionInfo(self: *Self) database.ConnectionInfo {
        const file_path = self.file_path orelse "unknown";
        return database.ConnectionInfo{
            .database_type = .excel,
            .host = "duckdb",
            .database = file_path,
            .username = "",
            .connected_at = std.time.timestamp(),
            .server_version = "DuckDB + Spatial",
        };
    }

    fn substituteParameters(self: Self, sql: []const u8, args: []const types.Value) ![]const u8 {
        var result = try self.allocator.alloc(u8, sql.len * 2); // Estimate size
        var result_len: usize = 0;
        var param_idx: usize = 0;
        var i: usize = 0;

        while (i < sql.len) {
            if (i + 1 < sql.len and sql[i] == '?' and param_idx < args.len) {
                const arg = args[param_idx];
                const value_str = try self.valueToString(arg);
                defer self.allocator.free(value_str);

                if (result_len + value_str.len > result.len) {
                    // Resize result buffer
                    const new_result = try self.allocator.alloc(u8, result.len * 2);
                    @memcpy(new_result[0..result_len], result[0..result_len]);
                    self.allocator.free(result);
                    result = new_result;
                }

                @memcpy(result[result_len .. result_len + value_str.len], value_str);
                result_len += value_str.len;
                param_idx += 1;
                i += 1; // Skip '?'
            } else {
                if (result_len >= result.len) {
                    // Resize result buffer
                    const new_result = try self.allocator.alloc(u8, result.len * 2);
                    @memcpy(new_result[0..result_len], result[0..result_len]);
                    self.allocator.free(result);
                    result = new_result;
                }
                result[result_len] = sql[i];
                result_len += 1;
                i += 1;
            }
        }

        return result[0..result_len];
    }

    fn valueToString(self: Self, value: types.Value) ![]const u8 {
        return switch (value) {
            .null => "NULL",
            .boolean => |b| if (b) "TRUE" else "FALSE",
            .integer => |i| try std.fmt.allocPrint(self.allocator, "{}", .{i}),
            .float => |f| try std.fmt.allocPrint(self.allocator, "{}", .{f}),
            .text => |t| try std.fmt.allocPrint(self.allocator, "'{s}'", .{std.mem.replaceOwned(u8, self.allocator, t, "'", "''") catch t}),
            .date => |d| try std.fmt.allocPrint(self.allocator, "'{}'", .{d}),
            .time => |t| try std.fmt.allocPrint(self.allocator, "'{}'", .{t}),
            .timestamp => |ts| try std.fmt.allocPrint(self.allocator, "'{}'", .{ts}),
            .binary => |b| try std.fmt.allocPrint(self.allocator, "x'{any}'", .{b}),
        };
    }

    fn convertSql(self: Self, sql: []const u8) ![*:0]const u8 {
        // Simple SQL conversion - replace table references with excel_data
        // This is a basic implementation - a real one would need proper SQL parsing
        var converted = try self.allocator.alloc(u8, sql.len + 1);
        @memcpy(converted[0..sql.len], sql);
        converted[sql.len] = 0; // Null terminate

        // Replace "FROM <table>" with "FROM excel_data" (basic implementation)
        if (std.mem.indexOf(u8, converted, "FROM")) |pos| {
            const after_from = pos + 4;
            // Skip whitespace
            var start = after_from;
            while (start < converted.len and converted[start] == ' ') : (start += 1) {}

            // Find end of table name
            var end = start;
            while (end < converted.len and
                (std.ascii.isAlphanumeric(converted[end]) or converted[end] == '_' or converted[end] == '.')) : (end += 1)
            {}

            if (start < end) {
                // Replace with excel_data
                const new_sql = try std.fmt.allocPrint(self.allocator, "{s}FROM excel_data{s}\x00", .{ converted[0..after_from], converted[end..] });
                self.allocator.free(converted);
                return new_sql;
            }
        }

        return converted.ptr[0..sql.len :0];
    }

    fn convertResult(self: Self, result: *duckdb_result) !types.ResultSet {
        const column_count = duckdb_column_count(result);
        const row_count = duckdb_row_count(result);

        // Build columns
        var columns = try self.allocator.alloc(types.Column, column_count);
        errdefer self.allocator.free(columns);

        for (0..column_count) |col_idx| {
            const name_cstr = duckdb_column_name(result, col_idx);
            const name = try self.allocator.dupe(u8, std.mem.sliceTo(name_cstr, 0));

            const duckdb_type_val = duckdb_column_type(result, col_idx);
            const column_type = switch (duckdb_type_val) {
                .DUCKDB_TYPE_BOOLEAN => .boolean,
                .DUCKDB_TYPE_INTEGER, .DUCKDB_TYPE_BIGINT => .integer,
                .DUCKDB_TYPE_FLOAT, .DUCKDB_TYPE_DOUBLE => .float,
                .DUCKDB_TYPE_DATE, .DUCKDB_TYPE_TIMESTAMP => .date,
                else => .text,
            };

            columns[col_idx] = types.Column{
                .name = name,
                .type = column_type,
                .nullable = true,
            };
        }

        // Build rows
        var rows = try self.allocator.alloc(types.Row, row_count);
        errdefer self.allocator.free(rows);

        for (0..row_count) |row_idx| {
            var values = try self.allocator.alloc(types.Value, column_count);
            errdefer self.allocator.free(values);

            for (0..column_count) |col_idx| {
                if (duckdb_value_is_null(result, col_idx, row_idx)) {
                    values[col_idx] = .null;
                } else {
                    const duckdb_type_val = duckdb_column_type(result, col_idx);
                    values[col_idx] = switch (duckdb_type_val) {
                        .DUCKDB_TYPE_BOOLEAN => .{ .boolean = duckdb_value_boolean(result, col_idx, row_idx) },
                        .DUCKDB_TYPE_INTEGER, .DUCKDB_TYPE_BIGINT => .{ .integer = duckdb_value_int64(result, col_idx, row_idx) },
                        .DUCKDB_TYPE_FLOAT, .DUCKDB_TYPE_DOUBLE => .{ .float = duckdb_value_double(result, col_idx, row_idx) },
                        else => {
                            const text_cstr = duckdb_value_varchar(result, col_idx, row_idx);
                            const text = try self.allocator.dupe(u8, std.mem.sliceTo(text_cstr, 0));
                            .{ .text = text };
                        },
                    };
                }
            }

            rows[row_idx] = types.Row.init(self.allocator, values);
        }

        return types.ResultSet.init(self.allocator, columns, rows);
    }
};

// Excel connection data
const ExcelConnectionData = struct {
    driver: *ExcelDriver,
    file_path: []const u8,
};

// Excel transaction data
const ExcelTransactionData = struct {
    driver: *ExcelDriver,
};

// Create driver interface
fn createDriverInterface(excel_driver: *ExcelDriver) database.Driver {
    const driver_data = excel_driver;

    const vtable = database.Driver.VTable{
        .connect = connect_impl,
        .close = close_impl,
        .executeQuery = executeQuery_impl,
        .executeUpdate = executeUpdate_impl,
        .beginTransaction = beginTransaction_impl,
        .getConnectionInfo = getConnectionInfo_impl,
    };

    return database.Driver{
        .vtable = &vtable,
        .driver_data = driver_data,
    };
}

// Driver implementation functions
fn connect_impl(driver_data: *anyopaque, config: database.ConnectionConfig) errors.DatabaseError!database.Connection {
    const excel_driver = @as(*ExcelDriver, @ptrCast(@alignCast(driver_data)));
    return excel_driver.connect(config);
}

fn close_impl(driver_data: *anyopaque) void {
    const excel_driver = @as(*ExcelDriver, @ptrCast(@alignCast(driver_data)));
    excel_driver.deinit();
}

fn executeQuery_impl(driver_data: *anyopaque, sql: []const u8, args: []const types.Value) errors.DatabaseError!types.ResultSet {
    const excel_driver = @as(*ExcelDriver, @ptrCast(@alignCast(driver_data)));
    return excel_driver.executeQuery(sql, args);
}

fn executeUpdate_impl(driver_data: *anyopaque, sql: []const u8, args: []const types.Value) errors.DatabaseError!u64 {
    const excel_driver = @as(*ExcelDriver, @ptrCast(@alignCast(driver_data)));
    return excel_driver.executeUpdate(sql, args);
}

fn beginTransaction_impl(driver_data: *anyopaque) errors.DatabaseError!database.Transaction {
    const excel_driver = @as(*ExcelDriver, @ptrCast(@alignCast(driver_data)));
    return excel_driver.beginTransaction();
}

fn getConnectionInfo_impl(driver_data: *anyopaque) database.ConnectionInfo {
    const excel_driver = @as(*ExcelDriver, @ptrCast(@alignCast(driver_data)));
    return excel_driver.getConnectionInfo();
}

// Factory function
pub fn createDriver(allocator: std.mem.Allocator) database.Driver {
    const excel_driver = allocator.create(ExcelDriver) catch unreachable;
    excel_driver.* = ExcelDriver.init(allocator);
    return createDriverInterface(excel_driver);
}
