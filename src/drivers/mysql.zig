const std = @import("std");
const database = @import("../database.zig");
const types = @import("../utils/types.zig");
const errors = @import("../utils/errors.zig");
const library_manager = @import("../utils/library_manager.zig");

// MySQL C API types and functions
const MYSQL = opaque {};
const MYSQL_RES = opaque {};
const MYSQL_ROW = [*c]const [*c]const u8;
const MYSQL_FIELD = opaque {};
const MYSQL_STMT = opaque {};
const MYSQL_BIND = opaque {};

// MySQL result codes
const MYSQL_OK = 0;
const MYSQL_ERROR = 1;

// MySQL data types
const MYSQL_TYPE_DECIMAL = 0;
const MYSQL_TYPE_TINY = 1;
const MYSQL_TYPE_SHORT = 2;
const MYSQL_TYPE_LONG = 3;
const MYSQL_TYPE_FLOAT = 4;
const MYSQL_TYPE_DOUBLE = 5;
const MYSQL_TYPE_NULL = 6;
const MYSQL_TYPE_TIMESTAMP = 7;
const MYSQL_TYPE_LONGLONG = 8;
const MYSQL_TYPE_INT24 = 9;
const MYSQL_TYPE_DATE = 10;
const MYSQL_TYPE_TIME = 11;
const MYSQL_TYPE_DATETIME = 12;
const MYSQL_TYPE_YEAR = 13;
const MYSQL_TYPE_NEWDATE = 14;
const MYSQL_TYPE_VARCHAR = 15;
const MYSQL_TYPE_BIT = 16;
const MYSQL_TYPE_NEWDECIMAL = 246;
const MYSQL_TYPE_ENUM = 247;
const MYSQL_TYPE_SET = 248;
const MYSQL_TYPE_TINY_BLOB = 249;
const MYSQL_TYPE_MEDIUM_BLOB = 250;
const MYSQL_TYPE_LONG_BLOB = 251;
const MYSQL_TYPE_BLOB = 252;
const MYSQL_TYPE_VAR_STRING = 253;
const MYSQL_TYPE_STRING = 254;
const MYSQL_TYPE_GEOMETRY = 255;

// MySQL C functions
extern fn mysql_init(mysql: ?*MYSQL) ?*MYSQL;
extern fn mysql_real_connect(mysql: *MYSQL, host: [*c]const u8, user: [*c]const u8, passwd: [*c]const u8, db: [*c]const u8, port: c_uint, unix_socket: [*c]const u8, client_flag: c_ulong) ?*MYSQL;
extern fn mysql_close(mysql: *MYSQL) void;
extern fn mysql_query(mysql: *MYSQL, q: [*c]const u8) c_int;
extern fn mysql_real_query(mysql: *MYSQL, q: [*c]const u8, length: c_ulong) c_int;
extern fn mysql_store_result(mysql: *MYSQL) ?*MYSQL_RES;
extern fn mysql_use_result(mysql: *MYSQL) ?*MYSQL_RES;
extern fn mysql_free_result(result: *MYSQL_RES) void;
extern fn mysql_fetch_row(result: *MYSQL_RES) MYSQL_ROW;
extern fn mysql_fetch_fields(result: *MYSQL_RES) [*c]MYSQL_FIELD;
extern fn mysql_num_fields(result: *MYSQL_RES) c_uint;
extern fn mysql_num_rows(result: *MYSQL_RES) c_ulonglong;
extern fn mysql_affected_rows(mysql: *MYSQL) c_ulonglong;
extern fn mysql_error(mysql: *MYSQL) [*c]const u8;
extern fn mysql_errno(mysql: *MYSQL) c_uint;
extern fn mysql_get_server_info(mysql: *MYSQL) [*c]const u8;
extern fn mysql_autocommit(mysql: *MYSQL, mode: c_uint) bool;
extern fn mysql_commit(mysql: *MYSQL) bool;
extern fn mysql_rollback(mysql: *MYSQL) bool;
extern fn mysql_stmt_init(mysql: *MYSQL) ?*MYSQL_STMT;
extern fn mysql_stmt_prepare(stmt: *MYSQL_STMT, query: [*c]const u8, length: c_ulong) c_int;
extern fn mysql_stmt_execute(stmt: *MYSQL_STMT) c_int;
extern fn mysql_stmt_close(stmt: *MYSQL_STMT) bool;
extern fn mysql_stmt_bind_param(stmt: *MYSQL_STMT, bind: [*c]MYSQL_BIND) c_int;
extern fn mysql_stmt_bind_result(stmt: *MYSQL_STMT, bind: [*c]MYSQL_BIND) c_int;
extern fn mysql_stmt_fetch(stmt: *MYSQL_STMT) c_int;
extern fn mysql_stmt_store_result(stmt: *MYSQL_STMT) c_int;
extern fn mysql_stmt_affected_rows(stmt: *MYSQL_STMT) c_ulonglong;
extern fn mysql_stmt_error(stmt: *MYSQL_STMT) [*c]const u8;
extern fn mysql_stmt_errno(stmt: *MYSQL_STMT) c_uint;

// Real MySQL driver implementation
pub const MySQLDriver = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    mysql: ?*MYSQL = null,

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        if (self.mysql) |mysql| {
            mysql_close(mysql);
            self.mysql = null;
        }
    }

    pub fn connect(self: *Self, config: database.ConnectionConfig) errors.DatabaseError!database.Connection {
        // Ensure MySQL library is available
        library_manager.ensureLibrary(.mysql) catch |err| switch (err) {
            error.LibraryNotFound => {
                std.debug.print("❌ MySQL library not found. Please install mysql-client development package.\n", .{});
                return errors.DatabaseError.LibraryNotFound;
            },
            else => return err,
        };

        const host = config.host orelse "localhost";
        const port = config.port orelse 3306;
        const db_name = config.database orelse return errors.DatabaseError.InvalidConfiguration;
        const username = config.username orelse return errors.DatabaseError.InvalidConfiguration;
        const password = config.password orelse "";

        self.mysql = mysql_init(null) orelse return errors.DatabaseError.ConnectionFailed;

        const connection_result = mysql_real_connect(
            self.mysql.?,
            host,
            username,
            password,
            db_name,
            port,
            null,
            0,
        );

        if (connection_result == null) {
            const error_msg = mysql_error(self.mysql.?);
            std.debug.print("MySQL connection error: {s}\n", .{error_msg});
            return errors.DatabaseError.ConnectionFailed;
        }

        // Create connection object
        const conn_data = try self.allocator.create(MySQLConnectionData);
        conn_data.* = MySQLConnectionData{
            .driver = self,
            .host = host,
            .database = db_name,
            .username = username,
        };

        return database.Connection{
            .driver = createDriverInterface(self),
            .connection_data = conn_data,
        };
    }

    pub fn executeQuery(self: *Self, sql: []const u8, args: []const types.Value) errors.DatabaseError!types.ResultSet {
        if (self.mysql == null) return errors.DatabaseError.InvalidConnection;

        if (args.len > 0) {
            return self.executePreparedStatement(sql, args);
        } else {
            return self.executeSimpleQuery(sql);
        }
    }

    pub fn executeUpdate(self: *Self, sql: []const u8, args: []const types.Value) errors.DatabaseError!u64 {
        if (self.mysql == null) return errors.DatabaseError.InvalidConnection;

        if (args.len > 0) {
            return self.executeUpdatePreparedStatement(sql, args);
        } else {
            return self.executeUpdateSimple(sql);
        }
    }

    pub fn beginTransaction(self: *Self) errors.DatabaseError!database.Transaction {
        if (self.mysql == null) return errors.DatabaseError.InvalidConnection;

        // Disable autocommit to start transaction
        const autocommit_result = mysql_autocommit(self.mysql.?, 0);
        if (!autocommit_result) {
            const error_msg = mysql_error(self.mysql.?);
            std.debug.print("MySQL begin transaction error: {s}\n", .{error_msg});
            return errors.DatabaseError.TransactionFailed;
        }

        const transaction_data = try self.allocator.create(MySQLTransactionData);
        transaction_data.* = MySQLTransactionData{ .driver = self };

        return database.Transaction{
            .driver = createDriverInterface(self),
            .transaction_data = transaction_data,
        };
    }

    pub fn commitTransaction(self: *Self) !void {
        if (self.mysql == null) return errors.DatabaseError.InvalidConnection;

        const commit_result = mysql_commit(self.mysql.?);
        if (!commit_result) {
            const error_msg = mysql_error(self.mysql.?);
            std.debug.print("MySQL commit error: {s}\n", .{error_msg});
            return errors.DatabaseError.TransactionFailed;
        }

        // Re-enable autocommit
        _ = mysql_autocommit(self.mysql.?, 1);
    }

    pub fn rollbackTransaction(self: *Self) !void {
        if (self.mysql == null) return errors.DatabaseError.InvalidConnection;

        const rollback_result = mysql_rollback(self.mysql.?);
        if (!rollback_result) {
            const error_msg = mysql_error(self.mysql.?);
            std.debug.print("MySQL rollback error: {s}\n", .{error_msg});
            return errors.DatabaseError.TransactionFailed;
        }

        // Re-enable autocommit
        _ = mysql_autocommit(self.mysql.?, 1);
    }

    pub fn getConnectionInfo(self: *Self) database.ConnectionInfo {
        const host = "localhost";
        const db = "unknown";
        const username = "unknown";
        const server_version = if (self.mysql) |mysql|
            std.mem.sliceTo(mysql_get_server_info(mysql), 0)
        else
            "Unknown";

        return database.ConnectionInfo{
            .database_type = .mysql,
            .host = host,
            .database = db,
            .username = username,
            .connected_at = std.time.timestamp(),
            .server_version = server_version,
        };
    }

    fn executeSimpleQuery(self: *Self, sql: []const u8) !types.ResultSet {
        const sql_cstr = try std.cstr.addNullByte(self.allocator, sql);
        defer self.allocator.free(sql_cstr);

        const query_result = mysql_query(self.mysql.?, sql_cstr.ptr);
        if (query_result != 0) {
            const error_msg = mysql_error(self.mysql.?);
            std.debug.print("MySQL query error: {s}\n", .{error_msg});
            return errors.DatabaseError.QueryFailed;
        }

        const result = mysql_store_result(self.mysql.?) orelse {
            // No result set (e.g., for INSERT/UPDATE/DELETE)
            return types.ResultSet.init(self.allocator, &[_]types.Column{}, &[_]types.Row{});
        };
        defer mysql_free_result(result);

        return self.buildResultSet(result);
    }

    fn executePreparedStatement(self: *Self, sql: []const u8, args: []const types.Value) !types.ResultSet {
        const stmt = mysql_stmt_init(self.mysql.?) orelse return errors.DatabaseError.QueryFailed;
        defer mysql_stmt_close(stmt);

        const sql_cstr = try std.cstr.addNullByte(self.allocator, sql);
        defer self.allocator.free(sql_cstr);

        const prepare_result = mysql_stmt_prepare(stmt, sql_cstr.ptr, sql_cstr.len - 1);
        if (prepare_result != 0) {
            const error_msg = mysql_stmt_error(stmt);
            std.debug.print("MySQL prepare error: {s}\n", .{error_msg});
            return errors.DatabaseError.QueryFailed;
        }

        // Bind parameters using native MYSQL_BIND structures
        if (args.len > 0) {
            try self.bindMySQLParameters(stmt, args);
        }

        const execute_result = mysql_stmt_execute(stmt);
        if (execute_result != 0) {
            const error_msg = mysql_stmt_error(stmt);
            std.debug.print("MySQL execute error: {s}\n", .{error_msg});
            return errors.DatabaseError.QueryFailed;
        }

        _ = mysql_stmt_store_result(stmt);
        return self.buildPreparedStatementResult(stmt);
    }

    fn bindMySQLParameters(self: *Self, stmt: *MYSQL_STMT, args: []const types.Value) !void {
        // Create MYSQL_BIND array for parameters
        const binds = try self.allocator.alloc(MYSQL_BIND, args.len);
        defer self.allocator.free(binds);

        // Create storage for parameter data
        var param_data = try self.allocator.alloc([64]u8, args.len);
        defer self.allocator.free(param_data);

        var param_lengths = try self.allocator.alloc(c_ulong, args.len);
        defer self.allocator.free(param_lengths);

        for (args, 0..) |arg, i| {
            std.mem.set(u8, &param_data[i], 0);
            binds[i] = std.mem.zeroes(MYSQL_BIND);

            switch (arg) {
                .null => {
                    binds[i].buffer_type = MYSQL_TYPE_NULL;
                    binds[i].is_null = &[_]bool{true};
                },
                .boolean => |b| {
                    binds[i].buffer_type = MYSQL_TYPE_TINY;
                    binds[i].buffer = &param_data[i];
                    param_data[i][0] = @intCast(b);
                    binds[i].length = &param_lengths[i];
                    param_lengths[i] = 1;
                },
                .integer => |int_val| {
                    binds[i].buffer_type = MYSQL_TYPE_LONGLONG;
                    binds[i].buffer = &param_data[i];
                    std.mem.copy(u8, &param_data[i], std.mem.asBytes(&int_val));
                    binds[i].length = &param_lengths[i];
                    param_lengths[i] = 8;
                },
                .float => |float_val| {
                    binds[i].buffer_type = MYSQL_TYPE_DOUBLE;
                    binds[i].buffer = &param_data[i];
                    std.mem.copy(u8, &param_data[i], std.mem.asBytes(&float_val));
                    binds[i].length = &param_lengths[i];
                    param_lengths[i] = 8;
                },
                .text => |text_val| {
                    binds[i].buffer_type = MYSQL_TYPE_VAR_STRING;
                    binds[i].buffer = text_val.ptr;
                    binds[i].length = &param_lengths[i];
                    param_lengths[i] = @intCast(text_val.len);
                },
                .date => |date_val| {
                    binds[i].buffer_type = MYSQL_TYPE_DATE;
                    binds[i].buffer = date_val.ptr;
                    binds[i].length = &param_lengths[i];
                    param_lengths[i] = @intCast(date_val.len);
                },
                .time => |time_val| {
                    binds[i].buffer_type = MYSQL_TYPE_TIME;
                    binds[i].buffer = time_val.ptr;
                    binds[i].length = &param_lengths[i];
                    param_lengths[i] = @intCast(time_val.len);
                },
                .timestamp => |ts_val| {
                    binds[i].buffer_type = MYSQL_TYPE_TIMESTAMP;
                    binds[i].buffer = ts_val.ptr;
                    binds[i].length = &param_lengths[i];
                    param_lengths[i] = @intCast(ts_val.len);
                },
                .binary => |binary_val| {
                    binds[i].buffer_type = MYSQL_TYPE_BLOB;
                    binds[i].buffer = binary_val.ptr;
                    binds[i].length = &param_lengths[i];
                    param_lengths[i] = @intCast(binary_val.len);
                },
            }
        }

        const bind_result = mysql_stmt_bind_param(stmt, binds.ptr);
        if (bind_result != 0) {
            const error_msg = mysql_stmt_error(stmt);
            std.debug.print("MySQL bind error: {s}\n", .{error_msg});
            return errors.DatabaseError.QueryFailed;
        }
    }

    fn executeUpdateSimple(self: *Self, sql: []const u8) !u64 {
        const sql_cstr = try std.cstr.addNullByte(self.allocator, sql);
        defer self.allocator.free(sql_cstr);

        const query_result = mysql_query(self.mysql.?, sql_cstr.ptr);
        if (query_result != 0) {
            const error_msg = mysql_error(self.mysql.?);
            std.debug.print("MySQL update error: {s}\n", .{error_msg});
            return errors.DatabaseError.QueryFailed;
        }

        const affected_rows = mysql_affected_rows(self.mysql.?);
        return @intCast(affected_rows);
    }

    fn executeUpdatePreparedStatement(self: *Self, sql: []const u8, args: []const types.Value) !u64 {
        // For now, use simple query with parameter substitution
        return self.executeUpdateSimpleWithParams(sql, args);
    }

    fn executeSimpleQueryWithParams(self: *Self, sql: []const u8, args: []const types.Value) !types.ResultSet {
        const formatted_sql = try self.formatSqlWithParams(sql, args);
        defer self.allocator.free(formatted_sql);
        return self.executeSimpleQuery(formatted_sql);
    }

    fn executeUpdateSimpleWithParams(self: *Self, sql: []const u8, args: []const types.Value) !u64 {
        const formatted_sql = try self.formatSqlWithParams(sql, args);
        defer self.allocator.free(formatted_sql);
        return self.executeUpdateSimple(formatted_sql);
    }

    fn formatSqlWithParams(self: *Self, sql: []const u8, args: []const types.Value) ![]u8 {
        var result = std.ArrayList(u8).init(self.allocator);
        errdefer result.deinit();

        var param_index: usize = 0;
        var i: usize = 0;

        while (i < sql.len) {
            if (i + 1 < sql.len and sql[i] == '?' and sql[i + 1] != '?') {
                if (param_index >= args.len) {
                    return errors.DatabaseError.QueryFailed;
                }

                try self.appendValue(&result, args[param_index]);
                param_index += 1;
                i += 1;
            } else {
                try result.append(sql[i]);
                i += 1;
            }
        }

        return result.toOwnedSlice();
    }

    fn appendValue(_: *Self, result: *std.ArrayList(u8), value: types.Value) !void {
        switch (value) {
            .null => try result.appendSlice("NULL"),
            .boolean => |b| try result.appendSlice(if (b) "TRUE" else "FALSE"),
            .integer => |i| try std.fmt.format(result.writer(), "{}", .{i}),
            .float => |f| try std.fmt.format(result.writer(), "{}", .{f}),
            .text => |t| {
                try result.append('\'');
                for (t) |char| {
                    if (char == '\'') {
                        try result.appendSlice("''");
                    } else {
                        try result.append(char);
                    }
                }
                try result.append('\'');
            },
            .date => |d| try std.fmt.format(result.writer(), "'{}'", .{d}),
            .time => |t| try std.fmt.format(result.writer(), "'{}'", .{t}),
            .timestamp => |ts| try std.fmt.format(result.writer(), "'{}'", .{ts}),
            .binary => |b| {
                try result.appendSlice("x'");
                for (b) |byte| {
                    try std.fmt.format(result.writer(), "{x:0>2}", .{byte});
                }
                try result.append('\'');
            },
        }
    }

    fn buildResultSet(self: *Self, result: *MYSQL_RES) !types.ResultSet {
        const field_count = mysql_num_fields(result);
        const row_count = mysql_num_rows(result);

        // Build columns
        var columns = try self.allocator.alloc(types.Column, @intCast(field_count));
        errdefer self.allocator.free(columns);

        _ = mysql_fetch_fields(result);
        for (0..field_count) |i| {
            // Simplified - would need proper field info extraction
            const name = try std.fmt.allocPrint(self.allocator, "col{}", .{i});
            columns[i] = types.Column{
                .name = name,
                .type = .text, // Default to text for simplicity
                .nullable = true,
            };
        }

        // Build rows
        var rows = try self.allocator.alloc(types.Row, @intCast(row_count));
        errdefer self.allocator.free(rows);

        for (0..row_count) |row_idx| {
            const mysql_row = mysql_fetch_row(result);
            if (mysql_row == null) break;

            var values = try self.allocator.alloc(types.Value, @intCast(field_count));
            errdefer self.allocator.free(values);

            for (0..field_count) |col_idx| {
                const field_value = mysql_row[col_idx];
                if (field_value) |val| {
                    const text = try self.allocator.dupe(u8, std.mem.sliceTo(val, 0));
                    values[col_idx] = .{ .text = text };
                } else {
                    values[col_idx] = .null;
                }
            }

            rows[row_idx] = types.Row.init(self.allocator, values);
        }

        return types.ResultSet.init(self.allocator, columns, rows);
    }

    fn buildPreparedStatementResult(self: *Self, _: *MYSQL_STMT) !types.ResultSet {
        // Simplified implementation
        return types.ResultSet.init(self.allocator, &[_]types.Column{}, &[_]types.Row{});
    }
};

// MySQL connection data
const MySQLConnectionData = struct {
    driver: *MySQLDriver,
    host: []const u8,
    database: []const u8,
    username: []const u8,
};

// MySQL transaction data
const MySQLTransactionData = struct {
    driver: *MySQLDriver,
};

// Create driver interface
fn createDriverInterface(mysql_driver: *MySQLDriver) database.Driver {
    const driver_data = mysql_driver;

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
    const mysql_driver = @as(*MySQLDriver, @ptrCast(@alignCast(driver_data)));
    return mysql_driver.connect(config);
}

fn close_impl(driver_data: *anyopaque) void {
    const mysql_driver = @as(*MySQLDriver, @ptrCast(@alignCast(driver_data)));
    mysql_driver.deinit();
}

fn executeQuery_impl(driver_data: *anyopaque, sql: []const u8, args: []const types.Value) errors.DatabaseError!types.ResultSet {
    const mysql_driver = @as(*MySQLDriver, @ptrCast(@alignCast(driver_data)));
    return mysql_driver.executeQuery(sql, args);
}

fn executeUpdate_impl(driver_data: *anyopaque, sql: []const u8, args: []const types.Value) errors.DatabaseError!u64 {
    const mysql_driver = @as(*MySQLDriver, @ptrCast(@alignCast(driver_data)));
    return mysql_driver.executeUpdate(sql, args);
}

fn beginTransaction_impl(driver_data: *anyopaque) errors.DatabaseError!database.Transaction {
    const mysql_driver = @as(*MySQLDriver, @ptrCast(@alignCast(driver_data)));
    return mysql_driver.beginTransaction();
}

fn getConnectionInfo_impl(driver_data: *anyopaque) database.ConnectionInfo {
    const mysql_driver = @as(*MySQLDriver, @ptrCast(@alignCast(driver_data)));
    return mysql_driver.getConnectionInfo();
}

// Factory function
pub fn createDriver(allocator: std.mem.Allocator) database.Driver {
    const mysql_driver = allocator.create(MySQLDriver) catch unreachable;
    mysql_driver.* = MySQLDriver.init(allocator);
    return createDriverInterface(mysql_driver);
}
