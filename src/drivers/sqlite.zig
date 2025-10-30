const std = @import("std");
const database = @import("../database.zig");
const types = @import("../utils/types.zig");
const errors = @import("../utils/errors.zig");
const library_manager = @import("../utils/library_manager.zig");

// SQLite C API types and functions
const sqlite3 = opaque {};
const sqlite3_stmt = opaque {};
const sqlite3_value = opaque {};

const sqlite3_destructor_type = *const fn (?*anyopaque) callconv(.c) void;

// SQLite result codes
const SQLITE_OK = 0;
const SQLITE_ERROR = 1;
const SQLITE_BUSY = 5;
const SQLITE_NOMEM = 7;
const SQLITE_READONLY = 8;
const SQLITE_INTERRUPT = 9;
const SQLITE_CANTOPEN = 14;
const SQLITE_CONSTRAINT = 19;
const SQLITE_MISMATCH = 20;
const SQLITE_MISUSE = 21;
const SQLITE_DONE = 101;
const SQLITE_ROW = 100;

// SQLite data types
const SQLITE_INTEGER = 1;
const SQLITE_FLOAT = 2;
const SQLITE_TEXT = 3;
const SQLITE_BLOB = 4;
const SQLITE_NULL = 5;

// SQLite C functions
extern fn sqlite3_open(filename: [*:0]const u8, ppDb: *?*sqlite3) c_int;
extern fn sqlite3_close(db: *sqlite3) c_int;
extern fn sqlite3_prepare_v2(db: *sqlite3, zSql: [*:0]const u8, nByte: c_int, ppStmt: *?*sqlite3_stmt, pzTail: *?[*:0]const u8) c_int;
extern fn sqlite3_finalize(pStmt: *sqlite3_stmt) c_int;
extern fn sqlite3_step(pStmt: *sqlite3_stmt) c_int;
extern fn sqlite3_reset(pStmt: *sqlite3_stmt) c_int;
extern fn sqlite3_bind_null(pStmt: *sqlite3_stmt, i: c_int) c_int;
extern fn sqlite3_bind_int(pStmt: *sqlite3_stmt, i: c_int, value: c_int) c_int;
extern fn sqlite3_bind_int64(pStmt: *sqlite3_stmt, i: c_int, value: i64) c_int;
extern fn sqlite3_bind_double(pStmt: *sqlite3_stmt, i: c_int, value: f64) c_int;
extern fn sqlite3_bind_text(pStmt: *sqlite3_stmt, i: c_int, value: [*:0]const u8, n: c_int, destructor: sqlite3_destructor_type) c_int;
extern fn sqlite3_bind_blob(pStmt: *sqlite3_stmt, i: c_int, value: ?*const anyopaque, n: c_int, destructor: sqlite3_destructor_type) c_int;
extern fn sqlite3_column_count(pStmt: *sqlite3_stmt) c_int;
extern fn sqlite3_column_name(pStmt: *sqlite3_stmt, N: c_int) [*:0]const u8;
extern fn sqlite3_column_type(pStmt: *sqlite3_stmt, iCol: c_int) c_int;
extern fn sqlite3_column_int(pStmt: *sqlite3_stmt, iCol: c_int) c_int;
extern fn sqlite3_column_int64(pStmt: *sqlite3_stmt, iCol: c_int) i64;
extern fn sqlite3_column_double(pStmt: *sqlite3_stmt, iCol: c_int) f64;
extern fn sqlite3_column_text(pStmt: *sqlite3_stmt, iCol: c_int) [*:0]const u8;
extern fn sqlite3_column_blob(pStmt: *sqlite3_stmt, iCol: c_int) ?*const anyopaque;
extern fn sqlite3_column_bytes(pStmt: *sqlite3_stmt, iCol: c_int) c_int;
extern fn sqlite3_changes(db: *sqlite3) c_int;
extern fn sqlite3_errmsg(db: *sqlite3) [*:0]const u8;
extern fn sqlite3_exec(db: *sqlite3, sql: [*:0]const u8, callback: ?*anyopaque, arg: ?*anyopaque, errmsg: *?[*:0]u8) c_int;
extern fn sqlite3_free(p: ?*anyopaque) void;

// Real SQLite driver implementation
pub const SQLiteDriver = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    db: ?*sqlite3 = null,

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        if (self.db) |db| {
            _ = sqlite3_close(db);
            self.db = null;
        }
    }

    pub fn connect(self: *Self, config: database.ConnectionConfig) errors.DatabaseError!database.Connection {
        // Ensure SQLite library is available
        library_manager.ensureLibrary(.sqlite) catch |err| switch (err) {
            error.LibraryNotFound => {
                std.debug.print("❌ SQLite library not found. Please install sqlite3 development package.\n", .{});
                return errors.DatabaseError.LibraryNotFound;
            },
            else => return errors.DatabaseError.LibraryNotFound,
        };

        const file_path_slice = config.file_path orelse return errors.DatabaseError.InvalidConfiguration;

        // Create null-terminated file path
        var file_path_cstr = try self.allocator.allocSentinel(u8, file_path_slice.len + 1, 0);
        defer self.allocator.free(file_path_cstr);
        @memcpy(file_path_cstr[0..file_path_slice.len], file_path_slice);

        var db: ?*sqlite3 = null;
        const result = sqlite3_open(file_path_cstr, &db);
        if (result != SQLITE_OK) {
            if (db) |opened_db| {
                const error_msg = sqlite3_errmsg(opened_db);
                std.debug.print("SQLite error: {s}\n", .{error_msg});
                _ = sqlite3_close(opened_db);
            }
            return errors.DatabaseError.ConnectionFailed;
        }

        self.db = db.?;

        // Enable foreign keys
        var errmsg: ?[*:0]u8 = null;
        _ = sqlite3_exec(self.db.?, "PRAGMA foreign_keys = ON", null, null, &errmsg);
        if (errmsg) |msg| {
            sqlite3_free(msg);
        }

        // Create connection object
        const conn_data = try self.allocator.create(SQLiteConnectionData);
        conn_data.* = SQLiteConnectionData{
            .driver = self,
            .file_path = file_path_slice,
        };

        return database.Connection{
            .driver = createDriverInterface(self),
            .connection_data = conn_data,
        };
    }

    pub fn executeQuery(self: *Self, sql: []const u8, args: []const types.Value) errors.DatabaseError!types.ResultSet {
        if (self.db == null) return errors.DatabaseError.InvalidConnection;

        var sql_cstr = try self.allocator.allocSentinel(u8, sql.len + 1, 0);
        defer self.allocator.free(sql_cstr);
        @memcpy(sql_cstr[0..sql.len], sql);

        var stmt: ?*sqlite3_stmt = null;
        var tail: ?[*:0]const u8 = null;
        const prepare_result = sqlite3_prepare_v2(self.db.?, sql_cstr, -1, &stmt, &tail);
        if (prepare_result != SQLITE_OK) {
            const error_msg = sqlite3_errmsg(self.db.?);
            std.debug.print("SQLite prepare error: {s}\n", .{error_msg});
            return errors.DatabaseError.QueryFailed;
        }
        defer _ = sqlite3_finalize(stmt.?);

        // Bind parameters
        for (args, 0..) |arg, i| {
            const bind_result = try self.bindParameter(stmt.?, @intCast(i + 1), arg);
            if (bind_result != SQLITE_OK) {
                const error_msg = sqlite3_errmsg(self.db.?);
                std.debug.print("SQLite bind error: {s}\n", .{error_msg});
                return errors.DatabaseError.QueryFailed;
            }
        }

        return self.executeStatement(stmt.?);
    }

    pub fn executeUpdate(self: *Self, sql: []const u8, args: []const types.Value) errors.DatabaseError!u64 {
        if (self.db == null) return errors.DatabaseError.InvalidConnection;

        var sql_cstr = try self.allocator.allocSentinel(u8, sql.len + 1, 0);
        defer self.allocator.free(sql_cstr);
        @memcpy(sql_cstr[0..sql.len], sql);

        var stmt: ?*sqlite3_stmt = null;
        var tail: ?[*:0]const u8 = null;
        const prepare_result = sqlite3_prepare_v2(self.db.?, sql_cstr, -1, &stmt, &tail);
        if (prepare_result != SQLITE_OK) {
            const error_msg = sqlite3_errmsg(self.db.?);
            std.debug.print("SQLite prepare error: {s}\n", .{error_msg});
            return errors.DatabaseError.QueryFailed;
        }
        defer _ = sqlite3_finalize(stmt.?);

        // Bind parameters
        for (args, 0..) |arg, i| {
            const bind_result = try self.bindParameter(stmt.?, @intCast(i + 1), arg);
            if (bind_result != SQLITE_OK) {
                const error_msg = sqlite3_errmsg(self.db.?);
                std.debug.print("SQLite bind error: {s}\n", .{error_msg});
                return errors.DatabaseError.QueryFailed;
            }
        }

        // Execute statement
        const step_result = sqlite3_step(stmt.?);
        if (step_result != SQLITE_DONE and step_result != SQLITE_ROW) {
            const error_msg = sqlite3_errmsg(self.db.?);
            std.debug.print("SQLite step error: {s}\n", .{error_msg});
            return errors.DatabaseError.QueryFailed;
        }

        const affected_rows = sqlite3_changes(self.db.?);
        return @intCast(affected_rows);
    }

    pub fn beginTransaction(self: *Self) errors.DatabaseError!database.Transaction {
        if (self.db == null) return errors.DatabaseError.InvalidConnection;

        // Start transaction
        var errmsg: ?[*:0]u8 = null;
        const result = sqlite3_exec(self.db.?, "BEGIN TRANSACTION", null, null, &errmsg);
        if (result != SQLITE_OK) {
            const error_msg = sqlite3_errmsg(self.db.?);
            std.debug.print("SQLite begin transaction error: {s}\n", .{error_msg});
            if (errmsg) |msg| {
                std.debug.print("SQLite exec error: {s}\n", .{msg});
                sqlite3_free(msg);
            }
            return errors.DatabaseError.TransactionFailed;
        }

        const transaction_data = try self.allocator.create(SQLiteTransactionData);
        transaction_data.* = SQLiteTransactionData{ .driver = self };

        return database.Transaction{
            .driver = createDriverInterface(self),
            .transaction_data = transaction_data,
        };
    }

    pub fn commitTransaction(self: *Self) !void {
        if (self.db == null) return errors.DatabaseError.InvalidConnection;

        var errmsg: ?[*:0]u8 = null;
        const result = sqlite3_exec(self.db.?, "COMMIT", null, null, &errmsg);
        if (result != SQLITE_OK) {
            const error_msg = sqlite3_errmsg(self.db.?);
            std.debug.print("SQLite commit error: {s}\n", .{error_msg});
            if (errmsg) |msg| {
                std.debug.print("SQLite exec error: {s}\n", .{msg});
                sqlite3_free(msg);
            }
            return errors.DatabaseError.TransactionFailed;
        }
    }

    pub fn rollbackTransaction(self: *Self) !void {
        if (self.db == null) return errors.DatabaseError.InvalidConnection;

        var errmsg: ?[*:0]u8 = null;
        const result = sqlite3_exec(self.db.?, "ROLLBACK", null, null, &errmsg);
        if (result != SQLITE_OK) {
            const error_msg = sqlite3_errmsg(self.db.?);
            std.debug.print("SQLite rollback error: {s}\n", .{error_msg});
            if (errmsg) |msg| {
                std.debug.print("SQLite exec error: {s}\n", .{msg});
                sqlite3_free(msg);
            }
            return errors.DatabaseError.TransactionFailed;
        }
    }

    pub fn getConnectionInfo(_: *Self) database.ConnectionInfo {
        const file_path = "database.db"; // Default if not set
        return database.ConnectionInfo{
            .database_type = .sqlite,
            .host = "file",
            .database = file_path,
            .username = "",
            .connected_at = std.time.timestamp(),
            .server_version = "SQLite",
        };
    }

    fn bindParameter(self: Self, stmt: *sqlite3_stmt, param_index: c_int, value: types.Value) !c_int {
        return switch (value) {
            .null => sqlite3_bind_null(stmt, param_index),
            .boolean => |b| sqlite3_bind_int(stmt, param_index, @intFromBool(b)),
            .integer => |i| sqlite3_bind_int64(stmt, param_index, i),
            .float => |f| sqlite3_bind_double(stmt, param_index, f),
            .text => |t| {
                var text_cstr = try self.allocator.allocSentinel(u8, t.len + 1, 0);
                defer self.allocator.free(text_cstr);
                @memcpy(text_cstr[0..t.len], t);
                return sqlite3_bind_text(stmt, param_index, text_cstr, -1, sqlite3_free);
            },
            .date => |d| {
                const date_str = try std.fmt.allocPrint(self.allocator, "{}", .{d});
                defer self.allocator.free(date_str);
                var date_cstr = try self.allocator.allocSentinel(u8, date_str.len + 1, 0);
                defer self.allocator.free(date_cstr);
                @memcpy(date_cstr[0..date_str.len], date_str);
                return sqlite3_bind_text(stmt, param_index, date_cstr, -1, sqlite3_free);
            },
            .time => |t| {
                const time_str = try std.fmt.allocPrint(self.allocator, "{}", .{t});
                defer self.allocator.free(time_str);
                var time_cstr = try self.allocator.allocSentinel(u8, time_str.len + 1, 0);
                defer self.allocator.free(time_cstr);
                @memcpy(time_cstr[0..time_str.len], time_str);
                return sqlite3_bind_text(stmt, param_index, time_cstr, -1, sqlite3_free);
            },
            .timestamp => |ts| {
                const ts_str = try std.fmt.allocPrint(self.allocator, "{}", .{ts});
                defer self.allocator.free(ts_str);
                var ts_cstr = try self.allocator.allocSentinel(u8, ts_str.len + 1, 0);
                defer self.allocator.free(ts_cstr);
                @memcpy(ts_cstr[0..ts_str.len], ts_str);
                return sqlite3_bind_text(stmt, param_index, ts_cstr, -1, sqlite3_free);
            },
            .binary => |b| {
                return sqlite3_bind_blob(stmt, param_index, b.ptr, @intCast(b.len), sqlite3_free);
            },
        };
    }

    fn executeStatement(self: Self, stmt: *sqlite3_stmt) !types.ResultSet {
        const column_count = sqlite3_column_count(stmt);

        // Build columns
        var columns = try self.allocator.alloc(types.Column, @intCast(column_count));
        errdefer self.allocator.free(columns);

        const col_count = @as(usize, @intCast(column_count));
        for (0..col_count) |col_idx| {
            const name_cstr = sqlite3_column_name(stmt, @intCast(col_idx));
            const name = try self.allocator.dupe(u8, std.mem.sliceTo(name_cstr, 0));

            const sqlite_type = sqlite3_column_type(stmt, @intCast(col_idx));
            const column_type: types.ValueType = blk: {
                switch (sqlite_type) {
                    SQLITE_INTEGER => break :blk .integer,
                    SQLITE_FLOAT => break :blk .float,
                    SQLITE_TEXT => break :blk .text,
                    SQLITE_BLOB => break :blk .binary,
                    SQLITE_NULL => break :blk .text,
                    else => break :blk .text,
                }
            };

            columns[col_idx] = types.Column{
                .name = name,
                .type = column_type,
                .nullable = true,
            };
        }

        // Build rows
        var rows = std.ArrayList(types.Row).initCapacity(self.allocator, 0) catch unreachable;
        errdefer rows.deinit(self.allocator);

        while (true) {
            const step_result = sqlite3_step(stmt);
            if (step_result == SQLITE_DONE) {
                break;
            } else if (step_result != SQLITE_ROW) {
                const error_msg = sqlite3_errmsg(self.db.?);
                std.debug.print("SQLite step error: {s}\n", .{error_msg});
                return errors.DatabaseError.QueryFailed;
            }

            var values = try self.allocator.alloc(types.Value, @intCast(column_count));
            errdefer self.allocator.free(values);

            for (0..@as(usize, @intCast(column_count))) |col_idx| {
                const sqlite_type = sqlite3_column_type(stmt, @intCast(col_idx));
                values[col_idx] = switch (sqlite_type) {
                    SQLITE_NULL => .null,
                    SQLITE_INTEGER => .{ .integer = sqlite3_column_int64(stmt, @intCast(col_idx)) },
                    SQLITE_FLOAT => .{ .float = sqlite3_column_double(stmt, @intCast(col_idx)) },
                    SQLITE_TEXT => blk: {
                        const text_cstr = sqlite3_column_text(stmt, @intCast(col_idx));
                        const text = try self.allocator.dupe(u8, std.mem.sliceTo(text_cstr, 0));
                        break :blk .{ .text = text };
                    },
                    SQLITE_BLOB => blk: {
                        const blob_ptr = sqlite3_column_blob(stmt, @intCast(col_idx));
                        const blob_len = sqlite3_column_bytes(stmt, @intCast(col_idx));
                        if (blob_ptr != null and blob_len > 0) {
                            const blob_data = try self.allocator.dupe(u8, @as([*]const u8, @ptrCast(blob_ptr))[0..@intCast(blob_len)]);
                            break :blk .{ .binary = blob_data };
                        } else {
                            break :blk .{ .binary = &[_]u8{} };
                        }
                    },
                    else => .null,
                };
            }

            try rows.append(self.allocator, types.Row.init(self.allocator, values));
        }

        return types.ResultSet.init(self.allocator, columns, try rows.toOwnedSlice(self.allocator));
    }
};

// SQLite connection data
const SQLiteConnectionData = struct {
    driver: *SQLiteDriver,
    file_path: []const u8,
};

// SQLite transaction data
const SQLiteTransactionData = struct {
    driver: *SQLiteDriver,
};

// Create driver interface
fn createDriverInterface(sqlite_driver: *SQLiteDriver) database.Driver {
    const driver_data = sqlite_driver;

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
    const sqlite_driver = @as(*SQLiteDriver, @ptrCast(@alignCast(driver_data)));
    return sqlite_driver.connect(config);
}

fn close_impl(driver_data: *anyopaque) void {
    const sqlite_driver = @as(*SQLiteDriver, @ptrCast(@alignCast(driver_data)));
    sqlite_driver.deinit();
}

fn executeQuery_impl(driver_data: *anyopaque, sql: []const u8, args: []const types.Value) errors.DatabaseError!types.ResultSet {
    const sqlite_driver = @as(*SQLiteDriver, @ptrCast(@alignCast(driver_data)));
    return sqlite_driver.executeQuery(sql, args);
}

fn executeUpdate_impl(driver_data: *anyopaque, sql: []const u8, args: []const types.Value) errors.DatabaseError!u64 {
    const sqlite_driver = @as(*SQLiteDriver, @ptrCast(@alignCast(driver_data)));
    return sqlite_driver.executeUpdate(sql, args);
}

fn beginTransaction_impl(driver_data: *anyopaque) errors.DatabaseError!database.Transaction {
    const sqlite_driver = @as(*SQLiteDriver, @ptrCast(@alignCast(driver_data)));
    return sqlite_driver.beginTransaction();
}

fn getConnectionInfo_impl(driver_data: *anyopaque) database.ConnectionInfo {
    const sqlite_driver = @as(*SQLiteDriver, @ptrCast(@alignCast(driver_data)));
    return sqlite_driver.getConnectionInfo();
}

// Factory function
pub fn createDriver(allocator: std.mem.Allocator) database.Driver {
    const sqlite_driver = allocator.create(SQLiteDriver) catch unreachable;
    sqlite_driver.* = SQLiteDriver.init(allocator);
    return createDriverInterface(sqlite_driver);
}
