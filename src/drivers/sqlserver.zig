const std = @import("std");
const database = @import("../database.zig");
const types = @import("../utils/types.zig");
const errors = @import("../utils/errors.zig");
const library_manager = @import("../utils/library_manager.zig");

// ODBC C API types and functions
const SQLHANDLE = *anyopaque;
const SQLHENV = SQLHANDLE;
const SQLHDBC = SQLHANDLE;
const SQLHSTMT = SQLHANDLE;
const SQLHDESC = SQLHANDLE;

const SQLRETURN = c_short;
const SQLSMALLINT = c_short;
const SQLUSMALLINT = c_ushort;
const SQLINTEGER = c_int;
const SQLUINTEGER = c_uint;
const SQLPOINTER = *anyopaque;
const SQLLEN = c_longlong;
const SQLULEN = c_ulonglong;
const SQLCHAR = u8;
const SQLWCHAR = u16;

// ODBC return codes
const SQL_SUCCESS = 0;
const SQL_SUCCESS_WITH_INFO = 1;
const SQL_NO_DATA = 100;
const SQL_ERROR = -1;
const SQL_INVALID_HANDLE = -2;

// ODBC data types
const SQL_CHAR = 1;
const SQL_VARCHAR = 12;
const SQL_LONGVARCHAR = -1;
const SQL_WCHAR = -8;
const SQL_WVARCHAR = -9;
const SQL_WLONGVARCHAR = -10;
const SQL_DECIMAL = 3;
const SQL_NUMERIC = 2;
const SQL_SMALLINT = 5;
const SQL_INTEGER = 4;
const SQL_REAL = 7;
const SQL_FLOAT = 6;
const SQL_DOUBLE = 8;
const SQL_BIT = -7;
const SQL_TINYINT = -6;
const SQL_BIGINT = -5;
const SQL_TYPE_DATE = 91;
const SQL_TYPE_TIME = 92;
const SQL_TYPE_TIMESTAMP = 93;
const SQL_BINARY = -2;
const SQL_VARBINARY = -3;
const SQL_LONGVARBINARY = -4;

// ODBC C functions
extern fn SQLAllocHandle(handleType: c_short, inputHandle: SQLHANDLE, outputHandle: *SQLHANDLE) SQLRETURN;
extern fn SQLFreeHandle(handleType: c_short, handle: SQLHANDLE) SQLRETURN;
extern fn SQLSetEnvAttr(environmentHandle: SQLHENV, attribute: c_int, value: SQLPOINTER, stringLength: c_int) SQLRETURN;
extern fn SQLDriverConnect(connectionHandle: SQLHDBC, windowHandle: SQLPOINTER, inConnectionString: [*c]const SQLCHAR, stringLength1: c_short, outConnectionString: [*c]SQLCHAR, bufferLength: c_short, stringLength2Ptr: *c_short, driverCompletion: c_ushort) SQLRETURN;
extern fn SQLDisconnect(connectionHandle: SQLHDBC) SQLRETURN;
extern fn SQLExecDirect(statementHandle: SQLHSTMT, statementText: [*c]const SQLCHAR, textLength: c_int) SQLRETURN;
extern fn SQLPrepare(statementHandle: SQLHSTMT, statementText: [*c]const SQLCHAR, textLength: c_int) SQLRETURN;
extern fn SQLExecute(statementHandle: SQLHSTMT) SQLRETURN;
extern fn SQLBindParameter(statementHandle: SQLHSTMT, parameterNumber: c_ushort, inputOutputType: c_short, valueType: c_short, parameterType: c_int, columnSize: SQLULEN, decimalDigits: c_short, parameterValue: SQLPOINTER, bufferLength: SQLLEN, strLen_or_Ind: *SQLLEN) SQLRETURN;
extern fn SQLFetch(statementHandle: SQLHSTMT) SQLRETURN;
extern fn SQLGetData(statementHandle: SQLHSTMT, col_or_param_num: c_ushort, targetType: c_short, targetValue: SQLPOINTER, bufferLength: SQLLEN, strLen_or_Ind: *SQLLEN) SQLRETURN;
extern fn SQLNumResultCols(statementHandle: SQLHSTMT, columnCount: *c_short) SQLRETURN;
extern fn SQLDescribeCol(statementHandle: SQLHSTMT, columnNumber: c_ushort, columnName: [*c]SQLCHAR, bufferLength: c_short, nameLength: *c_short, dataType: *c_short, columnSize: *SQLULEN, decimalDigits: *c_short, nullable: *c_short) SQLRETURN;
extern fn SQLRowCount(statementHandle: SQLHSTMT, rowCount: *SQLLEN) SQLRETURN;
extern fn SQLEndTran(handleType: c_short, handle: SQLHANDLE, completionType: c_short) SQLRETURN;
extern fn SQLGetDiagRec(handleType: c_short, handle: SQLHANDLE, recNumber: c_short, state: [*c]SQLCHAR, native: *c_int, messageText: [*c]SQLCHAR, bufferLength: c_short, textLength: *c_short) SQLRETURN;

// Handle types
const SQL_HANDLE_ENV = 1;
const SQL_HANDLE_DBC = 2;
const SQL_HANDLE_STMT = 3;

// Parameter types
const SQL_PARAM_INPUT = 1;
const SQL_PARAM_OUTPUT = 2;
const SQL_PARAM_INPUT_OUTPUT = 3;

// C data types
const SQL_C_CHAR = 1;
const SQL_C_SLONG = 6;
const SQL_C_DOUBLE = 8;
const SQL_C_BINARY = -2;

// Attributes
const SQL_ATTR_ODBC_VERSION = 200;
const SQL_OV_ODBC3 = 3;

// Connection attributes
const SQL_DRIVER_NOPROMPT = 0;

// Transaction completion types
const SQL_COMMIT = 0;
const SQL_ROLLBACK = 1;

// Real SQL Server driver implementation
pub const SQLServerDriver = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    env: SQLHENV = null,
    dbc: SQLHDBC = null,

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        if (self.dbc != null) {
            _ = SQLDisconnect(self.dbc);
            _ = SQLFreeHandle(SQL_HANDLE_DBC, self.dbc);
            self.dbc = null;
        }
        if (self.env != null) {
            _ = SQLFreeHandle(SQL_HANDLE_ENV, self.env);
            self.env = null;
        }
    }

    pub fn connect(self: *Self, config: database.ConnectionConfig) errors.DatabaseError!database.Connection {
        // Ensure ODBC library is available
        library_manager.ensureLibrary(.odbc) catch |err| switch (err) {
            error.LibraryNotFound => {
                std.debug.print("❌ ODBC library not found. Please install unixODBC development package.\n", .{});
                return errors.DatabaseError.LibraryNotFound;
            },
            else => return err,
        };

        // Allocate environment handle
        var env: SQLHENV = null;
        const alloc_env_result = SQLAllocHandle(SQL_HANDLE_ENV, null, &env);
        if (alloc_env_result != SQL_SUCCESS and alloc_env_result != SQL_SUCCESS_WITH_INFO) {
            return errors.DatabaseError.ConnectionFailed;
        }
        self.env = env;

        // Set ODBC version
        const set_attr_result = SQLSetEnvAttr(self.env, SQL_ATTR_ODBC_VERSION, @ptrCast(@as(c_int, SQL_OV_ODBC3)), 0);
        if (set_attr_result != SQL_SUCCESS and set_attr_result != SQL_SUCCESS_WITH_INFO) {
            return errors.DatabaseError.ConnectionFailed;
        }

        // Allocate connection handle
        var dbc: SQLHDBC = null;
        const alloc_dbc_result = SQLAllocHandle(SQL_HANDLE_DBC, self.env, &dbc);
        if (alloc_dbc_result != SQL_SUCCESS and alloc_dbc_result != SQL_SUCCESS_WITH_INFO) {
            return errors.DatabaseError.ConnectionFailed;
        }
        self.dbc = dbc;

        // Build connection string
        const host = config.host orelse "localhost";
        const port = config.port orelse 1433;
        const db_name = config.database orelse return errors.DatabaseError.InvalidConfiguration;
        const username = config.username orelse return errors.DatabaseError.InvalidConfiguration;
        const password = config.password orelse "";

        const conn_str = try std.fmt.allocPrint(self.allocator, "DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={s},{};DATABASE={s};UID={s};PWD={s};TrustServerCertificate=yes", .{ host, port, db_name, username, password });
        defer self.allocator.free(conn_str);

        // Connect to database
        var conn_str_out: [1024]u8 = undefined;
        var conn_str_out_len: c_short = 0;

        const connect_result = SQLDriverConnect(
            self.dbc,
            null,
            conn_str.ptr,
            @intCast(conn_str.len),
            &conn_str_out,
            1024,
            &conn_str_out_len,
            SQL_DRIVER_NOPROMPT,
        );

        if (connect_result != SQL_SUCCESS and connect_result != SQL_SUCCESS_WITH_INFO) {
            self.printODBCError(SQL_HANDLE_DBC, self.dbc);
            return errors.DatabaseError.ConnectionFailed;
        }

        // Create connection object
        const conn_data = try self.allocator.create(SQLServerConnectionData);
        conn_data.* = SQLServerConnectionData{
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
        if (self.dbc == null) return errors.DatabaseError.InvalidConnection;

        // Allocate statement handle
        var stmt: SQLHSTMT = null;
        const alloc_stmt_result = SQLAllocHandle(SQL_HANDLE_STMT, self.dbc, &stmt);
        if (alloc_stmt_result != SQL_SUCCESS and alloc_stmt_result != SQL_SUCCESS_WITH_INFO) {
            return errors.DatabaseError.QueryFailed;
        }
        defer _ = SQLFreeHandle(SQL_HANDLE_STMT, stmt);

        if (args.len > 0) {
            return self.executeParamQuery(stmt, sql, args);
        } else {
            return self.executeSimpleQuery(stmt, sql);
        }
    }

    pub fn executeUpdate(self: *Self, sql: []const u8, args: []const types.Value) errors.DatabaseError!u64 {
        if (self.dbc == null) return errors.DatabaseError.InvalidConnection;

        // Allocate statement handle
        var stmt: SQLHSTMT = null;
        const alloc_stmt_result = SQLAllocHandle(SQL_HANDLE_STMT, self.dbc, &stmt);
        if (alloc_stmt_result != SQL_SUCCESS and alloc_stmt_result != SQL_SUCCESS_WITH_INFO) {
            return errors.DatabaseError.QueryFailed;
        }
        defer _ = SQLFreeHandle(SQL_HANDLE_STMT, stmt);

        if (args.len > 0) {
            return self.executeParamUpdate(stmt, sql, args);
        } else {
            return self.executeSimpleUpdate(stmt, sql);
        }
    }

    pub fn beginTransaction(self: *Self) errors.DatabaseError!database.Transaction {
        if (self.dbc == null) return errors.DatabaseError.InvalidConnection;

        const commit_result = SQLEndTran(SQL_HANDLE_DBC, self.dbc, SQL_ROLLBACK);
        if (commit_result != SQL_SUCCESS and commit_result != SQL_SUCCESS_WITH_INFO) {
            self.printODBCError(SQL_HANDLE_DBC, self.dbc);
            return errors.DatabaseError.TransactionFailed;
        }

        const transaction_data = try self.allocator.create(SQLServerTransactionData);
        transaction_data.* = SQLServerTransactionData{ .driver = self };

        return database.Transaction{
            .driver = createDriverInterface(self),
            .transaction_data = transaction_data,
        };
    }

    pub fn commitTransaction(self: *Self) !void {
        if (self.dbc == null) return errors.DatabaseError.InvalidConnection;

        const commit_result = SQLEndTran(SQL_HANDLE_DBC, self.dbc, SQL_COMMIT);
        if (commit_result != SQL_SUCCESS and commit_result != SQL_SUCCESS_WITH_INFO) {
            self.printODBCError(SQL_HANDLE_DBC, self.dbc);
            return errors.DatabaseError.TransactionFailed;
        }
    }

    pub fn rollbackTransaction(self: *Self) !void {
        if (self.dbc == null) return errors.DatabaseError.InvalidConnection;

        const rollback_result = SQLEndTran(SQL_HANDLE_DBC, self.dbc, SQL_ROLLBACK);
        if (rollback_result != SQL_SUCCESS and rollback_result != SQL_SUCCESS_WITH_INFO) {
            self.printODBCError(SQL_HANDLE_DBC, self.dbc);
            return errors.DatabaseError.TransactionFailed;
        }
    }

    pub fn getConnectionInfo(_: *Self) database.ConnectionInfo {
        const host = "localhost";
        const db = "unknown";
        const username = "unknown";
        const server_version = "SQL Server";

        return database.ConnectionInfo{
            .database_type = .sqlserver,
            .host = host,
            .database = db,
            .username = username,
            .connected_at = std.time.timestamp(),
            .server_version = server_version,
        };
    }

    fn executeSimpleQuery(self: *Self, stmt: SQLHSTMT, sql: []const u8) !types.ResultSet {
        const sql_cstr = try std.cstr.addNullByte(self.allocator, sql);
        defer self.allocator.free(sql_cstr);

        const exec_result = SQLExecDirect(stmt, sql_cstr.ptr, @intCast(sql_cstr.len - 1));
        if (exec_result != SQL_SUCCESS and exec_result != SQL_SUCCESS_WITH_INFO) {
            self.printODBCError(SQL_HANDLE_STMT, stmt);
            return errors.DatabaseError.QueryFailed;
        }

        return self.buildResultSet(stmt);
    }

    fn executeParamQuery(self: *Self, stmt: SQLHSTMT, sql: []const u8, args: []const types.Value) !types.ResultSet {
        const sql_cstr = try std.cstr.addNullByte(self.allocator, sql);
        defer self.allocator.free(sql_cstr);

        // Prepare statement
        const prepare_result = SQLPrepare(stmt, sql_cstr.ptr, @intCast(sql_cstr.len - 1));
        if (prepare_result != SQL_SUCCESS and prepare_result != SQL_SUCCESS_WITH_INFO) {
            self.printODBCError(SQL_HANDLE_STMT, stmt);
            return errors.DatabaseError.QueryFailed;
        }

        // Bind parameters using native ODBC parameter binding
        if (args.len > 0) {
            try self.bindODBCParameters(stmt, args);
        }

        // Execute statement
        const exec_result = SQLExecute(stmt);
        if (exec_result != SQL_SUCCESS and exec_result != SQL_SUCCESS_WITH_INFO) {
            self.printODBCError(SQL_HANDLE_STMT, stmt);
            return errors.DatabaseError.QueryFailed;
        }

        return self.buildResultSet(stmt);
    }

    fn bindODBCParameters(self: *Self, stmt: SQLHSTMT, args: []const types.Value) !void {
        for (args, 0..) |arg, i| {
            const bind_result = switch (arg) {
                .null => SQLBindParameter(
                    stmt,
                    @intCast(i + 1),
                    SQL_PARAM_INPUT,
                    SQL_C_CHAR,
                    SQL_CHAR,
                    0,
                    0,
                    null,
                    0,
                    null,
                ),
                .boolean => |b| {
                    const bool_val: c_int = @intCast(b);
                    SQLBindParameter(
                        stmt,
                        @intCast(i + 1),
                        SQL_PARAM_INPUT,
                        SQL_C_SLONG,
                        SQL_INTEGER,
                        0,
                        0,
                        &bool_val,
                        0,
                        null,
                    );
                },
                .integer => |int_val| {
                    SQLBindParameter(
                        stmt,
                        @intCast(i + 1),
                        SQL_PARAM_INPUT,
                        SQL_C_SLONG,
                        SQL_INTEGER,
                        0,
                        0,
                        &int_val,
                        0,
                        null,
                    );
                },
                .float => |float_val| {
                    SQLBindParameter(
                        stmt,
                        @intCast(i + 1),
                        SQL_PARAM_INPUT,
                        SQL_C_DOUBLE,
                        SQL_DOUBLE,
                        0,
                        0,
                        &float_val,
                        0,
                        null,
                    );
                },
                .text => |text_val| {
                    const len: SQLLEN = @intCast(text_val.len);
                    SQLBindParameter(
                        stmt,
                        @intCast(i + 1),
                        SQL_PARAM_INPUT,
                        SQL_C_CHAR,
                        SQL_VARCHAR,
                        @intCast(text_val.len),
                        0,
                        text_val.ptr,
                        @intCast(text_val.len),
                        &len,
                    );
                },
                .date => |date_val| {
                    const len: SQLLEN = @intCast(date_val.len);
                    SQLBindParameter(
                        stmt,
                        @intCast(i + 1),
                        SQL_PARAM_INPUT,
                        SQL_C_CHAR,
                        SQL_TYPE_DATE,
                        0,
                        0,
                        date_val.ptr,
                        @intCast(date_val.len),
                        &len,
                    );
                },
                .time => |time_val| {
                    const len: SQLLEN = @intCast(time_val.len);
                    SQLBindParameter(
                        stmt,
                        @intCast(i + 1),
                        SQL_PARAM_INPUT,
                        SQL_C_CHAR,
                        SQL_TYPE_TIME,
                        0,
                        0,
                        time_val.ptr,
                        @intCast(time_val.len),
                        &len,
                    );
                },
                .timestamp => |ts_val| {
                    const len: SQLLEN = @intCast(ts_val.len);
                    SQLBindParameter(
                        stmt,
                        @intCast(i + 1),
                        SQL_PARAM_INPUT,
                        SQL_C_CHAR,
                        SQL_TYPE_TIMESTAMP,
                        0,
                        0,
                        ts_val.ptr,
                        @intCast(ts_val.len),
                        &len,
                    );
                },
                .binary => |binary_val| {
                    const len: SQLLEN = @intCast(binary_val.len);
                    SQLBindParameter(
                        stmt,
                        @intCast(i + 1),
                        SQL_PARAM_INPUT,
                        SQL_C_BINARY,
                        SQL_BINARY,
                        @intCast(binary_val.len),
                        0,
                        binary_val.ptr,
                        @intCast(binary_val.len),
                        &len,
                    );
                },
            };

            if (bind_result != SQL_SUCCESS and bind_result != SQL_SUCCESS_WITH_INFO) {
                self.printODBCError(SQL_HANDLE_STMT, stmt);
                return errors.DatabaseError.QueryFailed;
            }
        }
    }

    fn executeSimpleUpdate(self: *Self, stmt: SQLHSTMT, sql: []const u8) !u64 {
        const sql_cstr = try std.cstr.addNullByte(self.allocator, sql);
        defer self.allocator.free(sql_cstr);

        const exec_result = SQLExecDirect(stmt, sql_cstr.ptr, @intCast(sql_cstr.len - 1));
        if (exec_result != SQL_SUCCESS and exec_result != SQL_SUCCESS_WITH_INFO) {
            self.printODBCError(SQL_HANDLE_STMT, stmt);
            return errors.DatabaseError.QueryFailed;
        }

        var row_count: SQLLEN = 0;
        const row_count_result = SQLRowCount(stmt, &row_count);
        if (row_count_result != SQL_SUCCESS and row_count_result != SQL_SUCCESS_WITH_INFO) {
            return 0;
        }

        return @intCast(row_count);
    }

    fn executeParamUpdate(self: *Self, stmt: SQLHSTMT, sql: []const u8, args: []const types.Value) !u64 {
        // For now, use simple parameter substitution
        const formatted_sql = try self.formatSqlWithParams(sql, args);
        defer self.allocator.free(formatted_sql);
        return self.executeSimpleUpdate(stmt, formatted_sql);
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
            .boolean => |b| try result.appendSlice(if (b) "1" else "0"),
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
                try result.appendSlice("0x");
                for (b) |byte| {
                    try std.fmt.format(result.writer(), "{x:0>2}", .{byte});
                }
            },
        }
    }

    fn buildResultSet(self: *Self, stmt: SQLHSTMT) !types.ResultSet {
        var column_count: c_short = 0;
        const num_cols_result = SQLNumResultCols(stmt, &column_count);
        if (num_cols_result != SQL_SUCCESS and num_cols_result != SQL_SUCCESS_WITH_INFO) {
            return errors.DatabaseError.QueryFailed;
        }

        // Build columns
        var columns = try self.allocator.alloc(types.Column, @intCast(column_count));
        errdefer self.allocator.free(columns);

        for (1..@intCast(column_count + 1)) |col_idx| {
            var column_name: [256]u8 = undefined;
            var name_len: c_short = 0;
            var data_type: c_short = 0;
            var column_size: SQLULEN = 0;
            var decimal_digits: c_short = 0;
            var nullable: c_short = 0;

            const describe_result = SQLDescribeCol(
                stmt,
                @intCast(col_idx),
                &column_name,
                256,
                &name_len,
                &data_type,
                &column_size,
                &decimal_digits,
                &nullable,
            );

            if (describe_result != SQL_SUCCESS and describe_result != SQL_SUCCESS_WITH_INFO) {
                return errors.DatabaseError.QueryFailed;
            }

            const name = try self.allocator.dupe(u8, column_name[0..@intCast(name_len)]);
            const column_type = self.mapODBCTypeToColumnType(data_type);

            columns[col_idx - 1] = types.Column{
                .name = name,
                .type = column_type,
                .nullable = nullable != 0,
            };
        }

        // Build rows
        var rows = std.ArrayList(types.Row).init(self.allocator);
        errdefer rows.deinit();

        while (true) {
            const fetch_result = SQLFetch(stmt);
            if (fetch_result == SQL_NO_DATA) {
                break;
            }
            if (fetch_result != SQL_SUCCESS and fetch_result != SQL_SUCCESS_WITH_INFO) {
                self.printODBCError(SQL_HANDLE_STMT, stmt);
                return errors.DatabaseError.QueryFailed;
            }

            var values = try self.allocator.alloc(types.Value, @intCast(column_count));
            errdefer self.allocator.free(values);

            for (1..@intCast(column_count + 1)) |col_idx| {
                var buffer: [4096]u8 = undefined;
                var str_len: SQLLEN = 0;

                const get_data_result = SQLGetData(
                    stmt,
                    @intCast(col_idx),
                    SQL_CHAR,
                    &buffer,
                    4096,
                    &str_len,
                );

                if (str_len == -1) {
                    values[col_idx - 1] = .null;
                } else if (get_data_result == SQL_SUCCESS or get_data_result == SQL_SUCCESS_WITH_INFO) {
                    const text = try self.allocator.dupe(u8, buffer[0..@intCast(str_len)]);
                    values[col_idx - 1] = .{ .text = text };
                } else {
                    values[col_idx - 1] = .null;
                }
            }

            try rows.append(types.Row.init(self.allocator, values));
        }

        return types.ResultSet.init(self.allocator, columns, try rows.toOwnedSlice());
    }

    fn mapODBCTypeToColumnType(_: *Self, odbc_type: c_short) types.ValueType {
        return switch (odbc_type) {
            SQL_BIT => .boolean,
            SQL_TINYINT, SQL_SMALLINT, SQL_INTEGER, SQL_BIGINT => .integer,
            SQL_REAL, SQL_FLOAT, SQL_DOUBLE, SQL_DECIMAL, SQL_NUMERIC => .float,
            SQL_CHAR, SQL_VARCHAR, SQL_LONGVARCHAR, SQL_WCHAR, SQL_WVARCHAR, SQL_WLONGVARCHAR => .text,
            SQL_TYPE_DATE => .date,
            SQL_TYPE_TIME => .time,
            SQL_TYPE_TIMESTAMP => .timestamp,
            SQL_BINARY, SQL_VARBINARY, SQL_LONGVARBINARY => .binary,
            else => .text,
        };
    }

    fn printODBCError(_: *Self, handle_type: c_short, handle: SQLHANDLE) void {
        var state: [6]u8 = undefined;
        var native: c_int = 0;
        var message: [1024]u8 = undefined;
        var msg_len: c_short = 0;

        const result = SQLGetDiagRec(
            handle_type,
            handle,
            1,
            &state,
            &native,
            &message,
            1024,
            &msg_len,
        );

        if (result == SQL_SUCCESS or result == SQL_SUCCESS_WITH_INFO) {
            std.debug.print("SQL Server ODBC Error: {s} - {s}\n", .{ &state, message[0..@intCast(msg_len)] });
        } else {
            std.debug.print("SQL Server ODBC Error: Unknown error\n", .{});
        }
    }
};

// SQL Server connection data
const SQLServerConnectionData = struct {
    driver: *SQLServerDriver,
    host: []const u8,
    database: []const u8,
    username: []const u8,
};

// SQL Server transaction data
const SQLServerTransactionData = struct {
    driver: *SQLServerDriver,
};

// Create driver interface
fn createDriverInterface(sqlserver_driver: *SQLServerDriver) database.Driver {
    const driver_data = sqlserver_driver;

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
    const sqlserver_driver = @as(*SQLServerDriver, @ptrCast(@alignCast(driver_data)));
    return sqlserver_driver.connect(config);
}

fn close_impl(driver_data: *anyopaque) void {
    const sqlserver_driver = @as(*SQLServerDriver, @ptrCast(@alignCast(driver_data)));
    sqlserver_driver.deinit();
}

fn executeQuery_impl(driver_data: *anyopaque, sql: []const u8, args: []const types.Value) errors.DatabaseError!types.ResultSet {
    const sqlserver_driver = @as(*SQLServerDriver, @ptrCast(@alignCast(driver_data)));
    return sqlserver_driver.executeQuery(sql, args);
}

fn executeUpdate_impl(driver_data: *anyopaque, sql: []const u8, args: []const types.Value) errors.DatabaseError!u64 {
    const sqlserver_driver = @as(*SQLServerDriver, @ptrCast(@alignCast(driver_data)));
    return sqlserver_driver.executeUpdate(sql, args);
}

fn beginTransaction_impl(driver_data: *anyopaque) errors.DatabaseError!database.Transaction {
    const sqlserver_driver = @as(*SQLServerDriver, @ptrCast(@alignCast(driver_data)));
    return sqlserver_driver.beginTransaction();
}

fn getConnectionInfo_impl(driver_data: *anyopaque) database.ConnectionInfo {
    const sqlserver_driver = @as(*SQLServerDriver, @ptrCast(@alignCast(driver_data)));
    return sqlserver_driver.getConnectionInfo();
}

// Factory function
pub fn createDriver(allocator: std.mem.Allocator) database.Driver {
    const sqlserver_driver = allocator.create(SQLServerDriver) catch unreachable;
    sqlserver_driver.* = SQLServerDriver.init(allocator);
    return createDriverInterface(sqlserver_driver);
}
