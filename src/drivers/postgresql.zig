const std = @import("std");
const database = @import("../database.zig");
const types = @import("../utils/types.zig");
const errors = @import("../utils/errors.zig");
const library_manager = @import("../utils/library_manager.zig");

// PostgreSQL C API types and functions
const PGconn = opaque {};
const PGresult = opaque {};
const PGnotify = opaque {};
const PQconninfoOption = opaque {};

// PostgreSQL result status codes
const PGRES_EMPTY_QUERY = 0;
const PGRES_COMMAND_OK = 1;
const PGRES_TUPLES_OK = 2;
const PGRES_COPY_OUT = 3;
const PGRES_COPY_IN = 4;
const PGRES_BAD_RESPONSE = 5;
const PGRES_NONFATAL_ERROR = 6;
const PGRES_FATAL_ERROR = 7;
const PGRES_COPY_BOTH = 8;
const PGRES_SINGLE_TUPLE = 9;

// PostgreSQL field types (simplified)
const BOOLOID = 16;
const BYTEAOID = 17;
const CHAROID = 18;
const NAMEOID = 19;
const INT8OID = 20;
const INT2OID = 21;
const INT2VECTOROID = 22;
const INT4OID = 23;
const REGPROCOID = 24;
const TEXTOID = 25;
const OIDOID = 26;
const TIDOID = 27;
const XIDOID = 28;
const CIDOID = 29;
const OIDVECTOROID = 30;
const JSONOID = 114;
const XMLOID = 142;
const PGNODETREEOID = 194;
const POINTOID = 600;
const LSEGOID = 601;
const PATHOID = 602;
const BOXOID = 603;
const POLYGONOID = 604;
const LINEOID = 628;
const FLOAT4OID = 700;
const FLOAT8OID = 701;
const ABSTIMEOID = 702;
const RELTIMEOID = 703;
const TINTERVALOID = 704;
const UNKNOWNOID = 705;
const CIRCLEOID = 718;
const CASHOID = 790;
const MACADDROID = 829;
const INETOID = 869;
const CIDROID = 650;
const ACLITEMOID = 1033;
const BPCHAROID = 1042;
const VARCHAROID = 1043;
const DATEOID = 1082;
const TIMEOID = 1083;
const TIMESTAMPOID = 1114;
const TIMESTAMPTZOID = 1184;
const INTERVALOID = 1186;
const TIMETZOID = 1266;
const BITOID = 1560;
const VARBITOID = 1562;
const NUMERICOID = 1700;
const REFCURSOROID = 1790;
const REGPROCEDUREOID = 2202;
const REGOPEROID = 2203;
const REGOPERATOROID = 2204;
const REGCLASSOID = 2205;
const REGTYPEOID = 2206;
const UUIDOID = 2950;
const LSNOID = 3220;
const TSVECTOROID = 3614;
const GTSVECTOROID = 3642;
const TSQUERYOID = 3615;
const REGCONFIGOID = 3734;
const REGDICTIONARYOID = 3769;
const JSONBOID = 3802;
const JSONPATHOID = 4072;

// PostgreSQL C functions
extern fn PQconnectdb(conninfo: [*c]const u8) ?*PGconn;
extern fn PQfinish(conn: *PGconn) void;
extern fn PQstatus(conn: *PGconn) c_int;
extern fn PQerrorMessage(conn: *PGconn) [*c]const u8;
extern fn PQexec(conn: *PGconn, command: [*c]const u8) ?*PGresult;
extern fn PQexecParams(conn: *PGconn, command: [*c]const u8, nParams: c_int, paramTypes: [*c]const c_uint, paramValues: [*c]const [*c]const u8, paramLengths: [*c]const c_int, paramFormats: [*c]const c_int, resultFormat: c_int) ?*PGresult;
extern fn PQprepare(conn: *PGconn, stmtName: [*c]const u8, query: [*c]const u8, nParams: c_int, paramTypes: [*c]const c_uint) ?*PGresult;
extern fn PQexecPrepared(conn: *PGconn, stmtName: [*c]const u8, nParams: c_int, paramValues: [*c]const [*c]const u8, paramLengths: [*c]const c_int, paramFormats: [*c]const c_int, resultFormat: c_int) ?*PGresult;
extern fn PQclear(res: *PGresult) void;
extern fn PQresultStatus(res: *PGresult) c_int;
extern fn PQresultErrorMessage(res: *PGresult) [*c]const u8;
extern fn PQntuples(res: *PGresult) c_int;
extern fn PQnfields(res: *PGresult) c_int;
extern fn PQfname(res: *PGresult, field_num: c_int) [*c]const u8;
extern fn PQftype(res: *PGresult, field_num: c_int) c_uint;
extern fn PQgetvalue(res: *PGresult, tup_num: c_int, field_num: c_int) [*c]const u8;
extern fn PQgetisnull(res: *PGresult, tup_num: c_int, field_num: c_int) c_int;
extern fn PQgetlength(res: *PGresult, tup_num: c_int, field_num: c_int) c_int;
extern fn PQcmdTuples(res: *PGresult) [*c]const u8;
extern fn PQserverVersion(conn: *PGconn) c_int;
extern fn PQdb(conn: *PGconn) [*c]const u8;
extern fn PQuser(conn: *PGconn) [*c]const u8;
extern fn PQhost(conn: *PGconn) [*c]const u8;
extern fn PQport(conn: *PGconn) [*c]const u8;

// Real PostgreSQL driver implementation
pub const PostgreSQLDriver = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    conn: ?*PGconn = null,

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        if (self.conn) |conn| {
            PQfinish(conn);
            self.conn = null;
        }
    }

    pub fn connect(self: *Self, config: database.ConnectionConfig) errors.DatabaseError!database.Connection {
        // Ensure PostgreSQL library is available
        library_manager.ensureLibrary(.postgresql) catch |err| switch (err) {
            error.LibraryNotFound => {
                std.debug.print("❌ PostgreSQL library not found. Please install libpq development package.\n", .{});
                return errors.DatabaseError.LibraryNotFound;
            },
            else => return errors.DatabaseError.LibraryNotFound,
        };

        const host = config.host orelse "localhost";
        const port = config.port orelse 5432;
        const db_name = config.database orelse return errors.DatabaseError.InvalidConfiguration;
        const username = config.username orelse return errors.DatabaseError.InvalidConfiguration;
        const password = config.password orelse "";

        // Build connection string
        const conninfo = try std.fmt.allocPrint(self.allocator, "host={s} port={} dbname={s} user={s} password={s}", .{ host, port, db_name, username, password });
        defer self.allocator.free(conninfo);

        self.conn = PQconnectdb(conninfo.ptr) orelse return errors.DatabaseError.ConnectionFailed;

        // Check connection status
        const status = PQstatus(self.conn.?);
        if (status != 0) { // CONNECTION_OK
            const error_msg = PQerrorMessage(self.conn.?);
            std.debug.print("PostgreSQL connection error: {s}\n", .{error_msg});
            return errors.DatabaseError.ConnectionFailed;
        }

        // Create connection object
        const conn_data = try self.allocator.create(PostgreSQLConnectionData);
        conn_data.* = PostgreSQLConnectionData{
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
        if (self.conn == null) return errors.DatabaseError.InvalidConnection;

        if (args.len > 0) {
            return self.executeParamQuery(sql, args);
        } else {
            return self.executeSimpleQuery(sql);
        }
    }

    pub fn executeUpdate(self: *Self, sql: []const u8, args: []const types.Value) errors.DatabaseError!u64 {
        if (self.conn == null) return errors.DatabaseError.InvalidConnection;

        if (args.len > 0) {
            return self.executeParamUpdate(sql, args) catch return errors.DatabaseError.QueryFailed;
        } else {
            return self.executeSimpleUpdate(sql) catch return errors.DatabaseError.QueryFailed;
        }
    }

    pub fn beginTransaction(self: *Self) errors.DatabaseError!database.Transaction {
        if (self.conn == null) return errors.DatabaseError.InvalidConnection;

        const result = PQexec(self.conn.?, "BEGIN");
        defer if (result) |res| PQclear(res);

        if (result == null or PQresultStatus(result.?) != PGRES_COMMAND_OK) {
            const error_msg = if (result) |res| PQresultErrorMessage(res) else @as([*c]const u8, "Unknown error");
            std.debug.print("PostgreSQL begin transaction error: {s}\n", .{error_msg});
            return errors.DatabaseError.TransactionFailed;
        }

        const transaction_data = try self.allocator.create(PostgreSQLTransactionData);
        transaction_data.* = PostgreSQLTransactionData{ .driver = self };

        return database.Transaction{
            .driver = createDriverInterface(self),
            .transaction_data = transaction_data,
        };
    }

    pub fn commitTransaction(self: *Self) !void {
        if (self.conn == null) return errors.DatabaseError.InvalidConnection;

        const result = PQexec(self.conn.?, "COMMIT");
        defer PQclear(result);

        if (result == null or PQresultStatus(result) != PGRES_COMMAND_OK) {
            const error_msg = if (result) |res| PQresultErrorMessage(res) else "Unknown error";
            std.debug.print("PostgreSQL commit error: {s}\n", .{error_msg});
            return errors.DatabaseError.TransactionFailed;
        }
    }

    pub fn rollbackTransaction(self: *Self) !void {
        if (self.conn == null) return errors.DatabaseError.InvalidConnection;

        const result = PQexec(self.conn.?, "ROLLBACK");
        defer PQclear(result);

        if (result == null or PQresultStatus(result) != PGRES_COMMAND_OK) {
            const error_msg = if (result) |res| PQresultErrorMessage(res) else "Unknown error";
            std.debug.print("PostgreSQL rollback error: {s}\n", .{error_msg});
            return errors.DatabaseError.TransactionFailed;
        }
    }

    pub fn getConnectionInfo(self: *Self) database.ConnectionInfo {
        const host = if (self.conn) |conn|
            std.mem.sliceTo(PQhost(conn), 0)
        else
            "localhost";
        const db = if (self.conn) |conn|
            std.mem.sliceTo(PQdb(conn), 0)
        else
            "unknown";
        const username = if (self.conn) |conn|
            std.mem.sliceTo(PQuser(conn), 0)
        else
            "unknown";
        const server_version = if (self.conn) |conn|
            std.fmt.allocPrint(self.allocator, "{}", .{PQserverVersion(conn)}) catch "Unknown"
        else
            "Unknown";

        return database.ConnectionInfo{
            .database_type = .postgresql,
            .host = host,
            .database = db,
            .username = username,
            .connected_at = std.time.timestamp(),
            .server_version = server_version,
        };
    }

    fn executeSimpleQuery(self: *Self, sql: []const u8) !types.ResultSet {
        const sql_cstr = try self.allocator.alloc(u8, sql.len + 1);
        defer self.allocator.free(sql_cstr);
        @memcpy(sql_cstr[0..sql.len], sql);
        sql_cstr[sql.len] = 0;

        const result = PQexec(self.conn.?, sql_cstr.ptr) orelse return errors.DatabaseError.QueryFailed;
        defer PQclear(result);

        const status = PQresultStatus(result);
        if (status != PGRES_TUPLES_OK and status != PGRES_COMMAND_OK) {
            const error_msg = PQresultErrorMessage(result);
            std.debug.print("PostgreSQL query error: {s}\n", .{error_msg});
            return errors.DatabaseError.QueryFailed;
        }

        return self.buildResultSet(result);
    }

    fn executeParamQuery(self: *Self, sql: []const u8, args: []const types.Value) !types.ResultSet {
        const sql_cstr = try self.allocator.alloc(u8, sql.len + 1);
        defer self.allocator.free(sql_cstr);
        @memcpy(sql_cstr[0..sql.len], sql);
        sql_cstr[sql.len] = 0;

        // Convert parameters to C strings
        var param_values = try self.allocator.alloc([*c]const u8, args.len);
        defer self.allocator.free(param_values);

        var param_lengths = try self.allocator.alloc(c_int, args.len);
        defer self.allocator.free(param_lengths);

        var param_formats = try self.allocator.alloc(c_int, args.len);
        defer self.allocator.free(param_formats);

        for (args, 0..) |arg, i| {
            switch (arg) {
                .null => {
                    param_values[i] = null;
                    param_lengths[i] = 0;
                    param_formats[i] = 0;
                },
                .boolean => |b| {
                    const text = try std.fmt.allocPrint(self.allocator, "{}", .{b});
                    param_values[i] = text.ptr;
                    param_lengths[i] = @intCast(text.len);
                    param_formats[i] = 0;
                },
                .integer => |int_val| {
                    const text = try std.fmt.allocPrint(self.allocator, "{}", .{int_val});
                    param_values[i] = text.ptr;
                    param_lengths[i] = @intCast(text.len);
                    param_formats[i] = 0;
                },
                .float => |float_val| {
                    const text = try std.fmt.allocPrint(self.allocator, "{}", .{float_val});
                    param_values[i] = text.ptr;
                    param_lengths[i] = @intCast(text.len);
                    param_formats[i] = 0;
                },
                .text => |text_val| {
                    param_values[i] = text_val.ptr;
                    param_lengths[i] = @intCast(text_val.len);
                    param_formats[i] = 0;
                },
                .date => |date_val| {
                    const text = try std.fmt.allocPrint(self.allocator, "{}", .{date_val});
                    param_values[i] = text.ptr;
                    param_lengths[i] = @intCast(text.len);
                    param_formats[i] = 0;
                },
                .time => |time_val| {
                    const text = try std.fmt.allocPrint(self.allocator, "{}", .{time_val});
                    param_values[i] = text.ptr;
                    param_lengths[i] = @intCast(text.len);
                    param_formats[i] = 0;
                },
                .timestamp => |ts_val| {
                    const text = try std.fmt.allocPrint(self.allocator, "{}", .{ts_val});
                    param_values[i] = text.ptr;
                    param_lengths[i] = @intCast(text.len);
                    param_formats[i] = 0;
                },
                .binary => |binary_val| {
                    param_values[i] = binary_val.ptr;
                    param_lengths[i] = @intCast(binary_val.len);
                    param_formats[i] = 1; // Binary format
                },
            }
        }

        const result = PQexecParams(
            self.conn.?,
            sql_cstr.ptr,
            @intCast(args.len),
            null, // Let PostgreSQL infer types
            param_values.ptr,
            param_lengths.ptr,
            param_formats.ptr,
            0, // Return text format
        ) orelse return errors.DatabaseError.QueryFailed;

        defer PQclear(result);

        const status = PQresultStatus(result);
        if (status != PGRES_TUPLES_OK and status != PGRES_COMMAND_OK) {
            const error_msg = PQresultErrorMessage(result);
            std.debug.print("PostgreSQL param query error: {s}\n", .{error_msg});
            return errors.DatabaseError.QueryFailed;
        }

        return self.buildResultSet(result);
    }

    fn executeSimpleUpdate(self: *Self, sql: []const u8) !u64 {
        const sql_cstr = try self.allocator.alloc(u8, sql.len + 1);
        defer self.allocator.free(sql_cstr);
        @memcpy(sql_cstr[0..sql.len], sql);
        sql_cstr[sql.len] = 0;

        const result = PQexec(self.conn.?, sql_cstr.ptr) orelse return errors.DatabaseError.QueryFailed;
        defer PQclear(result);

        const status = PQresultStatus(result);
        if (status != PGRES_COMMAND_OK) {
            const error_msg = PQresultErrorMessage(result);
            std.debug.print("PostgreSQL update error: {s}\n", .{error_msg});
            return errors.DatabaseError.QueryFailed;
        }

        const tuples_str = PQcmdTuples(result);
        if (tuples_str[0] == 0) {
            return 0; // No rows affected
        }

        const affected_rows = try std.fmt.parseInt(u64, std.mem.sliceTo(tuples_str, 0), 10);
        return affected_rows;
    }

    fn executeParamUpdate(self: *Self, sql: []const u8, args: []const types.Value) !u64 {
        // For now, use simple parameter substitution
        const formatted_sql = try self.formatSqlWithParams(sql, args);
        defer self.allocator.free(formatted_sql);
        return self.executeSimpleUpdate(formatted_sql);
    }

    fn formatSqlWithParams(self: *Self, sql: []const u8, args: []const types.Value) ![]u8 {
        var buffer: [1024]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&buffer);
        const writer = fbs.writer();

        var param_index: usize = 0;
        var i: usize = 0;

        while (i < sql.len) {
            if (i + 1 < sql.len and sql[i] == '$' and std.ascii.isDigit(sql[i + 1])) {
                if (param_index >= args.len) {
                    return errors.DatabaseError.QueryFailed;
                }

                try self.appendValue(writer, args[param_index]);
                param_index += 1;
                i += 2; // Skip $ and digit
            } else {
                try writer.writeByte(sql[i]);
                i += 1;
            }
        }

        return self.allocator.dupe(u8, fbs.getWritten());
    }

    fn appendValue(_: *Self, writer: anytype, value: types.Value) !void {
        switch (value) {
            .null => try writer.writeAll("NULL"),
            .boolean => |b| try writer.writeAll(if (b) "TRUE" else "FALSE"),
            .integer => |i| try writer.print("{}", .{i}),
            .float => |f| try writer.print("{}", .{f}),
            .text => |t| {
                try writer.writeByte('\'');
                for (t) |char| {
                    if (char == '\'') {
                        try writer.writeAll("''");
                    } else {
                        try writer.writeByte(char);
                    }
                }
                try writer.writeByte('\'');
            },
            .date => |d| try writer.print("'{}'", .{d}),
            .time => |t| try writer.print("'{}'", .{t}),
            .timestamp => |ts| try writer.print("'{}'", .{ts}),
            .binary => |b| {
                try writer.writeAll("E'\\\\x");
                for (b) |byte| {
                    try writer.print("{x:0>2}", .{byte});
                }
                try writer.writeByte('\'');
            },
        }
    }

    fn buildResultSet(self: *Self, result: *PGresult) !types.ResultSet {
        const field_count = PQnfields(result);
        const tuple_count = PQntuples(result);

        // Build columns
        var columns = try self.allocator.alloc(types.Column, @intCast(field_count));
        errdefer self.allocator.free(columns);

        for (0..@as(usize, @intCast(field_count))) |i| {
            const name_cstr = PQfname(result, @intCast(i));
            const name = try self.allocator.dupe(u8, std.mem.sliceTo(name_cstr, 0));

            const pg_type = PQftype(result, @intCast(i));
            const column_type = self.mapPostgresTypeToColumnType(pg_type);

            columns[i] = types.Column{
                .name = name,
                .type = column_type,
                .nullable = true,
            };
        }

        // Build rows
        var rows = try self.allocator.alloc(types.Row, @intCast(tuple_count));
        errdefer self.allocator.free(rows);

        for (0..@as(usize, @intCast(tuple_count))) |row_idx| {
            var values = try self.allocator.alloc(types.Value, @intCast(field_count));
            errdefer self.allocator.free(values);

            for (0..@as(usize, @intCast(field_count))) |col_idx| {
                const is_null = PQgetisnull(result, @intCast(row_idx), @intCast(col_idx));
                if (is_null != 0) {
                    values[col_idx] = .null;
                } else {
                    const value_cstr = PQgetvalue(result, @intCast(row_idx), @intCast(col_idx));
                    const value_len = PQgetlength(result, @intCast(row_idx), @intCast(col_idx));
                    const value_slice = value_cstr[0..@intCast(value_len)];
                    const value = try self.allocator.dupe(u8, value_slice);
                    values[col_idx] = .{ .text = value };
                }
            }

            rows[row_idx] = types.Row.init(self.allocator, values);
        }

        return types.ResultSet.init(self.allocator, columns, rows);
    }

    fn mapPostgresTypeToColumnType(_: *Self, pg_type: c_uint) types.ValueType {
        return switch (pg_type) {
            BOOLOID => .boolean,
            INT2OID, INT4OID, INT8OID => .integer,
            FLOAT4OID, FLOAT8OID, NUMERICOID => .float,
            TEXTOID, VARCHAROID, CHAROID, BPCHAROID => .text,
            DATEOID => .date,
            TIMEOID, TIMETZOID => .time,
            TIMESTAMPOID, TIMESTAMPTZOID => .timestamp,
            BYTEAOID => .binary,
            else => .text,
        };
    }
};

// PostgreSQL connection data
const PostgreSQLConnectionData = struct {
    driver: *PostgreSQLDriver,
    host: []const u8,
    database: []const u8,
    username: []const u8,
};

// PostgreSQL transaction data
const PostgreSQLTransactionData = struct {
    driver: *PostgreSQLDriver,
};

// Create driver interface
fn createDriverInterface(pg_driver: *PostgreSQLDriver) database.Driver {
    const driver_data = pg_driver;

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
    const pg_driver = @as(*PostgreSQLDriver, @ptrCast(@alignCast(driver_data)));
    return pg_driver.connect(config);
}

fn close_impl(driver_data: *anyopaque) void {
    const pg_driver = @as(*PostgreSQLDriver, @ptrCast(@alignCast(driver_data)));
    pg_driver.deinit();
}

fn executeQuery_impl(driver_data: *anyopaque, sql: []const u8, args: []const types.Value) errors.DatabaseError!types.ResultSet {
    const pg_driver = @as(*PostgreSQLDriver, @ptrCast(@alignCast(driver_data)));
    return pg_driver.executeQuery(sql, args);
}

fn executeUpdate_impl(driver_data: *anyopaque, sql: []const u8, args: []const types.Value) errors.DatabaseError!u64 {
    const pg_driver = @as(*PostgreSQLDriver, @ptrCast(@alignCast(driver_data)));
    return pg_driver.executeUpdate(sql, args);
}

fn beginTransaction_impl(driver_data: *anyopaque) errors.DatabaseError!database.Transaction {
    const pg_driver = @as(*PostgreSQLDriver, @ptrCast(@alignCast(driver_data)));
    return pg_driver.beginTransaction();
}

fn getConnectionInfo_impl(driver_data: *anyopaque) database.ConnectionInfo {
    const pg_driver = @as(*PostgreSQLDriver, @ptrCast(@alignCast(driver_data)));
    return pg_driver.getConnectionInfo();
}

// Factory function
pub fn createDriver(allocator: std.mem.Allocator) database.Driver {
    const pg_driver = allocator.create(PostgreSQLDriver) catch unreachable;
    pg_driver.* = PostgreSQLDriver.init(allocator);
    return createDriverInterface(pg_driver);
}
