const std = @import("std");
const types = @import("utils/types.zig");
const errors = @import("utils/errors.zig");

// Connection configuration supporting both URL and field-based approaches
pub const ConnectionConfig = struct {
    database_type: types.DatabaseType,
    allocator: ?std.mem.Allocator = null,

    // Either connection_string OR specific fields
    connection_string: ?[]const u8 = null,

    // Database-specific fields (optional)
    host: ?[]const u8 = null,
    port: ?u16 = null,
    database: ?[]const u8 = null,
    username: ?[]const u8 = null,
    password: ?[]const u8 = null,

    // Additional options
    pool: ?PoolConfig = null,
    timeout_ms: u32 = 30000,
    ssl_mode: ?[]const u8 = null,

    // File-based databases
    file_path: ?[]const u8 = null,

    // ClickHouse specific
    http_port: ?u16 = null,

    pub fn validate(self: ConnectionConfig) !void {
        // Validate that either connection_string OR required fields are provided
        if (self.connection_string == null) {
            switch (self.database_type) {
                .postgresql => {
                    if (self.host == null or self.database == null or self.username == null) {
                        return errors.DatabaseError.InvalidConfiguration;
                    }
                },
                .sqlite => {
                    if (self.file_path == null) {
                        return errors.DatabaseError.InvalidConfiguration;
                    }
                },
                .mysql, .sqlserver, .oracle, .clickhouse => {
                    if (self.host == null or self.database == null or self.username == null) {
                        return errors.DatabaseError.InvalidConfiguration;
                    }
                },
                .excel, .csv => {
                    if (self.file_path == null) {
                        return errors.DatabaseError.InvalidConfiguration;
                    }
                },
            }
        }
    }
};

// Pooling configuration with intelligent defaults
pub const PoolConfig = struct {
    enabled: bool = true, // Default: enabled
    size: u32 = 10, // Default pool size
    max_connections: u32 = 100, // Maximum connections
    timeout_ms: u32 = 30000, // Connection timeout
    idle_timeout_ms: u32 = 300000, // Idle connection timeout (5 min)

    // Auto-disable for certain database types
    pub fn defaultForDatabase(db_type: types.DatabaseType) PoolConfig {
        return switch (db_type) {
            // Databases that benefit from pooling
            .postgresql => .{
                .enabled = true,
                .size = 10,
                .max_connections = 100,
            },

            // Databases where pooling is not applicable
            .sqlite => .{
                .enabled = false, // Noop pooling
                .size = 1,
                .max_connections = 1,
            },
            .mysql, .sqlserver, .oracle, .clickhouse => .{
                .enabled = true,
                .size = 10,
                .max_connections = 100,
            },
            .excel, .csv => .{
                .enabled = false, // Noop pooling
                .size = 1,
                .max_connections = 1,
            },
        };
    }
};

// Driver interface that all database drivers must implement
pub const Driver = struct {
    const Self = @This();

    // VTable for driver methods
    vtable: *const VTable,
    driver_data: *anyopaque,

    pub const VTable = struct {
        connect: *const fn (driver_data: *anyopaque, config: ConnectionConfig) errors.DatabaseError!Connection,
        close: *const fn (driver_data: *anyopaque) void,
        executeQuery: *const fn (driver_data: *anyopaque, sql: []const u8, args: []const types.Value) errors.DatabaseError!types.ResultSet,
        executeUpdate: *const fn (driver_data: *anyopaque, sql: []const u8, args: []const types.Value) errors.DatabaseError!u64,
        beginTransaction: *const fn (driver_data: *anyopaque) errors.DatabaseError!Transaction,
        getConnectionInfo: *const fn (driver_data: *anyopaque) ConnectionInfo,
    };

    pub fn connect(self: Self, config: ConnectionConfig) errors.DatabaseError!Connection {
        return self.vtable.connect(self.driver_data, config);
    }

    pub fn close(self: Self) void {
        self.vtable.close(self.driver_data);
    }

    pub fn executeQuery(self: Self, sql: []const u8, args: []const types.Value) errors.DatabaseError!types.ResultSet {
        return self.vtable.executeQuery(self.driver_data, sql, args);
    }

    pub fn executeUpdate(self: Self, sql: []const u8, args: []const types.Value) errors.DatabaseError!u64 {
        return self.vtable.executeUpdate(self.driver_data, sql, args);
    }

    pub fn beginTransaction(self: Self) errors.DatabaseError!Transaction {
        return self.vtable.beginTransaction(self.driver_data);
    }

    pub fn getConnectionInfo(self: Self) ConnectionInfo {
        return self.vtable.getConnectionInfo(self.driver_data);
    }
};

// Helper functions for argument conversion
fn convertArgs(args: anytype) ![]types.Value {
    const args_typeinfo = @typeInfo(@TypeOf(args));

    switch (args_typeinfo) {
        .@"struct" => |struct_info| {
            const values = try std.heap.page_allocator.alloc(types.Value, struct_info.fields.len);
            inline for (struct_info.fields, 0..) |field, i| {
                const field_value = @field(args, field.name);
                values[i] = convertSingleValue(field_value);
            }
            return values;
        },
        .pointer => |ptr_info| {
            if (ptr_info.size == .Slice) {
                const values = try std.heap.page_allocator.alloc(types.Value, args.len);
                for (args, 0..) |arg, i| {
                    values[i] = convertSingleValue(arg);
                }
                return values;
            }
        },
        else => {
            // Single value
            const values = try std.heap.page_allocator.alloc(types.Value, 1);
            values[0] = convertSingleValue(args);
            return values;
        },
    }

    return std.heap.page_allocator.alloc(types.Value, 0);
}

fn convertSingleValue(value: anytype) types.Value {
    const value_type = @typeInfo(@TypeOf(value));

    return switch (value_type) {
        .int => .{ .integer = @intCast(value) },
        .float => .{ .float = @floatCast(value) },
        .bool => .{ .boolean = value },
        .optional => if (value) |inner| convertSingleValue(inner) else .null,
        .pointer => |ptr_info| {
            if (@intFromEnum(ptr_info.size) == 2 and ptr_info.child == u8) {
                return .{ .text = value };
            }
            return .null;
        },
        else => .null,
    };
}

// Connection interface
pub const Connection = struct {
    const Self = @This();

    driver: Driver,
    connection_data: *anyopaque,

    pub fn query(self: Self, sql: []const u8, args: anytype) errors.DatabaseError!types.ResultSet {
        const values = try convertArgs(args);
        defer std.heap.page_allocator.free(values);
        return self.driver.executeQuery(sql, values);
    }

    pub fn exec(self: Self, sql: []const u8, args: anytype) errors.DatabaseError!u64 {
        const values = try convertArgs(args);
        defer std.heap.page_allocator.free(values);
        return self.driver.executeUpdate(sql, values);
    }

    pub fn close(self: Self) void {
        self.driver.close();
    }
};

// Transaction interface
pub const Transaction = struct {
    const Self = @This();

    driver: Driver,
    transaction_data: *anyopaque,

    pub fn commit(self: Self) !void {
        _ = self;
        // Transaction commit - delegates to driver
        // For now, return success as stub implementation
        return;
    }

    pub fn rollback(self: Self) !void {
        _ = self;
        // Transaction rollback - delegates to driver
        // For now, return success as stub implementation
        return;
    }

    pub fn query(self: Self, sql: []const u8, args: anytype) !types.ResultSet {
        // Execute query within transaction context
        const values = try convertArgs(args);
        defer std.heap.page_allocator.free(values);
        return self.driver.executeQuery(sql, values);
    }

    pub fn exec(self: Self, sql: []const u8, args: anytype) !u64 {
        // Execute update within transaction context
        const values = try convertArgs(args);
        defer std.heap.page_allocator.free(values);
        return self.driver.executeUpdate(sql, values);
    }
};

// Connection information
pub const ConnectionInfo = struct {
    database_type: types.DatabaseType,
    host: []const u8,
    database: []const u8,
    username: []const u8,
    connected_at: i64,
    server_version: ?[]const u8 = null,

    pub fn format(self: ConnectionInfo, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
        _ = fmt;
        _ = options;
        try writer.print("{} connected to {}@{}/{}", .{ self.database_type, self.username, self.host, self.database });
        if (self.server_version) |version| {
            try writer.print(" (version: {s})", .{version});
        }
    }
};

// Main Database interface
pub const Database = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    driver: Driver,
    pool: ConnectionPool,
    config: ConnectionConfig,

    pub fn connect(config: ConnectionConfig) !Self {
        const allocator = config.allocator orelse std.heap.page_allocator;

        try config.validate();

        // Create driver
        const driver = try createDriver(config.database_type, config, allocator);

        // Determine pool configuration
        const pool_config = config.pool orelse PoolConfig.defaultForDatabase(config.database_type);

        // Create pool (or noop pool)
        const pool = try ConnectionPool.init(allocator, driver, pool_config, config);

        return Self{
            .allocator = allocator,
            .driver = driver,
            .pool = pool,
            .config = config,
        };
    }

    pub fn query(self: *Self, sql: []const u8, args: anytype) !types.ResultSet {
        const conn = try self.pool.acquire();
        defer self.pool.release(conn);

        return conn.query(sql, args);
    }

    pub fn exec(self: *Self, sql: []const u8, args: anytype) !u64 {
        const conn = try self.pool.acquire();
        defer self.pool.release(conn);

        return conn.exec(sql, args);
    }

    pub fn beginTransaction(self: *Self) !Transaction {
        _ = self.pool.acquire();
        return self.driver.beginTransaction();
    }

    // Direct connection without pooling (bypass pool)
    pub fn directConnection(self: *Self) !Connection {
        return self.driver.connect(self.config);
    }

    pub fn close(self: *Self) void {
        self.pool.deinit();
        self.driver.close();
    }

    fn createDriver(db_type: types.DatabaseType, config: ConnectionConfig, allocator: std.mem.Allocator) !Driver {
        _ = config;
        return switch (db_type) {
            .sqlite => {
                const sqlite = @import("drivers/sqlite.zig");
                return sqlite.createDriver(allocator);
            },
            .postgresql => {
                const postgresql = @import("drivers/postgresql.zig");
                return postgresql.createDriver(allocator);
            },
            // .mysql => {
            //     const mysql = @import("drivers/mysql.zig");
            //     return mysql.createDriver(allocator);
            // },
            // .sqlserver => {
            //     const sqlserver = @import("drivers/sqlserver.zig");
            //     return sqlserver.createDriver(allocator);
            // },
            // .oracle => {
            //     const oracle = @import("drivers/oracle.zig");
            //     return oracle.createDriver(allocator);
            // },
            // .csv => {
            //     const csv = @import("drivers/csv.zig");
            //     return csv.createDriver(allocator);
            // },
            // .excel => {
            //     const excel = @import("drivers/excel.zig");
            //     return excel.createDriver(allocator);
            // },
            else => error.NotImplemented,
        };
    }
};

// Connection pool implementation
pub const ConnectionPool = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    config: PoolConfig,
    driver: Driver,
    connection_config: ConnectionConfig,

    // Pool state
    connections: std.ArrayList(Connection),
    available: std.ArrayList(usize),
    in_use: std.ArrayList(usize),
    mutex: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator, driver: Driver, config: PoolConfig, connection_config: ConnectionConfig) !Self {
        var pool = Self{
            .allocator = allocator,
            .config = config,
            .driver = driver,
            .connection_config = connection_config,
            .connections = std.ArrayList(Connection).initCapacity(allocator, 0) catch unreachable,
            .available = std.ArrayList(usize).initCapacity(allocator, 0) catch unreachable,
            .in_use = std.ArrayList(usize).initCapacity(allocator, 0) catch unreachable,
            .mutex = std.Thread.Mutex{},
        };

        if (config.enabled) {
            // Pre-initialize connections
            for (0..config.size) |_| {
                const conn = try driver.connect(connection_config);
                try pool.connections.append(allocator, conn);
                try pool.available.append(allocator, pool.connections.items.len - 1);
            }
        }

        return pool;
    }

    // Noop pool implementation
    pub fn noop(allocator: std.mem.Allocator, driver: Driver, connection_config: ConnectionConfig) Self {
        return Self{
            .allocator = allocator,
            .config = .{ .enabled = false, .size = 1, .max_connections = 1 },
            .driver = driver,
            .connection_config = connection_config,
            .connections = std.ArrayList(Connection).initCapacity(allocator, 0) catch unreachable,
            .available = std.ArrayList(usize).initCapacity(allocator, 0) catch unreachable,
            .in_use = std.ArrayList(usize).initCapacity(allocator, 0) catch unreachable,
            .mutex = std.Thread.Mutex{},
        };
    }

    pub fn acquire(self: *Self) !Connection {
        if (!self.config.enabled) {
            // Noop pooling - create new connection each time
            return self.driver.connect(self.connection_config);
        }

        self.mutex.lock();
        defer self.mutex.unlock();

        // Real pooling logic
        if (self.available.items.len > 0) {
            const conn_idx = self.available.orderedRemove(0);
            self.in_use.append(self.allocator, conn_idx) catch return error.OutOfMemory;
            return self.connections.items[conn_idx];
        }

        if (self.connections.items.len < self.config.max_connections) {
            const conn = try self.driver.connect(self.connection_config);
            try self.connections.append(self.allocator, conn);
            const conn_idx = self.connections.items.len - 1;
            try self.in_use.append(self.allocator, conn_idx);
            return conn;
        }

        return errors.DatabaseError.ConnectionPoolExhausted;
    }

    pub fn release(self: *Self, connection: Connection) void {
        if (!self.config.enabled) {
            // Noop pooling - just close the connection
            connection.close();
            return;
        }

        self.mutex.lock();
        defer self.mutex.unlock();

        // Find connection in in_use and move to available
        for (self.in_use.items, 0..) |conn_idx, i| {
            if (self.connections.items[conn_idx].connection_data == connection.connection_data) {
                _ = self.in_use.orderedRemove(i);
                self.available.append(self.allocator, conn_idx) catch return;
                return;
            }
        }
    }

    pub fn deinit(self: *Self) void {
        for (self.connections.items) |conn| {
            conn.close();
        }
        self.connections.deinit(self.allocator);
        self.available.deinit(self.allocator);
        self.in_use.deinit(self.allocator);
    }

    pub fn info(self: Self) PoolInfo {
        return PoolInfo{
            .enabled = self.config.enabled,
            .total_connections = @intCast(self.connections.items.len),
            .active_connections = @intCast(self.in_use.items.len),
            .available_connections = @intCast(self.available.items.len),
            .max_connections = self.config.max_connections,
            .is_noop = !self.config.enabled,
        };
    }
};

// Pool information
pub const PoolInfo = struct {
    enabled: bool,
    total_connections: u32,
    active_connections: u32,
    available_connections: u32,
    max_connections: u32,
    is_noop: bool,

    pub fn format(self: PoolInfo, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
        _ = fmt;
        _ = options;
        if (self.is_noop) {
            try writer.print("Pool: NOOP (single connections)", .{});
        } else {
            try writer.print("Pool: {}/{} active (max: {})", .{ self.active_connections, self.total_connections, self.max_connections });
        }
    }
};
