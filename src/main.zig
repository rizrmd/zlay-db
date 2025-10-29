const std = @import("std");
const database = @import("database.zig");
const types = @import("utils/types.zig");
const errors = @import("utils/errors.zig");

// Public API exports
pub const DatabaseType = types.DatabaseType;
pub const Database = database.Database;
pub const ConnectionConfig = database.ConnectionConfig;
pub const PoolConfig = database.PoolConfig;
pub const Value = types.Value;
pub const ResultSet = types.ResultSet;
pub const Row = types.Row;
pub const Column = types.Column;
pub const DatabaseError = errors.DatabaseError;
pub const DatabaseErrorWithContext = errors.DatabaseErrorWithContext;

// Convenience functions for common operations
pub fn connect(config: ConnectionConfig) !Database {
    return Database.connect(config);
}

// Builder pattern for connection configuration
pub const ConnectionBuilder = struct {
    config: ConnectionConfig,

    pub fn init(db_type: DatabaseType) ConnectionBuilder {
        return ConnectionBuilder{
            .config = ConnectionConfig{
                .database_type = db_type,
            },
        };
    }

    pub fn connectionString(self: *ConnectionBuilder, conn_str: []const u8) *ConnectionBuilder {
        self.config.connection_string = conn_str;
        return self;
    }

    pub fn host(self: *ConnectionBuilder, host_val: []const u8) *ConnectionBuilder {
        self.config.host = host_val;
        return self;
    }

    pub fn port(self: *ConnectionBuilder, port_val: u16) *ConnectionBuilder {
        self.config.port = port_val;
        return self;
    }

    pub fn database(self: *ConnectionBuilder, database_val: []const u8) *ConnectionBuilder {
        self.config.database = database_val;
        return self;
    }

    pub fn username(self: *ConnectionBuilder, username_val: []const u8) *ConnectionBuilder {
        self.config.username = username_val;
        return self;
    }

    pub fn password(self: *ConnectionBuilder, password_val: []const u8) *ConnectionBuilder {
        self.config.password = password_val;
        return self;
    }

    pub fn filePath(self: *ConnectionBuilder, file_path: []const u8) *ConnectionBuilder {
        self.config.file_path = file_path;
        return self;
    }

    pub fn poolSize(self: *ConnectionBuilder, size: u32) *ConnectionBuilder {
        if (self.config.pool == null) {
            self.config.pool = PoolConfig{};
        }
        self.config.pool.?.size = size;
        return self;
    }

    pub fn timeout(self: *ConnectionBuilder, timeout_ms: u32) *ConnectionBuilder {
        self.config.timeout_ms = timeout_ms;
        return self;
    }

    pub fn sslMode(self: *ConnectionBuilder, ssl_mode: []const u8) *ConnectionBuilder {
        self.config.ssl_mode = ssl_mode;
        return self;
    }

    pub fn allocator(self: *ConnectionBuilder, alloc: std.mem.Allocator) *ConnectionBuilder {
        self.config.allocator = alloc;
        return self;
    }

    pub fn build(self: *ConnectionBuilder) !Database {
        return Database.connect(self.config);
    }
};

// Utility functions
pub const utils = struct {
    pub fn parseConnectionString(url: []const u8, db_type: DatabaseType) !ConnectionConfig {
        var config = ConnectionConfig{
            .database_type = db_type,
            .connection_string = url,
        };

        // Parse URL format: protocol://[username[:password]@]host[:port]/database[?options]
        if (std.mem.indexOf(u8, url, "://")) |protocol_end| {
            const rest = url[protocol_end + 3 ..];

            // Check for credentials
            if (std.mem.indexOf(u8, rest, "@")) |creds_end| {
                const creds_part = rest[0..creds_end];
                if (std.mem.indexOf(u8, creds_part, ":")) |colon_pos| {
                    config.username = creds_part[0..colon_pos];
                    config.password = creds_part[colon_pos + 1 ..];
                } else {
                    config.username = creds_part;
                }

                const host_part = rest[creds_end + 1 ..];
                try parseHostAndDatabase(host_part, &config);
            } else {
                try parseHostAndDatabase(rest, &config);
            }
        }

        return config;
    }

    fn parseHostAndDatabase(host_part: []const u8, config: *ConnectionConfig) !void {
        // Split host and database
        if (std.mem.indexOf(u8, host_part, "/")) |slash_pos| {
            const host_db_part = host_part[0..slash_pos];
            config.database = host_part[slash_pos + 1 ..];

            // Parse port if present
            if (std.mem.indexOf(u8, host_db_part, ":")) |colon_pos| {
                config.host = host_db_part[0..colon_pos];
                const port_str = host_db_part[colon_pos + 1 ..];
                config.port = try std.fmt.parseInt(u16, port_str, 10);
            } else {
                config.host = host_db_part;
            }
        } else {
            config.host = host_part;
        }
    }

    pub fn buildConnectionString(config: ConnectionConfig) ![]const u8 {
        var buffer = std.ArrayList(u8).init(std.heap.page_allocator);
        defer buffer.deinit();

        try buffer.appendSlice(config.database_type.toString());
        try buffer.appendSlice("://");

        if (config.username) |username| {
            try buffer.appendSlice(username);
            if (config.password) |password| {
                try buffer.append(":");
                try buffer.appendSlice(password);
            }
            try buffer.append("@");
        }

        if (config.host) |host| {
            try buffer.appendSlice(host);
        }

        if (config.port) |port| {
            try buffer.print(":{}", .{port});
        }

        if (config.database) |db_name| {
            try buffer.append("/");
            try buffer.appendSlice(db_name);
        }

        return buffer.toOwnedSlice();
    }
};

// Version information
pub const version = struct {
    pub const major = 0;
    pub const minor = 1;
    pub const patch = 0;

    pub fn string() []const u8 {
        return "0.1.0";
    }
};

// Test function to verify the library compiles
pub fn testLibrary() !void {
    std.debug.print("zlay-db version {s} - Library test successful!\n", .{version.string()});

    // Test basic types
    const value = Value{ .integer = 42 };
    std.debug.print("Test value: {any}\n", .{value});

    // Test database type
    const db_type = DatabaseType.postgresql;
    std.debug.print("Test database type: {any}\n", .{db_type});

    // Test connection builder
    var builder = ConnectionBuilder.init(DatabaseType.postgresql);
    _ = builder.host("localhost")
        .port(5432)
        .database("testdb")
        .username("testuser")
        .password("testpass")
        .poolSize(10);

    std.debug.print("Connection builder test successful!\n", .{});
}

// Simple test when run as executable
pub fn main() !void {
    try testLibrary();
}
