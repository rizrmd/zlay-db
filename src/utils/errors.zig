const std = @import("std");

// Comprehensive error types for zlay-db
pub const DatabaseError = error{
    // Connection errors
    ConnectionFailed,
    ConnectionTimeout,
    ConnectionLost,
    InvalidConnection,
    ConnectionPoolExhausted,

    // Authentication errors
    AuthenticationFailed,
    PermissionDenied,

    // Query errors
    QueryFailed,
    QueryTimeout,
    InvalidQuery,
    QueryCancelled,

    // Data errors
    DataConversionFailed,
    InvalidDataType,
    DataTooLarge,
    NullValueNotAllowed,

    // Transaction errors
    TransactionFailed,
    TransactionAborted,
    TransactionTimeout,
    DeadlockDetected,

    // Configuration errors
    InvalidConfiguration,
    MissingRequiredField,
    InvalidDatabaseType,
    InvalidConnectionString,
    UnsupportedProtocol,

    // Resource errors
    OutOfMemory,
    FileNotFound,
    PermissionError,
    DiskFull,

    // Network errors
    NetworkError,
    HostNotFound,
    PortInvalid,

    // Driver-specific errors
    DriverNotFound,
    DriverInitializationFailed,
    DriverNotSupported,
    LibraryNotFound,

    // Pooling errors
    PoolInitializationFailed,
    PoolShutdownFailed,

    // File-based database errors
    FileCorrupted,
    InvalidFileFormat,

    // DuckDB specific errors (for Excel/CSV)
    DuckDBError,
    InvalidExcelFormat,
    InvalidCSVFormat,

    // Generic errors
    UnknownError,
    NotImplemented,
};

// Error context for better debugging
pub const ErrorContext = struct {
    database_type: []const u8,
    operation: []const u8,
    details: ?[]const u8 = null,
    error_code: ?i32 = null,

    pub fn format(self: ErrorContext, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
        _ = fmt;
        _ = options;
        try writer.print("{s} error in {s}", .{ self.database_type, self.operation });
        if (self.details) |details| {
            try writer.print(": {s}", .{details});
        }
        if (self.error_code) |code| {
            try writer.print(" (code: {})", .{code});
        }
    }
};

// Enhanced error with context
pub const DatabaseErrorWithContext = struct {
    err: DatabaseError,
    context: ErrorContext,

    pub fn format(self: DatabaseErrorWithContext, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
        _ = fmt;
        _ = options;
        try writer.print("{}: {}", .{ self.err, self.context });
    }
};

// Error mapping from database-specific errors to unified errors
pub const ErrorMapper = struct {
    pub fn mapPostgresError(pg_error: c_int) DatabaseError {
        return switch (pg_error) {
            // Connection errors
            0x8000...0x8007 => .ConnectionFailed,
            0x8008 => .ConnectionTimeout,

            // Authentication errors
            0x28000 => .AuthenticationFailed,

            // Query errors
            0x42601 => .InvalidQuery,
            0x42703 => .InvalidColumn,
            0x42P01 => .InvalidTable,

            // Transaction errors
            0x40P01 => .DeadlockDetected,
            0x25P02 => .TransactionAborted,

            // Data errors
            0x22012 => .DivisionByZero,
            0x22003 => .NumericValueOutOfRange,

            else => .UnknownError,
        };
    }

    pub fn mapMySQLError(mysql_error: c_uint) DatabaseError {
        return switch (mysql_error) {
            // Connection errors
            1045 => .AuthenticationFailed,
            1049 => .InvalidDatabase,
            2003 => .ConnectionFailed,
            2005 => .HostNotFound,

            // Query errors
            1064 => .InvalidQuery,
            1054 => .InvalidColumn,
            1146 => .InvalidTable,

            // Transaction errors
            1205 => .DeadlockDetected,
            1213 => .DeadlockDetected,

            // Data errors
            1366 => .DataConversionFailed,
            1292 => .DataConversionFailed,

            else => .UnknownError,
        };
    }

    pub fn mapSQLiteError(sqlite_error: c_int) DatabaseError {
        return switch (sqlite_error) {
            // Connection errors
            14 => .ConnectionFailed,
            23 => .PermissionDenied,

            // Query errors
            1 => .QueryFailed,
            19 => .InvalidConstraint,

            // Transaction errors
            5 => .DatabaseLocked,
            6 => .TableLocked,

            // Data errors
            20 => .DataMismatch,
            21 => .MissingColumn,

            // File errors
            14 => .FileNotFound,
            26 => .FileCorrupted,

            else => .UnknownError,
        };
    }

    pub fn mapODBCError(odbc_error: c_int) DatabaseError {
        return switch (odbc_error) {
            // Connection errors
            8001...8004 => .ConnectionFailed,
            8007 => .ConnectionLost,

            // Authentication errors
            28000 => .AuthenticationFailed,

            // Query errors
            42000 => .InvalidQuery,
            42002 => .InvalidTable,
            42022 => .InvalidColumn,

            // Transaction errors
            40001 => .DeadlockDetected,
            40001 => .DeadlockDetected,

            else => .UnknownError,
        };
    }
};

// Error recovery strategies
pub const RecoveryStrategy = enum {
    none,
    retry,
    reconnect,
    abort,

    pub fn forError(err: DatabaseError) RecoveryStrategy {
        return switch (err) {
            // Retryable errors
            .ConnectionTimeout, .QueryTimeout, .DeadlockDetected => .retry,

            // Reconnectable errors
            .ConnectionLost, .ConnectionFailed => .reconnect,

            // Fatal errors
            .AuthenticationFailed, .PermissionDenied, .InvalidConfiguration => .abort,

            // Data errors - no recovery
            .DataConversionFailed, .InvalidDataType, .NullValueNotAllowed => .none,

            // Default to no recovery
            else => .none,
        };
    }
};

// Error logging utilities
pub const ErrorLogger = struct {
    allocator: std.mem.Allocator,
    log_file: ?std.fs.File = null,

    pub fn init(allocator: std.mem.Allocator) ErrorLogger {
        return ErrorLogger{
            .allocator = allocator,
        };
    }

    pub fn log(self: ErrorLogger, err: DatabaseErrorWithContext) void {
        const timestamp = std.time.timestamp();
        const message = std.fmt.allocPrint(self.allocator, "[{}] {}\n", .{ timestamp, err }) catch return;
        defer self.allocator.free(message);

        std.debug.print("{s}", .{message});

        if (self.log_file) |file| {
            file.writeAll(message) catch {};
        }
    }

    pub fn enableFileLogging(self: *ErrorLogger, file_path: []const u8) !void {
        const file = try std.fs.createFileAbsolute(file_path, .{ .truncate = true });
        self.log_file = file;
    }

    pub fn deinit(self: ErrorLogger) void {
        if (self.log_file) |file| {
            file.close();
        }
    }
};
