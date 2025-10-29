const std = @import("std");
const errors = @import("errors.zig");

pub const LibraryType = enum {
    sqlite,
    mysql,
    postgresql,
    odbc,
    duckdb,
    oracle,
};

pub const LibraryInfo = struct {
    name: []const u8,
    homebrew_package: []const u8,
    apt_package: []const u8,
    test_function: []const u8,
    headers: []const []const u8,
    libraries: []const []const u8,
};

pub const library_configs = [_]struct { []const u8, LibraryInfo }{
    .{ "sqlite", LibraryInfo{
        .name = "SQLite",
        .homebrew_package = "sqlite3",
        .apt_package = "libsqlite3-dev",
        .test_function = "sqlite3_open",
        .headers = &[_][]const u8{"sqlite3.h"},
        .libraries = &[_][]const u8{"sqlite3"},
    } },
    .{ "mysql", LibraryInfo{
        .name = "MySQL Client Library",
        .homebrew_package = "mysql-client",
        .apt_package = "libmysqlclient-dev",
        .test_function = "mysql_init",
        .headers = &[_][]const u8{"mysql.h"},
        .libraries = &[_][]const u8{"mysqlclient"},
    } },
    .{ "postgresql", LibraryInfo{
        .name = "PostgreSQL Client Library",
        .homebrew_package = "libpq",
        .apt_package = "libpq-dev",
        .test_function = "PQconnectdb",
        .headers = &[_][]const u8{"libpq-fe.h"},
        .libraries = &[_][]const u8{"pq"},
    } },
    .{ "odbc", LibraryInfo{
        .name = "ODBC Driver Manager",
        .homebrew_package = "unixodbc",
        .apt_package = "unixodbc-dev",
        .test_function = "SQLAllocHandle",
        .headers = &[_][]const u8{ "sql.h", "sqlext.h" },
        .libraries = &[_][]const u8{"odbc"},
    } },
    .{ "duckdb", LibraryInfo{
        .name = "DuckDB",
        .homebrew_package = "duckdb",
        .apt_package = "libduckdb-dev",
        .test_function = "duckdb_open",
        .headers = &[_][]const u8{"duckdb.h"},
        .libraries = &[_][]const u8{"duckdb"},
    } },
    .{ "oracle", LibraryInfo{
        .name = "Oracle Instant Client",
        .homebrew_package = "instantclient-basic",
        .apt_package = "oracle-instantclient-basic",
        .test_function = "OCIEnvCreate",
        .headers = &[_][]const u8{"oci.h"},
        .libraries = &[_][]const u8{"clntsh"},
    } },
};

pub fn getLibraryConfig(name: []const u8) ?LibraryInfo {
    for (library_configs) |config| {
        if (std.mem.eql(u8, config[0], name)) {
            return config[1];
        }
    }
    return null;
}

pub const LibraryManager = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    checked_libraries: std.StringHashMap(bool),

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .allocator = allocator,
            .checked_libraries = std.StringHashMap(bool).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        self.checked_libraries.deinit();
    }

    pub fn isLibraryAvailableByName(self: *Self, library_name: []const u8) !bool {
        const config = getLibraryConfig(library_name) orelse return false;
        return self.isLibraryAvailableConfig(config);
    }

    pub fn ensureLibrary(self: *Self, library_type: LibraryType) !void {
        const library_name = @tagName(library_type);

        // Check if we already verified this library
        if (self.checked_libraries.get(library_name)) |checked| {
            if (checked) return;
        }

        // Check if library is available
        if (try self.isLibraryAvailable(library_type)) {
            try self.checked_libraries.put(library_name, true);
            return;
        }

        // Try to install the library
        try self.installLibrary(library_type);

        // Verify installation
        if (try self.isLibraryAvailable(library_type)) {
            try self.checked_libraries.put(library_name, true);
            return;
        }

        return errors.DatabaseError.LibraryNotFound;
    }

    fn isLibraryAvailable(self: *Self, library_type: LibraryType) !bool {
        const library_name = @tagName(library_type);
        const config = getLibraryConfig(library_name) orelse return false;
        return self.isLibraryAvailableConfig(config);
    }

    fn isLibraryAvailableConfig(_: *Self, config: LibraryInfo) !bool {
        // Try to compile a test program
        const test_code = try std.fmt.allocPrint(std.heap.page_allocator,
            \\#include <{s}>
            \\int main() {{
            \\    void* ptr = (void*){s};
            \\    return 0;
            \\}}
        , .{ config.headers[0], config.test_function });
        defer std.heap.page_allocator.free(test_code);

        const test_file = try std.fs.cwd().createFile("test_library.c", .{});
        defer test_file.close();

        try test_file.writeAll(test_code);

        // Try to compile with different link flags
        for (config.libraries) |lib| {
            const compile_cmd = try std.fmt.allocPrint(std.heap.page_allocator, "gcc -o test_library test_library.c -l{s} 2>/dev/null", .{lib});
            defer std.heap.page_allocator.free(compile_cmd);

            const result = std.process.Child.run(.{
                .allocator = std.heap.page_allocator,
                .argv = &[_][]const u8{ "sh", "-c", compile_cmd },
            }) catch continue;

            defer std.heap.page_allocator.free(result.stdout);
            defer std.heap.page_allocator.free(result.stderr);

            if (result.term.Exited == 0) {
                // Clean up test files
                _ = std.process.Child.run(.{
                    .allocator = std.heap.page_allocator,
                    .argv = &[_][]const u8{ "rm", "-f", "test_library.c", "test_library" },
                }) catch {};
                return true;
            }
        }

        // Clean up test files
        _ = std.process.Child.run(.{
            .allocator = std.heap.page_allocator,
            .argv = &[_][]const u8{ "rm", "-f", "test_library.c", "test_library" },
        }) catch {};

        return false;
    }

    fn installLibrary(self: *Self, library_type: LibraryType) !void {
        const config = library_configs.get(@tagName(library_type)).?;

        // Detect package manager
        const package_manager = try self.detectPackageManager();

        const install_cmd = switch (package_manager) {
            .homebrew => try std.fmt.allocPrint(self.allocator, "brew install {s}", .{config.homebrew_package}),
            .apt => try std.fmt.allocPrint(self.allocator, "sudo apt-get update && sudo apt-get install -y {s}", .{config.apt_package}),
            .yum => try std.fmt.allocPrint(self.allocator, "sudo yum install -y {s}", .{config.apt_package}),
            .none => {
                std.debug.print("Cannot auto-install {s}. Please install manually.\n", .{config.name});
                std.debug.print("On macOS: brew install {s}\n", .{config.homebrew_package});
                std.debug.print("On Ubuntu/Debian: sudo apt-get install {s}\n", .{config.apt_package});
                return errors.DatabaseError.LibraryNotFound;
            },
        };
        defer self.allocator.free(install_cmd);

        std.debug.print("Installing {s}...\n", .{config.name});

        const result = try std.process.Child.run(.{
            .allocator = self.allocator,
            .argv = &[_][]const u8{ "sh", "-c", install_cmd },
        });
        defer self.allocator.free(result.stdout);
        defer self.allocator.free(result.stderr);

        if (result.term.Exited != 0) {
            std.debug.print("Failed to install {s}: {s}\n", .{ config.name, result.stderr });
            return errors.DatabaseError.LibraryNotFound;
        }

        std.debug.print("Successfully installed {s}\n", .{config.name});
    }

    const PackageManager = enum {
        homebrew,
        apt,
        yum,
        none,
    };

    fn detectPackageManager(_: *Self) !PackageManager {

        // Check for Homebrew (macOS)
        const brew_result = try std.process.Child.run(.{
            .allocator = std.heap.page_allocator,
            .argv = &[_][]const u8{ "which", "brew" },
        });
        defer std.heap.page_allocator.free(brew_result.stdout);
        defer std.heap.page_allocator.free(brew_result.stderr);

        if (brew_result.term.Exited == 0) {
            return .homebrew;
        }

        // Check for APT (Debian/Ubuntu)
        const apt_result = try std.process.Child.run(.{
            .allocator = std.heap.page_allocator,
            .argv = &[_][]const u8{ "which", "apt-get" },
        });
        defer std.heap.page_allocator.free(apt_result.stdout);
        defer std.heap.page_allocator.free(apt_result.stderr);

        if (apt_result.term.Exited == 0) {
            return .apt;
        }

        // Check for YUM (RHEL/CentOS)
        const yum_result = try std.process.Child.run(.{
            .allocator = std.heap.page_allocator,
            .argv = &[_][]const u8{ "which", "yum" },
        });
        defer std.heap.page_allocator.free(yum_result.stdout);
        defer std.heap.page_allocator.free(yum_result.stderr);

        if (yum_result.term.Exited == 0) {
            return .yum;
        }

        return .none;
    }

    pub fn getLibraryFlags(library_type: LibraryType) []const []const u8 {
        return switch (library_type) {
            .sqlite => &[_][]const u8{ "-lc", "-lsqlite3" },
            .mysql => &[_][]const u8{ "-lc", "-lmysqlclient" },
            .postgresql => &[_][]const u8{ "-lc", "-lpq" },
            .odbc => &[_][]const u8{ "-lc", "-lodbc" },
            .duckdb => &[_][]const u8{ "-lc", "-lduckdb" },
            .oracle => &[_][]const u8{ "-lc", "-lclntsh" },
        };
    }

    pub fn listAvailableLibraries(self: *Self) !void {
        std.debug.print("Checking database library availability...\n", .{});

        for (library_configs) |config_pair| {
            const name = config_pair[0];
            const config = config_pair[1];

            const available = self.isLibraryAvailableByName(name) catch false;
            const status = if (available) "✓ Available" else "✗ Not found";

            std.debug.print("  {s}: {s}\n", .{ config.name, status });
        }
    }
};

// Global library manager instance
var global_library_manager: ?LibraryManager = null;
var library_manager_mutex = std.Thread.Mutex{};

pub fn getLibraryManager(allocator: std.mem.Allocator) *LibraryManager {
    library_manager_mutex.lock();
    defer library_manager_mutex.unlock();

    if (global_library_manager == null) {
        global_library_manager = allocator.create(LibraryManager) catch unreachable;
        global_library_manager.?.* = LibraryManager.init(allocator);
    }

    return global_library_manager.?;
}

pub fn ensureLibrary(library_type: LibraryType) !void {
    const manager = getLibraryManager(std.heap.page_allocator);
    try manager.ensureLibrary(library_type);
}
