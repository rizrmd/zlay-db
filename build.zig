const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Create library check command
    const library_check = b.addSystemCommand(&.{
        "sh", "-c",
        \\echo "🔍 Checking database library availability..."
        \\zig run src/utils/library_check.zig
    });

    // Add setup step
    const setup_step = b.step("setup", "Check and install required database libraries");
    setup_step.dependOn(&library_check.step);

    // Create main executable
    const exe = b.addExecutable(.{
        .name = "zlay-db",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    // Add library flags based on available libraries
    exe.linkLibC();

    // SQLite (always available)
    exe.linkSystemLibrary("sqlite3");

    // Optional libraries - comment out if not available
    // MySQL
    // exe.linkSystemLibrary("mysqlclient");

    // PostgreSQL (if available)
    // exe.linkSystemLibrary("pq");

    // DuckDB (if available)
    // exe.linkSystemLibrary("duckdb");

    // ODBC (if available)
    // exe.linkSystemLibrary("odbc");

    b.installArtifact(exe);

    // Run step
    const run_cmd = b.addRunArtifact(exe);
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run", "Run the application");
    run_step.dependOn(&run_cmd.step);

    // Test step that depends on library check
    const unit_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/database.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    unit_tests.linkLibC();
    unit_tests.linkSystemLibrary("sqlite3");
    // Optional libraries - comment out if not available
    // unit_tests.linkSystemLibrary("mysqlclient");
    // unit_tests.linkSystemLibrary("pq");
    // unit_tests.linkSystemLibrary("duckdb");
    // unit_tests.linkSystemLibrary("odbc");

    const run_unit_tests = b.addRunArtifact(unit_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_unit_tests.step);
    test_step.dependOn(&library_check.step);
}
