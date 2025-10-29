const std = @import("std");
const zlay = @import("../src/main.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Test SQLite connection
    const db = try zlay.connect(.{
        .database_type = .sqlite,
        .file_path = "test.db",
        .allocator = allocator,
    });
    defer db.close();

    std.debug.print("Connected to SQLite database\n");

    // Create a test table
    const create_result = try db.exec(
        \\ CREATE TABLE IF NOT EXISTS users (
        \\     id INTEGER PRIMARY KEY,
        \\     name TEXT NOT NULL,
        \\     email TEXT,
        \\     age INTEGER
        \\ )
    , .{});

    std.debug.print("Created table, affected rows: {}\n", .{create_result});

    // Insert some test data
    const insert_result = try db.exec("INSERT INTO users (name, email, age) VALUES (?, ?, ?)", .{ "Alice", "alice@example.com", 30 });
    std.debug.print("Inserted row, affected rows: {}\n", .{insert_result});

    // Query the data
    const result = try db.query("SELECT id, name, email, age FROM users", .{});
    defer result.deinit();

    std.debug.print("Query results:\n");
    std.debug.print("{any}\n", .{result});

    // Iterate through results
    var row_iter = result.iterator();
    while (row_iter.next()) |row| {
        const id = row.getInteger(0);
        const name = row.getString(1);
        const email = row.getString(2);
        const age = row.getInteger(3);

        std.debug.print("User: {} - {s} ({s}) - age {}\n", .{ id.?, name.?, email.?, age.? });
    }
}
