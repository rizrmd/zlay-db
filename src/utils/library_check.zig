const std = @import("std");
const library_manager = @import("library_manager.zig");

pub fn main() !void {
    const allocator = std.heap.page_allocator;
    var manager = library_manager.LibraryManager.init(allocator);
    defer manager.deinit();

    try manager.listAvailableLibraries();

    std.debug.print("\n💡 Libraries will be auto-installed when you first use a database driver.\n", .{});
    std.debug.print("   Run 'zig build setup' to check and install all libraries now.\n", .{});
}
