const std = @import("std");

// Database types supported by zlay-db
pub const DatabaseType = enum {
    postgresql,
    mysql,
    sqlite,
    sqlserver,
    oracle,
    clickhouse,
    excel,
    csv,

    pub fn jsonStringify(self: DatabaseType, _: std.json.StringifyOptions, writer: anytype) !void {
        try writer.writeAll(@tagName(self));
    }
};

// Data types for database values
pub const ValueType = enum {
    null,
    integer,
    float,
    text,
    boolean,
    binary,
    date,
    time,
    timestamp,
};

// Unified value representation
pub const Value = union(ValueType) {
    null: void,
    integer: i64,
    float: f64,
    text: []const u8,
    boolean: bool,
    binary: []const u8,
    date: Date,
    time: Time,
    timestamp: Timestamp,

    pub const Date = struct {
        year: u16,
        month: u8,
        day: u8,
    };

    pub const Time = struct {
        hour: u8,
        minute: u8,
        second: u8,
        nanosecond: u32 = 0,
    };

    pub const Timestamp = struct {
        date: Date,
        time: Time,
    };

    pub fn format(self: Value, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
        _ = fmt;
        _ = options;
        switch (self) {
            .null => try writer.writeAll("NULL"),
            .integer => |v| try writer.print("{}", .{v}),
            .float => |v| try writer.print("{}", .{v}),
            .text => |v| try writer.print("'{s}'", .{v}),
            .boolean => |v| try writer.print("{}", .{v}),
            .binary => |v| try writer.print("x'{}'", .{std.fmt.fmtSliceHexLower(v)}),
            .date => |v| try writer.print("{d:0>4}-{d:0>2}-{d:0>2}", .{ v.year, v.month, v.day }),
            .time => |v| try writer.print("{d:0>2}:{d:0>2}:{d:0>2}", .{ v.hour, v.minute, v.second }),
            .timestamp => |v| try writer.print("{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}", .{
                v.date.year, v.date.month,  v.date.day,
                v.time.hour, v.time.minute, v.time.second,
            }),
        }
    }

    pub fn asInteger(self: Value) ?i64 {
        return switch (self) {
            .integer => |v| v,
            .float => |v| @intFromFloat(v),
            .text => |v| std.fmt.parseInt(i64, v, 10) catch null,
            .boolean => |v| if (v) 1 else 0,
            else => null,
        };
    }

    pub fn asFloat(self: Value) ?f64 {
        return switch (self) {
            .float => |v| v,
            .integer => |v| @floatFromInt(v),
            .text => |v| std.fmt.parseFloat(f64, v) catch null,
            .boolean => |v| if (v) 1.0 else 0.0,
            else => null,
        };
    }

    pub fn asText(self: Value) ?[]const u8 {
        return switch (self) {
            .text => |v| v,
            .integer => |v| std.fmt.allocPrint(std.heap.page_allocator, "{}", .{v}) catch null,
            .float => |v| std.fmt.allocPrint(std.heap.page_allocator, "{}", .{v}) catch null,
            .boolean => |v| if (v) "true" else "false",
            .null => "NULL",
            else => null,
        };
    }

    pub fn asBoolean(self: Value) ?bool {
        return switch (self) {
            .boolean => |v| v,
            .integer => |v| v != 0,
            .float => |v| v != 0.0,
            .text => |v| std.ascii.eqlIgnoreCase(v, "true") or std.ascii.eqlIgnoreCase(v, "t") or std.ascii.eqlIgnoreCase(v, "1"),
            else => null,
        };
    }
};

// Column metadata
pub const Column = struct {
    name: []const u8,
    type: ValueType,
    nullable: bool,
    default_value: ?Value = null,

    pub fn format(self: Column, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
        _ = fmt;
        _ = options;
        try writer.print("{s}: {}", .{ self.name, self.type });
        if (self.nullable) try writer.writeAll("?");
        if (self.default_value) |default| {
            try writer.print(" DEFAULT {}", .{default});
        }
    }
};

// Row representation
pub const Row = struct {
    values: []Value,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, values: []Value) Row {
        return Row{
            .values = values,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: Row) void {
        for (self.values) |*value| {
            switch (value.*) {
                .text => |text| self.allocator.free(text),
                .binary => |binary| self.allocator.free(binary),
                else => {},
            }
        }
        self.allocator.free(self.values);
    }

    pub fn len(self: Row) usize {
        return self.values.len;
    }

    pub fn get(self: Row, index: usize) ?Value {
        if (index >= self.values.len) return null;
        return self.values[index];
    }

    pub fn getString(self: Row, index: usize) ?[]const u8 {
        return self.get(index).?.asText();
    }

    pub fn getInteger(self: Row, index: usize) ?i64 {
        return self.get(index).?.asInteger();
    }

    pub fn getFloat(self: Row, index: usize) ?f64 {
        return self.get(index).?.asFloat();
    }

    pub fn getBoolean(self: Row, index: usize) ?bool {
        return self.get(index).?.asBoolean();
    }
};

// Row iterator
pub const RowIterator = struct {
    result_set: *ResultSet,
    current_row: usize = 0,

    pub fn next(self: *RowIterator) ?Row {
        if (self.current_row >= self.result_set.rows.len) return null;

        const row = self.result_set.rows[self.current_row];
        self.current_row += 1;
        return row;
    }

    pub fn reset(self: *RowIterator) void {
        self.current_row = 0;
    }
};

// Result set
pub const ResultSet = struct {
    columns: []Column,
    rows: []Row,
    allocator: std.mem.Allocator,
    affected_rows: u64 = 0,

    pub fn init(allocator: std.mem.Allocator, columns: []Column, rows: []Row) ResultSet {
        return ResultSet{
            .columns = columns,
            .rows = rows,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: ResultSet) void {
        for (self.columns) |*column| {
            self.allocator.free(column.name);
        }
        self.allocator.free(self.columns);

        for (self.rows) |*row| {
            row.deinit();
        }
        self.allocator.free(self.rows);
    }

    pub fn columnCount(self: ResultSet) usize {
        return self.columns.len;
    }

    pub fn rowCount(self: ResultSet) usize {
        return self.rows.len;
    }

    pub fn getColumnIndex(self: ResultSet, name: []const u8) ?usize {
        for (self.columns, 0..) |column, i| {
            if (std.mem.eql(u8, column.name, name)) return i;
        }
        return null;
    }

    pub fn getValue(self: ResultSet, row: usize, col: usize) ?Value {
        if (row >= self.rows.len or col >= self.columns.len) return null;
        return self.rows[row].get(col);
    }

    pub fn iterator(self: *ResultSet) RowIterator {
        return RowIterator{
            .result_set = self,
        };
    }

    pub fn format(self: ResultSet, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
        _ = fmt;
        _ = options;

        // Print header
        for (self.columns, 0..) |column, i| {
            if (i > 0) try writer.writeAll(" | ");
            try writer.print("{s}", .{column.name});
        }
        try writer.writeAll("\n");

        // Print separator
        for (self.columns, 0..) |column, i| {
            if (i > 0) try writer.writeAll("-+-");
            for (0..column.name.len) |_| try writer.writeAll("-");
        }
        try writer.writeAll("\n");

        // Print rows
        for (self.rows) |row| {
            for (row.values, 0..) |value, i| {
                if (i > 0) try writer.writeAll(" | ");
                try writer.print("{}", .{value});
            }
            try writer.writeAll("\n");
        }
    }
};
