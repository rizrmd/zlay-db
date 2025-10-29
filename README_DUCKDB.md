# DuckDB Setup for zlay-db

This guide explains how to set up DuckDB library for the CSV and Excel drivers in zlay-db.

## Overview

The zlay-db project uses DuckDB as the backend for:
- **CSV Driver**: Reads CSV files using DuckDB's `read_csv_auto()` function
- **Excel Driver**: Reads Excel files using DuckDB's spatial extension `st_read_auto()` function

## Installation

### macOS (Recommended)

1. **Install via Homebrew** (easiest):
   ```bash
   brew install duckdb
   ```

2. **Or run the setup script**:
   ```bash
   ./setup_duckdb.sh
   ```

### Linux

1. **Download pre-built binaries**:
   ```bash
   wget https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-amd64.zip
   unzip duckdb_cli-linux-amd64.zip
   sudo cp duckdb /usr/local/bin/
   ```

2. **Install development libraries**:
   ```bash
   # Ubuntu/Debian
   sudo apt-get update
   sudo apt-get install build-essential cmake git
   
   # Build from source
   git clone https://github.com/duckdb/duckdb.git
   cd duckdb
   make
   sudo make install
   ```

### Windows

1. **Download from releases**:
   - Visit: https://github.com/duckdb/duckdb/releases
   - Download `duckdb_cli-windows-amd64.zip`
   - Extract to a directory of your choice

2. **Add to PATH**:
   - Add the extracted directory to your system PATH

## Build Configuration

### Automatic Setup (Recommended)

Run the setup script:
```bash
./setup_duckdb.sh
```

### Manual Configuration

If you prefer manual setup, update `build.zig` with your DuckDB paths:

```zig
const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // DuckDB configuration
    const duckdb = b.dependency("duckdb", .{
        .target = target,
        .optimize = optimize,
    });

    const exe = b.addExecutable(.{
        .name = "zlay-db",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    // Link DuckDB
    exe.linkLibrary(duckdb.artifact("duckdb"));
    exe.root_module.addImport("duckdb", duckdb.module("duckdb"));

    b.installArtifact(exe);
}
```

## Verification

### Test CSV Driver

1. **Create a test CSV file**:
   ```bash
   echo "id,name,age" > test.csv
   echo "1,Alice,25" >> test.csv
   echo "2,Bob,30" >> test.csv
   ```

2. **Run the test**:
   ```bash
   zig test test_csv.zig
   ```

### Test Excel Driver

1. **Create a test Excel file** (using LibreOffice, Excel, etc.)
2. **Run the test**:
   ```bash
   zig test test_excel.zig
   ```

## Troubleshooting

### Common Issues

1. **"undefined symbol: _duckdb_*" errors**:
   - DuckDB library is not linked properly
   - Run `./setup_duckdb.sh` to fix paths

2. **"duckdb.h not found"**:
   - DuckDB development headers are missing
   - Install DuckDB with development packages

3. **"spatial extension not found"**:
   - DuckDB spatial extension is required for Excel files
   - Ensure DuckDB is built with spatial extension support

### Manual Library Paths

If automatic detection fails, set these environment variables:

```bash
export DUCKDB_INCLUDE_PATH="/path/to/duckdb/include"
export DUCKDB_LIB_PATH="/path/to/duckdb/lib"
```

## API Usage

### CSV Driver Example

```zig
const csv = @import("src/drivers/csv.zig");

var driver = csv.CSVDriver.init(allocator);
defer driver.deinit();

const config = database.ConnectionConfig{
    .file_path = "data.csv",
};

const conn = try driver.connect(config);
defer conn.close();

const result = try conn.executeQuery("SELECT * FROM csv_data WHERE age > 25", &.{});
```

### Excel Driver Example

```zig
const excel = @import("src/drivers/excel.zig");

var driver = excel.ExcelDriver.init(allocator);
defer driver.deinit();

const config = database.ConnectionConfig{
    .file_path = "data.xlsx",
};

const conn = try driver.connect(config);
defer conn.close();

const result = try conn.executeQuery("SELECT * FROM excel_data WHERE column1 = 'value'", &.{});
```

## Performance Notes

- **CSV files**: DuckDB automatically detects delimiters, headers, and types
- **Excel files**: Requires spatial extension for `.xlsx` support
- **Memory usage**: DuckDB uses in-memory processing for best performance
- **Large files**: Consider using DuckDB's streaming capabilities for files > 1GB

## Support

- **DuckDB Documentation**: https://duckdb.org/docs/
- **DuckDB GitHub**: https://github.com/duckdb/duckdb
- **zlay-db Issues**: Report issues in the project repository