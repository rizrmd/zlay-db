#!/bin/bash

# DuckDB Setup Script for zlay-db
# This script helps set up DuckDB library for compilation

set -e

echo "🦆 Setting up DuckDB for zlay-db..."

# Check if we're on macOS
if [[ "$OSTYPE" == "darwin"* ]]; then
    echo "🍎 Detected macOS"
    
    # Check if Homebrew is installed
    if ! command -v brew &> /dev/null; then
        echo "❌ Homebrew not found. Please install Homebrew first:"
        echo "   /bin/bash -c \"\$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)\""
        exit 1
    fi
    
    # Install DuckDB via Homebrew
    echo "📦 Installing DuckDB via Homebrew..."
    brew install duckdb
    
    # Find DuckDB installation paths
    DUCKDB_PREFIX=$(brew --prefix duckdb)
    DUCKDB_INCLUDE="$DUCKDB_PREFIX/include"
    DUCKDB_LIB="$DUCKDB_PREFIX/lib"
    
    echo "✅ DuckDB installed to: $DUCKDB_PREFIX"
    echo "📁 Include path: $DUCKDB_INCLUDE"
    echo "📚 Library path: $DUCKDB_LIB"
    
    # Check for library files
    if [[ -f "$DUCKDB_LIB/libduckdb.dylib" ]]; then
        echo "✅ Found DuckDB dynamic library"
    elif [[ -f "$DUCKDB_LIB/libduckdb.a" ]]; then
        echo "✅ Found DuckDB static library"
    else
        echo "❌ DuckDB library not found in expected location"
        echo "   Please check the installation manually"
        exit 1
    fi
    
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    echo "🐧 Detected Linux"
    
    # Try to find DuckDB in common locations
    DUCKDB_PATHS=(
        "/usr/local/include/duckdb.h"
        "/usr/include/duckdb.h"
        "/opt/duckdb/include/duckdb.h"
    )
    
    DUCKDB_FOUND=false
    for path in "${DUCKDB_PATHS[@]}"; do
        if [[ -f "$path" ]]; then
            echo "✅ Found DuckDB header at: $path"
            DUCKDB_FOUND=true
            break
        fi
    done
    
    if [[ "$DUCKDB_FOUND" == false ]]; then
        echo "❌ DuckDB not found. Please install DuckDB manually:"
        echo "   1. Download from: https://github.com/duckdb/duckdb/releases"
        echo "   2. Or build from source: https://github.com/duckdb/duckdb"
        echo "   3. Install to /usr/local or /opt/duckdb"
        exit 1
    fi
else
    echo "❌ Unsupported OS: $OSTYPE"
    echo "   Please install DuckDB manually and update build.zig"
    exit 1
fi

echo ""
echo "🔧 Next steps:"
echo "   1. Update build.zig to link against DuckDB"
echo "   2. Run: zig build"
echo "   3. Test with: zig test test_csv.zig"
echo ""
echo "📚 See README.md for detailed instructions"