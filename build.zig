const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const src_lmdb = b.path("src/dep/lmdb/");
    // Build LMDB to a static library
    const lib_lmdb = b.addStaticLibrary(.{
        .name = "lmdb",
        .root_source_file = null,
        .target = target,
        .optimize = optimize,
    });

    lib_lmdb.addIncludePath(src_lmdb);
    lib_lmdb.addCSourceFiles(.{
        .root = src_lmdb,
        .files = &[_][]const u8{ "mdb.c", "midl.c" },
    });
    lib_lmdb.linkLibC();

    // Scramjet DB, as a static lib
    const src_scramjetdb = b.path("src/root.zig");
    const static_scramjetdb = b.addStaticLibrary(.{
        .name = "scramjetdb",
        .root_source_file = src_scramjetdb,
        .target = target,
        .optimize = optimize,
    });
    static_scramjetdb.addIncludePath(src_lmdb);
    static_scramjetdb.linkLibrary(lib_lmdb);
    static_scramjetdb.linkLibC();

    // Scramjet DB, as a shared lib (wrapping the static lib)
    const shared_scramjetdb = b.addSharedLibrary(.{
        .name = "scramjetdb",
        .root_source_file = null,
        .target = target,
        .optimize = optimize,
    });
    shared_scramjetdb.linkLibrary(static_scramjetdb);
    shared_scramjetdb.linkLibrary(lib_lmdb);
    shared_scramjetdb.linkLibC();
    b.installArtifact(shared_scramjetdb);

    // Executable service
    const exe = b.addExecutable(.{
        .name = "scramjetdb",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    exe.linkLibrary(static_scramjetdb);
    exe.linkLibrary(lib_lmdb);
    exe.linkLibC();
    b.installArtifact(exe);
}
