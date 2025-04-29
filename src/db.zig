const std = @import("std");

const lmdb = @cImport({
    @cInclude("lmdb.h");
});

const OpenShard = struct {
    env: *lmdb.MDB_env,
    dbi: lmdb.MDB_dbi,
};

pub const Scramjet = struct {
    // Base path for lmdb shards
    base_path: []const u8,
    // HashMap of open LMDB shards
    shards: std.AutoHashMap([]const u8, OpenShard),
    // LRU cache of shard keys
    shard_recency_list: std.DoublyLinkedList([]const u8, std.mem.Allocator),
    // Maximum number of shards to keep open
    max_hot_shards: usize,
};

pub fn init(base_path: []const u8, max_hot_shards: usize) !Scramjet {
    const allocator = std.heap.page_allocator;
    const shards = try std.AutoHashMap([]const u8, OpenShard).init(allocator);
    const shard_recency_list = try std.DoublyLinkedList([]const u8, allocator).init();

    return Scramjet{
        .base_path = base_path,
        .shards = shards,
        .shard_recency_list = shard_recency_list,
        .max_hot_shards = max_hot_shards,
    };
}

pub fn put()
