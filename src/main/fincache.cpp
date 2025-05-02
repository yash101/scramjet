#include <vector>
#include <string_view>
#include <iostream>
#include <unordered_map>
#include <tuple>
#include <filesystem>
#include <rocksdb/db.h>

using std::string_view;
using std::string;
using std::vector;
using std::cout;
using std::cerr;
using std::endl;
using std::unordered_map;
using std::tuple;
using std::get;
namespace fs = std::filesystem;

class ScramjetDB
{
private:
  std::unique_ptr<rocksdb::DB> m_db;

public:
  ScramjetDB(const std::string& dbPath)
  {
    rocksdb::Options options;
    options.create_if_missing = true;
    options.db_write_buffer_size = 4 << 30; // 4GB
    options.max_open_files = 500;
    options.max_background_jobs = 4;
    options.max_background_flushes = 2;
    options.max_background_compactions = 4;

    rocksdb::DB* db = nullptr;
    auto status = rocksdb::DB::Open(options, dbPath, &db);
    if (!status.ok())
    {
      throw std::runtime_error("Failed to open RocksDB: " + status.ToString());
    }
    m_db.reset(db);
  }

  void insert_one(const rocksdb::Slice& key_slice, const rocksdb::Slice& value_slice)
  {
    auto status = m_db->Put(rocksdb::WriteOptions(), key_slice, value_slice);
    if (!status.ok())
    {
      throw std::runtime_error("Failed to insert key-value pair: " + status.ToString());
    }
  }

  rocksdb::Slice read_one(const rocksdb::Slice& key)
  {
    static std::string emptyString = "";
    auto status = m_db->Get(rocksdb::ReadOptions(), key, &emptyString);
  }
};
