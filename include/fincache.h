#include <rocksdb/db.h>

#include <memory>

class FinCacheDB
{
private:
  std::unique_ptr<rocksdb::DB> m_db;

  int insert_one(const rocksdb::Slice& key_slice, const rocksdb::Slice& value_slice);
  int read_one(const rocksdb::Slice& key);
};
