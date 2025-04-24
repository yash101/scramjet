#include <vector>
#include <string_view>
#include <iostream>
#include <unordered_map>
#include <tuple>

#include <lmdb.h>

using std::string_view;
using std::string;
using std::vector;
using std::cout;
using std::cerr;
using std::endl;
using std::unordered_map;
using std::tuple;
using std::get;

/*
 * LMDB API
 * int mdb_env_create(MDB_env **env);
 * int mdb_env_open(MDB_env *env, const char *path, unsigned int flags, mdb_mode_t mode);
 * int mdb_txn_begin(MDB_env *env, MDB_txn *parent, unsigned int flags, MDB_txn **txn);
 * int mdb_dbi_open(MDB_txn *txn, const char *name, unsigned int flags, MDB_dbi *dbi);
 * int mdb_get(MDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data);
 * int mdb_put(MDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data, unsigned int flags);
 * int mdb_txn_commit(MDB_txn *txn);
 * void mdb_env_close(MDB_env *env);
 */

class MDB_txn_wrapper
{
public:
  MDB_txn* txn;

  MDB_txn_wrapper() :
    txn(nullptr)
  { }

  ~MDB_txn_wrapper()
  {
    if (txn) {
      mdb_txn_abort(txn); 
      txn = nullptr;
    }
  }

  inline int commit()
  {
    if (txn)
    {
      int rc = mdb_txn_commit(txn);
      if (rc) {
        cerr << "Failed to commit transaction: " << mdb_strerror(rc) << endl;
        mdb_txn_abort(txn);
      }
      txn = nullptr;
      return rc;
    }

    return 0;
  }

  inline void abort()
  {
    if (txn)
    {
      mdb_txn_abort(txn);
      txn = nullptr;
    }
  }
};

class MDB_env_wrapper
{
public:
  MDB_env* env;

  MDB_env_wrapper() :
    env(nullptr)
  { }

  ~MDB_env_wrapper()
  {
    if (env) {
      mdb_env_close(env);
      env = nullptr;
    }
  }
};

class MDB_dbi_wrapper
{
public:
  MDB_dbi dbi;
  MDB_env* env;

  MDB_dbi_wrapper() :
    dbi(0)
  { }

  ~MDB_dbi_wrapper()
  {
    if (dbi) {
      mdb_dbi_close(env, 0);
      dbi = 0;
    }
  }
};

class Shard
{
public:
  string filepath;
  MDB_env_wrapper env;
  MDB_dbi_wrapper dbi;
};

class MDBEntry
{
public:
  MDB_val key;
  MDB_val data;
};

class ScramjetDB
{
private:
  unordered_map<string, Shard> openShardMap;
  bool isDirectory;
  string path;
  MDB_env_wrapper wLmdbEnv;

  tuple<int, Shard&> prepareShard(string shard)
  {
    auto shrd = openShardMap.find(shard);
    if (shrd != openShardMap.end()) {
      return { 0, shrd->second };
    }

    // Create a new shard
    Shard newShard;
    newShard.filepath = string(path) + "/" + shard;

    int rc;
    rc = mdb_env_create(&newShard.env.env);
    if (rc)
    {
      cerr << "Failed to create LMDB environment: " << mdb_strerror(rc) << endl;
      static Shard nullShard;
      return { -1, nullShard };
    }

    mdb_env_set_mapsize(newShard.env.env, 10 * 1024 * 1024 * 1024); // 10GB

    rc = mdb_env_open(newShard.env.env, newShard.filepath.data(), 0, 0664);
    if (rc)
    {
      cerr << "Failed to open LMDB environment: " << mdb_strerror(rc) << endl;
      static Shard nullShard;
      return { -1, nullShard };
    }

    MDB_txn_wrapper txn;
    rc = mdb_txn_begin(newShard.env.env, nullptr, 0, &txn.txn);
    if (rc)
    {
      cerr << "Failed to begin transaction: " << mdb_strerror(rc) << endl;
      static Shard nullShard;
      return { rc, nullShard };
    }

    rc = mdb_dbi_open(txn.txn, nullptr, 0, &newShard.dbi.dbi);
    if (rc)
    {
      cerr << "Failed to open database: " << mdb_strerror(rc) << endl;
      static Shard nullShard;
      return { rc, nullShard };
    }

    rc = txn.commit();
    if (rc)
    {
      cerr << "Failed to commit transaction: " << mdb_strerror(rc) << endl;
      static Shard nullShard;
      return { rc, nullShard };
    }

    openShardMap[shard] = newShard;
    return { 0, openShardMap[shard] };
  }

public:
  ScramjetDB(string path) :
    path(path)
  {
    mdb_env_open(wLmdbEnv.env, path.data(), 0, 0664);
  }

  tuple<int, MDB_val> get(string shard, MDB_val key)
  {
    auto resolvedShard = prepareShard(shard);
    int rc = std::get<0>(resolvedShard);
    if (rc) {
      return { rc, MDB_val() };
    }

    Shard& shrd = std::get<1>(resolvedShard);

    if (rc) {
      return { rc, MDB_val() };
    }

    MDB_val val;
    MDB_txn_wrapper txn;

    rc = mdb_txn_begin(shrd.env.env, nullptr, 0, &txn.txn);
    if (rc)
    {
      cerr << "Failed to begin transaction: " << mdb_strerror(rc) << endl;
      return { rc, val };
    }

    rc = mdb_get(txn.txn, shrd.dbi.dbi, &key, &val);
    if (rc)
    {
      cerr << "Failed to get value: " << mdb_strerror(rc) << endl;
      return { rc, val };
    }

    return { 0, val };
  }

  /**
   * @brief Get a range of keys from the database.
   */
  tuple<int, size_t> get(string shard, string start, string end)
  {
    return { 0, 0 };
  }

  int put(string shard, string k, string v)
  {
    // Load the correct shard
    if (openShardMap.find(shard) == openShardMap.end()) {
      openShardMap[shard] = Shard();
      openShardMap[shard].filepath = string(path) + "/" + shard;

      MDB_txn* txn = nullptr;
    }
  }
};

class Args
{
public:
  bool isDirectory;
  string_view path;
};

int main(int argc, char** argv)
{
  auto args = vector<string_view>(argc - 1);
  for (int i = 1; i < argc; ++i) {
    args[i - 1] = argv[i];
  }
  
  for (size_t i = 0; i < args.size(); i++)
  {
    auto arg = args[i];

    if (arg == "--help" || arg == "-h") {
       cout << "Usage: " << argv[0] <<  R"( [options]
Options:
  -h, --help            Show this help message
  -d, --dir <dir>       Path to the directory for the database

Notes:
  -f and -d are exclusive. Only use one of them. If using a file,
  sharding will be disabled.
  
  Sharding works by creating multiple database files in the specified
  directory.
)" << endl;
    }
    else if (arg == "--dir" || arg == "-d")
    {
      if (i + 1 < args.size()) {
        string_view dir = args[++i];
      } else {
        cout << "Error: No directory specified after " << arg << endl;
      }
    }
  }

  ScramjetDB db("");
}
