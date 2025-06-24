#include <vector>
#include <string_view>
#include <iostream>
#include <unordered_map>
#include <tuple>
#include <filesystem>
#include <thread>
#include <mutex>
#include <array>
#include <optional>
#include <stdint.h>
#include <getopt.h>
#include <chrono>

#include <rocksdb/db.h>
#include <rbuf.h>

// #define ENABLE_NETWORK_BYTESWAP true
// #define DISABLE_WAL true

#if defined(WIN32) || defined(_WIN32) || defined(__WIN32__) || defined(__CYGWIN__)
  #include <winsock2.h>
  #include <ws2tcpip.h>
  #include <windows.h>
  #include <io.h>
  #include <afunix.h>

  typedef SOCKET UnixSocket;
#else
  #include <sys/socket.h>
  #include <sys/un.h>
  #include <sys/uio.h>
  #include <unistd.h>
  #include <netinet/in.h>
  #include <arpa/inet.h>

  typedef int UnixSocket;
#endif

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

constexpr char OP_GET_ONE = 0x01;
constexpr char OP_GET_N = 0x02;
constexpr char OP_GET_BETWEEN = 0x03;
constexpr char OP_PUT_ONE = 0x04;
constexpr char OP_PUT_MULTI = 0x05;
constexpr char OP_BULK_PUT = 0x06;

constexpr char STAT_OK = 0x00;
constexpr char STAT_NOT_FOUND = 0x01;
constexpr char STAT_ERR = 0x02;

static volatile bool g_stop = false;

#if defined(_MSC_VER)
  #define NOINLINE __declspec(noinline)
#elif defined(__GNUC__) || defined(__clang__)
  #define NOINLINE [[gnu::noinline]]
#else
  #define NOINLINE
#endif

// For smaller keys and values, allocate to the stack for performance
// Bumping this up will require a larger stack per thread.
#ifndef STACK_ALLOC_MAX_SIZE
  #define STACK_ALLOC_MAX_SIZE 512 << 10 // 512KB
#endif

#define MIN(a, b) ((a) < (b) ? (a) : (b))

// Truncate a value to a smaller type
template <typename U, typename T>
inline constexpr U truncate(T value)
{
  return static_cast<U>(value);
}

// Enables network byte order conversion to cross networks. Disabled by default for performance.
#ifdef ENABLE_NETWORK_BYTESWAP

  template <typename T>
  constexpr uint16_t toNet16(T val)
  {
    auto v = truncate<uint16_t>(val);
    return htons(v);
  }

  template <typename T>
  constexpr uint32_t toNet32(T val)
  {
    auto v = truncate<uint32_t>(val);
    return htonl(v);
  }

  template <typename T>
  constexpr uint16_t fromNet16(T val)
  {
    auto v = truncate<uint16_t>(val);
    return ntohs(v);
  }

  template <typename T>
  constexpr uint32_t fromNet32(T val)
  {
    auto v = truncate<uint32_t>(val);
    return ntohl(v);
  }

#else

  template <typename T>
  inline constexpr uint16_t toNet16(T val)
  {
    return truncate<uint16_t>(val);
  }

  template <typename T>
  inline constexpr uint32_t toNet32(T val)
  {
    return truncate<uint32_t>(val);
  }

  template <typename T>
  inline constexpr uint16_t fromNet16(T val)
  {
    return truncate<uint16_t>(val);
  }

  template <typename T>
  inline constexpr uint32_t fromNet32(T val)
  {
    return truncate<uint32_t>(val);
  }

#endif

class BufferedSocket
{
private:
  RingBuffer<uint8_t> m_buffer;
  UnixSocket m_socket;
public:
  BufferedSocket(size_t size, UnixSocket socket) :
    m_buffer(size),
    m_socket(socket)
  { }

  // Read exactly n bytes into the provided buffer (gotta love the pointer arithmetic)
  bool read_n(uint8_t* buffer, size_t n, struct timeval& timeout)
  {
    size_t bytesRead = m_buffer.pop_n(n, buffer);
    if (bytesRead == n)
      return true;
    
    while (bytesRead + m_buffer.size() < n)
    {
      if (g_stop)
      {
        throw std::runtime_error("Server is shutting down");
      }

      // Wait until the socket has data to read
      fd_set read_fds;
      FD_ZERO(&read_fds);
      FD_SET(m_socket, &read_fds);

      int select_result = select(m_socket + 1, &read_fds, nullptr, nullptr, &timeout);
      if (select_result <= 0)
      {
        int err = errno;

        if (g_stop)
        {
          throw std::runtime_error("Server is shutting down");
        }

        switch (err)
        {
          case EAGAIN: // Non-blocking socket would block
            continue;
#if EAGAIN != EWOULDBLOCK
          case EWOULDBLOCK:
            continue;
#endif
          case EINTR: // Interrupted by signal
            continue;
          case EBADF: // Invalid socket descriptor
            throw std::runtime_error("Invalid socket descriptor");
          case EINVAL:
            throw std::runtime_error("Invalid argument");
          case ENOMEM:
            throw std::runtime_error("Out of memory");
          default:
            return false;
        }
      }

      // Read chars from socket
      char tmpBuf[m_buffer.available_without_alloc()];
      ssize_t transferred = recv(m_socket, tmpBuf, m_buffer.available_without_alloc(), MSG_DONTWAIT); // nonblock read
      
      if (transferred <= 0)
      {
        int err = errno;
        switch (err)
        {
          case EAGAIN: // Non-blocking socket would block
            continue;
#if EAGAIN != EWOULDBLOCK
          case EWOULDBLOCK:
            continue;
#endif
          case EBADF:
            throw std::runtime_error("Invalid socket descriptor");
          case EINTR:
            continue;
          case EINVAL:
            throw std::runtime_error("Invalid argument");
          case EFAULT:
            // panic?
            signal(SIGSEGV, 0);
            throw std::runtime_error("Bad address");
          case ENOTCONN:
            // Socket is not connected
            throw std::runtime_error("Socket is not connected");
          case ENOTSOCK:
            // Not a socket
            throw std::runtime_error("Not a socket");
          default:
            return false;
        }
      }

      m_buffer.push_n(tmpBuf, transferred);
    }

    m_buffer.pop_n(n - bytesRead, buffer + bytesRead);

    return true;
  }

  // Write exactly n bytes from the provided buffer
  bool write_n(const char* buffer, size_t n, struct timeval& tv)
  {
    size_t bytesWritten = 0;
    while (bytesWritten < n)
    {
      ssize_t result = send(m_socket, buffer + bytesWritten, n - bytesWritten, 0);
      if (result == -1 && (errno == EAGAIN || errno == EWOULDBLOCK))
      {
        // If the socket is non-blocking and would block, wait for it to become writable
        fd_set write_fds;
        FD_ZERO(&write_fds);
        FD_SET(m_socket, &write_fds);

        int select_result = select(m_socket + 1, nullptr, &write_fds, nullptr, &tv);
        if (select_result <= 0) {
          return false; // Timeout or error
        }
        continue; // Retry sending after the socket becomes writable
      }
      if (result <= 0)
      {
        return false; // Socket closed or error
      }
      bytesWritten += result;
    }
    return true;
  }
};

// WorkerContext holds the context for each worker thread
// Note: absolutely NOT thread-safe. It's a per-thread context.
// Don't be stooopid and use it across threads without some sort of locking and questioning life choices.
class WorkerContext
{
public:
  WorkerContext(UnixSocket socket, struct sockaddr_un client_addr, rocksdb::DB* db) :
    m_socket(socket),
    m_client_addr(client_addr),
    m_db(db),
    m_buffered_socket(4 << 20, socket)
  { }

  ~WorkerContext()
  {
    close(m_socket);
  }

  UnixSocket m_socket;
  struct sockaddr_un m_client_addr;
  rocksdb::DB* m_db;
  rocksdb::ReadOptions m_read_options;
  rocksdb::WriteOptions m_write_options;
  BufferedSocket m_buffered_socket;
  rocksdb::PinnableSlice m_pinnable_slice;
};

size_t write_iov(UnixSocket socket, iovec* iov, int iov_count)
{
  assert(iov != nullptr);

  size_t total_writable = 0;
  size_t written = 0;
  size_t iov_written = 0;

  int iov_start = 0;

  for (size_t i = 0; i < iov_count; i++)
    total_writable += iov[i].iov_len;

  while (true)
  {
    struct timeval timeout = { 5, 0 };

    // Wait until the socket is writable
    struct fd_set write_fds;
    FD_ZERO(&write_fds);
    FD_SET(socket, &write_fds);
    int select_status = select(socket + 1, nullptr, &write_fds, nullptr, &timeout);
    if (select_status < 0)
    {
      int err = errno;
      switch (err)
      {
        case EAGAIN:
          continue;
#if EAGAIN != EWOULDBLOCK
        case EWOULDBLOCK:
          continue;
#endif
        case EINTR:
          continue;
        case EBADF:
          throw std::runtime_error("Invalid socket descriptor");
        case EINVAL:
          throw std::runtime_error("Invalid argument");
        default:
          // Handle other errors
          throw std::runtime_error("Error writing to socket");
      }
    }

    ssize_t status = writev(socket, iov + iov_start, iov_count - iov_start);
    if (status < 0)
    {
      int err = errno;
      switch (err)
      {
        case EAGAIN:
          continue;
#if EAGAIN != EWOULDBLOCK
        case EWOULDBLOCK:
          continue;
#endif
        case EINTR:
          continue;
        case EBADF:
          throw std::runtime_error("Invalid socket descriptor");
        case EINVAL:
          throw std::runtime_error("Invalid argument");
        default:
          // Handle other errors
          throw std::runtime_error("Error writing to socket");
      }
    }

    written += status;

    size_t full_iovs_written = 0;
    for (int iov_start = 0; iov_start < iov_count; iov_start++)
    {
      iovec& cur = iov[iov_start];
      if (full_iovs_written + cur.iov_len < written)
        full_iovs_written += cur.iov_len;
      else
        break;
    }

    iov[iov_start].iov_base = static_cast<char*>(iov[iov_start].iov_base) + (written - full_iovs_written);
    iov[iov_start].iov_len -= (written - full_iovs_written);
  }

  return written;
}

void print_usage(const char* program_name)
{
  string usage = R"(
Usage: [program_name] [options]
Options:
  --db-path <path>       Path to the RocksDB database (required)
  --socket-path <path>   Path to the UNIX socket to listen on (required)
  --write-buffer <size>  Write buffer size in bytes (default: 4GB)
  --max-files <count>    Maximum number of open files (default: 500)
  --help                 Show this help message
)";
  cout << usage;
}

UnixSocket bindAndListen(std::string& path)
{
  // Initialize the socket
  UnixSocket server_socket = socket(AF_UNIX, SOCK_STREAM, 0);
  if (server_socket == -1)
  {
    cerr << "Error creating socket: " << strerror(errno) << endl;
    return -1;
  }

  struct sockaddr_un server_addr;
  memset(&server_addr, 0, sizeof(server_addr));
  size_t pathLength = path.size() + 1;
  if (pathLength > sizeof(server_addr.sun_path))
  {
    cerr << "Error: Socket path too long. Max length is " << sizeof(server_addr.sun_path) - 1 << " characters." << endl;
    close(server_socket);
    return -1;
  }

  // Set the socket address
  server_addr.sun_family = AF_UNIX;
  strcpy(server_addr.sun_path, path.c_str()); // Copy the socket path. strcpy since length is checked above
  server_addr.sun_path[sizeof(server_addr.sun_path) - 1] = '\0';
  server_addr.sun_len = sizeof(server_addr);

  // Remove the socket file if it exists
  unlink(path.c_str());

  if (bind(server_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) == -1)
  {
    cerr << "Error binding socket: " << strerror(errno) << endl;
    close(server_socket);
    return -1;
  }

  if (listen(server_socket, 5) == -1)
  {
    cerr << "Error listening on socket: " << strerror(errno) << endl;
    close(server_socket);
    return -1;
  }

  cout << "Listening on socket: " << path << endl;
  return server_socket;
}

// DO NOT INLINE. THIS FUNCTION USES UNSAFE CODE (alloca)
NOINLINE void doGetOne(WorkerContext& context)
{
  uint32_t klen;
  uint8_t* kbuf;
  std::unique_ptr<uint8_t[]> kbuf_prot = nullptr;
  
  rocksdb::ReadOptions read_options;
  struct timeval timeout;

  read_options.fill_cache = false;
  read_options.total_order_seek = false;
  read_options.pin_data = true;

  // Don't worry about endianness. We only support UNIX sockets so assume data is local to system.
  timeout = { 5, 0 };
  if (!context.m_buffered_socket.read_n(reinterpret_cast<uint8_t*>(&klen), sizeof(klen), timeout));
    throw std::runtime_error("Failed to read key length");
  klen = fromNet32(klen);

  if (klen > STACK_ALLOC_MAX_SIZE)
  {
    // Allocate on heap, use a unique_ptr to manage memory
    kbuf_prot = std::make_unique<uint8_t[]>(klen);
    kbuf = kbuf_prot.get();
  }
  else
  {
    kbuf = reinterpret_cast<uint8_t*>(alloca(klen)); // Allocate on stack
  }

  // Read the key
  timeout = { 5, 0 };
  if (!context.m_buffered_socket.read_n(kbuf, klen, timeout))
    throw std::runtime_error("Failed to read key");

  // Find and read the value from the DB
  auto status = context.m_db->Get(
    read_options,
    nullptr,
    rocksdb::Slice(reinterpret_cast<const char*>(kbuf), klen),
    &context.m_pinnable_slice
  );

  // Check if the key was found
  if (status.IsNotFound())
  {
    char response[] = { STAT_NOT_FOUND };
    if (!context.m_buffered_socket.write_n(response, sizeof(response), timeout))
      throw std::runtime_error("Failed to write error response");
    
    return;
  }

  if (!status.ok())
  {
    string error = status.ToString();
    uint16_t errorLength = static_cast<uint16_t>(toNet16(error.size()));
    uint8_t errorHeader[] = {
      STAT_ERR,
      static_cast<uint8_t>(errorLength >> 8),
      static_cast<uint8_t>(errorLength & 0xFF)
    };

    timeout = { 5, 0 };
    if (!context.m_buffered_socket.write_n(reinterpret_cast<char*>(errorHeader), sizeof(error), timeout))
      throw std::runtime_error("Failed to write error response");

    timeout = { 5, 0 };
    if (!context.m_buffered_socket.write_n(error.data(), errorLength, timeout))
      throw std::runtime_error("Failed to write error response");
    
    return;
  }

  // Write the value length and value. Use iovec to reduce syscalls
  uint32_t vlen = static_cast<uint32_t>(toNet32(context.m_pinnable_slice.size()));
  uint8_t response[] = {
    STAT_OK,
    static_cast<uint8_t>(vlen >> 24),
    static_cast<uint8_t>((vlen >> 16) & 0xFF),
    static_cast<uint8_t>((vlen >> 8) & 0xFF),
    static_cast<uint8_t>(vlen & 0xFF)
  };

  struct iovec iov[2];

  iov[0].iov_base = reinterpret_cast<void*>(&response);
  iov[0].iov_len = sizeof(response);

  iov[1].iov_base = const_cast<char*>(context.m_pinnable_slice.data());
  iov[1].iov_len = vlen;

  write_iov(context.m_socket, iov, 2);
}

NOINLINE void doGetN(WorkerContext& context)
{
  uint32_t klen;
  uint32_t n;
  uint8_t* kbuf;
  std::unique_ptr<uint8_t[]> kbuf_prot = nullptr;

  rocksdb::ReadOptions read_options;
  struct timeval timeout;
  read_options.fill_cache = false;
  read_options.pin_data = true;
  read_options.total_order_seek = true;

  // Read the start key length and value
  timeout = { 5, 0 };
  if (!context.m_buffered_socket.read_n(reinterpret_cast<uint8_t*>(&klen), sizeof(klen), timeout))
    throw std::runtime_error("Failed to read key length");

  klen = fromNet32(klen);
  
  if (klen > STACK_ALLOC_MAX_SIZE)
  {
    // Allocate on heap, use a unique_ptr to manage memory
    kbuf_prot = std::make_unique<uint8_t[]>(klen);
    kbuf = kbuf_prot.get();
  }
  else
  {
    kbuf = reinterpret_cast<uint8_t*>(alloca(klen)); // Allocate on stack
  }

  // Read the key
  timeout = { 5, 0 };
  if (!context.m_buffered_socket.read_n(kbuf, klen, timeout))
    throw std::runtime_error("Failed to read key");

  // Read the number of keys to get
  timeout = { 5, 0 };
  if (!context.m_buffered_socket.read_n(reinterpret_cast<uint8_t*>(&n), sizeof(n), timeout))
    throw std::runtime_error("Failed to read number of keys");
  
  n = fromNet32(n);
  
  // Create an ierator and return the data
  std::unique_ptr<rocksdb::Iterator> iter(context.m_db->NewIterator(read_options));
  iter->Seek(rocksdb::Slice(reinterpret_cast<const char*>(kbuf), klen));

  for (size_t i = 0; i < n; i++)
  {
    if (!iter->Valid())
    {
      // Return a null KV pair and break
      std::string error = iter->status().ToString();
      uint16_t errorLength = static_cast<uint16_t>(toNet16(error.size()));

      uint8_t errorHeader[] = {
        STAT_ERR,
        static_cast<uint8_t>(errorLength >> 8),
        static_cast<uint8_t>(errorLength & 0xFF)
      };

      iovec iov[2];
      iov[0].iov_base = reinterpret_cast<void*>(&errorHeader);
      iov[0].iov_len = sizeof(errorHeader);
      iov[1].iov_base = const_cast<char*>(error.data());
      iov[1].iov_len = iov[0].iov_len;

      // Write the iovecs
      write_iov(context.m_socket, iov, 2);
      break;
    }

    // Get the key and value
    rocksdb::Slice kslice = iter->key();
    rocksdb::Slice vslice = iter->value();

    uint32_t klen = toNet32(kslice.size());
    uint32_t vlen = toNet32(vslice.size());

    uint8_t firstHeader[] = {
      STAT_OK,
      static_cast<uint8_t>(klen >> 24),
      static_cast<uint8_t>((klen >> 16) & 0xFF),
      static_cast<uint8_t>((klen >> 8) & 0xFF),
      static_cast<uint8_t>(klen & 0xFF)
    };

    struct iovec iov[2];

    iov[0].iov_base = reinterpret_cast<void*>(&klen);
    iov[0].iov_len = sizeof(klen);
    iov[1].iov_base = const_cast<char*>(kslice.data());
    iov[1].iov_len = klen;

    // Write the iovecs
    write_iov(context.m_socket, iov, 2);
  }
}

NOINLINE void doGetBetween(WorkerContext& context)
{
  unsigned int k0len;
  unsigned int k1len;
  uint8_t* k0buf;
  uint8_t* k1buf;
  std::unique_ptr<uint8_t[]> k0buf_prot = nullptr;
  std::unique_ptr<uint8_t[]> k1buf_prot = nullptr;

  rocksdb::ReadOptions read_options;
  struct timeval timeout;
  read_options.fill_cache = false;
  read_options.pin_data = true;
  read_options.total_order_seek = true;

  // Don't worry about endianness. We only support UNIX sockets so assume data is local to system.
  // Read the keys lengths and values
  timeout = { 5, 0 };
  if (!context.m_buffered_socket.read_n(reinterpret_cast<uint8_t*>(&k0len), sizeof(k0len), timeout))
    throw std::runtime_error("Failed to read key length");

  // allocate then read the first key
  k0len = fromNet32(k0len);
  if (k0len > STACK_ALLOC_MAX_SIZE)
  {
    // Allocate on heap, use a unique_ptr to manage memory
    k0buf_prot = std::make_unique<uint8_t[]>(k0len);
    k0buf = k0buf_prot.get();
  }
  else
  {
    k0buf = reinterpret_cast<uint8_t*>(alloca(k0len)); // Allocate on stack
  }
  if (!context.m_buffered_socket.read_n(k0buf, k0len, timeout))
    throw std::runtime_error("Failed to read key");

  timeout = { 5, 0 };
  if (!context.m_buffered_socket.read_n(reinterpret_cast<uint8_t*>(&k1len), sizeof(k1len), timeout))
    throw std::runtime_error("Failed to read key length");

  k1len = fromNet32(k1len);
  if (k1len > STACK_ALLOC_MAX_SIZE)
  {
    // Allocate on heap, use a unique_ptr to manage memory
    k1buf_prot = std::make_unique<uint8_t[]>(k1len);
    k1buf = k1buf_prot.get();
  }
  else
  {
    k1buf = reinterpret_cast<uint8_t*>(alloca(k1len)); // Allocate on stack
  }
  if (!context.m_buffered_socket.read_n(k1buf, k1len, timeout))
    throw std::runtime_error("Failed to read key");
  
  // Create an iterator and return the data
  std::unique_ptr<rocksdb::Iterator> iter(context.m_db->NewIterator(read_options));
  iter->Seek(rocksdb::Slice(reinterpret_cast<const char*>(k0buf), k0len));
  rocksdb::Slice k1slice(reinterpret_cast<const char*>(k1buf), k1len);
  while (iter->Valid())
  {
    // Get the key and value
    rocksdb::Slice kslice = iter->key();
    rocksdb::Slice vslice = iter->value();

    if (kslice.compare(k1slice) > 0)
      break;

    uint32_t klen = toNet32(kslice.size());
    uint32_t vlen = toNet32(vslice.size());

    uint8_t firstHeader[] = {
      STAT_OK,
      static_cast<uint8_t>(klen >> 24),
      static_cast<uint8_t>((klen >> 16) & 0xFF),
      static_cast<uint8_t>((klen >> 8) & 0xFF),
      static_cast<uint8_t>(klen & 0xFF)
    };

    struct iovec iov[4];
    iov[0].iov_base = reinterpret_cast<void*>(&firstHeader);
    iov[0].iov_len = sizeof(firstHeader);
    iov[1].iov_base = const_cast<char*>(kslice.data());
    iov[1].iov_len = klen;
    iov[2].iov_base = reinterpret_cast<void*>(&vlen);
    iov[2].iov_len = sizeof(vlen);
    iov[3].iov_base = const_cast<char*>(vslice.data());
    iov[3].iov_len = vlen;

    // Write the iovecs
    write_iov(context.m_socket, iov, 2);
  }

  // Write a null KV pair to indicate end of stream
  // status code, 4 byte key length, 4 byte value length
  uint8_t endHeader[] = { STAT_OK, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
  if (!context.m_buffered_socket.write_n(reinterpret_cast<char*>(endHeader), sizeof(endHeader), timeout))
    throw std::runtime_error("Failed to write null KV pair");
}

NOINLINE void doPutOne(WorkerContext& context)
{
  // Read the klen, key, vlen and value
  unsigned int klen;
  unsigned int vlen;
  char* kbuf;
  char* vbuf;
  std::unique_ptr<char[]> kbuf_prot = nullptr;
  std::unique_ptr<char[]> vbuf_prot = nullptr;

  struct timeval timeout;
  rocksdb::WriteOptions write_options;
  write_options.sync = false;
#ifdef DISABLE_WAL
  write_options.disable_wal = true;
#endif

  // Read KV pair
  timeout = { 5, 0 };
  if (!context.m_buffered_socket.read_n(reinterpret_cast<char*>(&klen), sizeof(klen), timeout))
    throw std::runtime_error("Failed to read key length");
  
  if (klen > STACK_ALLOC_MAX_SIZE)
  {
    // Allocate on heap, use a unique_ptr to manage memory
    kbuf_prot = std::make_unique<char[]>(klen);
    kbuf = kbuf_prot.get();
  }
  else
  {
    kbuf = reinterpret_cast<char*>(alloca(klen)); // Allocate on stack
  }

  timeout = { 5, 0 };
  if (!context.m_buffered_socket.read_n(kbuf, klen, timeout))
    throw std::runtime_error("Failed to read key");
  
  timeout = { 5, 0 };
  if (!context.m_buffered_socket.read_n(reinterpret_cast<char*>(&vlen), sizeof(vlen), timeout))
    throw std::runtime_error("Failed to read value length");

  if (vlen > STACK_ALLOC_MAX_SIZE)
  {
    // Allocate on heap, use a unique_ptr to manage memory
    vbuf_prot = std::make_unique<char[]>(vlen);
    vbuf = vbuf_prot.get();
  }
  else
  {
    vbuf = reinterpret_cast<char*>(alloca(vlen)); // Allocate on stack
  }

  timeout = { 5, 0 };
  if (!context.m_buffered_socket.read_n(vbuf, vlen, timeout))
    throw std::runtime_error("Failed to read value");

  // Write the key and value to the DB
  rocksdb::Slice kslice(kbuf, klen);
  rocksdb::Slice vslice(vbuf, vlen);

  auto status = context.m_db->Put(write_options, kslice, vslice);
  if (!status.ok())
  {
    // return an error opcode
    char error[] = { ERR_ERR, 0x00 }; // error of 0 length
    if (!context.m_buffered_socket.write_n(error, sizeof(error), timeout))
      throw std::runtime_error("Failed to write error response");
  }
  else
  {
    char status[] = { 0x00, 0x00 };
    if (!context.m_buffered_socket.write_n(status, sizeof(status), timeout))
      throw std::runtime_error("Failed to write success response");
  }
}

// NOINLINE boundary for alloca calls
NOINLINE bool doPutN_one(
  WorkerContext& context,
  std::unique_ptr<char[]>& kbuf_cache,
  std::unique_ptr<char[]>& vbuf_cache,
  size_t& kbuf_cache_alloc,
  size_t& vbuf_cache_alloc
)
{
  unsigned int klen;
  unsigned int vlen;
  char* kbuf;
  char* vbuf;

  struct timeval timeout;

  rocksdb::WriteOptions write_options;
  write_options.sync = false;

  // Read key
  timeout = { 5, 0 };
  if (!context.m_buffered_socket.read_n(reinterpret_cast<char*>(&klen), sizeof(klen), timeout))
    throw std::runtime_error("Failed to read key length");

  // Allocate space for the key
  if (klen == 0)
  {
    // No more keys to read
    return false;
  }
  else if (klen < STACK_ALLOC_MAX_SIZE)
  {
    kbuf = reinterpret_cast<char*>(alloca(klen));
  }
  else if (kbuf_cache != nullptr && kbuf_cache_alloc >= klen)
  {
    kbuf = kbuf_cache.get();
  }
  else
  {
    kbuf_cache = std::make_unique<char[]>(klen);
    kbuf_cache_alloc = klen;
    kbuf = kbuf_cache.get();
  }

  // Read the key
  timeout = { 5, 0 };
  if (!context.m_buffered_socket.read_n(kbuf, klen, timeout))
    throw std::runtime_error("Failed to read key");

  // Read the value length
  timeout = { 5, 0 };
  if (!context.m_buffered_socket.read_n(reinterpret_cast<char*>(&vlen), sizeof(vlen), timeout))
    throw std::runtime_error("Failed to read value length");
  
  // Allocate space for the value
  if (vlen < STACK_ALLOC_MAX_SIZE)
  {
    vbuf = reinterpret_cast<char*>(alloca(vlen));
  }
  else if (vbuf_cache != nullptr && vbuf_cache_alloc >= vlen)
  {
    vbuf = vbuf_cache.get();
  }
  else
  {
    vbuf_cache = std::make_unique<char[]>(vlen);
    vbuf_cache_alloc = vlen;
    vbuf = vbuf_cache.get();
  }

  // Read the value
  timeout = { 5, 0 };
  if (!context.m_buffered_socket.read_n(vbuf, vlen, timeout))
    throw std::runtime_error("Failed to read value");
  
  // Write the key and value to the DB
  rocksdb::Slice kslice(kbuf, klen);
  rocksdb::Slice vslice(vbuf, vlen);
  auto status = context.m_db->Put(write_options, kslice, vslice);

  // TODO: handle errors? For now, we just ignore.
  if (!status.ok())
  { }

  return true;
}

void doPutMulti(WorkerContext& context)
{
  std::unique_ptr<char[]> kbuf_cache;
  std::unique_ptr<char[]> vbuf_cache;

  size_t kbuf_cache_alloc;
  size_t vbuf_cache_alloc;

  while (doPutN_one(context, kbuf_cache, vbuf_cache, kbuf_cache_alloc, vbuf_cache_alloc)) { }
  // Send 0x00 to indicate end of stream
  char status = 0x00;

  struct timeval timeout = { 5, 0 };
  if (!context.m_buffered_socket.write_n(&status, sizeof(status), timeout))
    throw std::runtime_error("Failed to write end of stream response");
}

// NOINLINE boundary for alloca calls
NOINLINE bool doPutN_one(
  WorkerContext& context,
  std::unique_ptr<char[]>& kbuf_cache,
  std::unique_ptr<char[]>& vbuf_cache,
  size_t& kbuf_cache_alloc,
  size_t& vbuf_cache_alloc
)
{
  unsigned int klen;
  unsigned int vlen;
  char* kbuf;
  char* vbuf;

  struct timeval timeout;

  rocksdb::WriteOptions write_options;
  write_options.sync = false;

  // Read key
  timeout = { 5, 0 };
  if (!context.m_buffered_socket.read_n(reinterpret_cast<char*>(&klen), sizeof(klen), timeout))
    throw std::runtime_error("Failed to read key length");

  // Allocate space for the key
  if (klen == 0)
  {
    // No more keys to read
    return false;
  }
  else if (klen < STACK_ALLOC_MAX_SIZE)
  {
    kbuf = reinterpret_cast<char*>(alloca(klen));
  }
  else if (kbuf_cache != nullptr && kbuf_cache_alloc >= klen)
  {
    kbuf = kbuf_cache.get();
  }
  else
  {
    kbuf_cache = std::make_unique<char[]>(klen);
    kbuf_cache_alloc = klen;
    kbuf = kbuf_cache.get();
  }

  // Read the key
  timeout = { 5, 0 };
  if (!context.m_buffered_socket.read_n(kbuf, klen, timeout))
    throw std::runtime_error("Failed to read key");

  // Read the value length
  timeout = { 5, 0 };
  if (!context.m_buffered_socket.read_n(reinterpret_cast<char*>(&vlen), sizeof(vlen), timeout))
    throw std::runtime_error("Failed to read value length");
  
  // Allocate space for the value
  if (vlen < STACK_ALLOC_MAX_SIZE)
  {
    vbuf = reinterpret_cast<char*>(alloca(vlen));
  }
  else if (vbuf_cache != nullptr && vbuf_cache_alloc >= vlen)
  {
    vbuf = vbuf_cache.get();
  }
  else
  {
    vbuf_cache = std::make_unique<char[]>(vlen);
    vbuf_cache_alloc = vlen;
    vbuf = vbuf_cache.get();
  }

  // Read the value
  timeout = { 5, 0 };
  if (!context.m_buffered_socket.read_n(vbuf, vlen, timeout))
    throw std::runtime_error("Failed to read value");
  
  // Write the key and value to the DB
  rocksdb::Slice kslice(kbuf, klen);
  rocksdb::Slice vslice(vbuf, vlen);
  auto status = context.m_db->Put(write_options, kslice, vslice);

  if (!status.ok())
  {
    // return an error opcode
    char error[] = { ERR_ERR, 0x00 }; // error of 0 length
    if (!context.m_buffered_socket.write_n(error, sizeof(error), timeout))
      throw std::runtime_error("Failed to write error response");
  }

  return true;
}

/**
 * We create a new SST file and write the data to it.
 * Then, we merge that SST file into the main database.
 */
void doPutBulk(WorkerContext& context)
{
  std::unique_ptr<char[]> kbuf_cache;
  std::unique_ptr<char[]> vbuf_cache;

  size_t kbuf_cache_alloc;
  size_t vbuf_cache_alloc;

  fs::path dir = fs::temp_directory_path();
  auto filename = dir / fs::path("bulk_" + std::to_string(
    std::chrono::system_clock::now().time_since_epoch().count()
  ) + ".sst");

  std::string sst_file = dir;

  // Create an SST file
  rocksdb::SstFileWriter sst_file_writer(rocksdb::EnvOptions(), context.m_db->GetOptions());
  rocksdb::Status status = sst_file_writer.Open(sst_file);

  if (!status.ok())
  {
    // Send error code
    throw std::runtime_error("Failed to open SST file");
  }
}

// DO NOT INLINE. THIS FUNCTION USES UNSAFE CODE (alloca)
void handleRequest(WorkerContext& context)
{
  // get opcode
  char opcode;
  struct timeval timeout = { 5, 0 };

  if (!context.m_buffered_socket.read_n(&opcode, 1, timeout))
  {
    throw std::runtime_error("Failed to read opcode");
  }

  switch (opcode)
  {
    case OP_GET_ONE: // GET one
      doGetOne(context);
      return;
    case OP_GET_N: // GET n
      doGetN(context);
      return;
    case OP_GET_BETWEEN: // GET between
      doGetBetween(context);
      return;
    case OP_PUT_ONE: // PUT one
      doPutOne(context);
      return;
    case OP_PUT_MULTI: // PUT n
      doPutMulti(context);
      return;
    case OP_BULK_PUT: // BULK PUT into SST (perhaps make it behave like OP_PUT_N?)
      doPutBulk(context);
      return;
    default:
      return; // Probably close the connection because something is awry
  }
}

// Function to handle incoming connections
void workerThread(
  UnixSocket client_socket,
  struct sockaddr_un client_addr,
  rocksdb::DB* db
)
{
  try
  {
    cout << "Handling client connection..." << endl;
    WorkerContext context(client_socket, client_addr, db);

    // RERL: Read-Execute-Reply Loop
    while (true)
    {
      handleRequest(context);
    }
  }
  catch(const std::exception& e)
  {
    std::cerr << e.what() << '\n';
    close(client_socket);
  }
}

int main(int argc, char** argv)
{
  // Main loop to accept connections
  std::mutex giveChildTimeToCopyDataMutex;

  string dbPath;
  string socketPath;
  rocksdb::Options options;
  options.create_if_missing = true;
  options.db_write_buffer_size = 4 << 30; // Default: 4GB
  options.max_open_files = 500;           // Default: 500

  static struct option long_options[] = {
    {"db-path", required_argument, nullptr, 'd'},
    {"socket-path", required_argument, nullptr, 's'},
    {"write-buffer", required_argument, nullptr, 'w'},
    {"max-files", required_argument, nullptr, 'f'},
    {"help", no_argument, nullptr, 'h'},
    {nullptr, 0, nullptr, 0}};

  int opt;
  while ((opt = getopt_long(argc, argv, "d:s:w:f:h", long_options, nullptr)) != -1)
  {
    switch (opt)
    {
    case 'd':
      dbPath = optarg;
      break;
    case 's':
      socketPath = optarg;
      break;
    case 'w':
      options.db_write_buffer_size = std::stoull(optarg);
      break;
    case 'f':
      options.max_open_files = std::stoi(optarg);
      break;
    case 'h':
      print_usage(argv[0]);
      return 0;
    default:
      cerr << "Unknown option. Use --help for usage information.\n";
      return 1;
    }
  }

  if (dbPath.empty() || socketPath.empty())
  {
    cerr << "Error: --db-path and --socket-path are required.\n";
    print_usage(argv[0]);
    return 1;
  }

  // Initialize the database
  rocksdb::DB* db = nullptr;
  auto status = rocksdb::DB::Open(options, dbPath, &db);
  if (!status.ok())
  {
    cerr << "Error opening database: " << status.ToString() << endl;
    return 1;
  }

  auto socket = bindAndListen(socketPath);
  if (socket == -1)
  {
    cerr << "Error binding to socket: " << strerror(errno) << endl;
    goto cleanup_fail;
  }

  while (true)
  {
    struct sockaddr_un client_addr;
    socklen_t client_addr_len = sizeof(client_addr);
    UnixSocket client_socket = accept(socket, reinterpret_cast< sockaddr*>(&client_addr), &client_addr_len);
    if (client_socket == -1)
    {
      cerr << "Error accepting connection: " << strerror(errno) << endl;
      continue; // Continue to accept next connection
    }

    cout << "Accepted connection from client." << endl;
    // Handle the client connection here

    struct sockaddr_un client_addr_copy;
    memcpy(
      reinterpret_cast<void*>(&client_addr_copy),
      reinterpret_cast<void*>(&client_addr),
      sizeof(client_addr)
    );

    std::thread worker(workerThread, client_addr_copy, db);
    worker.detach();
  }

cleanup:  
  // De-initialize the database
  db->Close();
  delete db;
  db = nullptr;

  return 0;

cleanup_fail:
  // De-initialize the database
  db->Close();
  delete db;
  db = nullptr;

  return 1;
}
