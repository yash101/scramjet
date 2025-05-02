/**
 * @file server_unix.cpp
 * @brief Implementation of server functions for Unix sockets.
 */

// Networking headers
#include <netinet/in.h>
#include <sys/types.h>
#include <cerrno>
#include <cstring>
#include <sys/socket.h>
#include <arpa/inet.h>
#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#include <unistd.h>
#else
#include <netdb.h>
#endif

// STL headers
#include <string>
#include <memory>
#include <thread>

#include "server.h"

using std::shared_ptr;
using std::string;

class ServerUnix : Server
{
public:
  ServerUnix();
  ~ServerUnix();

  void bindAndListen(std::string path)
  {
  }

  std::thread start()
  {
  }
};
