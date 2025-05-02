// STL headers
#include <string>
#include <memory>
#include <string_view>

using std::shared_ptr;
using std::string;

// TODO: move into its own header and properly define
class Server
{
protected:
  shared_ptr<void> m_DbInstance;

  constexpr inline decltype(m_DbInstance)& db()
  {
    return m_DbInstance;
  }

  void put(string& key, string& val)
  {
    m_DbInstance.
  }

  void get(string& key)
  {
  }

  void getRange(string& keyStart, string& keyEnd)
  {
  }

  void getMultiple(string& keyStart, size_t n)
  {
  }
};
