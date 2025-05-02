#ifndef _FCSH_SERVER_H
#define _FCSH_SERVER_H

namespace fcsh
{
  class Server
  {
  protected:
    std::shared_ptr<void> m_DbInstance;

    constexpr inline decltype(m_DbInstance)& db()
    {
      return m_DbInstance;
    }
  }
}

#endif
