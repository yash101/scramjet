#include <memory>
#include <tuple>
#include <optional>

template <typename T>
T min(T a, T b)
{
  return (a < b) ? a : b;
}

template <typename T>
class RingBuffer
{
private:
  std::unique_ptr<T[]> m_buffer;
  size_t m_alloc;
  size_t m_size;
  size_t m_start;

public:

  RingBuffer(const RingBuffer&) = delete;
  RingBuffer& operator=(const RingBuffer&) = delete;
  RingBuffer(RingBuffer&&) = delete;
  RingBuffer& operator=(RingBuffer&&) = delete;

  RingBuffer() :
    m_buffer(nullptr),
    m_alloc(0),
    m_size(0),
    m_start(0)
  { }

  RingBuffer(size_t bsize) :
    m_buffer(new T[bsize]),
    m_alloc(bsize),
    m_size(0),
    m_start(0)
  { }

  void resize(size_t bsize)
  {
    // no resizing needed
    if (bsize == m_alloc)
      return;

    // if the new size is smaller than needed, throw an exception
    if (bsize < m_size)
      throw std::runtime_error("Buffer size too small");

    std::unique_ptr<T[]> newBuffer(new T[bsize]);

    // Copy to positions 0...m_size-1 in the new buffer, not starting at m_start
    for (size_t i = 0; i < m_size; ++i)
    {
      newBuffer[i] = m_buffer[(m_start + i) % m_alloc];
    }

    m_buffer.swap(newBuffer);
    m_alloc = bsize;
    m_start = 0;
  }

  void push(const T& value)
  {
    if (m_size == m_alloc)
    {
      resize(m_alloc * 2);
    }

    m_buffer[(m_start + m_size) % m_alloc] = value;
    ++m_size;
  }

  void push_n(const T* values, size_t count)
  {
    size_t newSize = m_size + count;
    size_t newAlloc = m_alloc;
    while (newAlloc < newSize)
    {
      newAlloc *= 2;
    }

    if (count > m_alloc - m_size)
    {
      resize(newAlloc);
    }

    for (size_t i = 0; i < count; i++)
    {
      m_buffer[(m_start + m_size) % m_alloc] = values[i];
      ++m_size;
    }
  }

  std::optional<T> pop()
  {
    if (m_size == 0)
    {
      return std::nullopt; // Buffer is empty
    }

    T value = m_buffer[m_start];
    m_start = (m_start + 1) % m_alloc;
    --m_size;
    return value;
  }

  std::optional<T> peek() const
  {
    if (m_size == 0)
    {
      return std::nullopt;
    }

    return m_buffer[m_start];
  }

  T& operator[](size_t index)
  {
    if (index >= m_size)
    {
      throw std::out_of_range("Index out of range");
    }
    return m_buffer[(m_start + index) % m_alloc];
  }

  size_t pop_n(size_t count, T* out)
  {
    if (count > m_size)
    {
      count = m_size;
    }

    for (size_t i = 0; i < count; ++i)
    {
      out[i] = m_buffer[(m_start + i) % m_alloc];
    }

    m_start = (m_start + count) % m_alloc;
    m_size -= count;

    return count;
  }

  bool empty() const
  {
    return m_size == 0;
  }

  bool full() const
  {
    return m_size == m_alloc;
  }

  size_t size() const
  {
    return m_size;
  }

  size_t capacity() const
  {
    return m_alloc;
  }

  size_t available_without_alloc() const
  {
    return m_alloc - m_size;
  }

  void clear()
  {
    m_size = 0;
    m_start = 0;
  }

  void shrink()
  {
    resize(m_size);
  }
};
