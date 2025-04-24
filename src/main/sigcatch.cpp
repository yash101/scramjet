#include <iostream>
#include <functional>
#include <utility>

// Variadic template function
template <
  typename Func,
  typename... Args
>
void signalTry(Func func,Args&&... args)
{
  // Call the function with all arguments except the one to skip
  func(std::forward<Args>(args)...);
}
