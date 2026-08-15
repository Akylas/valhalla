#ifndef VALHALLA_MIDGARD_LOGGING_H_
#define VALHALLA_MIDGARD_LOGGING_H_

#include <boost/property_tree/ptree_fwd.hpp>

#include <mutex>
#include <sstream>
#include <string>
#include <type_traits>
#include <unordered_map>

namespace valhalla {
namespace midgard {

namespace logging {

// Helper to handle both old string concatenation and new format strings
namespace detail {
// CARTOHACK: upstream uses std::format here, which Apple's libc++ gates behind an
// iOS 16.3 availability annotation (std::to_chars for floating point). This shim only
// substitutes plain "{}" — every log call in the tree uses that form.
// TODO: drop this and restore std::format once the iOS deployment target reaches 16.3.
inline void format_append(std::ostringstream& out, std::string_view fmt_str) {
  out << fmt_str;
}

template <typename Arg, typename... Rest>
inline void
format_append(std::ostringstream& out, std::string_view fmt_str, Arg&& arg, Rest&&... rest) {
  const auto pos = fmt_str.find("{}");
  if (pos == std::string_view::npos) {
    out << fmt_str;
    return;
  }
  out << fmt_str.substr(0, pos) << std::forward<Arg>(arg);
  format_append(out, fmt_str.substr(pos + 2), std::forward<Rest>(rest)...);
}

template <typename... Args>
inline std::string format_or_pass(std::string_view fmt_str, Args&&... args) {
  std::ostringstream out;
  format_append(out, fmt_str, std::forward<Args>(args)...);
  return out.str();
}
} // namespace detail

// a factory that can create loggers (that derive from 'logger') via function pointers
// this way you could make your own logger that sends log messages to who knows where
class Logger;
using LoggingConfig = std::unordered_map<std::string, std::string>;
using LoggerCreator = Logger* (*)(const LoggingConfig&);
class LoggerFactory : public std::unordered_map<std::string, LoggerCreator> {
public:
  Logger* Produce(const LoggingConfig& config) const;
};

// register your custom loggers here
bool RegisterLogger(const std::string& name, LoggerCreator function_ptr);

// the Log levels we support
enum class LogLevel : char { LogTrace, LogDebug, LogInfo, LogWarn, LogError };

// logger base class, not pure virtual so you can use as a null logger if you want
class Logger {
public:
  Logger() = delete;
  Logger(const LoggingConfig& config);
  virtual ~Logger();
  virtual void Log(const std::string&, const LogLevel);
  virtual void Log(const std::string&, const std::string& custom_directive = " [TRACE] ");

protected:
  std::mutex lock;
};

// statically get a logger using the factory
// CARTOHACK
Logger& GetLogger(const LoggingConfig& config = {{"type", ""}, {"color", "true"}});

// statically log manually without the macros below
void Log(const std::string&, const LogLevel);
void Log(const std::string&, const std::string& custom_directive = " [TRACE] ");

// statically configure logging
// try something like:
// logging::Configure({ {"type", "std_out"}, {"color", ""} })
// logging::Configure({ {"type", "file"}, {"file_name", "test.log"}, {"reopen_interval", "1"} })
void Configure(const LoggingConfig& config);

// configure logging from the top-level "logging" section of a boost property tree config
void ConfigureFromPtree(const boost::property_tree::ptree& config);

// guarding against redefinitions
#ifndef LOG_ERROR
#ifndef LOG_WARN
#ifndef LOG_INFO
#ifndef LOG_DEBUG
#ifndef LOG_TRACE

// convenience macros stand out when reading code
// default to seeing INFO and up if nothing was specified
#ifndef LOGGING_LEVEL_NONE
#ifndef LOGGING_LEVEL_ALL
#ifndef LOGGING_LEVEL_ERROR
#ifndef LOGGING_LEVEL_WARN
#ifndef LOGGING_LEVEL_INFO
#ifndef LOGGING_LEVEL_DEBUG
#ifndef LOGGING_LEVEL_TRACE
#define LOGGING_LEVEL_INFO
#endif
#endif
#endif
#endif
#endif
#endif
#endif
// mark all the stuff we should see
#ifndef LOGGING_LEVEL_NONE
#ifndef LOGGING_LEVEL_ERROR
#define LOGGING_LEVEL_ERROR
#ifndef LOGGING_LEVEL_WARN
#define LOGGING_LEVEL_WARN
#ifndef LOGGING_LEVEL_INFO
#define LOGGING_LEVEL_INFO
#ifndef LOGGING_LEVEL_DEBUG
#define LOGGING_LEVEL_DEBUG
#ifndef LOGGING_LEVEL_TRACE
#define LOGGING_LEVEL_TRACE
#endif
#endif
#endif
#endif
#endif
#endif
// no logging output
#ifdef LOGGING_LEVEL_NONE
#define LOG_ERROR(...)
#define LOG_WARN(...)
#define LOG_INFO(...)
#define LOG_DEBUG(...)
#define LOG_TRACE(...)
// all logging output
#elif defined(LOGGING_LEVEL_ALL)
#define LOG_ERROR(...)                                                                               \
  ::valhalla::midgard::logging::GetLogger()                                                          \
      .Log(::valhalla::midgard::logging::detail::format_or_pass(__VA_ARGS__),                        \
           ::valhalla::midgard::logging::LogLevel::LogError)
#define LOG_WARN(...)                                                                                \
  ::valhalla::midgard::logging::GetLogger()                                                          \
      .Log(::valhalla::midgard::logging::detail::format_or_pass(__VA_ARGS__),                        \
           ::valhalla::midgard::logging::LogLevel::LogWarn)
#define LOG_INFO(...)                                                                                \
  ::valhalla::midgard::logging::GetLogger()                                                          \
      .Log(::valhalla::midgard::logging::detail::format_or_pass(__VA_ARGS__),                        \
           ::valhalla::midgard::logging::LogLevel::LogInfo)
#define LOG_DEBUG(...)                                                                               \
  ::valhalla::midgard::logging::GetLogger()                                                          \
      .Log(::valhalla::midgard::logging::detail::format_or_pass(__VA_ARGS__),                        \
           ::valhalla::midgard::logging::LogLevel::LogDebug)
#define LOG_TRACE(...)                                                                               \
  ::valhalla::midgard::logging::GetLogger()                                                          \
      .Log(::valhalla::midgard::logging::detail::format_or_pass(__VA_ARGS__),                        \
           ::valhalla::midgard::logging::LogLevel::LogTrace)
// some level and up
#else
#ifdef LOGGING_LEVEL_ERROR
#define LOG_ERROR(...)                                                                               \
  ::valhalla::midgard::logging::GetLogger()                                                          \
      .Log(::valhalla::midgard::logging::detail::format_or_pass(__VA_ARGS__),                        \
           ::valhalla::midgard::logging::LogLevel::LogError)
#define LOGLN_ERROR(fmt, ...)                                                                        \
  ::valhalla::midgard::logging::GetLogger()                                                          \
      .Log(std::string(__FILE__) + ":" + std::to_string(__LINE__) + ": " +                           \
               ::valhalla::midgard::logging::detail::format_or_pass(__VA_ARGS__),                    \
           ::valhalla::midgard::logging::LogLevel::LogError)
#else
#define LOG_ERROR(...)
#define LOGLN_ERROR(...)
#endif
#ifdef LOGGING_LEVEL_WARN
#define LOG_WARN(...)                                                                                \
  ::valhalla::midgard::logging::GetLogger()                                                          \
      .Log(::valhalla::midgard::logging::detail::format_or_pass(__VA_ARGS__),                        \
           ::valhalla::midgard::logging::LogLevel::LogWarn)
#define LOGLN_WARN(...)                                                                              \
  ::valhalla::midgard::logging::GetLogger()                                                          \
      .Log(std::string(__FILE__) + ":" + std::to_string(__LINE__) + ": " +                           \
               ::valhalla::midgard::logging::detail::format_or_pass(__VA_ARGS__),                    \
           ::valhalla::midgard::logging::LogLevel::LogWarn)
#else
#define LOG_WARN(...)
#define LOGLN_WARN(...)
#endif
#ifdef LOGGING_LEVEL_INFO
#define LOG_INFO(...)                                                                                \
  ::valhalla::midgard::logging::GetLogger()                                                          \
      .Log(::valhalla::midgard::logging::detail::format_or_pass(__VA_ARGS__),                        \
           ::valhalla::midgard::logging::LogLevel::LogInfo)
#define LOGLN_INFO(...)                                                                              \
  ::valhalla::midgard::logging::GetLogger()                                                          \
      .Log(std::string(__FILE__) + ":" + std::to_string(__LINE__) + ": " +                           \
               ::valhalla::midgard::logging::detail::format_or_pass(__VA_ARGS__),                    \
           ::valhalla::midgard::logging::LogLevel::LogInfo)
#else
#define LOG_INFO(...) ;
#define LOGLN_INFO(...) ;
#endif
#ifdef LOGGING_LEVEL_DEBUG
#define LOG_DEBUG(...)                                                                               \
  ::valhalla::midgard::logging::GetLogger()                                                          \
      .Log(::valhalla::midgard::logging::detail::format_or_pass(__VA_ARGS__),                        \
           ::valhalla::midgard::logging::LogLevel::LogDebug)
#else
#define LOG_DEBUG(...)
#endif
#ifdef LOGGING_LEVEL_TRACE
#define LOG_TRACE(...)                                                                               \
  ::valhalla::midgard::logging::GetLogger()                                                          \
      .Log(std::string(__FILE__) + ":" + std::to_string(__LINE__) + ": " +                           \
               ::valhalla::midgard::logging::detail::format_or_pass(__VA_ARGS__),                    \
           ::valhalla::midgard::logging::LogLevel::LogTrace)
#else
#define LOG_TRACE(...)
#endif
#endif

// guarding against redefinitions
#endif
#endif
#endif
#endif
#endif

} // namespace logging

} // namespace midgard
} // namespace valhalla

#endif
