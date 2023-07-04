//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// macros.h
//
// Identification: src/include/common/macros.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cassert>
#include <stdexcept>
#include "common/logger.h"

namespace bustub {

// #define BUSTUB_ASSERT(expr, message) assert((expr) && (message)) 

#define BUSTUB_ASSERT(expr, ...)        \
    if (!(expr)) { \
      LOG_ERROR("assertion failed: %s \n", #expr); \
      ::fprintf(stdout, __VA_ARGS__);  \
      fflush(stdout); \
      while(true); \
   }  


#define UNIMPLEMENTED(message) throw std::logic_error(message)

#define BUSTUB_ENSURE(expr, message) \
  if (!(expr)) {                     \
    throw std::logic_error(message); \
  }

#define UNREACHABLE(message) throw std::logic_error(message)

// Macros to disable copying and moving
#define DISALLOW_COPY(cname)                                    \
  cname(const cname &) = delete;                   /* NOLINT */ \
  auto operator=(const cname &)->cname & = delete; /* NOLINT */

#define DISALLOW_MOVE(cname)                               \
  cname(cname &&) = delete;                   /* NOLINT */ \
  auto operator=(cname &&)->cname & = delete; /* NOLINT */

#define DISALLOW_COPY_AND_MOVE(cname) \
  DISALLOW_COPY(cname);               \
  DISALLOW_MOVE(cname);

}  // namespace bustub
