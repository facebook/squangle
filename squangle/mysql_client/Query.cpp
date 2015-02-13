/*
 *  Copyright (c) 2015, Facebook, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant
 *  of patent rights can be found in the PATENTS file in the same directory.
 *
 */

#include "squangle/mysql_client/Query.h"
#include "folly/String.h"
#include "folly/Format.h"

#include <boost/algorithm/string.hpp>
#include <algorithm>
#include <vector>

namespace facebook {
namespace common {
namespace mysql_client {

Query::Query(const StringPiece query_text, dynamic param)
    : query_text_(query_text.begin(), query_text.end()),
      unsafe_query_(false),
      params_(dynamic{param}) {}

Query::~Query() {}

namespace {
// Some helper functions for encoding/escaping.
void appendComment(folly::fbstring* s, const dynamic& d) {
  auto str = d.asString();
  boost::replace_all(str, "/*", " / * ");
  boost::replace_all(str, "*/", " * / ");
  s->append(str);
}

void appendColumnTableName(folly::fbstring* s, const dynamic& d) {
  if (d.isString()) {
    s->reserve(s->size() + d.size() + 4);
    s->push_back('`');
    for (char c : d.asString()) {
      // Toss in an extra ` if we see one.
      if (c == '`') {
        s->push_back('`');
      }
      s->push_back(c);
    }
    s->push_back('`');
  } else {
    s->append(d.asString());
  }
}

// Raise an exception with, hopefully, a helpful error message.
void parseError(const StringPiece s, size_t offset, const StringPiece message) {
  const std::string msg =
      folly::format(
          "Parse error at offset {}: {}, query: {}", offset, message, s).str();
  throw std::invalid_argument(msg);
}

// Consume the next x bytes from s, updating offset, and raising an
// exception if there aren't sufficient bytes left.
StringPiece advance(const StringPiece s, size_t* offset, size_t num) {
  if (s.size() <= *offset + num) {
    parseError(s, *offset, "unexpected end of string");
  }
  *offset += num;
  return StringPiece(s.data() + *offset - num + 1, s.data() + *offset + 1);
}

// Escape a string (or copy it through unmodified if no connection is
// available).
void appendEscapedString(folly::fbstring* dest,
                         const folly::fbstring& value,
                         MYSQL* connection) {
  if (!connection) {
    VLOG(1) << "connectionless escape performed; this should only occur in "
            << "testing.";
    *dest += value;
    return;
  }

  size_t old_size = dest->size();
  dest->resize(old_size + 2 * value.size() + 1);
  size_t actual_value_size = mysql_real_escape_string(
      connection, &(*dest)[old_size], value.data(), value.size());
  dest->resize(old_size + actual_value_size);
}

} // namespace

// Append a dynamic to the query string we're building.  We ensure the
// type matches the dynamic's type (or allow a magic 'v' type to be
// any value, but this isn't exposed to the users of the library).
void Query::appendValue(folly::fbstring* s,
                        size_t offset,
                        char type,
                        const dynamic& d,
                        MYSQL* connection) const {
  if (d.isString()) {
    if (type != 's' && type != 'v') {
      parseError(query_text_, offset, "%s used with non-string");
    }
    auto value = d.asString();
    s->reserve(s->size() + value.size() + 4);
    s->push_back('"');
    appendEscapedString(s, value, connection);
    s->push_back('"');
  } else if (d.isInt()) {
    if (type != 'd' && type != 'v') {
      parseError(query_text_, offset, "%d used with non-integer");
    }
    s->append(d.asString());
  } else if (d.isDouble()) {
    if (type != 'f' && type != 'v') {
      parseError(query_text_, offset, "%f used with non-double");
    }
    s->append(d.asString());
  } else if (d.isNull()) {
    s->append("NULL");
  } else {
    parseError(
        query_text_,
        offset,
        folly::format("invalid type for %{}: {}", type, d.typeName()).str());
  }
}

void Query::appendValueClauses(folly::fbstring* ret,
                               size_t* idx,
                               const char* sep,
                               const dynamic& param,
                               MYSQL* connection) const {
  if (!param.isObject()) {
    parseError(query_text_,
               *idx,
               folly::format("object expected for %Lx but received {}",
                             param.typeName()).str());
  }
  // Sort these to get consistent query ordering (mainly for
  // testing, but also aesthetics of the final query).
  std::vector<dynamic> keys(param.keys().begin(), param.keys().end());
  std::sort(keys.begin(), keys.end());

  bool first_param = true;
  for (const auto& key : keys) {
    if (!first_param) {
      ret->append(sep);
    }
    first_param = false;
    appendColumnTableName(ret, key);
    if (param[key].isNull()) {
      ret->append(" IS NULL");
    } else {
      ret->append(" = ");
      appendValue(ret, *idx, 'v', param[key], connection);
    }
  }
}

folly::fbstring Query::renderMultiQuery(MYSQL* connection,
                                        const std::vector<Query>& queries) {
  auto reserve_size = 0;
  for (const Query& query : queries) {
    reserve_size += query.query_text_.size() + 8 * query.params_.size();
  }
  folly::fbstring ret;
  ret.reserve(reserve_size);

  // Not adding `;` in the end
  for (const Query& query : queries) {
    if (!ret.empty()) {
      ret.append(";");
    }
    ret.append(query.render(connection));
  }

  return ret;
}

folly::fbstring Query::renderInsecure() const {
  return render(nullptr, params_);
}

folly::fbstring Query::renderInsecure(const dynamic params) const {
  return render(nullptr, params);
}

folly::fbstring Query::render(MYSQL* conn) const {
  return render(conn, params_);
}

folly::fbstring Query::render(MYSQL* conn, const dynamic params) const {
  if (unsafe_query_) {
    return query_text_;
  }

  auto offset = query_text_.find_first_of(";'\"`");
  if (offset != StringPiece::npos) {
    parseError(query_text_, offset, "Saw dangerous characters in SQL query");
  }

  folly::fbstring ret;
  ret.reserve(query_text_.size() + 8 * params.size());

  auto current_param = params.begin();
  bool after_percent = false;
  size_t idx;
  // Walk our string, watching for % values.
  for (idx = 0; idx < query_text_.size(); ++idx) {
    char c = query_text_[idx];
    if (!after_percent) {
      if (c != '%') {
        ret.push_back(c);
      } else {
        after_percent = true;
      }
      continue;
    }

    after_percent = false;
    if (c == '%') {
      ret.push_back('%');
      continue;
    }

    if (current_param == params.end()) {
      parseError(query_text_, idx, "too few parameters for query");
    }

    const auto& param = *current_param++;
    if (c == 'd' || c == 's' || c == 'f') {
      appendValue(&ret, idx, c, param, conn);
    } else if (c == 'K') {
      ret.append("/*");
      appendComment(&ret, param);
      ret.append("*/");
    } else if (c == 'T' || c == 'C') {
      appendColumnTableName(&ret, param);
    } else if (c == '=') {
      StringPiece type = advance(query_text_, &idx, 1);
      if (type != "d" && type != "s" && type != "f") {
        parseError(query_text_, idx, "expected %=d, %=c, or %=s");
      }

      if (param.isNull()) {
        ret.append(" IS NULL");
      } else {
        ret.append(" = ");
        appendValue(&ret, idx, type[0], param, conn);
      }
    } else if (c == 'V') {
      size_t col_idx;
      size_t row_len = 0;
      bool first_row = true;
      bool first_in_row = true;
      for (const auto& row : param) {
        first_in_row = true;
        col_idx = 0;
        if (!first_row) {
          ret.append(", ");
        }
        ret.append("(");
        for (const auto& col : row) {
          if (!first_in_row) {
            ret.append(", ");
          }
          appendValue(&ret, idx, 'v', col, conn);
          col_idx++;
          first_in_row = false;
          if (first_row) {
            row_len++;
          }
        }
        ret.append(")");
        if (first_row) {
          first_row = false;
        } else if (col_idx != row_len) {
          parseError(
              query_text_,
              idx,
              "not all rows provided for %V formatter are the same size");
        }
      }
    } else if (c == 'L') {
      StringPiece type = advance(query_text_, &idx, 1);
      if (type == "O" || type == "A") {
        ret.append("(");
        const char* sep = (type == "O") ? " OR " : " AND ";
        appendValueClauses(&ret, &idx, sep, param, conn);
        ret.append(")");
      } else {
        if (!param.isArray()) {
          parseError(query_text_, idx, "expected array for %L formatter");
        }

        bool first_param = true;
        for (const auto& val : param) {
          if (!first_param) {
            ret.append(", ");
          }
          first_param = false;
          if (type == "C") {
            appendColumnTableName(&ret, val);
          } else {
            appendValue(&ret, idx, type[0], val, conn);
          }
        }
      }
    } else if (c == 'U' || c == 'W') {
      if (c == 'W') {
        appendValueClauses(&ret, &idx, " AND ", param, conn);
      } else {
        appendValueClauses(&ret, &idx, ", ", param, conn);
      }
    } else if (c == 'Q') {
      ret.append((param).asString());
    } else {
      parseError(query_text_, idx, "unknown % code");
    }
  }

  if (after_percent) {
    parseError(query_text_, idx, "string ended with unfinished % code");
  }

  if (current_param != params.end()) {
    parseError(query_text_, 0, "too many parameters specified for query");
  }

  return ret;
}
}
}
} // namespace facebook::common::mysql_client
