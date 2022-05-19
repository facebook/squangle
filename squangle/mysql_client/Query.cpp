/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "squangle/mysql_client/Query.h"
#include <folly/Format.h>
#include <folly/String.h>

#include <boost/algorithm/string.hpp>
#include <boost/variant.hpp>
#include <boost/variant/get.hpp>
#include <algorithm>
#include <vector>

namespace facebook {
namespace common {
namespace mysql_client {

#define TYPE_CHECK(expected) \
  if (type_ != expected)     \
  throw std::invalid_argument("DataType doesn't match with the call")

typedef std::pair<std::string, QueryArgument> ArgPair;

// string constructors
QueryArgument::QueryArgument(folly::StringPiece val)
    : value_(std::string(val.data(), val.size())) {}

QueryArgument::QueryArgument(char const* val) : value_(std::string(val)) {}

QueryArgument::QueryArgument(const std::string& string_value)
    : value_(string_value) {}

QueryArgument::QueryArgument(std::string&& val) : value_(std::move(val)) {}

QueryArgument::QueryArgument(const folly::fbstring& val)
    : value_(std::string(val)) {}

QueryArgument::QueryArgument(double double_val) : value_(double_val) {}

QueryArgument::QueryArgument(std::initializer_list<QueryArgument> list)
    : value_(std::vector<QueryArgument>(list.begin(), list.end())) {}

QueryArgument::QueryArgument(std::vector<QueryArgument> arg_list)
    : value_(std::vector<QueryArgument>(arg_list.begin(), arg_list.end())) {}

QueryArgument::QueryArgument() : value_(std::vector<ArgPair>()) {}

QueryArgument::QueryArgument(folly::StringPiece param1, QueryArgument param2)
    : value_(std::vector<ArgPair>()) {
  pairs().emplace_back(ArgPair(param1.str(), param2));
}

QueryArgument::QueryArgument(Query q) : value_(std::move(q)) {}

bool QueryArgument::isString() const {
  return value_.type() == typeid(std::string);
}

bool QueryArgument::isQuery() const {
  return value_.type() == typeid(Query);
}

bool QueryArgument::isPairList() const {
  return value_.type() == typeid(std::vector<ArgPair>);
}

bool QueryArgument::isNull() const {
  return value_.type() == typeid(std::nullptr_t);
}

bool QueryArgument::isList() const {
  return value_.type() == typeid(std::vector<QueryArgument>);
}

bool QueryArgument::isDouble() const {
  return value_.type() == typeid(double);
}

bool QueryArgument::isInt() const {
  return value_.type() == typeid(int64_t);
}

bool QueryArgument::isTwoTuple() const {
  return value_.type() == typeid(std::tuple<std::string, std::string>);
}

bool QueryArgument::isThreeTuple() const {
  return value_.type() ==
      typeid(std::tuple<std::string, std::string, std::string>);
}

QueryArgument&& QueryArgument::operator()(
    folly::StringPiece q1,
    const QueryArgument& q2) {
  pairs().emplace_back(ArgPair(q1.str(), q2));
  return std::move(*this);
}

QueryArgument&& QueryArgument::operator()(
    std::string&& q1,
    QueryArgument&& q2) {
  pairs().emplace_back(ArgPair(std::move(q1), std::move(q2)));
  return std::move(*this);
}

namespace { // anonymous namespace to prevent class shadowing
struct StringConverter : public boost::static_visitor<std::string> {
  std::string operator()(const double& operand) const {
    return folly::to<std::string>(operand);
  }
  std::string operator()(const bool& operand) const {
    return folly::to<std::string>(operand);
  }
  std::string operator()(const int64_t& operand) const {
    return folly::to<std::string>(operand);
  }
  std::string operator()(const std::string& operand) const {
    return operand;
  }
  template <typename T>
  std::string operator()(const T& /*operand*/) const {
    throw std::invalid_argument(fmt::format(
        "Only allowed type conversions are Int, Double, Bool and String:"
        " type found: {}",
        boost::typeindex::type_id<T>().pretty_name()));
  }
};

} // namespace

std::string QueryArgument::asString() const {
  return boost::apply_visitor(StringConverter(), value_);
}

double QueryArgument::getDouble() const {
  return boost::get<double>(value_);
}

int64_t QueryArgument::getInt() const {
  return boost::get<int64_t>(value_);
}

const Query& QueryArgument::getQuery() const {
  return boost::get<Query>(value_);
}

const std::string& QueryArgument::getString() const {
  return boost::get<std::string>(value_);
}

const std::vector<QueryArgument>& QueryArgument::getList() const {
  return boost::get<std::vector<QueryArgument>>(value_);
}

const std::vector<ArgPair>& QueryArgument::getPairs() const {
  return boost::get<std::vector<ArgPair>>(value_);
}

const std::tuple<std::string, std::string>& QueryArgument::getTwoTuple() const {
  return boost::get<std::tuple<std::string, std::string>>(value_);
}

const std::tuple<std::string, std::string, std::string>&
QueryArgument::getThreeTuple() const {
  return boost::get<std::tuple<std::string, std::string, std::string>>(value_);
}

void QueryArgument::initFromDynamic(const folly::dynamic& param) {
  // Convert to basic values and get type
  if (param.isObject()) {
    // List of pairs
    std::vector<folly::dynamic> keys(param.keys().begin(), param.keys().end());
    std::sort(keys.begin(), keys.end());
    value_ = std::vector<ArgPair>();
    auto& vec = boost::get<std::vector<ArgPair>>(value_);
    vec.reserve(keys.size());

    for (const auto& key : keys) {
      QueryArgument q2(fromDynamic(param[key]));

      vec.emplace_back(ArgPair(key.asString(), q2));
    }
  } else if (param.isNull()) {
    value_ = nullptr;
  } else if (param.isArray()) {
    value_ = std::vector<QueryArgument>();
    boost::get<std::vector<QueryArgument>>(value_).reserve(param.size());
    auto& v = boost::get<std::vector<QueryArgument>>(value_);
    for (const auto& val : param) {
      v.emplace_back(fromDynamic(val));
    }
  } else if (param.isString()) {
    value_ = param.getString();
  } else if (param.isBool()) {
    value_ = static_cast<int64_t>(param.asBool());
  } else if (param.isDouble()) {
    value_ = param.asDouble();
  } else if (param.isInt()) {
    value_ = param.asInt();
  } else {
    throw std::invalid_argument("Dynamic type doesn't match to accepted ones");
  }
}

std::vector<ArgPair>& QueryArgument::pairs() {
  return boost::get<std::vector<ArgPair>>(value_);
}

Query::Query(
    const folly::StringPiece query_text,
    std::vector<QueryArgument> params)
    : query_text_(query_text), unsafe_query_(false), params_(params) {}

Query::~Query() {}

// Some helper functions for encoding/escaping.
void Query::QueryRenderer::appendComment(const QueryArgument& arg) {
  // Note this used to use regex processing, but that can be expensive and
  // required three passes through the string
  //  auto str = d.asString();
  //  boost::replace_all(str, "/*", " / * ");
  //  boost::replace_all(str, "*/", " * / ");
  //  working_.append(str);

  char last = '\0';
  for (char c : arg.getString()) {
    if ((c == '/' && last == '*') || (c == '*' && last == '/')) {
      working_.insert(working_.end() - 1, ' ');
      working_.push_back(' ');
      working_.push_back(c);
      working_.push_back(' ');
    } else {
      working_.push_back(c);
    }

    last = c;
  }
}

void Query::QueryRenderer::appendIdentifierWithBacktickEscaping(
    const QueryArgument& arg) {
  static constexpr const char kBacktick = '`';
  DCHECK(arg.isString());
  working_.reserve(working_.size() + arg.getString().size() + 4);
  quote(kBacktick, [&]() {
    for (char c : arg.getString()) {
      // Toss in an extra ` if we see one.
      if (c == kBacktick) {
        working_.push_back(kBacktick);
      }
      working_.push_back(c);
    }
  });
}

void Query::QueryRenderer::appendIdentifier(const QueryArgument& arg) {
  if (arg.isString()) {
    appendIdentifierWithBacktickEscaping(arg);
  } else if (arg.isTwoTuple()) {
    // If a two-tuple is provided we have a qualified column name
    const auto& t = arg.getTwoTuple();
    appendIdentifierWithBacktickEscaping(std::get<0>(t));
    working_.push_back('.');
    appendIdentifierWithBacktickEscaping(std::get<1>(t));
  } else if (arg.isThreeTuple()) {
    // If a three-tuple is provided we have a qualified column name
    // with an alias. This is helpful for constructing JOIN queries.
    const auto& t = arg.getThreeTuple();
    appendIdentifierWithBacktickEscaping(std::get<0>(t));
    working_.push_back('.');
    appendIdentifierWithBacktickEscaping(std::get<1>(t));
    working_.append(" AS ");
    appendIdentifierWithBacktickEscaping(std::get<2>(t));
  } else {
    working_.append(arg.asString());
  }
}

// Raise an exception with, hopefully, a helpful error message.
void Query::QueryRenderer::parseError(const std::string& message) const {
  throw std::invalid_argument(fmt::format(
      "Parse error at offset {}: {}, query: {}", offset_, message, query_));
}

// Raise an exception for format string/value mismatches
void Query::QueryRenderer::formatStringParseError(
    char fmt,
    folly::StringPiece value_type) const {
  parseError(fmt::format(
      "invalid value type {} for format string %{}", value_type, fmt));
}

// Escape a string (or copy it through unmodified if no connection is
// available).
void Query::QueryRenderer::appendEscapedString(folly::StringPiece value) {
  if (!mysql_) {
    VLOG(3) << "connectionless escape performed; this should only occur in "
            << "testing.";
    working_ += value;
    return;
  }

  size_t old_size = working_.size();
  working_.resize(old_size + 2 * value.size() + 1);
  size_t actual_value_size = mysql_real_escape_string(
      mysql_, &working_[old_size], value.data(), value.size());
  working_.resize(old_size + actual_value_size);
}

void Query::append(const Query& query2) {
  query_text_ += query2.query_text_;
  for (const auto& param2 : query2.params_) {
    params_.push_back(param2);
  }
}

void Query::append(Query&& query2) {
  query_text_ += query2.query_text_;
  for (const auto& param2 : query2.params_) {
    params_.push_back(std::move(param2));
  }
}

// Append a dynamic to the query string we're building.  We ensure the
// type matches the dynamic's type (or allow a magic 'v' type to be
// any value, but this isn't exposed to the users of the library).
void Query::QueryRenderer::appendValue(char type, const QueryArgument& d) {
  if (d.isString()) {
    if (type != 's' && type != 'v' && type != 'm') {
      formatStringParseError(type, "string");
    }

    if (normalize_) {
      working_.append("{S}");
    } else {
      auto value = d.asString();
      working_.reserve(working_.size() + value.size() + 4);
      quote('"', [&]() { appendEscapedString(value); });
    }
  } else if (d.isInt()) {
    if (type != 'd' && type != 'v' && type != 'm' && type != 'u') {
      formatStringParseError(type, "int");
    }
    if (normalize_) {
      working_.append("{N}");
    } else {
      if (type == 'u') {
        working_.append(
            folly::to<std::string>(static_cast<uint64_t>(d.getInt())));
      } else {
        working_.append(d.asString());
      }
    }
  } else if (d.isDouble()) {
    if (type != 'f' && type != 'v' && type != 'm') {
      formatStringParseError(type, "double");
    }
    if (normalize_) {
      working_.append("{N}");
    } else {
      working_.append(d.asString());
    }
  } else if (d.isQuery()) {
    working_.append(d.getQuery().render(mysql_, normalize_));
  } else if (d.isNull()) {
    working_.append("NULL");
  } else {
    formatStringParseError(type, d.typeName());
  }
}

void Query::QueryRenderer::appendValues(const QueryArgument& arg) {
  if (normalize_) {
    working_.append("{...}");
  } else {
    size_t row_len = 0;
    formatList(arg.getList(), [&](const auto& row, size_t count) {
      size_t col_count = 0;
      parenthesize([&]() {
        col_count = formatList(
            row.getList(),
            [&](const auto& col, size_t /*count*/) { appendValue('v', col); });
      });

      if (count == 0) {
        row_len = col_count;
      } else if (row_len != col_count) {
        // All subsequent rows need to have the same number of columns
        parseError("not all rows provided for %V formatter are the same size");
      }
    });
  }
}

void Query::QueryRenderer::appendList(char code, const QueryArgument& arg) {
  formatList(arg.getList(), [&](const auto& val, size_t /*count*/) {
    if (code == 'C') {
      appendIdentifier(val);
    } else {
      appendValue(code, val);
    }
  });
}

void Query::QueryRenderer::appendValueClauses(
    const char* sep,
    const QueryArgument& arg) {
  if (!arg.isPairList()) {
    parseError(
        fmt::format("object expected for %Lx but received {}", arg.typeName()));
  }

  // Sort these to get consistent query ordering (mainly for
  // testing, but also aesthetics of the final query).
  formatList(
      arg.getPairs(),
      [&](const auto& pair, size_t /*count*/) {
        const auto& [column, value] = pair;
        appendIdentifier(column);
        if (value.isNull() && sep[0] != ',') {
          working_.append(" IS NULL");
        } else {
          working_.append(" = ");
          appendValue('v', value);
        }
      },
      sep);
}

void Query::QueryRenderer::renderEqualitySpec(
    char code,
    const QueryArgument& arg) {
  if (code != 'd' && code != 's' && code != 'f' && code != 'u' && code != 'm') {
    parseError("expected %=d, %=f, %=s, %=u, or %=m");
  }

  if (arg.isNull()) {
    working_.append(" IS NULL");
  } else {
    working_.append(" = ");
    appendValue(code, arg);
  }
}

void Query::QueryRenderer::renderListSpec(char code, const QueryArgument& arg) {
  if (code == 'O' || code == 'A') {
    parenthesize(
        [&]() { appendValueClauses((code == 'O') ? kOr : kAnd, arg); });
  } else {
    if (!arg.isList()) {
      parseError("expected array for %L formatter");
    }

    appendList(code, arg);
  }
}

void Query::QueryRenderer::renderModifiedFormatSpec(
    char modifier,
    char code,
    const QueryArgument& arg) {
  if (modifier == 'L') {
    renderListSpec(code, arg);
  } else {
    DCHECK_EQ(modifier, '=');
    renderEqualitySpec(code, arg);
  }
}

void Query::QueryRenderer::renderDynamic(char code, const QueryArgument& arg) {
  if (!(arg.isString() || arg.isInt() || arg.isDouble() || arg.isNull())) {
    parseError("%m expects int/float/string/bool");
  }
  appendValue(code, arg);
}

void Query::QueryRenderer::renderComment(const QueryArgument& arg) {
  if (!normalize_) {
    working_.append("/*");
    appendComment(arg);
    working_.append("*/");
  }
}

void Query::QueryRenderer::renderValues(const QueryArgument& arg) {
  if (arg.isQuery()) {
    parseError("%V doesn't allow subquery");
  }

  appendValues(arg);
}

void Query::QueryRenderer::renderSubQuery(const QueryArgument& arg) {
  if (arg.isQuery()) {
    working_.append(arg.getQuery().render(mysql_, normalize_));
  } else {
    working_.append(arg.asString());
  }
}

void Query::QueryRenderer::renderFormatSpec(
    std::string_view format_spec,
    const QueryArgument& arg) {
  DCHECK_GE(format_spec.size(), 2);
  DCHECK_LE(format_spec.size(), 3);
  DCHECK_EQ(format_spec[0], '%');

  char code = format_spec.back();
  if (format_spec.size() == 3) {
    renderModifiedFormatSpec(format_spec[1], code, arg);
  } else {
    if (code == 'd' || code == 's' || code == 'f' || code == 'u') {
      appendValue(code, arg);
    } else if (code == 'm') {
      renderDynamic(code, arg);
    } else if (code == 'K') {
      renderComment(arg);
    } else if (code == 'T' || code == 'C') {
      appendIdentifier(arg);
    } else if (code == 'V') {
      renderValues(arg);
    } else if (code == 'U') {
      appendValueClauses(", ", arg);
    } else if (code == 'W') {
      appendValueClauses(kAnd, arg);
    } else if (code == 'Q') {
      renderSubQuery(arg);
    } else {
      parseError("unknown % code");
    }
  }
}

void Query::QueryRenderer::walkFormat(
    DataFunc datafunc,
    FormatFunc formatfunc) {
  auto it = args_.begin();
  size_t start = 0;

  enum State {
    NORMAL,
    SEEN_PCT,
    SEEN_MODIFIER,
  };

  State state = NORMAL;

  // walk through the string looking for % specs
  for (offset_ = 0; offset_ < query_.size(); ++offset_) {
    char c = query_[offset_];
    if (state == NORMAL) {
      if (c == '%') {
        if (offset_ != start) {
          datafunc(std::string_view(query_.data() + start, offset_ - start));
          start = offset_;
        }
        state = SEEN_PCT;
      }

      continue;
    }

    if (state == SEEN_PCT && c == '%') {
      datafunc(std::string_view("%"));
      state = NORMAL;
      start = offset_ + 1;
      continue;
    }

    if (state != SEEN_MODIFIER && (c == 'L' || c == '=')) {
      state = SEEN_MODIFIER;
      continue;
    }

    if (it == args_.end()) {
      parseError("too few parameters for query");
    }

    formatfunc(
        std::string_view(query_.data() + start, offset_ - start + 1), *it);
    ++it;
    start = offset_ + 1;
    state = NORMAL;
  }

  if (state != NORMAL) {
    parseError("string ended with unfinished % code");
  }

  if (it != args_.end()) {
    parseError("too many parameters specified for query");
  }

  // Don't forget any normal data after the last format spec
  if (start != offset_) {
    datafunc(std::string_view(query_.data() + start, offset_ - start));
  }
}

Query::QueryStringType Query::QueryRenderer::render(bool unsafe_query) {
  if (unsafe_query) {
    return query_.to<Query::QueryStringType>();
  }

  auto offset = query_.find_first_of(";'\"`");
  if (offset != folly::StringPiece::npos) {
    parseError("Saw dangerous characters in SQL query");
  }

  working_.reserve(query_.size() + 8 * args_.size());

  walkFormat(
      [&](auto data) {
        // String data just needs to be appended to the output
        DCHECK(!data.empty());
        working_.append(data);
      },
      [&](auto data, const auto& arg) {
        // Format spec:
        //   `data` contains %[modifier]spec
        //   `arg` contains the current QueryArgument
        DCHECK(!data.empty());
        DCHECK_EQ(data[0], '%');
        renderFormatSpec(data, arg);
      });

  return working_;
}

std::vector<Query::FormatArgumentPair> Query::QueryRenderer::introspect(
    bool unsafe_query) {
  if (unsafe_query) {
    return {};
  }

  std::vector<FormatArgumentPair> result;
  walkFormat(
      [&](auto data) { /* Do nothing with regular data */ },
      [&](auto data, const QueryArgument& arg) {
        DCHECK(!data.empty());
        DCHECK_EQ(data[0], '%');
        result.push_back(std::make_pair(
            data, std::reference_wrapper<const QueryArgument>(arg)));
      });

  return result;
}

folly::StringPiece MultiQuery::renderQuery(MYSQL* conn) {
  if (!unsafe_multi_query_.empty()) {
    return unsafe_multi_query_;
  }
  rendered_multi_query_ = Query::renderMultiQuery(conn, queries_);
  return folly::StringPiece(rendered_multi_query_);
}

} // namespace mysql_client
} // namespace common
} // namespace facebook
