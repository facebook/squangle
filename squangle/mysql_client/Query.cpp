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
  getPairs().emplace_back(ArgPair(param1.str(), param2));
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

bool QueryArgument::isBool() const {
  return value_.type() == typeid(bool);
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
  getPairs().emplace_back(ArgPair(q1.str(), q2));
  return std::move(*this);
}

QueryArgument&& QueryArgument::operator()(
    std::string&& q1,
    QueryArgument&& q2) {
  getPairs().emplace_back(ArgPair(std::move(q1), std::move(q2)));
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

bool QueryArgument::getBool() const {
  return boost::get<bool>(value_);
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
    value_ = param.asBool();
  } else if (param.isDouble()) {
    value_ = param.asDouble();
  } else if (param.isInt()) {
    value_ = param.asInt();
  } else {
    throw std::invalid_argument("Dynamic type doesn't match to accepted ones");
  }
}

std::vector<ArgPair>& QueryArgument::getPairs() {
  return boost::get<std::vector<ArgPair>>(value_);
}

Query::Query(
    const folly::StringPiece query_text,
    std::vector<QueryArgument> params)
    : query_text_(query_text), unsafe_query_(false), params_(params) {}

Query::~Query() {}

// Some helper functions for encoding/escaping.
void Query::QueryRenderer::appendComment(const QueryArgument& d) {
  // Note this used to use regex processing, but that can be expensive and
  // required three passes through the string
  //  auto str = d.asString();
  //  boost::replace_all(str, "/*", " / * ");
  //  boost::replace_all(str, "*/", " * / ");
  //  working_.append(str);

  char last = '\0';
  for (char c : d.getString()) {
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
    const QueryArgument& d) {
  static constexpr const char kBacktick = '`';
  DCHECK(d.isString());
  working_.reserve(working_.size() + d.getString().size() + 4);
  quote(kBacktick, [&]() {
    for (char c : d.getString()) {
      // Toss in an extra ` if we see one.
      if (c == kBacktick) {
        working_.push_back(kBacktick);
      }
      working_.push_back(c);
    }
  });
}

void Query::QueryRenderer::appendIdentifier(const QueryArgument& d) {
  if (d.isString()) {
    appendIdentifierWithBacktickEscaping(d);
  } else if (d.isTwoTuple()) {
    // If a two-tuple is provided we have a qualified column name
    const auto& t = d.getTwoTuple();
    appendIdentifierWithBacktickEscaping(std::get<0>(t));
    working_.push_back('.');
    appendIdentifierWithBacktickEscaping(std::get<1>(t));
  } else if (d.isThreeTuple()) {
    // If a three-tuple is provided we have a qualified column name
    // with an alias. This is helpful for constructing JOIN queries.
    const auto& t = d.getThreeTuple();
    appendIdentifierWithBacktickEscaping(std::get<0>(t));
    working_.push_back('.');
    appendIdentifierWithBacktickEscaping(std::get<1>(t));
    working_.append(" AS ");
    appendIdentifierWithBacktickEscaping(std::get<2>(t));
  } else {
    working_.append(d.asString());
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

// Consume the next x bytes from s, updating offset, and raising an
// exception if there aren't sufficient bytes left.
folly::StringPiece Query::QueryRenderer::advance(size_t num) {
  if (query_.size() <= offset_ + num) {
    parseError("unexpected end of string");
  }

  auto start = query_.data() + offset_ + 1;
  offset_ += num;
  return folly::StringPiece(start, start + num);
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
    auto value = d.asString();
    working_.reserve(working_.size() + value.size() + 4);
    quote('"', [&]() { appendEscapedString(value); });
  } else if (d.isBool()) {
    if (type != 'v' && type != 'm') {
      formatStringParseError(type, "bool");
    }
    working_.append(d.asString());
  } else if (d.isInt()) {
    if (type != 'd' && type != 'v' && type != 'm' && type != 'u') {
      formatStringParseError(type, "int");
    }
    if (type == 'u') {
      working_.append(
          folly::to<std::string>(static_cast<uint64_t>(d.getInt())));
    } else {
      working_.append(d.asString());
    }
  } else if (d.isDouble()) {
    if (type != 'f' && type != 'v' && type != 'm') {
      formatStringParseError(type, "double");
    }
    working_.append(d.asString());
  } else if (d.isQuery()) {
    working_.append(d.getQuery().render(mysql_));
  } else if (d.isNull()) {
    working_.append("NULL");
  } else {
    formatStringParseError(type, d.typeName());
  }
}

void Query::QueryRenderer::appendValues(const QueryArgument& d) {
  size_t row_len = 0;
  formatList(d.getList(), [&](const auto& row, size_t count) {
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

void Query::QueryRenderer::appendList(
    folly::StringPiece type,
    const QueryArgument& d) {
  formatList(d.getList(), [&](const auto& val, size_t /*count*/) {
    if (type == "C") {
      appendIdentifier(val);
    } else {
      appendValue(type[0], val);
    }
  });
}

void Query::QueryRenderer::appendValueClauses(
    const char* sep,
    const QueryArgument& param) {
  if (!param.isPairList()) {
    parseError(fmt::format(
        "object expected for %Lx but received {}", param.typeName()));
  }

  // Sort these to get consistent query ordering (mainly for
  // testing, but also aesthetics of the final query).
  formatList(
      param.getPairs(),
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

void Query::QueryRenderer::processEqualitySpec(const QueryArgument& param) {
  folly::StringPiece type = advance(1);
  if (type != "d" && type != "s" && type != "f" && type != "u" && type != "m") {
    parseError("expected %=d, %=f, %=s, %=u, or %=m");
  }

  if (param.isNull()) {
    working_.append(" IS NULL");
  } else {
    working_.append(" = ");
    appendValue(type[0], param);
  }
}

void Query::QueryRenderer::processListSpec(const QueryArgument& param) {
  folly::StringPiece type = advance(1);
  if (type == "O" || type == "A") {
    parenthesize(
        [&]() { appendValueClauses((type == "O") ? kOr : kAnd, param); });
  } else {
    if (!param.isList()) {
      parseError("expected array for %L formatter");
    }

    appendList(type, param);
  }
}

void Query::QueryRenderer::processFormatSpec(
    char c,
    const QueryArgument& param) {
  if (c == 'd' || c == 's' || c == 'f' || c == 'u') {
    appendValue(c, param);
  } else if (c == 'm') {
    if (!(param.isString() || param.isInt() || param.isDouble() ||
          param.isBool() || param.isNull())) {
      parseError("%m expects int/float/string/bool");
    }
    appendValue(c, param);
  } else if (c == 'K') {
    working_.append("/*");
    appendComment(param);
    working_.append("*/");
  } else if (c == 'T' || c == 'C') {
    appendIdentifier(param);
  } else if (c == '=') {
    processEqualitySpec(param);
  } else if (c == 'V') {
    if (param.isQuery()) {
      parseError("%V doesn't allow subquery");
    }

    appendValues(param);
  } else if (c == 'L') {
    processListSpec(param);
  } else if (c == 'U') {
    appendValueClauses(", ", param);
  } else if (c == 'W') {
    appendValueClauses(kAnd, param);
  } else if (c == 'Q') {
    if (param.isQuery()) {
      working_.append(param.getQuery().render(mysql_));
    } else {
      working_.append((param).asString());
    }
  } else {
    parseError("unknown % code");
  }
}

Query::QueryStringType Query::QueryRenderer::render(bool unsafe_query) {
  if (unsafe_query) {
    return query_.to<Query::QueryStringType>();
  }

  offset_ = query_.find_first_of(";'\"`");
  if (offset_ != folly::StringPiece::npos) {
    parseError("Saw dangerous characters in SQL query");
  }

  working_.reserve(query_.size() + 8 * args_.size());

  auto current_param = args_.begin();
  bool after_percent = false;
  // Walk our string, watching for % values.
  for (offset_ = 0; offset_ < query_.size(); ++offset_) {
    char c = query_[offset_];
    if (!after_percent) {
      if (c != '%') {
        working_.push_back(c);
      } else {
        after_percent = true;
      }
      continue;
    }

    after_percent = false;
    if (c == '%') {
      working_.push_back('%');
      continue;
    }

    if (current_param == args_.end()) {
      parseError("too few parameters for query");
    }

    processFormatSpec(c, *current_param++);
  }

  if (after_percent) {
    parseError("string ended with unfinished % code");
  }

  if (current_param != args_.end()) {
    parseError("too many parameters specified for query");
  }

  return working_;
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
