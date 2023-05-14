#include <mysql/mysql.h>
#include <iostream>
#include <iomanip>
#include <string>
#include <vector>
#include <cassert>
#include <chrono>

MYSQL *init_mysql_init_and_connect()
{
  MYSQL *mysql = mysql_init(nullptr);

  if (mysql) {
    if (!mysql_real_connect(mysql, "localhost", "stress", "g00db0y", 
        "stress", 0, nullptr, CLIENT_MULTI_STATEMENTS | CLIENT_MULTI_RESULTS))
      return nullptr;
  }
  
  return mysql;
}

struct Customer {
  std::vector<char> email;
  std::vector<char> lname;
  std::vector<char> fname;
  std::vector<char> addr;
  std::vector<char> city;
  std::vector<char> prov;
  std::vector<char> postal;
};

std::vector<char> randomString(size_t len)
{
  std::vector<char> result;
  unsigned seed = (unsigned)time(nullptr);
  result.reserve(len);
  for (size_t i = 0; i < len; ++i)
    result.push_back('A' + (rand_r(&seed) * 26L) / RAND_MAX);
  return result;
}

Customer randomCustomer()
{
  return {
    randomString(12),
    randomString(12),
    randomString(12),
    randomString(12),
    randomString(12),
    randomString(12),
    randomString(12)
  };
}

std::pair<char const *, std::vector<char> (Customer::*)> customerFields[] = {
  { "email", &Customer::email },
  { "lname", &Customer::lname },
  { "fname", &Customer::fname },
  { "addr", &Customer::addr },
  { "city", &Customer::city },
  { "prov", &Customer::prov },
  { "postal", &Customer::postal }
};
static constexpr size_t customerFieldCount = 
  sizeof(customerFields) / sizeof(*customerFields);

// -1 is failure, 0 means limit is excessive, 1 means success
int insertStress(MYSQL *conn, size_t limit)
{
  std::string sql;

  sql += "INSERT INTO `customer` (";
  for (size_t i = 0; i < customerFieldCount; ++i) {
    sql += '`';
    sql += customerFields[i].first;
    sql += '`';
    sql += ',';
  }
  sql.back() = ')';

  sql += " VALUES ";

  size_t st_sz = sql.size();
  size_t iter = 65535 / customerFieldCount;
  if (limit * 2 > iter)
    return 0;
  if (iter > limit)
    iter = limit;
  
  for (size_t i = 0; i < iter; ++i) {
    sql += "(";    

    for (auto const& field : customerFields) {
      char const *name = field.first;
      //std::vector<char> (Customer::*fieldPtr) = field.second;
      
      sql += name;
      sql += "=?,";
    }

    // Replace unwanted last comma with closing parenthesis
    sql.back() = ')';

    sql += ',';

    if (i == 0) {
      size_t en_sz = sql.size();
      sql.reserve(st_sz + ((en_sz - st_sz) * iter) + 1);
    }
  }

  // Remove unwanted last comma
  sql.back() = ';';
  
  MYSQL_STMT *stmt = mysql_stmt_init(conn);

  //std::cout << "Statements: " << sql << '\n';

  if (mysql_stmt_prepare(stmt, sql.c_str(), sql.size()) != 0) {
    char const *errMsg = mysql_error(conn);
    std::cerr << errMsg << '\n';
    return -1;
  }

  // unsigned long type = CURSOR_TYPE_READ_ONLY;
  // if (!mysql_stmt_attr_set(stmt, STMT_ATTR_CURSOR_TYPE, (void*) &type))
  //   return 1;
  
  // unsigned long prefetch_rows = 16;
  // if (!mysql_stmt_attr_set(stmt, STMT_ATTR_PREFETCH_ROWS, &prefetch_rows))
  //   return 1;

  std::vector<MYSQL_BIND> binds;
  binds.resize(customerFieldCount * iter);
  std::vector<Customer> customers(iter);

  std::chrono::steady_clock::time_point st = 
      std::chrono::steady_clock::now();
  size_t index = 0;
  for (size_t i = 0; i < iter; ++i) {
    customers[i] = randomCustomer();
    
    for (auto const& field : customerFields) {
      //char const *name = field.first;
      std::vector<char> (Customer::*fieldPtr) = field.second;
      MYSQL_BIND &bind = binds.at(index);
      bind.param_number = index;
      bind.buffer_type = MYSQL_TYPE_STRING;
      std::vector<char> &fieldSource = customers[i].*fieldPtr;
      bind.buffer = fieldSource.data();
      bind.buffer_length = fieldSource.size();
      bind.length_value = bind.buffer_length;
      ++index;
    }
  }

  size_t good_binds = 0;
  size_t bad_binds = 0;

  int err = mysql_stmt_bind_param(stmt, binds.data());

  if (!err)
    ++good_binds;
  else
    ++bad_binds;

  err = mysql_stmt_execute(stmt);

  int affected = mysql_stmt_affected_rows(stmt);

  assert(affected == iter);

  std::chrono::steady_clock::time_point en = 
      std::chrono::steady_clock::now();
  
  uint64_t ns = std::chrono::duration_cast<
      std::chrono::nanoseconds>(en - st).count();
  
  std::cout << '"' << iter << "\",\"" << (ns/1e+9) << "\"\n";

  return 1;
}

int main()
{
  int err = mysql_library_init(0, nullptr, nullptr);
  if (err)
    return err;
  
  std::cout << "\"batch\",\"secs\"\n";
  
  MYSQL *mysql = init_mysql_init_and_connect();

  mysql_query(mysql, "delete from customer where 1;");

  if (mysql) {
    int status;
    for (size_t limit = 1; 
        (status = insertStress(mysql, limit)) > 0 ; limit <<= 1) {
      // empty loop body
    }
    if (status < 0)
      std::cerr << "Failed\n";
  }
  
  mysql_library_end();

  return 0;
}
