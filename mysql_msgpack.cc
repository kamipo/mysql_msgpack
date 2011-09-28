extern "C" {
#include <mysql/mysql.h>
}
#include <string>
#include <sstream>
#include <msgpack.hpp>

extern "C" {
my_bool msgpack_get_init(UDF_INIT* initid, UDF_ARGS* args, char* message);
void msgpack_get_deinit(UDF_INIT* initid);
char* msgpack_get(UDF_INIT* initid, UDF_ARGS* args, char* result, unsigned long* length, char* is_null, char* error);
}

my_bool msgpack_get_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
  if (args->arg_count < 1) {
    strcpy(message, "msgpack_get: too few arguments");
    return 1;
  }
  if (args->arg_type[0] != STRING_RESULT) {
    strcpy(message, "msgpack_get: 1st argument should be a string");
    return 1;
  }
  // assert (or convert) succeeding arguments to either int or string
  for (unsigned i = 1; i < args->arg_count; ++i) {
    switch (args->arg_type[i]) {
    case INT_RESULT:
    case STRING_RESULT:
      break;
    default:
      args->arg_type[i] = STRING_RESULT;
      break;
    }
  }
  initid->ptr = (char*)(void*)new std::string();
  initid->const_item = 1;
  return 0;
}

void msgpack_get_deinit(UDF_INIT* initid)
{
  delete (std::string*)(void*)initid->ptr;
}

char* msgpack_get(UDF_INIT* initid, UDF_ARGS* args, char* result, unsigned long* length, char* is_null, char* error)
{
  std::string* out = (std::string*)(void*)initid->ptr;

  try {
    msgpack::unpacked msg;
    msgpack::unpack(&msg, args->args[0], args->lengths[0]);
    msgpack::object obj = msg.get();
    {
      std::ostringstream ss;
      ss << obj;
      *out = ss.str();
    }
    *length = out->size();
    return &(*out)[0];
  } catch (msgpack::unpack_error& e) {
    fprintf(stderr, "msgpack_get: unpack failed: %s\n", e.what());
    *error = 1;
    return NULL;
  } catch (msgpack::type_error&) {
    fprintf(stderr, "msgpack_get: type error\n");
    *error = 1;
    return NULL;
  }
}
