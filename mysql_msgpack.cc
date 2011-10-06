/*
 * Copyright (c) 2011 Ryuta Kamizono, All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *  1. Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *
 *  2. Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *
 *  3. Neither the name of the authors nor the names of its contributors
 *     may be used to endorse or promote products derived from this
 *     software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

extern "C" {
#include <mysql/mysql.h>
#include <string.h>
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
  try {
    msgpack::unpacked msg;
    msgpack::unpack(&msg, args->args[0], args->lengths[0]);
    msgpack::object obj = msg.get();

    for (unsigned i = 1; i < args->arg_count; ++i) {
      switch (args->arg_type[i]) {
      case INT_RESULT:
      {
        size_t idx = (size_t)*(long long*)args->args[i];
        if (obj.type == msgpack::type::ARRAY
          && idx < obj.via.array.size) {
          obj = obj.via.array.ptr[idx];
        } else {
          goto NOTFOUND;
        }
        break;
      }
      case STRING_RESULT:
      {
        if (obj.type == msgpack::type::MAP
          && obj.via.map.size != 0) {
          msgpack::object_kv* p(obj.via.map.ptr);
          msgpack::object_kv* const pend(obj.via.map.ptr + obj.via.map.size);
          for (; p < pend; ++p) {
            if (p->key.type == msgpack::type::RAW
              && p->key.via.raw.size == args->lengths[i]
              && strncmp(p->key.via.raw.ptr, args->args[i], args->lengths[i]) == 0) {
              obj = p->val;
              break;
            }
          }
          if (p == pend) {
            goto NOTFOUND;
          }
        } else {
          goto NOTFOUND;
        }
        break;
      }
      default:
      NOTFOUND:
        *length  = 0;
        *is_null = 1;
        return NULL;
      }
    }

    std::string* out = (std::string*)(void*)initid->ptr;
    std::ostringstream ss;

    switch (obj.type) {
    case msgpack::type::NIL:
      *length  = 0;
      *is_null = 1;
      return NULL;
    case msgpack::type::BOOLEAN:
      ss << (obj.via.boolean ? 1 : 0);
      break;
    case msgpack::type::RAW:
      ss.write(obj.via.raw.ptr, obj.via.raw.size);
      break;
    default:
      ss << obj;
    }

    *out = ss.str();
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
