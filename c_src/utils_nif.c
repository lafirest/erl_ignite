#include "erl_nif.h"
#include <ctype.h>
#include <stdio.h>

/** FNV1 hash offset basis. */
enum { FNV1_OFFSET_BASIS = 0x811C9DC5 };

/** FNV1 hash prime. */
enum { FNV1_PRIME = 0x01000193 };

enum { MAX_FIELD_NAME_LEN = 256 };

static ERL_NIF_TERM
data_hash(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
  int32_t hash = 0;
  ErlNifBinary binary;

  if(argc != 2 || !enif_inspect_binary(env, argv[0], &binary) || !enif_get_int(env, argv[1], &hash))
    return enif_make_badarg(env);

  for(int i = 0; i < binary.size; ++i)
    {
      signed char byte = binary.data[i];
      hash = 31 * hash + byte;
    }

  return enif_make_int(env, hash);
}

static ERL_NIF_TERM
lower_hash(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
  int32_t hash = 0;
  ErlNifBinary binary;

  if(argc != 1 || !enif_inspect_binary(env, argv[0], &binary))
    return enif_make_badarg(env);

  for(int i = 0; i < binary.size; ++i)
    {
      signed char byte = binary.data[i];
      hash = 31 * hash + tolower(byte);
    }

  return enif_make_int(env, hash);
}

static ERL_NIF_TERM
calculate_schema_id(ErlNifEnv * env, int argc, const ERL_NIF_TERM argv[])
{
  unsigned int len;
  char buffer[MAX_FIELD_NAME_LEN];  
  ERL_NIF_TERM head;
  ERL_NIF_TERM tail;
  ERL_NIF_TERM list = argv[0];
  int32_t schema_id = FNV1_OFFSET_BASIS;
  
  if(argc != 1 || !enif_is_list(env, list))
    return enif_make_badarg(env);

  enif_get_list_length(env, list, &len);
  if(len == 0)
    return enif_make_int(env, 0);

  for(int i = 0; i < len; ++i)
    {
      unsigned int str_len = 0;
      int32_t field_id = 0;
      enif_get_list_cell(env, list, &head, &tail);
      
      if(!enif_get_list_length(env, head, &str_len) || str_len > MAX_FIELD_NAME_LEN)
        return enif_make_badarg(env);
      
      enif_get_string(env, head, buffer, MAX_FIELD_NAME_LEN, ERL_NIF_LATIN1);

      for(int j = 0; j < str_len; ++j)
        {
          signed char byte = buffer[j];
          field_id = 31 * field_id + tolower(byte);
        }
       
      schema_id ^= field_id & 0xFF;
      schema_id *= FNV1_PRIME;
      schema_id ^= (field_id >> 8) & 0xFF;
      schema_id *= FNV1_PRIME;
      schema_id ^= (field_id >> 16) & 0xFF;
      schema_id *= FNV1_PRIME;
      schema_id ^= (field_id >> 24) & 0xFF;
      schema_id *= FNV1_PRIME;

      list = tail;
    }

  return enif_make_int(env, schema_id);
}

static ErlNifFunc nif_funcs[] =
  {
    {"data_hash", 2, data_hash},
    {"lower_hash", 1, lower_hash},
    {"calculate_schemaId", 1, calculate_schema_id}
  };

ERL_NIF_INIT(utils, nif_funcs, NULL, NULL, NULL, NULL);
