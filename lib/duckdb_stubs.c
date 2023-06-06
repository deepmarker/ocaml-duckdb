#include <string.h>
#include <caml/mlvalues.h>
#include <caml/alloc.h>
#include <caml/memory.h>
#include <caml/custom.h>
#include <caml/fail.h>
#include <duckdb.h>

static struct custom_operations database_ops = {
  "duckdb.database",
  custom_finalize_default,
  custom_compare_default,
  custom_hash_default,
  custom_serialize_default,
  custom_deserialize_default,
  custom_compare_ext_default,
  custom_fixed_length_default
};

static struct custom_operations connection_ops = {
  "duckdb.connection",
  custom_finalize_default,
  custom_compare_default,
  custom_hash_default,
  custom_serialize_default,
  custom_deserialize_default,
  custom_compare_ext_default,
  custom_fixed_length_default
};

#define Database_val(v) (((duckdb_database *) Data_custom_val(v)))
#define Connection_val(v) (((duckdb_connection *) Data_custom_val(v)))

CAMLprim value ml_duckdb_library_version(value unit) {
    CAMLparam1(unit);
    CAMLlocal1(x);
    x = caml_copy_string(duckdb_library_version());
    CAMLreturn(x);
}

CAMLprim value ml_duckdb_open_ext (value path, value cfg, value len) {
    CAMLparam2(path, cfg);
    CAMLlocal1(x);
    x = caml_alloc_custom(&database_ops,
                          sizeof (duckdb_database),
                          0, 1);
    duckdb_config config;
    if (duckdb_create_config(&config) == DuckDBError)
        caml_failwith("cannot create config");
    char *err;
    for (int i = 0; i<2*Int_val(len); i+=2) {
        duckdb_set_config(config, String_val(Field(cfg, i)), String_val(Field(cfg, i+1)));
    }
    duckdb_state ret =
        Is_some(path) ?
        duckdb_open_ext(String_val(Some_val(path)), Database_val(x), config, &err) :
        duckdb_open_ext(NULL, Database_val(x), config, &err);
    duckdb_destroy_config(&config);
    if (ret == DuckDBError) {
        char *buf = malloc(4096);
        strncpy(buf, err, 4096);
        duckdb_free(err);
        caml_failwith(buf);
    }
    CAMLreturn(x);
}

CAMLprim value ml_duckdb_connect(value db) {
    CAMLparam1(db);
    CAMLlocal1(x);
    x = caml_alloc_custom(&connection_ops,
                          sizeof (duckdb_connection),
                          0, 1);
    duckdb_state ret = duckdb_connect(*Database_val(db), Connection_val(x));
    if (ret == DuckDBError)
        caml_failwith("cannot create connection");

    CAMLreturn(x);
}

CAMLprim value ml_duckdb_disconnect(value con) {
    duckdb_disconnect(Connection_val(con));
    return Val_unit;
}

CAMLprim value ml_duckdb_close(value db) {
    duckdb_close(Database_val(db));
    return Val_unit;
}

CAMLprim value ml_duckdb_exec(value con, value query) {
    CAMLparam2(con, query);
    duckdb_result res;
    duckdb_state ret = duckdb_query(*Connection_val(con), String_val(query), &res);
    if (ret == DuckDBError)
        caml_failwith(duckdb_result_error(&res));
    CAMLreturn(Val_unit);
}
