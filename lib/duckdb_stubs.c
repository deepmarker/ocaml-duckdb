#include <string.h>
#include <caml/mlvalues.h>
#include <caml/alloc.h>
#include <caml/memory.h>
#include <caml/custom.h>
#include <caml/fail.h>
#include <caml/bigarray.h>
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

static struct custom_operations appender_ops = {
  "duckdb.appender",
  custom_finalize_default,
  custom_compare_default,
  custom_hash_default,
  custom_serialize_default,
  custom_deserialize_default,
  custom_compare_ext_default,
  custom_fixed_length_default
};

static struct custom_operations logical_type_ops = {
  "duckdb.logical_type",
  custom_finalize_default,
  custom_compare_default,
  custom_hash_default,
  custom_serialize_default,
  custom_deserialize_default,
  custom_compare_ext_default,
  custom_fixed_length_default
};

static struct custom_operations data_chunk_ops = {
  "duckdb.data_chunk",
  custom_finalize_default,
  custom_compare_default,
  custom_hash_default,
  custom_serialize_default,
  custom_deserialize_default,
  custom_compare_ext_default,
  custom_fixed_length_default
};

static struct custom_operations vector_ops = {
  "duckdb.vector",
  custom_finalize_default,
  custom_compare_default,
  custom_hash_default,
  custom_serialize_default,
  custom_deserialize_default,
  custom_compare_ext_default,
  custom_fixed_length_default
};

static struct custom_operations result_ops = {
  "duckdb.result",
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
#define Appender_val(v) (((duckdb_appender *) Data_custom_val(v)))
#define Logical_type_val(v) (((duckdb_logical_type *) Data_custom_val(v)))
#define Data_chunk_val(v) (((duckdb_data_chunk *) Data_custom_val(v)))
#define Vector_val(v) (((duckdb_vector *) Data_custom_val(v)))
#define Result_val(v) (((duckdb_result *) Data_custom_val(v)))

CAMLprim value ml_duckdb_library_version(value unit) {
    CAMLparam1(unit);
    CAMLlocal1(x);
    x = caml_copy_string(duckdb_library_version());
    CAMLreturn(x);
}

CAMLprim value ml_duckdb_vector_size(value unit) {
    return Val_int(duckdb_vector_size());
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
        char *buf = malloc(strlen(err));
        strncpy(buf, err, strlen(err));
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

CAMLprim value ml_duckdb_query(value con, value query) {
    CAMLparam2(con, query);
    CAMLlocal1(result);
    result = caml_alloc_custom(&result_ops,
                               sizeof (duckdb_result),
                               0, 1);
    duckdb_state ret = duckdb_query(*Connection_val(con), String_val(query), Result_val(result));
    if (ret == DuckDBError)
        caml_failwith(duckdb_result_error(Result_val(result)));
    CAMLreturn(result);
}

/* result functions */

CAMLprim value ml_duckdb_rows_changed(value res) {
    return Val_int(duckdb_rows_changed(Result_val(res)));
}

CAMLprim value ml_duckdb_result_chunk_count(value res) {
    return Val_int(duckdb_result_chunk_count(*Result_val(res)));
}

CAMLprim value ml_duckdb_result_get_chunk(value res, value idx) {
    CAMLparam2(res, idx);
    CAMLlocal1(chunk);
    chunk = caml_alloc_custom(&data_chunk_ops,
                              sizeof (duckdb_data_chunk),
                              0, 1);
    *Data_chunk_val(chunk) = duckdb_result_get_chunk(*Result_val(res), Int_val(idx));
    if (*Data_chunk_val(chunk) == NULL) {
        caml_failwith("duckdb_result_get_chunk: index out of bounds");
    }
    CAMLreturn(chunk);
}

/* appender functions */

CAMLprim value ml_duckdb_appender_create (value con, value sch, value tbl) {
    CAMLparam2(con, tbl);
    CAMLlocal1(x);
    x = caml_alloc_custom(&appender_ops,
                          sizeof (duckdb_appender),
                          0, 1);
    if (duckdb_appender_create(*Connection_val(con),
                               caml_string_length(sch) > 0 ? String_val(sch) : NULL,
                               String_val(tbl),
                               Appender_val(x)) == DuckDBError) {
        const char *err = duckdb_appender_error(*Appender_val(x));
        caml_failwith(err);
    }
    CAMLreturn(x);
}

CAMLprim value ml_duckdb_appender_destroy(value appender) {
    CAMLparam1(appender);
    if (duckdb_appender_destroy(Appender_val(appender)) == DuckDBError) {
        const char *err = duckdb_appender_error(*Appender_val(appender));
        caml_failwith(err);
    }
    CAMLreturn(Val_unit);
}

CAMLprim value ml_duckdb_appender_error(value appender) {
    CAMLparam1(appender);
    CAMLlocal1(s);
    s = caml_copy_string(duckdb_appender_error(*Appender_val(appender)));
    CAMLreturn(s);
}

CAMLprim value ml_duckdb_appender_end_row(value appender) {
    return Val_int(duckdb_appender_end_row(*Appender_val(appender)));
}

CAMLprim value ml_duckdb_append_int8(value appender, value i8) {
    return Val_int(duckdb_append_int8(*Appender_val(appender), Int_val(i8)));
}

CAMLprim value ml_duckdb_append_int(value appender, value i) {
    return Val_int(duckdb_append_int64(*Appender_val(appender), Long_val(i)));
}

CAMLprim value ml_duckdb_append_int32(value appender, value i32) {
    return Val_int(duckdb_append_int32(*Appender_val(appender), Int32_val(i32)));
}

CAMLprim value ml_duckdb_append_int64(value appender, value i64) {
    return Val_int(duckdb_append_int64(*Appender_val(appender), Int64_val(i64)));
}

CAMLprim value ml_duckdb_append_timestamp(value appender, value i) {
    duckdb_timestamp ts = { Int_val(i) };
    return Val_int(duckdb_append_timestamp(*Appender_val(appender), ts));
}

CAMLprim value ml_duckdb_append_data_chunk(value appender, value chunk) {
    return Val_int(duckdb_append_data_chunk(*Appender_val(appender), *Data_chunk_val((chunk))));
}

/* Logical types interface */

CAMLprim value ml_duckdb_create_logical_type(value t) {
    CAMLparam1(t);
    CAMLlocal1(x);
    x = caml_alloc_custom(&logical_type_ops,
                          sizeof (duckdb_logical_type),
                          0, 1);
    *Logical_type_val(x) = duckdb_create_logical_type((duckdb_type)Int_val(t));
    CAMLreturn(x);
}

CAMLprim value ml_duckdb_create_decimal_type(value w, value s) {
    CAMLparam2(w, s);
    CAMLlocal1(x);
    x = caml_alloc_custom(&logical_type_ops,
                          sizeof (duckdb_logical_type),
                          0, 1);
    *Logical_type_val(x) = duckdb_create_decimal_type(Int_val(w), Int_val(s));
    CAMLreturn(x);
}

CAMLprim value ml_duckdb_create_array_type(value t, value len) {
    CAMLparam2(t, len);
    CAMLlocal1(x);
    x = caml_alloc_custom(&logical_type_ops,
                          sizeof (duckdb_logical_type),
                          0, 1);
    *Logical_type_val(x) = duckdb_create_array_type(*Logical_type_val(t), Int_val(len));
    CAMLreturn(x);
}

CAMLprim value ml_duckdb_destroy_logical_type(value t) {
    duckdb_destroy_logical_type(Logical_type_val(t));
    return Val_unit;
}

/* Data chunk interface */

CAMLprim value ml_duckdb_create_data_chunk(value types) {
    CAMLparam1(types);
    CAMLlocal1(x);
    x = caml_alloc_custom(&data_chunk_ops,
                          sizeof (duckdb_data_chunk),
                          0, 1);
    duckdb_logical_type ltypes[256];
    for (int i = 0; i < Wosize_val(types); i++) {
        ltypes[i] = *Logical_type_val(Field(types, i));
    }
    *Data_chunk_val(x) = duckdb_create_data_chunk(ltypes, Wosize_val(types));
    CAMLreturn(x);
}

CAMLprim value ml_duckdb_data_chunk_reset(value dc) {
    duckdb_data_chunk_reset(*Data_chunk_val(dc));
    return Val_unit;
}

CAMLprim value ml_duckdb_destroy_data_chunk(value dc) {
    duckdb_destroy_data_chunk(Data_chunk_val(dc));
    return Val_unit;
}

CAMLprim value ml_duckdb_data_chunk_get_vector(value dc, value idx) {
    CAMLparam2(dc, idx);
    CAMLlocal1(x);
    x = caml_alloc_custom(&vector_ops,
                          sizeof (duckdb_vector),
                          0, 1);
    // returns a null pointer if dc is null or index out of bounds
    *Vector_val(x) = duckdb_data_chunk_get_vector(*Data_chunk_val(dc), Int_val(idx));
    if (*Vector_val(x) == NULL)
        caml_failwith("get_vector: array index out of bounds");
    CAMLreturn(x);
}

CAMLprim value ml_duckdb_data_chunk_get_size(value dc) {
    return Val_int(duckdb_data_chunk_get_size(*Data_chunk_val(dc)));
}

CAMLprim value ml_duckdb_data_chunk_get_column_count(value dc) {
    return Val_int(duckdb_data_chunk_get_column_count(*Data_chunk_val(dc)));
}

CAMLprim value ml_duckdb_data_chunk_set_size(value dc, value len) {
    duckdb_data_chunk_set_size(*Data_chunk_val(dc), Int_val(len));
    return Val_unit;
}

/* Vector interface */

CAMLprim value ml_duckdb_vector_get_data(value vect, value typ) {
    CAMLparam2(vect, typ);
    CAMLlocal1(arr);
    void *data = duckdb_vector_get_data(*Vector_val(vect));
    arr = caml_ba_alloc_dims(Int_val(typ)|CAML_BA_C_LAYOUT, 1, data, duckdb_vector_size());
    CAMLreturn(arr);
}

CAMLprim value ml_duckdb_array_vector_get_child(value vect) {
    CAMLparam1(vect);
    CAMLlocal1(chld);
    chld = caml_alloc_custom(&vector_ops,
                             sizeof (duckdb_vector),
                             0, 1);
    *Vector_val(chld) = duckdb_array_vector_get_child(*Vector_val(vect));
    CAMLreturn(chld);
}

CAMLprim value ml_duckdb_vector_get_validity(value vect, value typ) {
    CAMLparam2(vect, typ);
    CAMLlocal1(arr);
    duckdb_vector_ensure_validity_writable(*Vector_val(vect));
    uint64_t *validity = duckdb_vector_get_validity(*Vector_val(vect));
    arr = caml_ba_alloc_dims(CAML_BA_INT64|CAML_BA_C_LAYOUT, 1, validity, duckdb_vector_size() / 8);
    CAMLreturn(arr);
}

