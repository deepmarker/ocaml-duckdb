open Core
open Alcotest
open Duckdb

let basic () =
  let sz = Lazy.force vector_size in
  check int "vector_size" 2048 sz
;;

let logical_type () =
  let types =
    LogicalType.[ scalar TINYINT; scalar TIMESTAMP_NS; decimal ~width:10 ~scale:5 ]
  in
  List.iter types ~f:LogicalType.free
;;

let data_chunk () =
  let typ = LogicalType.decimal ~width:10 ~scale:5 in
  let c = DataChunk.create [| typ; typ; typ; LogicalType.scalar TIMESTAMP_NS |] in
  let sz = DataChunk.length c in
  check int "size" 0 sz;
  DataChunk.free c
;;

let generate_table con =
  let now = Time_ns.now () in
  let _ =
    query
      con
      "CREATE TABLE machin (ts TIMESTAMP_NS, px DECIMAL(10, 5), qty DECIMAL(10, 5))"
  in
  let dec = LogicalType.decimal ~width:10 ~scale:5 in
  let chunk = DataChunk.create [| LogicalType.scalar TIMESTAMP_NS; dec; dec |] in
  DataChunk.set_length chunk (Lazy.force vector_size);
  let vts = DataChunk.vector chunk 0 (Type.scalar (module Repr.Time_ns) Bigarray.int) in
  let vpx =
    DataChunk.vector chunk 1 (Type.scalar (module Repr.Bigdecimal) Bigarray.int)
  in
  let vqty =
    DataChunk.vector chunk 2 (Type.scalar (module Repr.Bigdecimal) Bigarray.int)
  in
  let ats = Vector.data vts in
  let apx = Vector.data vpx in
  let aqty = Vector.data vqty in
  Appender.with_appender con ~table:"machin" ~f:(fun a ->
    for i = 0 to Lazy.force vector_size - 1 do
      let ts =
        Time_ns.add now (Time_ns.Span.of_int_sec i) |> Time_ns.to_int_ns_since_epoch
      in
      Bigarray.Array1.set ats i ts;
      Bigarray.Array1.set apx i (Random.int 1_000_000);
      Bigarray.Array1.set aqty i (Random.int 1_000_000)
    done;
    Appender.append_chunk a chunk);
  let _ =
    query con "COPY machin TO '/tmp/machin.parquet' (FORMAT 'parquet', CODEC 'zstd')"
  in
  ()
;;

let generate_table () =
  with_db_connection generate_table;
  ()
;;

let query () =
  with_db_connection (fun con ->
    let res = query con "select 1+1" in
    let len = QResult.chunk_count res in
    check int "rows_changed" 0 (QResult.rows_changed res);
    let nb_rows = ref 0 in
    for i = 0 to len - 1 do
      let chunk = QResult.get_chunk res i in
      check int "col_count" 1 (DataChunk.get_col_count chunk);
      nb_rows := !nb_rows + DataChunk.length chunk
    done;
    check int "row_count" 1 !nb_rows)
;;

let array () =
  let open Duckdb in
  with_db_connection (fun con ->
    let res = query con "SELECT array_value(1, 2, 3)" in
    let len = QResult.chunk_count res in
    check int "chunk count" 1 len;
    for i = 0 to len - 1 do
      let chunk = QResult.get_chunk res i in
      check int "length" 1 (DataChunk.length chunk);
      check int "col_count" 1 (DataChunk.get_col_count chunk);
      let typ = Type.array (module Repr.Int32) Bigarray.int32 3 in
      let vect = DataChunk.vector chunk 0 typ in
      let ba = Vector.data vect in
      check bool "valid" true (Vector.valid vect 0);
      check bool "valid" true (Vector.valid vect 1);
      check bool "valid" true (Vector.valid vect 2);
      check int "dim" 3 (Bigarray.Array1.dim ba);
      check int32 "" 1l (Bigarray.Array1.get ba 0);
      check int32 "" 2l (Bigarray.Array1.get ba 1);
      check int32 "" 3l (Bigarray.Array1.get ba 2);
      ()
    done)
;;

let create_array_table () =
  let open Duckdb in
  let array3 = Type.array (module Repr.Int32) Bigarray.int32 3 in
  with_db_connection (fun con ->
    let _ = query con "CREATE TABLE a1 (x integer[3])" in
    let chunk = DataChunk.create LogicalType.[| array (scalar INTEGER) 3 |] in
    DataChunk.set_length chunk 3;
    let v = DataChunk.vector chunk 0 array3 in
    let d = Vector.data v in
    Appender.with_appender con ~table:"a1" ~f:(fun a ->
      for i = 0 to 2 do
        for j = 0 to 2 do
          Bigarray.Array1.set d ((3 * i) + j) (Int32.of_int_exn ((3 * i) + j))
        done
      done;
      DataChunk.set_length chunk 3;
      Appender.append_chunk a chunk);
    let res = query con "SELECT * FROM a1" in
    let _ = query con "COPY a1 TO '/tmp/a1.parquet' (FORMAT 'parquet', CODEC 'zstd')" in
    let chunk = QResult.get_chunk res 0 in
    check int "length" 3 (DataChunk.length chunk);
    check int "col_count" 1 (DataChunk.get_col_count chunk);
    let vect = DataChunk.vector chunk 0 array3 in
    let ba = Vector.data vect in
    check int "dim" 9 (Bigarray.Array1.dim ba);
    for i = 0 to 8 do
      check int32 "" (Int32.of_int_exn i) (Bigarray.Array1.get ba i)
    done)
;;

let basic =
  [ "basic", `Quick, basic
  ; "logical_type", `Quick, logical_type
  ; "data_chunk", `Quick, data_chunk
  ; "generate_table", `Quick, generate_table
  ; "query", `Quick, query
  ; "array", `Quick, array
  ; "create_array_table", `Quick, create_array_table
  ]
;;

let () = run "duckdb" [ "basic", basic ]
