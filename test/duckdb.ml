open Core
open Alcotest
open Duckdb

let basic () =
  let sz = Lazy.force vector_size in
  check int "vector_size" 2048 sz
;;

let data_chunk () =
  let x = Repr.decimal ~precision:18 ~scale:5 in
  let c =
    DataChunk.create
      Prod.[ Type.scalar x Bigarray.int; Type.scalar Repr.time Bigarray.int ]
  in
  let sz = DataChunk.length c in
  check int "size" 0 sz;
  DataChunk.free c
;;

let generate_table con =
  let now = Time_ns.now () in
  let _ =
    query
      con
      "CREATE OR REPLACE TABLE machin (ts TIMESTAMPTZ, px DECIMAL(10, 5), qty \
       DECIMAL(10, 5))"
      []
  in
  let x = Repr.decimal ~precision:10 ~scale:5 in
  let chunk =
    DataChunk.create
      [ Type.scalar Repr.time Bigarray.int
      ; Type.scalar x Bigarray.int
      ; Type.scalar x Bigarray.int
      ]
  in
  DataChunk.set_length chunk (Lazy.force vector_size);
  let v1, rest = DataChunk.hd chunk in
  let v2, rest = DataChunk.hd rest in
  let v3, _rest = DataChunk.hd rest in
  let len = Lazy.force vector_size in
  let d1 = Vector.data v1 in
  let d2 = Vector.data v2 in
  let d3 = Vector.data v3 in
  Appender.with_appender
    con
    ~table:"machin"
    [ Type.scalar Repr.time Bigarray.int
    ; Type.scalar x Bigarray.int
    ; Type.scalar x Bigarray.int
    ]
    ~f:(fun a ->
      for i = 0 to len - 1 do
        let ts = Time_ns.add now (Time_ns.Span.of_int_sec i) in
        Vector.set v1 d1 i ts;
        Bigarray.Array1.set d2 i (Random.int 1_000_000);
        Bigarray.Array1.set d3 i (Random.int 1_000_000)
      done;
      Appender.append_chunk a chunk);
  let _ =
    query con "COPY machin TO '/tmp/machin.parquet' (FORMAT 'parquet', CODEC 'zstd')" []
  in
  ()
;;

let generate_table () =
  let path = Fpath.v "/tmp/machin.db" in
  with_db_connection ~path generate_table;
  ()
;;

let query () =
  with_db_connection (fun con ->
    let res = query con "select 1+1" [ Type.scalar Repr.int Bigarray.int ] in
    let len = QResult.chunk_count res in
    check int "rows_changed" 0 (QResult.rows_changed res);
    let nb_rows = ref 0 in
    for i = 0 to len - 1 do
      let chunk = QResult.get_chunk res i in
      (* check int "col_count" 1 (DataChunk.get_col_count chunk); *)
      nb_rows := !nb_rows + DataChunk.length chunk;
      ()
    done;
    (* Bool *)
    let res = query con "select true" [ Type.scalar Repr.bool Bigarray.int8_unsigned ] in
    let len = QResult.chunk_count res in
    check int "rows_changed" 0 (QResult.rows_changed res);
    let nb_rows = ref 0 in
    for i = 0 to len - 1 do
      let chunk = QResult.get_chunk res i in
      let v, _rest = DataChunk.hd chunk in
      let a = Vector.data v in
      check bool "" true (Vector.get v a 0);
      (* check int "col_count" 1 (DataChunk.get_col_count chunk); *)
      nb_rows := !nb_rows + DataChunk.length chunk;
      ()
    done;
    check int "row_count" 1 !nb_rows;
    ())
;;

let array () =
  let open Duckdb in
  with_db_connection (fun con ->
    let res =
      query con "SELECT array_value(1, 2, 3)" [ Type.array Repr.int32 Bigarray.int32 3 ]
    in
    let len = QResult.chunk_count res in
    check int "chunk count" 1 len;
    for i = 0 to len - 1 do
      let chunk = QResult.get_chunk res i in
      check int "length" 1 (DataChunk.length chunk);
      (* check int "col_count" 1 (DataChunk.get_col_count chunk); *)
      let vect, _rest = DataChunk.hd chunk in
      let ba = Vector.data vect in
      check bool "valid" true (Vector.valid vect 0);
      check bool "valid" true (Vector.valid vect 1);
      check bool "valid" true (Vector.valid vect 2);
      check int "dim" (3 * 2048) (Bigarray.Array1.dim ba);
      check int32 "" 1l (Bigarray.Array1.get ba 0);
      check int32 "" 2l (Bigarray.Array1.get ba 1);
      check int32 "" 3l (Bigarray.Array1.get ba 2);
      ()
    done)
;;

let create_array_table () =
  let open Duckdb in
  let array3 = Type.array Repr.int32 Bigarray.int32 3 in
  with_db_connection (fun con ->
    let _ = query con "CREATE TABLE a1 (x integer[3])" [] in
    let chunk = DataChunk.create [ array3 ] in
    DataChunk.set_length chunk 3;
    let v, _rest = DataChunk.hd chunk in
    let d = Vector.data v in
    Appender.with_appender con ~table:"a1" [ array3 ] ~f:(fun a ->
      for i = 0 to 2 do
        for j = 0 to 2 do
          Bigarray.Array1.set d ((3 * i) + j) (Int32.of_int_exn ((3 * i) + j))
        done
      done;
      DataChunk.set_length chunk 3;
      Appender.append_chunk a chunk);
    let res = query con "SELECT * FROM a1" [ array3 ] in
    let _ =
      query con "COPY a1 TO '/tmp/a1.parquet' (FORMAT 'parquet', CODEC 'zstd')" []
    in
    let chunk = QResult.get_chunk res 0 in
    check int "length" 3 (DataChunk.length chunk);
    (* check int "col_count" 1 (DataChunk.get_col_count chunk); *)
    let vect, _rest = DataChunk.hd chunk in
    let ba = Vector.data vect in
    check int "dim" (3 * 2048) (Bigarray.Array1.dim ba);
    for i = 0 to 8 do
      check int32 "" (Int32.of_int_exn i) (Bigarray.Array1.get ba i)
    done)
;;

let basic =
  [ "basic", `Quick, basic
  ; "data_chunk", `Quick, data_chunk
  ; "generate_table", `Quick, generate_table
  ; "query", `Quick, query
  ; "array", `Quick, array
  ; "create_array_table", `Quick, create_array_table
  ]
;;

let () =
  run
    (* ~filter:(fun ~name:_ ~index -> *)
    (*   match index with *)
    (*   | 0 | 1 | 2 | 3 | 4 | 5 -> `Run *)
    (*   | _ -> `Skip) *)
    "duckdb"
    [ "basic", basic ]
;;
