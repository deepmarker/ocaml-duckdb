open Core
open Alcotest
open Duckdb

let basic _ctx =
  let sz = Lazy.force vector_size in
  check int "vector_size" 2048 sz
;;

let logical_type _ctx =
  let types =
    LogicalType.[ of_typ TINYINT; of_typ TIMESTAMP_NS; decimal ~width:10 ~scale:5 ]
  in
  List.iter types ~f:LogicalType.free
;;

let data_chunk _ctx =
  let typ = LogicalType.decimal ~width:10 ~scale:5 in
  let c = DataChunk.create [| typ; typ; typ; LogicalType.of_typ TIMESTAMP_NS |] in
  let sz = DataChunk.get_size c in
  check int "size" 0 sz;
  DataChunk.free c
;;

let generate_table con =
  let now = Time_ns.now () in
  query con "CREATE TABLE machin (ts TIMESTAMP_NS, px DECIMAL(10, 5), qty DECIMAL(10, 5))";
  let dec = LogicalType.decimal ~width:10 ~scale:5 in
  let chunk = DataChunk.create [| LogicalType.of_typ TIMESTAMP_NS; dec; dec |] in
  let vts = DataChunk.get_vector chunk 0 in
  let vpx = DataChunk.get_vector chunk 1 in
  let vqty = DataChunk.get_vector chunk 2 in
  let ats = Vector.data vts Bigarray.int in
  let apx = Vector.data vpx Bigarray.int in
  let aqty = Vector.data vqty Bigarray.int in
  Appender.with_appender con ~table:"machin" ~f:(fun a ->
    for i = 0 to Lazy.force vector_size - 1 do
      let ts =
        Time_ns.add now (Time_ns.Span.of_int_sec i) |> Time_ns.to_int_ns_since_epoch
      in
      Bigarray.Array1.set ats i ts;
      Bigarray.Array1.set apx i (Random.int 1_000_000);
      Bigarray.Array1.set aqty i (Random.int 1_000_000)
    done;
    DataChunk.set_size chunk (Lazy.force vector_size);
    Appender.append_chunk a chunk);
  query con "COPY machin TO '/tmp/machin.parquet' (FORMAT 'parquet', CODEC 'zstd')";
  ()
;;

let generate_table _ctx =
  with_db_connection generate_table;
  ()
;;

let basic =
  [ "basic", `Quick, basic
  ; "logical_type", `Quick, logical_type
  ; "data_chunk", `Quick, data_chunk
  ; "generate_table", `Quick, generate_table
  ]
;;

let () = run "duckdb" [ "basic", basic ]
