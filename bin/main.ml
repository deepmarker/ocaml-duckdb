open Core
open Duckdb

let src = Logs.Src.create "duckdb-test"

module Lo = (val Logs.src_log src : Logs.LOG)

let scrooge path =
  let cfg = [ "allow_unsigned_extensions", "TRUE" ] in
  let db = open_ ~cfg () in
  let con = connect db in
  (* Format.kasprintf (exec con) "LOAD '%s'" Sys.argv.(1); *)
  let _ =
    queryf
      con
      []
      {| CREATE OR REPLACE TABLE trade AS SELECT * FROM read_parquet('%s')|}
      path
  in
  let _ =
    query
      con
      {|
SELECT TIMEBUCKET(lts,'10S'::INTERVAL) ts,
       FIRST_S(px, lts) o,
       MAX(px) h,
       MIN(px) l,
       LAST_S(px, lts) C,
       SUM(qty) bv,
       SUM(qty) av,
  FROM trade
 WHERE FLAG = 'e'
 GROUP BY ts
|}
      []
  in
  disconnect con;
  close db;
  print_endline (Lazy.force version)
;;

let scrooge_cmd =
  Command.basic
    ~summary:"Test ScroogeMcDuck"
    (let open Command.Let_syntax in
     [%map_open
       let () = Logs_async_reporter.set_level_via_param []
       and () = Logs_async_reporter.set_color_via_param ()
       and path = anon ("path" %: string) in
       fun () ->
         Logs.set_reporter (Logs_async_reporter.reporter ());
         scrooge path])
;;

let memory_usage_test ?(flush = Int.max_value) cfg path n =
  let module Array = Bigarray.Array1 in
  let db = open_ ~cfg ~path () in
  let con = connect db in
  let _res =
    query
      con
      "CREATE OR REPLACE TABLE data (ts TIMESTAMPTZ NOT NULL, book DECIMAL(18,5)[100] \
       NOT NULL)"
      Prod.[]
  in
  let open Bigarray in
  let prd =
    Prod.
      [ Type.scalar Repr.time int
      ; Type.array (Repr.decimal ~precision:18 ~scale:5) int 100
      ]
  in
  let vect_size = Lazy.force vector_size in
  let chunk = DataChunk.create prd in
  DataChunk.set_length chunk vect_size;
  let tss, rest = DataChunk.hd chunk in
  let a, _rest = DataChunk.hd rest in
  let arr_len = 100 in
  let app = Appender.create con ~table:"data" prd in
  for j = 0 to n - 1 do
    let tss = Vector.data tss in
    let data = Vector.data a in
    for i = 0 to vect_size - 1 do
      tss.(i) <- 1_000_000 * ((vect_size * j) + i);
      for k = 0 to arr_len - 1 do
        data.((i * arr_len) + k) <- Random.int 10_000_000_000
      done
    done;
    DataChunk.set_length chunk vect_size;
    Appender.append_chunk app chunk;
    if j mod flush = 0 then Appender.flush app;
    DataChunk.reset chunk
  done;
  DataChunk.free chunk;
  Appender.free app;
  disconnect con;
  close db
;;

let memory_usage_cmd =
  Command.basic
    ~summary:"Test Memory usage"
    (let open Command.Let_syntax in
     [%map_open
       let () = Logs_async_reporter.set_level_via_param []
       and () = Logs_async_reporter.set_color_via_param ()
       and cfg = flag "cfg" (listed string) ~doc:"CFG Pass cfg to DuckDB"
       and flush = flag "flush" (optional int) ~doc:"INT Flush every N chunks"
       and path = anon ("path" %: string)
       and n = anon ("n" %: int) in
       fun () ->
         Logs.set_reporter (Logs_async_reporter.reporter ());
         let cfg = List.map cfg ~f:(fun x -> String.lsplit2_exn ~on:'=' x) in
         memory_usage_test ?flush cfg (Fpath.v path) n])
;;

let cmd =
  Command.group
    ~summary:"test duckdb binding"
    [ "scrooge", scrooge_cmd; "memory", memory_usage_cmd ]
;;

let () = Command_unix.run cmd
