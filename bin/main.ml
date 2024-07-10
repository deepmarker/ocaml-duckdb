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

let cmd = Command.group ~summary:"test duckdb binding" [ "scrooge", scrooge_cmd ]
let () = Command_unix.run cmd
