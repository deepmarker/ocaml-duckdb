open Duckdb

let () =
  let cfg = [ "allow_unsigned_extensions", "TRUE" ] in
  let db = open_ext None cfg in
  let con = connect db in
  (* Format.kasprintf (exec con) "LOAD '%s'" Sys.argv.(1); *)
  exec
    con
    {|
CREATE OR REPLACE TABLE trade AS
SELECT * FROM read_parquet('~/code/dm/ocaml/2022-12-29_03-09-33.634617/ETHUSD-PERP.trades.parquet')
|};
  exec
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
|};
  disconnect con;
  close db;
  print_endline (version ())
;;
