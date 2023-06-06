open Duckdb

let () =
  let cfg = [ "allow_unsigned_extensions", "TRUE" ] in
  let db = open_ext None cfg in
  let con = connect db in
  Format.kasprintf (exec con) "LOAD '%s'" Sys.argv.(1);
  disconnect con;
  close db;
  print_endline (version ())
;;
