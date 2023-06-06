open StdLabels

type db
type con

external version : unit -> string = "ml_duckdb_library_version"
external open_ext : string option -> string array -> int -> db = "ml_duckdb_open_ext"
external connect : db -> con = "ml_duckdb_connect"
external disconnect : con -> unit = "ml_duckdb_disconnect" [@@noalloc]
external close : db -> unit = "ml_duckdb_close" [@@noalloc]
external exec : con -> string -> unit = "ml_duckdb_exec"

let open_ext path cfg =
  let len = List.length cfg in
  let a = Array.make (2 * len) "" in
  List.iteri cfg ~f:(fun i (k, v) ->
    a.(i * 2) <- k;
    a.((i * 2) + 1) <- v);
  open_ext path a len
;;
