open StdLabels

type db
type con

external version : unit -> string = "ml_duckdb_library_version"
external open_ext : string option -> string array -> int -> db = "ml_duckdb_open_ext"
external connect : db -> con = "ml_duckdb_connect"
external disconnect : con -> unit = "ml_duckdb_disconnect" [@@noalloc]
external close : db -> unit = "ml_duckdb_close" [@@noalloc]
external exec : con -> string -> unit = "ml_duckdb_exec"

let open_ ?path ?(cfg = []) () =
  let len = List.length cfg in
  let a = Array.make (2 * len) "" in
  List.iteri cfg ~f:(fun i (k, v) ->
    a.(i * 2) <- k;
    a.((i * 2) + 1) <- v);
  let path = Option.map Fpath.to_string path in
  open_ext path a len
;;

let with_db ?path ?cfg f =
  let db = open_ ?path ?cfg () in
  Fun.protect (fun () -> f db) ~finally:(fun () -> close db)
;;

let with_connection db ~f =
  let con = connect db in
  Fun.protect (fun () -> f con) ~finally:(fun () -> disconnect con)
;;

let with_db_connection ?path ?cfg f = with_db ?path ?cfg (with_connection ~f)

let raise_if_error1 text f x =
  match f x with
  | 0 -> ()
  | _ -> failwith text
;;

let raise_if_error2 text f x y =
  match f x y with
  | 0 -> ()
  | _ -> failwith text
;;

module Appender = struct
  type t

  external create : con -> string -> string -> t = "ml_duckdb_appender_create"
  external destroy : t -> unit = "ml_duckdb_appender_destroy" [@@noalloc]
  external append_char : t -> char -> int = "ml_duckdb_append_int8" [@@noalloc]
  external append_int : t -> int -> int = "ml_duckdb_append_int64" [@@noalloc]
  external append_timestamp : t -> int -> int = "ml_duckdb_append_timestamp" [@@noalloc]
  external append_int32 : t -> int32 -> int = "ml_duckdb_append_int32" [@@noalloc]
  external append_int64 : t -> int64 -> int = "ml_duckdb_append_int64" [@@noalloc]
  external end_row : t -> int = "ml_duckdb_appender_end_row" [@@noalloc]

  let create ?(schema = "") con ~table = create con schema table
  let append_char = raise_if_error2 "append_char" append_char
  let append_int = raise_if_error2 "append_int" append_int
  let append_int32 = raise_if_error2 "append_int32" append_int32
  let append_int64 = raise_if_error2 "append_int64" append_int64
  let append_timestamp = raise_if_error2 "append_timestamp" append_timestamp
  let end_row = raise_if_error1 "end_row" end_row
end
