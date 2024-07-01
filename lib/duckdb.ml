open StdLabels
include Duckdb_intf

type db
type con

external version : unit -> string = "ml_duckdb_library_version"
external vector_size : unit -> int = "ml_duckdb_vector_size" [@@noalloc]

let version = lazy (version ())
let vector_size = lazy (vector_size ())

external open_ext : string option -> string array -> int -> db = "ml_duckdb_open_ext"
external connect : db -> con = "ml_duckdb_connect"
external disconnect : con -> unit = "ml_duckdb_disconnect" [@@noalloc]
external close : db -> unit = "ml_duckdb_close" [@@noalloc]
external query : con -> string -> unit = "ml_duckdb_query"

let queryf con = Format.kasprintf (query con)

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

module LogicalType = struct
  type t

  external of_typ : typ -> t = "ml_duckdb_create_logical_type"
  external decimal : int -> int -> t = "ml_duckdb_create_decimal_type"
  external array : t -> int -> t = "ml_duckdb_create_array_type"
  external free : t -> unit = "ml_duckdb_destroy_logical_type" [@@noalloc]

  let decimal ~width ~scale = decimal width scale
end

module Vector = struct
  open Bigarray
  module Array = Array1

  type t
  type validity = (int64, int64_elt, c_layout) Array1.t

  external data
    :  t
    -> ('a, 'b) kind
    -> ('a, 'b, c_layout) Array1.t
    = "ml_duckdb_vector_get_data"

  external validity : t -> validity = "ml_duckdb_vector_get_validity"

  let valid a i =
    let entry_idx = i / 64 in
    let idx_in_entry = i mod 64 in
    Int64.(logand a.(entry_idx) (shift_left one idx_in_entry) <> zero)
  ;;

  let set_valid a i valid =
    let entry_idx = i / 64 in
    let idx_in_entry = i mod 64 in
    let x =
      if valid
      then Int64.(logand a.(entry_idx) (shift_left one idx_in_entry))
      else Int64.(logand a.(entry_idx) (lognot (shift_left one idx_in_entry)))
    in
    a.(entry_idx) <- x
  ;;
end

module DataChunk = struct
  type t

  external create : LogicalType.t array -> t = "ml_duckdb_create_data_chunk"
  external free : t -> unit = "ml_duckdb_destroy_data_chunk" [@@noalloc]
  external get_vector : t -> int -> Vector.t = "ml_duckdb_data_chunk_get_vector"
  external get_size : t -> int = "ml_duckdb_data_chunk_get_size" [@@noalloc]
  external set_size : t -> int -> unit = "ml_duckdb_data_chunk_set_size" [@@noalloc]
  external reset : t -> unit = "ml_duckdb_data_chunk_reset" [@@noalloc]
end

module Appender = struct
  type t

  external get_error : t -> string = "ml_duckdb_appender_error"
  external create : con -> string -> string -> t = "ml_duckdb_appender_create"
  external free : t -> unit = "ml_duckdb_appender_destroy"
  external append_char : t -> char -> int = "ml_duckdb_append_int8" [@@noalloc]
  external append_timestamp : t -> int -> int = "ml_duckdb_append_timestamp" [@@noalloc]
  external append_int : t -> int -> int = "ml_duckdb_append_int" [@@noalloc]
  external append_int32 : t -> int32 -> int = "ml_duckdb_append_int32" [@@noalloc]
  external append_int64 : t -> int64 -> int = "ml_duckdb_append_int64" [@@noalloc]

  external append_chunk : t -> DataChunk.t -> int = "ml_duckdb_append_data_chunk"
  [@@noalloc]

  external end_row : t -> int = "ml_duckdb_appender_end_row" [@@noalloc]

  let raise_if_error f t =
    match f t with
    | 0 -> ()
    | _ -> failwith (get_error t)
  ;;

  let raise_if_error1 f t x =
    match f t x with
    | 0 -> ()
    | _ -> failwith (get_error t)
  ;;

  let create ?(schema = "") con ~table = create con schema table
  let append_char = raise_if_error1 append_char
  let append_int = raise_if_error1 append_int
  let append_int32 = raise_if_error1 append_int32
  let append_int64 = raise_if_error1 append_int64
  let append_timestamp = raise_if_error1 append_timestamp
  let append_chunk = raise_if_error1 append_chunk
  let end_row = raise_if_error end_row

  let with_appender ?schema con ~table ~f =
    let t = create ?schema con ~table in
    Fun.protect (fun () -> f t) ~finally:(fun () -> free t)
  ;;
end
