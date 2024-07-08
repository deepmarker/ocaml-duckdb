open StdLabels
include Duckdb_intf

type db
type con

external version : unit -> string = "ml_duckdb_library_version"
external vector_size : unit -> int = "ml_duckdb_vector_size" [@@noalloc]

let version = lazy (version ())
let vector_size = lazy (vector_size ())

module type Representable = sig
  type t
  type repr

  val to_repr : t -> repr
  val of_repr : repr -> t
end

module Repr = struct
  module Int = struct
    include Int

    type repr = int

    let to_repr = Fun.id
    let of_repr = Fun.id
  end

  module Int32 = struct
    include Int32

    type repr = int32

    let to_repr = Fun.id
    let of_repr = Fun.id
  end

  module Bigdecimal = struct
    include Bigdecimal

    type repr = int

    let to_repr x = mantissa x |> Bigint.to_int_exn
    let of_repr x = Bigint.of_int_exn x |> of_bigint
  end

  module Time_ns = struct
    open Core
    include Time_ns

    type repr = int

    let to_repr = to_int_ns_since_epoch
    let of_repr = of_int_ns_since_epoch
  end
end

module Type = struct
  open Bigarray

  type ('a, 'b) t =
    | Scalar :
        (module Representable with type t = 'v and type repr = 'a) * ('a, 'b) kind
        -> ('v, ('a, 'b) kind) t
    | Array :
        (module Representable with type t = 'v and type repr = 'a) * ('a, 'b) kind * int
        -> ('v, ('a, 'b) kind) t
    | String : (string, string) t
    | List :
        (module Representable with type t = 'v and type repr = 'a) * _ t
        -> ('v, _ t) t

  let scalar m kind = Scalar (m, kind)
  let array m kind len = Array (m, kind, len)
  let string = String
  let list m x = List (m, x)
end

module LogicalType = struct
  type t

  external scalar : scalar -> t = "ml_duckdb_create_logical_type"
  external decimal : int -> int -> t = "ml_duckdb_create_decimal_type"
  external array : t -> int -> t = "ml_duckdb_create_array_type"
  external free : t -> unit = "ml_duckdb_destroy_logical_type" [@@noalloc]

  let decimal ~width ~scale = decimal width scale
end

module Vector = struct
  open Bigarray
  module Array = Array1

  type vector
  type validity = (int64, int64_elt, c_layout) Array1.t

  type ('a, 'b) t =
    { vect : vector
    ; typ : ('a, 'b) Type.t
    ; validity : validity lazy_t
    ; len : int
    }

  external data
    :  vector
    -> ('a, 'b) kind
    -> ('a, 'b, c_layout) Array1.t
    = "ml_duckdb_vector_get_data"

  external get_array_child : vector -> vector = "ml_duckdb_array_vector_get_child"
  external validity : vector -> validity = "ml_duckdb_vector_get_validity"

  let create vect typ len =
    let validity = lazy (validity vect) in
    { vect; typ; validity; len }
  ;;

  let data : type a. (a, ('b, 'c) kind) t -> ('b, 'c, c_layout) Array1.t =
    fun t ->
    match t.typ with
    | Scalar (_, kind) -> Array.sub (data t.vect kind) 0 t.len
    | Array (_, kind, len) ->
      Array.sub (data (get_array_child t.vect) kind) 0 (t.len * len)
  ;;

  let get
    (type v repr)
    (t : (v, (repr, 'c) kind) t)
    (data : (repr, 'b, c_layout) Array1.t)
    i
    =
    match t.typ with
    | Scalar (repr, _) ->
      let module Repr = (val repr : Representable with type t = v and type repr = repr) in
      Repr.of_repr (Array1.get data i)
    | Array _ -> assert false
  ;;

  let set
    (type v repr)
    (t : (v, (repr, 'c) kind) t)
    (data : (repr, 'b, c_layout) Array1.t)
    i
    x
    =
    match t.typ with
    | Scalar (repr, _) ->
      let module Repr = (val repr : Representable with type t = v and type repr = repr) in
      Array1.set data i (Repr.to_repr x)
    | Array _ -> assert false
  ;;

  let valid t i =
    let a = Lazy.force t.validity in
    let entry_idx = i / 64 in
    let idx_in_entry = i mod 64 in
    Int64.(logand a.(entry_idx) (shift_left one idx_in_entry) <> zero)
  ;;

  let set_valid t i valid =
    let a = Lazy.force t.validity in
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
  external get_vector : t -> int -> Vector.vector = "ml_duckdb_data_chunk_get_vector"
  external length : t -> int = "ml_duckdb_data_chunk_get_size" [@@noalloc]
  external set_length : t -> int -> unit = "ml_duckdb_data_chunk_set_size" [@@noalloc]
  external reset : t -> unit = "ml_duckdb_data_chunk_reset" [@@noalloc]
  external get_col_count : t -> int = "ml_duckdb_data_chunk_get_column_count" [@@noalloc]

  let vector t i kind =
    let vect = get_vector t i in
    Vector.create vect kind (length t)
  ;;
end

module QResult = struct
  type t

  external rows_changed : t -> int = "ml_duckdb_rows_changed" [@@noalloc]
  external chunk_count : t -> int = "ml_duckdb_result_chunk_count" [@@noalloc]
  external get_chunk : t -> int -> DataChunk.t = "ml_duckdb_result_get_chunk"
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

external open_ext : string option -> string array -> int -> db = "ml_duckdb_open_ext"
external connect : db -> con = "ml_duckdb_connect"
external disconnect : con -> unit = "ml_duckdb_disconnect" [@@noalloc]
external close : db -> unit = "ml_duckdb_close" [@@noalloc]
external query : con -> string -> QResult.t = "ml_duckdb_query"

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
