open StdLabels
include Duckdb_intf

type db
type con

external version : unit -> string = "ml_duckdb_library_version"
external vector_size : unit -> int = "ml_duckdb_vector_size" [@@noalloc]

let version = lazy (version ())
let vector_size = lazy (vector_size ())

module LogicalType = struct
  module Binding = struct
    type t

    external scalar : typ -> t = "ml_duckdb_create_logical_type"
    external decimal : int -> int -> t = "ml_duckdb_create_decimal_type"
    external array : t -> int -> t = "ml_duckdb_create_array_type"
    external free : t -> unit = "ml_duckdb_destroy_logical_type" [@@noalloc]
  end

  module Repr = struct
    type t =
      (* scalar *)
      | Scalar of typ
      | Enum of string array
      | Decimal of int * int
      (* nested *)
      | List of t
      | Array of t * int
      | Map of (t * t)
      | Union of (string * t) array
      | Struct of (string * t) array

    let scalar scalar = Scalar scalar
    let enum vs = Enum vs
    let decimal ~precision ~scale = Decimal (precision, scale)
    let list t = List t
    let array t len = Array (t, len)
    let map k v = Map (k, v)
    let union vs = Union vs
    let struct_ vs = Struct vs
  end

  type t = Binding.t list

  let rec create = function
    | Repr.Scalar typ -> [ Binding.scalar typ ]
    | Array (t, len) ->
      let elt = create t in
      Binding.array (List.hd elt) len :: elt
    | Decimal (w, s) -> [ Binding.decimal w s ]
    | _ -> assert false
  ;;

  let free ts = List.iter ~f:Binding.free (List.rev ts)
end

module type Representable = sig
  type t
  type repr

  val logical : LogicalType.Repr.t
  val to_repr : t -> repr
  val of_repr : repr -> t
end

module Repr = struct
  module Char = struct
    include Char

    type repr = int

    let logical = LogicalType.Repr.scalar UTINYINT
    let to_repr = Char.code
    let of_repr = Char.chr
  end

  let char = (module Char : Representable with type t = char and type repr = int)

  module Bool = struct
    include Bool

    type repr = int

    let logical = LogicalType.Repr.scalar BOOLEAN

    let to_repr = function
      | false -> 0
      | true -> 1
    ;;

    let of_repr = function
      | 0 -> false
      | _ -> true
    ;;
  end

  let bool = (module Bool : Representable with type t = bool and type repr = int)

  module Int = struct
    include Int

    type repr = int

    let logical = LogicalType.Repr.scalar BIGINT
    let to_repr = Fun.id
    let of_repr = Fun.id
  end

  let int = (module Int : Representable with type t = int and type repr = int)

  module Int32 = struct
    include Int32

    type repr = int32

    let logical = LogicalType.Repr.scalar INTEGER
    let to_repr = Fun.id
    let of_repr = Fun.id
  end

  let int32 = (module Int32 : Representable with type t = int32 and type repr = int32)

  let decimal ~precision ~scale =
    let module Bigdecimal = struct
      include Bigdecimal

      type repr = int

      let logical = LogicalType.Repr.decimal ~precision ~scale
      let to_repr x = mantissa x |> Bigint.to_int_exn
      let of_repr x = Bigint.of_int_exn x |> of_bigint
    end
    in
    (module Bigdecimal : Representable with type t = Bigdecimal.t and type repr = int)
  ;;

  module Time_ns = struct
    open Core
    include Time_ns

    type repr = int

    let logical = LogicalType.Repr.scalar TIMESTAMP_TZ
    let to_repr x = to_int_ns_since_epoch x / 1000
    let of_repr x = of_int_ns_since_epoch (x * 1000)
  end

  let time = (module Time_ns : Representable with type t = Time_ns.t and type repr = int)
end

module Type = struct
  open Bigarray

  type ('v, 'a, 'ba_repr) t =
    | Scalar :
        (module Representable with type t = 'v and type repr = 'a) * ('a, 'b) kind
        -> ('v, 'a, ('a, 'b) kind) t
    | Array :
        (module Representable with type t = 'v and type repr = 'a) * ('a, 'b) kind * int
        -> ('v, 'a, ('a, 'b) kind) t
  (* | Varchar : (string, string) t *)
  (* | Blob : (string, string) t *)
  (* | List : *)
  (*     (module Representable with type t = 'v and type repr = 'a) * _ t *)
  (*     -> ('v, _ t) t *)

  let scalar m kind = Scalar (m, kind)
  let array m kind len = Array (m, kind, len)

  let length = function
    | Scalar _ -> Lazy.force vector_size
    | Array (_, _, len) -> len * Lazy.force vector_size
  ;;

  let logical : type v repr x. (v, repr, x) t -> LogicalType.t =
    fun t ->
    let open LogicalType in
    match t with
    | Scalar (sc, _) ->
      let module X = (val sc : Representable with type t = v and type repr = repr) in
      create X.logical
    | Array (sc, _, len) ->
      let module X = (val sc : Representable with type t = v and type repr = repr) in
      create (Repr.array X.logical len)
  ;;
end

module Prod = struct
  type 'a t =
    | [] : unit t
    | ( :: ) : ('v, 'a, 'b) Type.t * 't t -> ('v * 'a * 'b -> 't) t
end

module Vector = struct
  open Bigarray
  module Array = Array1

  type vector
  type validity = (int64, int64_elt, c_layout) Array1.t

  type ('v, 'a, 'b) t =
    { vect : vector
    ; typ : ('v, 'a, 'b) Type.t
    ; validity : validity lazy_t
    }

  external data
    :  vector
    -> ('a, 'b) kind
    -> int
    -> ('a, 'b, c_layout) Array1.t
    = "ml_duckdb_vector_get_data"

  external get_array_child : vector -> vector = "ml_duckdb_array_vector_get_child"
  external validity : vector -> validity = "ml_duckdb_vector_get_validity"

  let create vect typ =
    let validity = lazy (validity vect) in
    { vect; typ; validity }
  ;;

  let data t =
    let len = Type.length t.typ in
    match t.typ with
    | Scalar (_, kind) -> data t.vect kind len
    | Array (_, kind, _alen) -> data (get_array_child t.vect) kind len
  ;;

  let get
    (type v repr)
    (t : (v, repr, (repr, 'c) kind) t)
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
    (t : (v, repr, (repr, 'c) kind) t)
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
  module Binding = struct
    type t

    external create : LogicalType.Binding.t array -> t = "ml_duckdb_create_data_chunk"
    external free : t -> unit = "ml_duckdb_destroy_data_chunk" [@@noalloc]
    external get_vector : t -> int -> Vector.vector = "ml_duckdb_data_chunk_get_vector"
    external length : t -> int = "ml_duckdb_data_chunk_get_size" [@@noalloc]
    external set_length : t -> int -> unit = "ml_duckdb_data_chunk_set_size" [@@noalloc]
    external reset : t -> unit = "ml_duckdb_data_chunk_reset" [@@noalloc]

    (* external get_col_count : t -> int = "ml_duckdb_data_chunk_get_column_count" *)
    (* [@@noalloc] *)
  end

  type 'a t =
    { prod : 'a Prod.t
    ; types : LogicalType.t array
    ; chunk : Binding.t
    ; i : int
    }

  let length t = Binding.length t.chunk
  let set_length t = Binding.set_length t.chunk
  let reset t = Binding.reset t.chunk

  let create ?chunk prod =
    let rec loop : type x. 'a -> x Prod.t -> 'a =
      fun a prod ->
      match prod with
      | [] -> a
      | typ :: rest -> loop (Type.logical typ :: a) rest
    in
    let types = List.rev (loop [] prod) |> Array.of_list in
    let chunk =
      Option.value chunk ~default:(Binding.create (Array.map ~f:List.hd types))
    in
    { prod; types; chunk; i = 0 }
  ;;

  let of_chunk chunk prod = create ~chunk prod
  let create prod = create ?chunk:None prod

  let free { types; chunk; _ } =
    Binding.free chunk;
    Array.iter types ~f:LogicalType.free
  ;;

  let hd : type v a b rest. (v * a * b -> rest) t -> (v, a, b) Vector.t * rest t =
    fun t ->
    match t.prod with
    | typ :: rest ->
      let vect = Binding.get_vector t.chunk t.i in
      Vector.create vect typ, { t with prod = rest; i = succ t.i }
  ;;
end

module QResult = struct
  module Binding = struct
    type t

    external rows_changed : t -> int = "ml_duckdb_rows_changed" [@@noalloc]
    external chunk_count : t -> int = "ml_duckdb_result_chunk_count" [@@noalloc]
    external get_chunk : t -> int -> DataChunk.Binding.t = "ml_duckdb_result_get_chunk"
  end

  type 'a t =
    { prod : 'a Prod.t
    ; t : Binding.t
    }

  let create t prod = { prod; t }

  let get_chunk { prod; t } i =
    let chunk = Binding.get_chunk t i in
    DataChunk.of_chunk chunk prod
  ;;

  let rows_changed { t; _ } = Binding.rows_changed t
  let chunk_count { t; _ } = Binding.chunk_count t
end

module Appender = struct
  module Binding = struct
    type t

    external get_error : t -> string = "ml_duckdb_appender_error"
    external create : con -> string -> string -> t = "ml_duckdb_appender_create"
    external flush : t -> unit = "ml_duckdb_appender_flush"
    external free : t -> unit = "ml_duckdb_appender_destroy"

    external append_chunk
      :  t
      -> DataChunk.Binding.t
      -> int
      = "ml_duckdb_append_data_chunk"
  end

  type 'a t = Binding.t

  let flush = Binding.flush
  let free = Binding.free

  let raise_if_error1 f t x =
    match f t x with
    | 0 -> ()
    | _ -> failwith (Binding.get_error t)
  ;;

  let create ?(schema = "") con ~table _prod = Binding.create con schema table
  let append_chunk t chunk = raise_if_error1 Binding.append_chunk t chunk.DataChunk.chunk

  let with_appender ?schema con ~table prod ~f =
    let t = create ?schema con ~table prod in
    Fun.protect (fun () -> f t) ~finally:(fun () -> Binding.free t)
  ;;
end

external open_ext : string option -> string array -> int -> db = "ml_duckdb_open_ext"
external connect : db -> con = "ml_duckdb_connect"
external disconnect : con -> unit = "ml_duckdb_disconnect" [@@noalloc]
external close : db -> unit = "ml_duckdb_close"
external query : con -> string -> QResult.Binding.t = "ml_duckdb_query"

let query con str typ =
  let res = query con str in
  QResult.create res typ
;;

let queryf con typ = Format.kasprintf (fun str -> query con str typ)

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
