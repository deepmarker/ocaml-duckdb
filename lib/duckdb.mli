include module type of Duckdb_intf

type db
type con

module type Representable = sig
  type t
  type repr

  val to_repr : t -> repr
  val of_repr : repr -> t
end

module Repr : sig
  module Int : Representable with type t = int and type repr = int
  module Int32 : Representable with type t = int32 and type repr = int32
  module Bigdecimal : Representable with type t = Bigdecimal.t and type repr = int
  module Time_ns : Representable with type t = Core.Time_ns.t and type repr = int
end

module Type : sig
  open Bigarray

  type (_, _) t

  val scalar
    :  (module Representable with type t = 'v and type repr = 'a)
    -> ('a, 'b) kind
    -> ('v, ('a, 'b) kind) t

  val array
    :  (module Representable with type t = 'v and type repr = 'a)
    -> ('a, 'b) kind
    -> int
    -> ('v, ('a, 'b) kind) t

  val string : (string, string) t

  val list
    :  (module Representable with type t = 'v and type repr = 'a)
    -> ('a, 'b) t
    -> ('v, ('a, 'b) t) t
end

module LogicalType : sig
  type t

  val scalar : scalar -> t
  val decimal : width:int -> scale:int -> t
  val array : t -> int -> t
  val free : t -> unit
end

module Vector : sig
  open Bigarray

  type (_, _) t

  (** [data t] is the underlying array of [t] if [t] contains
      non-nested, same size elements. *)
  val data : (_, ('a, 'b) kind) t -> ('a, 'b, c_layout) Array1.t

  val get : ('v, ('a, 'b) kind) t -> ('a, 'b, c_layout) Array1.t -> int -> 'v
  val set : ('v, ('a, 'b) kind) t -> ('a, 'b, c_layout) Array1.t -> int -> 'v -> unit
  val valid : _ t -> int -> bool
  val set_valid : _ t -> int -> bool -> unit
end

module DataChunk : sig
  type t

  val create : LogicalType.t array -> t

  (** [length t] is the current number of tuples in [t]. *)
  val length : t -> int

  val set_length : t -> int -> unit
  val vector : t -> int -> ('a, 'b) Type.t -> ('a, 'b) Vector.t
  val get_col_count : t -> int

  (** [reset t] sets size to zero and resets validity map. *)
  val reset : t -> unit

  val free : t -> unit
end

module QResult : sig
  type t

  val rows_changed : t -> int
  val chunk_count : t -> int
  val get_chunk : t -> int -> DataChunk.t
end

module Appender : sig
  type t

  val create : ?schema:string -> con -> table:string -> t

  (** [free t] finishes appending and flush all the rows to the
      table, then frees [t] memory. *)
  val free : t -> unit

  val append_char : t -> char -> unit
  val append_int : t -> int -> unit
  val append_int32 : t -> int32 -> unit
  val append_int64 : t -> int64 -> unit

  (** [append_timestamp t micros] appends the timestamp in microseconds [micros] to [t]. *)
  val append_timestamp : t -> int -> unit

  val append_chunk : t -> DataChunk.t -> unit
  val end_row : t -> unit
  val with_appender : ?schema:string -> con -> table:string -> f:(t -> 'a) -> 'a
end

val version : string lazy_t
val vector_size : int lazy_t
val open_ : ?path:Fpath.t -> ?cfg:(string * string) list -> unit -> db
val close : db -> unit

(** [with_db ?path ?cfg f] will close [db] after [f] returns or
    raises. *)
val with_db : ?path:Fpath.t -> ?cfg:(string * string) list -> (db -> 'a) -> 'a

val connect : db -> con
val disconnect : con -> unit

(** [with_connection db ~f] will disconnect [con] after [f] returns or raises. *)
val with_connection : db -> f:(con -> 'a) -> 'a

(** [with_db_connection ?path ?cfg f] will open a database and create
    a connection, and will disconnect and close the db when [f]
    returns or raises. *)
val with_db_connection : ?path:Fpath.t -> ?cfg:(string * string) list -> (con -> 'a) -> 'a

val query : con -> string -> QResult.t
val queryf : con -> ('a, Format.formatter, unit, QResult.t) format4 -> 'a
