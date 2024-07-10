type db
type con

include module type of Duckdb_intf

module LogicalType : sig
  module Repr : sig
    type t

    val scalar : typ -> t
    val enum : string array -> t
    val decimal : precision:int -> scale:int -> t
    val list : t -> t
    val array : t -> int -> t
    val map : t -> t -> t
    val union : (string * t) array -> t
    val struct_ : (string * t) array -> t
  end
end

module type Representable = sig
  type t
  type repr

  val logical : LogicalType.Repr.t
  val to_repr : t -> repr
  val of_repr : repr -> t
end

module Repr : sig
  val char : (module Representable with type t = char and type repr = int)
  val bool : (module Representable with type t = bool and type repr = int)
  val int : (module Representable with type t = int and type repr = int)
  val int32 : (module Representable with type t = int32 and type repr = int32)

  val decimal
    :  precision:int
    -> scale:int
    -> (module Representable with type t = Bigdecimal.t and type repr = int)

  val time : (module Representable with type t = Core.Time_ns.t and type repr = int)
end

module Type : sig
  open Bigarray

  type (_, _, _) t

  val scalar
    :  (module Representable with type t = 'v and type repr = 'a)
    -> ('a, 'b) kind
    -> ('v, 'a, ('a, 'b) kind) t

  val array
    :  (module Representable with type t = 'v and type repr = 'a)
    -> ('a, 'b) kind
    -> int
    -> ('v, 'a, ('a, 'b) kind) t
end

module Prod : sig
  type 'a t =
    | [] : unit t
    | ( :: ) : ('v, 'a, 'b) Type.t * 't t -> ('v * 'a * 'b -> 't) t
end

module Vector : sig
  open Bigarray

  type (_, _, _) t

  (** [data t] is the underlying array of [t]. *)
  val data : (_, 'a, ('a, 'b) kind) t -> ('a, 'b, c_layout) Array1.t

  val get : ('v, 'repr, ('repr, 'c) kind) t -> ('repr, 'b, c_layout) Array1.t -> int -> 'v

  val set
    :  ('v, 'repr, ('repr, 'c) kind) t
    -> ('repr, 'b, c_layout) Array1.t
    -> int
    -> 'v
    -> unit

  val valid : _ t -> int -> bool
  val set_valid : _ t -> int -> bool -> unit
end

module DataChunk : sig
  type _ t

  val create : 'a Prod.t -> 'a t

  (** [length t] is the current number of tuples in [t]. *)
  val length : _ t -> int

  val set_length : _ t -> int -> unit
  val hd : 'v 'a 'b 'rest. ('v * 'a * 'b -> 'rest) t -> ('v, 'a, 'b) Vector.t * 'rest t

  (** [reset t] sets size to zero and resets validity map. *)
  val reset : _ t -> unit

  (** [free chunk] will free all resources associated with
      [chunk]. Nothing more needs to be cleaned at this point (will
      cleanup datachunk objects and logical types hierarchy.*)
  val free : _ t -> unit
end

module QResult : sig
  type _ t

  val rows_changed : _ t -> int
  val chunk_count : _ t -> int
  val get_chunk : 'a t -> int -> 'a DataChunk.t
end

module Appender : sig
  type 'a t

  val create : ?schema:string -> con -> table:string -> 'a Prod.t -> 'a t

  (** [free t] finishes appending and flush all the rows to the
      table, then frees [t] memory. *)
  val free : _ t -> unit

  val append_chunk : 'a t -> 'a DataChunk.t -> unit

  val with_appender
    :  ?schema:string
    -> con
    -> table:string
    -> 'a Prod.t
    -> f:('a t -> 'b)
    -> 'b
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

val query : con -> string -> 'a Prod.t -> 'a QResult.t
val queryf : con -> 'a Prod.t -> ('b, Format.formatter, unit, 'a QResult.t) format4 -> 'b
