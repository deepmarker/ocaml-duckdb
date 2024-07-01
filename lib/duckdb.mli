include module type of Duckdb_intf

type db
type con

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

val query : con -> string -> unit
val queryf : con -> ('a, Format.formatter, unit, unit) format4 -> 'a

module LogicalType : sig
  type t

  val of_typ : typ -> t
  val decimal : width:int -> scale:int -> t
  val array : t -> int -> t
  val free : t -> unit
end

module Vector : sig
  open Bigarray

  type t
  type validity

  val data : t -> ('a, 'b) kind -> ('a, 'b, c_layout) Array1.t
  val validity : t -> validity
  val valid : validity -> int -> bool
  val set_valid : validity -> int -> bool -> unit
end

module DataChunk : sig
  type t

  val create : LogicalType.t array -> t
  val get_vector : t -> int -> Vector.t

  (** [get_size t] is the current number of tuples in [t]. *)
  val get_size : t -> int

  val set_size : t -> int -> unit

  (** [reset t] sets size to zero and resets validity map. *)
  val reset : t -> unit

  val free : t -> unit
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
