type db
type con

val version : unit -> string
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

val exec : con -> string -> unit

module Appender : sig
  type t

  val create : ?schema:string -> con -> table:string -> t

  (** [destroy t] finishes appending and flush all the rows to the
      table, then frees [t] memory. *)
  val destroy : t -> unit

  val append_char : t -> char -> unit
  val append_int : t -> int -> unit
  val append_int32 : t -> int32 -> unit
  val append_int64 : t -> int64 -> unit
  val append_timestamp : t -> int -> unit
  val end_row : t -> unit
end
