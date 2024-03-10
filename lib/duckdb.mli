type db
type con

val version : unit -> string
val open_ext : string option -> (string * string) list -> db
val close : db -> unit
val connect : db -> con
val disconnect : con -> unit
val exec : con -> string -> unit

module Appender : sig
  type t

  val create : con -> string -> t
  val destroy : t -> unit
  val append_char : t -> char -> unit
  val append_int : t -> int -> unit
  val append_int32 : t -> int32 -> unit
  val append_int64 : t -> int64 -> unit
  val append_timestamp : t -> int -> unit
end
