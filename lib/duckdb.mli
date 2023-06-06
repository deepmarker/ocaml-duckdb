type db
type con

val version : unit -> string
val open_ext : string option -> (string * string) list -> db
val close : db -> unit
val connect : db -> con
val disconnect : con -> unit
val exec : con -> string -> unit
