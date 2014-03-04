val git_revision: string
val compile_time: string
val machine:string
val major:int
val minor:int
val patch:int
val dependencies: string


module Bdb : sig
  type bdb
  type bdbcur
  val default_mode : int
  val readonly_mode : int
  type opt = BDBTLARGE
  val put : bdb -> string -> string -> unit
  val get: bdb -> string -> string
  val get_nolock : bdb -> string -> string
  val out: bdb -> string -> unit
  val range: bdb ->
    string -> bool ->
    string option -> bool -> int ->
    string array


  val exists : bdb -> string -> bool
  val delete_prefix : bdb -> string -> int
  val prefix_keys : bdb -> string -> int -> string array

  val first: bdb -> bdbcur -> bool
  val jump : bdb -> bdbcur -> string -> bool
  val next : bdb -> bdbcur -> bool
  val prev : bdb -> bdbcur -> bool
  val last : bdb -> bdbcur -> bool
  val cur_out  : bdb -> bdbcur -> bool
  (** [cur_out b c] deletes the current key value pair and jumps to next *)

  val key: bdb -> bdbcur -> string
  val value: bdb -> bdbcur -> string
  val record: bdb -> bdbcur -> string * string

  val with_cursor : bdb -> (bdb -> bdbcur -> 'a) -> 'a

  val _tranbegin: bdb -> unit
  val _trancommit: bdb -> unit
  val _tranabort: bdb -> unit

  type flag = BDBFOPEN | BDBFFATAL
  val flags: bdb -> flag list

  val defrag : ?step:int64 -> bdb -> int

  val copy_from_cursor : source:bdb -> cursor:bdbcur -> target:bdb -> max:int option -> int
end

module Hotc : sig
  type t
  type bdb = Bdb.bdb
  type bdbcur = Bdb.bdbcur
  val filename : t -> string
  val create : ?mode:int ->
    ?lcnum:int ->
    ?ncnum:int ->
    string -> Bdb.opt list -> t Lwt.t
  val get_bdb: t -> bdb
  val transaction :  t ->  (bdb -> 'd Lwt.t) -> 'd Lwt.t
  val with_cursor : bdb -> (bdb -> bdbcur -> 'a Lwt.t) -> 'a Lwt.t
  val read : t -> (bdb -> 'b Lwt.t) -> 'b Lwt.t
  val optimize : t -> unit Lwt.t
  val defrag : ?step:int64 -> t -> int Lwt.t
  val sync :t -> unit Lwt.t
  val close : t -> unit Lwt.t
  val reopen: t -> (unit -> unit Lwt.t) -> int -> unit Lwt.t
end
