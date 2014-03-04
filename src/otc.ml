(* use Hotc for highlevel locked access *)

let next_prefix prefix =
  let next_char c =
    let code = Char.code c + 1 in
    match code with
      | 256 -> Char.chr 0, true
      | code -> Char.chr code, false in
  let rec inner s pos =
    let c, carry = next_char s.[pos] in
    s.[pos] <- c;
    match carry, pos with
      | false, _ -> Some s
      | true, 0 -> None
      | true, pos -> inner s (pos - 1) in
  let copy = String.copy prefix in
  inner copy ((String.length copy) - 1)

let prefix_match prefix k =
  let pl = String.length prefix in
  let rec ok i = (i = pl) || (prefix.[i] = k.[i] && ok (i+1)) in
  String.length k >= pl && ok 0

module Bdb = struct

  type bdb (* type stays abstract *)

  let oreader = 1
  let owriter = 2
  let ocreat  = 4
  let otrunc  = 8
  let onolck  = 16
  let olcknb  = 32
  let otsync  = 64

  let default_mode = (oreader lor owriter lor ocreat lor olcknb)
  let readonly_mode = (oreader lor onolck)

  type bdbcur (* type stays abstract *)

  external first: bdb -> bdbcur -> bool = "bdb_first"
  external next: bdb -> bdbcur -> bool = "bdb_next"
  external prev: bdb -> bdbcur -> bool = "bdb_prev"
  external last: bdb -> bdbcur -> bool = "bdb_last"
  external key: bdb -> bdbcur -> string = "bdb_key"
  external value: bdb -> bdbcur -> string = "bdb_value"
  external record: bdb -> bdbcur -> string * string = "bdb_record"
  external jump: bdb -> bdbcur -> string -> bool = "bdb_jump"

  let current = 0
  let before = 1
  let after = 2

  external cur_out: bdb -> bdbcur -> bool = "bdb_cur_out"

  external out: bdb -> string -> unit = "bdb_out"
  external put: bdb -> string -> string -> unit = "bdb_put"
  external get: bdb -> string -> string = "bdb_get"
  external get_nolock : bdb -> string -> string = "bdb_get_nolock"
  external putkeep: bdb -> string -> string -> unit = "bdb_putkeep"

  (* TODO: let getters return a "string option" isof throwing Not_found *)

  (* TODO: maybe loose the delete calls and hook it up in GC *)

    (* don't use these directly , use Hotc *)
  external _make: unit -> bdb   = "bdb_make"
  external _delete: bdb -> unit = "bdb_delete"

  external _dbopen: bdb -> string -> int -> unit = "bdb_dbopen"
  external _dbclose: bdb -> unit                 = "bdb_dbclose"
  external _dbsync: bdb -> unit                  = "bdb_dbsync"

  external _cur_make: bdb -> bdbcur = "bdb_cur_make"
  external _cur_delete: bdbcur -> unit = "bdb_cur_delete"

  external _tranbegin: bdb -> unit = "bdb_tranbegin"
  external _trancommit: bdb -> unit = "bdb_trancommit"
  external _tranabort: bdb -> unit = "bdb_tranabort"

  external range: bdb -> string -> bool -> string option -> bool -> int -> string array
    = "bdb_range_bytecode" "bdb_range_native"

  external prefix_keys: bdb -> string -> int -> string array = "bdb_prefix_keys"
  external bdb_optimize: bdb -> unit = "bdb_optimize"

  external _bdb_defrag: bdb -> int64 -> int = "bdb_defrag"
  let defrag ?(step=Int64.max_int) bdb = _bdb_defrag bdb step

  external get_key_count: bdb -> int64 = "bdb_key_count"

  external setcache: bdb -> int -> int -> unit = "bdb_setcache"

  type opt = BDBTLARGE
  external _tune : bdb -> (* int -> int -> int -> int -> int -> *) int -> unit = "bdb_tune"
  let tune bdb opts =
    let int_of_opt = function
      BDBTLARGE -> 1 lsl 0
    in
    let int_of_opts = List.fold_left (fun a b -> a lor int_of_opt b) 0 in
    _tune bdb (int_of_opts opts)

  let with_cursor bdb (f:bdb -> 'a) =
    let cursor = _cur_make bdb in
    try
      let x = f bdb cursor in
      let () = _cur_delete cursor in
      x
    with
      | exn ->
        let () = _cur_delete cursor in
        raise exn


  let delete_prefix bdb prefix =
    with_cursor
      bdb
      (fun bdb cur ->
       if jump bdb cur prefix
       then
         begin
           let rec step n =
             let jumped_key = key bdb cur in
             if prefix_match prefix jumped_key
             then
               begin
                 if cur_out bdb cur  (* and jump to next *)
                 then
                   step (n + 1)
                 else
                   n
               end
             else
               n
           in
           step 0
         end
       else
         0)


  let exists bdb key =
    try
      let _ = get bdb key in true
    with
      | Not_found -> false

  external _flags : bdb -> int = "bdb_flags"

  type flag = BDBFOPEN | BDBFFATAL

  let flags bdb =
    let f = _flags bdb in
    List.fold_left
      (fun acc (s, c) -> if f land c <> 0 then s :: acc else acc)
      []
      (* Shifts taken from tcbdb.h and tchdb.h *)
      [(BDBFOPEN, 1 lsl 0); (BDBFFATAL, 1 lsl 1)]

  external _copy_from_cursor : bdb -> bdbcur -> bdb -> int -> int = "bdb_copy_from_cursor"

  let copy_from_cursor ~source ~cursor ~target ~max =
    let count = match max with
      | None -> -1
      | Some i -> i
    in
    _copy_from_cursor source cursor target count
end
