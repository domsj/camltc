open Otc

open Lwt



module Hotc = struct
  type bdb = Bdb.bdb

  type bdbcur = Bdb.bdbcur
  type t = {
    filename:string;
    bdb:bdb;
    mutex:Lwt_mutex.t;
  }

  let get_bdb (wrapper: t) =
    wrapper.bdb

  let _do_locked t f =
    Lwt.catch (fun () -> Lwt_mutex.with_lock t.mutex f)
      (fun e -> Lwt.fail e)

  let _open t mode =
    Bdb._dbopen t.bdb t.filename mode

  let _open_lwt t mode = Lwt.return (_open t mode )

  let _close t =
    Bdb._dbclose t.bdb

  let _close_lwt t = Lwt.return (_close t)

  let _sync t =
    Bdb._dbsync t.bdb

  let _sync_lwt t = Lwt.return (_sync t)

  let _setcache t lcnum ncnum = Bdb.setcache t.bdb lcnum ncnum

  let create ?(mode=Bdb.default_mode)
      ?(lcnum = 1024)
      ?(ncnum = 512)
      filename opts =
    let res = {
      filename = filename;
      bdb = Bdb._make ();
      mutex = Lwt_mutex.create ();
    } in
    _do_locked res (fun () ->
      let () = Bdb.setcache res.bdb lcnum ncnum in
      Bdb.tune res.bdb opts;
      _open res mode;
      Lwt.return ()) >>= fun () ->
    Lwt.return res

  let close t =
    _do_locked t (fun () -> _close_lwt t)

  let sync t =
    _do_locked t (fun () -> _sync_lwt t)

  let read t (f:Bdb.bdb -> 'a) = _do_locked t (fun ()-> f t.bdb)

  let filename t = t.filename

  let optimize t =
    Lwt_preemptive.detach (
      fun() ->
        Lwt.ignore_result( Lwt_log.debug "Optimizing database" );
        Bdb.bdb_optimize t.bdb
    ) ()

  let defrag ?step t =
    _do_locked t (fun () -> let r = Bdb.defrag ?step t.bdb in
                            Lwt.return r)

  let reopen t when_closed mode=
    _do_locked t
      (fun () ->
        _close_lwt t >>= fun () ->
        when_closed () >>= fun () ->
        _open_lwt  t mode
      )

  let _delete t =
    Bdb._dbclose t.bdb;
    Bdb._delete t.bdb


  let _delete_lwt t = Lwt.return(_delete t)

  let delete t = _do_locked t (fun () -> _delete_lwt t)

  let _transaction t (f:Bdb.bdb -> 'a) =
    let bdb = t.bdb in
    Bdb._tranbegin bdb;
    Lwt.catch
      (fun () ->
	f bdb >>= fun res ->
	Bdb._trancommit bdb;
	Lwt.return res
      )
      (fun x ->
	Bdb._tranabort bdb ;
	Lwt.fail x)


  let transaction t (f:Bdb.bdb -> 'a) =
    _do_locked t (fun () -> _transaction t f)


  let with_cursor bdb (f:Bdb.bdb -> 'a) =
    let cursor = Bdb._cur_make bdb in
    Lwt.finalize
      (fun () -> f bdb cursor)
      (fun () -> let () = Bdb._cur_delete cursor in Lwt.return ())


  let exists t key =
    Lwt.catch
      ( fun () ->
        let bdb = get_bdb t in
        Lwt.return (Bdb.get bdb key) >>= fun _ ->
	    Lwt.return true
      )
      (function | Not_found -> Lwt.return false | exn -> Lwt.fail exn)
end
