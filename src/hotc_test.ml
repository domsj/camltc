open OUnit
open Hotc
open Otc
open Prefix_otc
open Extra
open Lwt
open Logging

let eq_string s1 s2 = eq_string "TEST" s1 s2

let test_overal db =
  Hotc.transaction db
    (fun db ->
      let () = Bdb.put db "hello" "world" in
      let x = Bdb.get db "hello" in
      Lwt.return x
    ) >>= fun res ->
  let () = eq_string "world" res in
  Lwt.return ()

let test_with_cursor db =
  Hotc.transaction db
    (fun db' ->
     let () = Bdb.put db' "hello" "world" in
     Hotc.with_cursor db'
                      (fun _ cursor ->
                       let _ = Bdb.first db' cursor in
                       let x = Bdb.value db' cursor in
                       Lwt.return x
                      )
    ) >>= fun res ->
  let () = eq_string "world" res in
  Lwt.return ()

let test_prefix db =
  Hotc.transaction db
    (fun db' ->
      Prefix_otc.put db' "VOL" "hello" "world" >>= fun () ->
      let x = Bdb.get db' "VOLhello" in
      Prefix_otc.get db' "VOL" "hello" >>= fun y ->
      Lwt.return (x,y)
    ) >>= fun (res1, res2) ->
  let () = eq_string "world" res1 in
  let () = eq_string "world" res2 in
  Lwt.return ()

let test_prefix_fold db =
  Hotc.transaction db
    (fun db' ->
      Prefix_otc.put db' "VOL" "hello" "world" >>= fun () ->
      Prefix_otc.put db' "VOL" "hi" "mars"
    ) >>= fun () ->
  Hotc.transaction db
    (fun db' -> Prefix_otc.iter (log "%s %s") db' "VOL")


let test_transaction db =
  let key = "test_transaction:1"
  and bad_key = "test_transaction:does_not_exist"
  in
  Lwt.catch
    (fun () ->
      Hotc.transaction db
	(fun db ->
	  Bdb.put db key "one";
	  Bdb.out db bad_key;
	  Lwt.return ()
	)
    )
    (function
      | Not_found ->
	Lwt_log.debug "yes, this key was not found" >>= fun () ->
	Lwt.return ()
      | x -> Lwt.fail x
    )
  >>= fun () ->
Lwt.catch
  (fun () ->
    Hotc.transaction db
      (fun db ->
	let v = Bdb.get db key in
	Lwt_io.printf "value=%s\n" v >>= fun () ->
	OUnit.assert_failure "this is not a transaction"
      )
  )
  (function
    | Not_found -> Lwt.return ()
    | x -> Lwt.fail x
  )



let eq_string str i1 i2 =
  let msg = Printf.sprintf "%s expected:\"%s\" actual:\"%s\"" str (String.escaped i1) (String.escaped i2) in
  OUnit.assert_equal ~msg i1 i2

let eq_list eq_ind str l1 l2 =
  let len1 = List.length l1 in
  let len2 = List.length l2 in
  if len1 <> len2 then
    OUnit.assert_failure (Printf.sprintf "%s: lists not equal in length l1:%d l2:%d" str len1 len2)
  else
    let cl = List.combine l1 l2 in
    let (_:int) = List.fold_left
      (fun i (x1,x2) ->
        let () = eq_ind (Printf.sprintf "ele:%d %s" i str) x1 x2 in
        i+1) 0 cl in
    ()

let eq_tuple eq1 eq2 str v1 v2 =
  let (v11, v12) = v1 in
  let (v21, v22) = v2 in
  let () = eq1 ("t1:"^str) v11 v21 in
  let () = eq2 ("t2:"^str) v12 v22 in
  ()

let show_l l =
  let l2 = List.map (fun (k,v) -> Printf.sprintf "('%s','%s')" k v) l in
  "[" ^ (String.concat ";" l2) ^ "]"

let test_next_prefix =
  let get = function | Some x -> x | _ -> raise Exit in
  let some_next_prefix s = get (next_prefix s) in
  let assert_next prefix next = eq_string "" (some_next_prefix prefix) next in
  assert_next "aa" "ab";
  assert_next "a\255\255" "b\000\000";
  let next = next_prefix "\255\255" in
  if (next <> None) then failwith "next prefix not correct"


let load db kvs = List.iter (fun (k,v) -> Bdb.put db k v) kvs


let test_delete_prefix db =
  let bdb = Hotc.get_bdb db in
  let rec fill i =
    if i = 0
    then ()
    else
      let () = Bdb.put bdb (Printf.sprintf "my_prefix_%i"  i) "value does not matter" in
      fill (i-1)
  in
  let () = fill 100 in
  let g0 = Bdb.delete_prefix bdb "x" in
  let my_test a b = OUnit.assert_equal ~printer:string_of_int a b in
  my_test 0 g0;
  let g11 = Bdb.delete_prefix bdb "my_prefix_2" in
  my_test 11 g11;
  let g89 = Bdb.delete_prefix bdb "my_prefix_" in
  my_test 89 g89;
  Lwt.return ()

let setup () = Hotc.create "/tmp/foo.tc" []

let teardown db =
  Hotc.delete db >>= fun () ->
  let () = Unix.unlink "/tmp/foo.tc" in
  Lwt.return ()

let suite =
  let wrap f = lwt_bracket setup f teardown in
  "Hotc" >:::
    [
      "overal" >:: wrap test_overal;
      "with_cursor" >:: wrap test_with_cursor;
      "prefix" >:: wrap test_prefix;
      "prefix_fold" >:: wrap test_prefix_fold;
      "transaction" >:: wrap test_transaction;
      "delete_prefix" >:: wrap test_delete_prefix;
    ]
