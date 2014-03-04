open OUnit
open Otc
open Logging
open Extra

let db_fn suff = Printf.sprintf "/tmp/db%s.tc" suff

let setup_tc suff _ =
  let db = Bdb._make () in
  let _ = Bdb._dbopen db (db_fn suff) (Bdb.owriter lor Bdb.ocreat lor Bdb.otrunc) in
    db

let teardown_tc suff db =
  let _ = Bdb._dbclose db in
  let _ = Bdb._delete db in
  let _ = Unix.unlink (db_fn suff) in
    ()

let test_make () =
  let db = Bdb._make () in
  let _ = Bdb._delete db in
    ()

let test_open db = ()

let test_basic db =
  let _ = Bdb.put db "hello" "world" in
  let v = Bdb.get db "hello" in
  let _ = Extra.eq_string "put/get" "world" v in
    ()

let test_cursor db =
  let _ = Bdb.put db "key1" "value1" in
  let _ = Bdb.put db "key2" "value2" in
  let _ = Bdb.put db "key3" "value3" in
  let cur = Bdb._cur_make db in
  let _ = Bdb.first db cur in
  let _ = eq_string "key1" "key1" (Bdb.key db cur) in
  let _ = eq_string "value1" "value1" (Bdb.value db cur) in
  let key,value = Bdb.record db cur in
  let _ = eq_string "key1" "key1" key in
  let _ = eq_string "value1" "value1" value in
  let _ = Bdb.next db cur in
  let _ = eq_string "key2" "key2" (Bdb.key db cur) in
  let _ = eq_string "value2" "value2" (Bdb.value db cur) in
  let _ = Bdb.next db cur in
  let _ = eq_string "key3" "key3" (Bdb.key db cur) in
  let _ = eq_string "value3" "value3" (Bdb.value db cur) in
  let _ = if not (Bdb.next db cur) then assert_failure "expecting failure" in
  let _ = Bdb._cur_delete cur in
  ()

let load db kvs = List.iter (fun (k,v) -> Bdb.put db k v) kvs

let test_range db =
  let () = load db [
    "kex1","value1";
    "key2","value2";
    "key3","value3";
    "kez3","value4";
  ]
  in
  let a = Bdb.range db "key" true (Some "kez") false (-1) in
  let () = eq_int "num==2" 2 (Array.length a) in
  let () = eq_string "key2" "key2" a.(0) in
  let () = eq_string "key3" "key3" a.(1) in
  ()


let test_unknown db =
  let _ = try Bdb.get db "hello" with
    | Not_found -> ""
    | _ -> assert_failure "unexpected exception"
  in ()

let test_prefix_keys db =
  let () = load db [
    "kex1", "value1";
    "key2", "value2";
    "key3", "value3";
    "kez3", "value4"]
  in
  let a = Bdb.prefix_keys db "key" (-1) in
  let () = eq_int "num==2" 2 (Array.length a) in
  let () = eq_string "key2" "key2" a.(0) in
  let () = eq_string "key3" "key3" a.(1) in
    ()

let test_null db =
  let str = String.make 5 (char_of_int 0) in
  let () = Bdb.put db "key1" str in
  let str2 = Bdb.get db "key1" in
  let () = eq_int "length==5" 5 (String.length str2) in
  let () = eq_int "char0" 0 (int_of_char str2.[0]) in
  let () = eq_int "char0" 0 (int_of_char str2.[1]) in
  let () = eq_int "char0" 0 (int_of_char str2.[2]) in
  let () = eq_int "char0" 0 (int_of_char str2.[3]) in
  let () = eq_int "char0" 0 (int_of_char str2.[4]) in
    ()

let test_flags db =
  match Bdb.flags db with
    | [Bdb.BDBFOPEN] -> ()
    | _ -> assert_failure "Unexpected flags set"

let _test_copy_from_cursor db1 db2 max expected =
  Bdb.with_cursor
    db1
    (fun sdb cur ->
     ignore (Bdb.first sdb cur);
     let cnt = Bdb.copy_from_cursor sdb cur db2 max in
     OUnit.assert_equal ~printer:string_of_int expected cnt)

let test_copy_from_cursor_0 db1 db2 =
  _test_copy_from_cursor db1 db2 None 0

let _fill db =
  let rec loop = function
    | 0 -> ()
    | n -> Bdb.put db (Printf.sprintf "key%d" n) "value"; loop (n - 1)
  in
  loop

let test_copy_from_cursor_1 db1 db2 =
  _fill db1 1;
  _test_copy_from_cursor db1 db2 None 1

let test_copy_from_cursor_2 db1 db2 =
  _fill db1 2;
  _test_copy_from_cursor db1 db2 None 2

let test_copy_from_cursor_3 db1 db2 =
  _fill db1 10;
  _test_copy_from_cursor db1 db2 (Some 5) 5

let test_copy_from_cursor_4 db1 db2 =
  _fill db1 10;
  _test_copy_from_cursor db1 db2 (Some 20) 10

let test_copy_from_cursor_5 db1 db2 =
  _fill db1 100;

  let test sdb cur tdb max =
    let rec loop i c =
      let cnt = Bdb.copy_from_cursor sdb cur tdb max in
      match cnt with
        | 0 -> (i, c)
        | n -> loop (i + 1) (c + n)
    in
    loop 0 0
  in
  Bdb.with_cursor db1 (fun sdb cur ->
    ignore (Bdb.first sdb cur);
    let (iters, cnt) = test sdb cur db2 (Some 11) in
    OUnit.assert_equal ~printer:string_of_int 10 iters;
    OUnit.assert_equal ~printer:string_of_int 100 cnt)

let test_copy_from_cursor_6 db1 db2 =
  _fill db1 100;

  let cnt = Bdb.with_cursor db1 (fun sdb cur ->
    ignore (Bdb.first sdb cur);
    Bdb.copy_from_cursor sdb cur db2 None) in
  OUnit.assert_equal ~printer:string_of_int 100 cnt;
  let r1 = Bdb.range db1 "k" true (next_prefix "k") true (-1) in
  let r2 = Bdb.range db2 "k" true (next_prefix "k") true (-1) in
  OUnit.assert_equal ~printer:string_of_int 100 (Array.length r1);
  OUnit.assert_equal r1 r2

let test_copy_from_cursor_7 db1 db2 =
  _fill db1 10;
  _test_copy_from_cursor db1 db2 (Some 10) 10

let suite =
  let wrap f = bracket (setup_tc "0") f (teardown_tc "0") in
  let wrap2 f = bracket
                  (setup_tc "0")
                  (fun db0 ->
                    bracket
                      (setup_tc "1")
                      (f db0)
                      (teardown_tc "1")
                      ())
                  (teardown_tc "0")
  in
  "Otc" >:::
    [
      "make" >:: test_make;
      "open" >:: wrap test_open;
      "basic" >:: wrap test_basic;
      "cursor" >:: wrap test_cursor;
      "range" >:: wrap test_range;
      "unknown" >:: wrap test_unknown;
      "prefix_keys" >:: wrap test_prefix_keys;
      "null" >:: wrap test_null;
      "flags" >:: wrap test_flags;
      "copy_from_cursor" >::: [
        "0" >:: wrap2 test_copy_from_cursor_0;
        "1" >:: wrap2 test_copy_from_cursor_1;
        "2" >:: wrap2 test_copy_from_cursor_2;
        "3" >:: wrap2 test_copy_from_cursor_3;
        "4" >:: wrap2 test_copy_from_cursor_4;
        "5" >:: wrap2 test_copy_from_cursor_5;
        "6" >:: wrap2 test_copy_from_cursor_6;
        "7" >:: wrap2 test_copy_from_cursor_7;
      ];
    ]
