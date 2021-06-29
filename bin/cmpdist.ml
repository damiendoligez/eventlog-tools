open Rresult.R.Infix

module Events = struct

  type t = {
    h : (Eventlog.phase, int) Hashtbl.t;
      (* temporary table holding the entry event to track an event's lifetime *)
    mutable last_flush : (int * int); (* timestamp * duration *)
    events : (Eventlog.phase, int list) Hashtbl.t;
  }

  (* note: when computing an event duration, we check if the last flush
     event happened inbetween. if so, we deduce the duration of the
     flush event from this event. *)
  let update t name v_start v_end =
    match Hashtbl.find_opt t.events name with
    | Some l ->
      let last_flush_ts = fst t.last_flush in
      let v =
        if (v_start < last_flush_ts) && (last_flush_ts < v_end) then
          (v_end - v_start) - (snd t.last_flush)
        else
          (v_end - v_start)
      in
      Hashtbl.replace t.events name (v::l)
    | None -> Hashtbl.add t.events name [v_end - v_start]

  let handle_exit ({ h; _ } as t) name v =
    match Hashtbl.find_opt h name with
    | Some v' -> if v' > v then assert false else
        begin
          Hashtbl.remove h name;
          update t name v' v
        end;
    | None -> failwith "no attached entry event"

  let handle_entry { h;_ } name v =
    match Hashtbl.find_opt h name with
    | Some _ -> failwith "overlapping entry events"
    | None -> Hashtbl.add h name v

  let handle_flush t ts dur = t.last_flush <- ts, dur

  let create () = {
    h = Hashtbl.create 12;
    events = Hashtbl.create 12;
    last_flush = (0, 0);
  }

  let get { events; _ } name = Hashtbl.find events name

  let iter t f =
    let l = Hashtbl.fold (fun x y acc -> (x, y) :: acc) t.events [] in
    let cmp (x1, _) (x2, _) =
      Stdlib.compare (Eventlog.string_of_phase x1) (Eventlog.string_of_phase x2)
    in
    List.iter (fun (x, y) -> f x y) (List.sort cmp l)

end

type allocs = (Eventlog.bucket, int) Hashtbl.t
type counters = (Eventlog.counter_kind, int list) Hashtbl.t

type t = {
  events : Events.t;
  allocs : allocs;
  counters : counters;
  mutable flushs : int list;
}

let read_event
      { Eventlog.payload; timestamp; _ } ({ allocs; events; counters; _ } as t)
  =
  match payload with
  | Alloc { bucket; count; } -> begin
    match Hashtbl.find_opt allocs bucket with
    | Some v -> Hashtbl.replace allocs bucket (v + count)
    | _ -> Hashtbl.add allocs bucket count
  end
  | Entry {phase; } -> Events.handle_entry events phase timestamp
  | Exit {phase; } -> Events.handle_exit events phase timestamp
  | Counter { kind; count; } -> begin
     match Hashtbl.find_opt counters kind with
     | Some l -> Hashtbl.replace counters kind (count::l)
     | None -> Hashtbl.add counters kind [count]
  end
  | Flush {duration; } ->
    t.flushs <- duration::t.flushs;
    Events.handle_flush events timestamp duration


(* pretty-printing and output *)

(* borrowed from https://github.com/ocaml/ocaml/blob/trunk/utils/misc.ml#L780 *)
let pp_two_columns ?max_lines ppf lines =
  let left_column_size =
    List.fold_left (fun acc (s, _) -> max acc (String.length s)) 0 lines in
  let lines_nb = List.length lines in
  let ellipsed_first, ellipsed_last =
    match max_lines with
    | Some max_lines when lines_nb > max_lines ->
        let printed_lines = max_lines - 1 in (* the ellipsis uses one line *)
        let lines_before = printed_lines / 2 + printed_lines mod 2 in
        let lines_after = printed_lines / 2 in
        (lines_before, lines_nb - lines_after - 1)
    | _ -> (-1, -1)
  in
  Format.fprintf ppf "@[<v>";
  List.iteri (fun k (line_l, line_r) ->
    if k = ellipsed_first then Format.fprintf ppf "...@,";
    if ellipsed_first <= k && k <= ellipsed_last then ()
    else Format.fprintf ppf "%*s: %d@," left_column_size line_l line_r
  ) lines;
  Format.fprintf ppf "@]";
  Format.pp_print_newline ppf ()

let print_allocs allocs =
  print_endline "==== allocs";
  let l =
    Hashtbl.fold begin fun bucket count acc ->
      (Printf.sprintf "%-11s" (Eventlog.string_of_alloc_bucket bucket), count)
      :: acc
    end allocs []
  in
  pp_two_columns Format.std_formatter (List.sort Stdlib.compare l)

let print3 suffix x =
  let s = Printf.sprintf "%3.1f" x in
  if String.length s > 3 then
    Printf.sprintf "%3.0f%s" x suffix
  else
    s ^ suffix

let pprint_time ns =
  if ns < 1000. then
    print3 "ns" ns
  else if ns < (1_000_000.) then
    print3 "us" (ns /. 1_000.)
  else if ns < (1_000_000_000.) then
    print3 "ms" (ns /. 1_000_000.)
  else
    print3 "s " (ns /. 1_000_000_000.)

let pprint_quantity q =
  if q < 1000. then
    print3 " " q
  else if q < (1_000_000.) then
    print3 "k" (q /. 1_000.)
  else
    print3 "M" (q /. 1_000_000.)

let bins mul =
  Array.map (( *. ) mul) [| 100.; 147.; 215.; 316.; 464.; 681. |]

let default_bins = Array.concat [
    [| 0. |];
    bins 1.;
    bins 10.;
    bins 100.;
    bins 1000.;
    bins 10000.;
    bins 100000.;
    bins 1000000.;
  ]

let make_bins max =
  let max_in_default_bins = Array.get default_bins (Array.length default_bins - 1) in
  let bins = Array.concat [
      default_bins;
    if max > max_in_default_bins then [|max|] else [||];
  ]
  in
  `Bins bins

let print_histogram name l pprint =
  let open Owl_base_stats in
  Printf.printf "==== %s\n" name;
  let arr = l |> Array.of_list |> Array.map float_of_int in
  let bins = make_bins (max arr) in
  let h = histogram bins arr in
  let l = ref [] in
  for i = 0 to (Array.length h.bins - 2) do
    if h.counts.(i) > 0 then
          l := (Printf.sprintf "%-4s..%4s" (pprint h.bins.(i)) (pprint h.bins.(i + 1)),
                h.counts.(i))::!l
  done;
  pp_two_columns Format.std_formatter !l

let print_events_stats name events =
  match Events.get events name with
  | exception Not_found -> ()
  | events -> print_histogram (Eventlog.string_of_phase name) events pprint_time

let print_flushes flushs =
  let a = Array.of_list flushs |> Array.map float_of_int in
  let median = Owl_base_stats.median a in
  let total = Owl_base_stats.sum a in
  Printf.printf "==== eventlog/flush\n";
  Printf.printf "median flush time: %s\n" (pprint_time median);
  Printf.printf "total flush time: %s\n" (pprint_time total);
  Printf.printf "flush count: %d\n" (List.length flushs)

let make_histogram l =
  let open Owl_base_stats in
  let arr = l |> Array.of_list |> Array.map float_of_int in
  let bins = make_bins (max arr) in
  histogram bins arr

let compare_distribs l1 l2 =
  let d1 = 1. /. (Float.of_int (List.length l1)) in
  let d2 = -1. /. (Float.of_int (List.length l2)) in
  let h1 = make_histogram l1 in
  let h2 = make_histogram l2 in
  let l1 = Array.to_list h1.counts in
  let l2 = Array.to_list h2.counts in
  let rec f l1 l2 lo hi cur =
    let step delta ll1 ll2 =
      let cur = cur +. delta in
      f ll1 ll2 (Float.min lo cur) (Float.max hi cur) cur
    in
    match l1, l2 with
    | [], h2 :: t2 -> step (Float.of_int h2 *. d2) [] t2
    | h1 :: t1, [] -> step (Float.of_int h1 *. d1) t1 []
    | h1 :: t1, h2 :: t2 ->
      step (Float.of_int h1 *. d1 +. Float.of_int h2 *. d2) t1 t2
    | [], [] -> (lo, hi)
  in
  f l1 l2 0. 0. 0.

let load_file path =
  let open Rresult.R.Infix in
  Fpath.of_string path
  >>= Bos.OS.File.read
  >>= fun content ->
  Ok (Bigstringaf.of_string ~off:0 ~len:(String.length content) content)

let main in_file numeric =
  let module P = Eventlog.Parser in
  load_file in_file >>= fun data ->
  let decoder = P.decoder () in
  let total_len = Bigstringaf.length data in
  P.src decoder data 0 total_len true;
  let allocs = Hashtbl.create 10 in
  let events = Events.create () in
  let counters = Hashtbl.create 10 in
  let t = { allocs; events; flushs = []; counters; } in
  let rec aux () =
    match P.decode decoder with
    | `Ok Event ev ->
      read_event ev t;
      aux ()
    | `Ok Header _ -> aux ()
    | `Error (`Msg msg) ->
      Printf.eprintf "some events were discarded: %s" msg;
      Ok ()
    | `End -> Ok ()
    | `Await -> Ok ()
  in
  aux () >>= fun () ->
  let marks = ref [] and sweeps = ref [] in
  let f phase l =
    match Eventlog.string_of_phase phase with
    | "major/mark" -> marks := l
    | "major/sweep" -> sweeps := l
    | _ -> ()
  in
  Events.iter events f;
  let (lo, hi) = compare_distribs !marks !sweeps in
  if numeric then
    Printf.printf "%f\n" (Float.max (-. lo) hi)
  else
    Printf.printf "ECDF distance min=%f max=%f\n" lo hi
  ;
  Ok ()

module Args = struct
  open Cmdliner

  let trace =
    let doc = "Print a basic report from an OCaml eventlog file" in
    Arg.(required & pos 0 (some string) None  & info [] ~doc )

  let numeric =
    let doc = "output a single number" in
    Arg.(value & flag & info ["v"] ~doc)

  let info =
    let doc = "" in
    let man = [
      `S Manpage.s_bugs;
    ]
    in
    Term.info "ocaml-eventlog-report" ~version:"%%VERSION%%" ~doc
      ~exits:Term.default_exits ~man

end

let () =
  let open Cmdliner in
  Term.exit @@ Term.eval Term.(const main $ Args.trace $ Args.numeric, Args.info)
