extern crate timely;
extern crate graph_map;
extern crate differential_dataflow;

#[cfg(feature = "jemalloc")]
extern crate jemallocator;

use std::io::{BufRead, BufReader};
use std::fs::File;

use timely::dataflow::Scope;
use timely::order::Product;

use differential_dataflow::input::Input;
use differential_dataflow::trace::implementations::ord::OrdValSpine;
use differential_dataflow::operators::*;
use differential_dataflow::operators::arrange::Arrange;
use differential_dataflow::operators::iterate::Variable;

#[cfg(feature = "jemalloc")]
#[global_allocator] static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

type Node = u32;
type Time = ();
type Iter = u32;

fn main() {

    let mut args = std::env::args();
    let _ = args.next().unwrap();
    let filename: String = args.next().expect("expected filename");
    let zerocopy_workers: usize = args.next().expect("expected zerocopy_workers").parse().expect("invalid zerocopy_workers");
    let bind_cores: bool = args.next().expect("expected bind cores?").parse().expect("invalid bind cores?");

    let allocators =
        ::timely::communication::allocator::zero_copy::allocator_process::ProcessBuilder::new_vector(zerocopy_workers);
    timely::execute::execute_from(allocators, Box::new(()), move |worker| {

        let timer = ::std::time::Instant::now();

        let peers = worker.peers();
        let index = worker.index();

        if bind_cores {
            let core_ids = core_affinity::get_core_ids().unwrap();
            core_affinity::set_for_current(core_ids[index % core_ids.len()]);
        }

        let (mut nodes, mut edges) = worker.dataflow::<Time,_,_>(|scope| {

            // let timer = timer.clone();

            let (n_handle, nodes) = scope.new_collection();
            let (e_handle, edges) = scope.new_collection();

            let edges = edges.arrange::<OrdValSpine<_,_,_,_>>();

            // a N c  <-  a N b && b E c
            // N(a,c) <-  N(a,b), E(b, c)
            let reached =
            nodes.scope().iterative::<Iter,_,_>(|inner| {

                let nodes = nodes.enter(inner).map(|(a,b)| (b,a));
                let edges = edges.enter(inner);

                let labels = Variable::new(inner, Product::new(Default::default(), 1));

                let next =
                labels.join_core(&edges, |_b, a, c| Some((*c, *a)))
                      .concat(&nodes)
                      .arrange::<OrdValSpine<_,_,_,_>>()
                      .distinct_total();

                labels.set(&next);
                next.leave()
            });

            reached
                .map(|_| ())
                .consolidate()
                .inspect(|x| println!("{:?}", x))
                ;

            (n_handle, e_handle)
        });

        if index == 0 { println!("{:?}:\tDataflow assembled", timer.elapsed()); }

        // snag a filename to use for the input graph.
        let file = BufReader::new(File::open(filename.clone()).unwrap());
        for readline in file.lines() {
            let line = readline.ok().expect("read error");
            if !line.starts_with('#') && line.len() > 0 {
                let mut elts = line[..].split_whitespace();
                let src: Node = elts.next().unwrap().parse().ok().expect("malformed src");
                if (src as usize) % peers == index {
                    let dst: Node = elts.next().unwrap().parse().ok().expect("malformed dst");
                    let typ: &str = elts.next().unwrap();
                    match typ {
                        "n" => { nodes.update((src, dst), 1isize); },
                        "e" => { edges.update((src, dst), 1isize); },
                        unk => { panic!("unknown type: {}", unk)},
                    }
                }
            }
        }

        if index == 0 { println!("{:?}:\tData loaded", timer.elapsed()); }

        nodes.close();
        edges.close();
        while worker.step() { }

        if index == 0 {
            let elapsed = timer.elapsed();
            println!("{:?}:\tComputation complete", elapsed);
            println!("[result]\t{}\tms", elapsed.as_millis());
        }

    }).unwrap();
}
