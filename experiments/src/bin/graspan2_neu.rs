extern crate timely;
extern crate graph_map;
extern crate differential_dataflow;

#[cfg(feature = "jemalloc")]
extern crate jemallocator;

use std::io::{BufRead, BufReader};
use std::fs::File;

use timely::dataflow::Scope;
use timely::order::Product;

use differential_dataflow::operators::iterate::Variable;

use differential_dataflow::Collection;
use differential_dataflow::input::Input;
use differential_dataflow::operators::*;
use differential_dataflow::operators::arrange::Arrange;
use differential_dataflow::trace::implementations::ord::{OrdValSpine, OrdKeySpine};

#[cfg(feature = "jemalloc")]
#[global_allocator] static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

type Node = u32;
type Time = ();
type Iter = u32;
type Diff = isize;

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
enum Mode {
    Vanilla,
    Opt,
    OptPlus,
}

fn main() {
    let mut args = std::env::args();
    let _ = args.next().unwrap();
    let mode: Mode = match args.next().expect("expected optimize").as_str() {
        "vanilla" => Mode::Vanilla,
        "opt" => Mode::Opt,
        "optplus" => Mode::OptPlus,
        _ => panic!("invalid optimize"),
    };
    let filename: String = args.next().expect("expected filename");
    let zerocopy_workers: usize = args.next().expect("expected zerocopy_workers").parse().expect("invalid zerocopy_workers");
    let bind_cores: bool = args.next().expect("expected bind cores?").parse().expect("invalid bind cores?");

    match mode {
        Mode::Vanilla => unoptimized(filename, zerocopy_workers, bind_cores),
        Mode::Opt => optimized_no_sharing(filename, zerocopy_workers, bind_cores),
        Mode::OptPlus => optimized(filename, zerocopy_workers, bind_cores),
    }
}

fn unoptimized(filename: String, zerocopy_workers: usize, bind_cores: bool) {

    let allocators =
        ::timely::communication::allocator::zero_copy::allocator_process::ProcessBuilder::new_vector(zerocopy_workers);
    timely::execute::execute_from(allocators, Box::new(()), move |worker| {

        let peers = worker.peers();
        let index = worker.index();

        if bind_cores {
            let core_ids = core_affinity::get_core_ids().unwrap();
            core_affinity::set_for_current(core_ids[index % core_ids.len()]);
        }

        let timer = ::std::time::Instant::now();

        let (mut a, mut d) = worker.dataflow::<Time,_,_>(|scope| {

            // let timer = timer.clone();

            let (a_handle, assignment) = scope.new_collection::<_,Diff>();
            let (d_handle, dereference) = scope.new_collection::<_,Diff>();

            let nodes =
            assignment
                .flat_map(|(a,b)| vec![a,b])
                .concat(&dereference.flat_map(|(a,b)| vec![a,b]));

            let dereference = dereference.arrange::<OrdValSpine<_,_,_,_>>();

            let (value_flow, memory_alias, value_alias) =
            scope
                .iterative::<Iter,_,_>(|scope| {

                    let nodes = nodes.enter(scope);
                    let assignment = assignment.enter(scope);
                    let dereference = dereference.enter(scope);

                    let value_flow = Variable::new(scope, Product::new(Default::default(), 1));
                    let memory_alias = Variable::new(scope, Product::new(Default::default(), 1));

                    let value_flow_arranged = value_flow.arrange::<OrdValSpine<_,_,_,_>>();
                    let memory_alias_arranged = memory_alias.arrange::<OrdValSpine<_,_,_,_>>();

                    // VA(a,b) <- VF(x,a),VF(x,b)
                    // VA(a,b) <- VF(x,a),MA(x,y),VF(y,b)
                    let value_alias_next = value_flow_arranged.join_core(&value_flow_arranged, |_,&a,&b| Some((a,b)));
                    let value_alias_next = value_flow_arranged.join_core(&memory_alias_arranged, |_,&a,&b| Some((b,a)))
                                                              .arrange::<OrdValSpine<_,_,_,_>>()
                                                              .join_core(&value_flow_arranged, |_,&a,&b| Some((a,b)))
                                                              .concat(&value_alias_next);

                    // VF(a,a) <-
                    // VF(a,b) <- A(a,x),VF(x,b)
                    // VF(a,b) <- A(a,x),MA(x,y),VF(y,b)
                    let value_flow_next =
                    assignment
                        .map(|(a,b)| (b,a))
                        .arrange::<OrdValSpine<_,_,_,_>>()
                        .join_core(&memory_alias_arranged, |_,&a,&b| Some((b,a)))
                        .concat(&assignment.map(|(a,b)| (b,a)))
                        .arrange::<OrdValSpine<_,_,_,_>>()
                        .join_core(&value_flow_arranged, |_,&a,&b| Some((a,b)))
                        .concat(&nodes.map(|n| (n,n)));

                    let value_flow_next =
                    value_flow_next
                        .arrange::<OrdKeySpine<_,_,_>>()
                        // .distinct_total_core::<Diff>()
                        .distinct_total();
                        //.threshold_semigroup(|_,_,x| if x.is_none() { Some(Present) } else { None });
                        //;

                    // MA(a,b) <- D(x,a),VA(x,y),D(y,b)
                    let memory_alias_next: Collection<_,_,Diff> =
                    value_alias_next
                        .join_core(&dereference, |_x,&y,&a| Some((y,a)))
                        .arrange::<OrdValSpine<_,_,_,_>>()
                        .join_core(&dereference, |_y,&a,&b| Some((a,b)));

                    let memory_alias_next: Collection<_,_,Diff>  =
                    memory_alias_next
                        .arrange::<OrdKeySpine<_,_,_>>()
                        .distinct_total();
                        // .distinct_total_core::<Diff>()
                        //.threshold_semigroup(|_,_,x| if x.is_none() { Some(Present) } else { None });
                        //;

                    value_flow.set(&value_flow_next);
                    memory_alias.set(&memory_alias_next);

                    (value_flow_next.leave(), memory_alias_next.leave(), value_alias_next.leave())
                });

                value_flow.map(|_| ()).consolidate().inspect(|x| println!("VF: {:?}", x));
                memory_alias.map(|_| ()).consolidate().inspect(|x| println!("MA: {:?}", x));
                value_alias.map(|_| ()).consolidate().inspect(|x| println!("VA: {:?}", x));

            (a_handle, d_handle)
        });

        if index == 0 { println!("{:?}:\tDataflow assembled", timer.elapsed()); }

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
                        "a" => a.update((src,dst), 1),
                        "d" => d.update((src,dst), 1),
                        _ => { },
                        // x => panic!("Unexpected type: {:?}", x),
                    }
                }
            }
        }

        if index == 0 { println!("{:?}:\tData loaded", timer.elapsed().as_nanos()); }

        a.close();
        d.close();
        while worker.step() { }

        if index == 0 { println!("{:?}:\tComputation complete", timer.elapsed().as_nanos()); }
    }).unwrap();
}

fn optimized(filename: String, zerocopy_workers: usize, bind_cores: bool) {

    let allocators =
        ::timely::communication::allocator::zero_copy::allocator_process::ProcessBuilder::new_vector(zerocopy_workers);
    timely::execute::execute_from(allocators, Box::new(()), move |worker| {

        let peers = worker.peers();
        let index = worker.index();

        if bind_cores {
            let core_ids = core_affinity::get_core_ids().unwrap();
            core_affinity::set_for_current(core_ids[index % core_ids.len()]);
        }

        let timer = ::std::time::Instant::now();

        let (mut a, mut d) = worker.dataflow::<(),_,_>(|scope| {

            // let timer = timer.clone();

            let (a_handle, assignment) = scope.new_collection();
            let (d_handle, dereference) = scope.new_collection();

            let nodes =
            assignment
                .flat_map(|(a,b)| vec![a,b])
                .concat(&dereference.flat_map(|(a,b)| vec![a,b]));

            let dereference = dereference.arrange::<OrdValSpine<_,_,_,_>>();

            let (value_flow, memory_alias) =
            scope
                .iterative::<Iter,_,_>(|scope| {

                    let nodes = nodes.enter(scope);
                    let assignment = assignment.enter(scope);
                    let dereference = dereference.enter(scope);

                    let value_flow = Variable::new(scope, Product::new(Default::default(), 1));
                    let memory_alias = Variable::new(scope, Product::new(Default::default(), 1));

                    let value_flow_arranged = value_flow.arrange::<OrdValSpine<_,_,_,_>>();
                    let memory_alias_arranged = memory_alias.arrange::<OrdValSpine<_,_,_,_>>();

                    // VF(a,a) <-
                    // VF(a,b) <- A(a,x),VF(x,b)
                    // VF(a,b) <- A(a,x),MA(x,y),VF(y,b)
                    let value_flow_next =
                    assignment
                        .map(|(a,b)| (b,a))
                        .arrange::<OrdValSpine<_,_,_,_>>()
                        .join_core(&memory_alias_arranged, |_,&a,&b| Some((b,a)))
                        .concat(&assignment.map(|(a,b)| (b,a)))
                        .arrange::<OrdValSpine<_,_,_,_>>()
                        .join_core(&value_flow_arranged, |_,&a,&b| Some((a,b)))
                        .concat(&nodes.map(|n| (n,n)))
                        .arrange::<OrdKeySpine<_,_,_>>()
                        // .distinct_total_core::<Diff>()
                        .distinct_total();
                        //.threshold_semigroup(|_,_,x| if x.is_none() { Some(Present) } else { None });
                        // ;

                    // VFD(a,b) <- VF(a,x),D(x,b)
                    let value_flow_deref =
                    value_flow
                        .map(|(a,b)| (b,a))
                        .arrange::<OrdValSpine<_,_,_,_>>()
                        .join_core(&dereference, |_x,&a,&b| Some((a,b)))
                        .arrange::<OrdValSpine<_,_,_,_>>();

                    // MA(a,b) <- VFD(x,a),VFD(y,b)
                    // MA(a,b) <- VFD(x,a),MA(x,y),VFD(y,b)
                    let memory_alias_next =
                    value_flow_deref
                        .join_core(&value_flow_deref, |_y,&a,&b| Some((a,b)));

                    let memory_alias_next =
                    memory_alias_arranged
                        .join_core(&value_flow_deref, |_x,&y,&a| Some((y,a)))
                        .arrange::<OrdValSpine<_,_,_,_>>()
                        .join_core(&value_flow_deref, |_y,&a,&b| Some((a,b)))
                        .concat(&memory_alias_next)
                        .arrange::<OrdKeySpine<_,_,_>>()
                        .distinct_total();
                        // .distinct_total_core::<Diff>()
                        //.threshold_semigroup(|_,_,x| if x.is_none() { Some(Present) } else { None });
                        //;

                    value_flow.set(&value_flow_next);
                    memory_alias.set(&memory_alias_next);

                    (value_flow_next.leave(), memory_alias_next.leave())
                });

                value_flow.map(|_| ()).consolidate().inspect(|x| println!("VF: {:?}", x));
                memory_alias.map(|_| ()).consolidate().inspect(|x| println!("MA: {:?}", x));

            (a_handle, d_handle)
        });

        if index == 0 { println!("{:?}:\tDataflow assembled", timer.elapsed()); }

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
                        "a" => { a.update((src, dst), 1); },
                        "d" => { d.update((src, dst), 1); },
                        _ => { },
                        // x => panic!("Unexpected type: {:?}", x),
                    }
                }
            }
        }

        if index == 0 { println!("{:?}:\tData loaded", timer.elapsed().as_nanos()); }

        a.close();
        d.close();
        while worker.step() { }

        if index == 0 { println!("{:?}:\tComputation complete", timer.elapsed().as_nanos()); }

    }).unwrap();
}

fn optimized_no_sharing(filename: String, zerocopy_workers: usize, bind_cores: bool) {

    let allocators =
        ::timely::communication::allocator::zero_copy::allocator_process::ProcessBuilder::new_vector(zerocopy_workers);
    timely::execute::execute_from(allocators, Box::new(()), move |worker| {

        let peers = worker.peers();
        let index = worker.index();

        if bind_cores {
            let core_ids = core_affinity::get_core_ids().unwrap();
            core_affinity::set_for_current(core_ids[index % core_ids.len()]);
        }

        let timer = ::std::time::Instant::now();

        let (mut a, mut d) = worker.dataflow::<(),_,_>(|scope| {

            // let timer = timer.clone();

            let (a_handle, assignment) = scope.new_collection();
            let (d_handle, dereference) = scope.new_collection();

            let nodes =
            assignment
                .flat_map(|(a,b)| vec![a,b])
                .concat(&dereference.flat_map(|(a,b)| vec![a,b]));

            let dereference = dereference.arrange::<OrdValSpine<_,_,_,_>>();

            let (value_flow, memory_alias) =
            scope
                .iterative::<Iter,_,_>(|scope| {

                    let nodes = nodes.enter(scope);
                    let assignment = assignment.enter(scope);
                    let dereference = dereference.enter(scope);

                    let value_flow = Variable::new(scope, Product::new(Default::default(), 1));
                    let memory_alias = Variable::new(scope, Product::new(Default::default(), 1));

                    let (value_flow_next, memory_alias_next) = {
                        let value_flow_arranged = || { value_flow.arrange::<OrdValSpine<_,_,_,_>>() };
                        let memory_alias_arranged = || { memory_alias.arrange::<OrdValSpine<_,_,_,_>>() };

                        // VF(a,a) <-
                        // VF(a,b) <- A(a,x),VF(x,b)
                        // VF(a,b) <- A(a,x),MA(x,y),VF(y,b)
                        let value_flow_next =
                        assignment
                            .map(|(a,b)| (b,a))
                            .arrange::<OrdValSpine<_,_,_,_>>()
                            .join_core(&memory_alias_arranged(), |_,&a,&b| Some((b,a)))
                            .concat(&assignment.map(|(a,b)| (b,a)))
                            .arrange::<OrdValSpine<_,_,_,_>>()
                            .join_core(&value_flow_arranged(), |_,&a,&b| Some((a,b)))
                            .concat(&nodes.map(|n| (n,n)))
                            .arrange::<OrdKeySpine<_,_,_>>()
                            // .distinct_total_core::<Diff>()
                            .distinct_total();
                            //.threshold_semigroup(|_,_,x| if x.is_none() { Some(Present) } else { None });
                            // ;

                        // VFD(a,b) <- VF(a,x),D(x,b)
                        let value_flow_deref_join =
                        value_flow
                            .map(|(a,b)| (b,a))
                            .arrange::<OrdValSpine<_,_,_,_>>()
                            .join_core(&dereference, |_x,&a,&b| Some((a,b)));

                        let value_flow_deref = || { value_flow_deref_join.arrange::<OrdValSpine<_,_,_,_>>() };

                        // MA(a,b) <- VFD(x,a),VFD(y,b)
                        // MA(a,b) <- VFD(x,a),MA(x,y),VFD(y,b)
                        let memory_alias_next =
                        value_flow_deref()
                            .join_core(&value_flow_deref(), |_y,&a,&b| Some((a,b)));

                        let memory_alias_next =
                        memory_alias_arranged()
                            .join_core(&value_flow_deref(), |_x,&y,&a| Some((y,a)))
                            .arrange::<OrdValSpine<_,_,_,_>>()
                            .join_core(&value_flow_deref(), |_y,&a,&b| Some((a,b)))
                            .concat(&memory_alias_next)
                            .arrange::<OrdKeySpine<_,_,_>>()
                            .distinct_total();
                            // .distinct_total_core::<Diff>()
                            //.threshold_semigroup(|_,_,x| if x.is_none() { Some(Present) } else { None });
                            //;
                            //
                        (value_flow_next, memory_alias_next)
                    };

                    value_flow.set(&value_flow_next);
                    memory_alias.set(&memory_alias_next);

                    (value_flow_next.leave(), memory_alias_next.leave())
                });

                value_flow.map(|_| ()).consolidate().inspect(|x| println!("VF: {:?}", x));
                memory_alias.map(|_| ()).consolidate().inspect(|x| println!("MA: {:?}", x));

            (a_handle, d_handle)
        });

        if index == 0 { println!("{:?}:\tDataflow assembled", timer.elapsed()); }

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
                        "a" => { a.update((src, dst), 1); },
                        "d" => { d.update((src, dst), 1); },
                        _ => { },
                        // x => panic!("Unexpected type: {:?}", x),
                    }
                }
            }
        }

        if index == 0 { println!("{:?}:\tData loaded", timer.elapsed().as_nanos()); }

        a.close();
        d.close();
        while worker.step() { }

        if index == 0 { println!("{:?}:\tComputation complete", timer.elapsed().as_nanos()); }

    }).unwrap();
}
