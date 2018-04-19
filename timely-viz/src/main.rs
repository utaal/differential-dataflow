extern crate timely;
extern crate abomonation;
#[macro_use] extern crate abomonation_derive;
extern crate differential_dataflow;

use std::hash::{Hash, Hasher};
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::net::TcpListener;

use timely::dataflow::operators::{Delay, Inspect, Map, Filter};
// use timely::dataflow::operators::aggregation::Aggregate;
use timely::dataflow::operators::capture::{EventReader, Replay};
// use timely::dataflow::operators::generic::Unary;
// use timely::dataflow::channels::pact::Exchange;
use timely::logging::{StartStop, TimelyEvent, TimelySetup, ChannelsEvent, OperatesEvent};
use timely::progress::timestamp::RootTimestamp;

use differential_dataflow::operators::*;
use differential_dataflow::Collection;

fn main() {
    println!("digraph G {{");
    timely::execute_from_args(std::env::args(), |worker| {
        let mut args = std::env::args();
        args.next();

        let source_peers = args.next().unwrap().parse::<usize>().unwrap();
        // The log server
        let server: String = args.next().unwrap();

        // create replayers from disjoint partition of source worker identifiers.
        let replayers =
        (0 .. source_peers)
            .map(|i| {
                let addr = format!("{}:{}", server, 2102 + i);
                eprintln!("Listening to {}",addr);
                TcpListener::bind(addr).unwrap()})
            .collect::<Vec<_>>()
            .into_iter()
            .map(|l| l.incoming().next().unwrap().unwrap())
            .map(|r| EventReader::<_,(u64, TimelySetup, TimelyEvent),_>::new(r))
            .collect::<Vec<_>>();

        worker.dataflow::<u64,_,_>(|scope| {

            let all = replayers
                .replay_into(scope)
                .inspect_batch(|ts, xs| {
                    for &(ref _ts, ref _setup, ref event) in xs {
                        match event {
                            // print only topology-related logs
                            TimelyEvent::Operates(_) => {eprintln!("raw: {} {:?}", ts.inner, event);},
                            TimelyEvent::Channels(_) => {eprintln!("raw: {} {:?}", ts.inner, event);},
                            _ => {}
                        }
                    }
                });

            let channels = all.filter(|&(ref _ts, ref setup, ref event)|
                setup.index == 0 && match event {
                    TimelyEvent::Channels(c) => true,
                    _ => false,
                }
            ).map(|(ts, setup, event)| match event {
                TimelyEvent::Channels(c) => (RootTimestamp::new(ts), setup, c), // ChannelsEvent { id, scope_addr, source, target }) => (ts, c, 1),
                _ => panic!("unexpected event"),
            });

            let channels_src_coll = Collection::new(
                channels.map(|(ts, _setup, ChannelsEvent {id, mut scope_addr, source, target})| {
                    scope_addr.remove(0);
                    let src = {
                        let mut x = scope_addr.clone();
                        x.push(source.0);
                        x
                    };
                    let dst = {
                        let mut x = scope_addr.clone();
                        x.push(target.0);
                        x
                    };
                    ((src.clone(), (id, src, dst)), ts, 1)
                }));

            let channels_port_coll = Collection::new(channels.map(|(ts, _setup, ChannelsEvent {id, source, target, ..})| {
                ((id, (source.1, target.1)), ts, 1)
            }));

            let operates = all.filter(|&(ref _ts, ref setup, ref event)|
                setup.index == 0 && match event {
                    TimelyEvent::Operates(o) => true,
                    _ => false,
                }
            ).map(|(ts, setup, event)| match event {
                TimelyEvent::Operates(o) => (RootTimestamp::new(ts), setup, o), // ChannelsEvent { id, scope_addr, source, target }) => (ts, c, 1),
                _ => panic!("unexpected event"),
            });

            let operates_coll = Collection::new(
                operates.map(|(ts, _setup, o)| (o, ts, 1)));

            let operates_coll_with_subg = operates_coll.map_in_place(|oe| {
                oe.addr.push(0);
            }).concat(&operates_coll);

            let operates_addr_id_coll = operates_coll_with_subg.map(|OperatesEvent { mut addr, id, .. }| {
                addr.remove(0);
                (addr, id)
            });

            let channels_coll = channels_src_coll.join_map(&operates_addr_id_coll,
                                             |_, &(ref id, _, ref dst), src_id| {
                (dst.clone(), (*id, src_id.clone(), dst.clone()))
            }).join_map(&operates_addr_id_coll,
                        |_, &(ref id, ref src_id, _), dst_id| {
                (*id, (src_id.clone(), dst_id.clone()))
            }).join(&channels_port_coll);

            fn join_with_dashes(v: &Vec<usize>) -> String {
                return (*v).iter().map(|a| format!("{}", a)).collect::<Vec<_>>().join("-");
            }

            let operates_lines = operates_coll.map(|OperatesEvent { addr, id, name }| {
                let addr_str = join_with_dashes(&addr);
                format!("{} [label=\"{} {} at {}\"]", id, id, name, addr_str)
            });

            let channels_lines = channels_coll.map(|(id, (src_id, dst_id), (src_port, dst_port))| {
                format!("{} -> {} [label=\"{} ({} → {})\"]", src_id, dst_id, id, src_port, dst_port)
            });

            let result = operates_lines.concat(&channels_lines).consolidate()
                .inner.filter(|&(_, _, delta)| delta >= 1).map(|(v, _, _)| v);

            result.inspect(move |x| {
                println!("{}\n", x);
                ()
            });

        })
    }).unwrap(); // asserts error-free execution
    println!("}}");
}
