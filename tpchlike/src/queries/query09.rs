use timely::dataflow::*;
use timely::dataflow::operators::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::AsCollection;
use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

use ::Collections;

// -- $ID$
// -- TPC-H/TPC-R Product Type Profit Measure Query (Q9)
// -- Functional Query Definition
// -- Approved February 1998
// :x
// :o
// select
//     nation,
//     o_year,
//     sum(amount) as sum_profit
// from
//     (
//         select
//             n_name as nation,
//             extract(year from o_orderdate) as o_year,
//             l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
//         from
//             part,
//             supplier,
//             lineitem,
//             partsupp,
//             orders,
//             nation
//         where
//             s_suppkey = l_suppkey
//             and ps_suppkey = l_suppkey
//             and ps_partkey = l_partkey
//             and p_partkey = l_partkey
//             and o_orderkey = l_orderkey
//             and s_nationkey = n_nationkey
//             and p_name like '%:1%'
//     ) as profit
// group by
//     nation,
//     o_year
// order by
//     nation,
//     o_year desc;
// :n -1

fn substring(source: &[u8], query: &[u8]) -> bool {
    (0 .. (source.len() - query.len())).any(|offset| 
        (0 .. query.len()).all(|i| source[i + offset] == query[i])
    )
}

pub fn query<G: Scope>(collections: &mut Collections<G>) -> ProbeHandle<G::Timestamp> 
where G::Timestamp: Lattice+Ord {

    let parts =
    collections
        .parts()
        .flat_map(|x| if substring(&x.name.as_bytes(), b"green") { Some(x.part_key) } else { None } );

    collections
        .lineitems()
        .map(|l| (l.part_key, (l.supp_key, l.order_key, l.extended_price * (100 - l.discount) / 100, l.quantity)))
        .semijoin_u(&parts)
        .map(|(part_key, (supp_key, order_key, revenue, quantity))| ((part_key, supp_key), (order_key, revenue, quantity)))
        .join(&collections.partsupps().map(|ps| ((ps.part_key, ps.supp_key), ps.supplycost)))
        .inner
        .map(|(((_part_key, supp_key), (order_key, revenue, quantity), supplycost),t,d)| ((order_key, supp_key), t, ((revenue - supplycost * quantity) as isize) * d))
        .as_collection()
        .join_u(&collections.orders().map(|o| (o.order_key, o.order_date >> 16)))
        .map(|(_, supp_key, order_year)| (supp_key, order_year))
        .join_u(&collections.suppliers().map(|s| (s.supp_key, s.nation_key)))
        .map(|(_, order_year, nation_key)| (nation_key, order_year))
        .join_u(&collections.nations().map(|n| (n.nation_key, n.name)))
        .count()
        .probe()
}