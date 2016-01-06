use timely::Data;
use timely::dataflow::Scope;
use timely::dataflow::operators::*;

use ::Collection;

pub trait InnerMap<G: Scope, D: Data> {
    fn map<D2: Data, L: Fn(D) -> D2 + 'static>(&self, logic: L) -> Collection<G, D2>;
    fn map_in_place<L: Fn(&mut D) + 'static>(&self, logic: L) -> Collection<G, D>;
}

impl<G: Scope, D: Data> InnerMap<G, D> for Collection<G, D> {
    fn map<D2: Data, L: Fn(D) -> D2 + 'static>(&self, logic: L) -> Collection<G, D2> {
        Collection {
            inner: self.inner.map(move |(data, delta)| (logic(data), delta))
        }
    }

    fn map_in_place<L: Fn(&mut D) + 'static>(&self, logic: L) -> Collection<G, D> {
        Collection {
            inner: self.inner.map_in_place(move |&mut (ref mut data, _)| logic(data))
        }
    }
}
