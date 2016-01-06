use timely::Data;
use timely::dataflow::Scope;
use timely::dataflow::operators::*;

use ::Collection;

pub trait InnerFilter<G: Scope, D: Data> {
    fn filter<L: Fn(&D) -> bool + 'static>(&self, logic: L) -> Collection<G, D>;
}

impl<G: Scope, D: Data> InnerFilter<G, D> for Collection<G, D> {
    fn filter<L: Fn(&D) -> bool + 'static>(&self, logic: L) -> Collection<G, D> {
        Collection {
            inner: self.inner.filter(move |&(ref data, _)| logic(data))
        }
    }
}
