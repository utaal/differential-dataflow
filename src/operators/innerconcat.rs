use timely::Data;
use timely::dataflow::Scope;
use timely::dataflow::operators::*;

use ::Collection;


pub trait InnerConcat<G: Scope, D: Data> {
    fn concat(&self, other: &Collection<G, D>) -> Collection<G, D>;
}

impl<G: Scope, D: Data> InnerConcat<G, D> for Collection<G, D> {
    fn concat(&self, other: &Collection<G, D>) -> Collection<G, D> {
        Collection {
            inner: self.inner.concat(&other.inner)
        }
    }
}
