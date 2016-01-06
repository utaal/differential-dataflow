use timely::Data;
use timely::progress::Timestamp;
use timely::dataflow::Scope;
use timely::dataflow::operators::*;

use ::Delta;
use ::Collection;

pub trait InnerInspect<G: Scope, D: Data> {
    fn inspect<F: FnMut(&(D, Delta))+'static>(&self, func: F) -> Collection<G, D>;

    fn inspect_batch<F: FnMut(&G::Timestamp, &[(D, Delta)])+'static>(&self, func: F) -> Collection<G, D>;
}

impl<G: Scope, D: Data> InnerInspect<G, D> for Collection<G, D> {
    fn inspect<F: FnMut(&(D, Delta))+'static>(&self, func: F) -> Collection<G, D> {
        Collection {
            inner: self.inner.inspect(func)
        }
    }

    fn inspect_batch<F: FnMut(&G::Timestamp, &[(D, Delta)])+'static>(&self, func: F) -> Collection<G, D> {
        Collection {
            inner: self.inner.inspect_batch(func)
        }
    }
}
