use timely::Data;
use timely::progress::Timestamp;
use timely::dataflow::Scope;
use timely::dataflow::operators::*;

use ::Collection;

pub trait InnerProbe<G: Scope, D: Data> {
    fn probe(&self) -> (probe::Handle<G::Timestamp>, Collection<G, D>);
}

impl<G: Scope, D: Data> InnerProbe<G, D> for Collection<G, D> {
    fn probe(&self) -> (probe::Handle<G::Timestamp>, Collection<G, D>) {
        let (handle, stream) = self.inner.probe();
        (handle, Collection {
            inner: stream
        })
    }
}

