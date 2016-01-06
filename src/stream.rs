use timely::Data;
use timely::dataflow::{Scope, Stream};

use ::Delta;

/// A mutable collection of values of type `D`
pub struct Collection<G: Scope, D: Data> {
    pub inner: Stream<G, (D, Delta)>
}

impl<G: Scope, D: Data> Collection<G, D> {
    pub fn new(inner: Stream<G, (D, Delta)>) -> Collection<G, D> {
        Collection {
            inner: inner
        }
    }

    pub fn scope(&self) -> G {
        self.inner.scope()
    }
}

