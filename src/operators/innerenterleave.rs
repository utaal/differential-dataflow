use std::hash::Hash;

use timely::Data;
use timely::progress::Timestamp;
use timely::dataflow::scopes::Child;
use timely::dataflow::Scope;
use timely::dataflow::operators::*;

use ::Delta;
use ::Collection;

pub trait InnerEnter<G: Scope, D: Data> {
    fn enter<T: Timestamp>(&self, child: &Child<G, T>) -> Collection<Child<G, T>, D>;
    fn enter_at<T: Timestamp, F: Fn(&(D, Delta)) -> T + 'static>(&self, child: &Child<G, T>, initial: F) -> Collection<Child<G, T>, D> where G::Timestamp: Hash, T: Hash;
}

impl<G: Scope, D: Data> InnerEnter<G, D> for Collection<G, D> {
    fn enter<T: Timestamp>(&self, child: &Child<G, T>) -> Collection<Child<G, T>, D> {
        Collection {
            inner: self.inner.enter(child)
        }
    }

    fn enter_at<T: Timestamp, F: Fn(&(D, Delta)) -> T + 'static>(&self, child: &Child<G, T>, initial: F) -> Collection<Child<G, T>, D> where G::Timestamp: Hash, T: Hash {
        Collection {
            inner: self.inner.enter_at(child, initial)
        }
    }
}

pub trait InnerLeave<G: Scope, D: Data> {
    fn leave(&self) -> Collection<G, D>;
}

impl<G: Scope, T: Timestamp, D: Data> InnerLeave<G, D> for Collection<Child<G, T>, D> {
    fn leave(&self) -> Collection<G, D> {
        Collection {
            inner: self.inner.leave()
        }
    }
}
