//! Wrappers to transform the timestamps of updates.
//!
//! These wrappers are primarily intended to support the re-use of a multi-version index
//! as if it were frozen at a particular (nested) timestamp. For example, if one wants to
//! re-use an index multiple times with minor edits, and only observe the edits at one
//! logical time (meaning: observing all edits less or equal to that time, advanced to that
//! time), this should allow that behavior.
//!
//! Informally, this wrapper is parameterized by a function `F: Fn(&T)->Option<T>` which 
//! provides the opportunity to alter the time at which an update happens and to suppress
//! that update, if appropriate. For example, the function
//!
//! ```ignore
//! |t| if t.inner <= 10 { let mut t = t.clone(); t.inner = 10; Some(t) } else { None }
//! ```
//!
//! could be used to present all updates through inner iteration 10, but advanced to inner
//! iteration 10, as if they all occurred exactly at that moment.

use std::rc::Rc;

use timely::dataflow::Scope;
use timely::dataflow::operators::Map;

use operators::arrange::{Arranged, BatchWrapper};
use lattice::Lattice;
use trace::{TraceReader, BatchReader, Description};
use trace::cursor::Cursor;
use trace::BatchIdentifier;

/// Freezes updates to an arrangement using a supplied function.
///
/// This method is experimental, and should be used with care. The intent is that the function
/// `func` can be used to restrict and lock in updates at a particular time, as suggested in the
/// module-level documentation.
pub fn freeze<G, K, V, R, T, F>(arranged: &Arranged<G, K, V, R, T>, func: F) -> Arranged<G, K, V, R, TraceFreeze<K, V, G::Timestamp, R, T, F>>
where
    G: Scope,
    G::Timestamp: Lattice+Ord,
    K: 'static,
    V: 'static,
    R: 'static,
    T: TraceReader<K, V, G::Timestamp, R>+Clone,
    F: Fn(&G::Timestamp)->Option<G::Timestamp>+'static,
{
    let func1 = Rc::new(func);
    let func2 = func1.clone();
    Arranged {
        stream: arranged.stream.map(move |bw| BatchWrapper { item: BatchFreeze::make_from(bw.item, func1.clone()) }),
        trace: TraceFreeze::make_from(arranged.trace.clone(), func2),
    }
}

/// Wrapper to provide trace to nested scope.
pub struct TraceFreeze<K, V, T, R, Tr, F> 
where
    T: Lattice+Clone+'static,
    Tr: TraceReader<K, V, T, R>,
    F: Fn(&T)->Option<T>,
{
    phantom: ::std::marker::PhantomData<(K, V, R, T)>,
    trace: Tr,
    func: Rc<F>,
}

impl<K,V,T,R,Tr,F> Clone for TraceFreeze<K, V, T, R, Tr, F> 
where 
    T: Lattice+Clone+'static,
    Tr: TraceReader<K, V, T, R>+Clone,
    F: Fn(&T)->Option<T>,
{
    fn clone(&self) -> Self {
        TraceFreeze {
            phantom: ::std::marker::PhantomData,
            trace: self.trace.clone(),
            func: self.func.clone(),
        }
    }
}

impl<K, V, T, R, Tr, F> TraceReader<K, V, T, R> for TraceFreeze<K, V, T, R, Tr, F>
where
    Tr: TraceReader<K, V, T, R>,
    Tr::Batch: Clone,
    K: 'static,
    V: 'static,
    T: Lattice+Clone+Default+'static,
    R: 'static,
    F: Fn(&T)->Option<T>+'static,
{
    type Batch = BatchFreeze<K, V, T, R, Tr::Batch, F>;
    type Cursor = CursorFreeze<K, V, T, R, Tr::Cursor, F>;

    fn map_batches<F2: FnMut(&Self::Batch)>(&mut self, mut f: F2) { 
        let func = &self.func;
        self.trace.map_batches(|batch| {
            f(&Self::Batch::make_from(batch.clone(), func.clone()));
        })
    }

    fn advance_by(&mut self, frontier: &[T]) { self.trace.advance_by(frontier) }
    fn advance_frontier(&mut self) -> &[T] { self.trace.advance_frontier() }

    fn distinguish_since(&mut self, frontier: &[T]) { self.trace.distinguish_since(frontier) }
    fn distinguish_frontier(&mut self) -> &[T] { self.trace.distinguish_frontier() }

    fn cursor_through(&mut self, upper: &[T]) -> Option<(Self::Cursor, <Self::Cursor as Cursor<K, V, T, R>>::Storage)> {
        let func = &self.func;
        self.trace.cursor_through(upper)
            .map(|(cursor, storage)| (CursorFreeze::new(cursor, func.clone()), storage))
    }
}

impl<K, V, T, R, Tr, F> TraceFreeze<K, V, T, R, Tr, F>
where
    Tr: TraceReader<K, V, T, R>,
    Tr::Batch: Clone,
    K: 'static,
    V: 'static,
    T: Lattice+Clone+Default+'static,
    R: 'static,
    F: Fn(&T)->Option<T>,
{
    /// Makes a new trace wrapper
    pub fn make_from(trace: Tr, func: Rc<F>) -> Self {
        TraceFreeze {
            phantom: ::std::marker::PhantomData,
            trace: trace,
            func: func,
        }
    }
}


/// Wrapper to provide batch to nested scope.
pub struct BatchFreeze<K, V, T, R, B, F> {
    phantom: ::std::marker::PhantomData<(K, V, R, T)>,
    batch: B,
    func: Rc<F>,
}

impl<K, V, T: Clone, R, B: Clone, F> Clone for BatchFreeze<K, V, T, R, B, F> {
    fn clone(&self) -> Self { 
        BatchFreeze {
            phantom: ::std::marker::PhantomData,
            batch: self.batch.clone(),
            func: self.func.clone(),
        }
    }
}

impl<K, V, T, R, B, F> BatchReader<K, V, T, R> for BatchFreeze<K, V, T, R, B, F> 
where
    B: BatchReader<K, V, T, R>,
    T: Clone+Default,
    F: Fn(&T)->Option<T>,
{
    type Cursor = BatchCursorFreeze<K, V, T, R, B, F>;

    fn cursor(&self) -> Self::Cursor { 
        BatchCursorFreeze::new(self.batch.cursor(), self.func.clone())
    }
    fn len(&self) -> usize { self.batch.len() }
    fn description(&self) -> &Description<T> { self.batch.description() }
    fn identifier(&self) -> &BatchIdentifier { self.batch.identifier() }
}

impl<K, V, T, R, B, F> BatchFreeze<K, V, T, R, B, F>
where
    B: BatchReader<K, V, T, R>,
    T: Clone,
    F: Fn(&T)->Option<T>
{
    /// Makes a new batch wrapper
    pub fn make_from(batch: B, func: Rc<F>) -> Self {
        BatchFreeze {
            phantom: ::std::marker::PhantomData,
            batch: batch,
            func: func,
        }
    }
}

/// Wrapper to provide cursor to nested scope.
pub struct CursorFreeze<K, V, T, R, C: Cursor<K, V, T, R>, F> {
    phantom: ::std::marker::PhantomData<(K, V, R, T)>,
    cursor: C,
    func: Rc<F>,
}

impl<K, V, T, R, C: Cursor<K, V, T, R>, F> CursorFreeze<K, V, T, R, C, F> {
    fn new(cursor: C, func: Rc<F>) -> Self {
        CursorFreeze {
            phantom: ::std::marker::PhantomData,
            cursor: cursor,
            func: func,
        }
    }
}

impl<K, V, T, R, C, F> Cursor<K, V, T, R> for CursorFreeze<K, V, T, R, C, F> 
where
    C: Cursor<K, V, T, R>,
    F: Fn(&T)->Option<T>,
{

    type Storage = C::Storage;

    #[inline(always)] fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.key_valid(storage) }
    #[inline(always)] fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.val_valid(storage) }

    #[inline(always)] fn key<'a>(&self, storage: &'a Self::Storage) -> &'a K { self.cursor.key(storage) }
    #[inline(always)] fn val<'a>(&self, storage: &'a Self::Storage) -> &'a V { self.cursor.val(storage) }

    #[inline(always)] fn map_times<L: FnMut(&T, R)>(&mut self, storage: &Self::Storage, mut logic: L) {
        let func = &self.func;
        self.cursor.map_times(storage, |time, diff| {
            if let Some(time) = func(time) {
                logic(&time, diff);
            }
        })
    }

    #[inline(always)] fn step_key(&mut self, storage: &Self::Storage) { self.cursor.step_key(storage) }
    #[inline(always)] fn seek_key(&mut self, storage: &Self::Storage, key: &K) { self.cursor.seek_key(storage, key) }
    
    #[inline(always)] fn step_val(&mut self, storage: &Self::Storage) { self.cursor.step_val(storage) }
    #[inline(always)] fn seek_val(&mut self, storage: &Self::Storage, val: &V) { self.cursor.seek_val(storage, val) }

    #[inline(always)] fn rewind_keys(&mut self, storage: &Self::Storage) { self.cursor.rewind_keys(storage) }
    #[inline(always)] fn rewind_vals(&mut self, storage: &Self::Storage) { self.cursor.rewind_vals(storage) }
}


/// Wrapper to provide cursor to nested scope.
pub struct BatchCursorFreeze<K, V, T, R, B: BatchReader<K, V, T, R>, F> {
    phantom: ::std::marker::PhantomData<(K, V, R, T)>,
    cursor: B::Cursor,
    func: Rc<F>,
}

impl<K, V, T, R, B: BatchReader<K, V, T, R>, F> BatchCursorFreeze<K, V, T, R, B, F> {
    fn new(cursor: B::Cursor, func: Rc<F>) -> Self {
        BatchCursorFreeze {
            phantom: ::std::marker::PhantomData,
            cursor: cursor,
            func: func,
        }
    }
}

impl<K, V, T, R, B: BatchReader<K, V, T, R>, F> Cursor<K, V, T, R> for BatchCursorFreeze<K, V, T, R, B, F> 
where
    F: Fn(&T)->Option<T>,
{

    type Storage = BatchFreeze<K, V, T, R, B, F>;

    #[inline(always)] fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.key_valid(&storage.batch) }
    #[inline(always)] fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.val_valid(&storage.batch) }

    #[inline(always)] fn key<'a>(&self, storage: &'a Self::Storage) -> &'a K { self.cursor.key(&storage.batch) }
    #[inline(always)] fn val<'a>(&self, storage: &'a Self::Storage) -> &'a V { self.cursor.val(&storage.batch) }

    #[inline(always)] fn map_times<L: FnMut(&T, R)>(&mut self, storage: &Self::Storage, mut logic: L) {
        let func = &self.func;
        self.cursor.map_times(&storage.batch, |time, diff| {
            if let Some(time) = func(time) {
                logic(&time, diff);
            }
        })
    }

    #[inline(always)] fn step_key(&mut self, storage: &Self::Storage) { self.cursor.step_key(&storage.batch) }
    #[inline(always)] fn seek_key(&mut self, storage: &Self::Storage, key: &K) { self.cursor.seek_key(&storage.batch, key) }
    
    #[inline(always)] fn step_val(&mut self, storage: &Self::Storage) { self.cursor.step_val(&storage.batch) }
    #[inline(always)] fn seek_val(&mut self, storage: &Self::Storage, val: &V) { self.cursor.seek_val(&storage.batch, val) }

    #[inline(always)] fn rewind_keys(&mut self, storage: &Self::Storage) { self.cursor.rewind_keys(&storage.batch) }
    #[inline(always)] fn rewind_vals(&mut self, storage: &Self::Storage) { self.cursor.rewind_vals(&storage.batch) }
}