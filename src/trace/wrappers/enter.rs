//! Wrappers to provide trace access to nested scopes.

// use timely::progress::nested::product::Product;
use timely::progress::timestamp::Refines;
use timely::progress::Timestamp;

use lattice::Lattice;
use trace::{TraceReader, BatchReader, Description};
use trace::cursor::Cursor;
use trace::BatchIdentifier;

/// Wrapper to provide trace to nested scope.
pub struct TraceEnter<K, V, T, R, Tr, TInner> {
    phantom: ::std::marker::PhantomData<(K, V, R)>,
    trace: Tr,
    stash1: Vec<T>,
    stash2: Vec<TInner>,
}

impl<K,V,T,R,Tr,TInner> Clone for TraceEnter<K, V, T, R, Tr, TInner>
where
    Tr: TraceReader<K, V, T, R>+Clone,
{
    fn clone(&self) -> Self {
        TraceEnter {
            phantom: ::std::marker::PhantomData,
            trace: self.trace.clone(),
            stash1: Vec::new(),
            stash2: Vec::new(),
        }
    }
}

impl<K, V, T, R, Tr, TInner> TraceReader<K, V, TInner, R> for TraceEnter<K, V, T, R, Tr, TInner>
where
    Tr: TraceReader<K, V, T, R>,
    Tr::Batch: Clone,
    K: 'static,
    V: 'static,
    T: Timestamp,
    TInner: Refines<T>+Lattice,
    R: 'static {

    type Batch = BatchEnter<K, V, T, R, Tr::Batch, TInner>;
    type Cursor = CursorEnter<K, V, T, R, Tr::Cursor, TInner>;

    fn map_batches<F: FnMut(&Self::Batch)>(&mut self, mut f: F) {
        self.trace.map_batches(|batch| {
            f(&Self::Batch::make_from(batch.clone()));
        })
    }

    fn advance_by(&mut self, frontier: &[TInner]) {
        self.stash1.clear();
        for time in frontier.iter() {
            self.stash1.push(time.clone().to_outer());
        }
        self.trace.advance_by(&self.stash1[..]);
    }
    fn advance_frontier(&mut self) -> &[TInner] {
        self.stash2.clear();
        for time in self.trace.advance_frontier().iter() {
            self.stash2.push(TInner::to_inner(time.clone()));
        }
        &self.stash2[..]
    }

    fn distinguish_since(&mut self, frontier: &[TInner]) {
        self.stash1.clear();
        for time in frontier.iter() {
            self.stash1.push(time.clone().to_outer());
        }
        self.trace.distinguish_since(&self.stash1[..]);
    }
    fn distinguish_frontier(&mut self) -> &[TInner] {
        self.stash2.clear();
        for time in self.trace.distinguish_frontier().iter() {
            self.stash2.push(TInner::to_inner(time.clone()));
        }
        &self.stash2[..]
    }

    fn cursor_through(&mut self, upper: &[TInner]) -> Option<(Self::Cursor, <Self::Cursor as Cursor<K, V, TInner, R>>::Storage)> {
        self.stash1.clear();
        for time in upper.iter() {
            self.stash1.push(time.clone().to_outer());
        }
        self.trace.cursor_through(&self.stash1[..]).map(|(x,y)| (CursorEnter::new(x), y))
    }
}

impl<K, V, T, R, Tr, TInner> TraceEnter<K, V, T, R, Tr, TInner>
where
    Tr: TraceReader<K, V, T, R>,
    T: Timestamp,
    TInner: Refines<T>+Lattice,
{
    /// Makes a new trace wrapper
    pub fn make_from(trace: Tr) -> Self {
        TraceEnter {
            phantom: ::std::marker::PhantomData,
            trace: trace,
            stash1: Vec::new(),
            stash2: Vec::new(),
        }
    }
}


/// Wrapper to provide batch to nested scope.
pub struct BatchEnter<K, V, T, R, B, TInner> {
    phantom: ::std::marker::PhantomData<(K, V, T, R)>,
    batch: B,
    description: Description<TInner>,
    identifier: BatchIdentifier,
}

impl<K, V, T, R, B: Clone, TInner: Clone> Clone for BatchEnter<K, V, T, R, B, TInner> {
    fn clone(&self) -> Self {
        BatchEnter {
            phantom: ::std::marker::PhantomData,
            batch: self.batch.clone(),
            description: self.description.clone(),
            identifier: self.identifier.clone(),
        }
    }
}

impl<K, V, T, R, B, TInner> BatchReader<K, V, TInner, R> for BatchEnter<K, V, T, R, B, TInner>
where
    B: BatchReader<K, V, T, R>,
    T: Timestamp,
    TInner: Refines<T>+Lattice,
{
    type Cursor = BatchCursorEnter<K, V, T, R, B, TInner>;

    fn cursor(&self) -> Self::Cursor {
        BatchCursorEnter::new(self.batch.cursor())
    }
    fn len(&self) -> usize { self.batch.len() }
    fn description(&self) -> &Description<TInner> { &self.description }
    fn identifier(&self) -> &BatchIdentifier { &self.identifier }
}

impl<K, V, T, R, B, TInner> BatchEnter<K, V, T, R, B, TInner>
where
    B: BatchReader<K, V, T, R>,
    T: Timestamp,
    TInner: Refines<T>+Lattice,
{
    /// Makes a new batch wrapper
    pub fn make_from(batch: B) -> Self {
        let lower: Vec<_> = batch.description().lower().iter().map(|x| TInner::to_inner(x.clone())).collect();
        let upper: Vec<_> = batch.description().upper().iter().map(|x| TInner::to_inner(x.clone())).collect();
        let since: Vec<_> = batch.description().since().iter().map(|x| TInner::to_inner(x.clone())).collect();

        let identifier = batch.identifier().clone();
        BatchEnter {
            phantom: ::std::marker::PhantomData,
            batch: batch,
            description: Description::new(&lower[..], &upper[..], &since[..]),
            identifier: identifier,
        }
    }
}

/// Wrapper to provide cursor to nested scope.
pub struct CursorEnter<K, V, T, R, C: Cursor<K, V, T, R>, TInner> {
    phantom: ::std::marker::PhantomData<(K, V, T, R, TInner)>,
    cursor: C,
}

impl<K, V, T, R, C: Cursor<K, V, T, R>, TInner> CursorEnter<K, V, T, R, C, TInner> {
    fn new(cursor: C) -> Self {
        CursorEnter {
            phantom: ::std::marker::PhantomData,
            cursor: cursor,
        }
    }
}

impl<K, V, T, R, C, TInner> Cursor<K, V, TInner, R> for CursorEnter<K, V, T, R, C, TInner>
where
    C: Cursor<K, V, T, R>,
    T: Timestamp,
    TInner: Refines<T>+Lattice,
{
    type Storage = C::Storage;

    #[inline(always)] fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.key_valid(storage) }
    #[inline(always)] fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.val_valid(storage) }

    #[inline(always)] fn key<'a>(&self, storage: &'a Self::Storage) -> &'a K { self.cursor.key(storage) }
    #[inline(always)] fn val<'a>(&self, storage: &'a Self::Storage) -> &'a V { self.cursor.val(storage) }

    #[inline(always)]
    fn map_times<L: FnMut(&TInner, R)>(&mut self, storage: &Self::Storage, mut logic: L) {
        self.cursor.map_times(storage, |time, diff| {
            logic(&TInner::to_inner(time.clone()), diff)
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
pub struct BatchCursorEnter<K, V, T, R, B: BatchReader<K, V, T, R>, TInner> {
    phantom: ::std::marker::PhantomData<(K, V, R, TInner)>,
    cursor: B::Cursor,
}

impl<K, V, T, R, B: BatchReader<K, V, T, R>, TInner> BatchCursorEnter<K, V, T, R, B, TInner> {
    fn new(cursor: B::Cursor) -> Self {
        BatchCursorEnter {
            phantom: ::std::marker::PhantomData,
            cursor: cursor,
        }
    }
}

impl<K, V, T, R, TInner, B: BatchReader<K, V, T, R>> Cursor<K, V, TInner, R> for BatchCursorEnter<K, V, T, R, B, TInner>
where
    T: Timestamp,
    TInner: Refines<T>+Lattice,
{
    type Storage = BatchEnter<K, V, T, R, B, TInner>;

    #[inline(always)] fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.key_valid(&storage.batch) }
    #[inline(always)] fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.val_valid(&storage.batch) }

    #[inline(always)] fn key<'a>(&self, storage: &'a Self::Storage) -> &'a K { self.cursor.key(&storage.batch) }
    #[inline(always)] fn val<'a>(&self, storage: &'a Self::Storage) -> &'a V { self.cursor.val(&storage.batch) }

    #[inline(always)]
    fn map_times<L: FnMut(&TInner, R)>(&mut self, storage: &Self::Storage, mut logic: L) {
        self.cursor.map_times(&storage.batch, |time, diff| {
            logic(&TInner::to_inner(time.clone()), diff)
        })
    }

    #[inline(always)] fn step_key(&mut self, storage: &Self::Storage) { self.cursor.step_key(&storage.batch) }
    #[inline(always)] fn seek_key(&mut self, storage: &Self::Storage, key: &K) { self.cursor.seek_key(&storage.batch, key) }

    #[inline(always)] fn step_val(&mut self, storage: &Self::Storage) { self.cursor.step_val(&storage.batch) }
    #[inline(always)] fn seek_val(&mut self, storage: &Self::Storage, val: &V) { self.cursor.seek_val(&storage.batch, val) }

    #[inline(always)] fn rewind_keys(&mut self, storage: &Self::Storage) { self.cursor.rewind_keys(&storage.batch) }
    #[inline(always)] fn rewind_vals(&mut self, storage: &Self::Storage) { self.cursor.rewind_vals(&storage.batch) }
}
