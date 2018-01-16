//! Traits and datastructures representing a collection trace.
//!
//! A collection trace is a set of updates of the form `(key, val, time, diff)`, which determine the contents
//! of a collection at given times by accumulating updates whose time field is less or equal to the target field.
//!
//! The `Trace` trait describes those types and methods that a data structure must implement to be viewed as a
//! collection trace. This trait allows operator implementations to be generic with respect to the type of trace,
//! and allows various data structures to be interpretable as multiple different types of trace.

pub mod cursor;
pub mod description;
pub mod implementations;
pub mod layers;
pub mod wrappers;

use ::Diff;
use ::lattice::Lattice;
pub use self::cursor::Cursor;
pub use self::description::Description;

// 	The traces and batch and cursors want the flexibility to appear as if they manage certain types of keys and 
// 	values and such, while perhaps using other representations, I'm thinking mostly of wrappers around the keys
// 	and vals that change the `Ord` implementation, or stash hash codes, or the like.
// 	
// 	This complicates what requirements we make so that the trace is still usable by someone who knows only about
// 	the base key and value types. For example, the complex types should likely dereference to the simpler types,
//	so that the user can make sense of the result as if they were given references to the simpler types. At the 
//  same time, the collection should be formable from base types (perhaps we need an `Into` or `From` constraint)
//  and we should, somehow, be able to take a reference to the simple types to compare against the more complex 
//  types. This second one is also like an `Into` or `From` constraint, except that we start with a reference and 
//  really don't need anything more complex than a reference, but we can't form an owned copy of the complex type 
//  without cloning it. 
//
//  We could just start by cloning things. Worry about wrapping references later on.

/// A trace whose contents may be read.
///
/// This is a restricted interface to the more general `Trace` trait, which extends this trait with further methods
/// to update the contents of the trace. These methods are used to examine the contents, and to update the reader's
/// capabilities (which may release restrictions on the mutations to the underlying trace and cause work to happen).
pub trait TraceReader<Key, Val, Time, R> {

	/// The type of an immutable collection of updates.
	type Batch: BatchReader<Key, Val, Time, R>+Clone+'static;

	/// The type used to enumerate the collections contents.
	type Cursor: Cursor<Key, Val, Time, R>;

	/// Provides a cursor over updates contained in the trace.
	fn cursor(&mut self) -> (Self::Cursor, <Self::Cursor as Cursor<Key, Val, Time, R>>::Storage) {
		if let Some(cursor) = self.cursor_through(&[]) {
			cursor
		}
		else {
			panic!("unable to acquire complete cursor for trace; is it closed?");
		}
	}

	/// Acquires a cursor to the restriction of the collection's contents to updates at times not greater or 
	/// equal to an element of `upper`.
	///
	/// This method is expected to work if called with an `upper` that (i) was an observed bound in batches from
	/// the trace, and (ii) the trace has not been advanced beyond `upper`. Practically, the implementation should
	/// be expected to look for a "clean cut" using `upper`, and if it finds such a cut can return a cursor. This
	/// should allow `upper` such as `&[]` as used by `self.cursor()`, though it is difficult to imagine other uses.
	fn cursor_through(&mut self, upper: &[Time]) -> Option<(Self::Cursor, <Self::Cursor as Cursor<Key, Val, Time, R>>::Storage)>;

	/// Advances the frontier of times the collection must be correctly accumulable through.
	///
	/// Practically, this allows the trace to advance times in updates it maintains as long as the advanced times 
	/// still compare equivalently to any times greater or equal to some element of `frontier`. Times not greater
	/// or equal to some element of `frontier` may no longer correctly accumulate, so do not advance a trace unless
	/// you are quite sure you no longer require the distinction.
	fn advance_by(&mut self, frontier: &[Time]);

	/// Reports the frontier from which all time comparisions should be accurate.
	///
	/// Times that are not greater or equal to some element of the advance frontier may accumulate inaccurately as
	/// the trace may have lost the ability to distinguish between such times. Accumulations are only guaranteed to
	/// be accurate from the frontier onwards.
	fn advance_frontier(&mut self) -> &[Time];

	/// Advances the frontier that may be used in `cursor_through`.
	///
	/// Practically, this allows the trace to merge batches whose upper frontier comes before `frontier`. The trace
	/// is likely to be annoyed or confused if you use a frontier other than one observed as an upper bound of an 
	/// actual batch. This doesn't seem likely to be a problem, but get in touch if it is.
	///
	/// Calling `distinguish_since(&[])` indicates that all batches may be merged at any point, which essentially 
	/// disables the use of `cursor_through` with any parameter other than `&[]`, which is the behavior of `cursor`.
	fn distinguish_since(&mut self, frontier: &[Time]);

	/// Reports the frontier from which the collection may be subsetted.
	///
	/// The semantics are less elegant here, but the underlying trace will not merge batches in advance of this 
	/// frontier, which ensures that operators can extract the subset of the trace at batch boundaries from this
	/// frontier onward. These boundaries may be used in `cursor_through`, whereas boundaries not in advance of 
	/// this frontier are not guaranteed to return a cursor.
	fn distinguish_frontier(&mut self) -> &[Time];

	/// Maps some logic across the batches the collection manages.
	///
	/// This is currently used only to extract historical data to prime late-starting operators who want to reproduce
	/// the stream of batches moving past the trace. It could also be a fine basis for a default implementation of the
	/// cursor methods, as they (by default) just move through batches accumulating cursors into a cursor list.
	fn map_batches<F: FnMut(&Self::Batch)>(&mut self, f: F);

}

/// An append-only collection of `(key, val, time, diff)` tuples.
///
/// The trace must pretend to look like a collection of `(Key, Val, Time, isize)` tuples, but is permitted
/// to introduce new types `KeyRef`, `ValRef`, and `TimeRef` which can be dereference to the types above.
///
/// The trace must be constructable from, and navigable by the `Key`, `Val`, `Time` types, but does not need
/// to return them.
pub trait Trace<Key, Val, Time, R> : TraceReader<Key, Val, Time, R> where <Self as TraceReader<Key, Val, Time, R>>::Batch: Batch<Key, Val, Time, R> {

	/// Allocates a new empty trace.
	fn new(info: ::timely::dataflow::operators::generic::OperatorInfo, logging: Option<::logging::Logger>) -> Self;

	/// Introduces a batch of updates to the trace.
	///
	/// Batches describe the time intervals they contain, and they should be added to the trace in contiguous
	/// intervals. If a batch arrives with a lower bound that does not equal the upper bound of the most recent
	/// addition, the trace will add an empty batch. It is an error to then try to populate that region of time.
	///
	/// This restriction could be relaxed, especially if we discover ways in which batch interval order could 
	/// commute. For now, the trace should complain, to the extent that it cares about contiguous intervals.
	fn insert(&mut self, batch: Self::Batch);

	/// Introduces an empty batch concluding the trace.
	///
	/// This method should be logically equivalent to introducing an empty batch whose lower frontier equals
	/// the upper frontier of the most recently introduced batch, and whose upper frontier is empty.
	fn close(&mut self);

    /// Reconstitute batches
    /// TODO(andreal)
	fn reconstitute(
		_batch_identifier: BatchIdentifier) -> Vec<Self::Batch>
		where
			Self: ::std::marker::Sized,
			Time: ::lattice::Lattice+Ord+Clone,
    {
		vec![]
	}
}

#[derive(Clone, Debug, Abomonation, PartialEq, Eq)]
/// An identifier for the batch
pub struct BatchIdentifier {
    address: Vec<usize>,
    trace_id: usize,
}

impl BatchIdentifier {
	/// Make a new BatchIdentifier from an operator address
    pub fn new(address: Vec<usize>, trace_id: usize) -> Self {
        BatchIdentifier {
			address: address,
			trace_id: trace_id,
		}
	}

    /// To filename string format
    pub fn to_string(&self) -> String {
        format!("{}.{}",
				self.address.iter().map(|a| a.to_string()).collect::<Vec<_>>().join("."),
				self.trace_id.to_string())
	}

	/// Parse filename string format
	pub fn from_string(input: &str) -> Self {
		let mut elems: Vec<_> = input.split('.').collect();
		let trace_id: usize = elems.pop().expect(format!("invalid BatchIdentifier: {}", input).as_str())
			.parse().expect(format!("invalid BatchIdentifier: {}", input).as_str());
		let address: Vec<usize> = elems.into_iter()
			.map(|s| s.parse().expect(format!("invalid BatchIdentifier: {}", input).as_str())).collect();

		BatchIdentifier {
			address,
			trace_id,
		}
	}
}

#[test]
fn batch_identifier_string_roundtrip() {
    let batch_ident = BatchIdentifier {
		address: vec![3, 5, 2],
		trace_id: 12,
	};

	let str = batch_ident.to_string();
	let parsed_batch_ident = BatchIdentifier::from_string(str.as_str());

	assert_eq!(parsed_batch_ident, batch_ident)
}

/// A batch of updates whose contents may be read.
///
/// This is a restricted interface to batches of updates, which support the reading of the batch's contents,
/// but do not expose ways to construct the batches. This trait is appropriate for views of the batch, and is
/// especially useful for views derived from other sources in ways that prevent the construction of batches
/// from the type of data in the view (for example, filtered views, or views with extended time coordinates).
pub trait BatchReader<K, V, T, R> where Self: ::std::marker::Sized 
{
	/// The type used to enumerate the batch's contents.
	type Cursor: Cursor<K, V, T, R, Storage=Self>;
	/// Acquires a cursor to the batch's contents.
	fn cursor(&self) -> Self::Cursor;
	/// The number of updates in the batch.
	fn len(&self) -> usize;
	/// Describes the times of the updates in the batch.
	fn description(&self) -> &Description<T>;
	/// Batch identifier
	fn identifier(&self) -> &BatchIdentifier;

	/// All times in the batch are greater or equal to an element of `lower`.
	fn lower(&self) -> &[T] { self.description().lower() }
	/// All times in the batch are not greater or equal to any element of `upper`.
	fn upper(&self) -> &[T] { self.description().upper() }
}

/// An immutable collection of updates.
pub trait Batch<K, V, T, R> : BatchReader<K, V, T, R> where Self: ::std::marker::Sized {
	/// A type used to assemble batches from disordered updates.
	type Batcher: Batcher<K, V, T, R, Self>;
	/// A type used to assemble batches from ordered update sequences.
	type Builder: Builder<K, V, T, R, Self>;
	/// A type used to progressively merge batches.
	type Merger: Merger<K, V, T, R, Self::Base, Self>;

	/// Base batch type
	type Base: Batch<K, V, T, R>;

	/// Base batch
	fn base(&self) -> &Self::Base;

	/// Merges two consecutive batches.
	///
	/// Panics if `self.upper()` does not equal `other.lower()`. This is almost certainly a logic bug,
	/// as the resulting batch does not have a contiguous description. If you would like to put an empty
	/// interval between the two, you can create an empty interval and do two merges.
	fn merge(this: &Self::Base, other: &Self::Base) -> Self;

	/// Initiates the merging of consecutive batches.
	///
	/// The result of this method can be exercised to eventually produce the same result
	/// that a call to `self.merge(other)` would produce, but it can be done in a measured
	/// fashion. This can help to avoid latency spikes where a large merge needs to happen.
	fn begin_merge(this: &Self::Base, other: &Self::Base) -> Self::Merger;

	/// Advance times to `frontier` creating a new batch.
	fn advance_ref(&self, frontier: &[T]) -> Self where K: Ord+Clone, V: Ord+Clone, T: Lattice+Ord+Clone, R: Diff {

		assert!(frontier.len() > 0);

		// TODO: This is almost certainly too much `with_capacity`.
		let mut builder = Self::Builder::with_capacity(self.len());

		let mut times = Vec::new();
		let mut cursor = self.cursor();

		while cursor.key_valid(self) {
			while cursor.val_valid(self) {
				cursor.map_times(self, |time: &T, diff| times.push((time.advance_by(frontier), diff)));
				consolidate(&mut times, 0);
				for (time, diff) in times.drain(..) {
					builder.push((cursor.key(self).clone(), cursor.val(self).clone(), time, diff));
				}
				cursor.step_val(self);
			}
			cursor.step_key(self);
		}

		builder.done(self.description().lower(), self.description().upper(), frontier, self.identifier().clone())
	}
	/// Advance times to `frontier` updating this batch.
	///
	/// This method gives batches the ability to collapse in-place when possible, and is the common 
	/// entry point to advance batches. Most types of batches do have shared state, but `advance` is 
	/// commonly invoked just after a batch is formed from a merge and when there is a unique owner 
	/// of the shared state. 
	#[inline(never)]
	fn advance_mut(&mut self, frontier: &[T]) where K: Ord+Clone, V: Ord+Clone, T: Lattice+Ord+Clone, R: Diff {
		*self = self.advance_ref(frontier);
	}
}

/// Functionality for collecting and batching updates.
pub trait Batcher<K, V, T, R, Output: Batch<K, V, T, R>> {
	/// Allocates a new empty batcher.
	fn new() -> Self;
	/// Allocates a new empty batcher with a lower frontier set.
	fn with_lower(lower: Vec<T>) -> Self;
    /// Returns the lower frontier
	fn lower(&self) -> &[T];
	/// Adds an unordered batch of elements to the batcher.
	fn push_batch(&mut self, batch: &mut Vec<((K, V), T, R)>);
	/// Returns all updates not greater or equal to an element of `upper`.
	fn seal(&mut self, upper: &[T], identifer: BatchIdentifier) -> Output;
	/// Returns the lower envelope of contained update times.
	fn frontier(&mut self) -> &[T];
}

/// Functionality for building batches from ordered update sequences.
pub trait Builder<K, V, T, R, Output: Batch<K, V, T, R>> {
	/// Allocates an empty builder.
	fn new() -> Self;
	/// Allocates an empty builder with some capacity.
	fn with_capacity(cap: usize) -> Self;
	/// Adds an element to the batch.
	fn push(&mut self, element: (K, V, T, R));
	/// Adds an ordered sequence of elements to the batch.
	fn extend<I: Iterator<Item=(K,V,T,R)>>(&mut self, iter: I) {
		for item in iter { self.push(item); }
	}
	/// Completes building and returns the batch.
	fn done(self, lower: &[T], upper: &[T], since: &[T], identifer: BatchIdentifier) -> Output;
}

/// Represents a merge in progress.
pub trait Merger<K, V, T, R, Input: Batch<K, V, T, R>, Output: Batch<K, V, T, R>> {
	/// Perform some amount of work, decrementing `fuel`.
	///
	/// If `fuel` is non-zero after the call, the merging is complete and
	/// one should call `done` to extract the merged results.
	fn work(&mut self, source1: &Input, source2: &Input, frontier: &Option<Vec<T>>, fuel: &mut usize);
	/// Extracts merged results.
	///
	/// This method should only be called after `work` has been called and
	/// has not brought `fuel` to zero. Otherwise, the merge is still in 
	/// progress.
	fn done(self) -> Output;
}


/// Blanket implementations for reference counted batches.
pub mod rc_blanket_impls {

	use std::rc::Rc;

	use ::Diff;
	use ::lattice::Lattice;
	use super::{Batch, BatchReader, Batcher, Builder, Merger, Cursor, Description};
	use trace::BatchIdentifier;

	impl<K, V, T, R, B: BatchReader<K,V,T,R>> BatchReader<K,V,T,R> for Rc<B> {

		/// The type used to enumerate the batch's contents.
		type Cursor = RcBatchCursor<K, V, T, R, B>;
		/// Acquires a cursor to the batch's contents.
		fn cursor(&self) -> Self::Cursor {
			RcBatchCursor::new((&**self).cursor())
		}

		/// The number of updates in the batch.
		fn len(&self) -> usize { (&**self).len() }
		/// Describes the times of the updates in the batch.
		fn description(&self) -> &Description<T> { (&**self).description() }
		fn identifier(&self) -> &BatchIdentifier { (&**self).identifier() }
	}

	/// Wrapper to provide cursor to nested scope.
	pub struct RcBatchCursor<K, V, T, R, B: BatchReader<K, V, T, R>> {
	    phantom: ::std::marker::PhantomData<(K, V, T, R)>,
	    cursor: B::Cursor,
	}

	impl<K, V, T, R, B: BatchReader<K, V, T, R>> RcBatchCursor<K, V, T, R, B> {
	    fn new(cursor: B::Cursor) -> Self {
	        RcBatchCursor {
	            cursor,
	            phantom: ::std::marker::PhantomData,
	        }
	    }
	}

	impl<K, V, T, R, B: BatchReader<K, V, T, R>> Cursor<K, V, T, R> for RcBatchCursor<K, V, T, R, B> {

	    type Storage = Rc<B>;

	    #[inline(always)] fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.key_valid(storage) }
	    #[inline(always)] fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.val_valid(storage) }

	    #[inline(always)] fn key<'a>(&self, storage: &'a Self::Storage) -> &'a K { self.cursor.key(storage) }
	    #[inline(always)] fn val<'a>(&self, storage: &'a Self::Storage) -> &'a V { self.cursor.val(storage) }

	    #[inline(always)]
	    fn map_times<L: FnMut(&T, R)>(&mut self, storage: &Self::Storage, logic: L) { 
	    	self.cursor.map_times(storage, logic)
	    }

	    #[inline(always)] fn step_key(&mut self, storage: &Self::Storage) { self.cursor.step_key(storage) }
	    #[inline(always)] fn seek_key(&mut self, storage: &Self::Storage, key: &K) { self.cursor.seek_key(storage, key) }
	    
	    #[inline(always)] fn step_val(&mut self, storage: &Self::Storage) { self.cursor.step_val(storage) }
	    #[inline(always)] fn seek_val(&mut self, storage: &Self::Storage, val: &V) { self.cursor.seek_val(storage, val) }

	    #[inline(always)] fn rewind_keys(&mut self, storage: &Self::Storage) { self.cursor.rewind_keys(storage) }
	    #[inline(always)] fn rewind_vals(&mut self, storage: &Self::Storage) { self.cursor.rewind_vals(storage) }
	}



	/// An immutable collection of updates.
	impl<K,V,T,R,B: Batch<K,V,T,R>> Batch<K, V, T, R> for Rc<B> {

		type Batcher = RcBatcher<K, V, T, R, B>;
		type Builder = RcBuilder<K, V, T, R, B>;
		type Merger = RcMerger<K, V, T, R, B>;
		type Base = <B as Batch<K, V, T, R>>::Base;

		fn base(&self) -> &Self::Base { self.as_ref().base() }

		fn merge(this: &Self::Base, other: &Self::Base) -> Self { Rc::new(B::merge(this, other)) }

		fn begin_merge(this: &Self::Base, other: &Self::Base) -> Self::Merger { RcMerger { merger: B::begin_merge(this, other) } }

		fn advance_mut(&mut self, frontier: &[T]) where K: Ord+Clone, V: Ord+Clone, T: Lattice+Ord+Clone, R: Diff {
			let mut updated = false;
			if let Some(batch) = Rc::get_mut(self) {
				batch.advance_mut(frontier);
				updated = true;
			}

			if !updated {
				*self = self.advance_ref(frontier);
			}
		}
	}

	/// Wrapper type for batching reference counted batches.
	pub struct RcBatcher<K,V,T,R,B:Batch<K,V,T,R>> { batcher: B::Batcher }

	/// Functionality for collecting and batching updates.
	impl<K,V,T,R,B:Batch<K,V,T,R>> Batcher<K, V, T, R, Rc<B>> for RcBatcher<K,V,T,R,B> {
		fn new() -> Self { RcBatcher { batcher: <B::Batcher as Batcher<K,V,T,R,B>>::new() } }
		fn with_lower(lower: Vec<T>) -> Self { let new = <B::Batcher as Batcher<K,V,T,R,B>>::with_lower(lower); RcBatcher { batcher: new } }
		fn lower(&self) -> &[T] { self.batcher.lower() }
		fn push_batch(&mut self, batch: &mut Vec<((K, V), T, R)>) { self.batcher.push_batch(batch) }
		fn seal(&mut self, upper: &[T], identifier: BatchIdentifier) -> Rc<B> { Rc::new(self.batcher.seal(upper, identifier)) }
		fn frontier(&mut self) -> &[T] { self.batcher.frontier() }
	}

	/// Wrapper type for building reference counted batches.
	pub struct RcBuilder<K,V,T,R,B:Batch<K,V,T,R>> { builder: B::Builder }

	/// Functionality for building batches from ordered update sequences.
	impl<K,V,T,R,B:Batch<K,V,T,R>> Builder<K, V, T, R, Rc<B>> for RcBuilder<K,V,T,R,B> {
		fn new() -> Self { RcBuilder { builder: <B::Builder as Builder<K,V,T,R,B>>::new() } }
		fn with_capacity(cap: usize) -> Self { RcBuilder { builder: <B::Builder as Builder<K,V,T,R,B>>::with_capacity(cap) } }
		fn push(&mut self, element: (K, V, T, R)) { self.builder.push(element) }
		fn done(self, lower: &[T], upper: &[T], since: &[T], identifier: BatchIdentifier) -> Rc<B> { Rc::new(self.builder.done(lower, upper, since, identifier)) }
	}

	/// Wrapper type for merging reference counted batches.
	pub struct RcMerger<K,V,T,R,B:Batch<K,V,T,R>> { merger: B::Merger }

	/// Represents a merge in progress.
	impl<K,V,T,R,Base:Batch<K,V,T,R>,B:Batch<K,V,T,R,Base=Base>> Merger<K, V, T, R, Base, Rc<B>> for RcMerger<K,V,T,R,B>
		where <B as Batch<K, V, T, R>>::Merger: Merger<K, V, T, R, Base, B>
	{
		fn work(&mut self, source1: &Base, source2: &Base, frontier: &Option<Vec<T>>, fuel: &mut usize) { self.merger.work(source1, source2, frontier, fuel) }
		fn done(self) -> Rc<B> { Rc::new(self.merger.done()) }
	}
}

/// Blanket implementations for reference counted batches.
pub mod abomonated_blanket_impls {

	extern crate abomonation;

	use std::ops::DerefMut;

	use memmap::MmapMut;

	use abomonation::{Abomonation, measure};
	use abomonation::abomonated::Abomonated;

	use super::{Batch, BatchReader, Batcher, Builder, Merger, Cursor, Description};
	use trace::BatchIdentifier;

	fn duration_to_string(duration: &::std::time::Duration) -> String {
		format!("{:020}{:09}", duration.as_secs(), duration.subsec_nanos())
	}

	#[test]
	fn duration_to_string_as_expected() {
		assert_eq!(duration_to_string(&::std::time::Duration::new(59087389724, 5001000)),
				   "00000000059087389724005001000");
	}

	/// Methods to size and populate a byte slice.
	pub trait SizedDerefMut : DerefMut<Target=[u8]> {
		/// Allocate a new instance with a supplied capacity.
		fn allocate_for<K,V,T,R,B: BatchReader<K,V,T,R>+Abomonation>(batch: &B) -> Self;
	}

	impl SizedDerefMut for Vec<u8> {
		fn allocate_for<K,V,T,R,B: BatchReader<K,V,T,R>+Abomonation>(batch: &B) -> Self {
            // TODO??
			vec![0; measure(batch)]
		}
	}

	impl SizedDerefMut for MmapMut {
		fn allocate_for<K,V,T,R,B: BatchReader<K,V,T,R>+Abomonation>(batch: &B) -> Self {
			let time = ::std::time::UNIX_EPOCH.elapsed().expect("UNIX_EPOCH.elapsed() failed");
			let filename = format!("durability/{}-{}.abom", batch.identifier().to_string(), duration_to_string(&time));
			let file = ::std::fs::OpenOptions::new().read(true).write(true).create_new(true).open(filename).expect("failed to open file");
			file.set_len(measure(batch) as u64).expect("failed to set length");

			unsafe { ::memmap::MmapOptions::new().map_mut(&file).expect("failed to map copy") }
		}
	}

	impl<K, V, T, R, B: BatchReader<K,V,T,R>+Abomonation, S: SizedDerefMut> BatchReader<K,V,T,R> for Abomonated<B, S> {

		/// The type used to enumerate the batch's contents.
		type Cursor = AbomonatedBatchCursor<K, V, T, R, B, S>;
		/// Acquires a cursor to the batch's contents.
		fn cursor(&self) -> Self::Cursor {
			AbomonatedBatchCursor::new((&**self).cursor())
		}

		/// The number of updates in the batch.
		fn len(&self) -> usize { (&**self).len() }
		/// Describes the times of the updates in the batch.
		fn description(&self) -> &Description<T> { (&**self).description() }
		fn identifier(&self) -> &BatchIdentifier { (&**self).identifier() }
	}

	/// Wrapper to provide cursor to nested scope.
	pub struct AbomonatedBatchCursor<K, V, T, R, B: BatchReader<K, V, T, R>, S: SizedDerefMut> {
	    phantom: ::std::marker::PhantomData<(K, V, T, R, S)>,
	    cursor: B::Cursor,
	}

	impl<K, V, T, R, B: BatchReader<K, V, T, R>, S: SizedDerefMut> AbomonatedBatchCursor<K, V, T, R, B, S> {
	    fn new(cursor: B::Cursor) -> Self {
	        AbomonatedBatchCursor {
	            cursor,
	            phantom: ::std::marker::PhantomData,
	        }
	    }
	}

	impl<K, V, T, R, B: BatchReader<K, V, T, R>+Abomonation, S: SizedDerefMut> Cursor<K, V, T, R> for AbomonatedBatchCursor<K, V, T, R, B, S> {

	    type Storage = Abomonated<B, S>;

	    #[inline(always)] fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.key_valid(storage) }
	    #[inline(always)] fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.val_valid(storage) }

	    #[inline(always)] fn key<'a>(&self, storage: &'a Self::Storage) -> &'a K { self.cursor.key(storage) }
	    #[inline(always)] fn val<'a>(&self, storage: &'a Self::Storage) -> &'a V { self.cursor.val(storage) }

	    #[inline(always)]
	    fn map_times<L: FnMut(&T, R)>(&mut self, storage: &Self::Storage, logic: L) { 
	    	self.cursor.map_times(storage, logic)
	    }

	    #[inline(always)] fn step_key(&mut self, storage: &Self::Storage) { self.cursor.step_key(storage) }
	    #[inline(always)] fn seek_key(&mut self, storage: &Self::Storage, key: &K) { self.cursor.seek_key(storage, key) }
	    
	    #[inline(always)] fn step_val(&mut self, storage: &Self::Storage) { self.cursor.step_val(storage) }
	    #[inline(always)] fn seek_val(&mut self, storage: &Self::Storage, val: &V) { self.cursor.seek_val(storage, val) }

	    #[inline(always)] fn rewind_keys(&mut self, storage: &Self::Storage) { self.cursor.rewind_keys(storage) }
	    #[inline(always)] fn rewind_vals(&mut self, storage: &Self::Storage) { self.cursor.rewind_vals(storage) }
	}

	/// An immutable collection of updates.
	impl<K,V,T,R,B: Batch<K,V,T,R>+Abomonation, S: SizedDerefMut> Batch<K, V, T, R> for Abomonated<B, S> {

		type Batcher = AbomonatedBatcher<K, V, T, R, B>;
		type Builder = AbomonatedBuilder<K, V, T, R, B>;
		type Merger = AbomonatedMerger<K, V, T, R, B>;
		type Base = <B as Batch<K, V, T, R>>::Base;

		fn base(&self) -> &Self::Base { (**self).base() }

		fn merge(this: &Self::Base, other: &Self::Base) -> Self {
			let batch = B::merge(this, other);
			let mut bytes = S::allocate_for(&batch);//Vec::with_capacity(measure(&batch));
			unsafe { abomonation::encode(&batch, &mut bytes.deref_mut()).unwrap() };
			unsafe { Abomonated::<B,_>::new(bytes).unwrap() }
		}

		fn begin_merge(this: &Self::Base, other: &Self::Base) -> Self::Merger { AbomonatedMerger { merger: B::begin_merge(this, other) } }
	}

	/// Wrapper type for batching reference counted batches.
	pub struct AbomonatedBatcher<K,V,T,R,B:Batch<K,V,T,R>> { batcher: B::Batcher }

	/// Functionality for collecting and batching updates.
	impl<K,V,T,R,B:Batch<K,V,T,R>+Abomonation, S:SizedDerefMut> Batcher<K, V, T, R, Abomonated<B,S>> for AbomonatedBatcher<K,V,T,R,B> {
		fn new() -> Self { AbomonatedBatcher { batcher: <B::Batcher as Batcher<K,V,T,R,B>>::new() } }
		fn with_lower(lower: Vec<T>) -> Self { let new = <B::Batcher as Batcher<K,V,T,R,B>>::with_lower(lower); AbomonatedBatcher { batcher: new } }
		fn lower(&self) -> &[T] { self.batcher.lower() }
		fn push_batch(&mut self, batch: &mut Vec<((K, V), T, R)>) { self.batcher.push_batch(batch) }
		fn seal(&mut self, upper: &[T], identifier: BatchIdentifier) -> Abomonated<B, S> {
			let batch = self.batcher.seal(upper, identifier);
			let mut bytes = S::allocate_for(&batch);//Vec::with_capacity(measure(&batch));
			unsafe { abomonation::encode(&batch, &mut bytes.deref_mut()).unwrap() };
			unsafe { Abomonated::<B,_>::new(bytes).unwrap() }
		}
		fn frontier(&mut self) -> &[T] { self.batcher.frontier() }
	}

	/// Wrapper type for building reference counted batches.
	pub struct AbomonatedBuilder<K,V,T,R,B:Batch<K,V,T,R>> { builder: B::Builder }

	/// Functionality for building batches from ordered update sequences.
	impl<K,V,T,R,B:Batch<K,V,T,R>+Abomonation,S:SizedDerefMut> Builder<K, V, T, R, Abomonated<B,S>> for AbomonatedBuilder<K,V,T,R,B> {
		fn new() -> Self { AbomonatedBuilder { builder: <B::Builder as Builder<K,V,T,R,B>>::new() } }
		fn with_capacity(cap: usize) -> Self { AbomonatedBuilder { builder: <B::Builder as Builder<K,V,T,R,B>>::with_capacity(cap) } }
		fn push(&mut self, element: (K, V, T, R)) { self.builder.push(element) }
		fn done(self, lower: &[T], upper: &[T], since: &[T], identifier: BatchIdentifier) -> Abomonated<B, S> {
			let batch = self.builder.done(lower, upper, since, identifier);
			let mut bytes = S::allocate_for(&batch);
			unsafe { abomonation::encode(&batch, &mut bytes.deref_mut()).unwrap() };
			unsafe { Abomonated::<B,_>::new(bytes).unwrap() }
		}
	}

	/// Wrapper type for merging reference counted batches.
	pub struct AbomonatedMerger<K,V,T,R,B:Batch<K,V,T,R>> { merger: B::Merger }

	/// Represents a merge in progress.
	impl<K,V,T,R,Base:Batch<K,V,T,R>,B:Batch<K,V,T,R,Base=Base>+Abomonation,S:SizedDerefMut> Merger<K, V, T, R, Base, Abomonated<B,S>> for AbomonatedMerger<K,V,T,R,B>
		where <B as Batch<K, V, T, R>>::Merger: Merger<K, V, T, R, Base, B>
	{
		fn work(&mut self, source1: &Base, source2: &Base, frontier: &Option<Vec<T>>, fuel: &mut usize) {
			self.merger.work(source1, source2, frontier, fuel) 
		}
		fn done(self) -> Abomonated<B, S> { 
			let batch = self.merger.done();
			let mut bytes = S::allocate_for(&batch);//Vec::with_capacity(measure(&batch));
			unsafe { abomonation::encode(&batch, &mut bytes.deref_mut()).unwrap() };
			unsafe { Abomonated::<B,_>::new(bytes).unwrap() }
		}
	}
}

/// Either Left or Right
pub enum Either<L, R> {
	/// Left
	Left(L),
	/// Right
	Right(R),
}

/// Blanket implementations for Either batches
pub mod either_blanket_impls {
	use super::Either;

	use ::Diff;
	use ::lattice::Lattice;
	use super::{Batch, BatchReader, Batcher, Builder, Merger, Cursor, Description};
	use trace::BatchIdentifier;

	macro_rules! symmetric_either_ref {
		($v:ident = $s:expr, $o:expr) => (match $s {
            &Either::Left(ref $v) => $o,
            &Either::Right(ref $v) => $o,
        });
	}

	macro_rules! symmetric_either_ref_mut {
		($v:ident = $s:expr, $o:expr) => (match $s {
            &mut Either::Left(ref mut $v) => $o,
            &mut Either::Right(ref mut $v) => $o,
        });
	}

	macro_rules! same_either {
		($v:ident = $s:expr, $v2:ident = $s2:expr, $o:expr) => (match (&$s, $s2) {
			(&Either::Left(ref $v), &Either::Left(ref $v2)) => $o,
			(&Either::Right(ref $v), &Either::Right(ref $v2)) => $o,
			_ => panic!("unexpected Either combination"),
        });
	}

	macro_rules! same_either_mut {
		($v:ident = $s:expr, $v2:ident = $s2:expr, $o:expr) => (match (&mut $s, $s2) {
			(&mut Either::Left(ref mut $v), &Either::Left(ref $v2)) => $o,
			(&mut Either::Right(ref mut $v), &Either::Right(ref $v2)) => $o,
			_ => panic!("unexpected Either combination"),
        });
	}

	impl<K, V, T, R, B1: BatchReader<K,V,T,R>, B2: BatchReader<K,V,T,R>> BatchReader<K,V,T,R> for Either<B1, B2> {

		/// The type used to enumerate the batch's contents.
		type Cursor = EitherBatchCursor<K, V, T, R, B1, B2>;
		/// Acquires a cursor to the batch's contents.
		fn cursor(&self) -> Self::Cursor {
			EitherBatchCursor::new( //(&**self).cursor())
				match self {
                    &Either::Left(ref s) => Either::Left(s.cursor()),
					&Either::Right(ref s) => Either::Right(s.cursor()),
				})
		}

		/// The number of updates in the batch.
		fn len(&self) -> usize {
			symmetric_either_ref!(s = self, s.len())
		}
		/// Describes the times of the updates in the batch.
		fn description(&self) -> &Description<T> {
            symmetric_either_ref!(s = self, s.description())
		}
		fn identifier(&self) -> &BatchIdentifier {
            symmetric_either_ref!(s = self, s.identifier())
        }
	}

	/// Wrapper to provide cursor to nested scope.
	pub struct EitherBatchCursor<K, V, T, R, B1: BatchReader<K, V, T, R>, B2: BatchReader<K, V, T, R>> {
	    phantom: ::std::marker::PhantomData<(K, V, T, R)>,
	    cursor: Either<B1::Cursor, B2::Cursor>,
	}

	impl<K, V, T, R, B1: BatchReader<K, V, T, R>, B2: BatchReader<K, V, T, R>> EitherBatchCursor<K, V, T, R, B1, B2> {
	    fn new(cursor: Either<B1::Cursor, B2::Cursor>) -> Self {
	        EitherBatchCursor {
	            cursor: cursor,
	            phantom: ::std::marker::PhantomData,
	        }
	    }
	}

	impl<K, V, T, R, B1: BatchReader<K, V, T, R>, B2: BatchReader<K, V, T, R>> Cursor<K, V, T, R> for EitherBatchCursor<K, V, T, R, B1, B2> {

	    type Storage = Either<B1, B2>;

	    #[inline(always)] fn key_valid(&self, storage: &Self::Storage) -> bool { same_either!(c = self.cursor, s = storage, c.key_valid(s)) }
	    #[inline(always)] fn val_valid(&self, storage: &Self::Storage) -> bool { same_either!(c = self.cursor, s = storage, c.val_valid(s)) }

	    #[inline(always)] fn key<'a>(&self, storage: &'a Self::Storage) -> &'a K { same_either!(c = self.cursor, s = storage, c.key(s)) }
	    #[inline(always)] fn val<'a>(&self, storage: &'a Self::Storage) -> &'a V { same_either!(c = self.cursor, s = storage, c.val(s)) }

	    #[inline(always)]
	    fn map_times<L: FnMut(&T, R)>(&mut self, storage: &Self::Storage, logic: L) {
	    	same_either_mut!(c = self.cursor, s = storage, c.map_times(s, logic))
	    }

	    #[inline(always)] fn step_key(&mut self, storage: &Self::Storage) { same_either_mut!(c = self.cursor, s = storage, c.step_key(s)) }
	    #[inline(always)] fn seek_key(&mut self, storage: &Self::Storage, key: &K) { same_either_mut!(c = self.cursor, s = storage, c.seek_key(s, key)) }

	    #[inline(always)] fn step_val(&mut self, storage: &Self::Storage) { same_either_mut!(c = self.cursor, s = storage, c.step_val(s)) }
	    #[inline(always)] fn seek_val(&mut self, storage: &Self::Storage, val: &V) { same_either_mut!(c = self.cursor, s = storage, c.seek_val(s, val)) }

	    #[inline(always)] fn rewind_keys(&mut self, storage: &Self::Storage) { same_either_mut!(c = self.cursor, s = storage, c.rewind_keys(s)) }
	    #[inline(always)] fn rewind_vals(&mut self, storage: &Self::Storage) { same_either_mut!(c = self.cursor, s = storage, c.rewind_vals(s)) }
	}

	/// An immutable collection of updates.
	impl<K,V,T,R,Base: Batch<K,V,T,R>, B1: Batch<K,V,T,R,Base=Base>, B2: Batch<K,V,T,R,Base=Base>> Batch<K, V, T, R> for Either<B1, B2>
		where <B1 as Batch<K, V, T, R>>::Merger: Merger<K, V, T, R, Base, B1>, <B2 as Batch<K, V, T, R>>::Merger: Merger<K, V, T, R, Base, B2>
	{

		type Batcher = EitherBatcher<K, V, T, R, B1, B2>;
		type Builder = EitherBuilder<K, V, T, R, B1, B2>;
		type Merger = EitherMerger<K, V, T, R, B1, B2>;
		type Base = Base;

		fn base(&self) -> &Self::Base { symmetric_either_ref!(s = self, s.base()) }

		fn merge(this: &Self::Base, other: &Self::Base) -> Self { Either::Left(B1::merge(this, other)) }

		fn begin_merge(this: &Self::Base, other: &Self::Base) -> Self::Merger { EitherMerger { merger: B1::begin_merge(this, other), phantom: ::std::marker::PhantomData } }

		fn advance_mut(&mut self, frontier: &[T]) where K: Ord+Clone, V: Ord+Clone, T: Lattice+Ord+Clone, R: Diff {
            symmetric_either_ref_mut!(s = self, s.advance_mut(frontier));
		}
	}

	/// Wrapper type for batching reference counted batches.
	pub struct EitherBatcher<K,V,T,R,B1:Batch<K,V,T,R>,B2:Batch<K,V,T,R>> {
		batcher: B1::Batcher,
		phantom: ::std::marker::PhantomData<(B1, B2)>,
	}

	/// Functionality for collecting and batching updates.
	impl<K,V,T,R,Base:Batch<K,V,T,R>,B1:Batch<K,V,T,R,Base=Base>,B2:Batch<K,V,T,R,Base=Base>> Batcher<K, V, T, R, Either<B1, B2>> for EitherBatcher<K,V,T,R,B1,B2>
		where <B1 as Batch<K, V, T, R>>::Merger: Merger<K, V, T, R, Base, B1>, <B2 as Batch<K, V, T, R>>::Merger: Merger<K, V, T, R, Base, B2>
	{
		fn new() -> Self { EitherBatcher { batcher: <B1::Batcher as Batcher<K,V,T,R,B1>>::new(), phantom: ::std::marker::PhantomData, } }

		fn with_lower(lower: Vec<T>) -> Self { let new = <B1::Batcher as Batcher<K,V,T,R,B1>>::with_lower(lower); EitherBatcher { batcher: new, phantom: ::std::marker::PhantomData, } }
		fn lower(&self) -> &[T] { self.batcher.lower() }
		fn push_batch(&mut self, batch: &mut Vec<((K, V), T, R)>) { self.batcher.push_batch(batch) }
		fn seal(&mut self, upper: &[T], identifier: BatchIdentifier) -> Either<B1, B2> { Either::Left(self.batcher.seal(upper, identifier)) }
		fn frontier(&mut self) -> &[T] { self.batcher.frontier() }
	}

	/// Wrapper type for building reference counted batches.
	pub struct EitherBuilder<K,V,T,R,B1:Batch<K,V,T,R>,B2:Batch<K,V,T,R>> { builder: B1::Builder, phantom: ::std::marker::PhantomData<(B1, B2)>, }

	/// Functionality for building batches from ordered update sequences.
	impl<K,V,T,R,Base:Batch<K,V,T,R>,B1:Batch<K,V,T,R,Base=Base>,B2:Batch<K,V,T,R,Base=Base>> Builder<K, V, T, R, Either<B1,B2>> for EitherBuilder<K,V,T,R,B1,B2>
		where <B1 as Batch<K, V, T, R>>::Merger: Merger<K, V, T, R, Base, B1>, <B2 as Batch<K, V, T, R>>::Merger: Merger<K, V, T, R, Base, B2>
	{
		fn new() -> Self { EitherBuilder { builder: <B1::Builder as Builder<K,V,T,R,B1>>::new(), phantom: ::std::marker::PhantomData, } }
		fn with_capacity(cap: usize) -> Self { EitherBuilder { builder: <B1::Builder as Builder<K,V,T,R,B1>>::with_capacity(cap), phantom: ::std::marker::PhantomData, } }
		fn push(&mut self, element: (K, V, T, R)) { self.builder.push(element) }
		fn done(self, lower: &[T], upper: &[T], since: &[T], identifier: BatchIdentifier) -> Either<B1, B2> { Either::Left(self.builder.done(lower, upper, since, identifier)) }
	}

	/// Wrapper type for merging reference counted batches.
	pub struct EitherMerger<K,V,T,R,B1:Batch<K,V,T,R>,B2:Batch<K,V,T,R>> { merger: B1::Merger, phantom: ::std::marker::PhantomData<(B1, B2)>,}

	/// Represents a merge in progress.
	impl<K,V,T,R,Base:Batch<K,V,T,R>,B1:Batch<K,V,T,R,Base=Base>,B2:Batch<K,V,T,R,Base=Base>> Merger<K, V, T, R, Base, Either<B1, B2>> for EitherMerger<K,V,T,R,B1,B2>
		where
			<B1 as Batch<K, V, T, R>>::Merger: Merger<K, V, T, R, Base, B1>,
			<B2 as Batch<K, V, T, R>>::Merger: Merger<K, V, T, R, Base, B2>,
	{
		fn work(&mut self, source1: &Base, source2: &Base, frontier: &Option<Vec<T>>, fuel: &mut usize) { self.merger.work(source1, source2, frontier, fuel) }
		fn done(self) -> Either<B1, B2> { Either::Left(self.merger.done()) }
	}
}

/// Scans `vec[off..]` and consolidates differences of adjacent equivalent elements.
pub fn consolidate<T: Ord+Clone, R: Diff>(vec: &mut Vec<(T, R)>, off: usize) {
	consolidate_by(vec, off, |x,y| x.cmp(&y));
}


/// Scans `vec[off..]` and consolidates differences of adjacent equivalent elements.
pub fn consolidate_by<T: Eq+Clone, L: Fn(&T, &T)->::std::cmp::Ordering, R: Diff>(vec: &mut Vec<(T, R)>, off: usize, cmp: L) {
	vec[off..].sort_by(|x,y| cmp(&x.0, &y.0));
	for index in (off + 1) .. vec.len() {
		if vec[index].0 == vec[index - 1].0 {
			vec[index].1 = vec[index].1 + vec[index - 1].1;
			vec[index - 1].1 = R::zero();
		}
	}
	let mut cursor = off;
	for index in off .. vec.len() {
		if !vec[index].1.is_zero() {
			vec.swap(cursor, index);
			// vec[cursor] = vec[index].clone();
			cursor += 1;
		}
	}
	vec.truncate(cursor);
}
