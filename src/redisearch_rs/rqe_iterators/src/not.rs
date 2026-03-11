/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Supporting types for [`Not`].

use std::{ptr::NonNull, time::Duration};

use ffi::{IteratorType, IteratorType_NOT_ITERATOR, RS_FIELDMASK_ALL, t_docId};
use inverted_index::RSIndexResult;

use crate::{
    Empty, RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome,
    maybe_empty::MaybeEmpty, not_optimized::NotOptimized, util::TimeoutContext,
    wildcard::new_wildcard_iterator,
};

/// An iterator that negates the results of its child iterator.
///
/// Yields all document IDs from 1 to `max_doc_id` (inclusive) that are **not**
/// present in the child iterator.
pub struct Not<'index, I> {
    /// The child iterator whose results are negated.
    child: MaybeEmpty<I>,
    /// The maximum document ID to iterate up to (inclusive).
    max_doc_id: t_docId,
    /// Set to `true` in case the NOT Iterator
    /// detected using the [`TimeoutContext`] a timeout,
    /// and reset to `false` at [`RQEIterator::rewind`].
    forced_eof: bool,
    /// A reusable result object to avoid allocations on each [`read`](RQEIterator::read) call.
    result: RSIndexResult<'index>,
    /// Tracks the execution deadline for this iterator.
    ///
    /// Uses an amortized check to minimize overhead in hot paths. The timeout
    /// is absolute for the iterator's lifetime and does not reset upon rewinding.
    timeout_ctx: Option<TimeoutContext>,
}

impl<'index, I> Not<'index, I>
where
    I: RQEIterator<'index>,
{
    pub fn new(
        child: I,
        max_doc_id: t_docId,
        weight: f64,
        timeout: Duration,
        skip_timeout_checks: bool,
    ) -> Self {
        Self {
            child: MaybeEmpty::new(child),
            max_doc_id,
            forced_eof: false,
            result: RSIndexResult::build_virt()
                .weight(weight)
                .field_mask(RS_FIELDMASK_ALL)
                .build(),
            // The `limit` of 5_000 determines the granularity of the timeout check.
            // Each time [`TimeoutContext::check_timeout`] is called (during `read` / `skip_to`),
            // the internal counter goes up. When it reaches this `limit` of 5_000 it will
            // reset that counter and do the actual (OS) expensive timeout check.
            timeout_ctx: if skip_timeout_checks {
                None
            } else {
                Some(TimeoutContext::new(timeout, 5_000, false))
            },
        }
    }

    /// Wrapper around [`TimeoutContext::check_timeout`] to ensure that in case of an error (timeout),
    /// we also mark this iterator as EOF.
    ///
    /// Returns error [`RQEIteratorError::TimedOut`] if the deadline has been reached or exceeded.
    ///
    /// In case no timeout is enforced it will just return `Ok(())`.
    #[inline(always)]
    fn check_timeout(&mut self) -> Result<(), RQEIteratorError> {
        let Some(result) = self.timeout_ctx.as_mut().map(|ctx| ctx.check_timeout()) else {
            return Ok(());
        };
        if matches!(result, Err(RQEIteratorError::TimedOut)) {
            // NOTE: this is not done for optimized version of NOT iterator in C
            self.forced_eof = true;
        }
        result
    }

    /// Wrapper around [`TimeoutContext::reset_counter`] to reset the timeout counter.
    ///
    /// Does nothing when no timeout is enforced.
    #[inline(always)]
    const fn reset_timeout(&mut self) {
        if let Some(ctx) = self.timeout_ctx.as_mut() {
            ctx.reset_counter();
        }
    }

    /// Get a shared reference to the _child_ iterator
    /// wrapped by this [`Not`] iterator.
    pub const fn child(&self) -> Option<&I> {
        self.child.as_ref()
    }

    /// Set the child of this [`Not`] iterator.
    pub fn set_child(&mut self, new_child: I) {
        self.child = MaybeEmpty::new(new_child);
    }

    /// Unset the child of this [`Not`] iterator (make it `None`).
    pub fn unset_child(&mut self) {
        self.child = MaybeEmpty::new_empty();
    }

    /// Take the child of this [`Not`] iterator if it exists.
    pub fn take_child(&mut self) -> Option<I> {
        self.child.take_iterator()
    }
}

impl<'index, I> RQEIterator<'index> for Not<'index, I>
where
    I: RQEIterator<'index>,
{
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        Some(&mut self.result)
    }

    #[inline(always)]
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        // skip all child docs, while not EOF and in sync with child
        while !self.at_eof() {
            self.result.doc_id += 1;

            // Sync child if we've moved past its last known position
            let child_at_eof = if self.result.doc_id > self.child.last_doc_id() {
                self.child.read()?.is_none()
            } else {
                false
            };

            // Comparison Logic
            // If child is EOF, or we haven't reached the child's position,
            // or the child skipped past us, this document is a valid result.
            if child_at_eof || self.result.doc_id != self.child.last_doc_id() {
                self.reset_timeout();
                return Ok(Some(&mut self.result));
            }

            // Unified Checkpoint: Exactly one check per iteration.
            // This occurs AFTER the child.read() and before we decide to return.
            self.check_timeout()?;

            // Otherwise: doc_id == child.last_doc_id(), so we skip and loop again.
        }

        debug_assert!(self.at_eof());
        Ok(None)
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        debug_assert!(self.last_doc_id() < doc_id);

        if self.at_eof() {
            return Ok(None);
        }

        // Do not skip beyond max_doc_id
        if doc_id > self.max_doc_id {
            self.result.doc_id = self.max_doc_id;
            return Ok(None);
        }

        // Case 1: Child is ahead or at EOF - docId is not in child
        // When child is at EOF, only accept doc_id if it's past the child's last document
        if self.child.last_doc_id() > doc_id
            || (self.child.at_eof() && doc_id > self.child.last_doc_id())
        {
            self.result.doc_id = doc_id;
            self.check_timeout()?;

            return Ok(Some(SkipToOutcome::Found(&mut self.result)));
        }
        // Case 2: Child is behind docId - need to check if docId is in child
        if self.child.last_doc_id() < doc_id {
            let rc = self.child.skip_to(doc_id)?;
            match rc {
                Some(SkipToOutcome::Found(_)) => {
                    // Found value - do not return
                }
                None | Some(SkipToOutcome::NotFound(_)) => {
                    // Not found or EOF - return
                    self.result.doc_id = doc_id;

                    self.check_timeout()?;

                    return Ok(Some(SkipToOutcome::Found(&mut self.result)));
                }
            }
        }

        self.check_timeout()?;

        // If we are here, Child has DocID (either already lastDocID == docId or the SkipTo returned OK)
        // We need to return NOTFOUND and set the current result to the next valid docId
        self.result.doc_id = doc_id;
        match self.read()? {
            Some(_) => Ok(Some(SkipToOutcome::NotFound(&mut self.result))),
            None => Ok(None),
        }
    }

    #[inline(always)]
    fn rewind(&mut self) {
        self.forced_eof = false;
        self.result.doc_id = 0;
        self.child.rewind();
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        self.max_doc_id as usize
    }

    #[inline(always)]
    fn last_doc_id(&self) -> t_docId {
        self.result.doc_id
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.forced_eof || self.result.doc_id >= self.max_doc_id
    }

    #[inline(always)]
    fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        // Get child status
        match self.child.revalidate()? {
            RQEValidateStatus::Aborted => {
                self.child = MaybeEmpty::new_empty();
                Ok(RQEValidateStatus::Ok)
            }
            RQEValidateStatus::Moved { .. } => {
                // Invariant: after read/skip_to, child is always ahead of NOT's position (or at EOF).
                // Moved means child moved forward (can't move backward), so our doc remains valid.
                // Special case: both at initial state (doc_id = 0) is also valid.
                debug_assert!(
                    self.child.at_eof()
                        || self.child.last_doc_id() > self.last_doc_id()
                        || (self.child.last_doc_id() == 0 && self.last_doc_id() == 0)
                );
                Ok(RQEValidateStatus::Ok)
            }
            RQEValidateStatus::Ok => {
                // Child did not move - we did not move
                Ok(RQEValidateStatus::Ok)
            }
        }
    }
}

/// Trait for NOT iterators ([`Not`] and [`NotOptimized`]).
pub trait NotIterator<'index>: RQEIterator<'index> {
    /// Get a shared reference to the child iterator, or `None` if unset.
    fn child(&self) -> Option<&dyn RQEIterator<'index>>;

    /// Replace the child iterator.
    fn set_child(&mut self, child: Box<dyn RQEIterator<'index> + 'index>);

    /// Take ownership of the child iterator, leaving it unset.
    fn take_child(&mut self) -> Option<Box<dyn RQEIterator<'index> + 'index>>;
}

pub(crate) type BoxedChild<'index> = Box<dyn RQEIterator<'index> + 'index>;

impl<'index> NotIterator<'index> for Not<'index, BoxedChild<'index>> {
    fn child(&self) -> Option<&dyn RQEIterator<'index>> {
        self.child
            .as_ref()
            .map(|c| &**c as &dyn RQEIterator<'index>)
    }

    fn set_child(&mut self, child: BoxedChild<'index>) {
        self.child = MaybeEmpty::new(child);
    }

    fn take_child(&mut self) -> Option<BoxedChild<'index>> {
        self.child.take_iterator()
    }
}

impl<'index, W> NotIterator<'index> for NotOptimized<'index, W, BoxedChild<'index>>
where
    W: RQEIterator<'index>,
{
    fn child(&self) -> Option<&dyn RQEIterator<'index>> {
        NotOptimized::child(self).map(|c| &**c as &dyn RQEIterator<'index>)
    }

    fn set_child(&mut self, child: BoxedChild<'index>) {
        NotOptimized::set_child(self, child);
    }

    fn take_child(&mut self) -> Option<BoxedChild<'index>> {
        NotOptimized::take_child(self)
    }
}

/// The result of [`not_iterator_reducer`].
enum NotReduction<'index, I> {
    /// The NOT was reduced to a simpler iterator (e.g. wildcard or empty).
    Reduced(Box<dyn RQEIterator<'index> + 'index>, IteratorType),
    /// No reduction was possible. The child is returned unchanged.
    NotReduced(I),
}

/// Attempt to reduce a NOT iterator into a simpler form.
///
/// Applies the following reduction rules:
/// 1. If the child is empty, the NOT matches everything — return a wildcard.
/// 2. If the child is a wildcard, the NOT matches nothing — return empty.
///
/// # Safety
///
/// When the child is empty (rule 1), this function calls
/// [`new_wildcard_iterator`] — all its safety preconditions on `query` must
/// hold.
unsafe fn not_iterator_reducer<'index, I>(
    child: I,
    weight: f64,
    query: NonNull<ffi::QueryEvalCtx>,
) -> NotReduction<'index, I>
where
    I: RQEIterator<'index> + 'index,
{
    if child.is_empty() {
        // Rule 1: child is empty → NOT matches everything → wildcard.
        drop(child);
        // SAFETY: Caller guarantees the preconditions of `new_wildcard_iterator`.
        let (mut wc, wc_type) = unsafe { new_wildcard_iterator(query, weight) };
        if let Some(result) = wc.current() {
            result.freq = 0;
        }
        NotReduction::Reduced(wc, wc_type)
    } else if child.is_wildcard() {
        // Rule 2: child is wildcard → NOT matches nothing → empty.
        drop(child);
        NotReduction::Reduced(Box::new(Empty), ffi::IteratorType_EMPTY_ITERATOR)
    } else {
        // No reduction applicable.
        NotReduction::NotReduced(child)
    }
}

/// The result of [`new_not_iterator`].
pub enum NewNotIterator<'index> {
    /// The child was trivially reducible; the NOT was replaced by a simpler
    /// iterator via [`not_iterator_reducer`]:
    ///
    /// - Child is [`Empty`] → returns a [`WildcardIterator`](crate::WildcardIterator)
    ///   (NOT nothing = everything).
    /// - Child is a [`WildcardIterator`](crate::WildcardIterator) → returns
    ///   [`Empty`] (NOT everything = nothing).
    Reduced(Box<dyn RQEIterator<'index> + 'index>, IteratorType),
    /// No reduction was possible; a proper NOT iterator was created:
    ///
    /// - [`Not`] when the index does not have an `existingDocs` inverted
    ///   index (sequential scan from 1 to `max_doc_id`).
    /// - [`NotOptimized`] when the index has an `existingDocs` inverted
    ///   index (backed by a [`WildcardIterator`](crate::WildcardIterator)).
    NotReduced(Box<dyn NotIterator<'index> + 'index>, IteratorType),
}

/// Construct a NOT iterator, choosing between [`Not`] (sequential) and
/// [`NotOptimized`] (wildcard-backed) based on the query evaluation context.
///
/// If the child is trivially reducible (empty or wildcard), the reducer is
/// applied first and a simplified iterator is returned directly as
/// [`NewNotIterator::Reduced`].
///
/// # Safety
///
/// 1. `query` must point to a valid [`QueryEvalCtx`](ffi::QueryEvalCtx)
///    that remains valid for `'index`.
/// 2. `query.sctx` must be a non-null pointer to a valid
///    [`RedisSearchCtx`](ffi::RedisSearchCtx) that remains valid for `'index`.
/// 3. `query.sctx.spec` must be a non-null pointer to a valid
///    [`IndexSpec`](ffi::IndexSpec) that remains valid for `'index`.
/// 4. `query.sctx.spec.rule`, when non-null, must point to a valid
///    [`SchemaRule`](ffi::SchemaRule).
/// 5. When the optimized path is taken, all preconditions of
///    [`new_wildcard_iterator`] must also hold.
pub unsafe fn new_not_iterator<'index, I>(
    child: I,
    max_doc_id: t_docId,
    weight: f64,
    timeout: Duration,
    skip_timeout_checks: bool,
    query: NonNull<ffi::QueryEvalCtx>,
) -> NewNotIterator<'index>
where
    I: RQEIterator<'index> + 'index,
{
    // SAFETY: Caller guarantees the preconditions for `new_wildcard_iterator`
    // (used by the reducer when the child is empty).
    let child = match unsafe { not_iterator_reducer(child, weight, query) } {
        NotReduction::Reduced(reduced, iter_type) => {
            return NewNotIterator::Reduced(reduced, iter_type);
        }
        NotReduction::NotReduced(child) => child,
    };

    // Box the child for type-erased child access via `NotIterator`.
    let child: BoxedChild<'index> = Box::new(child);

    // SAFETY: Caller guarantees `query` points to a valid `QueryEvalCtx` (1).
    let query_ref = unsafe { query.as_ref() };
    let sctx = NonNull::new(query_ref.sctx).expect("query.sctx is null");
    // SAFETY: Caller guarantees `query.sctx` is a valid, non-null pointer (2).
    let sctx_ref = unsafe { sctx.as_ref() };
    // SAFETY: Caller guarantees `query.sctx.spec` is a valid, non-null pointer (3).
    let spec = unsafe { &*sctx_ref.spec };

    let has_disk_spec = !spec.diskSpec.is_null();
    let index_all = NonNull::new(spec.rule)
        .map(|rule| {
            // SAFETY: Caller guarantees `spec.rule`, when non-null, points to
            // a valid `SchemaRule` (4).
            unsafe { rule.as_ref() }.index_all
        })
        .unwrap_or(false);

    let optimized = index_all || has_disk_spec;

    if optimized {
        // SAFETY: Caller guarantees the preconditions of
        // `new_wildcard_iterator` hold when the optimized path is taken (5).
        let (wcii, _) = unsafe { new_wildcard_iterator(query, weight) };
        // Upcast to `Box<dyn RQEIterator>` since `Box<dyn WildcardIterator>`
        // does not directly implement `RQEIterator`.
        let wcii: Box<dyn RQEIterator<'index> + 'index> = wcii;
        NewNotIterator::NotReduced(
            Box::new(NotOptimized::new(
                wcii,
                child,
                max_doc_id,
                weight,
                timeout,
                skip_timeout_checks,
            )),
            IteratorType_NOT_ITERATOR,
        )
    } else {
        NewNotIterator::NotReduced(
            Box::new(Not::new(
                child,
                max_doc_id,
                weight,
                timeout,
                skip_timeout_checks,
            )),
            IteratorType_NOT_ITERATOR,
        )
    }
}
