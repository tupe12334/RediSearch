/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use ffi::{IteratorType_OPTIONAL_OPTIMIZED_ITERATOR, QueryIterator, RedisSearchCtx, t_docId};
use inverted_index::RSIndexResult;
use rqe_iterators::c2rust::CRQEIterator;
use rqe_iterators::interop::RQEIteratorWrapper;
use rqe_iterators::optional_optimized::OptionalOptimized;
use rqe_iterators::wildcard::WildcardIterator;
use rqe_iterators::{RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome};

/// A wrapper around [`CRQEIterator`] that satisfies the [`WildcardIterator`] marker
/// required by [`OptionalOptimized`]'s `wcii` type parameter.
///
/// The C code stored the wildcard child (`wcii`) as a raw `QueryIterator *`.
/// Here we wrap it so Rust can carry it inside [`OptionalOptimized`] while
/// preserving ownership semantics (the inner [`CRQEIterator`] calls `Free` on drop).
struct OpaqueWildcardIterator(CRQEIterator);

impl<'index> RQEIterator<'index> for OpaqueWildcardIterator {
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        self.0.current()
    }
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        self.0.read()
    }
    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        self.0.skip_to(doc_id)
    }
    fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        self.0.revalidate()
    }
    fn rewind(&mut self) {
        self.0.rewind()
    }
    fn num_estimated(&self) -> usize {
        self.0.num_estimated()
    }
    fn last_doc_id(&self) -> t_docId {
        self.0.last_doc_id()
    }
    fn at_eof(&self) -> bool {
        self.0.at_eof()
    }
    fn is_wildcard(&self) -> bool {
        self.0.is_wildcard()
    }
}

impl<'index> WildcardIterator<'index> for OpaqueWildcardIterator {}

type OptionalOptimizedFfi<'a> = OptionalOptimized<'a, OpaqueWildcardIterator, CRQEIterator>;

#[unsafe(no_mangle)]
/// Create a new optimized optional iterator.
///
/// The wildcard child (`wcii`) is created internally from `sctx` via
/// [`NewWildcardIterator_Optimized`](crate::wildcard::NewWildcardIterator_Optimized).
///
/// # Safety
///
/// 1. `sctx` must satisfy all preconditions of [`NewWildcardIterator_Optimized`].
/// 2. `child` must be a valid non-null owning pointer to a C query iterator.
/// 3. `child` must not be aliased.
pub unsafe extern "C" fn NewOptionalOptimizedIterator(
    sctx: *const RedisSearchCtx,
    child: *mut QueryIterator,
    max_doc_id: t_docId,
    weight: f64,
) -> *mut QueryIterator {
    // SAFETY: thanks to 1
    let wcii_ptr = unsafe { crate::wildcard::NewWildcardIterator_Optimized(sctx, 0.0) };
    let wcii_ptr =
        NonNull::new(wcii_ptr).expect("NewWildcardIterator_Optimized returned a null pointer");
    // SAFETY: NewWildcardIterator_Optimized returns a valid owning QueryIterator pointer
    let wcii = OpaqueWildcardIterator(unsafe { CRQEIterator::new(wcii_ptr) });

    let child = NonNull::new(child)
        .expect("Trying to create an optimized optional iterator with a NULL child");
    // SAFETY: thanks to 2 + 3
    let child = unsafe { CRQEIterator::new(child) };

    RQEIteratorWrapper::boxed_new(
        IteratorType_OPTIONAL_OPTIMIZED_ITERATOR,
        OptionalOptimized::new(wcii, child, max_doc_id, weight),
    )
}

#[unsafe(no_mangle)]
/// Get the child pointer of the optimized optional iterator, or NULL if there is no child.
///
/// # Safety
///
/// 1. `header` must be a valid non-null pointer created via [`NewOptionalOptimizedIterator`].
pub unsafe extern "C" fn GetOptionalOptimizedIteratorChild(
    header: *const QueryIterator,
) -> *const QueryIterator {
    debug_assert!(!header.is_null());
    debug_assert_eq!(
        // SAFETY: Safe thanks to 1
        unsafe { *header }.type_,
        IteratorType_OPTIONAL_OPTIMIZED_ITERATOR,
        "Expected an optimized optional iterator"
    );
    // SAFETY: Safe thanks to 1
    let wrapper =
        unsafe { RQEIteratorWrapper::<OptionalOptimizedFfi>::ref_from_header_ptr(header) };
    wrapper
        .inner
        .child()
        .map(|p| p.as_ref() as *const _)
        .unwrap_or(std::ptr::null())
}

#[unsafe(no_mangle)]
/// Take ownership over the child of the optimized optional iterator,
/// or return NULL if there is no child.
///
/// # Safety
///
/// 1. `header` must be a valid non-null pointer created via [`NewOptionalOptimizedIterator`].
pub unsafe extern "C" fn TakeOptionalOptimizedIteratorChild(
    header: *mut QueryIterator,
) -> *mut QueryIterator {
    debug_assert!(!header.is_null());
    debug_assert_eq!(
        // SAFETY: Safe thanks to 1
        unsafe { *header }.type_,
        IteratorType_OPTIONAL_OPTIMIZED_ITERATOR,
        "Expected an optimized optional iterator"
    );
    // SAFETY: Safe thanks to 1
    let wrapper =
        unsafe { RQEIteratorWrapper::<OptionalOptimizedFfi>::mut_ref_from_header_ptr(header) };
    wrapper
        .inner
        .take_child()
        .map(|p| p.into_raw().as_ptr())
        .unwrap_or(std::ptr::null_mut())
}

#[unsafe(no_mangle)]
/// Set (or overwrite) the child iterator of the optimized optional iterator.
///
/// # Safety
///
/// 1. `header` must be a valid non-null pointer created via [`NewOptionalOptimizedIterator`].
/// 2. `child` must be a valid non-null non-aliased owning pointer to a C query iterator.
pub unsafe extern "C" fn SetOptionalOptimizedIteratorChild(
    header: *mut QueryIterator,
    child: *mut QueryIterator,
) {
    debug_assert!(!header.is_null());
    debug_assert!(!child.is_null());
    debug_assert_eq!(
        // SAFETY: thanks to 1
        unsafe { *header }.type_,
        IteratorType_OPTIONAL_OPTIMIZED_ITERATOR,
        "Expected an optimized optional iterator"
    );
    // SAFETY: thanks to 1
    let wrapper =
        unsafe { RQEIteratorWrapper::<OptionalOptimizedFfi>::mut_ref_from_header_ptr(header) };
    let child =
        NonNull::new(child).expect("Trying to set a NULL child for an optimized optional iterator");
    // SAFETY: thanks to 2
    let child = unsafe { CRQEIterator::new(child) };
    wrapper.inner.set_child(child);
}
