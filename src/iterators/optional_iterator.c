/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "optional_iterator.h"
#include "iterator_api.h"
#include "types_rs.h"
#include "iterators_rs.h"

/**
 * Reduce the optional iterator by applying these rules:
 * 1. If the child is an empty iterator or NULL, return a wildcard iterator
 * 2. If the child is a wildcard iterator, return it
 * 3. Otherwise, return NULL and let the caller create the optional iterator
 */
static QueryIterator *OptionalIteratorReducer(QueryIterator *it, QueryEvalCtx *q, double weight) {
  QueryIterator *ret = NULL;
  if (!it || it->type == EMPTY_ITERATOR) {
    // If the child is NULL, we return a wildcard iterator. All will be virtual hits
    ret = NewWildcardIterator(q, 0);
    if (it) {
      it->Free(it);
    }
  } else if (IsWildcardIterator(it)) {
    // All will be real hits
    ret = it;
    ret->current->weight = weight;
  }
  return ret;
}

// Create a new OPTIONAL iterator
QueryIterator *NewOptionalIterator(QueryIterator *it, QueryEvalCtx *q, double weight) {
  RS_ASSERT(q && q->sctx && q->sctx->spec && q->docTable);
  QueryIterator *ret = OptionalIteratorReducer(it, q, weight);
  if (ret != NULL) {
    return ret;
  }

  bool optimized = q->sctx->spec->rule && q->sctx->spec->rule->index_all;
  optimized |= q && q->sctx && q->sctx->spec && q->sctx->spec->diskSpec;
  t_docId maxDocId = q->docTable->maxDocId;

  if (optimized) {
    ret = NewOptionalOptimizedIterator(q->sctx, it, maxDocId, weight);
  } else {
    ret = NewOptionalNonOptimizedIterator(it, maxDocId, weight);
  }

  return ret;
}

QueryIterator const *GetOptionalIteratorChild(const QueryIterator *base) {
  if (base->type == OPTIONAL_OPTIMIZED_ITERATOR) {
    return GetOptionalOptimizedIteratorChild(base);
  } else {
    return GetOptionalNonOptimizedIteratorChild(base);
  }
}

QueryIterator *TakeOptionalIteratorChild(QueryIterator *base) {
  if (base->type == OPTIONAL_OPTIMIZED_ITERATOR) {
    return TakeOptionalOptimizedIteratorChild(base);
  } else {
    return TakeOptionalNonOptimizedIteratorChild(base);
  }
}

void SetOptionalIteratorChild(QueryIterator *base, QueryIterator *newChild) {
  if (base->type == OPTIONAL_OPTIMIZED_ITERATOR) {
    SetOptionalOptimizedIteratorChild(base, newChild);
  } else {
    SetOptionalNonOptimizedIteratorChild(base, newChild);
  }
}

