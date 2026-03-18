/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"

#include "src/iterators/optional_iterator.h"
#include "iterators_rs.h"
#include "index_utils.h"
#include "inverted_index.h"
#include "types_rs.h"


// Test OptionalIteratorReducer optimizations
class OptionalIteratorReducerTest : public ::testing::Test {};

TEST_F(OptionalIteratorReducerTest, TestOptionalWithNullChild) {
  // Test rule 1: If the child is NULL, return a wildcard iterator
  t_docId maxDocId = 100;
  size_t numDocs = 50;
  double weight = 2.0;

  // Create a mock QueryEvalCtx
  MockQueryEvalCtx ctx(maxDocId, numDocs);

  // Create optional iterator with NULL child
  QueryIterator *it = NewOptionalIterator(nullptr, &ctx.qctx, weight);

  // Verify iterator type
  ASSERT_TRUE(it->type == WILDCARD_ITERATOR);

  // Read first document and check properties
  ASSERT_EQ(it->Read(it), ITERATOR_OK);
  ASSERT_EQ(it->current->docId, 1);
  ASSERT_EQ(it->current->weight, 0);
  ASSERT_EQ(it->current->data.tag, RSResultData_Virtual);

  it->Free(it);
}

TEST_F(OptionalIteratorReducerTest, TestOptionalWithEmptyChild) {
  // Test rule 1: If the child is an empty iterator, return a wildcard iterator
  t_docId maxDocId = 100;
  size_t numDocs = 50;
  double weight = 2.0;

  // Create a mock QueryEvalCtx
  MockQueryEvalCtx ctx(maxDocId, numDocs);

  // Create empty child iterator
  QueryIterator *emptyChild = NewEmptyIterator();

  // Create optional iterator with empty child
  QueryIterator *it = NewOptionalIterator(emptyChild, &ctx.qctx, weight);

  // Verify iterator type
  ASSERT_TRUE(it->type == WILDCARD_ITERATOR);

  // Read first document and check properties
  ASSERT_EQ(it->Read(it), ITERATOR_OK);
  ASSERT_EQ(it->current->docId, 1);
  ASSERT_EQ(it->current->weight, 0);
  ASSERT_EQ(it->current->data.tag, RSResultData_Virtual);

  it->Free(it);
}

TEST_F(OptionalIteratorReducerTest, TestOptionalWithWildcardChild) {
  // Test rule 2: If the child is a wildcard iterator, return it directly
  t_docId maxDocId = 100;
  size_t numDocs = 50;
  double childWeight = 3.0;

  // Create a mock QueryEvalCtx
  MockQueryEvalCtx ctx(maxDocId, numDocs);

  // Create wildcard child iterator
  QueryIterator *wildcardChild = NewWildcardIterator_NonOptimized(maxDocId, 2.0);

  // Create optional iterator with wildcard child - should return the child directly
  QueryIterator *it = NewOptionalIterator(wildcardChild, &ctx.qctx, childWeight);

  // Verify it's the same iterator (optimization returns child directly)
  ASSERT_TRUE(it->type == WILDCARD_ITERATOR);
  ASSERT_EQ(it, wildcardChild);

  // Read first document and check properties - should have child's weight
  ASSERT_EQ(it->Read(it), ITERATOR_OK);
  ASSERT_EQ(it->current->docId, 1);
  ASSERT_EQ(it->current->weight, childWeight);
  ASSERT_EQ(it->current->data.tag, RSResultData_Virtual);

  it->Free(it);
}

TEST_F(OptionalIteratorReducerTest, TestOptionalWithReaderWildcardChild) {
  t_docId maxDocId = 100;
  size_t numDocs = 50;
  double childWeight = 3.0;

  // Create a mock QueryEvalCtx
  MockQueryEvalCtx ctx(maxDocId, numDocs);
  size_t memsize;
  InvertedIndex *idx = NewInvertedIndex(static_cast<IndexFlags>(Index_DocIdsOnly), &memsize);
  ASSERT_TRUE(idx != nullptr);
  for (t_docId i = 1; i < 1000; ++i) {
    auto res = (RSIndexResult) {
      .docId = i,
      .data = {.tag = RSResultData_Virtual},
    };
    InvertedIndex_WriteEntryGeneric(idx, &res);
  }
  MockQueryEvalCtx mockQctx(1000, 1000);
  QueryIterator *wildcardChild = NewInvIndIterator_WildcardQuery(idx, &mockQctx.sctx, 1.0);
  // Create optional iterator with wildcard child - should return the child directly
  QueryIterator *it = NewOptionalIterator(wildcardChild, &ctx.qctx, 2.0);

  // Verify it's the same iterator (optimization returns child directly)
  ASSERT_TRUE(it->type == INV_IDX_WILDCARD_ITERATOR);
  ASSERT_EQ(it, wildcardChild);
  it->Free(it);
  InvertedIndex_Free(idx);
}
