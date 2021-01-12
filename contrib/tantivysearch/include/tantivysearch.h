// SPDX-License-Identifier: Apache-2.0

#ifndef TANTIVYSEARCH_H
#define TANTIVYSEARCH_H

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

struct TantivySearchIndexAndReader;

struct TantivySearchIterWrapper;

extern "C" {

void tantivysearch_hello();

TantivySearchIndexAndReader *tantivysearch_open_index(const char *dir_ptr);

TantivySearchIterWrapper *tantivysearch_search(TantivySearchIndexAndReader *inr,
                                               const char *query_ptr);

unsigned char tantivysearch_iter_next(TantivySearchIterWrapper *iter_ptr, uint64_t *value_ptr);

void tantivysearch_iter_free(TantivySearchIterWrapper *iter_ptr);

} // extern "C"

#endif // TANTIVYSEARCH_H
