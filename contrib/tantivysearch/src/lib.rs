use std::ffi::CString;
use std::ffi::CStr;
use std::mem;
use std::ptr;
use std::slice;
use std::iter::FusedIterator;
use std::cmp::Ordering;

use libc::*;

use tantivy::collector::{TopDocs, Count};
use tantivy::query::QueryParser;
use tantivy::schema::*;
use tantivy::collector::{Collector, SegmentCollector};
use tantivy::{Index, IndexReader, IndexWriter, SegmentReader, SegmentLocalId, DocId, Score, DocAddress, TantivyError};
use tantivy::ReloadPolicy;
use rayon::prelude::*;
use std::sync::Arc;

mod cache;

static CACHE: once_cell::sync::Lazy<cache::ConcurrentCache<(usize, String, u64, bool), Arc<(Vec<u64>, Vec<u64>)>>> = once_cell::sync::Lazy::new(|| {
    cache::ConcurrentCache::with_capacity(100, 3600, 110)
});

const TIMING: bool = true;

macro_rules! start {
    ($val:ident) => {
        let $val = if TIMING {
            Some(std::time::Instant::now())
        } else {
            None
        };
    };
}

macro_rules! end {
    ($val:ident) => {
        if TIMING {
            let $val = $val.unwrap();
            dbg!($val.elapsed());
        }
    };
    ($val:ident, $ex:expr) => {
        if TIMING {
            let $val = $val.unwrap();
            dbg!($val.elapsed(), $ex);
        }
    };
}

#[derive(Default)]
pub struct Docs {
    limit: usize
}

impl Docs {
    pub fn with_limit(limit: usize) -> Docs {
        Docs { limit }
    }
}

impl Collector for Docs {
    type Fruit = Vec<(Score, DocAddress)>;

    type Child = SegmentDocsCollector;

    fn for_segment(
        &self,
        segment_local_id: SegmentLocalId,
        _: &SegmentReader,
    ) -> tantivy::Result<SegmentDocsCollector> {
        Ok(SegmentDocsCollector { docs: vec!(), segment_local_id, limit: self.limit })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, segment_docs: Vec<Vec<(Score, DocAddress)>>) -> tantivy::Result<Vec<(Score, DocAddress)>> {
        start!(merge);
        let lens: Vec<_> = segment_docs.iter().map(|v| v.len()).collect();
        let full_len = lens.iter().sum();

        let mut all = Vec::with_capacity(full_len);
        unsafe { all.set_len(full_len) };

        let mut mut_slice = &mut all[..];
        let mut mut_slices = vec!();
        for len in lens {
            let (slice, rest) = mut_slice.split_at_mut(len);
            mut_slices.push(slice);
            mut_slice = rest;
        }

        segment_docs.into_par_iter().zip(mut_slices.into_par_iter()).for_each(|(vec, slice)| {
            slice.copy_from_slice(&vec[..]);
        });
        end!(merge);

        start!(resize);
        if all.len() > self.limit {
            all.resize(self.limit, (0.0f32, DocAddress(0, 0)));
        }
        end!(resize);

        Ok(all)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct OrdDoc(Score, DocAddress);

impl Ord for OrdDoc {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.partial_cmp(&other.0).unwrap_or(self.1.cmp(&other.1))
    }
}

impl PartialOrd for OrdDoc {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for OrdDoc {
    fn eq(&self, other: &Self) -> bool {
        self.1 == other.1
    }
}

impl Eq for OrdDoc {}

#[derive(Default)]
pub struct RankedDocs {
    limit: usize
}

impl RankedDocs {
    pub fn with_limit(limit: usize) -> RankedDocs {
        RankedDocs { limit }
    }
}

impl Collector for RankedDocs {
    type Fruit = Vec<OrdDoc>;

    type Child = SegmentOrdDocsCollector;

    fn for_segment(
        &self,
        segment_local_id: SegmentLocalId,
        _: &SegmentReader,
    ) -> tantivy::Result<SegmentOrdDocsCollector> {
        Ok(SegmentOrdDocsCollector { docs: vec!(), segment_local_id, limit: self.limit })
    }

    fn requires_scoring(&self) -> bool {
        true
    }

    fn merge_fruits(&self, segment_docs: Vec<Vec<OrdDoc>>) -> tantivy::Result<Vec<OrdDoc>> {
        start!(merge);
        let lens: Vec<_> = segment_docs.iter().map(|v| v.len()).collect();
        let full_len = lens.iter().sum();

        let mut all = Vec::with_capacity(full_len);
        unsafe { all.set_len(full_len) };

        let mut mut_slice = &mut all[..];
        let mut mut_slices = vec!();
        for len in lens {
            let (slice, rest) = mut_slice.split_at_mut(len);
            mut_slices.push(slice);
            mut_slice = rest;
        }

        segment_docs.into_par_iter().zip(mut_slices.into_par_iter()).for_each(|(vec, slice)| {
            slice.copy_from_slice(&vec[..]);
        });
        end!(merge);

        start!(sort);
        all.par_sort();
        end!(sort);

        start!(resize);
        if all.len() > self.limit {
            all.resize(self.limit, OrdDoc(0.0f32, DocAddress(0, 0)));
        }
        end!(resize);

        Ok(all)
    }
}

#[derive(Default)]
pub struct SegmentOrdDocsCollector {
    docs: Vec<OrdDoc>,
    segment_local_id: SegmentLocalId,
    limit: usize
}

impl SegmentCollector for SegmentOrdDocsCollector {
    type Fruit = Vec<OrdDoc>;

    #[inline]
    fn collect(&mut self, doc_id: DocId, score: Score) {
        if self.docs.len() < self.limit {
            self.docs.push(OrdDoc(score, DocAddress(self.segment_local_id, doc_id)));
        }
    }

    fn harvest(self) -> Vec<OrdDoc> {
        self.docs
    }
}

#[derive(Default)]
pub struct SegmentDocsCollector {
    docs: Vec<(Score, DocAddress)>,
    segment_local_id: SegmentLocalId,
    limit: usize
}

impl SegmentCollector for SegmentDocsCollector {
    type Fruit = Vec<(Score, DocAddress)>;

    #[inline]
    fn collect(&mut self, doc_id: DocId, score: Score) {
        if self.docs.len() < self.limit {
            self.docs.push((score, DocAddress(self.segment_local_id, doc_id)));
        }
    }

    fn harvest(self) -> Vec<(Score, DocAddress)> {
        self.docs
    }
}

fn leak_buf(v: Vec<u8>, vallen: *mut size_t) -> *mut c_char {
    unsafe {
        *vallen = v.len();
    }
    let mut bsv = v.into_boxed_slice();
    let val = bsv.as_mut_ptr() as *mut _;
    mem::forget(bsv);
    val
}

// #[no_mangle]
// pub unsafe extern "C" fn tantivy_free_buf(buf: *mut c_char, sz: size_t) {
//     drop(Vec::from_raw_parts(buf, sz, sz));
// }
#[derive(Clone)]
pub struct IterWrapper {
    inner: Arc<(Vec<u64>, Vec<u64>)>,
    offset: usize
}

impl From<Arc<(Vec<u64>, Vec<u64>)>> for IterWrapper {
    fn from(inner: Arc<(Vec<u64>, Vec<u64>)>) -> IterWrapper {
        IterWrapper { inner, offset: 0 }
    }
}

impl Iterator for IterWrapper {
    type Item = (u64, u64);

    #[inline]
    fn next(&mut self) -> Option<(u64, u64)> {
        if self.offset >= self.inner.0.len() {
            None
        } else {
            let result = Some((self.inner.0[self.offset], self.inner.1[self.offset]));
            self.offset += 1;
            result
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let size = self.inner.0.len() - self.offset;
        (size, Some(size))
    }

    #[inline]
    fn count(self) -> usize {
        self.inner.0.len() - self.offset
    }
}

impl FusedIterator for IterWrapper {}


#[derive(Clone)]
pub struct VecIterWrapper {
    iter: std::vec::IntoIter<(u64, u64)>
}

impl From<std::vec::IntoIter<(u64, u64)>> for VecIterWrapper {
    fn from(iter: std::vec::IntoIter<(u64, u64)>) -> VecIterWrapper {
        VecIterWrapper { iter }
    }
}

impl Iterator for VecIterWrapper {
    type Item = (u64, u64);

    #[inline]
    fn next(&mut self) -> Option<(u64, u64)> {
        self.iter.next()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }

    #[inline]
    fn count(self) -> usize {
        self.iter.count()
    }
}

impl DoubleEndedIterator for VecIterWrapper {
    #[inline]
    fn next_back(&mut self) -> Option<(u64, u64)> {
        self.iter.next_back()
    }
}

impl FusedIterator for VecIterWrapper {}

pub struct IndexRW {
    pub path: String,
    pub index: Index,
    pub reader: IndexReader,
    pub writer: IndexWriter
}

#[no_mangle]
pub extern "C" fn tantivysearch_open_or_create_index(dir_ptr: *const c_char) -> *mut IndexRW {
    let dir_c_str = unsafe {
        assert!(!dir_ptr.is_null());

        CStr::from_ptr(dir_ptr)
    };

    let dir_str = dir_c_str.to_str().expect("failed to get &str from cstr");

    println!("Opening index on {}", dir_str);
    let mut index = match Index::open_in_dir(dir_str) {
        Ok(index) => index,
        Err(e) => {
            match e {
                TantivyError::PathDoesNotExist(_) => {
                    println!("Creating index on {}", dir_str);
                    std::fs::create_dir_all(dir_str).expect("failed to create index dir");
                    let mut schema_builder = Schema::builder();
                    schema_builder.add_u64_field("primary_id", FAST);
                    schema_builder.add_u64_field("secondary_id", FAST);
                    schema_builder.add_text_field("body", TEXT);
                    let schema = schema_builder.build();
                    Index::create_in_dir(dir_str, schema).expect("failed to create index")
                }
                _ => {
                    panic!("this should not happen");
                }
            }
        }
    };

    index.set_default_multithread_executor().expect("failed to create thread pool");
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommit)
        .try_into().expect("failed to create reader");
    let writer = index
        .writer(1024 * 1024 * 1024)
        .expect("failed to create writer");

    // let mut policy = tantivy::merge_policy::LogMergePolicy::default();
    // policy.set_max_merge_size(3_000_000);

    // writer.set_merge_policy(Box::new(policy));

    Box::into_raw(Box::new(IndexRW { index, reader, writer, path: dir_str.to_string() }))
}

pub fn tantivysearch_search_impl(irw: *mut IndexRW, query_str: &str, limit: u64) -> Arc<(Vec<u64>, Vec<u64>)> {
    CACHE.resolve((irw as usize, query_str.to_string(), limit, false), move || {
        println!("Searching index for {} with limit {}", query_str, limit);
        let search = std::time::Instant::now();

        let schema = unsafe { (*irw).index.schema() };

        let body = schema.get_field("body").expect("missing field body");
        let primary_id = schema.get_field("primary_id").expect("missing field primary_id");
        let secondary_id = schema.get_field("secondary_id").expect("missing field secondary_id");

        let searcher = unsafe { (*irw).reader.searcher() };
        let segment_readers = searcher.segment_readers();
        let ff_readers_primary: Vec<_> = segment_readers.iter().map(|seg_r| {
            let ffs = seg_r.fast_fields();
            ffs.u64(primary_id).unwrap()
        }).collect();
        let ff_readers_secondary: Vec<_> = segment_readers.iter().map(|seg_r| {
            let ffs = seg_r.fast_fields();
            ffs.u64(secondary_id).unwrap()
        }).collect();


        let query_parser = QueryParser::for_index(unsafe { &(*irw).index }, vec![body]);

        let query = query_parser.parse_query(query_str).expect("failed to parse query");
        let docs = searcher.search(&query, &Docs::with_limit(limit as usize)).expect("failed to search");
        let mut results: (Vec<_>, Vec<_>) = docs.into_par_iter().map(|(_score, doc_address)| {
            let ff_reader_primary = &ff_readers_primary[doc_address.segment_ord() as usize];
            let ff_reader_secondary = &ff_readers_secondary[doc_address.segment_ord() as usize];
            let primary_id: u64 = ff_reader_primary.get(doc_address.doc());
            let secondary_id: u64 = ff_reader_secondary.get(doc_address.doc());
            (primary_id, secondary_id)
        }).unzip();

        dbg!(search.elapsed());
        Arc::new(results)
    })
}

pub fn tantivysearch_ranked_search_impl(irw: *mut IndexRW, query_str: &str, limit: u64) -> Arc<(Vec<u64>, Vec<u64>)> {
    CACHE.resolve((irw as usize, query_str.to_string(), limit, true), move || {
        println!("Searching index for {} with limit {} and ranking", query_str, limit);
        let search = std::time::Instant::now();

        let schema = unsafe { (*irw).index.schema() };

        let body = schema.get_field("body").expect("missing field body");
        let primary_id = schema.get_field("primary_id").expect("missing field primary_id");
        let secondary_id = schema.get_field("secondary_id").expect("missing field secondary_id");

        let searcher = unsafe { (*irw).reader.searcher() };
        let segment_readers = searcher.segment_readers();
        let ff_readers_primary: Vec<_> = segment_readers.iter().map(|seg_r| {
            let ffs = seg_r.fast_fields();
            ffs.u64(primary_id).unwrap()
        }).collect();
        let ff_readers_secondary: Vec<_> = segment_readers.iter().map(|seg_r| {
            let ffs = seg_r.fast_fields();
            ffs.u64(secondary_id).unwrap()
        }).collect();


        let query_parser = QueryParser::for_index(unsafe { &(*irw).index }, vec![body]);

        let query = query_parser.parse_query(query_str).expect("failed to parse query");
        let docs = searcher.search(&query, &RankedDocs::with_limit(limit as usize)).expect("failed to search");
        let mut results: (Vec<_>, Vec<_>) = docs.into_par_iter().map(|OrdDoc(_score, doc_address)| {
            let ff_reader_primary = &ff_readers_primary[doc_address.segment_ord() as usize];
            let ff_reader_secondary = &ff_readers_secondary[doc_address.segment_ord() as usize];
            let primary_id: u64 = ff_reader_primary.get(doc_address.doc());
            let secondary_id: u64 = ff_reader_secondary.get(doc_address.doc());
            (primary_id, secondary_id)
        }).unzip();

        dbg!(search.elapsed());
        Arc::new(results)
    })
}

#[no_mangle]
pub extern "C" fn tantivysearch_search(irw: *mut IndexRW, query_ptr: *const c_char, limit: u64) -> *mut IterWrapper {
    assert!(!irw.is_null());

    let query_c_str = unsafe {
        assert!(!query_ptr.is_null());

        CStr::from_ptr(query_ptr)
    };

    let query_str = query_c_str.to_str().expect("failed to get &str from cstr");

    let results = tantivysearch_search_impl(irw, query_str, limit);

    println!("Search results: {}", results.0.len());

    Box::into_raw(Box::new(results.into()))
}

#[no_mangle]
pub extern "C" fn tantivysearch_ranked_search(irw: *mut IndexRW, query_ptr: *const c_char, limit: u64) -> *mut IterWrapper {
    assert!(!irw.is_null());

    let query_c_str = unsafe {
        assert!(!query_ptr.is_null());

        CStr::from_ptr(query_ptr)
    };

    let query_str = query_c_str.to_str().expect("failed to get &str from cstr");

    let results = tantivysearch_ranked_search_impl(irw, query_str, limit);

    println!("Search results: {}", results.0.len());

    Box::into_raw(Box::new(results.into()))
}

#[no_mangle]
pub extern "C" fn tantivysearch_index(irw: *mut IndexRW, primary_ids: *const u64, secondary_ids: *const u64, chars: *const c_char, offsets: *const u64, size: size_t) -> c_uchar {
    assert!(!irw.is_null());
    assert!(!primary_ids.is_null());
    assert!(!secondary_ids.is_null());
    assert!(!offsets.is_null());
    assert!(!chars.is_null());
    if size == 0 {
        return 1;
    }
    let primary_slice = unsafe { slice::from_raw_parts(primary_ids, size) };
    let secondary_slice = unsafe { slice::from_raw_parts(secondary_ids, size) };
    let offsets_slice = unsafe { slice::from_raw_parts(offsets, size) };
    let chars_len: usize = (*offsets_slice.iter().last().unwrap()) as usize;
    let chars_slice = unsafe { slice::from_raw_parts(chars as *const u8, chars_len) };
    let mut strs = Vec::with_capacity(size);
    let mut current_start = 0;
    for i in 0..size {
        let end: usize = (offsets_slice[i] as usize - 1);
        strs.push(unsafe { std::str::from_utf8_unchecked(&chars_slice[current_start..end]) });
        current_start = end + 1;
    }

    let schema = unsafe { (*irw).index.schema() };

    let body = schema.get_field("body").expect("missing field body");
    let primary_id = schema.get_field("primary_id").expect("missing field primary_id");
    let secondary_id = schema.get_field("secondary_id").expect("missing field secondary_id");

    for i in 0..size {
        let mut doc = Document::default();
        doc.add_u64(primary_id, primary_slice[i]);
        doc.add_u64(secondary_id, secondary_slice[i]);
        doc.add_text(body, strs[i]);
        unsafe { (*irw).writer.add_document(doc) };
    }

    1
}

#[no_mangle]
pub extern "C" fn tantivysearch_writer_commit(irw: *mut IndexRW) -> c_uchar {
    assert!(!irw.is_null());
    match unsafe { (*irw).writer.commit() } {
        Ok(_) => 1,
        Err(e) => {
            eprintln!("Failed to commit writer: {}", e);
            0
        }
    }
}

#[no_mangle]
pub extern "C" fn tantivysearch_index_truncate(irw: *mut IndexRW) -> c_uchar {
    assert!(!irw.is_null());
    match unsafe { (*irw).writer.delete_all_documents() } {
        Ok(_) =>  {
            match unsafe { (*irw).writer.commit() } {
                Ok(_) => 1,
                Err(e) => {
                    eprintln!("Failed to commit writer: {}", e);
                    0
                }
            }
        },
        Err(e) => {
            eprintln!("Failed to delete all documents: {}", e);
            0
        }
    }
}

#[no_mangle]
pub extern "C" fn tantivysearch_iter_next(iter_ptr: *mut IterWrapper, primary_id_ptr: *mut u64, secondary_id_ptr: *mut u64) -> c_uchar {
    assert!(!iter_ptr.is_null());
    match unsafe { (*iter_ptr).next() } {
        Some((primary_id, secondary_id)) => {
            unsafe {
                *primary_id_ptr = primary_id;
                *secondary_id_ptr = secondary_id;
            }
            1
        }
        None => 0
    }
}

#[no_mangle]
pub extern "C" fn tantivysearch_iter_batch(iter_ptr: *mut IterWrapper, count: u64, primary_ids_ptr: *mut u64, secondary_ids_ptr: *mut u64) -> size_t {
    assert!(!iter_ptr.is_null());
    if primary_ids_ptr.is_null() {
        return 0;
    }

    let iter_size = unsafe { (*iter_ptr).inner.0.len() - (*iter_ptr).offset };
    let n_to_write = std::cmp::min(count as usize, iter_size);

    unsafe {
        let src_ptr = (*iter_ptr).inner.0.as_ptr().offset((*iter_ptr).offset as isize);
        std::ptr::copy_nonoverlapping(src_ptr, primary_ids_ptr, n_to_write);
    }

    if !secondary_ids_ptr.is_null() {
        unsafe {
            let src_ptr = (*iter_ptr).inner.1.as_ptr().offset((*iter_ptr).offset as isize);
            std::ptr::copy_nonoverlapping(src_ptr, secondary_ids_ptr, n_to_write);
        }
    }

    unsafe { (*iter_ptr).offset += n_to_write };

    n_to_write
}

#[no_mangle]
pub extern "C" fn tantivysearch_iter_count(iter_ptr: *mut IterWrapper) -> size_t {
    assert!(!iter_ptr.is_null());
    unsafe { (*iter_ptr).inner.0.len() - (*iter_ptr).offset }
}

#[no_mangle]
pub extern "C" fn tantivysearch_iter_free(iter_ptr: *mut IterWrapper) {
    assert!(!iter_ptr.is_null());
    drop(unsafe { Box::from_raw(iter_ptr) });
}

#[no_mangle]
pub extern "C" fn tantivysearch_index_free(irw: *mut IndexRW) {
    assert!(!irw.is_null());
    drop(unsafe { Box::from_raw(irw) });
}

#[no_mangle]
pub extern "C" fn tantivysearch_index_delete(irw: *mut IndexRW) {
    assert!(!irw.is_null());
    let path = unsafe { (*irw).path.clone() };
    std::fs::remove_dir_all(path).expect("failed to delete index");
    println!("removed dir");
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
