use std::ffi::CString;
use std::ffi::CStr;
use std::mem;
use std::ptr;
use std::slice;
use std::iter::FusedIterator;

use libc::*;

use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::*;
use tantivy::collector::{Collector, SegmentCollector};
use tantivy::{Index, IndexReader, SegmentReader, SegmentLocalId, DocId, Score, DocAddress};
use tantivy::ReloadPolicy;

pub struct Docs;

impl Collector for Docs {
    type Fruit = Vec<(Score, DocAddress)>;

    type Child = SegmentDocsCollector;

    fn for_segment(
        &self,
        segment_local_id: SegmentLocalId,
        _: &SegmentReader,
    ) -> tantivy::Result<SegmentDocsCollector> {
        Ok(SegmentDocsCollector { docs: vec!(), segment_local_id })
    }

    fn requires_scoring(&self) -> bool {
        true
    }

    fn merge_fruits(&self, segment_docs: Vec<Vec<(Score, DocAddress)>>) -> tantivy::Result<Vec<(Score, DocAddress)>> {
        let mut all = segment_docs.into_iter().fold(None, |acc: Option<Vec<(Score, DocAddress)>>, vec| {
            match acc {
                Some(mut v) => {
                    v.extend(vec);
                    Some(v)
                },
                None => Some(vec)
            }
        }).unwrap_or_else(|| vec!());

        all.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        Ok(all)
    }
}

#[derive(Default)]
pub struct SegmentDocsCollector {
    docs: Vec<(Score, DocAddress)>,
    segment_local_id: SegmentLocalId
}

impl SegmentCollector for SegmentDocsCollector {
    type Fruit = Vec<(Score, DocAddress)>;

    fn collect(&mut self, doc_id: DocId, score: Score) {
        self.docs.push((score, DocAddress(self.segment_local_id, doc_id)));
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
    iter: std::vec::IntoIter<u64>
}

impl From<std::vec::IntoIter<u64>> for IterWrapper {
    fn from(iter: std::vec::IntoIter<u64>) -> IterWrapper {
        IterWrapper { iter }
    }
}

impl Iterator for IterWrapper {
    type Item = u64;

    #[inline]
    fn next(&mut self) -> Option<u64> {
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

impl DoubleEndedIterator for IterWrapper {
    #[inline]
    fn next_back(&mut self) -> Option<u64> {
        self.iter.next_back()
    }
}

impl FusedIterator for IterWrapper {}

pub struct IndexAndReader {
    pub index: Index,
    pub reader: IndexReader
}

// struct VecIter<T> {
//     inner: Vec<T>
// }
//
// impl<T> VecIter<T> {
//     fn new(inner: Vec<T>) -> VecIter {
//         VecIter { inner }
//     }
// }

#[no_mangle]
pub extern "C" fn tantivysearch_hello() {
    println!("HELLO from rust ++++++++++++++++++++++++");
}

#[no_mangle]
pub extern "C" fn tantivysearch_open_index(dir_ptr: *const c_char) -> *mut IndexAndReader {
    let dir_c_str = unsafe {
        assert!(!dir_ptr.is_null());

        CStr::from_ptr(dir_ptr)
    };

    let dir_str = dir_c_str.to_str().expect("failed to get &str from cstr");

    println!("Opening index on {}", dir_str);
    let mut index = Index::open_in_dir(dir_str).expect("failed to open index");
    index.set_default_multithread_executor().expect("failed to create thread pool");
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommit)
        .try_into().expect("failed to create reader");

    Box::into_raw(Box::new(IndexAndReader { index, reader }))
}

#[no_mangle]
pub extern "C" fn tantivysearch_search(inr: *mut IndexAndReader, query_ptr: *const c_char) -> *mut IterWrapper {
    assert!(!inr.is_null());

    let query_c_str = unsafe {
        assert!(!query_ptr.is_null());

        CStr::from_ptr(query_ptr)
    };

    let query_str = query_c_str.to_str().expect("failed to get &str from cstr");

    println!("Searching index for {}", query_str);

    let schema = unsafe { (*inr).index.schema() };

    let body = schema.get_field("body").expect("missing field body");
    let proc_id = schema.get_field("proc_id").expect("missing field proc_id");

    let searcher = unsafe { (*inr).reader.searcher() };
    let segment_readers = searcher.segment_readers();
    let ff_readers: Vec<_> = segment_readers.iter().map(|seg_r| {
        let ffs = seg_r.fast_fields();
        ffs.u64(proc_id).unwrap()
    }).collect();

    let query_parser = QueryParser::for_index(unsafe { &(*inr).index }, vec![body]);

    let query = query_parser.parse_query(query_str).expect("failed to parse query");
    let docs = searcher.search(&query, &Docs).expect("failed to search");
    let mut results = vec!();
    for (_score, doc_address) in docs {
        let ff_reader = &ff_readers[doc_address.segment_ord() as usize];
        let value: u64 = ff_reader.get(doc_address.doc());
        results.push(value);
    }

    println!("Search results: {}", results.len());

    Box::into_raw(Box::new(results.into_iter().into()))
}

#[no_mangle]
pub extern "C" fn tantivysearch_iter_next(iter_ptr: *mut IterWrapper, value_ptr: *mut u64) -> c_uchar {
    match unsafe { (*iter_ptr).next() } {
        Some(data) => {
            unsafe {
                *value_ptr = data;
            }
            1
        }
        None => 0
    }
}

#[no_mangle]
pub extern "C" fn tantivysearch_iter_free(iter_ptr: *mut IterWrapper) {
    drop(unsafe { Box::from_raw(iter_ptr) });
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
