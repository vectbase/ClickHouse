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
use tantivy::{Index, IndexReader, IndexWriter, SegmentReader, SegmentLocalId, DocId, Score, DocAddress, TantivyError};
use tantivy::ReloadPolicy;
use rayon::prelude::*;

mod cache;

static CACHE: once_cell::sync::Lazy<cache::ConcurrentCache<(String, u64), Vec<(u64, u64)>>> = once_cell::sync::Lazy::new(|| {
    cache::ConcurrentCache::with_capacity(100, 60, 110)
});

#[derive(Default)]
pub struct Docs {
    limit: u64
}

impl Docs {
    pub fn with_limit(limit: u64) -> Docs {
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
        let mut all = segment_docs.into_iter().fold(None, |acc: Option<Vec<(Score, DocAddress)>>, vec| {
            match acc {
                Some(mut v) => {
                    v.extend(vec);
                    Some(v)
                },
                None => Some(vec)
            }
        }).unwrap_or_else(|| vec!());

        // all.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        if all.len() > self.limit as usize {
            all.resize(self.limit as usize, (0.0f32, DocAddress(0, 0)));
        }

        Ok(all)
    }
}

#[derive(Default)]
pub struct SegmentDocsCollector {
    docs: Vec<(Score, DocAddress)>,
    segment_local_id: SegmentLocalId,
    limit: u64
}

impl SegmentCollector for SegmentDocsCollector {
    type Fruit = Vec<(Score, DocAddress)>;

    fn collect(&mut self, doc_id: DocId, score: Score) {
        if self.docs.len() < self.limit as usize {
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
    iter: std::vec::IntoIter<(u64, u64)>
}

impl From<std::vec::IntoIter<(u64, u64)>> for IterWrapper {
    fn from(iter: std::vec::IntoIter<(u64, u64)>) -> IterWrapper {
        IterWrapper { iter }
    }
}

impl Iterator for IterWrapper {
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

impl DoubleEndedIterator for IterWrapper {
    #[inline]
    fn next_back(&mut self) -> Option<(u64, u64)> {
        self.iter.next_back()
    }
}

impl FusedIterator for IterWrapper {}

pub struct IndexRW {
    pub path: String,
    pub index: Index,
    pub reader: IndexReader,
    pub writer: IndexWriter
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
                    schema_builder.add_u64_field("proc_id", FAST);
                    schema_builder.add_u64_field("mov_id", FAST);
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

    Box::into_raw(Box::new(IndexRW { index, reader, writer, path: dir_str.to_string() }))
}

// #[cached(
//     size=100,
//     time=3600,
//     key = "(String, u64)",
//     convert = r#"{ (query_str.to_string(), limit) }"#
// )]
pub fn tantivysearch_search_impl(irw: *mut IndexRW, query_str: &str, limit: u64) -> Vec<(u64, u64)> {
    CACHE.resolve((query_str.to_string(), limit), move || {
        println!("Searching index for {}", query_str);

        let schema = unsafe { (*irw).index.schema() };

        let body = schema.get_field("body").expect("missing field body");
        let proc_id = schema.get_field("proc_id").expect("missing field proc_id");
        let mov_id = schema.get_field("mov_id").expect("missing field mov_id");

        let searcher = unsafe { (*irw).reader.searcher() };
        let segment_readers = searcher.segment_readers();
        let ff_readers_proc: Vec<_> = segment_readers.iter().map(|seg_r| {
            let ffs = seg_r.fast_fields();
            ffs.u64(proc_id).unwrap()
        }).collect();
        let ff_readers_mov: Vec<_> = segment_readers.iter().map(|seg_r| {
            let ffs = seg_r.fast_fields();
            ffs.u64(mov_id).unwrap()
        }).collect();


        let query_parser = QueryParser::for_index(unsafe { &(*irw).index }, vec![body]);

        let query = query_parser.parse_query(query_str).expect("failed to parse query");
        let docs = searcher.search(&query, &Docs::with_limit(limit)).expect("failed to search");
        let mut results: Vec<_> = docs.into_par_iter().map(|(_score, doc_address)| {
            let ff_reader_proc = &ff_readers_proc[doc_address.segment_ord() as usize];
            let ff_reader_mov = &ff_readers_mov[doc_address.segment_ord() as usize];
            let proc_id: u64 = ff_reader_proc.get(doc_address.doc());
            let mov_id: u64 = ff_reader_mov.get(doc_address.doc());
            (proc_id, mov_id)
        }).collect();

        results
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

    println!("Search results: {}", results.len());

    Box::into_raw(Box::new(results.into_iter().into()))
}

#[no_mangle]
pub extern "C" fn tantivysearch_index(irw: *mut IndexRW, proc_ids: *const u64, mov_ids: *const u64, chars: *const c_char, offsets: *const u64, size: size_t) -> c_uchar {
    assert!(!irw.is_null());
    assert!(!proc_ids.is_null());
    assert!(!mov_ids.is_null());
    assert!(!offsets.is_null());
    assert!(!chars.is_null());
    if size == 0 {
        return 1;
    }
    let proc_slice = unsafe { slice::from_raw_parts(proc_ids, size) };
    let mov_slice = unsafe { slice::from_raw_parts(mov_ids, size) };
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
    let proc_id = schema.get_field("proc_id").expect("missing field proc_id");
    let mov_id = schema.get_field("mov_id").expect("missing field mov_id");

    for i in 0..size {
        let mut doc = Document::default();
        doc.add_u64(proc_id, proc_slice[i]);
        doc.add_u64(mov_id, mov_slice[i]);
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
pub extern "C" fn tantivysearch_iter_next(iter_ptr: *mut IterWrapper, proc_id_ptr: *mut u64, mov_id_ptr: *mut u64) -> c_uchar {
    assert!(!iter_ptr.is_null());
    match unsafe { (*iter_ptr).next() } {
        Some((proc_id, mov_id)) => {
            unsafe {
                *proc_id_ptr = proc_id;
                *mov_id_ptr = mov_id;
            }
            1
        }
        None => 0
    }
}

#[no_mangle]
pub extern "C" fn tantivysearch_iter_count(iter_ptr: *mut IterWrapper) -> size_t {
    assert!(!iter_ptr.is_null());
    unsafe { (*iter_ptr).iter.as_slice().len() }
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
