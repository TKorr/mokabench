use super::{CacheDriver, Counters, Key, Value};
use crate::{config::Config, parser::TraceEntry, report::Report};

use std::sync::Arc;
use parking_lot::Mutex;

// Import cachekit types
use cachekit::builder::{Cache, CacheBuilder, CachePolicy};

/// Wrapper for cachekit cache implementations
pub struct CachekitCache {
    config: Arc<Config>,
    cache: Arc<Mutex<Cache<Key, Value>>>,
}

impl Clone for CachekitCache {
    fn clone(&self) -> Self {
        Self {
            config: Arc::clone(&self.config),
            cache: Arc::clone(&self.cache),
        }
    }
}

impl CachekitCache {
    pub fn new(config: &Config, capacity: usize, policy: CachePolicy) -> Self {
        if config.ttl.is_some() {
            todo!("TTL not yet supported for cachekit");
        }
        if config.tti.is_some() {
            todo!("TTI not yet supported for cachekit");
        }
        if config.size_aware {
            todo!("Size-aware caching not yet supported for cachekit");
        }

        let cache = CacheBuilder::new(capacity).build::<Key, Value>(policy);

        Self {
            config: Arc::new(config.clone()),
            cache: Arc::new(Mutex::new(cache)),
        }
    }

    fn get(&self, key: &Key) -> bool {
        self.cache.lock().get(key).is_some()
    }

    fn insert(&self, key: Key, req_id: usize) {
        let value = super::make_value(&self.config, key, req_id);
        super::sleep_thread_for_insertion(&self.config);
        self.cache.lock().insert(key, value);
    }
}

impl CacheDriver<TraceEntry> for CachekitCache {
    fn get_or_insert(&mut self, entry: &TraceEntry, report: &mut Report) {
        let mut counters = Counters::default();
        let mut req_id = entry.line_number();

        for block in entry.range() {
            if self.get(&block) {
                counters.read_hit();
            } else {
                self.insert(block, req_id);
                counters.inserted();
                counters.read_missed();
            }
            req_id += 1;
        }

        counters.add_to_report(report);
    }

    fn get_or_insert_once(&mut self, _entry: &TraceEntry, _report: &mut Report) {
        unimplemented!();
    }

    fn update(&mut self, entry: &TraceEntry, report: &mut Report) {
        let mut counters = Counters::default();
        let mut req_id = entry.line_number();

        for block in entry.range() {
            self.insert(block, req_id);
            counters.inserted();
            req_id += 1;
        }

        counters.add_to_report(report);
    }
}

// Helper functions to create specific cache instances
pub fn create_lru_cache(config: &Config, capacity: usize) -> impl CacheDriver<TraceEntry> + Clone + Send {
    CachekitCache::new(config, capacity, CachePolicy::Lru)
}

pub fn create_fifo_cache(config: &Config, capacity: usize) -> impl CacheDriver<TraceEntry> + Clone + Send {
    CachekitCache::new(config, capacity, CachePolicy::Fifo)
}

pub fn create_lfu_cache(config: &Config, capacity: usize) -> impl CacheDriver<TraceEntry> + Clone + Send {
    CachekitCache::new(config, capacity, CachePolicy::Lfu { bucket_hint: None })
}

pub fn create_lru_k_cache(config: &Config, capacity: usize) -> impl CacheDriver<TraceEntry> + Clone + Send {
    CachekitCache::new(config, capacity, CachePolicy::LruK { k: 2 })
}

pub fn create_s3_fifo_cache(config: &Config, capacity: usize) -> impl CacheDriver<TraceEntry> + Clone + Send {
    CachekitCache::new(
        config,
        capacity,
        CachePolicy::S3Fifo {
            small_ratio: 0.1,
            ghost_ratio: 0.9,
        },
    )
}

pub fn create_two_q_cache(config: &Config, capacity: usize) -> impl CacheDriver<TraceEntry> + Clone + Send {
    CachekitCache::new(
        config,
        capacity,
        CachePolicy::TwoQ {
            probation_frac: 0.25,
        },
    )
}

pub fn create_clock_cache(config: &Config, capacity: usize) -> impl CacheDriver<TraceEntry> + Clone + Send {
    CachekitCache::new(config, capacity, CachePolicy::Clock)
}

pub fn create_clock_pro_cache(config: &Config, capacity: usize) -> impl CacheDriver<TraceEntry> + Clone + Send {
    CachekitCache::new(config, capacity, CachePolicy::ClockPro)
}

pub fn create_slru_cache(config: &Config, capacity: usize) -> impl CacheDriver<TraceEntry> + Clone + Send {
    CachekitCache::new(
        config,
        capacity,
        CachePolicy::Slru {
            probationary_frac: 0.2,
        },
    )
}

pub fn create_nru_cache(config: &Config, capacity: usize) -> impl CacheDriver<TraceEntry> + Clone + Send {
    CachekitCache::new(config, capacity, CachePolicy::Nru)
}

pub fn create_random_cache(config: &Config, capacity: usize) -> impl CacheDriver<TraceEntry> + Clone + Send {
    CachekitCache::new(config, capacity, CachePolicy::Random)
}

pub fn create_lifo_cache(config: &Config, capacity: usize) -> impl CacheDriver<TraceEntry> + Clone + Send {
    CachekitCache::new(config, capacity, CachePolicy::Lifo)
}

pub fn create_heap_lfu_cache(config: &Config, capacity: usize) -> impl CacheDriver<TraceEntry> + Clone + Send {
    CachekitCache::new(config, capacity, CachePolicy::HeapLfu)
}

pub fn create_mfu_cache(config: &Config, capacity: usize) -> impl CacheDriver<TraceEntry> + Clone + Send {
    CachekitCache::new(config, capacity, CachePolicy::Mfu {
        bucket_hint: None
    })
}

pub fn create_mru_cache(config: &Config, capacity: usize) -> impl CacheDriver<TraceEntry> + Clone + Send {
    CachekitCache::new(config, capacity, CachePolicy::Mru)
}

pub fn create_arc_cache(config: &Config, capacity: usize) -> impl CacheDriver<TraceEntry> + Clone + Send {
    CachekitCache::new(config, capacity, CachePolicy::Arc)
}

pub fn create_fast_lru_cache(config: &Config, capacity: usize) -> impl CacheDriver<TraceEntry> + Clone + Send {
    CachekitCache::new(config, capacity, CachePolicy::FastLru)
}
