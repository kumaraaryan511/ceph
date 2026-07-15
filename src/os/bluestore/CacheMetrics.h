// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
* Ceph - scalable distributed file system
*
* Copyright (C) 2024 Red Hat
*
* This is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License version 2.1, as published by the Free Software
* Foundation.  See file COPYING.
*
*/


#ifndef CEPH_BLUESTORE_CACHE_METRICS_H
#define CEPH_BLUESTORE_CACHE_METRICS_H


#include <array>
#include <atomic>
#include "include/ceph_assert.h"
#include "common/ceph_mutex.h"
#include "common/ceph_time.h"


/**
* CachePerformanceMetrics
*
* Tracks cache performance metrics with both short-term (5-minute rolling window)
* and long-term (since startup) averages. Measures:
* - M (miss cost): Average time to reconstruct/fetch on cache miss
* - F (frequency): Cache access rate (accesses per second)
* - R (hit ratio): Percentage of cache hits
* - Cache Gain: M × F × R (performance boost from caching)
*
* Thread-safe with minimal locking overhead using atomic operations for
* the current sample and mutex-protected rotation.
*/
class CachePerformanceMetrics {
public:
 struct Metrics {
   double hit_ratio = 0.0;           // 0.0 to 1.0
   double avg_miss_cost_us = 0.0;    // microseconds
   double access_frequency = 0.0;    // accesses per second
   double cache_gain = 0.0;          // M × F × R
   uint64_t total_accesses = 0;
   uint64_t total_hits = 0;
   uint64_t total_misses = 0;
  
   Metrics() = default;
   Metrics(double hr, double cost, double freq, double gain,
           uint64_t acc, uint64_t hits, uint64_t miss)
     : hit_ratio(hr), avg_miss_cost_us(cost), access_frequency(freq),
       cache_gain(gain), total_accesses(acc), total_hits(hits),
       total_misses(miss) {}
 };


private:
 // Rolling window configuration
 static constexpr size_t WINDOW_SIZE_SECONDS = 300; // 5 minutes
  struct Sample {
   std::atomic<uint64_t> accesses{0};
   std::atomic<uint64_t> hits{0};
   std::atomic<uint64_t> misses{0};
   std::atomic<uint64_t> total_miss_cost_ns{0}; // nanoseconds for precision
   ceph::mono_time timestamp;
  
   Sample() : timestamp(ceph::mono_clock::zero()) {}
  
   void reset(ceph::mono_time ts) {
     accesses.store(0, std::memory_order_relaxed);
     hits.store(0, std::memory_order_relaxed);
     misses.store(0, std::memory_order_relaxed);
     total_miss_cost_ns.store(0, std::memory_order_relaxed);
     timestamp = ts;
   }
  
   // Get snapshot of sample (not atomic across all fields)
   void snapshot(uint64_t& acc, uint64_t& h, uint64_t& m, uint64_t& cost) const {
     acc = accesses.load(std::memory_order_relaxed);
     h = hits.load(std::memory_order_relaxed);
     m = misses.load(std::memory_order_relaxed);
     cost = total_miss_cost_ns.load(std::memory_order_relaxed);
   }
 };
  // Circular buffer for rolling window
 std::array<Sample, WINDOW_SIZE_SECONDS> samples;
 std::atomic<size_t> current_index{0};
  // Current second accumulator (lock-free for high-frequency updates)
 Sample current_sample;
  // Since-startup totals (updated during rotation)
 std::atomic<uint64_t> total_accesses{0};
 std::atomic<uint64_t> total_hits{0};
 std::atomic<uint64_t> total_misses{0};
 std::atomic<uint64_t> total_miss_cost_ns{0};
  ceph::mono_time start_time;
 ceph::mono_time last_rotation;
  // Mutex only for rotation (not for recording hits/misses)
 mutable ceph::mutex rotation_lock = ceph::make_mutex("CachePerformanceMetrics::rotation");


public:
 CachePerformanceMetrics() {
   start_time = ceph::mono_clock::now();
   last_rotation = start_time;
   current_sample.reset(start_time);
  
   // Initialize all samples
   for (auto& sample : samples) {
     sample.reset(ceph::mono_clock::zero());
   }
 }
  /**
  * Record a cache hit
  * Lock-free, safe to call from any thread
  */
 void record_hit() {
   current_sample.accesses.fetch_add(1, std::memory_order_relaxed);
   current_sample.hits.fetch_add(1, std::memory_order_relaxed);
   maybe_rotate_sample();
 }
  /**
  * Record a cache miss with its cost
  * @param cost_us Cost of the miss in microseconds
  * Lock-free, safe to call from any thread
  */
 void record_miss(double cost_us) {
   uint64_t cost_ns = static_cast<uint64_t>(cost_us * 1000.0);
   current_sample.accesses.fetch_add(1, std::memory_order_relaxed);
   current_sample.misses.fetch_add(1, std::memory_order_relaxed);
   current_sample.total_miss_cost_ns.fetch_add(cost_ns, std::memory_order_relaxed);
   maybe_rotate_sample();
 }
  /**
  * Get short-term metrics (5-minute rolling window)
  */
 Metrics get_short_term_metrics() const {
   std::lock_guard l(rotation_lock);
  
   uint64_t window_accesses = 0;
   uint64_t window_hits = 0;
   uint64_t window_misses = 0;
   uint64_t window_cost_ns = 0;
  
   // Sum all samples in the window
   for (const auto& sample : samples) {
     uint64_t acc, hits, miss, cost;
     sample.snapshot(acc, hits, miss, cost);
     window_accesses += acc;
     window_hits += hits;
     window_misses += miss;
     window_cost_ns += cost;
   }
  
   // Add current sample
   uint64_t curr_acc, curr_hits, curr_miss, curr_cost;
   current_sample.snapshot(curr_acc, curr_hits, curr_miss, curr_cost);
   window_accesses += curr_acc;
   window_hits += curr_hits;
   window_misses += curr_miss;
   window_cost_ns += curr_cost;
  
   return calculate_metrics(window_accesses, window_hits, window_misses,
                           window_cost_ns, WINDOW_SIZE_SECONDS);
 }
  /**
  * Get long-term metrics (since startup)
  */
 Metrics get_long_term_metrics() const {
   uint64_t acc = total_accesses.load(std::memory_order_relaxed);
   uint64_t hits = total_hits.load(std::memory_order_relaxed);
   uint64_t miss = total_misses.load(std::memory_order_relaxed);
   uint64_t cost_ns = total_miss_cost_ns.load(std::memory_order_relaxed);
  
   // Add current sample
   uint64_t curr_acc, curr_hits, curr_miss, curr_cost;
   current_sample.snapshot(curr_acc, curr_hits, curr_miss, curr_cost);
   acc += curr_acc;
   hits += curr_hits;
   miss += curr_miss;
   cost_ns += curr_cost;
  
   auto now = ceph::mono_clock::now();
   double duration_sec = std::chrono::duration<double>(now - start_time).count();
  
   return calculate_metrics(acc, hits, miss, cost_ns, duration_sec);
 }


private:
 /**
  * Check if we need to rotate to a new sample (called on every access)
  * Uses try_lock to avoid blocking on rotation
  */
 void maybe_rotate_sample() {
   auto now = ceph::mono_clock::now();
   auto elapsed = std::chrono::duration<double>(now - last_rotation).count();
  
   // Rotate approximately every second
   if (elapsed >= 1.0) {
     // Try to acquire lock; if busy, skip rotation (will happen on next call)
     std::unique_lock l(rotation_lock, std::try_to_lock);
     if (l.owns_lock()) {
       // Double-check after acquiring lock
       elapsed = std::chrono::duration<double>(now - last_rotation).count();
       if (elapsed >= 1.0) {
         rotate_sample(now);
       }
     }
   }
 }
  /**
  * Rotate current sample into the circular buffer
  * Must be called with rotation_lock held
  */
 void rotate_sample(ceph::mono_time now) {
   // Get snapshot of current sample
   uint64_t acc, hits, miss, cost_ns;
   current_sample.snapshot(acc, hits, miss, cost_ns);
  
   // Update totals
   total_accesses.fetch_add(acc, std::memory_order_relaxed);
   total_hits.fetch_add(hits, std::memory_order_relaxed);
   total_misses.fetch_add(miss, std::memory_order_relaxed);
   total_miss_cost_ns.fetch_add(cost_ns, std::memory_order_relaxed);
  
   // Move to circular buffer
   size_t idx = current_index.load(std::memory_order_relaxed);
   samples[idx].accesses.store(acc, std::memory_order_relaxed);
   samples[idx].hits.store(hits, std::memory_order_relaxed);
   samples[idx].misses.store(miss, std::memory_order_relaxed);
   samples[idx].total_miss_cost_ns.store(cost_ns, std::memory_order_relaxed);
   samples[idx].timestamp = last_rotation;
  
   // Advance index
   current_index.store((idx + 1) % WINDOW_SIZE_SECONDS, std::memory_order_relaxed);
  
   // Reset current sample
   current_sample.reset(now);
   last_rotation = now;
 }
  /**
  * Calculate metrics from raw counters
  */
 Metrics calculate_metrics(uint64_t accesses, uint64_t hits, uint64_t misses,
                          uint64_t total_cost_ns, double duration_sec) const {
   if (accesses == 0 || duration_sec <= 0.0) {
     return Metrics();
   }
  
   double hit_ratio = static_cast<double>(hits) / static_cast<double>(accesses);
   double avg_miss_cost_us = 0.0;
  
   if (misses > 0) {
     // Convert nanoseconds to microseconds
     avg_miss_cost_us = (static_cast<double>(total_cost_ns) / 1000.0) /
                        static_cast<double>(misses);
   }
  
   double access_frequency = static_cast<double>(accesses) / duration_sec;
  
   // Cache gain = M × F × R
   // This represents the total time saved per second due to caching
   double cache_gain = avg_miss_cost_us * access_frequency * hit_ratio;
  
   return Metrics(hit_ratio, avg_miss_cost_us, access_frequency, cache_gain,
                 accesses, hits, misses);
 }
};


#endif // CEPH_BLUESTORE_CACHE_METRICS_H


// Made with Bob



