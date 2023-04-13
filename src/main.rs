use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
    thread,
};

use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;

#[derive(Clone)]
struct Entry<K: Clone> {
    second_chance: bool,
    key: K,
}

struct MapEntry<V> {
    value: V,
    index: usize,
}

pub struct ClockDashMap<K: Clone, V> {
    shards: Vec<Arc<Mutex<ClockMap<K, V>>>>,
    next_modify: Arc<Mutex<usize>>,
}

pub struct ClockMap<K: Clone, V> {
    value_map: HashMap<K, MapEntry<V>>,
    clock_list: Vec<Option<Entry<K>>>,
    pointer: usize,
    miss: i32,
    capacity: usize,
}

impl<K: std::cmp::Eq + std::hash::Hash + Clone, V: Clone> ClockDashMap<K, V> {
    pub fn new(cap: usize) -> Self {
        let num_shards = thread::available_parallelism().unwrap().get();
        let mut shards = Vec::default();
        for i in 0..num_shards {
            let mut this_cap = (cap - cap % num_shards) / num_shards;
            if i == num_shards - 1 {
                this_cap += cap % num_shards;
            }
            shards.push(Arc::new(Mutex::new(ClockMap::new(this_cap))));
        }
        ClockDashMap {
            shards,
            next_modify: Arc::new(Mutex::new(0)),
        }
    }

    #[inline(always)]
    fn get_shard<'a>(&'a self, key: &K) -> MutexGuard<'a,ClockMap<K, V>> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let shard_idx = (hasher.finish() as usize) % self.shards.len();
        self.shards[shard_idx].lock().unwrap()
    }

    pub fn insert(&mut self, key: K, val: V) {
        self.get_shard(&key).insert(key, val);
    }

    pub fn read(&mut self, key: &K) -> Option<V> {
        self.get_shard(&key).read(key)
    }
}

impl<'a, K: std::cmp::Eq + std::hash::Hash + Clone, V: Clone> ClockMap<K, V> {
    pub fn new(cap: usize) -> Self {
        ClockMap {
            value_map: HashMap::default(),
            clock_list: vec![None; cap],
            pointer: 0,
            miss: 0,
            capacity: cap,
        }
    }

    pub fn read(&mut self, key: &K) -> Option<V> {
        if self.value_map.contains_key(key) {
            let idx = self.value_map.get(&key).unwrap().index;
            if let Some(Entry {
                key: entry_key,
                second_chance,
            }) = &mut self.clock_list[idx]
            {
                if key == entry_key {
                    *second_chance = true;
                    return Some(self.value_map.get(&entry_key).unwrap().value.clone());
                }
            }
        }
        return None;
    }

    pub fn insert(&mut self, key: K, value: V) {
        if self.value_map.contains_key(&key) {
            let idx = self.value_map.get(&key).unwrap().index;
            if self.clock_list[idx].as_ref().unwrap().key == key {
                match &mut self.clock_list[idx] {
                    None => {} // need to throw exceptions?
                    Some(Entry { second_chance, .. }) => {
                        *second_chance = true;
                    }
                }
                let new_map_entry = MapEntry {
                    index: idx,
                    value,
                };
                self.value_map.insert(key, new_map_entry);
            }
            // println!(
            //     "hit, pointer: {}, map_len: {}",
            //     self.pointer,
            //     self.value_map.len()
            // );
            return;
        } else {
            for i in self.pointer..self.pointer + self.capacity {
                match &mut self.clock_list[i % self.capacity] {
                    None => {
                        let new_entry = Entry {
                            second_chance: false,
                            key: key.clone(),
                        };
                        self.clock_list[i] = Some(new_entry);
                        let new_map_entry = MapEntry {
                            index: i,
                            value,
                        };
                        self.value_map.insert(key, new_map_entry);
                    }
                    Some(Entry { second_chance, .. }) => {
                        if *second_chance {
                            *second_chance = false;
                            continue;
                        }
                        self.value_map
                            .remove(&self.clock_list[i % self.capacity].clone().unwrap().key);
                        let new_map_entry = MapEntry {
                            index: i,
                            value,
                        };
                        self.value_map.insert(key.clone(), new_map_entry);
                        self.clock_list[i % self.capacity] = Some(Entry {
                            second_chance: false,
                            key,
                        });
                    }
                }
                self.pointer = (i + 1) % self.capacity;
                self.miss += 1;
                // println!(
                //     "miss, pointer: {}, map_len: {}",
                //     self.pointer,
                //     self.value_map.len()
                // );
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ClockMap;
    #[test]
    fn test_1() {
        let mut clockmap1 = ClockMap::new(3);
        let input_keys1 = vec![0, 4, 1, 4, 2, 4, 3, 4, 2, 4, 0, 4, 1, 4, 2, 4, 3, 4];
        let input_vals1 = vec![0, 4, 1, 4, 2, 4, 3, 4, 2, 4, 0, 4, 1, 4, 2, 4, 3, 4];

        for i in 0..input_keys1.len() {
            clockmap1.insert(input_keys1[i], input_vals1[i]);
        }

        assert_eq!(clockmap1.miss, 9);
    }

    #[test]
    fn test_2() {
        let mut clockmap2 = ClockMap::new(4);
        let input_keys2 = vec![2, 5, 10, 1, 2, 2, 6, 9, 1, 2, 10, 2, 6, 1, 2, 1, 6, 9, 5, 1];
        let input_vals2 = vec![2, 5, 10, 1, 2, 2, 6, 9, 1, 2, 10, 2, 6, 1, 2, 1, 6, 9, 5, 1];

        for i in 0..input_keys2.len() {
            clockmap2.insert(input_keys2[i], input_vals2[i]);
        }

        assert_eq!(clockmap2.miss, 11)
    }
    #[test]
    fn test_multiple_thread() {

    }
}

fn main() {}
