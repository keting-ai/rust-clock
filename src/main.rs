use std::{collections::HashMap, process::id};

#[derive(Clone)]
struct Entry<K: Clone> {
    second_chance: bool,
    key: K,
}

struct MapEntry<V> {
    value: V,
    index: usize,
}

// change
pub struct ClockMap<K: Clone, V> {
    value_map: HashMap<K, MapEntry<V>>,
    clock_list: Vec<Option<Entry<K>>>,
    pointer: usize,
    miss: i32,
    capacity: usize,
}

impl<'a, K: std::cmp::Eq + std::hash::Hash + Clone, V: Clone> ClockMap<K, V> {
    fn new(cap: usize) -> Self {
        ClockMap {
            value_map: HashMap::default(),
            clock_list: vec![None; cap],
            pointer: 0,
            miss: 0,
            capacity: cap,
        }
    }
    fn read(&mut self, key: &K) -> Option<V> {
        if self.value_map.contains_key(&key) {
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
    fn insert(&mut self, key: K, value: V) {
        if self.value_map.contains_key(&key) {
            let idx = self.value_map.get(&key).unwrap().index;
            if self.clock_list[idx].as_ref().unwrap().key == key {
                match &mut self.clock_list[idx] {
                    None => {}
                    Some(Entry { second_chance, .. }) => {
                        *second_chance = true;
                    }
                }
                let new_map_entry = MapEntry {
                    index: idx,
                    value: value,
                };
                self.value_map.insert(key, new_map_entry);
            }
            println!(
                "hit, pointer: {}, map_len: {}",
                self.pointer,
                self.value_map.len()
            );
            return;
        } else {
            for i in self.pointer..self.pointer + self.capacity {
                match &mut self.clock_list[i % self.capacity] {
                    None => {
                        print!("none: ");
                        let new_entry = Entry {
                            second_chance: false,
                            key: key.clone(),
                        };
                        self.clock_list[i] = Some(new_entry);
                        let new_map_entry = MapEntry {
                            index: i,
                            value: value,
                        };
                        self.value_map.insert(key, new_map_entry);
                    }
                    Some(Entry { second_chance, .. }) => {
                        print!("not none: ");
                        if *second_chance {
                            *second_chance = false;
                            continue;
                        }
                        self.value_map
                            .remove(&self.clock_list[i % self.capacity].clone().unwrap().key);
                        let new_map_entry = MapEntry {
                            index: i,
                            value: value,
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
                println!(
                    "miss, pointer: {}, map_len: {}",
                    self.pointer,
                    self.value_map.len()
                );
                break;
            }
        }
    }
}

fn main() {
    let mut clockmap1 = ClockMap::new(3);
    let input_keys1 = vec![0, 4, 1, 4, 2, 4, 3, 4, 2, 4, 0, 4, 1, 4, 2, 4, 3, 4];
    let input_vals1 = vec![0, 4, 1, 4, 2, 4, 3, 4, 2, 4, 0, 4, 1, 4, 2, 4, 3, 4];

    for i in 0..input_keys1.len() {
        clockmap1.insert(input_keys1[i], input_vals1[i]);
    }
    println!(
        "misses: {}, length: {}",
        clockmap1.miss,
        clockmap1.clock_list.len()
    );
    for i in 0..clockmap1.clock_list.len() {
        print!("{:#?},", clockmap1.clock_list[i].as_ref().unwrap().key);
    }
    println!();

    let mut clockmap2 = ClockMap::new(4);
    let input_keys2 = vec![2, 5, 10, 1, 2, 2, 6, 9, 1, 2, 10, 2, 6, 1, 2, 1, 6, 9, 5, 1];
    let input_vals2 = vec![2, 5, 10, 1, 2, 2, 6, 9, 1, 2, 10, 2, 6, 1, 2, 1, 6, 9, 5, 1];
    for i in 0..input_keys2.len() {
        clockmap2.insert(input_keys2[i], input_vals2[i]);
    }
    println!(
        "misses: {}, length: {}",
        clockmap2.miss,
        clockmap2.clock_list.len()
    );
    for i in 0..clockmap2.clock_list.len() {
        print!("{:#?},", clockmap2.clock_list[i].as_ref().unwrap().key);
    }
    println!();
}
