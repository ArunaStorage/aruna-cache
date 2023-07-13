use std::time::Instant;

use aruna_cache::cache::{self, Cache};
use aruna_cache::structs::Resource::*;
use diesel_ulid::DieselUlid;

pub fn main () {

    let new_cache = Cache::new();


    let mut start = Instant::now();
    let mut project = Project(DieselUlid::generate());
    let mut object = Object(DieselUlid::generate());
    for x in 0..50 {
        project = Project(DieselUlid::generate());
        for y in 0..50 {
            let collection = Collection(DieselUlid::generate());
            new_cache.add_link(project.clone(), collection.clone()).unwrap();
            for z in 0..50 {
                let dataset = Dataset(DieselUlid::generate());
                new_cache.add_link(collection.clone(), dataset.clone()).unwrap();

                for a in 0..100 {
                    object = Object(DieselUlid::generate());
                    new_cache.add_link(dataset.clone(), object.clone()).unwrap();
                }
            }
        }
    }
    println!("Took {} ms to create 12.5 Mio nodes", start.elapsed().as_millis());
    start = Instant::now();
    new_cache.traverse_graph(project).unwrap();
    println!("Took {} ms to traverse subgraph", start.elapsed().as_millis());
    start = Instant::now();
    new_cache.get_parents(object).unwrap();
    println!("Took {} ms to get parents in subgraph", start.elapsed().as_millis());
}