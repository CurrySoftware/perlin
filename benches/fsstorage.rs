extern crate rand;

use std::fs::create_dir_all;

fn prepare_storage(item_count: usize, item_size: usize) -> FsStorage<Vec<u64>> {
    let dir = format!("/tmp/perlin_bench/store{}-{}", item_count, item_size);
    create_dir_all(Path::new(dir));
    let mut store = FsStorage::new(Path::new(dir));
    for i in 0..item_count {
        store.store(i, (0..item_size).map(|_| rand::random::<u64>()).collect::<Vec<_>>());
    }
}

lazy_static!{
    pub static ref STORES: [FsStorage<Vec<usize>>; 6] = [
        prepare_storage(1000, 10),
        prepare_storage(10000, 10),
        prepare_storage(100000, 10),
        prepare_storage(1000, 100),
        prepare_storage(1000, 1000),
        prepare_storage(1000, 10000)
    ];
}



#[bench]
fn get_small_items(b: &mut Bencher)
{
    b.iter(||
           STORES[0].get(0).unwrap().len()
           );
}
