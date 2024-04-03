#![feature(get_mut_unchecked)]

use std::sync::Arc;

use trans::memstore::memdb::{MemDB, TableSchema};
use trans::memstore::{MemStoreValue, RobinhoodMemStore};
use trans::occ::occ_local::OccLocal;
use trans::occ::Occ;
use trans::occ::RwType;

#[repr(C)]
#[derive(Clone)]
struct Account {
    balance: u64,
}

impl Default for Account {
    fn default() -> Self {
        Self {
            balance: 10000,
        }
    }
}

impl MemStoreValue for Account {}

fn prepare_data(memdb: &Arc<MemDB>) 
{
    let mut occ1 = OccLocal::<8>::new(1, memdb);
    occ1.start();

    let idx = occ1.write::<Account>(0, 0, 10037, RwType::INSERT);
    occ1.set_value(false, idx, &Account{
        balance: 34567
    });

    let idx = occ1.write::<Account>(0, 0, 13356, RwType::INSERT);
    occ1.set_value(false, idx, &Account{
        balance: 67890
    });

    occ1.commit();

    assert_eq!(occ1.is_commited(), true);

    let mut occ2 = OccLocal::<8>::new(2,memdb);
    occ2.start();

    let idx = occ2.read::<Account>(0, 0, 10037);
    assert_eq!(occ2.get_value::<Account>(false, idx).balance, 34567);

    let idx = occ2.read::<Account>(0, 0, 13356);
    assert_eq!(occ2.get_value::<Account>(false, idx).balance, 67890);

    occ2.commit();

    assert_eq!(occ2.is_commited(), true);

}

fn test_conflicts(memdb: &Arc<MemDB>) {
    let mut occ1 = OccLocal::<8>::new(1,memdb);
    occ1.start();

    let mut occ2 = OccLocal::<8>::new(2,memdb);
    occ2.start();

    let idx2 = occ2.read::<Account>(0, 0, 10037);
    let balance2 = occ2.get_value::<Account>(false, idx2).balance;
    let idx2 = occ2.write::<Account>(0, 0, 13356, RwType::UPDATE);
    occ2.set_value::<Account>(false, idx2, &Account{
        balance: balance2,
    });

    let idx1 = occ1.fetch_write::<Account>(0, 0, 13356);
    let balance1 = occ1.get_value::<Account>(true, idx1).balance + 1;

    occ1.set_value(true, idx1, &Account{
        balance: balance1,
    });

    occ2.commit();
    assert_eq!(occ2.is_aborted(), true);

    occ1.commit();
    assert_eq!(occ1.is_commited(), true);

    let mut occ3 = OccLocal::<8>::new(3,memdb);
    let idx3 = occ3.read::<Account>(0, 0, 13356);
    let balance3 = occ3.get_value::<Account>(false, idx3).balance;

    assert_eq!(balance3, balance1);

    occ3.commit();
    assert_eq!(occ3.is_commited(), true);

}

#[test]
fn occlocal_test()
{
    let mut memdb = Arc::new(MemDB::new());
    let memstore = RobinhoodMemStore::<Account>::new();
    
    unsafe {
        Arc::get_mut_unchecked(&mut memdb).add_schema(0, TableSchema::default(), memstore);
    }

    prepare_data(&memdb);

    test_conflicts(&memdb);
}