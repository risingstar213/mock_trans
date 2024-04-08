use std::marker::PhantomData;
use std::ops::{Index, IndexMut};

use libc::{free, memalign};

pub struct RawArrayRegion<T: Default, const ITEM_COUNT: usize> {
    inner: usize,
    phan:  PhantomData<T>,
    ok:    bool,
}

impl<T: Default, const ITEM_COUNT: usize> Drop for RawArrayRegion<T, ITEM_COUNT> {
    fn drop(&mut self) {
        if self.ok {
            unsafe { free(self.inner as _); }
        }
    }
}

impl<T: Default, const ITEM_COUNT: usize> RawArrayRegion<T, ITEM_COUNT> {
    pub fn new_from_raw(ptr: *mut u8, len: usize) -> Option<Self> {
        let demand = std::mem::size_of::<T>() * ITEM_COUNT;
        if ptr.is_null() || demand != len {
            return None;
        }

        let slice_mut = unsafe {
            std::slice::from_raw_parts_mut::<T>(ptr as _, demand)
        };

        for i in 0..ITEM_COUNT {
            slice_mut[i] = T::default();
        }

        Some(Self {
            inner: ptr as usize,
            phan:  PhantomData::default(),
            ok:    false,
        })
    }

    pub fn new() -> Self {
        let demand = std::mem::size_of::<T>() * ITEM_COUNT;
        let ptr = unsafe { memalign(4096, demand) };

        let slice_mut = unsafe {
            std::slice::from_raw_parts_mut::<T>(ptr as _, demand)
        };

        for i in 0..ITEM_COUNT {
            slice_mut[i] = T::default();
        }

        Self {
            inner: ptr as usize,
            phan:  PhantomData::default(),
            ok:    true,
        }
    }

    pub unsafe fn get_inner(&mut self) -> *mut u8 {
        self.inner as *mut u8
    }

    #[inline]
    pub const fn get_length(&self) -> usize {
        std::mem::size_of::<T>() * ITEM_COUNT
    }

    pub fn swap(&mut self, lid: usize, rid: usize) {
        assert!(lid < ITEM_COUNT);
        assert!(rid < ITEM_COUNT);
        let slice_mut = unsafe {
            std::slice::from_raw_parts_mut::<T>(self.inner as _, self.get_length())
        };

        slice_mut.swap(lid, rid);
    }
}


impl<T: Default, const ITEM_COUNT: usize> Index<usize> for RawArrayRegion<T, ITEM_COUNT> {
    type Output = T;
    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        assert!(index < ITEM_COUNT);
        unsafe {
            let slice = std::slice::from_raw_parts(self.inner as _, self.get_length());
            &slice[index]
        }
    }
}

impl<T: Default, const ITEM_COUNT: usize> IndexMut<usize> for RawArrayRegion<T, ITEM_COUNT> {
    #[inline]
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        assert!(index < ITEM_COUNT);
        unsafe {
            let slice = std::slice::from_raw_parts_mut(self.inner as _, self.get_length());
            &mut slice[index]
        }
    }
}