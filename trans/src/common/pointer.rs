use std::ptr::NonNull;

#[derive(Clone, Copy)]
pub struct TransRawPointer {
    pub inner: NonNull<usize>,
    pub payload: usize
}

impl TransRawPointer {
    
}