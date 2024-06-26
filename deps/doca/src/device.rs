//! Wrap DOCA Device into rust struct.
//! The DOCA device represents an available processing unit backed by
//! hardware or software implementation.
//!
//! With the help of the wrapper, creating, managing and querying
//! the device is extremely simple.
//! Note that we also use `Arc` to automatically manage the lifecycle of the
//! device-related data structures.
//!
//! Example usage of opening a device context with a given device name:
//!
//! ```
//! use doca::open_device_with_pci;
//! let device_ctx = open_device_with_pci("03:00.0");
//!
//!
//! ```
//!
//! or
//!
//! ```
//! use doca::devices;
//! let device_ctx = devices().unwrap().get(0).unwrap().open().unwrap();
//! ```
//!

use ffi::doca_error;
use std::{ptr::NonNull, sync::Arc};

use crate::{DOCAError, DOCAResult};

/// DOCA Device list
pub struct DeviceList(&'static mut [*mut ffi::doca_devinfo]);

unsafe impl Sync for DeviceList {}
unsafe impl Send for DeviceList {}

impl Drop for DeviceList {
    fn drop(&mut self) {
        unsafe { ffi::doca_devinfo_list_destroy(self.0.as_mut_ptr()) };

        // Show drop order only in `debug` mode
        #[cfg(debug_assertions)]
        println!("DeviceList is dropped!");
    }
}

/// Get list of all available local devices.
///
/// # Errors
///
///  - `DOCA_ERROR_INVALID_VALUE`: received invalid input.
///  - `DOCA_ERROR_NO_MEMORY`: failed to allocate enough space.
///  - `DOCA_ERROR_NOT_FOUND`: failed to get RDMA devices list
///
pub fn devices() -> DOCAResult<Arc<DeviceList>> {
    let mut n = 0u32;
    let mut dev_list: *mut *mut ffi::doca_devinfo = std::ptr::null_mut();
    let ret = unsafe { ffi::doca_devinfo_list_create(&mut dev_list as *mut _, &mut n as *mut _) };

    if dev_list.is_null() || ret != doca_error::DOCA_SUCCESS {
        return Err(ret);
    }

    let devices = unsafe { std::slice::from_raw_parts_mut(dev_list, n as usize) };

    Ok(Arc::new(DeviceList(devices)))
}

impl DeviceList {
    /// Returns the number of devices.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if there are any devices.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the number of devices.
    pub fn num_devices(&self) -> usize {
        self.len()
    }

    /// Returns the device at the given `index`, or `None` if out of bounds.
    pub fn get(self: &Arc<Self>, index: usize) -> Option<Arc<Device>> {
        self.0
            .get(index)
            .map(|d| {
                let inner_ptr = NonNull::new(*d);

                let inner = match inner_ptr {
                    Some(inner) => inner,
                    None => return None,
                };

                Some(Arc::new(Device {
                    inner: inner,
                    parent_devlist: self.clone(),
                }))
            })
            .flatten()
    }
}

/// An DOCA device
pub struct Device {
    inner: NonNull<ffi::doca_devinfo>,

    // a device hold to ensure the device list is not freed
    // before the Device is freed
    #[allow(dead_code)]
    parent_devlist: Arc<DeviceList>,
}

unsafe impl Sync for Device {}
unsafe impl Send for Device {}

impl Device {
    /// Return the PCIe address of the doca device, e.g "17:00.1".
    /// The matching between the str & `doca_pci_bdf` can be seen
    /// as below.
    /// ---------------------------------------
    /// -- 4 -- b -- : -- 0 -- 0 -- . -- 1 ----
    /// --   BUS     |    DEVICE    | FUNCTION
    ///
    /// # Errors
    ///
    ///  - `DOCA_ERROR_INVALID_VALUE`: received invalid input.
    ///
    /// 
    pub fn name(&self) -> DOCAResult<String> {
        let mut pci_str = vec![0_u8; 16];
        let ret =
            unsafe { ffi::doca_devinfo_get_pci_addr_str(self.inner_ptr(), pci_str.as_mut_ptr().cast()) };
        
        if ret != doca_error::DOCA_SUCCESS {
            println!("get name str error!!!");
            return Err(ret);
        }

        Ok(String::from(std::str::from_utf8(&pci_str[5..12]).unwrap()))
    }

    /// Open a DOCA device and store it as a context for further use.
    pub fn open(self: &Arc<Self>) -> DOCAResult<Arc<DevContext>> {
        DevContext::with_device(self.clone())
    }

    /// Get the maximum supported buffer size for DMA job.
    pub fn get_max_buf_size(&self) -> DOCAResult<u64> {
        let mut num: u64 = 0;
        let ret = unsafe { ffi::doca_dma_get_max_buf_size(self.inner_ptr(), &mut num as *mut _) };

        if ret != doca_error::DOCA_SUCCESS {
            return Err(ret);
        }

        Ok(num)
    }

    /// Return the device
    pub unsafe fn inner_ptr(&self) -> *mut ffi::doca_devinfo {
        self.inner.as_ptr()
    }
}

/// An opened Doca Device
pub struct DevContext {
    ctx: NonNull<ffi::doca_dev>,
}

impl Drop for DevContext {
    fn drop(&mut self) {
        unsafe { ffi::doca_dev_close(self.ctx.as_ptr()) };

        // Show drop order only in `debug` mode
        #[cfg(debug_assertions)]
        println!("Device Context is dropped!");
    }
}

impl DevContext {
    /// Opens a context for the given device, so we can use it later.
    pub fn with_device(dev: Arc<Device>) -> DOCAResult<Arc<DevContext>> {
        let mut ctx: *mut ffi::doca_dev = std::ptr::null_mut();
        let ret = unsafe { ffi::doca_dev_open(dev.inner_ptr(), &mut ctx as *mut _) };

        if ret != doca_error::DOCA_SUCCESS {
            return Err(ret);
        }

        Ok(Arc::new(DevContext {
            ctx: NonNull::new(ctx).ok_or(doca_error::DOCA_ERROR_INVALID_VALUE)?,
        }))
    }

    /// Return the DOCA Device context raw pointer
    #[inline]
    pub unsafe fn inner_ptr(&self) -> *mut ffi::doca_dev {
        self.ctx.as_ptr()
    }
}

/// Open a DOCA Device with the given PCI address
///
/// Examples
/// ```
/// use doca::open_device_with_pci;
/// let device = open_device_with_pci("03:00.0");
/// ```
///
pub fn open_device_with_pci(pci: &str) -> DOCAResult<Arc<DevContext>> {
    let dev_list = devices()?;

    // println!("cmp pci {:?}", pci.as_bytes().to_vec());
    for i in 0..dev_list.num_devices() {
        let device = dev_list.get(i).unwrap();
        let pci_addr = device.name()?;
        // println!("get name {:?}, len = {}", pci_addr.as_bytes(), pci.len());
        if pci_addr.eq(pci) {
            // open the device
            return device.open();
        }
    }

    Err(doca_error::DOCA_ERROR_INVALID_VALUE)
}

/// DOCA Device list
pub struct DeviceRepList(&'static mut [*mut ffi::doca_devinfo_rep]);

unsafe impl Sync for DeviceRepList {}
unsafe impl Send for DeviceRepList {}

impl Drop for DeviceRepList {
    fn drop(&mut self) {
        unsafe { ffi::doca_devinfo_rep_list_destroy(self.0.as_mut_ptr()) };

        // Show drop order only in `debug` mode
        #[cfg(debug_assertions)]
        println!("DeviceList is dropped!");
    }
}

impl DeviceRepList {
    /// Returns the number of devices.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if there are any devices.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the number of devices.
    pub fn num_devices(&self) -> usize {
        self.len()
    }

    /// Returns the device at the given `index`, or `None` if out of bounds.
    pub fn get(self: &Arc<Self>, index: usize) -> Option<Arc<DeviceRep>> {
        self.0
            .get(index)
            .map(|d| {
                let inner_ptr = NonNull::new(*d);

                let inner = match inner_ptr {
                    Some(inner) => inner,
                    None => return None,
                };

                Some(Arc::new(DeviceRep {
                    inner: inner,
                    parent_devlist: self.clone(),
                }))
            })
            .flatten()
    }
}

/// An DOCA device
pub struct DeviceRep {
    inner: NonNull<ffi::doca_devinfo_rep>,

    // a device hold to ensure the device list is not freed
    // before the Device is freed
    #[allow(dead_code)]
    parent_devlist: Arc<DeviceRepList>,
}

unsafe impl Sync for DeviceRep {}
unsafe impl Send for DeviceRep {}

impl DeviceRep {
    /// Return the PCIe address of the doca device, e.g "17:00.1".
    /// The matching between the str & `doca_pci_bdf` can be seen
    /// as below.
    /// ---------------------------------------
    /// -- 4 -- b -- : -- 0 -- 0 -- . -- 1 ----
    /// --   BUS     |    DEVICE    | FUNCTION
    ///
    /// # Errors
    ///
    ///  - `DOCA_ERROR_INVALID_VALUE`: received invalid input.
    ///
    /// 
    pub fn name(&self) -> DOCAResult<String> {
        let mut pci_str = vec![0_u8; 16];
        let ret =
            unsafe { ffi::doca_devinfo_rep_get_pci_addr_str(self.inner_ptr(), pci_str.as_mut_ptr().cast()) };
        
        if ret != doca_error::DOCA_SUCCESS {
            println!("get name str error!!!");
            return Err(ret);
        }

        Ok(String::from(std::str::from_utf8(&pci_str[5..12]).unwrap()))
    }

    /// Open a DOCA device and store it as a context for further use.
    pub fn open(self: &Arc<Self>) -> DOCAResult<Arc<DevRepContext>> {
        DevRepContext::with_device(self.clone())
    }

    /// Return the device
    pub unsafe fn inner_ptr(&self) -> *mut ffi::doca_devinfo_rep {
        self.inner.as_ptr()
    }
}

/// An opened Doca Device
pub struct DevRepContext {
    ctx: NonNull<ffi::doca_dev_rep>,
}

impl Drop for DevRepContext {
    fn drop(&mut self) {
        unsafe { ffi::doca_dev_rep_close(self.ctx.as_ptr()) };

        // Show drop order only in `debug` mode
        #[cfg(debug_assertions)]
        println!("Device Context is dropped!");
    }
}

impl DevRepContext {
    /// Opens a context for the given device, so we can use it later.
    pub fn with_device(dev: Arc<DeviceRep>) -> DOCAResult<Arc<DevRepContext>> {
        let mut ctx: *mut ffi::doca_dev_rep = std::ptr::null_mut();
        let ret = unsafe { ffi::doca_dev_rep_open(dev.inner_ptr(), &mut ctx as *mut _) };

        if ret != doca_error::DOCA_SUCCESS {
            return Err(ret);
        }

        Ok(Arc::new(DevRepContext {
            ctx: NonNull::new(ctx).ok_or(doca_error::DOCA_ERROR_INVALID_VALUE)?,
        }))
    }

    /// Return the DOCA Device context raw pointer
    #[inline]
    pub unsafe fn inner_ptr(&self) -> *mut ffi::doca_dev_rep {
        self.ctx.as_ptr()
    }
}

/// simplied realizarion for create rep with no wrapper...
/// I have no time temporarily
pub fn open_device_rep_with_pci(local_dev: &Arc<DevContext>, rep_pci_addr: &str) -> DOCAResult<Arc<DevRepContext>> {
    let mut num_devs: u32 = 0;
    let mut dev_list: *mut *mut ffi::doca_devinfo_rep = std::ptr::null_mut();

    let ret = unsafe { ffi::doca_devinfo_rep_list_create(local_dev.inner_ptr(), 2 /* DOCA_DEV_REP_FILTER_NET */, &mut dev_list, &mut num_devs) };
    if ret != DOCAError::DOCA_SUCCESS {
        panic!("Failed to create devinfo representations list. Representor devices are available only on DPU, do not run on Host");
    }

    let devices = unsafe { std::slice::from_raw_parts_mut(dev_list, num_devs as usize) };

    let dev_list = Arc::new(DeviceRepList(devices));

    for i in 0..dev_list.num_devices() {
        let device = dev_list.get(i).unwrap();
        let pci_addr = device.name().unwrap();

        if pci_addr.eq(rep_pci_addr) {
            // open the device
            return device.open();
        }
    }

    Err(doca_error::DOCA_ERROR_INVALID_VALUE)
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_get_device_and_check() {
        let ret = crate::device::devices();
        assert!(ret.is_ok());

        let devices = ret.unwrap();

        // specially, there're 4 local doca devices on `pro0`
        // which has been checked by the original C program
        println!("len: {}", devices.len());
        assert_ne!(devices.len(), 0);

        for i in 0..devices.num_devices() {
            let device = devices.get(i).unwrap();
            let pci_addr = device.name().unwrap();
            println!("device pci addr {}", pci_addr);
        }
    }

    #[test]
    fn test_get_and_open_a_device() {
        let device = crate::device::devices().unwrap().get(0).unwrap().open();
        assert!(device.is_ok());
    }

    #[test]
    fn test_dev_max_buf() {
        let device = crate::device::devices().unwrap().get(0).unwrap();
        let ret = device.get_max_buf_size();
        assert!(ret.is_ok());
        println!("max buf size: {}", ret.unwrap());
    }
}
