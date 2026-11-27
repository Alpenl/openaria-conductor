use crate::hardware::jpeg;
use libloading::Library;
use miro_ego_core::sys::dynlib::load_symbol;
use std::ffi::{CStr, c_char, c_int, c_ulong, c_void};
use std::ptr::NonNull;
use std::sync::{Arc, OnceLock};

const TJXOP_NONE: c_int = 0;
const TJXOPT_CROP: c_int = 4;

#[repr(C)]
#[derive(Clone, Copy)]
struct Region {
    x: c_int,
    y: c_int,
    w: c_int,
    h: c_int,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct Transform {
    region: Region,
    operation: c_int,
    options: c_int,
    data: *mut c_void,
    custom_filter: *mut c_void,
}

type InitTransform = unsafe extern "C" fn() -> *mut c_void;
type TransformJpeg = unsafe extern "C" fn(
    *mut c_void,
    *const u8,
    c_ulong,
    c_int,
    *mut *mut u8,
    *mut c_ulong,
    *mut Transform,
    c_int,
) -> c_int;
type Destroy = unsafe extern "C" fn(*mut c_void) -> c_int;
type Free = unsafe extern "C" fn(*mut u8);
type ErrorString = unsafe extern "C" fn(*mut c_void) -> *const c_char;

trait TurboApi: Send + Sync {
    fn init_transform(&self) -> *mut c_void;
    unsafe fn transform(
        &self,
        handle: *mut c_void,
        payload: &[u8],
        outputs: &mut [*mut u8; 2],
        sizes: &mut [c_ulong; 2],
        transforms: &mut [Transform; 2],
    ) -> c_int;
    unsafe fn destroy(&self, handle: *mut c_void);
    unsafe fn free(&self, output: *mut u8);
    unsafe fn error(&self, handle: *mut c_void) -> String;
}

struct DynamicApi {
    _library: Library,
    init_transform: InitTransform,
    transform: TransformJpeg,
    destroy: Destroy,
    free: Free,
    error: ErrorString,
}

static TURBOJPEG_API: OnceLock<Arc<DynamicApi>> = OnceLock::new();

impl DynamicApi {
    fn cached() -> Result<Arc<Self>, TurboJpegError> {
        if let Some(api) = TURBOJPEG_API.get() {
            return Ok(Arc::clone(api));
        }
        let loaded = Arc::new(Self::load()?);
        Ok(Arc::clone(TURBOJPEG_API.get_or_init(|| loaded)))
    }

    fn load() -> Result<Self, TurboJpegError> {
        let mut last_error = None;
        for name in ["libturbojpeg.so.0", "libturbojpeg.so"] {
            // SAFETY: The library is retained for at least as long as all copied symbols.
            let library = match unsafe { Library::new(name) } {
                Ok(library) => library,
                Err(error) => {
                    last_error = Some(error.to_string());
                    continue;
                }
            };
            // SAFETY: Symbol names and C signatures match the TurboJPEG public ABI.
            unsafe {
                let init_transform = load_symbol(&library, b"tjInitTransform\0")
                    .map_err(|error| TurboJpegError::unavailable(error.to_string()))?;
                let transform = load_symbol(&library, b"tjTransform\0")
                    .map_err(|error| TurboJpegError::unavailable(error.to_string()))?;
                let destroy = load_symbol(&library, b"tjDestroy\0")
                    .map_err(|error| TurboJpegError::unavailable(error.to_string()))?;
                let free = load_symbol(&library, b"tjFree\0")
                    .map_err(|error| TurboJpegError::unavailable(error.to_string()))?;
                let error = load_symbol(&library, b"tjGetErrorStr2\0")
                    .map_err(|error| TurboJpegError::unavailable(error.to_string()))?;
                return Ok(Self {
                    _library: library,
                    init_transform,
                    transform,
                    destroy,
                    free,
                    error,
                });
            }
        }
        Err(TurboJpegError::unavailable(last_error.unwrap_or_else(
            || "TurboJPEG library not found".to_owned(),
        )))
    }
}

impl TurboApi for DynamicApi {
    fn init_transform(&self) -> *mut c_void {
        // SAFETY: Function pointer was loaded from the retained TurboJPEG library.
        unsafe { (self.init_transform)() }
    }

    unsafe fn transform(
        &self,
        handle: *mut c_void,
        payload: &[u8],
        outputs: &mut [*mut u8; 2],
        sizes: &mut [c_ulong; 2],
        transforms: &mut [Transform; 2],
    ) -> c_int {
        // SAFETY: Callers provide a live handle and arrays matching n=2.
        unsafe {
            (self.transform)(
                handle,
                payload.as_ptr(),
                payload.len() as c_ulong,
                2,
                outputs.as_mut_ptr(),
                sizes.as_mut_ptr(),
                transforms.as_mut_ptr(),
                0,
            )
        }
    }

    unsafe fn destroy(&self, handle: *mut c_void) {
        // SAFETY: The handle was created by this API and is destroyed exactly once.
        unsafe { (self.destroy)(handle) };
    }

    unsafe fn free(&self, output: *mut u8) {
        // SAFETY: The pointer was allocated by TurboJPEG and is freed exactly once.
        unsafe { (self.free)(output) };
    }

    unsafe fn error(&self, handle: *mut c_void) -> String {
        // SAFETY: TurboJPEG owns the NUL-terminated error string.
        let pointer = unsafe { (self.error)(handle) };
        if pointer.is_null() {
            return "TurboJPEG transform failed".to_owned();
        }
        // SAFETY: TurboJPEG promises a valid NUL-terminated error string.
        unsafe { CStr::from_ptr(pointer) }
            .to_string_lossy()
            .into_owned()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurboJpegError {
    pub code: &'static str,
    pub message: String,
}

impl TurboJpegError {
    fn unavailable(message: String) -> Self {
        Self {
            code: "turbojpeg_unavailable",
            message,
        }
    }

    fn operation(message: String) -> Self {
        Self {
            code: "turbojpeg_transform_failed",
            message,
        }
    }
}

struct OutputBuffer {
    api: Arc<dyn TurboApi>,
    pointer: NonNull<u8>,
    length: usize,
}

impl OutputBuffer {
    fn to_vec(&self) -> Vec<u8> {
        // SAFETY: TurboJPEG owns a readable allocation of exactly `length` bytes.
        unsafe { std::slice::from_raw_parts(self.pointer.as_ptr(), self.length) }.to_vec()
    }
}

impl Drop for OutputBuffer {
    fn drop(&mut self) {
        // SAFETY: OutputBuffer uniquely owns this TurboJPEG allocation.
        unsafe { self.api.free(self.pointer.as_ptr()) };
    }
}

pub struct TransformHandle {
    api: Arc<dyn TurboApi>,
    handle: Option<NonNull<c_void>>,
}

// TurboJPEG handles may move between threads but are only used through the Python-side mutex.
unsafe impl Send for TransformHandle {}

impl TransformHandle {
    pub fn open() -> Result<Self, TurboJpegError> {
        Self::with_api(DynamicApi::cached()?)
    }

    fn with_api(api: Arc<dyn TurboApi>) -> Result<Self, TurboJpegError> {
        let handle = NonNull::new(api.init_transform()).ok_or_else(|| {
            // SAFETY: A null handle is the documented way to query init errors.
            TurboJpegError::unavailable(unsafe { api.error(std::ptr::null_mut()) })
        })?;
        Ok(Self {
            api,
            handle: Some(handle),
        })
    }

    pub fn close(&mut self) {
        if let Some(handle) = self.handle.take() {
            // SAFETY: take() makes repeated close a no-op and guarantees one destroy.
            unsafe { self.api.destroy(handle.as_ptr()) };
        }
    }

    pub fn split(
        &mut self,
        payload: &[u8],
        width: i32,
        height: i32,
    ) -> Result<(Vec<u8>, Vec<u8>), TurboJpegError> {
        if payload.is_empty() || width <= 0 || width % 2 != 0 || height <= 0 {
            return Err(TurboJpegError {
                code: "bad_frame",
                message: "payload and dimensions must describe a non-empty even-width SBS JPEG"
                    .to_owned(),
            });
        }
        let handle = self.handle.ok_or_else(|| TurboJpegError {
            code: "native_splitter_closed",
            message: "native splitter is closed".to_owned(),
        })?;
        let eye_width = width / 2;
        let mut outputs = [std::ptr::null_mut(); 2];
        let mut sizes = [0; 2];
        let mut transforms = [
            Transform {
                region: Region {
                    x: 0,
                    y: 0,
                    w: eye_width,
                    h: height,
                },
                operation: TJXOP_NONE,
                options: TJXOPT_CROP,
                data: std::ptr::null_mut(),
                custom_filter: std::ptr::null_mut(),
            },
            Transform {
                region: Region {
                    x: eye_width,
                    y: 0,
                    w: eye_width,
                    h: height,
                },
                operation: TJXOP_NONE,
                options: TJXOPT_CROP,
                data: std::ptr::null_mut(),
                custom_filter: std::ptr::null_mut(),
            },
        ];
        // SAFETY: The handle and input remain live, and arrays have exactly two entries.
        let result = unsafe {
            self.api.transform(
                handle.as_ptr(),
                payload,
                &mut outputs,
                &mut sizes,
                &mut transforms,
            )
        };
        let buffers = outputs.map(NonNull::new);
        let owned = buffers.map(|pointer| {
            pointer.map(|pointer| OutputBuffer {
                api: Arc::clone(&self.api),
                pointer,
                length: 0,
            })
        });
        if result != 0 {
            // SAFETY: The live handle can provide the current transform error.
            return Err(TurboJpegError::operation(unsafe {
                self.api.error(handle.as_ptr())
            }));
        }
        let [left, right] = owned;
        let mut left =
            left.ok_or_else(|| TurboJpegError::operation("left output missing".into()))?;
        let mut right =
            right.ok_or_else(|| TurboJpegError::operation("right output missing".into()))?;
        left.length = sizes[0] as usize;
        right.length = sizes[1] as usize;
        Ok((left.to_vec(), right.to_vec()))
    }

    pub fn split_sbs(
        &mut self,
        payload: &[u8],
        width: i32,
        height: i32,
    ) -> Result<(Vec<u8>, Vec<u8>), TurboJpegError> {
        if width <= 0 || width % 2 != 0 || height <= 0 {
            return Err(TurboJpegError {
                code: "bad_frame",
                message: "SBS dimensions must be positive with an even width".to_owned(),
            });
        }
        let width_u16 = u16::try_from(width).map_err(|_| TurboJpegError {
            code: "bad_frame",
            message: "SBS width exceeds JPEG 16-bit dimension limit".to_owned(),
        })?;
        let height_u16 = u16::try_from(height).map_err(|_| TurboJpegError {
            code: "bad_frame",
            message: "SBS height exceeds JPEG 16-bit dimension limit".to_owned(),
        })?;
        let expected_eye = (width_u16 / 2, height_u16);

        let ranges = jpeg::ranges(payload);
        if ranges.len() == 2 {
            let (left_start, left_end) = ranges[0];
            let (right_start, right_end) = ranges[1];
            let left_slice = &payload[left_start..left_end];
            let right_slice = &payload[right_start..right_end];
            for (eye_name, slice) in [("left", left_slice), ("right", right_slice)] {
                if let Some(actual) = jpeg::dimensions(slice) {
                    if actual != expected_eye {
                        return Err(TurboJpegError {
                            code: "bad_frame",
                            message: format!(
                                "{eye_name} JPEG dimensions are {}x{}, expected {}x{}",
                                actual.0, actual.1, expected_eye.0, expected_eye.1
                            ),
                        });
                    }
                }
            }
            return Ok((left_slice.to_vec(), right_slice.to_vec()));
        }
        if ranges.len() != 1 {
            return Err(TurboJpegError {
                code: "bad_frame",
                message: "payload must contain one or two complete JPEG images".to_owned(),
            });
        }
        let (start, end) = ranges[0];
        let selected = &payload[start..end];
        if let Some(actual) = jpeg::dimensions(selected) {
            if actual != (width_u16, height_u16) {
                return Err(TurboJpegError {
                    code: "bad_frame",
                    message: format!(
                        "JPEG dimensions are {}x{}, expected {width}x{height}",
                        actual.0, actual.1
                    ),
                });
            }
        }
        let (left, right) = self.split(selected, width, height)?;
        if jpeg::dimensions(&left) != Some(expected_eye)
            || jpeg::dimensions(&right) != Some(expected_eye)
        {
            return Err(TurboJpegError::operation(
                "output eye dimensions are invalid".to_owned(),
            ));
        }
        Ok((left, right))
    }
}

impl Drop for TransformHandle {
    fn drop(&mut self) {
        self.close();
    }
}

pub fn available() -> bool {
    TransformHandle::open().is_ok()
}

#[cfg(test)]
mod tests {
    use super::{Transform, TransformHandle, TurboApi};
    use std::collections::HashMap;
    use std::ffi::{c_int, c_ulong, c_void};
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct State {
        destroyed: usize,
        freed: usize,
        fail: bool,
        allocations: HashMap<usize, Box<[u8]>>,
    }

    #[derive(Default)]
    struct FakeApi(Mutex<State>);

    impl TurboApi for FakeApi {
        fn init_transform(&self) -> *mut c_void {
            std::ptr::dangling_mut::<c_void>()
        }

        unsafe fn transform(
            &self,
            _handle: *mut c_void,
            _payload: &[u8],
            outputs: &mut [*mut u8; 2],
            sizes: &mut [c_ulong; 2],
            transforms: &mut [Transform; 2],
        ) -> c_int {
            assert_eq!(transforms[0].region.x, 0);
            assert_eq!(transforms[1].region.x, 1920);
            let mut state = self.0.lock().unwrap();
            for (index, value) in [b"left".as_slice(), b"right".as_slice()]
                .into_iter()
                .enumerate()
            {
                if state.fail && index == 1 {
                    break;
                }
                let mut allocation = value.to_vec().into_boxed_slice();
                let pointer = allocation.as_mut_ptr();
                state.allocations.insert(pointer as usize, allocation);
                outputs[index] = pointer;
                sizes[index] = value.len() as c_ulong;
            }
            if state.fail { -1 } else { 0 }
        }

        unsafe fn destroy(&self, _handle: *mut c_void) {
            self.0.lock().unwrap().destroyed += 1;
        }

        unsafe fn free(&self, output: *mut u8) {
            let mut state = self.0.lock().unwrap();
            state.allocations.remove(&(output as usize)).unwrap();
            state.freed += 1;
        }

        unsafe fn error(&self, _handle: *mut c_void) -> String {
            "fake transform failure".to_owned()
        }
    }

    #[test]
    fn successful_transform_frees_outputs_and_repeated_close_destroys_once() {
        let api = Arc::new(FakeApi::default());
        let mut handle = TransformHandle::with_api(api.clone()).unwrap();
        assert_eq!(
            handle.split(b"jpeg", 3840, 1080).unwrap(),
            (b"left".to_vec(), b"right".to_vec())
        );
        handle.close();
        handle.close();
        let state = api.0.lock().unwrap();
        assert_eq!(
            (state.freed, state.destroyed, state.allocations.len()),
            (2, 1, 0)
        );
    }

    #[test]
    fn failed_transform_frees_partial_output_and_drop_destroys_handle() {
        let api = Arc::new(FakeApi::default());
        api.0.lock().unwrap().fail = true;
        let mut handle = TransformHandle::with_api(api.clone()).unwrap();
        let error = handle.split(b"bad", 3840, 1080).unwrap_err();
        assert_eq!(error.code, "turbojpeg_transform_failed");
        drop(handle);
        let state = api.0.lock().unwrap();
        assert_eq!(
            (state.freed, state.destroyed, state.allocations.len()),
            (1, 1, 0)
        );
    }

    #[test]
    fn two_complete_jpegs_preserve_direction_without_calling_transform() {
        let api = Arc::new(FakeApi::default());
        let mut handle = TransformHandle::with_api(api.clone()).unwrap();
        let payload = b"pad\xff\xd8left\xff\xd9\xff\xd8right\xff\xd9\0";
        assert_eq!(
            handle.split_sbs(payload, 3840, 1080).unwrap(),
            (
                b"\xff\xd8left\xff\xd9".to_vec(),
                b"\xff\xd8right\xff\xd9".to_vec()
            )
        );
        assert_eq!(api.0.lock().unwrap().freed, 0);

        // Dual JPEGs with mismatched SOF0 eye dimensions are rejected on the fast path
        let wrong_eye =
            b"\xff\xd8\xff\xc0\0\x11\x08\x01\x00\x02\x00\x03\x01\x11\0\x02\x11\0\x03\x11\0\xff\xd9";
        let mut bad_dual = Vec::new();
        bad_dual.extend_from_slice(wrong_eye);
        bad_dual.extend_from_slice(wrong_eye);
        assert_eq!(
            handle.split_sbs(&bad_dual, 3840, 1080).unwrap_err().code,
            "bad_frame"
        );
    }

    #[test]
    fn invalid_input_and_closed_handle_return_stable_errors() {
        let api = Arc::new(FakeApi::default());
        let mut handle = TransformHandle::with_api(api).unwrap();
        assert_eq!(
            handle.split_sbs(b"bad", 3840, 1080).unwrap_err().code,
            "bad_frame"
        );
        assert_eq!(
            handle
                .split_sbs(b"\xff\xd8x\xff\xd9", 70_000, 1080)
                .unwrap_err()
                .code,
            "bad_frame"
        );
        handle.close();
        assert_eq!(
            handle
                .split(b"\xff\xd8x\xff\xd9", 3840, 1080)
                .unwrap_err()
                .code,
            "native_splitter_closed"
        );
    }
}
