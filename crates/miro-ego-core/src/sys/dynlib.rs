use libloading::Library;

/// Load a typed function pointer from a retained `Library`.
///
/// # Safety
/// The caller must ensure `T` matches the exact C ABI signature of `name`
/// and that the returned pointer does not outlive `library`.
pub unsafe fn load_symbol<T: Copy>(library: &Library, name: &[u8]) -> Result<T, libloading::Error> {
    // SAFETY: Caller guarantees signature match and Library lifetime.
    let symbol = unsafe { library.get::<T>(name) }?;
    Ok(*symbol)
}
