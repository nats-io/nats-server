extern crate alloc;
extern crate core;
extern crate wee_alloc;

use alloc::vec::Vec;
use serde::{Deserialize, Serialize};
use std::mem::MaybeUninit;
use std::slice;

pub fn callout(connect_info: ConnectInfo, cert: Key) -> HTTPRequest {

}

pub fn process_callout_response(resp: HTTPResponse) -> JWT {

}

pub fn process(message: Message) -> std::io::Result<Option<Message>> {
    if message.payload == Some(b"filter".to_vec()) {
        return Ok(None);
    }
    if message.payload == Some(b"error".to_vec()) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "error in process",
        ));
    }
    let m = Message {
        payload: Some(b"blabla".to_vec()),
        reply: "reply".to_string(),
        ..message
    };
    Ok(Some(m))
}

fn transform(message: Vec<u8>) -> Vec<u8> {
    let mut m: Message = serde_json::from_slice(message.as_ref()).unwrap();
    if let Some(p) = m.payload.as_mut() {
        p.truncate(p.len().saturating_sub(2))
    }
    serde_json::to_vec(&process(m).map_or_else(
        |err| Response::Error {
            error: err.to_string(),
        },
        |mut message| {
            message.as_mut().map(|p| {
                p.payload.as_mut().map(|p| {
                    p.append(&mut b"\r\n".to_vec());
                    p
                })
            });
            Response::Message { message }
        },
    ))
    .unwrap()
}

#[cfg_attr(all(target_arch = "wasm32"), export_name = "transform")]
#[no_mangle]
pub unsafe extern "C" fn _transform(ptr: u32, len: u32) -> u64 {
    let name = ptr_to_bytes(ptr, len);
    // let re = r#"{"Subject":"blada"}"#.to_string();
    let transformed = transform(name);
    let (ptr, len) = string_to_ptr(&transformed);
    // Note: This changes ownership of the pointer to the external caller. If
    // we didn't call forget, the caller would read back a corrupt value. Since
    // we call forget, the caller must deallocate externally to prevent leaks.
    std::mem::forget(transformed);
    return ((ptr as u64) << 32) | len as u64;
}

/// Returns a string from WebAssembly compatible numeric types representing
/// its pointer and length.
unsafe fn ptr_to_bytes(ptr: u32, len: u32) -> Vec<u8> {
    slice::from_raw_parts_mut(ptr as *mut u8, len as usize).to_vec()
}

/// Returns a pointer and size pair for the given string in a way compatible
/// with WebAssembly numeric types.
///
/// Note: This doesn't change the ownership of the String. To intentionally
/// leak it, use [`std::mem::forget`] on the input after calling this.
unsafe fn string_to_ptr(s: &Vec<u8>) -> (u32, u32) {
    return (s.as_ptr() as u32, s.len() as u32);
}

/// Set the global allocator to the WebAssembly optimized one.
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

/// WebAssembly export that allocates a pointer (linear memory offset) that can
/// be used for a string.
///
/// This is an ownership transfer, which means the caller must call
/// [`deallocate`] when finished.
#[cfg_attr(all(target_arch = "wasm32"), export_name = "allocate")]
#[no_mangle]
pub extern "C" fn _allocate(size: u32) -> *mut u8 {
    allocate(size as usize)
}

/// Allocates size bytes and leaks the pointer where they start.
fn allocate(size: usize) -> *mut u8 {
    // Allocate the amount of bytes needed.
    let vec: Vec<MaybeUninit<u8>> = Vec::with_capacity(size);

    // into_raw leaks the memory to the caller.
    Box::into_raw(vec.into_boxed_slice()) as *mut u8
}

/// WebAssembly export that deallocates a pointer of the given size (linear
/// memory offset, byteCount) allocated by [`allocate`].
#[cfg_attr(all(target_arch = "wasm32"), export_name = "deallocate")]
#[no_mangle]
pub unsafe extern "C" fn _deallocate(ptr: u32, size: u32) {
    deallocate(ptr as *mut u8, size as usize);
}

/// Retakes the pointer which allows its memory to be freed.
unsafe fn deallocate(ptr: *mut u8, size: usize) {
    let _ = Vec::from_raw_parts(ptr, 0, size);
}

#[derive(Serialize, Deserialize)]
pub struct Message {
    #[serde(rename = "Message", with = "base64")]
    pub payload: Option<Vec<u8>>,
    #[serde(rename = "Subject")]
    pub subject: String,
    #[serde(rename = "Reply")]
    pub reply: String,
}

#[derive(Serialize)]
#[serde(untagged)]
pub enum Response {
    #[serde(rename = "Message")]
    Message { message: Option<Message> },
    #[serde(rename = "Error")]
    Error { error: String },
}

mod base64 {
    use serde::{Deserialize, Serialize};
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &Option<Vec<u8>>, s: S) -> Result<S::Ok, S::Error> {
        let base64 = match v {
            Some(v) => Some(base64::encode(v)),
            None => None,
        };
        <Option<String>>::serialize(&base64, s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Vec<u8>>, D::Error> {
        let base64 = <Option<String>>::deserialize(d)?;
        match base64 {
            Some(v) => base64::decode(v.as_bytes())
                .map(|v| Some(v))
                .map_err(|e| serde::de::Error::custom(e)),
            None => Ok(None),
        }
    }
}
