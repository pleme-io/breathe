//! Compile the gRPC proto. Prefer a system `protoc` (nix nativeBuildInput =
//! protobuf); fall back to the vendored binary for local dev builds.
fn main() {
    if std::env::var_os("PROTOC").is_none() && which::which("protoc").is_err() {
        if let Ok(p) = protoc_bin_vendored::protoc_bin_path() {
            // SAFETY: build scripts are single-threaded; setting PROTOC for tonic-build.
            unsafe {
                std::env::set_var("PROTOC", p);
            }
        }
    }
    tonic_build::compile_protos("proto/breathe.proto").expect("compile proto/breathe.proto");
    println!("cargo:rerun-if-changed=proto/breathe.proto");
}
