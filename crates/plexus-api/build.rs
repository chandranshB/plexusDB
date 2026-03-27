fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = "src/generated";
    std::fs::create_dir_all(out_dir)?;

    let proto_file = "../../proto/plexus.proto";
    if std::path::Path::new(proto_file).exists() {
        match tonic_build::configure()
            .build_server(true)
            .build_client(true)
            .out_dir(out_dir)
            .compile_protos(&[proto_file], &["../../proto"])
        {
            Ok(_) => println!("cargo:warning=protobuf codegen successful"),
            Err(e) => {
                println!("cargo:warning=protobuf codegen skipped: {e}");
                println!("cargo:warning=install protoc to regenerate gRPC stubs");
            }
        }
    }
    Ok(())
}
