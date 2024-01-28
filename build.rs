pub fn compile_protos() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("src/consensus/proto/raft.proto")?;
    Ok(())
}

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    compile_protos()?;
    Ok(())
}