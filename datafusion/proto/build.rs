// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

type Error = Box<dyn std::error::Error>;
type Result<T, E = Error> = std::result::Result<T, E>;

fn main() -> Result<(), String> {
    // for use in docker build where file changes can be wonky
    println!("cargo:rerun-if-env-changed=FORCE_REBUILD");
    println!("cargo:rerun-if-changed=proto/datafusion.proto");

    build()?;

    Ok(())
}

#[cfg(feature = "serde")]
fn build() -> Result<(), String> {
    let descriptor_path = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap())
        .join("proto_descriptor.bin");

    tonic_build::configure()
        .file_descriptor_set_path(&descriptor_path)
        .compile_well_known_types(true)
        .extern_path(".google.protobuf", "::pbjson_types")
        .compile(&["proto/datafusion.proto"], &["proto"])
        .map_err(|e| format!("protobuf compilation failed: {}", e))?;

    let descriptor_set = std::fs::read(descriptor_path).unwrap();
    pbjson_build::Builder::new()
        .register_descriptors(&descriptor_set)
        .unwrap()
        .build(&[".datafusion"])
        .map_err(|e| format!("pbjson compilation failed: {}", e))?;

    Ok(())
}

#[cfg(not(feature = "serde"))]
fn build() -> Result<(), String> {
    tonic_build::configure()
        .compile(&["proto/datafusion.proto"], &["proto"])
        .map_err(|e| format!("protobuf compilation failed: {}", e))
}
