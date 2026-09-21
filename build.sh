#!/usr/bin/sh


cargo build --release --target x86_64-unknown-linux-musl
cp target/x86_64-unknown-linux-musl/release/mitm-transformer ../../bin/bin/mitm-transformer-main

