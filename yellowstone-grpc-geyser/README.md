# Magicblock Validator Compatible Yellowstone gRPC Geyser Plugin

A high-performance gRPC plugin for Magicblock Geyser that streams validator's execution data in real-time.

## Compatibility

This plugin is **compatible with Magic Block Validator version 6.0**.

### Toolchain & Agave Versions

The toolchain and Agave versions used in this project are intentionally pinned to maintain compatibility with Magic Block Validator 6.0. Upgrading these versions may break compatibility with the validator. Only update if:

1. Magic Block Validator has been updated to support newer versions
2. You have verified compatibility in a test environment
3. You understand the potential impact on validator stability

## Building

```bash
cargo build --release
```

The compiled plugin will be available at `target/release/libyellowstone_grpc_geyser.so`.

## Configuration

Configuration is done via JSON file. See `config.json` for a complete example with all available options:

- **gRPC server**: Address, port, TLS configuration
- **Compression**: Algorithms and policies
- **Filter limits**: Constraints on accounts, slots, transactions, blocks
- **Prometheus**: Metrics endpoint configuration
- **Tokio runtime**: Worker threads settings

## Plugin Integration

This is a Solana Geyser plugin. To use it with a validator:

1. Build the plugin
2. Add to your validator's config:

  ```toml
  [aperture]
  geyser-plugins = ["path/to/plugin-config.json"]
  ```

3. Ensure the `libpath` in config.json points to the compiled `.so` file

