use std::path::PathBuf;

use rosetta_mq::config::Config;

/// Simulates a shipped binary launched from an arbitrary working directory (a systemd unit, a
/// desktop shortcut, a different shell session than the one the config lives in) -- a relative
/// `proto_file` in the config must resolve against the *config file's own directory*, not
/// whatever directory the process happens to be running from.
#[test]
fn relative_proto_file_resolves_against_config_directory_not_process_cwd() {
    let test_dir = std::env::temp_dir().join(format!(
        "rosetta-mq-relative-path-test-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&test_dir).unwrap();

    let fixtures_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    std::fs::copy(
        fixtures_dir.join("protobuf/device.proto"),
        test_dir.join("device.proto"),
    )
    .unwrap();

    // `device.proto` imports `common/status.proto`; mirror that relative layout under `test_dir`
    // so the decoder's *default* include path (device.proto's own directory) still covers it --
    // this test is about `proto_file` resolution, not `include_paths`, so keep that unrelated.
    std::fs::create_dir_all(test_dir.join("common")).unwrap();
    std::fs::copy(
        fixtures_dir.join("common/status.proto"),
        test_dir.join("common/status.proto"),
    )
    .unwrap();

    let config_path = test_dir.join("rosetta-mq.toml");
    std::fs::write(
        &config_path,
        r#"
        [broker]
        host = "127.0.0.1"
        port = 1883
        client_id = "rosetta-mq"

        [[topic]]
        topic_filter = "devices/+/raw"
        decoder = "protobuf"
        proto_file = "device.proto"
        message_type = "device.v1.DeviceReading"
    "#,
    )
    .unwrap();

    // Move the process's CWD somewhere unrelated to both the config and the schema, so the only
    // way this can succeed is if resolution is anchored to the config file's path, not the CWD.
    std::env::set_current_dir(std::env::temp_dir()).unwrap();

    let config = Config::load(&config_path).unwrap();
    let base_dir = config_path.parent().unwrap();
    let result = config.topics[0].decoder.build(base_dir);

    std::fs::remove_dir_all(&test_dir).ok();

    assert!(
        result.is_ok(),
        "expected relative proto_file to resolve against the config file's directory \
         regardless of the process's current directory, got: {:?}",
        result.err()
    );
}
