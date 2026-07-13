use nativelink_error::Error;
use walkdir::WalkDir;

#[test]
fn walkdir_source_error() {
    for entry in WalkDir::new("/bad/path") {
        let err: Error = entry.unwrap_err().into();
        let os_error = {
            #[cfg(unix)]
            {
                "No such file or directory (os error 2)"
            }
            #[cfg(windows)]
            {
                "The system cannot find the path specified. (os error 3)"
            }
        };
        assert_eq!(
            err.messages,
            vec![
                os_error,
                &format!("IO error for operation on /bad/path: {os_error}")
            ]
        );
    }
}

mod version_marker_tests {
    use nativelink_error::{Code, Error, VERSION_MESSAGE_SUFFIX, make_err};

    /// Issue #1044: errors returned to REAPI clients over tonic carry the
    /// `NativeLink` version.
    #[test]
    fn tonic_status_carries_version_marker() {
        let status: tonic::Status = make_err!(Code::NotFound, "Blob not found").into();
        assert_eq!(
            status.message(),
            format!("Blob not found{VERSION_MESSAGE_SUFFIX}")
        );
        assert_eq!(status.code(), Code::NotFound);
    }

    /// Issue #1044: errors serialized into proto bodies (for example inside
    /// `ExecuteResponse`) carry the `NativeLink` version too.
    #[test]
    fn google_rpc_status_carries_version_marker() {
        let status: nativelink_proto::google::rpc::Status =
            make_err!(Code::Internal, "Store failed").into();
        assert_eq!(
            status.message,
            format!("Store failed{VERSION_MESSAGE_SUFFIX}")
        );
        assert_eq!(status.code, Code::Internal as i32);
    }

    /// An empty error message becomes just the (trimmed) version marker
    /// rather than a leading-space oddity.
    #[test]
    fn empty_message_becomes_bare_version_marker() {
        let status: tonic::Status = Error::new(Code::NotFound, String::new()).into();
        assert_eq!(status.message(), VERSION_MESSAGE_SUFFIX.trim_start());
    }

    /// Proxy chains convert Status -> Error -> Status; the marker must not
    /// stack for the same version.
    #[test]
    fn version_marker_is_idempotent_across_round_trips() {
        let status: tonic::Status = make_err!(Code::Unavailable, "Upstream down").into();
        let round_tripped: Error = status.into();
        let status_again: tonic::Status = round_tripped.into();
        assert_eq!(
            status_again.message().matches("(nativelink v").count(),
            1,
            "Version marker must appear exactly once, got: {}",
            status_again.message()
        );
    }
}
