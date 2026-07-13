{
  nativelink,
  azurite,
  wait4x,
  bazelisk,
  curl,
  writeShellScriptBin,
}:
writeShellScriptBin "azurite-with-nativelink-test" ''
  set -euo pipefail

  cleanup() {
    local pids=$(jobs -pr)
    [ -n "$pids" ] && kill $pids
  }
  trap "cleanup" INT QUIT TERM EXIT

  TMPDIR=$HOME/.cache/nativelink/
  mkdir -p "$TMPDIR"

  AZURITE_DATA_DIR="''${TMPDIR}azurite-data"
  rm -Rf "$AZURITE_DATA_DIR"
  mkdir -p "$AZURITE_DATA_DIR"

  # --skipApiVersionCheck: the Rust Azure SDK sends a newer x-ms-version
  # than Azurite 3.35 accepts by default; without the flag every request
  # fails with InvalidHeaderValue.
  ${azurite}/bin/azurite \
    --blobHost 127.0.0.1 \
    --blobPort 10000 \
    --location "$AZURITE_DATA_DIR" \
    --skipApiVersionCheck \
    --silent 2>&1 | tee -i integration_tests/azure/azurite.log &
  ${wait4x}/bin/wait4x tcp 127.0.0.1:10000

  # Create the container used by integration_tests/azure/azure.json5. The
  # SAS token is deterministic and PUBLIC: it is signed with the documented
  # Azurite well-known dev key for devstoreaccount1 (no secrets involved).
  SAS="sv=2022-11-02&ss=b&srt=co&sp=rwdlac&st=2020-01-01T00:00:00Z&se=2099-01-01T00:00:00Z&spr=https,http&sig=JXUVFU%2FxumX1QKPqDU2%2FSfTR8TUdLRmQkVtXY2P%2BDpk%3D"
  create_code=$(${curl}/bin/curl -s -o /dev/null -w "%{http_code}" -X PUT \
    "http://127.0.0.1:10000/devstoreaccount1/nativelink-cas?restype=container&''${SAS}")
  case $create_code in
    201|409 )
      echo "Container ready (HTTP $create_code)"
    ;;
    *)
      echo "Failed to create container: HTTP $create_code"
      exit 1
    ;;
  esac

  ${nativelink}/bin/nativelink -- integration_tests/azure/azure.json5 2>&1 | tee -i integration_tests/azure/nativelink.log &
  ${wait4x}/bin/wait4x tcp localhost:50051

  if [[ $OSTYPE == "darwin"* ]]; then
      CACHE_DIR=$(mktemp -d "''${TMPDIR}azure-integration-test")
  else
      echo "Assumes Linux/WSL"
      CACHE_DIR=$(mktemp -d --tmpdir="$TMPDIR" --suffix="-azure-integration-test")
  fi
  BAZEL_CACHE_DIR="$CACHE_DIR/bazel"
  rm -Rf BAZEL_CACHE_DIR

  ${bazelisk}/bin/bazelisk --output_base="$BAZEL_CACHE_DIR" clean --expunge
  bazel_output=$(${bazelisk}/bin/bazelisk --output_base="$BAZEL_CACHE_DIR" test --config self_test //:dummy_test 2>&1 | tee -i integration_tests/azure/bazel-azure.log)
  ${bazelisk}/bin/bazelisk shutdown

  case $bazel_output in
    *"1 test passes"* )
      echo "Saw a successful bazel+azurite build"
    ;;
    *)
      echo 'Failed azure build:'
      echo $bazel_output
      exit 1
    ;;
  esac

  nativelink_output=$(cat integration_tests/azure/nativelink.log)

  case $nativelink_output in
    *"ERROR"* )
      echo "Error in nativelink build"
      exit 1
    ;;
    *)
      echo 'Successful nativelink build'
    ;;
  esac
''
