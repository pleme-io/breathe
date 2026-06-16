# breathe-controller image — the resource-homeostasis controller in a distroless OCI.
#
# Build binary: nix build .#breathe-controller
# Build image:  nix build .#image          (Linux only)
# Result:       ghcr.io/pleme-io/breathe-controller:<version>
#
# Rust workspace (breathe-control/-provider/-core/-catalog/-kube/-crd +
# dimension-memory + the controller bin). rustls everywhere → no openssl;
# cmake/perl are for aws-lc-rs (rustls' default crypto provider).
{
  description = "breathe — resource-homeostasis controller";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, fenix }:
    flake-utils.lib.eachSystem [
      "x86_64-linux"
      "aarch64-linux"
      "x86_64-darwin"
      "aarch64-darwin"
    ] (system: let
      pkgs = import nixpkgs { inherit system; };
      version = "0.1.2";

      rustToolchain = fenix.packages.${system}.stable.toolchain;
      rustPlatform = pkgs.makeRustPlatform { cargo = rustToolchain; rustc = rustToolchain; };

      controller = rustPlatform.buildRustPackage {
        pname = "breathe-controller";
        version = version;
        src = ./.;
        cargoLock = { lockFile = ./Cargo.lock; };
        # Build + test the whole workspace; image entrypoint is the bin.
        cargoBuildFlags = [ "-p" "breathe-controller" ];
        nativeBuildInputs = with pkgs; [ pkg-config cmake perl protobuf ];
        doCheck = true;
        meta = {
          description = "breathe resource-homeostasis controller (theory/BREATHE.md)";
          license = pkgs.lib.licenses.mit;
          mainProgram = "breathe-controller";
        };
      };

      # The HANDS — the host agent (a privileged DaemonSet). Same workspace src,
      # different bin. Runs as root (writes /sys; nsenter to host systemd for the
      # cgroup dimension later). util-linux ships nsenter for that future path.
      agent = rustPlatform.buildRustPackage {
        pname = "breathe-host-agent";
        version = version;
        src = ./.;
        cargoLock = { lockFile = ./Cargo.lock; };
        cargoBuildFlags = [ "-p" "breathe-host-agent" ];
        nativeBuildInputs = with pkgs; [ pkg-config cmake perl protobuf ];
        doCheck = false; # the workspace is tested by the controller build above
        meta = {
          description = "breathe host agent — the hands (host-dimension reconcile)";
          license = pkgs.lib.licenses.mit;
          mainProgram = "breathe-host-agent";
        };
      };

      mkImage = { name, bin, user, extraContents ? [], extraPath ? "", logTarget }:
        if pkgs.stdenv.isLinux then
          pkgs.dockerTools.buildLayeredImage {
            name = name;
            tag = version;
            contents = (with pkgs; [ cacert dockerTools.fakeNss ]) ++ extraContents;
            config = {
              Entrypoint = [ "${bin}" ];
              User = user;
              Env = [
                "PATH=${dirOf bin}${extraPath}"
                "SSL_CERT_FILE=/etc/ssl/certs/ca-bundle.crt"
                "RUST_LOG=info,${logTarget}=info"
              ];
              Labels = {
                "org.opencontainers.image.source" = "https://github.com/pleme-io/breathe";
                "org.opencontainers.image.description" = "breathe ${logTarget}";
                "org.opencontainers.image.licenses" = "MIT";
                "org.opencontainers.image.version" = version;
              };
            };
          }
        else
          pkgs.runCommand "${name}-image-darwin-stub" {} ''
            mkdir -p $out
            echo "Build the OCI image on Linux: nix build .#<image> --system x86_64-linux" > $out/README
          '';

      image = mkImage {
        name = "ghcr.io/pleme-io/breathe-controller";
        bin = "${controller}/bin/breathe-controller";
        user = "65532:65532";
        # The app-plane actuators reach their backends through TYPED Rust clients
        # (the `redis` crate, `reqwest`) — NO shelling, NO CLI in the image (the
        # stack-only / no-shell law). The image stays a pure distroless binary.
        extraContents = [ controller ];
        logTarget = "breathe_controller";
      };

      agentImage = mkImage {
        name = "ghcr.io/pleme-io/breathe-host-agent";
        bin = "${agent}/bin/breathe-host-agent";
        user = "0:0"; # root: writes /host/sys, nsenter to host systemd (cgroup, later)
        extraContents = with pkgs; [ agent util-linux ];
        extraPath = ":${pkgs.util-linux}/bin";
        logTarget = "breathe_host_agent";
      };

      apiServerImage = mkImage {
        name = "ghcr.io/pleme-io/breathe-api-server";
        bin = "${apiServer}/bin/breathe-api-server";
        user = "65532:65532"; # nonroot — a normal pod, no host access (reads/patches CRs)
        extraContents = [ apiServer ];
        logTarget = "breathe_api_server";
      };

      # The MCP surface — a model drives breathe over stdio. Run with a kubeconfig
      # pointing at the target cluster: `KUBECONFIG=… nix run .#breathe-mcp`.
      mcp = rustPlatform.buildRustPackage {
        pname = "breathe-mcp";
        version = version;
        src = ./.;
        cargoLock = { lockFile = ./Cargo.lock; };
        cargoBuildFlags = [ "-p" "breathe-mcp" ];
        nativeBuildInputs = with pkgs; [ pkg-config cmake perl protobuf ];
        doCheck = false; # tested by the controller build
        meta = {
          description = "breathe MCP surface — drive the homeostasis substrate from a model";
          license = pkgs.lib.licenses.mit;
          mainProgram = "breathe-mcp";
        };
      };

      # The REST surface (axum) over the same facade. `BREATHE_API_BIND=… nix run .#breathe-api-server`.
      apiServer = rustPlatform.buildRustPackage {
        pname = "breathe-api-server";
        version = version;
        src = ./.;
        cargoLock = { lockFile = ./Cargo.lock; };
        cargoBuildFlags = [ "-p" "breathe-api-server" ];
        nativeBuildInputs = with pkgs; [ pkg-config cmake perl protobuf ];
        doCheck = false;
        meta = {
          description = "breathe REST API (axum) over the BreatheStore facade";
          license = pkgs.lib.licenses.mit;
          mainProgram = "breathe-api-server";
        };
      };

    in {
      packages = {
        default = controller;
        breathe-controller = controller;
        breathe-host-agent = agent;
        breathe-mcp = mcp;
        breathe-api-server = apiServer;
        image = image;
        agent-image = agentImage;
        api-server-image = apiServerImage;
      };
      apps.default = { type = "app"; program = "${controller}/bin/breathe-controller"; };
      apps.breathe-mcp = { type = "app"; program = "${mcp}/bin/breathe-mcp"; };
      apps.breathe-api-server = { type = "app"; program = "${apiServer}/bin/breathe-api-server"; };
      devShells.default = pkgs.mkShellNoCC {
        buildInputs = with pkgs; [ rustToolchain pkg-config cmake skopeo kubectl helm ];
      };
    });
}
