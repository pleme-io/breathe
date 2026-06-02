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
      version = "0.1.0";

      rustToolchain = fenix.packages.${system}.stable.toolchain;
      rustPlatform = pkgs.makeRustPlatform { cargo = rustToolchain; rustc = rustToolchain; };

      controller = rustPlatform.buildRustPackage {
        pname = "breathe-controller";
        version = version;
        src = ./.;
        cargoLock = { lockFile = ./Cargo.lock; };
        # Build + test the whole workspace; image entrypoint is the bin.
        cargoBuildFlags = [ "-p" "breathe-controller" ];
        nativeBuildInputs = with pkgs; [ pkg-config cmake perl ];
        doCheck = true;
        meta = {
          description = "breathe resource-homeostasis controller (theory/BREATHE.md)";
          license = pkgs.lib.licenses.mit;
          mainProgram = "breathe-controller";
        };
      };

      image = if pkgs.stdenv.isLinux then
        pkgs.dockerTools.buildLayeredImage {
          name = "ghcr.io/pleme-io/breathe-controller";
          tag = version;
          contents = with pkgs; [ controller cacert dockerTools.fakeNss ];
          config = {
            Entrypoint = [ "${controller}/bin/breathe-controller" ];
            User = "65532:65532";
            Env = [
              "PATH=${controller}/bin"
              "SSL_CERT_FILE=/etc/ssl/certs/ca-bundle.crt"
              "RUST_LOG=info,breathe_controller=info"
            ];
            Labels = {
              "org.opencontainers.image.source" = "https://github.com/pleme-io/breathe";
              "org.opencontainers.image.description" = "breathe resource-homeostasis controller";
              "org.opencontainers.image.licenses" = "MIT";
              "org.opencontainers.image.version" = version;
            };
          };
        }
      else
        pkgs.runCommand "breathe-controller-image-darwin-stub" {} ''
          mkdir -p $out
          echo "Build the OCI image on Linux: nix build .#image --system x86_64-linux" > $out/README
        '';

    in {
      packages = {
        default = controller;
        breathe-controller = controller;
        image = image;
      };
      apps.default = { type = "app"; program = "${controller}/bin/breathe-controller"; };
      devShells.default = pkgs.mkShellNoCC {
        buildInputs = with pkgs; [ rustToolchain pkg-config cmake skopeo kubectl helm ];
      };
    });
}
