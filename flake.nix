# breathe — the resource-homeostasis controller, built the ONE pleme-io way.
#
# Build pattern: the single pleme-io Rust pattern — substrate's gen-driven
# `lockfile-builder.mkProject` over the committed `Cargo.gen.lock` delta
# (per-crate buildRustCrate derivations). NO buildRustPackage, NO fenix,
# NO crate2nix. After any Cargo.lock change run `gen build .` to re-emit
# the committed `Cargo.gen.lock` (the gitignored `Cargo.build-spec.json`
# is the derived full spec lockfile-builder consumes).
#
# Binaries: nix build .#breathe-controller   (the brain)
#           nix build .#breathe-host-agent   (the hands — DaemonSet)
#           nix build .#breathe-mcp          (the model surface)
#           nix build .#breathe-api-server   (the REST surface)
# Images:   nix build .#image / .#agent-image / .#api-server-image  (Linux)
#
# sqlx (postgres) → sqlx-sqlite → libsqlite3-sys builds clean because the
# composed `pkgs.defaultCrateOverrides` carries nixpkgs' libsqlite3-sys
# override (sqlite buildInput + pkg-config → build.rs detects system
# sqlite → prepare_v3 bindings). The substrate solved this long ago; the
# only requirement is composing defaultCrateOverrides, which we do.
{
  description = "breathe — resource-homeostasis controller (gen/lockfile-builder)";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-25.11";
    flake-utils.url = "github:numtide/flake-utils";
    substrate = {
      url = "github:pleme-io/substrate";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, substrate }:
    flake-utils.lib.eachSystem [
      "x86_64-linux"
      "aarch64-linux"
      "x86_64-darwin"
      "aarch64-darwin"
    ] (system: let
      pkgs = import nixpkgs { inherit system; };
      version = "0.1.2";

      # The ONE Rust pattern: gen-driven lockfile-builder. Per-crate
      # buildRustCrate derivations over the committed Cargo.gen.lock delta;
      # defaultCrateOverrides is composed so nixpkgs' per-crate quirks
      # (libsqlite3-sys → sqlite, aws-lc-sys → cmake, prost-build → protoc)
      # AND the fleet plemeCrateOverrides both apply.
      plemeCrateOverrides = import "${substrate}/lib/build/rust/pleme-crate-overrides.nix";
      lockfileBuilder = import "${substrate}/lib/build/rust/lockfile-builder.nix" { inherit pkgs; };
      # Per-member build accommodations for gen's src=workspaceSrc model.
      memberOverrides = {
        # breathe-api-server's build.rs compiles proto/breathe.proto via
        # tonic-build → prost-build, which shells out to `protoc`. The
        # hermetic sandbox has no protoc, so the proto-compiling member
        # declares its build-time tool need (the gen-pattern analog of
        # buildRustPackage's nativeBuildInputs = [ protobuf ]).
        breathe-api-server = attrs: {
          nativeBuildInputs = (attrs.nativeBuildInputs or [ ]) ++ [ pkgs.protobuf ];
          PROTOC = "${pkgs.protobuf}/bin/protoc";
        };
        # breathe-store embeds its SQL via sqlx::migrate!("./migrations"),
        # a COMPILE-TIME macro that canonicalizes the path against the
        # build root. Under gen the crate builds with src=workspaceSrc, so
        # the build root is the workspace root and `./migrations` misses
        # (the files live at breathe-store/migrations). Symlink them at the
        # root so the macro resolves. No-op under cargo dev (CWD is already
        # the crate dir, where ./migrations exists).
        breathe-store = attrs: {
          prePatch = (attrs.prePatch or "") + ''
            [ -e migrations ] || ln -s breathe-store/migrations migrations
          '';
        };
      };
      project = lockfileBuilder.mkProject {
        src = self;
        defaultCrateOverrides = pkgs.defaultCrateOverrides // plemeCrateOverrides // memberOverrides;
      };

      controller = project.workspaceMembers.breathe-controller.build;
      agent      = project.workspaceMembers.breathe-host-agent.build;
      mcp        = project.workspaceMembers.breathe-mcp.build;
      apiServer  = project.workspaceMembers.breathe-api-server.build;

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
        buildInputs = with pkgs; [ cargo rustc pkg-config cmake skopeo kubectl helm ];
      };
    });
}
