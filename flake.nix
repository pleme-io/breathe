# breathe — the resource-homeostasis controller, built the ONE pleme-io way.
#
# Build: gen-driven substrate.rust.library over the committed Cargo.gen.lock
# (per-crate buildRustCrate derivations — no crate2nix, no Cargo.nix; Cargo.nix
# regen was failing in the bump job, the reason 3669c84 converted this repo to
# the gen pattern). The shape builds exactly ONE workspace member per call, so
# each binary this repo ships (controller / host-agent / mcp / api-server)
# gets its own `substrate.rust.library` invocation.
#
# Images: hardened via substrate's oci/hardened-base.nix (distroless-glibc —
# no shell, nonroot-by-default, Pillar 8) instead of a hand-rolled
# dockerTools.buildLayeredImage — the SAME hardened substrate every other
# pleme-io image builds against. This restores the `image` / `agent-image` /
# `api-server-image` package outputs `.github/workflows/image.yml` needs
# (dropped when 3669c84's conversion only carried over the single-binary
# breathe-api-server + breathe-mcp shape) AND upgrades them from a plain
# `cacert + binary` layer to the fleet's hardened base, matching image.yml's
# own zero-tolerance CVE gate (pleme-io/actions/image-scan, fail-on-severity:
# HIGH).
#
# Charts: helm lifecycle apps (lint/release/mirror/template/bump) for
# charts/pleme-breathe are restored too — helm-release.yml's `nix run
# .#release` depends on them and was silently broken by the same commit.
{
  description = "breathe — resource-homeostasis controller (gen/lockfile-builder, hardened images)";

  inputs = {
    substrate.url = "github:pleme-io/substrate";
    nixpkgs.follows = "substrate/nixpkgs";
    forge = {
      url = "github:pleme-io/forge";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, substrate, nixpkgs, forge, ... }:
    let
      version = "0.1.2";
      allSystems = [ "aarch64-darwin" "x86_64-darwin" "x86_64-linux" "aarch64-linux" ];
      linuxSystems = [ "x86_64-linux" "aarch64-linux" ];
      lib = nixpkgs.lib;

      # Per-member build accommodations for gen's src=workspaceSrc model —
      # ported over from the pre-3669c84 flake.nix's `memberOverrides`, which
      # the gen-pattern conversion silently dropped (root cause of the first
      # post-fix CI failure: breathe-store's sqlx::migrate!("./migrations")
      # canonicalizes against the build root, which under gen IS the
      # workspace root, so the bare "./migrations" relative path misses —
      # the files live at breathe-store/migrations).
      sharedCrateOverrides = {
        breathe-store = attrs: {
          prePatch = (attrs.prePatch or "") + ''
            [ -e migrations ] || ln -s breathe-store/migrations migrations
          '';
        };
      };

      # One `substrate.rust.library` build per workspace member — the shape
      # only builds a single binary per call (see the sibling breathe-mcp
      # graft this file carried even before this rewrite).
      memberFlake = member: extra: substrate.rust.library ({
        src = ./.;
        inherit member;
        crateOverrides = sharedCrateOverrides // (extra.crateOverrides or { });
      } // (removeAttrs extra [ "crateOverrides" ]));

      controllerFlake = memberFlake "breathe-controller" { };
      agentFlake      = memberFlake "breathe-host-agent" { };
      mcpFlake        = memberFlake "breathe-mcp" { };
      # breathe-api-server's build.rs compiles proto/breathe.proto via
      # tonic-build → prost-build, which shells out to `protoc` — the
      # gen-pattern analog of buildRustPackage's nativeBuildInputs =
      # [ protobuf ]. `nativeBuildInputs` here is a list of nixpkgs
      # attribute NAMES (resolved per-target-system by tool-release.nix),
      # not derivations, so no per-system `pkgs` needs to be in scope here.
      #
      # NOTE: do NOT also pass a `crateOverrides.breathe-api-server` entry
      # here -- tool-release.nix's own merge is `defaultCrateOverrides //
      # plemeCrateOverrides // { ${crateKey} = <the nativeBuildInputs-
      # wiring closure> } // crateOverrides`, a shallow `//`, not a deep
      # merge. A caller-supplied `crateOverrides.breathe-api-server` entry
      # REPLACES that closure wholesale, silently discarding the
      # `nativeBuildInputs` wiring above -- confirmed live 2026-07-18: a
      # first attempt that also set `PROTOC = "protoc"` via crateOverrides
      # clobbered the protobuf nativeBuildInput and the build failed with
      # "Could not find `protoc`" even though `nativeBuildInputs =
      # ["protobuf"]" was right there. Once protobuf is genuinely on PATH
      # via nativeBuildInputs, prost-build's own `which protoc` fallback
      # finds it -- no PROTOC env var needed at all.
      apiServerFlake  = memberFlake "breathe-api-server" {
        nativeBuildInputs = [ "protobuf" ];
      };

      binFor = flake: system: flake.packages.${system}.default;

      # ── hardened images — distroless-glibc: no shell, nonroot by default ──
      mkImages = system: let
        pkgs = import nixpkgs { inherit system; };
        hardened = import "${substrate}/lib/build/oci/hardened-base.nix" { inherit pkgs; };
        controllerBin = binFor controllerFlake system;
        agentBin      = binFor agentFlake system;
        apiServerBin  = binFor apiServerFlake system;
        sslEnv = "SSL_CERT_FILE=/etc/ssl/certs/ca-bundle.crt";
      in {
        image = hardened.mkPackageImage {
          service = "breathe-controller";
          base = hardened.bases.distroless-glibc;
          package = controllerBin;
          publishName = "ghcr.io/pleme-io/breathe-controller";
          publishTag = version;
          entrypoint = [ "${controllerBin}/bin/breathe-controller" ];
          env = [ sslEnv "RUST_LOG=info,breathe_controller=info" ];
        };
        agent-image = hardened.mkPackageImage {
          service = "breathe-host-agent";
          base = hardened.bases.distroless-glibc;
          package = agentBin;
          publishName = "ghcr.io/pleme-io/breathe-host-agent";
          publishTag = version;
          entrypoint = [ "${agentBin}/bin/breathe-host-agent" ];
          # root: writes /host/sys, nsenter into host systemd for cgroup control
          user = "0:0";
          extraContents = [ pkgs.util-linux ];
          env = [
            sslEnv
            "PATH=${agentBin}/bin:${pkgs.util-linux}/bin"
            "RUST_LOG=info,breathe_host_agent=info"
          ];
        };
        api-server-image = hardened.mkPackageImage {
          service = "breathe-api-server";
          base = hardened.bases.distroless-glibc;
          package = apiServerBin;
          publishName = "ghcr.io/pleme-io/breathe-api-server";
          publishTag = version;
          entrypoint = [ "${apiServerBin}/bin/breathe-api-server" ];
          # nonroot (mkPackageImage's default) — a normal pod, no host access
          env = [ sslEnv "RUST_LOG=info,breathe_api_server=info" ];
        };
      };

      darwinImageStub = system: name:
        (import nixpkgs { inherit system; }).runCommand "${name}-image-darwin-stub" { } ''
          mkdir -p $out
          echo "Build the OCI image on Linux: nix build .#${name} --system x86_64-linux" > $out/README
        '';

      packagesFor = system:
        {
          default = binFor controllerFlake system;
          breathe-controller = binFor controllerFlake system;
          breathe-host-agent = binFor agentFlake system;
          breathe-mcp = binFor mcpFlake system;
          breathe-api-server = binFor apiServerFlake system;
        }
        // (if lib.elem system linuxSystems
          then mkImages system
          else {
            image = darwinImageStub system "image";
            agent-image = darwinImageStub system "agent-image";
            api-server-image = darwinImageStub system "api-server-image";
          });

      # Chart lifecycle apps (lint/release/mirror/template/bump) for the ONE
      # chart in this repo — helm-release.yml's `nix run .#release` call.
      helmAppsFor = system: let
        pkgs = import nixpkgs { inherit system; };
        substrateLib = substrate.libFor {
          inherit pkgs system;
          forge = forge.packages.${system}.default;
        };
      in substrateLib.mkHelmAllApps {
        charts = [
          { name = "pleme-breathe"; chartDir = ./charts/pleme-breathe; }
        ];
        registry = "oci://ghcr.io/pleme-io/charts";
      };

      appsFor = system:
        (controllerFlake.apps.${system} or { })
        // (helmAppsFor system)
        // {
          default = {
            type = "app";
            program = "${binFor controllerFlake system}/bin/breathe-controller";
          };
          breathe-mcp = {
            type = "app";
            program = "${binFor mcpFlake system}/bin/breathe-mcp";
          };
          breathe-api-server = {
            type = "app";
            program = "${binFor apiServerFlake system}/bin/breathe-api-server";
          };
        };
    in {
      packages = lib.genAttrs allSystems packagesFor;
      apps = lib.genAttrs allSystems appsFor;
      devShells = lib.genAttrs allSystems (system: controllerFlake.devShells.${system} or { });

      # Regression coverage for the module trio + multi-arch release wiring.
      # Pure flake wiring is exactly what rots unnoticed: it evaluates fine
      # until an accessor changes, and then fails as a daemon silently running
      # on prescribed defaults, or as `exec format error` on an arm node.
      checks = lib.genAttrs allSystems (system: let
        pkgs = import nixpkgs { inherit system; };
      in (controllerFlake.checks.${system} or { }) // {
        host-agent-module =
          import ./nix/tests/host-agent-module-test.nix { inherit pkgs; inherit self; };
      });

      # ── ★ THE MODULE TRIO ───────────────────────────────────────────
      # Top-level, NOT per-system: a module is evaluated against the
      # CONSUMER's pkgs, so it is system-agnostic by definition.
      #
      # WHY IT EXISTS: breathe shipped the host agent as a package and an OCI
      # image and nothing else, so a consumer wanting to run it on a HOST — the
      # entire L1 layer pleme-io/nix `node-budget.nix` documents riding within
      # its static envelopes — had no module to enable. The layer was named in
      # prose and absent in fact, which is why that fleet's static memory
      # budget sat three months stale and inverted against measured use: no
      # live controller existed to correct it, and no supported way to run one.
      #
      # `withShikumiConfig` is what makes breathe-host-agent/src/config.rs
      # reachable from a host: the NixOS/Darwin arms render the typed YAML to
      # /etc and put BREATHE_HOST_AGENT_CONFIG on the daemon unit, which is
      # exactly the `Custom(path)` tier `HostAgentConfig::load()` resolves.
      # That system-scope render landed in substrate 7cde12c — before it, the
      # flag was home-manager-only and a system daemon got no config at all.
    } // (
      let
        trio = (import "${substrate}/lib/module-trio.nix" {
          inherit (nixpkgs) lib;
        }).mkModuleTrio {
          name = "breathe-host-agent";
          description = "breathe host agent — the hands (host-dimension reconcile)";
          binaryName = "breathe-host-agent";
          packageAttr = "breathe-host-agent";

          # A privileged host daemon: it writes /sys and drives systemd. The HM
          # arm still exists (the trio emits all three) but the DAEMON is
          # system-scope only — a user-scope agent could not write the things
          # it exists to write, so offering the option would be offering a
          # no-op.
          withSystemDaemon = true;
          withUserDaemon = false;
          # The binary serves with NO subcommand — main goes straight into the
          # reconcile loop. "" yields ZERO argv entries; a literal "" would
          # reach clap on Darwin as an unexpected argument.
          daemonSubcommand = "";

          withShikumiConfig = true;
          # Mirrors HostAgentConfig::prescribed_default(). Minimal on purpose:
          # `#[serde(default)]` on the Rust side means anything omitted still
          # resolves to the prescribed value rather than to a zero, so this
          # carries only what an operator is likely to touch.
          shikumiDefaults = {
            reconcile = { requeue-seconds = 30; controller-name = "breathe-host-agent"; };
            # 9101, never 9100 — 9100 is the host node-exporter.
            metrics = { enabled = true; address = "0.0.0.0"; port = 9101; };
            logging = { filter = "info,breathe_host_agent=info"; format = "json"; };
            dimensions = { arc = true; cgroup-memory = true; cgroup-cpu = true; };
          };
        };
      in {
        nixosModules.default = trio.nixosModule;
        nixosModules.breathe-host-agent = trio.nixosModule;
        darwinModules.default = trio.darwinModule;
        darwinModules.breathe-host-agent = trio.darwinModule;
        homeManagerModules.default = trio.homeManagerModule;
        homeManagerModules.breathe-host-agent = trio.homeManagerModule;
      }
    );
}
