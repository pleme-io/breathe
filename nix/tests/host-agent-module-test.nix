# Regression tests for the breathe-host-agent module trio.
#
# ── Why ────────────────────────────────────────────────────────────────────
# The trio is what makes the host agent RUNNABLE on a host — the L1 layer
# pleme-io/nix `node-budget.nix` documents riding within its static envelopes.
# It is pure flake wiring, and pure wiring is exactly what rots without a
# test: it evaluates fine right up until someone changes an accessor, and the
# failure then surfaces as a daemon silently running on prescribed defaults —
# which LOOKS correct, because that is a valid tier.
#
# The system-scope half is the part worth guarding hardest. Until substrate
# 7cde12c, `withShikumiConfig` was home-manager-only, so a system daemon got
# the `daemon` options and no way to feed them. These tests fail if that
# regresses.
#
# IFD-free: evaluates the emitted NixOS module against a stub universe. It
# never builds an image and never instantiates a real nixosSystem.
#
# Run: nix build .#checks.<system>.host-agent-module
{ pkgs, self, lib ? pkgs.lib }:

let
  dummyPkg = pkgs.runCommand "breathe-host-agent" { }
    "mkdir -p $out/bin; touch $out/bin/breathe-host-agent; chmod +x $out/bin/breathe-host-agent";

  # Stub universe: only the options the emitted module writes into.
  evalNixos = extra: lib.evalModules {
    modules = [
      self.nixosModules.default
      {
        options = {
          environment.etc = lib.mkOption { type = lib.types.attrsOf lib.types.anything; default = {}; };
          environment.systemPackages = lib.mkOption { type = lib.types.listOf lib.types.anything; default = []; };
          systemd.services = lib.mkOption { type = lib.types.attrsOf lib.types.anything; default = {}; };
        };
      }
      ({ ... }: { services.breathe-host-agent = { enable = true; package = dummyPkg; } // extra; })
    ];
    specialArgs = { inherit pkgs; };
  };

  configured = evalNixos {
    daemon.enable = true;
    settings = { metrics = { port = 9201; }; };
  };
  bare = evalNixos { daemon.enable = true; };

  unit = e: e.config.systemd.services."breathe-host-agent-daemon" or null;
  envOf = e: let u = unit e; in
    if u == null then {} else (u.environment or (u.serviceConfig.Environment or {}));

  results = lib.runTests {
    # ── The trio exists on all three arms ──────────────────────────────
    # A module that exists on one platform and not another is how a fleet
    # ends up with a NixOS-only feature nobody notices is missing on Darwin.
    testAllThreeArmsExist = {
      expr = {
        nixos = self.nixosModules ? default;
        darwin = self.darwinModules ? default;
        hm = self.homeManagerModules ? default;
      };
      expected = { nixos = true; darwin = true; hm = true; };
    };

    # ── The daemon actually renders ────────────────────────────────────
    testDaemonUnitIsRendered = {
      expr = unit configured != null;
      expected = true;
    };

    # ── ★ SYSTEM-SCOPE shikumi reaches the daemon ──────────────────────
    # This is the whole point of the substrate module-trio change. Without
    # it the agent silently runs on prescribed defaults, which LOOKS correct
    # (it is a valid tier!) and ignores every operator setting.
    testSettingsRenderToEtc = {
      expr = (configured.config.environment.etc) ? "breathe-host-agent/breathe-host-agent.yaml";
      expected = true;
    };

    testDaemonPointsAtTheRenderedConfig = {
      expr = (envOf configured).BREATHE_HOST_AGENT_CONFIG or null;
      expected = "/etc/breathe-host-agent/breathe-host-agent.yaml";
    };

    # The env var spelling is derived by mkModuleTrio from the package name
    # and asserted against `config::CONFIG_ENV_VAR` on the Rust side. If these
    # two ever disagree the agent reads no config at all, silently.
    testEnvVarMatchesRustConstant = {
      expr = builtins.hasAttr "BREATHE_HOST_AGENT_CONFIG" (envOf configured);
      expected = true;
    };

    # ── No settings ⇒ no file, no env var ──────────────────────────────
    # An empty YAML would make the binary resolve its `Custom` tier against a
    # document with no keys instead of resolving the prescribed tier.
    testUnconfiguredRendersNothing = {
      expr = {
        etc = (bare.config.environment.etc) ? "breathe-host-agent/breathe-host-agent.yaml";
        env = (envOf bare) ? "BREATHE_HOST_AGENT_CONFIG";
      };
      expected = { etc = false; env = false; };
    };

    # NOTE: there is deliberately NO release-app assertion here.
    # image.yml already builds the agent for amd64 AND arm64 (native, on
    # ubuntu-24.04-arm) and joins them in `host-agent-manifest`, all through
    # substrate's image-push.yml reusable. A second release path in this flake
    # would race those tags. What IS still worth pinning is that both arches
    # have an image to publish at all — below.

    # ── The agent image is built for both arches ───────────────────────
    # image.yml publishes both arches; this pins that both exist to publish.
    testAgentImageExistsOnBothLinuxArches = {
      expr = {
        amd = self.packages.x86_64-linux ? agent-image;
        arm = self.packages.aarch64-linux ? agent-image;
      };
      expected = { amd = true; arm = true; };
    };
  };
in
pkgs.runCommand "host-agent-module-test" { passthru.results = results; }
  (if results == [ ] then ''
    echo "breathe-host-agent module + release wiring: all regression tests passed"
    touch $out
  '' else ''
    echo "breathe-host-agent module FAILED:"
    cat <<'EOF'
    ${builtins.toJSON results}
    EOF
    exit 1
  '')
