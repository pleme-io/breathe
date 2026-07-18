{
  description = "breathe — resource-homeostasis controller (gen/lockfile-builder)";

  # substrate.rust.library dispatches over Cargo.gen.lock (the slim gen delta,
  # reconstructed to the full BuildSpec in pure Nix) — no crate2nix, no Cargo.nix.
  inputs = {
    substrate.url = "github:pleme-io/substrate";
    # For lib.genAttrs in the breathe-mcp secondary-package graft below.
    nixpkgs.follows = "substrate/nixpkgs";
  };

  outputs = { substrate, nixpkgs, ... }:
    let
      base = substrate.rust.library {
        src = ./.;
        member = "breathe-api-server";
      };

      # `breathe-mcp` — the MCP surface a model uses to drive a running breathe
      # instance (crate breathe-mcp). The fleet's claude MCP overlay consumes
      # `breathe.packages.<system>.breathe-mcp` (nix overlays/breathe.nix), but the
      # bare `member = "breathe-api-server"` build dropped it. Restore as a second
      # member build grafted per-system.
      mcpBase = substrate.rust.library {
        src = ./.;
        member = "breathe-mcp";
      };
      mcpSystems = [ "aarch64-darwin" "x86_64-darwin" "x86_64-linux" "aarch64-linux" ];
      withMcp = nixpkgs.lib.genAttrs mcpSystems (system:
        (base.packages.${system} or { }) // {
          breathe-mcp = mcpBase.packages.${system}.default;
        });
    in
    base // {
      packages = withMcp;
    };
}
