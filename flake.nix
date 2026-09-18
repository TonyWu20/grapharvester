{
  description = "rust environment";
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    devshell.url = "github:numtide/devshell";
  };
  outputs = { nixpkgs, fenix, devshell, ... }:
    let
      systems = [ "x86_64-linux" "aarch64-darwin" ];
      pkgsFor = system: import nixpkgs {
        inherit system;
        config.allowUnfree = true;
        overlays = [ fenix.overlays.default devshell.overlays.default ];
      };

      forAllSystems = nixpkgs.lib.genAttrs systems;
    in
    {
      devShells = forAllSystems (
        system:
        let
          pkgs = pkgsFor system;
        in
        {
          default = pkgs.devshell.mkShell {
            packages = with pkgs; [
              (fenix.packages.${system}.stable.withComponents [
                "cargo"
                "clippy"
                "rust-src"
                "rustc"
                "rustfmt"
                "rust-analyzer"
              ])
              stdenv
              fish
              python3
              uv
            ];
          };
        }
      );


    };

}
