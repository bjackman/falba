{
  description = "Flake utils demo";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-25.05";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let pkgs = import nixpkgs { inherit system; }; in
      {
        packages = rec {
          falba = pkgs.buildGoModule {
            name = "falba";
            vendorHash = "sha256-Qdd5dImFn2LI2q1BAEdu3MLkakpHiqd2LHAUCzvyjDI=";
            src = ./.;
            buildInputs = with pkgs; [ arrow-cpp duckdb ];
          };
          default = falba;
        };
        apps = rec {
          falba = flake-utils.lib.mkApp { drv = self.packages.${system}.falba; };
          default = falba;
        };
      }
    );
}