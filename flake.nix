{
  description = "Flake utils demo";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-25.05";
    flake-utils.url = "github:numtide/flake-utils";
    limmat = {
      url = "github:bjackman/limmat";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      limmat,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs { inherit system; };
      in
      {
        formatter = pkgs.nixfmt-tree;
        packages = rec {
          # Just falba itself.
          falba = pkgs.buildGoModule {
            name = "falba";
            vendorHash = "sha256-Qdd5dImFn2LI2q1BAEdu3MLkakpHiqd2LHAUCzvyjDI=";
            src = ./.;
            buildInputs = with pkgs; [
              arrow-cpp
              duckdb
            ];
          };
          # Wrapped falba that includes the duckdb binary so that you don't need
          # to set --duckdb-cli when using the `falba sql` command.
          falba-with-duckdb =
            pkgs.runCommand "falba"
              {
                nativeBuildInputs = [ pkgs.makeWrapper ];
              }
              ''
                mkdir -p $out/bin

                makeWrapper ${falba}/bin/falba $out/bin/falba \
                  --prefix PATH : ${pkgs.lib.makeBinPath [ pkgs.duckdb ]}
              '';
          default = falba-with-duckdb;
        };
        apps = rec {
          falba = flake-utils.lib.mkApp { drv = self.packages.${system}.falba-with-duckdb; };
          default = falba;
        };
        devShells.default = pkgs.mkShell {
          packages = [ limmat.packages."${system}".default ];
          inputsFrom = [ self.packages."${system}".falba ];
        };
      }
    );
}
