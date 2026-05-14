{
  description = "ducktape — DuckDB ↔ tech.v3.dataset bridge via Java Panama FFM";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
      in {
        devShells.default = pkgs.mkShell {
          buildInputs = [
            pkgs.clojure
            pkgs.duckdb
          ];
          shellHook = ''
            export DUCKDB_HOME="${pkgs.duckdb.lib}/lib"
            echo "ducktape dev shell — DuckDB $(${pkgs.duckdb}/bin/duckdb -version) at $DUCKDB_HOME"
          '';
        };
      });
}
