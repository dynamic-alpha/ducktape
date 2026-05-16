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
        jdk = pkgs.jdk25;
        # Re-build clojure against jdk25 so the `clojure` launcher invokes the
        # right `java` (nixpkgs' default `clojure` is pinned to the default
        # JDK, which is still 21).
        clojure = pkgs.clojure.override { inherit jdk; };
      in {
        devShells.default = pkgs.mkShell {
          buildInputs = [
            jdk
            clojure
            pkgs.duckdb
            pkgs.clj-kondo
            pkgs.cljfmt
          ];
          shellHook = ''
            export DUCKDB_HOME="${pkgs.duckdb.lib}/lib"
            export JAVA_HOME="${jdk}"
            echo "ducktape dev shell — DuckDB $(${pkgs.duckdb}/bin/duckdb -version)  /  JDK $(${jdk}/bin/java --version | head -1)"
            echo "  DUCKDB_HOME=$DUCKDB_HOME"
            echo "  JAVA_HOME=$JAVA_HOME"
          '';
        };
      });
}
