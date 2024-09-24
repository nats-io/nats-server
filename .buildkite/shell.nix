let
  pkgs = import <nixpkgs> {};
in
  pkgs.mkShell {
    packages = [
      pkgs.go
      pkgs.gotestsum
      pkgs.curl
    ];
  }
