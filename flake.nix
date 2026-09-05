{
  description = "Basic R, Python, and Julia Flake";
  inputs = {
    system-flake.url = "path:/etc/nixos";
    nixpkgs.follows = "system-flake/nixpkgs";
  };

  outputs =
    { nixpkgs, ... }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs {
        inherit system;
        config.allowUnfree = true;
      };
      myRPackages = with pkgs.rPackages; [
        ISLR2
        RColorBrewer
        corrplot
        ggcorrplot
        FactoMineR
        factoextra
        corrr
        dplyr
        ggplot2
        GGally
        glmnet
        lmtest
        leaps
        fmcmc
        randomForest
        reshape2
        rmarkdown
        nortest
        writexl
        olsrr
        readxl
        xtable
        stargazer
        broom
        rgl
      ];
      myR = pkgs.rWrapper.override { packages = myRPackages; };
      myRStudio = pkgs.rstudioWrapper.override { packages = myRPackages; };
    in
    {
      devShells.${system}.default = pkgs.mkShellNoCC {
        name = "RPy Flake";

        buildInputs =
          with pkgs;
          [
            myR
            myRStudio
            python313
          ]
          ++ (with python313Packages; [
            marimo
            matplotlib
            numpy
            openpyxl
            pandas
            pip
            python-lsp-server
            requests
            scikit-learn
            scipy
            seaborn
            sympy
            tensorflow
          ]);

        shellHook = ''
          alias rs="rstudio > /dev/null 2>&1 &"
        '';
      };
    };
}
