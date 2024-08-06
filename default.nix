{ lib
, python3
, fetchPypi
, fetchFromGitHub
}:
let
  atomicwrites = python3.pkgs.buildPythonPackage rec {
    pname = "atomicwrites";
    version = "1.4.0";
    pyproject = true;

    src = fetchPypi {
      inherit pname version;
      hash = "sha256-rnA5atGkNPnHBG/S3RlvwEsS+ekf+4WRZBk76LYWino=";
    };

    nativeBuildInputs = [
      python3.pkgs.setuptools
      python3.pkgs.wheel
    ];

    pythonImportsCheck = [ "atomicwrites" ];

    meta = with lib; {
      description = "Atomic file writes";
      homepage = "https://pypi.org/project/atomicwrites/";
      license = licenses.mit;
      maintainers = with maintainers; [ ];
      mainProgram = "atomicwrites";
    };
  };
in
python3.pkgs.buildPythonPackage rec {
  pname = "zinc";
  version = "unstable-2024-04-09";
  pyproject = true;

  src = ./.;
  ###################################################################
  # src = fetchFromGitHub {                                         #
  #   owner = "mindsnacks";                                         #
  #   repo = "Zinc";                                                #
  #   rev = "8efa92e4d9da30b5eb5c379abf64090f6833aec5";             #
  #   hash = "sha256-zjkv7SuulaQVWl2N1O/32An3nDMChR3JC+OYfYM58ZA="; #
  # };                                                              #
  ###################################################################

  nativeBuildInputs = [
    python3.pkgs.setuptools
    python3.pkgs.wheel
  ];

  propagatedBuildInputs = with python3.pkgs; [
    setuptools
    atomicwrites
    boto3
    jsonschema
    lockfile
    redis
    requests
    toml
  ];

  pythonImportsCheck = [ "zinc" ];

  meta = with lib; {
    description = "Asset distribution system for mobile clients";
    homepage = "https://github.com/mindsnacks/Zinc";
    license = licenses.mit;
    maintainers = with maintainers; [ ];
    mainProgram = "zinc";
  };
}
