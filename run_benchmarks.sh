rm -rf build
mkdir -p build
cd build
# TODO Fetch git submodules here.
cmake -DCMAKE_BUILD_TYPE=Debug .. && cmake --build .
# TODO: Add Cmake flags to control flags in config.h and use that to build.
echo "Starting random keys benchmark"
./BenchmarkLM 0
cp stats.csv ../random_keys.csv
echo "Starting zipf dist benchmark"
./BenchmarkLM 1
cp stats.csv ../zipf.csv
cd ..