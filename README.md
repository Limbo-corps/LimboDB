# LimboDB

A lightweight SQL database engine written in C++

## Features

- SQL query parsing and execution
- Record and index management
- Table catalog system
- Disk-based storage
- Memory management
- Debug support

## Quick Start

### Prerequisites

**Linux (Ubuntu/Debian):**
```bash
sudo apt update
sudo apt install build-essential cmake git
```

**Linux (CentOS/RHEL/Fedora):**
```bash
# CentOS/RHEL
sudo yum install gcc-c++ cmake git make
# Fedora
sudo dnf install gcc-c++ cmake git make
```

**macOS:**
```bash
# Install Xcode Command Line Tools
xcode-select --install
# Install CMake (using Homebrew)
brew install cmake
```

**Windows:**
1. Install [MSYS2](https://www.msys2.org/)
2. Open MSYS2 UCRT64 terminal and run:
```bash
pacman -S mingw-w64-ucrt-x86_64-gcc mingw-w64-ucrt-x86_64-cmake mingw-w64-ucrt-x86_64-make git
```
3. Add `C:\msys64\ucrt64\bin` to your system PATH

### Local Build

**Linux/macOS:**
```bash
git clone https://github.com/prasangeet/LimboDB.git
cd LimboDB
mkdir build && cd build
cmake ..
make
./dbms
```

**Windows (MSYS2 UCRT64 terminal):**
```bash
git clone https://github.com/prasangeet/LimboDB.git
cd LimboDB
mkdir build && cd build
cmake ..
make
./dbms.exe
```

### Docker

```bash
docker build -t limbo-db .
docker run -it --rm limbo-db
```

## Contributing

Bug reports and pull requests are welcome on GitHub.

## License

MIT License - see [LICENSE](LICENSE) file.

## Contact

**Author:** b23ch1033@iitj.ac.in  
**Issues:** [GitHub Issues](https://github.com/prasangeet/LimboDB/issues)