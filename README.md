# plane.watch sbsmux

Receives SBS data via connect out or connect in. Sends SBS data via connect out or connect in.

Originally written to merge various sources of SATCOM ADS-C data.

## Usage

| Command Line Argument | Purpose | Default |
|-----------------------|---------|---------|
| **Global Options** |||
| `--help`, `-h` | show help | |
| `--version`, `-v` | print the version | |
| **SBS In** |||
| `--inputconnect` | Connect to SBS data source to retrieve data. </br> Can be specified multiple times. | |
| `--inputlisten` | Listen on this TCP address for connections to receive data. | `:30103` |
| **SBS Out** |||
| `--outputconnect` | Connect to SBS data source to send data. </br> Can be specified multiple times. | |
| `--outputlisten` | Listen on this TCP address for connections to send data. | `:30003` |
| **Timeouts/Durations** |||
| `--reconnecttimeout` | Delay between connection attempts (seconds). | `10` |
| `--statsinterval` | Delay between printing per-connection statistics (minutes) | `10` |

## Building

Requires Go version 1.22.0 or higher.

* `make` will build `sbsmux` to `./bin/sbsmux`
* `make install` will build & install to `/usr/local/sbin/sbsmux`
* `make clean` will remove all build artifacts
