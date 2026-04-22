# How to contribute

If you would like to contribute, thank you :)

Here are a few informations you need to know before starting:

## Branches

Pull requests should be made against the default branch (currently 7.x).

## How to make good contributions

- Before starting to work, maybe open an issue to find whether your change would be accepted.
- Create relatively small PRs. This is easier to review, and will be merged faster. Do not send huge PRs with multiple unrelated changes.
- Make sure that you followed the design/style (see bellow).
- Make sure that your changes do not introduce new compiler warnings or errors.
- Do not make changes that would break existing code.

## Testing

Tests are in phpt file format in the tests directory.

### Using your own machine for building and testing. 

Tests can be run by following compilation and installation procedure 
and executing `make test`.

To run integration tests, make sure you have Kafka instance running.
Then, rename `test_env.php.sample` to `test_env.php` and adjust it
with values proper for your kafka instance.

## Design / naming things

php-rdkafka's goal is to expose the librdkafka APIs to PHP scripts, without
abstracting it. Rationale:

- Abstractions would be inherently opinionated, which would make the extension
  less than ideal or unusable in some cases.
- Abstractions are easily implemented in pure PHP on top of the extension.
- Remaining close to librdkafka in terms of naming/design makes it possible to
  refer to librdkafka's documentation and other resources when needed.

As a result, php-rdkafka will:

 - Follow librdkafka's naming for everything
 - Avoid introducing functions, helpers, classes that do not exist in
   librdkafka (these are easy to implement in pure PHP, on top of the
   extension).

However, in order to make the API PHP-ish, some transformations have to be done.

Here is the full design/style guide:

 - For librdkafka functions that return an error type, or signal errors via
   errno, php-rdkafka throws a Rdkafka\Exception
 - librdkafka structs are exposed as PHP objects. The object name is derived
   from the struct name like this:
   - Remove the `rd_kafka_` prefix
   - Convert from snake case to camel case
   - Add `Rdkafka\` namespace
 - `rd_kafka_*_new` functions are implemented as PHP object constructors / object
   instantiation
 - `rd_kafka_*_destroy` functions are implemented as PHP object free handlers
 - librdkaka functions that take a struct as first argument are implemented as
   a method of the struct's related PHP object
 - The user should not be required to manage memory (e.g. free somthing)
 - Do not change librdkafka's default behavior
 - Be safe: No user error should cause a crash or a memory leak.

## Local development environment

A Docker-based environment is provided via `docker-compose.yml` and `.docker/Dockerfile`. It starts a Kafka broker (+ Zookeeper) and a `build-env` container built from phusion/baseimage noble with PHP (ondrej/php PPA) and librdkafka compiled from source.

**Prerequisites:** Docker with Compose v2.

### First-time setup

Build the image and start all services:

```sh
docker compose up -d --build
```

The `build-env` image build takes a few minutes the first time (compiles librdkafka from source). Subsequent starts are instant.

### Building the extension

Open a shell in the build container:

```sh
docker compose exec build-env bash
```

Then inside the container:

```sh
cd /src
phpize --clean
phpize
./configure --with-rdkafka
make -j$(nproc)
```

`phpize --clean` is important if you have previously run `phpize` on the host (macOS): the generated `configure` script is platform-specific and the Linux toolchain will reject a macOS-generated one with a libtool syntax error. Re-run `phpize && ./configure` only when `config.m4` changes; otherwise incremental `make` is enough.

### Running tests

```sh
# Inside the build-env container
cp tests/test_env.php.sample tests/test_env.php
make test
```

`TEST_KAFKA_BROKERS` and `TEST_KAFKA_BROKER_VERSION` are already set in the container's environment, so the copied sample file works without edits.

To run a single test:

```sh
php run-tests.php tests/name-of-test.phpt
```

### Running the OAuth integration tests

The two `oauthbearer_integration*.phpt` tests require a second broker with SASL/OAUTHBEARER. Start it with the `oauth` profile:

```sh
# From the host, restart with the oauth profile
docker compose --profile oauth up -d
```

`TEST_KAFKA_OAUTH_BROKERS` is already set correctly in the container's environment, so no additional configuration is needed.

### Stopping the environment

```sh
docker compose down
```

### Switching librdkafka versions

The build container ships with v1.7.0, v1.9.2, v2.6.0, and v2.14.1 pre-built under `/opt/librdkafka/`.
Use the `use-librdkafka` script to switch the active version:

```sh
use-librdkafka v1.9.2
```

The script symlinks headers, libraries, and pkg-config files into `/usr/local`, then runs `ldconfig`.
After switching you must rebuild the extension:

```sh
phpize --clean && phpize && ./configure --with-rdkafka && make -j$(nproc)
```

### Changing the PHP version

Edit the `PHP_VERSION` arg in the `build-env` service in `docker-compose.yml`, then rebuild:

```sh
docker compose build build-env
```
