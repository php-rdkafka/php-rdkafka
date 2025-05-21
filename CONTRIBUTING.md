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

