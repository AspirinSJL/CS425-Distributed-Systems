# Distributed log querier (CS 425 MP1)

## How to prepare

1. Because this program is totally implemented by Python, a dynamic language, you don't need to compile this program before using.
2. Nevertheless, you need to install Python (with proper PATH setting) and the dependent libs used in this program. Namely, you need `yaml` for configuration. If you want to run the unit tests, you should also install `rstr` for generating strings of certain regular expressions.

## How to run

1. To run the application on server end, just run `python server.py`. And keep it alive.
2. Before running the client end, you may want to run the unit test to verify it first. On each server, `python gen_testlog.py` will generate a log file in a different directory other than the one normal queries use. `python test.py` on a querying client will do the tests.
3. To run the application on client end, just run `python client.py` and type your grep command. It will print the result like the original grep does, plus some additional logs for the distributed log querier itself.