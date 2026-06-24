.PHONY: all compile test shell check clean

all: compile

# Compile the C NIF (via the rebar pre-hook) and the Erlang module.
compile:
	rebar3 compile

# Run the EUnit test suite.
test: compile
	rebar3 eunit

# Erlang shell with elmdb loaded.
shell: compile
	rebar3 shell

# Dialyzer static analysis.
check: compile
	rebar3 dialyzer

clean:
	rebar3 clean
	rm -rf _build priv/*.so priv/*.dylib
