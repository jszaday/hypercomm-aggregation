CHARMC=$(CHARM_HOME)/bin/charmc

all: tracert

tracert: tracert.cc
	$(CHARMC) -nomain ./tracert.cc -o tracert -I../../include/

test: all
	./tracert
