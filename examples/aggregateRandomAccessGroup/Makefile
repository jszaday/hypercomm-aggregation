CHARMC = $(CHARM_HOME)/bin/charmc
TESTRUN = $(CHARM_HOME)/bin/testrun
TESTFLAGS ?= +p4 ++local

BINARY = random_access

CHARMCFLAGS = $(OPTS) -I../../include

all: $(BINARY)

random_access: randomAccess.o
	$(CHARMC) $(CHARMCFLAGS) -language charm++ -o $@ $+

.SECONDARY: $(patsubst %.C,%.decl.h,$(wildcard *.C))
.SECONDARY: $(patsubst %.C,%.def.h,$(wildcard *.C))

randomAccess.def.h randomAccess.decl.h: randomAccess.ci.stamp

randomAccess.ci.stamp: randomAccess.ci
	$(CHARMC) $(CHARMCFLAGS) $<
	touch $@

randomAccess.o: randomAccess.C randomAccess.decl.h randomAccess.def.h
	$(CHARMC) $(CHARMCFLAGS) -c randomAccess.C

test: $(BINARY)
	$(TESTRUN) $(TESTFLAGS) ./random_access 14 8

clean:
	rm -f *.o *.decl.h *.def.h $(BINARY) charmrun* *.stamp
