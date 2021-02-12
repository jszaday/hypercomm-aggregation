CHARMC=$(CHARM_HOME)/bin/charmc -g3 $(OPTS) -I../../include

OBJS = tracert.o

all: tracert

tracert: $(OBJS)
	$(CHARMC) -language charm++ -o tracert $(OBJS)

tracert.decl.h: tracert.ci
	$(CHARMC) tracert.ci

clean:
	rm -f *.decl.h *.def.h conv-host *.o tracert charmrun

tracert.o: tracert.cc tracert.decl.h
	$(CHARMC) -c tracert.cc

test: all
	./tracert ++quiet
