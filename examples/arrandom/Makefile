CHARMC=$(CHARM_HOME)/bin/charmc -g3 $(OPTS) -I../../include
BINARY = arrandom
OBJS = $(BINARY).o

all: $(BINARY)

$(BINARY): $(OBJS)
	$(CHARMC) -language charm++ -o $(BINARY) $(OBJS)

$(BINARY).decl.h: $(BINARY).ci
	$(CHARMC) $(BINARY).ci

clean:
	rm -f *.decl.h *.def.h conv-host *.o $(BINARY) charmrun

$(BINARY).o: $(BINARY).cc $(BINARY).decl.h
	$(CHARMC) -c $(BINARY).cc

test: all
	./charmrun ++local ++auto-provision ./$(BINARY) 10
