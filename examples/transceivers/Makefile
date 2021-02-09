CHARMC=$(CHARM_HOME)/bin/charmc -g3 $(OPTS) -I../../include

OBJS = hello.o

all: hello

hello: $(OBJS)
	$(CHARMC) -language charm++ -o hello $(OBJS)

hello.decl.h: hello.ci
	$(CHARMC)  hello.ci

clean:
	rm -f *.decl.h *.def.h conv-host *.o hello charmrun

hello.o: hello.cc hello.decl.h
	$(CHARMC) -c hello.cc

test: all
	./charmrun ++local ++auto-provision ./hello 10
