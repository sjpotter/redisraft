
BUILDDIR := $(CURDIR)/.build
CFLAGS = -g -std=c99 -I$(BUILDDIR)/include -fPIC
CPPFLAGS = -D_POSIX_C_SOURCE=200112L -D_GNU_SOURCE
LDFLAGS = -shared
LIBS = $(BUILDDIR)/lib/libraft.a $(BUILDDIR)/lib/libuv.a -lpthread

OBJECTS = redisraft.o node.o util.o

redisraft.so: deps $(OBJECTS)
	$(LD) $(LDFLAGS) -o $@ $(OBJECTS) $(LIBS)

clean:
	rm -f redisraft.so $(OBJECTS)

cleanall: clean
	rm -rf $(BUILDDIR)
	$(MAKE) -C deps clean PREFIX=$(BUILDDIR)

.PHONY: deps
deps: $(BUILDDIR)/.deps_installed

$(BUILDDIR)/.deps_installed:
	mkdir -p $(BUILDDIR)
	$(MAKE) -C deps PREFIX=$(BUILDDIR)
	touch $(BUILDDIR)/.deps_installed
