# tachyon -- zero-dependency build front end.
#
# CMake is the primary build system. This Makefile exists because "how do I
# build this" should have a one-word answer, and because a library that needs
# no dependencies should not require one to compile.

CC      ?= gcc
AR      ?= ar
CFLAGS  ?= -std=c11 -O3 -Wall -Wextra -Wshadow -Iinclude -Isrc
LDLIBS  ?= -lpthread -lrt
PREFIX  ?= /usr/local

SRCS     = $(wildcard src/*.c)
OBJS     = $(SRCS:.c=.o)
TESTS    = test_basic test_crash test_chaos test_stress
BUILDDIR = build

.PHONY: all lib test check bench clean install examples asan ubsan tsan help

all: lib examples tests bench

lib: $(BUILDDIR)/libtachyon.a $(BUILDDIR)/libtachyon.so

$(BUILDDIR):
	@mkdir -p $(BUILDDIR) $(BUILDDIR)/tests

$(BUILDDIR)/%.o: src/%.c src/internal.h | $(BUILDDIR)
	$(CC) $(CFLAGS) -fPIC -c $< -o $@

$(BUILDDIR)/libtachyon.a: $(patsubst src/%.c,$(BUILDDIR)/%.o,$(SRCS))
	$(AR) rcs $@ $^

$(BUILDDIR)/libtachyon.so: $(patsubst src/%.c,$(BUILDDIR)/%.o,$(SRCS))
	$(CC) -shared -o $@ $^ $(LDLIBS)

examples: lib
	@mkdir -p $(BUILDDIR)/examples
	$(CC) $(CFLAGS) examples/hello_ipc.c $(BUILDDIR)/libtachyon.a \
		-o $(BUILDDIR)/examples/hello_ipc $(LDLIBS)

tests: lib
	@mkdir -p $(BUILDDIR)/tests
	for t in $(TESTS); do \
		$(CC) $(CFLAGS) tests/$$t.c $(BUILDDIR)/libtachyon.a \
			-o $(BUILDDIR)/tests/$$t $(LDLIBS) || exit 1; \
	done

bench: lib
	$(CC) $(CFLAGS) benchmarks/bench_ipc.c $(BUILDDIR)/libtachyon.a \
		-o $(BUILDDIR)/bench_ipc $(LDLIBS)

# The tests that matter most: correctness, then crash recovery under SIGKILL.
# test_chaos forks and kills children, so it is not run under sanitizers.
check: tests
	@set -e; for t in $(TESTS); do \
		echo "--- $$t ---"; $(BUILDDIR)/tests/$$t || exit 1; \
	done

# Address + UB sanitizers. Single-process tests only.
asan:
	@mkdir -p $(BUILDDIR)-asan
	$(CC) $(CFLAGS) -g -O1 -fsanitize=address,undefined -fno-omit-frame-pointer \
		$(SRCS) tests/test_basic.c -o $(BUILDDIR)-asan/test_basic $(LDLIBS)
	$(BUILDDIR)-asan/test_basic

# Thread sanitizer: catches data races the eye cannot.
tsan:
	@mkdir -p $(BUILDDIR)-tsan
	$(CC) $(CFLAGS) -g -O1 -fsanitize=thread -fno-omit-frame-pointer \
		$(SRCS) tests/test_stress.c -o $(BUILDDIR)-tsan/test_stress $(LDLIBS)
	$(BUILDDIR)-tsan/test_stress

bench-run: bench
	./scripts/run_benchmarks.sh

chaos: tests
	BUILD_DIR=$(BUILDDIR)/tests ./scripts/chaos_kill.sh 2000

install: lib
	install -d $(PREFIX)/lib $(PREFIX)/include/tachyon
	install -m 644 $(BUILDDIR)/libtachyon.a $(BUILDDIR)/libtachyon.so $(PREFIX)/lib
	install -m 644 include/tachyon/tachyon.h $(PREFIX)/include/tachyon/

clean:
	rm -rf $(BUILDDIR) $(BUILDDIR)-asan $(BUILDDIR)-tsan

help:
	@echo "targets:"
	@echo "  make            build library, examples, tests, benchmarks"
	@echo "  make check      run the full test suite"
	@echo "  make asan       single-process tests under ASan + UBSan"
	@echo "  make tsan       stress test under ThreadSanitizer"
	@echo "  make bench-run  reproduce the README numbers"
	@echo "  make chaos      2000 rounds of SIGKILL, verify zero leaks"
	@echo "  make install    install to PREFIX ($(PREFIX))"
