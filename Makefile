# FreaKV Official Production Makefile
#
# Targets:
#   make                  — build freakv
#   make clean            — remove build artefacts

CC      := gcc
CFLAGS  := -std=c11 -O3 -Wall -Wextra -Wpedantic \
            -Wno-unused-parameter \
            -D_GNU_SOURCE \
            -march=native \
            -fwrapv \
            -I. -Icore -Ihashtable -Imem -Inet -Iprotocol

LDFLAGS := -lpthread

TARGET   := freakv

# ── Sources ────────────────────────────────────────────────────────────

LIB_SRCS := \
    core/shard.c \
    core/snapshot.c \
    hashtable/htable.c \
    hashtable/hash.c \
    mem/mem_api.c \
    mem/mem_arena.c \
    mem/mem_heap.c \
    mem/mem_linear.c \
    mem/mem_pool.c \
    mem/mem_barrier.c \
    memory/slab.c \
    net/reactor.c

OBJDIR   := .build
LIB_OBJS := $(patsubst %.c,$(OBJDIR)/%.o,$(LIB_SRCS))
MAIN_OBJ := $(OBJDIR)/server.o
ALL_OBJS := $(LIB_OBJS) $(MAIN_OBJ)

DEPS     := $(ALL_OBJS:.o=.d)

# ── Rules ──────────────────────────────────────────────────────────────

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(ALL_OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

$(OBJDIR)/%.o: %.c
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -MMD -MP -c -o $@ $<

-include $(DEPS)

clean:
	rm -rf .build $(TARGET)
