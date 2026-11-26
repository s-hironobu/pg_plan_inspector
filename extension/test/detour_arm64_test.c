/*
 * Minimal test of the AArch64 route D patching (detour_arm64.h), without
 * PostgreSQL.  Cross-build and run under qemu, for example:
 *
 *   gcc -O2 -fno-omit-frame-pointer -I.. detour_arm64_test.c -o t
 *
 * A target with a normal prologue is patched; calling it runs the
 * replacement, which calls the original through the trampoline.  A target
 * whose first instructions return (too short) must be refused.
 */
#include "detour_arm64.h"

#include <stdlib.h>

static volatile long sink;

__attribute__((noinline)) static long
helper(long x)
{
	sink += x;
	return x + 1;
}

/* a function with a frame: stp x29, x30 / mov x29, sp ... */
__attribute__((noinline)) long target_fn(long a, long b);
__attribute__((noinline)) long
target_fn(long a, long b)
{
	long		v[4];

	for (int i = 0; i < 4; i++)
		v[i] = helper(a + i) * b;
	return v[0] + v[1] + v[2] + v[3];
}

/* a leaf that returns at once: add x0, x0, x1; ret */
__attribute__((noinline)) long tiny_fn(long a, long b);
__attribute__((noinline)) long
tiny_fn(long a, long b)
{
	return a + b;
}

static long (*orig_target) (long, long);

__attribute__((noinline)) static long
repl(long a, long b)
{
	return orig_target(a, b) * 10 + 1;
}

int
main(void)
{
	long		psz = sysconf(_SC_PAGESIZE);
	uint8_t    *page = a64_alloc_near((void *) &target_fn, psz);
	char		note[128];
	long		expect = target_fn(3, 2);
	void	   *dummy = NULL;
	int			fails = 0;

	if (page == NULL)
	{
		printf("no trampoline page near the target\n");
		return 1;
	}
	if (!a64_install((void *) &target_fn, (void *) &repl, (void **) &orig_target, page, psz, note, sizeof(note)))
	{
		printf("FAIL: target_fn not patched: %s\n", note);
		return 1;
	}
	mprotect(page, psz, PROT_READ | PROT_EXEC);
	printf("target_fn: %s\n", note);
	if (target_fn(3, 2) != expect * 10 + 1)
	{
		printf("FAIL: patched call gave %ld, expected %ld\n", target_fn(3, 2), expect * 10 + 1);
		fails++;
	}
	if (orig_target(5, 7) != 7 * ((5 + 1) + (6 + 1) + (7 + 1) + (8 + 1)))
	{
		printf("FAIL: original through the trampoline gave %ld\n", orig_target(5, 7));
		fails++;
	}
	if (a64_install((void *) &target_fn, (void *) &repl, &dummy, page + 64, psz, note, sizeof(note)))
		fails++, printf("FAIL: patched twice\n");
	else
		printf("target_fn again: %s\n", note);
	if (a64_install((void *) &tiny_fn, (void *) &repl, &dummy, page + 64, psz, note, sizeof(note)))
		fails++, printf("FAIL: tiny_fn should be refused\n");
	else
		printf("tiny_fn: %s\n", note);
	printf(fails ? "FAILED\n" : "OK\n");
	return fails ? 1 : 0;
}
