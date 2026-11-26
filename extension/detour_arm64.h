/*-------------------------------------------------------------------------
 * detour_arm64.h
 *	  Route D patching for AArch64 (Linux). It is plain C with no PostgreSQL
 *	  headers, so a small test program can include it (see
 *	  test/detour_arm64_test.c).
 *
 * The first four instructions (16 bytes) of the target are replaced by
 *
 *		ldr  x16, #8		; 0x58000050
 *		br   x16			; 0xd61f0200
 *		.quad replacement
 *
 * x16 (IP0) is the intra-procedure-call scratch register. The ABI says that it
 * is free at function entry. The four original instructions are copied into a
 * trampoline, followed by a jump back to target + 16, so the replacement can
 * call the original.
 *
 * The jump back is a direct B when the trampoline is within 128 MB of the
 * target (a64_alloc_near tries to place it there). The reason is branch target
 * identification (BTI): when BTI is enforced, an indirect branch may only land
 * on a BTI instruction, and target + 16 is not one. Otherwise, the jump back is
 * the same absolute jump through x16.
 *
 * The jump into the replacement is indirect. It lands on the own BTI/PACIASP
 * of the replacement, if it has one.
 *
 * The copied instructions must not depend on their address, and must not touch
 * x16/x17. ADR/ADRP are common in prologues (the stack-protector canary, the
 * GOT). They are relocated: the trampoline loads the same address from a
 * literal instead. Other PC-relative instructions (B/BL, B.cond, CB(N)Z,
 * TB(N)Z, LDR literal) and returns/branches are refused, and that target falls
 * back to route H.
 *-------------------------------------------------------------------------
 */
#ifndef PGPI_DETOUR_ARM64_H
#define PGPI_DETOUR_ARM64_H

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#define A64_LDR_X16_8	0x58000050u
#define A64_BR_X16		0xd61f0200u
#define A64_PATCH_LEN	16			/* bytes overwritten in the target */
#define A64_SLOT_SIZE	64			/* trampoline slot: 16 code + 16 jump back + 32 literals */
#define A64_LIT_OFF		32			/* literal pool offset in the slot */

static inline bool
a64_is_adr(uint32_t insn)
{
	return (insn & 0x1f000000u) == 0x10000000u;
}

/* Return the address that an ADR/ADRP at pc computes. */
static inline uint64_t
a64_adr_value(uint32_t insn, uint64_t pc)
{
	int64_t		imm = (int64_t) ((((insn >> 5) & 0x7ffff) << 2) | ((insn >> 29) & 3));

	imm = (imm << 43) >> 43;	/* sign-extend 21 bits */
	if (insn & 0x80000000u)		/* ADRP: 4 KB pages */
		return (pc & ~(uint64_t) 0xfff) + (uint64_t) (imm << 12);
	return pc + (uint64_t) imm;
}

/* Return why an instruction cannot be moved into the trampoline, or NULL. */
static inline const char *
a64_unsafe(uint32_t insn)
{
	if (a64_is_adr(insn))
		return ((insn & 0x1f) == 16 || (insn & 0x1f) == 17) ? "ADR/ADRP to x16/x17" : NULL;
	if ((insn & 0x7c000000u) == 0x14000000u)
		return "B/BL";
	if ((insn & 0xfe000000u) == 0x54000000u)
		return "B.cond";
	if ((insn & 0x7e000000u) == 0x34000000u)
		return "CBZ/CBNZ";
	if ((insn & 0x7e000000u) == 0x36000000u)
		return "TBZ/TBNZ";
	if ((insn & 0x3b000000u) == 0x18000000u)
		return "LDR literal";
	if ((insn & 0xfffffc1fu) == 0xd65f0000u || (insn & 0xfffffc1fu) == 0xd61f0000u ||
		(insn & 0xfffffc1fu) == 0xd63f0000u)
		return "RET/BR/BLR";
	/* x16/x17 (our scratch) in a register field. The hint/system space (BTI, PAC*) is excepted. */
	if ((insn & 0xffc00000u) != 0xd5000000u)
	{
		uint32_t	rd = insn & 0x1f,
					rn = (insn >> 5) & 0x1f;

		if (rd == 16 || rd == 17 || rn == 16 || rn == 17)
			return "uses x16/x17";
		if ((insn & 0x3a000000u) == 0x28000000u &&	/* load/store pair: Rt2 */
			(((insn >> 10) & 0x1f) == 16 || ((insn >> 10) & 0x1f) == 17))
			return "uses x16/x17";
		if ((insn & 0x0e000000u) == 0x0a000000u &&	/* data processing (register): Rm */
			(((insn >> 16) & 0x1f) == 16 || ((insn >> 16) & 0x1f) == 17))
			return "uses x16/x17";
	}
	return NULL;
}

static inline void
a64_write_abs_jump(uint32_t *at, const void *dest)
{
	uint64_t	addr = (uint64_t) (uintptr_t) dest;

	at[0] = A64_LDR_X16_8;
	at[1] = A64_BR_X16;
	memcpy(at + 2, &addr, 8);
}

/* Return a page within +-128 MB of anchor (so that a direct B reaches it), or NULL. */
static inline uint8_t *
a64_alloc_near(const void *anchor, long page_size)
{
	uintptr_t	base = (uintptr_t) anchor & ~((uintptr_t) page_size - 1);

	for (uintptr_t delta = 0x100000; delta < 0x7000000; delta += 0x100000)
	{
		for (int dir = -1; dir <= 1; dir += 2)
		{
			uintptr_t	hint = (dir < 0) ? base - delta : base + delta;
			void	   *p;

			if (dir < 0 && delta > base)
				continue;
#ifdef MAP_FIXED_NOREPLACE
			p = mmap((void *) hint, page_size, PROT_READ | PROT_WRITE,
					 MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED_NOREPLACE, -1, 0);
#else
			p = mmap((void *) hint, page_size, PROT_READ | PROT_WRITE,
					 MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
#endif
			if (p == MAP_FAILED)
				continue;
			if ((intptr_t) ((uintptr_t) p - base) > -0x7000000 &&
				(intptr_t) ((uintptr_t) p - base) < 0x7000000)
				return (uint8_t *) p;
			munmap(p, page_size);
		}
	}
	return NULL;
}

/* Return a direct B from 'from' to 'to', or 0 if it is out of range. */
static inline uint32_t
a64_branch(const void *from, const void *to)
{
	intptr_t	d = (intptr_t) to - (intptr_t) from;

	if ((d & 3) || d < -(1 << 27) || d >= (1 << 27))
		return 0;
	return 0x14000000u | ((uint32_t) (d >> 2) & 0x03ffffffu);
}

/*
 * Patch target to jump to replacement. *orig gets the trampoline (in slot,
 * which must be writable and A64_SLOT_SIZE bytes). Return false with a
 * reason in note if the target cannot be patched.
 */
static inline bool
a64_install(void *target, void *replacement, void **orig, uint8_t *slot,
			long page_size, char *note, size_t notesz)
{
	uint32_t   *t = (uint32_t *) target;
	uint32_t   *s = (uint32_t *) slot;
	uintptr_t	pg_start,
				pg_end;

	if ((uintptr_t) t & 3)
	{
		snprintf(note, notesz, "misaligned target");
		return false;
	}
	if (t[0] == A64_LDR_X16_8 && t[1] == A64_BR_X16)
	{
		snprintf(note, notesz, "already patched");
		return false;
	}
	for (int i = 0; i < A64_PATCH_LEN / 4; i++)
	{
		const char *why = a64_unsafe(t[i]);

		if (why)
		{
			snprintf(note, notesz, "unsupported instruction at +%d (%08x, %s)", i * 4, t[i], why);
			return false;
		}
	}

	/* trampoline: the four original instructions (ADR/ADRP as literal loads), then a jump back to target + 16 */
	for (int i = 0; i < A64_PATCH_LEN / 4; i++)
	{
		if (a64_is_adr(t[i]))
		{
			uint64_t	v = a64_adr_value(t[i], (uint64_t) (uintptr_t) (t + i));
			uint32_t	off = A64_LIT_OFF + 8 * i - 4 * i;		/* from s[i] to its literal */

			memcpy(slot + A64_LIT_OFF + 8 * i, &v, 8);
			s[i] = 0x58000000u | ((off / 4) << 5) | (t[i] & 0x1f);	/* ldr xd, literal */
		}
		else
			s[i] = t[i];
	}
	if ((s[4] = a64_branch(s + 4, (uint8_t *) target + A64_PATCH_LEN)) == 0)
		a64_write_abs_jump(s + 4, (uint8_t *) target + A64_PATCH_LEN);
	__builtin___clear_cache((char *) s, (char *) s + A64_SLOT_SIZE);
	*orig = s;

	pg_start = (uintptr_t) t & ~((uintptr_t) page_size - 1);
	pg_end = ((uintptr_t) t + A64_PATCH_LEN + page_size - 1) & ~((uintptr_t) page_size - 1);
	if (mprotect((void *) pg_start, pg_end - pg_start, PROT_READ | PROT_WRITE | PROT_EXEC) != 0)
	{
		snprintf(note, notesz, "mprotect failed");
		*orig = NULL;
		return false;
	}
	a64_write_abs_jump(t, replacement);
	__builtin___clear_cache((char *) t, (char *) t + A64_PATCH_LEN);
	mprotect((void *) pg_start, pg_end - pg_start, PROT_READ | PROT_EXEC);
	snprintf(note, notesz, "patched %d bytes%s", A64_PATCH_LEN,
			 (s[4] & 0xfc000000u) == 0x14000000u ? "" : " (absolute return: not BTI-safe)");
	return true;
}

#endif							/* PGPI_DETOUR_ARM64_H */
