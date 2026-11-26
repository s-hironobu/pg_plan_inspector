/*-------------------------------------------------------------------------
 * detour.c
 *	  Route D: run-time detours of exported size-estimation functions.
 *
 * At load time, we overwrite the first instructions of each target function
 * in the postgres executable with a jump to our replacement. The overwritten
 * instructions are copied into a trampoline. The trampoline continues at the
 * rest of the original function, so the replacement can still call the
 * original.
 *
 * The architecture is chosen at build time (Makefile: PGPI_ARCH):
 *
 *	 x86_64  a 5-byte relative jump to a relay stub near the target. The
 *			 prologue must consist of instructions that do not depend on
 *			 their address (endbr64, push, register moves, ...)
 *	 arm64   (Linux) a 16-byte absolute jump through x16; see detour_arm64.h
 *	 none	 route D is disabled and only route H is used (e.g. macOS, where
 *			 signed code pages cannot be modified)
 *
 * A function that cannot be patched is refused, and that target falls back to
 * route H.
 *
 * When pgpi is loaded through shared_preload_libraries, the patch is applied
 * once in the postmaster. Every forked backend and parallel worker inherits
 * it. PostgreSQL backends are single-threaded, so patching is race-free.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stdint.h>
#include <sys/mman.h>
#include <unistd.h>

#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"

#include "pgpi.h"

/* Architecture of route D: from the Makefile, or from the compiler if built in another way. */
#if !defined(PGPI_DETOUR_X86_64) && !defined(PGPI_DETOUR_ARM64) && !defined(PGPI_DETOUR_NONE)
#if defined(__x86_64__)
#define PGPI_DETOUR_X86_64
#elif defined(__aarch64__) && defined(__linux__)
#define PGPI_DETOUR_ARM64
#else
#define PGPI_DETOUR_NONE
#endif
#endif
#if defined(PGPI_DETOUR_X86_64) && !defined(__x86_64__)
#error "PGPI_ARCH=x86_64 but the compiler does not target x86-64"
#endif
#if defined(PGPI_DETOUR_ARM64) && !(defined(__aarch64__) && defined(__linux__))
#error "PGPI_ARCH=arm64 needs an AArch64 Linux target (on macOS use PGPI_ARCH=none)"
#endif
#if defined(PGPI_DETOUR_ARM64)
#include "detour_arm64.h"
#endif

/* The originals, reachable through the trampolines. */
static void (*orig_set_baserel_size_estimates) (PlannerInfo *, RelOptInfo *);
static double (*orig_get_parameterized_baserel_size) (PlannerInfo *, RelOptInfo *, List *);
static double (*orig_get_parameterized_joinrel_size) (PlannerInfo *, RelOptInfo *,
													  Path *, Path *,
													  SpecialJoinInfo *, List *);
static void (*orig_set_joinrel_size_estimates) (PlannerInfo *, RelOptInfo *,
												RelOptInfo *, RelOptInfo *,
												SpecialJoinInfo *, List *);
static Selectivity (*orig_clauselist_selectivity_ext) (PlannerInfo *, List *, int,
													   JoinType, SpecialJoinInfo *,
													   bool);

enum
{
	D_BASE, D_PBASE, D_PJOIN, D_JOIN, D_CLAUSELIST, D_COUNT
};

static PgpiDetour detours[D_COUNT];
static bool detours_initialized = false;

/* Re-entrancy guards. They are reset on abort through pgpi_planning_reset(). */
static int	suppress_clauselist = 0;
static int	clauselist_depth = 0;

void
pgpi_detour_reset_state(void)
{
	suppress_clauselist = 0;
	clauselist_depth = 0;
}

int
pgpi_detour_count(void)
{
	return detours_initialized ? D_COUNT : 0;
}

PgpiDetour *
pgpi_detour_get(int i)
{
	return &detours[i];
}

bool
pgpi_detour_active(const char *name)
{
	if (!pgpi_enable_detour || !detours_initialized)
		return false;
	for (int i = 0; i < D_COUNT; i++)
		if (strcmp(detours[i].name, name) == 0)
			return detours[i].installed;
	return false;
}

static bool
detour_on(void)
{
	return pgpi_enable_detour && pgpi_mode != PGPI_MODE_OFF;
}

/* ---------- replacements ---------- */

static void
pgpi_set_baserel_size_estimates(PlannerInfo *root, RelOptInfo *rel)
{
	detours[D_BASE].hits++;
	suppress_clauselist++;
	orig_set_baserel_size_estimates(root, rel);
	suppress_clauselist--;

	if (detour_on() && pgpi_is_plain_baserel(root, rel))
	{
		PgpiRequest req = {0};

		req.point = PGPI_PT_BASE;
		req.route = 'D';
		req.root = root;
		req.relids = rel->relids;
		req.clauses = rel->baserestrictinfo;
		req.jointype = JOIN_INNER;
		req.pg_value = rel->rows;
		req.tuples = rel->tuples;
		rel->rows = pgpi_intervene(&req);
	}
}

static Relids
clause_relids_of(PlannerInfo *root, List *clauses)
{
	Relids		result = NULL;
	ListCell   *lc;

	foreach(lc, clauses)
	{
		Node	   *n = (Node *) lfirst(lc);

		if (IsA(n, RestrictInfo))
			result = bms_add_members(result, ((RestrictInfo *) n)->clause_relids);
		else
			result = bms_add_members(result, pull_varnos(root, n));
	}
	return result;
}

static double
pgpi_get_parameterized_baserel_size(PlannerInfo *root, RelOptInfo *rel,
									List *param_clauses)
{
	double		rows;

	detours[D_PBASE].hits++;
	suppress_clauselist++;
	rows = orig_get_parameterized_baserel_size(root, rel, param_clauses);
	suppress_clauselist--;

	if (detour_on() && pgpi_is_plain_baserel(root, rel))
	{
		PgpiRequest req = {0};

		req.point = PGPI_PT_PARAM_BASE;
		req.route = 'D';
		req.root = root;
		req.relids = rel->relids;
		req.outer_relids = bms_del_members(clause_relids_of(root, param_clauses),
										   rel->relids);
		req.clauses = list_concat_unique_ptr(list_copy(rel->baserestrictinfo),
											 param_clauses);
		req.jointype = JOIN_INNER;
		req.pg_value = rows;
		req.tuples = rel->tuples;
		rows = pgpi_intervene(&req);
	}
	return rows;
}

static double
pgpi_get_parameterized_joinrel_size(PlannerInfo *root, RelOptInfo *rel,
									Path *outer_path, Path *inner_path,
									SpecialJoinInfo *sjinfo,
									List *restrict_clauses)
{
	double		rows;

	detours[D_PJOIN].hits++;
	suppress_clauselist++;
	rows = orig_get_parameterized_joinrel_size(root, rel, outer_path, inner_path,
											   sjinfo, restrict_clauses);
	suppress_clauselist--;

	if (detour_on())
	{
		PgpiRequest req = {0};
		List	   *clauses;

		clauses = list_concat_unique_ptr(list_copy(pgpi_all_clauses(root, rel)),
										 restrict_clauses);
		if (outer_path->param_info)
			clauses = list_concat_unique_ptr(clauses, outer_path->param_info->ppi_clauses);
		if (inner_path->param_info)
			clauses = list_concat_unique_ptr(clauses, inner_path->param_info->ppi_clauses);

		req.point = PGPI_PT_PARAM_JOIN;
		req.route = 'D';
		req.root = root;
		req.relids = rel->relids;
		req.outer_relids = bms_del_members(bms_union(PATH_REQ_OUTER(outer_path),
													 PATH_REQ_OUTER(inner_path)),
										   rel->relids);
		req.clauses = clauses;
		req.jointype = sjinfo ? sjinfo->jointype : JOIN_INNER;
		req.pg_value = rows;
		rows = pgpi_intervene(&req);
	}
	return rows;
}

static void
pgpi_set_joinrel_size_estimates(PlannerInfo *root, RelOptInfo *rel,
								RelOptInfo *outer_rel, RelOptInfo *inner_rel,
								SpecialJoinInfo *sjinfo, List *restrictlist)
{
	detours[D_JOIN].hits++;
	suppress_clauselist++;
	orig_set_joinrel_size_estimates(root, rel, outer_rel, inner_rel, sjinfo,
									restrictlist);
	suppress_clauselist--;

	pgpi_record_joinrel(root, rel, outer_rel, inner_rel, restrictlist);
	if (detour_on())
	{
		PgpiRequest req = {0};

		req.point = PGPI_PT_JOIN;
		req.route = 'D';
		req.root = root;
		req.relids = rel->relids;
		req.split = outer_rel->relids;
		req.outer_rows = outer_rel->rows;
		req.inner_rows = inner_rel->rows;
		req.clauses = pgpi_all_clauses(root, rel);
		req.jointype = sjinfo ? sjinfo->jointype : JOIN_INNER;
		req.pg_value = rel->rows;
		rel->rows = pgpi_intervene(&req);
	}
}

/*
 * Selectivity of a clause list on one base relation, possibly parameterized
 * by other relations. This call is used for index-path costing, so it is
 * what makes a corrected inner-side row count change the plan. Calls made
 * from inside the size functions above are passed through, because their
 * result is corrected at the size level. Nested calls are passed through too.
 */
static Selectivity
pgpi_clauselist_selectivity_ext(PlannerInfo *root, List *clauses, int varRelid,
								JoinType jointype, SpecialJoinInfo *sjinfo,
								bool use_extended_stats)
{
	Selectivity s;
	Relids		all;
	Relids		base = NULL;
	int			target;
	int			rti = -1;
	RelOptInfo *rel;
	PgpiRequest req = {0};

	detours[D_CLAUSELIST].hits++;
	if (clauselist_depth > 0 || suppress_clauselist > 0 || !detour_on() ||
		!pgpi_detour_clauselist || root == NULL || root->simple_rel_array == NULL ||
		clauses == NIL || jointype != JOIN_INNER || sjinfo != NULL)
		goto passthrough;

	all = clause_relids_of(root, clauses);
	while ((rti = bms_next_member(all, rti)) >= 0)
	{
		RangeTblEntry *rte = (rti < root->simple_rel_array_size) ?
			root->simple_rte_array[rti] : NULL;

		if (rte && rte->rtekind != RTE_JOIN)
			base = bms_add_member(base, rti);
	}
	if (varRelid > 0)
		target = varRelid;
	else if (bms_membership(base) == BMS_SINGLETON)
		target = bms_singleton_member(base);
	else
		goto passthrough;
	if (target >= root->simple_rel_array_size ||
		(rel = root->simple_rel_array[target]) == NULL ||
		!pgpi_is_plain_baserel(root, rel))
		goto passthrough;

	clauselist_depth++;
	s = orig_clauselist_selectivity_ext(root, clauses, varRelid, jointype, sjinfo,
										use_extended_stats);
	clauselist_depth--;

	req.point = PGPI_PT_CLAUSELIST;
	req.route = 'D';
	req.root = root;
	req.relids = bms_make_singleton(target);
	req.outer_relids = bms_del_member(base, target);
	req.clauses = clauses;
	req.jointype = JOIN_INNER;
	req.pg_value = s;
	req.tuples = rel->tuples;
	return (Selectivity) pgpi_intervene(&req);

passthrough:
	clauselist_depth++;
	s = orig_clauselist_selectivity_ext(root, clauses, varRelid, jointype, sjinfo,
										use_extended_stats);
	clauselist_depth--;
	return s;
}

/* ---------- x86-64 patching ---------- */

#if defined(PGPI_DETOUR_X86_64)

#define SLOT_SIZE 64

static uint8 *tramp_page = NULL;
static long page_size = 0;

/* Length of a ModRM(+SIB+disp) operand, or -1 for RIP-relative. */
static int
modrm_len(const uint8 *q)
{
	int			mod = q[0] >> 6;
	int			rm = q[0] & 7;
	int			len = 1;

	if (mod == 3)
		return 1;
	if (rm == 4)
	{
		len++;
		if (mod == 0 && (q[1] & 7) == 5)
			len += 4;
	}
	else if (mod == 0 && rm == 5)
		return -1;				/* RIP-relative: would need relocation */
	if (mod == 1)
		len += 1;
	else if (mod == 2)
		len += 4;
	return len;
}

/*
 * Length of one position-independent instruction, or -1 if it is not in the
 * small set that we can relocate unchanged.
 */
static int
insn_len(const uint8 *p)
{
	int			i = 0;
	uint8		op;
	int			m;

	if (p[0] == 0xF3 && p[1] == 0x0F && p[2] == 0x1E && p[3] == 0xFA)
		return 4;				/* endbr64 */
	if ((p[0] & 0xF0) == 0x40)
		i = 1;					/* REX prefix */
	op = p[i];
	if (op >= 0x50 && op <= 0x57)
		return i + 1;			/* push r64 */
	switch (op)
	{
		case 0x01:				/* add r/m, r */
		case 0x03:				/* add r, r/m */
		case 0x29:				/* sub r/m, r */
		case 0x2B:				/* sub r, r/m */
		case 0x31:				/* xor r/m, r */
		case 0x33:				/* xor r, r/m */
		case 0x85:				/* test r/m, r */
		case 0x89:				/* mov r/m, r */
		case 0x8B:				/* mov r, r/m */
			m = modrm_len(p + i + 1);
			return m < 0 ? -1 : i + 1 + m;
		case 0x83:				/* grp1 r/m, imm8 (e.g. sub rsp, imm8) */
			m = modrm_len(p + i + 1);
			return m < 0 ? -1 : i + 1 + m + 1;
		case 0x81:				/* grp1 r/m, imm32 */
			m = modrm_len(p + i + 1);
			return m < 0 ? -1 : i + 1 + m + 4;
		default:
			return -1;
	}
}

static uint8 *
alloc_near(uint8 *anchor)
{
	uintptr_t	base = (uintptr_t) anchor & ~((uintptr_t) page_size - 1);

	for (uintptr_t delta = 0x100000; delta < 0x70000000; delta += 0x100000)
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
			if ((intptr_t) ((uintptr_t) p - (uintptr_t) anchor) > INT32_MIN / 2 &&
				(intptr_t) ((uintptr_t) p - (uintptr_t) anchor) < INT32_MAX / 2)
				return (uint8 *) p;
			munmap(p, page_size);
		}
	}
	return NULL;
}

static void
write_abs_jump(uint8 *at, const void *dest)
{
	uint64		addr = (uint64) (uintptr_t) dest;

	at[0] = 0xFF;				/* jmp qword ptr [rip+0] */
	at[1] = 0x25;
	memset(at + 2, 0, 4);
	memcpy(at + 6, &addr, 8);
}

static bool
install_one(PgpiDetour *d, int slot)
{
	uint8	   *t = (uint8 *) d->target;
	uint8	   *s;
	int			n = 0;
	int64		rel;
	int32		rel32;
	uintptr_t	pg_start,
				pg_end;

	if (t[0] == 0xE9)
	{
		snprintf(d->note, sizeof(d->note), "already patched");
		return false;
	}
	while (n < 5)
	{
		int			l = insn_len(t + n);

		if (l <= 0)
		{
			snprintf(d->note, sizeof(d->note),
					 "unsupported instruction at +%d (%02x %02x %02x %02x)",
					 n, t[n], t[n + 1], t[n + 2], t[n + 3]);
			return false;
		}
		n += l;
	}
	if (n > 15)
	{
		snprintf(d->note, sizeof(d->note), "prologue too long (%d bytes)", n);
		return false;
	}

	s = tramp_page + slot * SLOT_SIZE;
	rel = (int64) ((intptr_t) s - (intptr_t) (t + 5));
	if (rel < INT32_MIN || rel > INT32_MAX)
	{
		snprintf(d->note, sizeof(d->note), "trampoline out of rel32 range");
		return false;
	}

	/* relay: jump to the replacement */
	write_abs_jump(s, d->replacement);
	/* trampoline: copied prologue, then jump back into the original */
	memcpy(s + 16, t, n);
	write_abs_jump(s + 16 + n, t + n);
	*d->orig = s + 16;

	/* patch the target: jmp rel32 to the relay, pad with NOPs */
	pg_start = (uintptr_t) t & ~((uintptr_t) page_size - 1);
	pg_end = ((uintptr_t) t + n + page_size - 1) & ~((uintptr_t) page_size - 1);
	if (mprotect((void *) pg_start, pg_end - pg_start,
				 PROT_READ | PROT_WRITE | PROT_EXEC) != 0)
	{
		snprintf(d->note, sizeof(d->note), "mprotect failed: %m");
		*d->orig = NULL;
		return false;
	}
	rel32 = (int32) rel;
	t[0] = 0xE9;
	memcpy(t + 1, &rel32, 4);
	for (int i = 5; i < n; i++)
		t[i] = 0x90;
	mprotect((void *) pg_start, pg_end - pg_start, PROT_READ | PROT_EXEC);

	d->installed = true;
	snprintf(d->note, sizeof(d->note), "patched %d bytes", n);
	return true;
}

#endif							/* PGPI_DETOUR_X86_64 */

#define DETOUR(idx, fname, repl, origp) \
	do { \
		detours[idx].name = #fname; \
		detours[idx].target = (void *) &fname; \
		detours[idx].replacement = (void *) &repl; \
		detours[idx].orig = (void **) &origp; \
		detours[idx].installed = false; \
		detours[idx].hits = 0; \
		detours[idx].note[0] = '\0'; \
	} while (0)

void
pgpi_install_detours(void)
{
	if (detours_initialized)
		return;

	DETOUR(D_BASE, set_baserel_size_estimates, pgpi_set_baserel_size_estimates,
		   orig_set_baserel_size_estimates);
	DETOUR(D_PBASE, get_parameterized_baserel_size, pgpi_get_parameterized_baserel_size,
		   orig_get_parameterized_baserel_size);
	DETOUR(D_PJOIN, get_parameterized_joinrel_size, pgpi_get_parameterized_joinrel_size,
		   orig_get_parameterized_joinrel_size);
	DETOUR(D_JOIN, set_joinrel_size_estimates, pgpi_set_joinrel_size_estimates,
		   orig_set_joinrel_size_estimates);
	DETOUR(D_CLAUSELIST, clauselist_selectivity_ext, pgpi_clauselist_selectivity_ext,
		   orig_clauselist_selectivity_ext);
	detours_initialized = true;

#if defined(PGPI_DETOUR_X86_64)
	page_size = sysconf(_SC_PAGESIZE);
	tramp_page = alloc_near((uint8 *) detours[0].target);
	if (tramp_page == NULL)
	{
		for (int i = 0; i < D_COUNT; i++)
			snprintf(detours[i].note, sizeof(detours[i].note),
					 "no trampoline memory within rel32 range");
		return;
	}
	for (int i = 0; i < D_COUNT; i++)
		install_one(&detours[i], i);
	mprotect(tramp_page, page_size, PROT_READ | PROT_EXEC);
#elif defined(PGPI_DETOUR_ARM64)
	{
		long		psz = sysconf(_SC_PAGESIZE);
		uint8	   *page;

		StaticAssertStmt(D_COUNT * A64_SLOT_SIZE <= 4096, "trampoline slots exceed a page");
		page = a64_alloc_near(detours[0].target, psz);
		if (page == NULL)
			page = mmap(NULL, psz, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
		if (page == MAP_FAILED)
		{
			for (int i = 0; i < D_COUNT; i++)
				snprintf(detours[i].note, sizeof(detours[i].note), "no trampoline memory");
			return;
		}
		for (int i = 0; i < D_COUNT; i++)
			detours[i].installed = a64_install(detours[i].target, detours[i].replacement,
											   detours[i].orig, page + i * A64_SLOT_SIZE, psz,
											   detours[i].note, sizeof(detours[i].note));
		mprotect(page, psz, PROT_READ | PROT_EXEC);
	}
#else
	for (int i = 0; i < D_COUNT; i++)
		snprintf(detours[i].note, sizeof(detours[i].note),
				 "route D not built (PGPI_ARCH=none)");
#endif
}
