/*-------------------------------------------------------------------------
 * qpam.h
 *
 * Copyright (c) 2021-2025, Hironobu Suzuki @ interdb.jp
 *
 *-------------------------------------------------------------------------
 */
#ifndef __QPAM_H__
#define __QPAM_H__

#include "common.h"

extern void set_query(struct queryPlanData *qpd, const int nested_level,
					  const uint64 queryId, const bool nested);
extern void set_plan(struct queryPlanData *qpd, const int nested_level,
					 const bool nested, const PrintFormat format);
extern char *get_query_plan(struct queryPlanData *qpd, const PrintFormat format,
							bool *is_null, const int nested_level);
extern void init_qpd(struct queryPlanData *qpd);
extern void init_qi(struct queryInfo *qi);

#endif							/* __QPAM_H__ */
