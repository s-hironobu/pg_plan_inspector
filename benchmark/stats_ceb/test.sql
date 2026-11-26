-- STATS-CEB subset for pgpi (benchmark/stats_ceb).  Queries from STATS-CEB, Han et al.,
-- "Cardinality Estimation in DBMS: A Comprehensive Benchmark Evaluation", PVLDB 15, 2022
-- (https://github.com/ZhengtongYan/End-to-End-CardEst-Benchmark, workloads/stats_CEB/stats_CEB.sql).
-- Test: the STATS-CEB queries that take at most 5 s with PostgreSQL alone, at most 2 per join graph.
--   A (57): join graphs seen in training (train.sql has 10 variants of each query)
--   B (32): join graphs held out (no training query has their tables and joins)
-- '-- A q12' names the next query: group, and its number in stats_CEB.sql.

-- A q43
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE u.Id = p.OwnerUserId AND p.OwnerUserId = ph.UserId AND ph.UserId = b.UserId AND ph.PostHistoryTypeId=3 AND p.Score>=-7 AND u.Reputation>=1 AND u.UpVotes>=0 AND u.UpVotes<=117;

-- A q44
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE u.Id = p.OwnerUserId AND p.OwnerUserId = ph.UserId AND ph.UserId = b.UserId AND ph.CreationDate>='2010-09-06 11:41:43'::timestamp AND ph.CreationDate<='2014-09-03 16:41:18'::timestamp AND p.Score>=-1 AND p.ViewCount>=0 AND p.ViewCount<=39097 AND p.AnswerCount>=0 AND p.CommentCount>=0 AND p.CommentCount<=11 AND p.FavoriteCount<=10 AND p.CreationDate>='2010-08-13 02:18:09'::timestamp AND p.CreationDate<='2014-09-09 10:20:27'::timestamp AND u.Views>=0 AND u.DownVotes>=0 AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.UpVotes<=37;

-- A q145
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND p.Id = pl.PostId AND p.Id = ph.PostId AND p.PostTypeId=1 AND p.AnswerCount>=0 AND p.CreationDate>='2010-07-21 15:23:53'::timestamp AND p.CreationDate<='2014-09-11 23:26:14'::timestamp AND pl.CreationDate>='2010-11-16 01:27:37'::timestamp AND pl.CreationDate<='2014-08-21 15:25:23'::timestamp AND ph.PostHistoryTypeId=5 AND v.CreationDate>='2010-07-21 00:00:00'::timestamp AND u.UpVotes>=0 AND u.CreationDate<='2014-09-11 20:31:48'::timestamp;

-- A q146
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND p.Id = pl.PostId AND p.Id = ph.PostId AND c.CreationDate>='2010-07-26 19:37:03'::timestamp AND p.Score>=-2 AND p.CommentCount<=18 AND p.CreationDate>='2010-07-21 13:50:08'::timestamp AND p.CreationDate<='2014-09-11 00:53:10'::timestamp AND pl.CreationDate<='2014-08-05 18:27:51'::timestamp AND ph.CreationDate>='2010-11-27 03:38:45'::timestamp AND u.DownVotes>=0 AND u.UpVotes>=0;

-- A q26
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, users as u WHERE p.Id = c.PostId AND p.Id = pl.RelatedPostId AND p.OwnerUserId = u.Id AND c.CreationDate>='2010-07-21 11:05:37'::timestamp AND c.CreationDate<='2014-08-25 17:59:25'::timestamp AND u.UpVotes>=0 AND u.CreationDate>='2010-08-21 21:27:38'::timestamp;

-- A q20
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u WHERE ph.PostId = p.Id AND p.OwnerUserId = u.Id AND ph.CreationDate<='2014-08-17 21:24:11'::timestamp AND p.CreationDate>='2010-07-26 19:26:37'::timestamp AND p.CreationDate<='2014-08-22 14:43:39'::timestamp AND u.Reputation>=1 AND u.Reputation<=6524 AND u.Views>=0;

-- A q92
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u WHERE ph.PostId = p.Id AND p.OwnerUserId = u.Id AND p.CreationDate>='2010-08-17 19:08:05'::timestamp AND p.CreationDate<='2014-08-31 06:58:12'::timestamp AND u.UpVotes>=0 AND u.UpVotes<=9;

-- A q119
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE b.UserId = u.Id AND p.OwnerUserId = u.Id AND ph.UserId = u.Id AND ph.PostHistoryTypeId=5 AND p.ViewCount>=0 AND p.ViewCount<=2024 AND u.Reputation>=1 AND u.Reputation<=750 AND b.Date>='2010-07-20 10:34:10'::timestamp;

-- A q121
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE b.UserId = u.Id AND p.OwnerUserId = u.Id AND ph.UserId = u.Id AND ph.PostHistoryTypeId=2 AND ph.CreationDate>='2011-01-08 03:03:48'::timestamp AND ph.CreationDate<='2014-08-25 14:04:43'::timestamp AND p.AnswerCount<=4 AND p.CommentCount>=0 AND p.CommentCount<=12 AND p.FavoriteCount>=0 AND p.FavoriteCount<=89 AND p.CreationDate<='2014-09-02 10:21:04'::timestamp AND u.Reputation<=705 AND u.CreationDate>='2010-07-28 23:56:00'::timestamp AND u.CreationDate<='2014-09-02 10:04:41'::timestamp AND b.Date>='2010-07-20 20:47:27'::timestamp AND b.Date<='2014-09-09 13:24:28'::timestamp;

-- A q82
SELECT COUNT(*) FROM comments as c, postHistory as ph, users as u WHERE c.UserId = u.Id AND ph.UserId = u.Id AND c.CreationDate>='2010-08-09 07:24:50'::timestamp AND c.CreationDate<='2014-09-10 03:46:02'::timestamp AND u.Reputation>=1 AND u.Views<=80 AND u.UpVotes>=0 AND u.CreationDate>='2010-08-02 20:31:12'::timestamp;

-- A q83
SELECT COUNT(*) FROM comments as c, postHistory as ph, users as u WHERE c.UserId = u.Id AND ph.UserId = u.Id AND u.Reputation>=1 AND u.Reputation<=7931 AND u.Views<=109 AND u.DownVotes>=0 AND u.CreationDate<='2014-09-12 13:12:56'::timestamp;

-- A q133
SELECT COUNT(*) FROM comments as c, postHistory as ph, badges as b, votes as v, users as u WHERE ph.UserId = u.Id AND v.UserId = u.Id AND c.UserId = u.Id AND b.UserId = u.Id AND b.Date>='2010-09-26 12:17:14'::timestamp AND v.BountyAmount>=0 AND v.CreationDate>='2010-07-20 00:00:00'::timestamp AND v.CreationDate<='2014-09-11 00:00:00'::timestamp AND u.DownVotes>=0 AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.UpVotes<=31 AND u.CreationDate<='2014-08-06 20:38:52'::timestamp;

-- A q37
SELECT COUNT(*) FROM comments as c, postLinks as pl, postHistory as ph, votes as v WHERE pl.PostId = c.PostId AND c.PostId = ph.PostId AND ph.PostId = v.PostId AND ph.CreationDate>='2011-05-07 21:47:19'::timestamp AND ph.CreationDate<='2014-09-10 13:19:54'::timestamp;

-- A q22
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.UserId = p.OwnerUserId AND p.OwnerUserId = u.Id AND p.CommentCount>=0 AND p.CommentCount<=12 AND u.CreationDate>='2010-07-22 04:38:06'::timestamp AND u.CreationDate<='2014-09-08 15:55:02'::timestamp;

-- A q5
SELECT COUNT(*) FROM comments as c, votes as v WHERE c.UserId = v.UserId AND c.Score=0;

-- A q32
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE u.Id = v.UserId AND v.UserId = ph.UserId AND ph.UserId =c.UserId AND c.CreationDate>='2010-08-12 20:33:46'::timestamp AND c.CreationDate<='2014-09-13 19:26:55'::timestamp AND ph.CreationDate>='2011-04-11 14:46:09'::timestamp AND ph.CreationDate<='2014-08-17 16:37:23'::timestamp AND v.CreationDate>='2010-07-26 00:00:00'::timestamp AND v.CreationDate<='2014-09-12 00:00:00'::timestamp AND u.Views>=0 AND u.Views<=783 AND u.DownVotes>=0 AND u.DownVotes<=1 AND u.UpVotes<=123;

-- A q33
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE u.Id = v.UserId AND v.UserId = ph.UserId AND ph.UserId =c.UserId AND v.BountyAmount>=0 AND v.CreationDate>='2010-07-26 00:00:00'::timestamp AND v.CreationDate<='2014-09-08 00:00:00'::timestamp AND u.Reputation>=1 AND u.Views>=0 AND u.Views<=110 AND u.UpVotes=0 AND u.CreationDate>='2010-07-28 19:29:11'::timestamp AND u.CreationDate<='2014-08-14 05:29:30'::timestamp;

-- A q93
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.UserId = u.Id AND p.OwnerUserId = u.Id AND p.PostTypeId=2 AND p.CreationDate<='2014-08-26 22:40:26'::timestamp AND u.Views>=0;

-- A q94
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.UserId = u.Id AND p.OwnerUserId = u.Id AND p.CommentCount>=0 AND u.CreationDate>='2010-12-15 06:00:24'::timestamp;

-- A q69
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, badges as b WHERE p.Id = c.PostId AND p.Id = pl.RelatedPostId AND p.Id = ph.PostId AND p.Id = v.PostId AND b.UserId = c.UserId AND c.Score=0 AND p.Score<=32 AND p.ViewCount<=4146 AND pl.LinkTypeId=1 AND v.CreationDate<='2014-09-10 00:00:00'::timestamp;

-- A q71
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, badges as b WHERE p.Id = c.PostId AND p.Id = pl.RelatedPostId AND p.Id = ph.PostId AND p.Id = v.PostId AND b.UserId = c.UserId AND p.CommentCount>=0 AND ph.PostHistoryTypeId=2 AND v.VoteTypeId=5;

-- A q31
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE u.Id = v.UserId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND v.CreationDate<='2014-09-06 00:00:00'::timestamp AND p.Score<=48 AND p.AnswerCount<=8 AND b.Date>='2011-01-03 20:50:19'::timestamp AND b.Date<='2014-09-02 15:35:07'::timestamp AND u.CreationDate>='2010-11-16 06:03:04'::timestamp;

-- A q103
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE u.Id = b.UserId AND u.Id = p.OwnerUserId AND u.Id = v.UserId AND p.PostTypeId=1 AND p.CommentCount>=0 AND p.CommentCount<=15 AND u.Reputation>=1 AND u.DownVotes>=0 AND u.DownVotes<=1;

-- A q3
SELECT COUNT(*) FROM comments as c, postHistory as ph WHERE c.UserId = ph.UserId AND c.Score=0 AND ph.PostHistoryTypeId=1;

-- A q4
SELECT COUNT(*) FROM comments as c, postHistory as ph WHERE c.UserId = ph.UserId AND ph.PostHistoryTypeId=1 AND ph.CreationDate>='2010-09-14 11:59:07'::timestamp;

-- A q100
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND p.Score>=0 AND p.Score<=16 AND p.ViewCount>=0 AND p.CreationDate<='2014-09-09 12:00:50'::timestamp AND u.Reputation>=1 AND u.CreationDate>='2010-07-19 19:08:49'::timestamp AND u.CreationDate<='2014-08-28 12:15:56'::timestamp;

-- A q101
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND c.Score=0 AND c.CreationDate<='2014-09-10 02:47:53'::timestamp AND p.Score>=0 AND p.Score<=19 AND p.CommentCount<=10 AND p.CreationDate<='2014-08-28 13:31:33'::timestamp AND v.CreationDate<='2014-09-12 00:00:00'::timestamp AND u.DownVotes>=0;

-- A q56
SELECT COUNT(*) FROM tags as t, posts as p, users as u, votes as v, badges as b WHERE p.Id = t.ExcerptPostId AND u.Id = v.UserId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND u.DownVotes>=0;

-- A q57
SELECT COUNT(*) FROM tags as t, posts as p, users as u, votes as v, badges as b WHERE p.Id = t.ExcerptPostId AND u.Id = v.UserId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND u.Views>=0 AND u.Views<=515 AND u.UpVotes>=0 AND u.CreationDate<='2014-09-07 13:46:41'::timestamp AND v.BountyAmount>=0 AND v.BountyAmount<=200 AND b.Date<='2014-09-12 12:56:22'::timestamp;

-- A q6
SELECT COUNT(*) FROM badges as b, posts as p WHERE b.UserId = p.OwnerUserId AND b.Date<='2014-09-11 08:55:52'::timestamp AND p.AnswerCount>=0 AND p.AnswerCount<=4 AND p.CommentCount>=0 AND p.CommentCount<=17;

-- A q46
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v WHERE p.Id = c.PostId AND p.Id = pl.PostId AND p.Id = ph.PostId AND p.Id = v.PostId AND c.CreationDate>='2010-08-01 12:12:41'::timestamp AND p.Score<=44 AND p.FavoriteCount>=0 AND p.FavoriteCount<=3 AND p.CreationDate>='2010-08-11 13:53:56'::timestamp AND p.CreationDate<='2014-09-03 11:52:36'::timestamp AND pl.LinkTypeId=1 AND pl.CreationDate<='2014-08-11 17:26:31'::timestamp AND ph.CreationDate>='2010-09-20 19:11:45'::timestamp AND v.CreationDate<='2014-09-11 00:00:00'::timestamp;

-- A q109
SELECT COUNT(*) FROM comments as c, postLinks as pl, postHistory as ph, votes as v, posts as p WHERE pl.PostId = p.Id AND c.PostId = p.Id AND v.PostId = p.Id AND ph.PostId = p.Id AND c.Score=0 AND pl.CreationDate>='2011-11-21 22:39:41'::timestamp AND pl.CreationDate<='2014-09-01 16:29:56'::timestamp AND v.CreationDate>='2010-07-22 00:00:00'::timestamp AND v.CreationDate<='2014-09-14 00:00:00'::timestamp;

-- A q86
SELECT COUNT(*) FROM comments as c, votes as v, users as u, posts as p WHERE c.PostId = p.Id AND u.Id = c.UserId AND v.PostId = p.Id AND c.Score=0 AND u.Views>=0 AND u.Views<=74;

-- A q53
SELECT COUNT(*) FROM comments as c, postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND p.Id = c.PostId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND c.CreationDate<='2014-09-08 15:58:08'::timestamp AND p.ViewCount>=0 AND u.Reputation>=1;

-- A q54
SELECT COUNT(*) FROM comments as c, postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND p.Id = c.PostId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND pl.LinkTypeId=1 AND pl.CreationDate>='2011-04-12 15:23:59'::timestamp AND p.Score=1 AND p.ViewCount>=0 AND p.FavoriteCount>=0 AND u.CreationDate>='2011-02-08 18:11:37'::timestamp;

-- A q40
SELECT COUNT(*) FROM postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND pl.LinkTypeId=1 AND p.Score>=-1 AND p.CommentCount<=8 AND p.CreationDate>='2010-07-21 12:30:43'::timestamp AND p.CreationDate<='2014-09-07 01:11:03'::timestamp AND u.Views<=40 AND u.CreationDate>='2010-07-26 19:11:25'::timestamp AND u.CreationDate<='2014-09-11 22:26:42'::timestamp;

-- A q41
SELECT COUNT(*) FROM postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND pl.CreationDate<='2014-08-17 01:23:50'::timestamp AND p.Score>=-1 AND p.Score<=10 AND p.AnswerCount<=5 AND p.CommentCount=2 AND p.FavoriteCount>=0 AND p.FavoriteCount<=6 AND u.Views<=33 AND u.DownVotes>=0 AND u.CreationDate>='2010-08-19 17:31:36'::timestamp AND u.CreationDate<='2014-08-06 07:23:12'::timestamp AND b.Date<='2014-09-10 22:50:06'::timestamp;

-- A q7
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl WHERE c.UserId = p.OwnerUserId AND p.Id = pl.PostId AND p.CommentCount<=18 AND p.CreationDate>='2010-07-23 07:27:31'::timestamp AND p.CreationDate<='2014-09-09 01:43:00'::timestamp;

-- A q8
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl WHERE c.UserId = p.OwnerUserId AND p.Id = pl.PostId AND c.Score=0 AND p.CreationDate>='2010-09-06 00:58:21'::timestamp AND p.CreationDate<='2014-09-12 10:02:21'::timestamp AND pl.LinkTypeId=1 AND pl.CreationDate>='2011-07-09 22:35:44'::timestamp;

-- A q15
SELECT COUNT(*) FROM comments as c, badges as b, users as u WHERE u.Id = c.UserId AND c.UserId = b.UserId AND c.Score=0 AND c.CreationDate>='2010-07-24 06:46:49'::timestamp AND b.Date>='2010-07-19 20:34:06'::timestamp AND b.Date<='2014-09-12 15:11:36'::timestamp AND u.UpVotes>=0;

-- A q16
SELECT COUNT(*) FROM comments as c, badges as b, users as u WHERE u.Id = c.UserId AND c.UserId = b.UserId AND c.Score=0 AND b.Date>='2010-07-19 20:54:06'::timestamp AND u.DownVotes>=0 AND u.UpVotes>=0 AND u.UpVotes<=17 AND u.CreationDate>='2010-08-06 07:03:05'::timestamp AND u.CreationDate<='2014-09-08 04:18:44'::timestamp;

-- A q25
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, votes as v WHERE p.Id = c.PostId AND c.PostId = pl.PostId AND pl.PostId = v.PostId AND c.CreationDate>='2010-08-02 23:52:10'::timestamp AND p.Score>=-3 AND v.VoteTypeId=2 AND v.CreationDate<='2014-09-12 00:00:00'::timestamp;

-- A q28
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE p.Id = v.PostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND p.Score<=22 AND u.Reputation>=1;

-- A q29
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE p.Id = v.PostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND v.CreationDate<='2014-09-12 00:00:00'::timestamp AND p.PostTypeId=1 AND p.Score>=-1 AND p.FavoriteCount>=0 AND p.FavoriteCount<=20 AND b.Date>='2010-07-20 19:02:22'::timestamp AND b.Date<='2014-09-03 23:36:09'::timestamp AND u.DownVotes<=2 AND u.UpVotes>=0 AND u.CreationDate>='2010-11-26 03:34:11'::timestamp;

-- A q21
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.PostId = p.Id AND v.UserId = u.Id AND v.CreationDate<='2014-09-12 00:00:00'::timestamp AND p.Score>=-1 AND p.CreationDate>='2010-10-21 13:21:24'::timestamp AND p.CreationDate<='2014-09-09 15:12:22'::timestamp AND u.UpVotes>=0 AND u.CreationDate>='2010-07-27 17:15:57'::timestamp AND u.CreationDate<='2014-09-03 12:47:42'::timestamp;

-- A q1
SELECT COUNT(*) FROM badges as b, users as u WHERE b.UserId= u.Id AND u.UpVotes>=0;

-- A q130
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, users as u WHERE pl.RelatedPostId = p.Id AND u.Id= c.UserId AND c.PostId = p.Id AND ph.PostId = p.Id AND c.CreationDate>='2010-07-11 12:25:05'::timestamp AND c.CreationDate<='2014-09-11 13:43:09'::timestamp AND p.CommentCount>=0 AND p.CommentCount<=14 AND pl.LinkTypeId=1 AND ph.CreationDate>='2010-08-06 03:14:53'::timestamp AND u.Reputation>=1 AND u.Reputation<=491 AND u.DownVotes>=0 AND u.DownVotes<=0;

-- A q55
SELECT COUNT(*) FROM tags as t, posts as p, users as u, postHistory as ph, badges as b WHERE p.Id = t.ExcerptPostId AND u.Id = ph.UserId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND p.CommentCount>=0 AND u.DownVotes<=0 AND b.Date<='2014-08-22 02:21:55'::timestamp;

-- A q104
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE v.UserId = u.Id AND c.UserId = u.Id AND ph.UserId = u.Id AND ph.CreationDate>='2010-07-28 09:11:34'::timestamp AND ph.CreationDate<='2014-09-06 06:51:53'::timestamp AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.UpVotes<=72;

-- A q106
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE v.UserId = u.Id AND c.UserId = u.Id AND ph.UserId = u.Id AND c.CreationDate<='2014-08-28 07:25:55'::timestamp AND ph.PostHistoryTypeId=2 AND u.Reputation>=1 AND u.Views>=0 AND u.DownVotes>=0 AND u.UpVotes>=0 AND u.UpVotes<=15 AND u.CreationDate>='2010-09-03 11:45:16'::timestamp AND u.CreationDate<='2014-08-18 17:19:53'::timestamp;

-- A q49
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, badges as b, users as u WHERE u.Id =c.UserId AND c.UserId = p.OwnerUserId AND p.OwnerUserId = v.UserId AND v.UserId = b.UserId AND c.Score=1 AND p.Score>=-1 AND p.Score<=29 AND p.CreationDate>='2010-07-19 20:40:36'::timestamp AND p.CreationDate<='2014-09-10 20:52:30'::timestamp AND v.BountyAmount<=50 AND b.Date<='2014-08-25 19:05:46'::timestamp AND u.DownVotes<=11 AND u.CreationDate>='2010-07-31 17:32:56'::timestamp AND u.CreationDate<='2014-09-07 16:06:26'::timestamp;

-- A q50
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, badges as b, users as u WHERE u.Id =c.UserId AND c.UserId = p.OwnerUserId AND p.OwnerUserId = v.UserId AND v.UserId = b.UserId AND c.Score=1 AND p.Score>=-2 AND p.Score<=23 AND p.ViewCount<=2432 AND p.CommentCount=0 AND p.FavoriteCount>=0 AND u.Reputation>=1 AND u.Reputation<=113 AND u.Views>=0 AND u.Views<=51;

-- A q12
SELECT COUNT(*) FROM comments as c, postHistory as ph, users as u WHERE u.Id = c.UserId AND c.UserId = ph.UserId AND u.Reputation>=1 AND u.Reputation<=487 AND u.UpVotes<=27 AND u.CreationDate>='2010-10-22 22:40:35'::timestamp AND u.CreationDate<='2014-09-10 17:01:31'::timestamp;

-- A q17
SELECT COUNT(*) FROM posts as p, tags as t, votes as v WHERE p.Id = t.ExcerptPostId AND p.OwnerUserId = v.UserId AND p.CreationDate>='2010-07-20 02:01:05'::timestamp;

-- A q48
SELECT COUNT(*) FROM comments as c, postHistory as ph, badges as b, votes as v, users as u WHERE u.Id =b.UserId AND b.UserId = ph.UserId AND ph.UserId = v.UserId AND v.UserId = c.UserId AND c.CreationDate>='2010-07-20 21:37:31'::timestamp AND ph.PostHistoryTypeId=12 AND u.UpVotes=0;

-- A q13
SELECT COUNT(*) FROM comments as c, votes as v, users as u WHERE u.Id = c.UserId AND u.Id = v.UserId AND c.CreationDate>='2010-08-10 17:55:45'::timestamp AND u.Reputation>=1 AND u.Reputation<=691;

-- A q14
SELECT COUNT(*) FROM comments as c, votes as v, users as u WHERE u.Id = c.UserId AND u.Id = v.UserId AND c.CreationDate>='2010-10-01 20:45:26'::timestamp AND c.CreationDate<='2014-09-05 12:51:17'::timestamp AND v.BountyAmount<=100 AND u.UpVotes=0 AND u.CreationDate<='2014-09-12 03:25:34'::timestamp;

-- B q27
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, users as u WHERE u.Id = c.UserId AND u.Id = p.OwnerUserId AND u.Id = v.UserId AND c.Score=0 AND c.CreationDate<='2014-09-13 20:12:15'::timestamp AND p.CreationDate>='2010-07-27 01:51:15'::timestamp AND v.BountyAmount<=50 AND v.CreationDate<='2014-09-12 00:00:00'::timestamp AND u.UpVotes>=0 AND u.UpVotes<=12 AND u.CreationDate>='2010-07-19 19:09:39'::timestamp;

-- B q97
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, users as u WHERE u.Id = p.OwnerUserId AND u.Id = c.UserId AND u.Id = v.UserId AND c.CreationDate>='2010-07-27 12:03:40'::timestamp AND p.Score>=0 AND p.Score<=28 AND p.ViewCount>=0 AND p.ViewCount<=6517 AND p.AnswerCount>=0 AND p.AnswerCount<=5 AND p.FavoriteCount>=0 AND p.FavoriteCount<=8 AND p.CreationDate>='2010-07-27 11:29:20'::timestamp AND p.CreationDate<='2014-09-13 02:50:15'::timestamp AND u.Views>=0 AND u.CreationDate>='2010-07-27 09:38:05'::timestamp;

-- B q64
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, users as u WHERE p.Id = pl.PostId AND p.Id = ph.PostId AND p.Id = c.PostId AND u.Id = c.UserId AND u.Id = v.UserId AND c.CreationDate<='2014-09-11 13:24:22'::timestamp AND p.PostTypeId=1 AND p.Score=2 AND p.FavoriteCount<=12 AND pl.CreationDate>='2010-08-13 11:42:08'::timestamp AND pl.CreationDate<='2014-08-29 00:27:05'::timestamp AND ph.CreationDate>='2011-01-03 23:47:35'::timestamp AND ph.CreationDate<='2014-09-08 12:48:36'::timestamp AND v.CreationDate>='2010-07-27 00:00:00'::timestamp AND u.Reputation>=1 AND u.DownVotes>=0;

-- B q65
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, users as u WHERE p.Id = pl.PostId AND p.Id = ph.PostId AND p.Id = c.PostId AND u.Id = c.UserId AND u.Id = v.UserId AND c.Score=0 AND c.CreationDate>='2010-07-20 06:26:28'::timestamp AND c.CreationDate<='2014-09-11 18:45:09'::timestamp AND p.PostTypeId=1 AND p.FavoriteCount>=0 AND p.FavoriteCount<=2 AND ph.PostHistoryTypeId=5 AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.CreationDate>='2010-09-18 01:58:41'::timestamp;

-- B q2
SELECT COUNT(*) FROM comments as c, badges as b WHERE c.UserId = b.UserId AND c.Score=0 AND b.Date<='2014-09-11 14:33:06'::timestamp;

-- B q74
SELECT COUNT(*) FROM comments as c, posts as p, postHistory as ph, votes as v, badges as b, users as u WHERE u.Id = p.OwnerUserId AND u.Id = b.UserId AND p.Id = c.PostId AND p.Id = ph.PostId AND p.Id = v.PostId AND p.PostTypeId=1 AND p.Score<=192 AND p.ViewCount>=0 AND p.ViewCount<=2772 AND p.AnswerCount<=5 AND u.DownVotes>=0;

-- B q75
SELECT COUNT(*) FROM comments as c, posts as p, postHistory as ph, votes as v, badges as b, users as u WHERE u.Id = p.OwnerUserId AND u.Id = b.UserId AND p.Id = c.PostId AND p.Id = ph.PostId AND p.Id = v.PostId AND c.Score=0 AND c.CreationDate<='2014-09-09 19:58:29'::timestamp AND p.Score>=-4 AND p.ViewCount>=0 AND p.ViewCount<=5977 AND p.AnswerCount<=4 AND p.CommentCount>=0 AND p.CommentCount<=11 AND p.CreationDate>='2011-01-25 08:31:41'::timestamp AND u.Reputation<=312 AND u.DownVotes<=0;

-- B q10
SELECT COUNT(*) FROM comments as c, posts as p, users as u WHERE c.UserId = u.Id AND u.Id = p.OwnerUserId AND c.CreationDate>='2010-08-05 00:36:02'::timestamp AND c.CreationDate<='2014-09-08 16:50:49'::timestamp AND p.ViewCount>=0 AND p.ViewCount<=2897 AND p.CommentCount>=0 AND p.CommentCount<=16 AND p.FavoriteCount>=0 AND p.FavoriteCount<=10;

-- B q11
SELECT COUNT(*) FROM comments as c, posts as p, users as u WHERE c.UserId = u.Id AND u.Id = p.OwnerUserId AND c.Score=0 AND p.Score>=0 AND p.Score<=15 AND p.ViewCount>=0 AND p.ViewCount<=3002 AND p.AnswerCount<=3 AND p.CommentCount<=10 AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.CreationDate>='2010-08-23 16:21:10'::timestamp AND u.CreationDate<='2014-09-02 09:50:06'::timestamp;

-- B q60
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, votes as v, badges as b, users as u WHERE p.Id = c.PostId AND p.Id = pl.RelatedPostId AND p.Id = v.PostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND u.Views<=190 AND u.CreationDate>='2010-07-20 08:05:39'::timestamp AND u.CreationDate<='2014-08-27 09:31:28'::timestamp;

-- B q61
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, votes as v, badges as b, users as u WHERE p.Id = c.PostId AND p.Id = pl.RelatedPostId AND p.Id = v.PostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND c.CreationDate>='2010-08-06 12:21:39'::timestamp AND c.CreationDate<='2014-09-11 20:55:34'::timestamp AND p.Score>=0 AND p.Score<=13 AND p.FavoriteCount>=0 AND pl.LinkTypeId=1 AND pl.CreationDate>='2011-03-11 18:50:29'::timestamp AND v.VoteTypeId=2 AND v.CreationDate<='2014-09-11 00:00:00'::timestamp AND u.Reputation>=1 AND u.CreationDate>='2011-02-17 03:42:02'::timestamp AND u.CreationDate<='2014-09-01 10:54:39'::timestamp;

-- B q18
SELECT COUNT(*) FROM posts as p, postLinks as pl, postHistory as ph WHERE p.Id = pl.PostId AND pl.PostId = ph.PostId AND p.CreationDate>='2010-07-19 20:08:37'::timestamp AND ph.CreationDate>='2010-07-20 00:30:00'::timestamp;

-- B q89
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u WHERE p.OwnerUserId = u.Id AND ph.UserId = u.Id AND p.Score>=-1 AND p.CommentCount>=0 AND p.CommentCount<=23 AND u.DownVotes=0 AND u.UpVotes>=0 AND u.UpVotes<=244;

-- B q90
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u WHERE p.OwnerUserId = u.Id AND ph.UserId = u.Id AND ph.CreationDate>='2011-05-20 18:43:03'::timestamp AND p.FavoriteCount<=5 AND u.Views>=0 AND u.UpVotes>=0 AND u.CreationDate>='2010-11-27 21:46:49'::timestamp AND u.CreationDate<='2014-08-18 13:00:22'::timestamp;

-- B q35
SELECT COUNT(*) FROM comments as c, votes as v, badges as b, users as u WHERE u.Id = c.UserId AND u.Id = v.UserId AND u.Id = b.UserId AND c.Score=0 AND v.BountyAmount>=0 AND v.CreationDate<='2014-09-11 00:00:00'::timestamp AND u.DownVotes<=57 AND u.CreationDate>='2010-08-26 09:01:31'::timestamp AND u.CreationDate<='2014-08-10 11:01:39'::timestamp;

-- B q36
SELECT COUNT(*) FROM comments as c, votes as v, badges as b, users as u WHERE u.Id = c.UserId AND u.Id = v.UserId AND u.Id = b.UserId AND c.Score=0 AND v.BountyAmount>=0 AND v.BountyAmount<=300 AND v.CreationDate>='2010-07-29 00:00:00'::timestamp AND u.UpVotes>=0 AND u.UpVotes<=18;

-- B q39
SELECT COUNT(*) FROM comments as c, postHistory as ph, badges as b, users as u WHERE u.Id = c.UserId AND u.Id = ph.UserId AND u.Id = b.UserId AND c.Score=0 AND c.CreationDate>='2010-07-20 10:52:57'::timestamp AND ph.PostHistoryTypeId=5 AND ph.CreationDate>='2011-01-31 15:35:37'::timestamp AND u.Reputation>=1 AND u.Reputation<=356 AND u.DownVotes<=34 AND u.CreationDate>='2010-07-19 21:29:29'::timestamp AND u.CreationDate<='2014-08-20 14:31:46'::timestamp;

-- B q113
SELECT COUNT(*) FROM comments as c, postHistory as ph, badges as b, users as u WHERE u.Id = b.UserId AND u.Id = ph.UserId AND u.Id = c.UserId AND c.CreationDate<='2014-09-10 00:33:30'::timestamp AND u.DownVotes<=0 AND u.UpVotes<=47;

-- B q87
SELECT COUNT(*) FROM comments as c, badges as b, users as u WHERE c.UserId = u.Id AND b.UserId = u.Id AND c.CreationDate>='2010-08-12 20:27:30'::timestamp AND c.CreationDate<='2014-09-12 12:49:19'::timestamp AND u.Views>=0 AND u.DownVotes>=0 AND u.DownVotes<=2;

-- B q88
SELECT COUNT(*) FROM comments as c, badges as b, users as u WHERE c.UserId = u.Id AND b.UserId = u.Id AND c.Score=0 AND c.CreationDate>='2010-07-24 06:46:49'::timestamp AND b.Date>='2010-07-19 20:34:06'::timestamp AND b.Date<='2014-09-12 15:11:36'::timestamp AND u.UpVotes>=0;

-- B q19
SELECT COUNT(*) FROM posts as p, postLinks as pl, users as u WHERE p.Id = pl.PostId AND p.OwnerUserId = u.Id AND p.CommentCount<=17 AND u.CreationDate<='2014-09-12 07:12:16'::timestamp;

-- B q42
SELECT COUNT(*) FROM postHistory as ph, posts as p, votes as v, users as u WHERE p.Id = ph.PostId AND u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.PostTypeId=1 AND p.Score>=-1 AND p.CommentCount>=0 AND p.CommentCount<=11;

-- B q117
SELECT COUNT(*) FROM postHistory as ph, posts as p, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = ph.PostId AND p.Id = v.PostId AND ph.CreationDate>='2010-07-21 00:44:08'::timestamp AND p.ViewCount>=0 AND p.CommentCount>=0 AND v.VoteTypeId=2 AND u.Views>=0 AND u.Views<=34 AND u.UpVotes>=0;

-- B q45
SELECT COUNT(*) FROM postHistory as ph, votes as v, users as u, badges as b WHERE u.Id = ph.UserId AND u.Id = v.UserId AND u.Id = b.UserId AND ph.PostHistoryTypeId=2 AND u.Views=5 AND u.DownVotes>=0 AND u.UpVotes>=0 AND u.UpVotes<=224 AND u.CreationDate<='2014-09-04 04:41:22'::timestamp AND b.Date>='2010-07-19 19:39:10'::timestamp AND b.Date<='2014-09-05 18:37:48'::timestamp;

-- B q124
SELECT COUNT(*) FROM postHistory as ph, votes as v, users as u, badges as b WHERE u.Id = b.UserId AND u.Id = ph.UserId AND u.Id = v.UserId AND v.CreationDate<='2014-09-10 00:00:00'::timestamp AND u.DownVotes>=0 AND u.DownVotes<=3 AND u.UpVotes>=0 AND u.UpVotes<=71 AND b.Date>='2010-07-19 21:54:06'::timestamp;

-- B q23
SELECT COUNT(*) FROM votes as v, badges as b, users as u WHERE u.Id = v.UserId AND v.UserId = b.UserId AND u.DownVotes>=0 AND u.DownVotes<=0;

-- B q24
SELECT COUNT(*) FROM votes as v, badges as b, users as u WHERE u.Id = v.UserId AND v.UserId = b.UserId AND v.BountyAmount>=0 AND v.BountyAmount<=50 AND u.DownVotes=0;

-- B q95
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, posts as p WHERE ph.PostId = p.Id AND c.PostId = p.Id AND v.PostId = p.Id AND v.CreationDate<='2014-09-12 00:00:00'::timestamp;

-- B q96
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, posts as p WHERE ph.PostId = p.Id AND c.PostId = p.Id AND v.PostId = p.Id AND c.Score=0 AND c.CreationDate>='2010-08-26 06:55:11'::timestamp AND ph.CreationDate<='2014-09-05 06:39:25'::timestamp AND v.VoteTypeId=2;

-- B q131
SELECT COUNT(*) FROM comments as c, posts as p, postHistory as ph, votes as v, users as u WHERE v.UserId = u.Id AND c.UserId = u.Id AND p.OwnerUserId = u.Id AND ph.UserId = u.Id AND c.Score=2 AND p.AnswerCount>=0 AND p.AnswerCount<=9 AND p.CreationDate>='2010-07-20 18:17:25'::timestamp AND p.CreationDate<='2014-08-26 12:57:22'::timestamp AND ph.CreationDate<='2014-09-02 07:58:47'::timestamp AND v.BountyAmount>=0 AND v.CreationDate>='2010-05-19 00:00:00'::timestamp AND u.UpVotes<=230 AND u.CreationDate>='2010-09-22 01:07:10'::timestamp AND u.CreationDate<='2014-08-15 05:52:23'::timestamp;

-- B q132
SELECT COUNT(*) FROM comments as c, posts as p, postHistory as ph, votes as v, users as u WHERE v.UserId = u.Id AND c.UserId = u.Id AND p.OwnerUserId = u.Id AND ph.UserId = u.Id AND p.ViewCount>=0 AND p.AnswerCount>=0 AND p.AnswerCount<=5 AND ph.PostHistoryTypeId=2 AND ph.CreationDate>='2010-11-05 01:25:39'::timestamp AND ph.CreationDate<='2014-09-09 07:14:12'::timestamp AND v.BountyAmount>=0 AND v.BountyAmount<=100 AND v.CreationDate>='2010-07-26 00:00:00'::timestamp AND u.Views>=0 AND u.Views<=13;

-- B q9
SELECT COUNT(*) FROM comments as c, posts as p, postHistory as ph WHERE p.Id = c.PostId AND p.Id = ph.PostId AND p.CommentCount>=0 AND p.CommentCount<=25;
