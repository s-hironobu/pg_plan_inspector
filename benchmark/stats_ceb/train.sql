-- STATS-CEB subset for pgpi (benchmark/stats_ceb).  Queries from STATS-CEB, Han et al.,
-- "Cardinality Estimation in DBMS: A Comprehensive Benchmark Evaluation", PVLDB 15, 2022
-- (https://github.com/ZhengtongYan/End-to-End-CardEst-Benchmark, workloads/stats_CEB/stats_CEB.sql).
-- Training: variants of the A queries of test.sql, with constants drawn from the columns'
-- distributions (same tables, joins and filtered columns), each finishing within 5 s.
-- '-- train q12' names the test query a variant belongs to.

-- train q43
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE u.Id = p.OwnerUserId AND p.OwnerUserId = ph.UserId AND ph.UserId = b.UserId AND ph.PostHistoryTypeId=2 AND p.Score>=0 AND u.Reputation>=16 AND u.UpVotes>=0 AND u.UpVotes<=0;

-- train q43
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE u.Id = p.OwnerUserId AND p.OwnerUserId = ph.UserId AND ph.UserId = b.UserId AND ph.PostHistoryTypeId=1 AND p.Score>=1 AND u.Reputation>=11 AND u.UpVotes>=0 AND u.UpVotes<=11442;

-- train q43
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE u.Id = p.OwnerUserId AND p.OwnerUserId = ph.UserId AND ph.UserId = b.UserId AND ph.PostHistoryTypeId=2 AND p.Score>=1 AND u.Reputation>=6 AND u.UpVotes>=0 AND u.UpVotes<=2;

-- train q43
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE u.Id = p.OwnerUserId AND p.OwnerUserId = ph.UserId AND ph.UserId = b.UserId AND ph.PostHistoryTypeId=2 AND p.Score>=1 AND u.Reputation>=1 AND u.UpVotes>=0 AND u.UpVotes<=0;

-- train q43
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE u.Id = p.OwnerUserId AND p.OwnerUserId = ph.UserId AND ph.UserId = b.UserId AND ph.PostHistoryTypeId=4 AND p.Score>=1 AND u.Reputation>=26 AND u.UpVotes>=0 AND u.UpVotes<=0;

-- train q43
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE u.Id = p.OwnerUserId AND p.OwnerUserId = ph.UserId AND ph.UserId = b.UserId AND ph.PostHistoryTypeId=5 AND p.Score>=-19 AND u.Reputation>=1 AND u.UpVotes>=0 AND u.UpVotes<=0;

-- train q43
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE u.Id = p.OwnerUserId AND p.OwnerUserId = ph.UserId AND ph.UserId = b.UserId AND ph.PostHistoryTypeId=5 AND p.Score>=0 AND u.Reputation>=1 AND u.UpVotes>=0 AND u.UpVotes<=0;

-- train q43
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE u.Id = p.OwnerUserId AND p.OwnerUserId = ph.UserId AND ph.UserId = b.UserId AND ph.PostHistoryTypeId=5 AND p.Score>=2 AND u.Reputation>=1 AND u.UpVotes>=0 AND u.UpVotes<=1;

-- train q43
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE u.Id = p.OwnerUserId AND p.OwnerUserId = ph.UserId AND ph.UserId = b.UserId AND ph.PostHistoryTypeId=3 AND p.Score>=1 AND u.Reputation>=1 AND u.UpVotes>=0 AND u.UpVotes<=0;

-- train q43
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE u.Id = p.OwnerUserId AND p.OwnerUserId = ph.UserId AND ph.UserId = b.UserId AND ph.PostHistoryTypeId=5 AND p.Score>=0 AND u.Reputation>=1 AND u.UpVotes>=0 AND u.UpVotes<=1;

-- train q44
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE u.Id = p.OwnerUserId AND p.OwnerUserId = ph.UserId AND ph.UserId = b.UserId AND ph.CreationDate>='2012-07-25 23:18:13'::timestamp AND ph.CreationDate<='2014-03-25 00:33:01'::timestamp AND p.Score>=1 AND p.ViewCount>=153 AND p.ViewCount<=107 AND p.AnswerCount>=0 AND p.CommentCount>=0 AND p.CommentCount<=1 AND p.FavoriteCount<=2 AND p.CreationDate>='2009-02-02 14:21:12'::timestamp AND p.CreationDate<='2013-06-23 12:50:00'::timestamp AND u.Views>=0 AND u.DownVotes>=0 AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.UpVotes<=0;

-- train q44
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE u.Id = p.OwnerUserId AND p.OwnerUserId = ph.UserId AND ph.UserId = b.UserId AND ph.CreationDate>='2013-09-02 11:29:35'::timestamp AND ph.CreationDate<='2014-03-25 00:33:01'::timestamp AND p.Score>=2 AND p.ViewCount>=44 AND p.ViewCount<=2147 AND p.AnswerCount>=1 AND p.CommentCount>=0 AND p.CommentCount<=4 AND p.FavoriteCount<=2 AND p.CreationDate>='2013-08-20 10:19:52'::timestamp AND p.CreationDate<='2014-08-01 05:10:10'::timestamp AND u.Views>=0 AND u.DownVotes>=0 AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.UpVotes<=0;

-- train q44
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE u.Id = p.OwnerUserId AND p.OwnerUserId = ph.UserId AND ph.UserId = b.UserId AND ph.CreationDate>='2012-02-07 13:46:01'::timestamp AND ph.CreationDate<='2014-05-06 12:40:26'::timestamp AND p.Score>=0 AND p.ViewCount>=19 AND p.ViewCount<=493 AND p.AnswerCount>=0 AND p.CommentCount>=1 AND p.CommentCount<=1 AND p.FavoriteCount<=2 AND p.CreationDate>='2012-03-28 16:24:28'::timestamp AND p.CreationDate<='2013-02-15 03:31:21'::timestamp AND u.Views>=0 AND u.DownVotes>=0 AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.UpVotes<=0;

-- train q44
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE u.Id = p.OwnerUserId AND p.OwnerUserId = ph.UserId AND ph.UserId = b.UserId AND ph.CreationDate>='2011-01-24 07:48:41'::timestamp AND ph.CreationDate<='2014-02-11 05:09:03'::timestamp AND p.Score>=1 AND p.ViewCount>=36 AND p.ViewCount<=153 AND p.AnswerCount>=0 AND p.CommentCount>=0 AND p.CommentCount<=7 AND p.FavoriteCount<=1 AND p.CreationDate>='2013-08-20 10:19:52'::timestamp AND p.CreationDate<='2012-11-26 23:54:07'::timestamp AND u.Views>=0 AND u.DownVotes>=0 AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.UpVotes<=9;

-- train q44
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE u.Id = p.OwnerUserId AND p.OwnerUserId = ph.UserId AND ph.UserId = b.UserId AND ph.CreationDate>='2009-02-02 14:21:12'::timestamp AND ph.CreationDate<='2014-07-31 22:42:34'::timestamp AND p.Score>=2 AND p.ViewCount>=153 AND p.ViewCount<=2147 AND p.AnswerCount>=1 AND p.CommentCount>=2 AND p.CommentCount<=2 AND p.FavoriteCount<=2 AND p.CreationDate>='2012-09-11 15:29:12'::timestamp AND p.CreationDate<='2014-06-18 20:06:36'::timestamp AND u.Views>=0 AND u.DownVotes>=0 AND u.DownVotes<=1920 AND u.UpVotes>=0 AND u.UpVotes<=0;

-- train q44
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE u.Id = p.OwnerUserId AND p.OwnerUserId = ph.UserId AND ph.UserId = b.UserId AND ph.CreationDate>='2011-10-14 08:09:28'::timestamp AND ph.CreationDate<='2014-02-11 05:09:03'::timestamp AND p.Score>=1 AND p.ViewCount>=185 AND p.ViewCount<=2147 AND p.AnswerCount>=1 AND p.CommentCount>=0 AND p.CommentCount<=4 AND p.FavoriteCount<=8 AND p.CreationDate>='2009-02-02 14:21:12'::timestamp AND p.CreationDate<='2013-08-20 10:19:52'::timestamp AND u.Views>=0 AND u.DownVotes>=0 AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.UpVotes<=2;

-- train q44
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE u.Id = p.OwnerUserId AND p.OwnerUserId = ph.UserId AND ph.UserId = b.UserId AND ph.CreationDate>='2013-09-02 11:29:35'::timestamp AND ph.CreationDate<='2014-05-06 12:40:26'::timestamp AND p.Score>=2 AND p.ViewCount>=90 AND p.ViewCount<=493 AND p.AnswerCount>=0 AND p.CommentCount>=0 AND p.CommentCount<=4 AND p.FavoriteCount<=1 AND p.CreationDate>='2012-06-19 22:27:44'::timestamp AND p.CreationDate<='2013-02-15 03:31:21'::timestamp AND u.Views>=0 AND u.DownVotes>=0 AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.UpVotes<=2;

-- train q44
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE u.Id = p.OwnerUserId AND p.OwnerUserId = ph.UserId AND ph.UserId = b.UserId AND ph.CreationDate>='2013-07-12 12:21:54'::timestamp AND ph.CreationDate<='2012-12-27 00:14:56'::timestamp AND p.Score>=1 AND p.ViewCount>=185 AND p.ViewCount<=90 AND p.AnswerCount>=1 AND p.CommentCount>=1 AND p.CommentCount<=3 AND p.FavoriteCount<=1 AND p.CreationDate>='2011-04-26 07:24:16'::timestamp AND p.CreationDate<='2013-04-21 19:38:59'::timestamp AND u.Views>=0 AND u.DownVotes>=0 AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.UpVotes<=3;

-- train q44
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE u.Id = p.OwnerUserId AND p.OwnerUserId = ph.UserId AND ph.UserId = b.UserId AND ph.CreationDate>='2012-02-07 13:46:01'::timestamp AND ph.CreationDate<='2014-06-18 13:33:16'::timestamp AND p.Score>=0 AND p.ViewCount>=44 AND p.ViewCount<=107 AND p.AnswerCount>=0 AND p.CommentCount>=0 AND p.CommentCount<=2 AND p.FavoriteCount<=1 AND p.CreationDate>='2013-06-23 12:50:00'::timestamp AND p.CreationDate<='2014-03-25 12:00:30'::timestamp AND u.Views>=0 AND u.DownVotes>=0 AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.UpVotes<=0;

-- train q44
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE u.Id = p.OwnerUserId AND p.OwnerUserId = ph.UserId AND ph.UserId = b.UserId AND ph.CreationDate>='2012-10-11 15:16:49'::timestamp AND ph.CreationDate<='2013-09-02 11:29:35'::timestamp AND p.Score>=0 AND p.ViewCount>=90 AND p.ViewCount<=175495 AND p.AnswerCount>=1 AND p.CommentCount>=1 AND p.CommentCount<=2 AND p.FavoriteCount<=3 AND p.CreationDate>='2013-08-20 10:19:52'::timestamp AND p.CreationDate<='2014-02-08 20:51:44'::timestamp AND u.Views>=0 AND u.DownVotes>=0 AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.UpVotes<=0;

-- train q145
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND p.Id = pl.PostId AND p.Id = ph.PostId AND p.PostTypeId=5 AND p.AnswerCount>=1 AND p.CreationDate>='2009-02-02 14:21:12'::timestamp AND p.CreationDate<='2014-02-08 20:51:44'::timestamp AND pl.CreationDate>='2010-07-21 14:47:33'::timestamp AND pl.CreationDate<='2014-06-25 13:05:06'::timestamp AND ph.PostHistoryTypeId=3 AND v.CreationDate>='2013-04-26 00:00:00'::timestamp AND u.UpVotes>=0 AND u.CreationDate<='2014-07-07 19:57:03'::timestamp;

-- train q145
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND p.Id = pl.PostId AND p.Id = ph.PostId AND p.PostTypeId=5 AND p.AnswerCount>=1 AND p.CreationDate>='2012-11-26 23:54:07'::timestamp AND p.CreationDate<='2013-08-20 10:19:52'::timestamp AND pl.CreationDate>='2011-05-10 17:09:25'::timestamp AND pl.CreationDate<='2013-11-13 10:12:40'::timestamp AND ph.PostHistoryTypeId=5 AND v.CreationDate>='2009-02-02 00:00:00'::timestamp AND u.UpVotes>=0 AND u.CreationDate<='2014-01-10 18:05:07'::timestamp;

-- train q145
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND p.Id = pl.PostId AND p.Id = ph.PostId AND p.PostTypeId=5 AND p.AnswerCount>=0 AND p.CreationDate>='2013-02-15 03:31:21'::timestamp AND p.CreationDate<='2013-08-20 10:19:52'::timestamp AND pl.CreationDate>='2012-06-15 12:31:41'::timestamp AND pl.CreationDate<='2013-11-13 10:12:40'::timestamp AND ph.PostHistoryTypeId=1 AND v.CreationDate>='2011-03-18 00:00:00'::timestamp AND u.UpVotes>=0 AND u.CreationDate<='2014-01-10 18:05:07'::timestamp;

-- train q145
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND p.Id = pl.PostId AND p.Id = ph.PostId AND p.PostTypeId=2 AND p.AnswerCount>=1 AND p.CreationDate>='2012-09-11 15:29:12'::timestamp AND p.CreationDate<='2012-11-26 23:54:07'::timestamp AND pl.CreationDate>='2013-11-13 10:12:40'::timestamp AND pl.CreationDate<='2013-07-24 12:34:55'::timestamp AND ph.PostHistoryTypeId=2 AND v.CreationDate>='2012-11-26 00:00:00'::timestamp AND u.UpVotes>=0 AND u.CreationDate<='2013-03-18 20:44:37'::timestamp;

-- train q145
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND p.Id = pl.PostId AND p.Id = ph.PostId AND p.PostTypeId=5 AND p.AnswerCount>=1 AND p.CreationDate>='2013-02-15 03:31:21'::timestamp AND p.CreationDate<='2013-12-14 13:32:25'::timestamp AND pl.CreationDate>='2013-02-18 03:03:17'::timestamp AND pl.CreationDate<='2014-08-02 21:16:08'::timestamp AND ph.PostHistoryTypeId=4 AND v.CreationDate>='2013-02-12 00:00:00'::timestamp AND u.UpVotes>=0 AND u.CreationDate<='2014-07-07 19:57:03'::timestamp;

-- train q145
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND p.Id = pl.PostId AND p.Id = ph.PostId AND p.PostTypeId=5 AND p.AnswerCount>=0 AND p.CreationDate>='2012-06-19 22:27:44'::timestamp AND p.CreationDate<='2014-06-18 20:06:36'::timestamp AND pl.CreationDate>='2010-07-21 14:47:33'::timestamp AND pl.CreationDate<='2013-07-24 12:34:55'::timestamp AND ph.PostHistoryTypeId=1 AND v.CreationDate>='2011-10-26 00:00:00'::timestamp AND u.UpVotes>=0 AND u.CreationDate<='2013-09-19 17:00:57'::timestamp;

-- train q145
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND p.Id = pl.PostId AND p.Id = ph.PostId AND p.PostTypeId=4 AND p.AnswerCount>=0 AND p.CreationDate>='2013-08-20 10:19:52'::timestamp AND p.CreationDate<='2014-05-06 17:16:49'::timestamp AND pl.CreationDate>='2013-09-20 11:04:34'::timestamp AND pl.CreationDate<='2013-11-13 10:12:40'::timestamp AND ph.PostHistoryTypeId=4 AND v.CreationDate>='2012-11-26 00:00:00'::timestamp AND u.UpVotes>=0 AND u.CreationDate<='2013-03-18 20:44:37'::timestamp;

-- train q145
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND p.Id = pl.PostId AND p.Id = ph.PostId AND p.PostTypeId=5 AND p.AnswerCount>=0 AND p.CreationDate>='2013-04-21 19:38:59'::timestamp AND p.CreationDate<='2013-06-23 12:50:00'::timestamp AND pl.CreationDate>='2011-05-10 17:09:25'::timestamp AND pl.CreationDate<='2013-09-20 11:04:34'::timestamp AND ph.PostHistoryTypeId=4 AND v.CreationDate>='2012-04-26 00:00:00'::timestamp AND u.UpVotes>=0 AND u.CreationDate<='2014-06-03 08:58:38'::timestamp;

-- train q145
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND p.Id = pl.PostId AND p.Id = ph.PostId AND p.PostTypeId=6 AND p.AnswerCount>=0 AND p.CreationDate>='2012-11-26 23:54:07'::timestamp AND p.CreationDate<='2014-09-14 02:09:23'::timestamp AND pl.CreationDate>='2012-06-15 12:31:41'::timestamp AND pl.CreationDate<='2014-06-25 13:05:06'::timestamp AND ph.PostHistoryTypeId=2 AND v.CreationDate>='2013-04-26 00:00:00'::timestamp AND u.UpVotes>=0 AND u.CreationDate<='2014-09-14 01:01:44'::timestamp;

-- train q145
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND p.Id = pl.PostId AND p.Id = ph.PostId AND p.PostTypeId=1 AND p.AnswerCount>=1 AND p.CreationDate>='2011-08-31 21:28:05'::timestamp AND p.CreationDate<='2014-06-18 20:06:36'::timestamp AND pl.CreationDate>='2012-09-01 05:33:57'::timestamp AND pl.CreationDate<='2014-06-25 13:05:06'::timestamp AND ph.PostHistoryTypeId=3 AND v.CreationDate>='2012-11-26 00:00:00'::timestamp AND u.UpVotes>=0 AND u.CreationDate<='2014-02-16 15:09:45'::timestamp;

-- train q146
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND p.Id = pl.PostId AND p.Id = ph.PostId AND c.CreationDate>='2011-05-30 17:17:48'::timestamp AND p.Score>=0 AND p.CommentCount<=4 AND p.CreationDate>='2012-03-28 16:24:28'::timestamp AND p.CreationDate<='2014-09-14 02:09:23'::timestamp AND pl.CreationDate<='2014-09-13 20:54:31'::timestamp AND ph.CreationDate>='2011-01-24 07:48:41'::timestamp AND u.DownVotes>=0 AND u.UpVotes>=0;

-- train q146
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND p.Id = pl.PostId AND p.Id = ph.PostId AND c.CreationDate>='2012-01-28 20:58:07'::timestamp AND p.Score>=0 AND p.CommentCount<=2 AND p.CreationDate>='2013-08-20 10:19:52'::timestamp AND p.CreationDate<='2012-11-26 23:54:07'::timestamp AND pl.CreationDate<='2013-09-20 11:04:34'::timestamp AND ph.CreationDate>='2013-05-17 08:19:48'::timestamp AND u.DownVotes>=0 AND u.UpVotes>=0;

-- train q146
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND p.Id = pl.PostId AND p.Id = ph.PostId AND c.CreationDate>='2013-08-10 10:42:04'::timestamp AND p.Score>=1 AND p.CommentCount<=7 AND p.CreationDate>='2012-06-19 22:27:44'::timestamp AND p.CreationDate<='2013-04-21 19:38:59'::timestamp AND pl.CreationDate<='2013-11-13 10:12:40'::timestamp AND ph.CreationDate>='2012-05-07 03:11:00'::timestamp AND u.DownVotes>=0 AND u.UpVotes>=0;

-- train q146
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND p.Id = pl.PostId AND p.Id = ph.PostId AND c.CreationDate>='2009-02-02 14:45:19'::timestamp AND p.Score>=1 AND p.CommentCount<=0 AND p.CreationDate>='2012-06-19 22:27:44'::timestamp AND p.CreationDate<='2013-04-21 19:38:59'::timestamp AND pl.CreationDate<='2014-08-02 21:16:08'::timestamp AND ph.CreationDate>='2012-10-11 15:16:49'::timestamp AND u.DownVotes>=0 AND u.UpVotes>=0;

-- train q146
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND p.Id = pl.PostId AND p.Id = ph.PostId AND c.CreationDate>='2011-10-04 13:23:07'::timestamp AND p.Score>=1 AND p.CommentCount<=4 AND p.CreationDate>='2011-08-31 21:28:05'::timestamp AND p.CreationDate<='2013-12-14 13:32:25'::timestamp AND pl.CreationDate<='2014-02-08 14:31:25'::timestamp AND ph.CreationDate>='2013-05-17 08:19:48'::timestamp AND u.DownVotes>=0 AND u.UpVotes>=0;

-- train q146
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND p.Id = pl.PostId AND p.Id = ph.PostId AND c.CreationDate>='2012-08-31 10:10:28'::timestamp AND p.Score>=1 AND p.CommentCount<=2 AND p.CreationDate>='2012-09-11 15:29:12'::timestamp AND p.CreationDate<='2014-05-06 17:16:49'::timestamp AND pl.CreationDate<='2013-07-24 12:34:55'::timestamp AND ph.CreationDate>='2012-05-07 03:11:00'::timestamp AND u.DownVotes>=0 AND u.UpVotes>=0;

-- train q146
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND p.Id = pl.PostId AND p.Id = ph.PostId AND c.CreationDate>='2013-02-02 20:12:58'::timestamp AND p.Score>=0 AND p.CommentCount<=1 AND p.CreationDate>='2011-04-26 07:24:16'::timestamp AND p.CreationDate<='2012-11-26 23:54:07'::timestamp AND pl.CreationDate<='2013-07-24 12:34:55'::timestamp AND ph.CreationDate>='2012-05-07 03:11:00'::timestamp AND u.DownVotes>=0 AND u.UpVotes>=0;

-- train q146
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND p.Id = pl.PostId AND p.Id = ph.PostId AND c.CreationDate>='2012-08-31 10:10:28'::timestamp AND p.Score>=0 AND p.CommentCount<=4 AND p.CreationDate>='2012-06-19 22:27:44'::timestamp AND p.CreationDate<='2013-10-20 19:44:22'::timestamp AND pl.CreationDate<='2013-04-04 23:36:21'::timestamp AND ph.CreationDate>='2011-06-11 20:40:55'::timestamp AND u.DownVotes>=0 AND u.UpVotes>=0;

-- train q146
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND p.Id = pl.PostId AND p.Id = ph.PostId AND c.CreationDate>='2012-06-25 15:50:24'::timestamp AND p.Score>=0 AND p.CommentCount<=1 AND p.CreationDate>='2010-11-26 16:46:09'::timestamp AND p.CreationDate<='2013-04-21 19:38:59'::timestamp AND pl.CreationDate<='2014-09-13 20:54:31'::timestamp AND ph.CreationDate>='2012-10-11 15:16:49'::timestamp AND u.DownVotes>=0 AND u.UpVotes>=0;

-- train q146
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND p.Id = pl.PostId AND p.Id = ph.PostId AND c.CreationDate>='2012-08-31 10:10:28'::timestamp AND p.Score>=2 AND p.CommentCount<=45 AND p.CreationDate>='2013-02-15 03:31:21'::timestamp AND p.CreationDate<='2014-09-14 02:09:23'::timestamp AND pl.CreationDate<='2014-06-25 13:05:06'::timestamp AND ph.CreationDate>='2012-05-07 03:11:00'::timestamp AND u.DownVotes>=0 AND u.UpVotes>=0;

-- train q26
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, users as u WHERE p.Id = c.PostId AND p.Id = pl.RelatedPostId AND p.OwnerUserId = u.Id AND c.CreationDate>='2013-06-17 03:23:06'::timestamp AND c.CreationDate<='2013-06-17 03:23:06'::timestamp AND u.UpVotes>=0 AND u.CreationDate>='2011-12-27 18:54:39'::timestamp;

-- train q26
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, users as u WHERE p.Id = c.PostId AND p.Id = pl.RelatedPostId AND p.OwnerUserId = u.Id AND c.CreationDate>='2012-06-25 15:50:24'::timestamp AND c.CreationDate<='2014-04-26 01:11:26'::timestamp AND u.UpVotes>=0 AND u.CreationDate>='2010-07-19 06:55:26'::timestamp;

-- train q26
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, users as u WHERE p.Id = c.PostId AND p.Id = pl.RelatedPostId AND p.OwnerUserId = u.Id AND c.CreationDate>='2011-05-30 17:17:48'::timestamp AND c.CreationDate<='2014-09-14 02:04:27'::timestamp AND u.UpVotes>=0 AND u.CreationDate>='2013-01-16 20:29:10'::timestamp;

-- train q26
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, users as u WHERE p.Id = c.PostId AND p.Id = pl.RelatedPostId AND p.OwnerUserId = u.Id AND c.CreationDate>='2009-02-02 14:45:19'::timestamp AND c.CreationDate<='2014-07-29 16:38:44'::timestamp AND u.UpVotes>=0 AND u.CreationDate>='2013-05-14 19:05:54'::timestamp;

-- train q26
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, users as u WHERE p.Id = c.PostId AND p.Id = pl.RelatedPostId AND p.OwnerUserId = u.Id AND c.CreationDate>='2012-11-12 19:23:15'::timestamp AND c.CreationDate<='2013-04-12 14:14:02'::timestamp AND u.UpVotes>=0 AND u.CreationDate>='2012-10-29 20:13:00'::timestamp;

-- train q26
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, users as u WHERE p.Id = c.PostId AND p.Id = pl.RelatedPostId AND p.OwnerUserId = u.Id AND c.CreationDate>='2011-10-04 13:23:07'::timestamp AND c.CreationDate<='2014-07-29 16:38:44'::timestamp AND u.UpVotes>=0 AND u.CreationDate>='2013-09-19 17:00:57'::timestamp;

-- train q26
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, users as u WHERE p.Id = c.PostId AND p.Id = pl.RelatedPostId AND p.OwnerUserId = u.Id AND c.CreationDate>='2011-10-04 13:23:07'::timestamp AND c.CreationDate<='2014-09-14 02:04:27'::timestamp AND u.UpVotes>=0 AND u.CreationDate>='2010-07-19 06:55:26'::timestamp;

-- train q26
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, users as u WHERE p.Id = c.PostId AND p.Id = pl.RelatedPostId AND p.OwnerUserId = u.Id AND c.CreationDate>='2013-04-12 14:14:02'::timestamp AND c.CreationDate<='2013-06-17 03:23:06'::timestamp AND u.UpVotes>=0 AND u.CreationDate>='2011-08-15 19:36:17'::timestamp;

-- train q26
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, users as u WHERE p.Id = c.PostId AND p.Id = pl.RelatedPostId AND p.OwnerUserId = u.Id AND c.CreationDate>='2011-10-04 13:23:07'::timestamp AND c.CreationDate<='2014-04-26 01:11:26'::timestamp AND u.UpVotes>=0 AND u.CreationDate>='2013-11-15 10:35:16'::timestamp;

-- train q26
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, users as u WHERE p.Id = c.PostId AND p.Id = pl.RelatedPostId AND p.OwnerUserId = u.Id AND c.CreationDate>='2012-06-25 15:50:24'::timestamp AND c.CreationDate<='2013-10-08 14:15:52'::timestamp AND u.UpVotes>=0 AND u.CreationDate>='2013-05-14 19:05:54'::timestamp;

-- train q20
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u WHERE ph.PostId = p.Id AND p.OwnerUserId = u.Id AND ph.CreationDate<='2014-07-31 22:42:34'::timestamp AND p.CreationDate>='2013-04-21 19:38:59'::timestamp AND p.CreationDate<='2014-09-14 02:09:23'::timestamp AND u.Reputation>=26 AND u.Reputation<=11 AND u.Views>=0;

-- train q20
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u WHERE ph.PostId = p.Id AND p.OwnerUserId = u.Id AND ph.CreationDate<='2014-06-18 13:33:16'::timestamp AND p.CreationDate>='2013-04-21 19:38:59'::timestamp AND p.CreationDate<='2013-02-15 03:31:21'::timestamp AND u.Reputation>=1 AND u.Reputation<=101 AND u.Views>=0;

-- train q20
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u WHERE ph.PostId = p.Id AND p.OwnerUserId = u.Id AND ph.CreationDate<='2012-12-27 00:14:56'::timestamp AND p.CreationDate>='2012-06-19 22:27:44'::timestamp AND p.CreationDate<='2013-12-14 13:32:25'::timestamp AND u.Reputation>=1 AND u.Reputation<=6 AND u.Views>=0;

-- train q20
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u WHERE ph.PostId = p.Id AND p.OwnerUserId = u.Id AND ph.CreationDate<='2013-10-28 01:49:50'::timestamp AND p.CreationDate>='2012-09-11 15:29:12'::timestamp AND p.CreationDate<='2014-05-06 17:16:49'::timestamp AND u.Reputation>=1 AND u.Reputation<=101 AND u.Views>=0;

-- train q20
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u WHERE ph.PostId = p.Id AND p.OwnerUserId = u.Id AND ph.CreationDate<='2013-07-12 12:21:54'::timestamp AND p.CreationDate>='2012-03-28 16:24:28'::timestamp AND p.CreationDate<='2014-06-18 20:06:36'::timestamp AND u.Reputation>=11 AND u.Reputation<=51 AND u.Views>=0;

-- train q20
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u WHERE ph.PostId = p.Id AND p.OwnerUserId = u.Id AND ph.CreationDate<='2013-12-18 19:08:02'::timestamp AND p.CreationDate>='2010-11-26 16:46:09'::timestamp AND p.CreationDate<='2014-06-18 20:06:36'::timestamp AND u.Reputation>=6 AND u.Reputation<=87393 AND u.Views>=0;

-- train q20
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u WHERE ph.PostId = p.Id AND p.OwnerUserId = u.Id AND ph.CreationDate<='2013-12-18 19:08:02'::timestamp AND p.CreationDate>='2010-11-26 16:46:09'::timestamp AND p.CreationDate<='2013-06-23 12:50:00'::timestamp AND u.Reputation>=6 AND u.Reputation<=16 AND u.Views>=0;

-- train q20
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u WHERE ph.PostId = p.Id AND p.OwnerUserId = u.Id AND ph.CreationDate<='2013-07-12 12:21:54'::timestamp AND p.CreationDate>='2010-11-26 16:46:09'::timestamp AND p.CreationDate<='2013-02-15 03:31:21'::timestamp AND u.Reputation>=1 AND u.Reputation<=26 AND u.Views>=1;

-- train q20
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u WHERE ph.PostId = p.Id AND p.OwnerUserId = u.Id AND ph.CreationDate<='2014-09-14 02:54:13'::timestamp AND p.CreationDate>='2011-12-23 07:35:55'::timestamp AND p.CreationDate<='2013-08-20 10:19:52'::timestamp AND u.Reputation>=16 AND u.Reputation<=26 AND u.Views>=0;

-- train q20
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u WHERE ph.PostId = p.Id AND p.OwnerUserId = u.Id AND ph.CreationDate<='2014-05-06 12:40:26'::timestamp AND p.CreationDate>='2010-11-26 16:46:09'::timestamp AND p.CreationDate<='2012-11-26 23:54:07'::timestamp AND u.Reputation>=1 AND u.Reputation<=163 AND u.Views>=0;

-- train q92
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u WHERE ph.PostId = p.Id AND p.OwnerUserId = u.Id AND p.CreationDate>='2012-06-19 22:27:44'::timestamp AND p.CreationDate<='2014-03-25 12:00:30'::timestamp AND u.UpVotes>=0 AND u.UpVotes<=1;

-- train q92
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u WHERE ph.PostId = p.Id AND p.OwnerUserId = u.Id AND p.CreationDate>='2012-11-26 23:54:07'::timestamp AND p.CreationDate<='2014-09-14 02:09:23'::timestamp AND u.UpVotes>=0 AND u.UpVotes<=0;

-- train q92
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u WHERE ph.PostId = p.Id AND p.OwnerUserId = u.Id AND p.CreationDate>='2013-08-20 10:19:52'::timestamp AND p.CreationDate<='2014-09-14 02:09:23'::timestamp AND u.UpVotes>=0 AND u.UpVotes<=11442;

-- train q92
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u WHERE ph.PostId = p.Id AND p.OwnerUserId = u.Id AND p.CreationDate>='2013-08-20 10:19:52'::timestamp AND p.CreationDate<='2013-02-15 03:31:21'::timestamp AND u.UpVotes>=0 AND u.UpVotes<=1;

-- train q92
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u WHERE ph.PostId = p.Id AND p.OwnerUserId = u.Id AND p.CreationDate>='2012-09-11 15:29:12'::timestamp AND p.CreationDate<='2013-04-21 19:38:59'::timestamp AND u.UpVotes>=0 AND u.UpVotes<=2;

-- train q92
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u WHERE ph.PostId = p.Id AND p.OwnerUserId = u.Id AND p.CreationDate>='2013-04-21 19:38:59'::timestamp AND p.CreationDate<='2014-06-18 20:06:36'::timestamp AND u.UpVotes>=0 AND u.UpVotes<=0;

-- train q92
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u WHERE ph.PostId = p.Id AND p.OwnerUserId = u.Id AND p.CreationDate>='2009-02-02 14:21:12'::timestamp AND p.CreationDate<='2012-11-26 23:54:07'::timestamp AND u.UpVotes>=0 AND u.UpVotes<=3;

-- train q92
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u WHERE ph.PostId = p.Id AND p.OwnerUserId = u.Id AND p.CreationDate>='2013-08-20 10:19:52'::timestamp AND p.CreationDate<='2013-04-21 19:38:59'::timestamp AND u.UpVotes>=0 AND u.UpVotes<=0;

-- train q92
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u WHERE ph.PostId = p.Id AND p.OwnerUserId = u.Id AND p.CreationDate>='2013-02-15 03:31:21'::timestamp AND p.CreationDate<='2014-05-06 17:16:49'::timestamp AND u.UpVotes>=0 AND u.UpVotes<=9;

-- train q92
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u WHERE ph.PostId = p.Id AND p.OwnerUserId = u.Id AND p.CreationDate>='2012-06-19 22:27:44'::timestamp AND p.CreationDate<='2014-02-08 20:51:44'::timestamp AND u.UpVotes>=0 AND u.UpVotes<=3;

-- train q119
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE b.UserId = u.Id AND p.OwnerUserId = u.Id AND ph.UserId = u.Id AND ph.PostHistoryTypeId=3 AND p.ViewCount>=28 AND p.ViewCount<=2147 AND u.Reputation>=26 AND u.Reputation<=101 AND b.Date>='2013-06-02 08:30:40'::timestamp;

-- train q119
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE b.UserId = u.Id AND p.OwnerUserId = u.Id AND ph.UserId = u.Id AND ph.PostHistoryTypeId=4 AND p.ViewCount>=185 AND p.ViewCount<=288 AND u.Reputation>=1 AND u.Reputation<=6 AND b.Date>='2012-09-06 10:09:57'::timestamp;

-- train q119
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE b.UserId = u.Id AND p.OwnerUserId = u.Id AND ph.UserId = u.Id AND ph.PostHistoryTypeId=5 AND p.ViewCount>=28 AND p.ViewCount<=185 AND u.Reputation>=26 AND u.Reputation<=26 AND b.Date>='2011-11-17 23:53:38'::timestamp;

-- train q119
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE b.UserId = u.Id AND p.OwnerUserId = u.Id AND ph.UserId = u.Id AND ph.PostHistoryTypeId=1 AND p.ViewCount>=107 AND p.ViewCount<=90 AND u.Reputation>=1 AND u.Reputation<=163 AND b.Date>='2012-03-11 12:03:02'::timestamp;

-- train q119
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE b.UserId = u.Id AND p.OwnerUserId = u.Id AND ph.UserId = u.Id AND ph.PostHistoryTypeId=1 AND p.ViewCount>=153 AND p.ViewCount<=2147 AND u.Reputation>=1 AND u.Reputation<=116 AND b.Date>='2013-06-02 08:30:40'::timestamp;

-- train q119
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE b.UserId = u.Id AND p.OwnerUserId = u.Id AND ph.UserId = u.Id AND ph.PostHistoryTypeId=3 AND p.ViewCount>=36 AND p.ViewCount<=128 AND u.Reputation>=1 AND u.Reputation<=6 AND b.Date>='2013-09-24 02:09:52'::timestamp;

-- train q119
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE b.UserId = u.Id AND p.OwnerUserId = u.Id AND ph.UserId = u.Id AND ph.PostHistoryTypeId=4 AND p.ViewCount>=90 AND p.ViewCount<=2147 AND u.Reputation>=1 AND u.Reputation<=101 AND b.Date>='2011-01-04 03:20:19'::timestamp;

-- train q119
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE b.UserId = u.Id AND p.OwnerUserId = u.Id AND ph.UserId = u.Id AND ph.PostHistoryTypeId=2 AND p.ViewCount>=36 AND p.ViewCount<=153 AND u.Reputation>=1 AND u.Reputation<=101 AND b.Date>='2012-11-18 13:52:29'::timestamp;

-- train q119
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE b.UserId = u.Id AND p.OwnerUserId = u.Id AND ph.UserId = u.Id AND ph.PostHistoryTypeId=4 AND p.ViewCount>=107 AND p.ViewCount<=703 AND u.Reputation>=6 AND u.Reputation<=11 AND b.Date>='2013-09-24 02:09:52'::timestamp;

-- train q119
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE b.UserId = u.Id AND p.OwnerUserId = u.Id AND ph.UserId = u.Id AND ph.PostHistoryTypeId=2 AND p.ViewCount>=1 AND p.ViewCount<=128 AND u.Reputation>=16 AND u.Reputation<=6 AND b.Date>='2011-07-01 17:18:21'::timestamp;

-- train q121
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE b.UserId = u.Id AND p.OwnerUserId = u.Id AND ph.UserId = u.Id AND ph.PostHistoryTypeId=2 AND ph.CreationDate>='2011-10-14 08:09:28'::timestamp AND ph.CreationDate<='2014-05-06 12:40:26'::timestamp AND p.AnswerCount<=1 AND p.CommentCount>=1 AND p.CommentCount<=1 AND p.FavoriteCount>=1 AND p.FavoriteCount<=233 AND p.CreationDate<='2014-03-25 12:00:30'::timestamp AND u.Reputation<=11 AND u.CreationDate>='2012-07-30 02:08:59'::timestamp AND u.CreationDate<='2013-11-15 10:35:16'::timestamp AND b.Date>='2011-07-01 17:18:21'::timestamp AND b.Date<='2014-06-30 18:24:30'::timestamp;

-- train q121
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE b.UserId = u.Id AND p.OwnerUserId = u.Id AND ph.UserId = u.Id AND ph.PostHistoryTypeId=1 AND ph.CreationDate>='2009-02-02 14:21:12'::timestamp AND ph.CreationDate<='2014-06-18 13:33:16'::timestamp AND p.AnswerCount<=3 AND p.CommentCount>=2 AND p.CommentCount<=4 AND p.FavoriteCount>=1 AND p.FavoriteCount<=233 AND p.CreationDate<='2013-04-21 19:38:59'::timestamp AND u.Reputation<=6 AND u.CreationDate>='2013-01-16 20:29:10'::timestamp AND u.CreationDate<='2014-02-16 15:09:45'::timestamp AND b.Date>='2013-09-24 02:09:52'::timestamp AND b.Date<='2013-11-18 20:16:10'::timestamp;

-- train q121
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE b.UserId = u.Id AND p.OwnerUserId = u.Id AND ph.UserId = u.Id AND ph.PostHistoryTypeId=1 AND ph.CreationDate>='2013-03-15 11:56:33'::timestamp AND ph.CreationDate<='2013-07-12 12:21:54'::timestamp AND p.AnswerCount<=1 AND p.CommentCount>=0 AND p.CommentCount<=1 AND p.FavoriteCount>=1 AND p.FavoriteCount<=233 AND p.CreationDate<='2013-02-15 03:31:21'::timestamp AND u.Reputation<=51 AND u.CreationDate>='2011-12-27 18:54:39'::timestamp AND u.CreationDate<='2014-07-07 19:57:03'::timestamp AND b.Date>='2012-09-06 10:09:57'::timestamp AND b.Date<='2014-04-07 18:51:36'::timestamp;

-- train q121
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE b.UserId = u.Id AND p.OwnerUserId = u.Id AND ph.UserId = u.Id AND ph.PostHistoryTypeId=1 AND ph.CreationDate>='2011-06-11 20:40:55'::timestamp AND ph.CreationDate<='2014-06-18 13:33:16'::timestamp AND p.AnswerCount<=1 AND p.CommentCount>=0 AND p.CommentCount<=2 AND p.FavoriteCount>=0 AND p.FavoriteCount<=1 AND p.CreationDate<='2013-10-20 19:44:22'::timestamp AND u.Reputation<=101 AND u.CreationDate>='2012-07-30 02:08:59'::timestamp AND u.CreationDate<='2014-01-10 18:05:07'::timestamp AND b.Date>='2013-07-29 16:38:09'::timestamp AND b.Date<='2014-05-19 14:48:50'::timestamp;

-- train q121
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE b.UserId = u.Id AND p.OwnerUserId = u.Id AND ph.UserId = u.Id AND ph.PostHistoryTypeId=4 AND ph.CreationDate>='2009-02-02 14:21:12'::timestamp AND ph.CreationDate<='2014-03-25 00:33:01'::timestamp AND p.AnswerCount<=1 AND p.CommentCount>=0 AND p.CommentCount<=7 AND p.FavoriteCount>=1 AND p.FavoriteCount<=2 AND p.CreationDate<='2014-05-06 17:16:49'::timestamp AND u.Reputation<=6 AND u.CreationDate>='2013-03-18 20:44:37'::timestamp AND u.CreationDate<='2013-07-15 02:31:25'::timestamp AND b.Date>='2011-11-17 23:53:38'::timestamp AND b.Date<='2014-09-14 02:31:28'::timestamp;

-- train q121
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE b.UserId = u.Id AND p.OwnerUserId = u.Id AND ph.UserId = u.Id AND ph.PostHistoryTypeId=5 AND ph.CreationDate>='2009-02-02 14:21:12'::timestamp AND ph.CreationDate<='2013-07-12 12:21:54'::timestamp AND p.AnswerCount<=3 AND p.CommentCount>=0 AND p.CommentCount<=1 AND p.FavoriteCount>=1 AND p.FavoriteCount<=3 AND p.CreationDate<='2014-05-06 17:16:49'::timestamp AND u.Reputation<=116 AND u.CreationDate>='2011-02-11 21:16:21'::timestamp AND u.CreationDate<='2014-09-14 01:01:44'::timestamp AND b.Date>='2010-07-19 19:39:07'::timestamp AND b.Date<='2013-06-02 08:30:40'::timestamp;

-- train q121
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE b.UserId = u.Id AND p.OwnerUserId = u.Id AND ph.UserId = u.Id AND ph.PostHistoryTypeId=5 AND ph.CreationDate>='2011-10-14 08:09:28'::timestamp AND ph.CreationDate<='2014-05-06 12:40:26'::timestamp AND p.AnswerCount<=1 AND p.CommentCount>=1 AND p.CommentCount<=1 AND p.FavoriteCount>=0 AND p.FavoriteCount<=233 AND p.CreationDate<='2014-09-14 02:09:23'::timestamp AND u.Reputation<=51 AND u.CreationDate>='2012-10-29 20:13:00'::timestamp AND u.CreationDate<='2014-07-07 19:57:03'::timestamp AND b.Date>='2012-09-06 10:09:57'::timestamp AND b.Date<='2014-09-14 02:31:28'::timestamp;

-- train q121
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE b.UserId = u.Id AND p.OwnerUserId = u.Id AND ph.UserId = u.Id AND ph.PostHistoryTypeId=2 AND ph.CreationDate>='2012-05-07 03:11:00'::timestamp AND ph.CreationDate<='2014-09-14 02:54:13'::timestamp AND p.AnswerCount<=3 AND p.CommentCount>=1 AND p.CommentCount<=3 AND p.FavoriteCount>=2 AND p.FavoriteCount<=2 AND p.CreationDate<='2013-10-20 19:44:22'::timestamp AND u.Reputation<=26 AND u.CreationDate>='2013-05-14 19:05:54'::timestamp AND u.CreationDate<='2013-11-15 10:35:16'::timestamp AND b.Date>='2013-06-02 08:30:40'::timestamp AND b.Date<='2014-01-14 22:31:05'::timestamp;

-- train q121
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE b.UserId = u.Id AND p.OwnerUserId = u.Id AND ph.UserId = u.Id AND ph.PostHistoryTypeId=1 AND ph.CreationDate>='2012-07-25 23:18:13'::timestamp AND ph.CreationDate<='2014-02-11 05:09:03'::timestamp AND p.AnswerCount<=136 AND p.CommentCount>=0 AND p.CommentCount<=2 AND p.FavoriteCount>=1 AND p.FavoriteCount<=8 AND p.CreationDate<='2014-02-08 20:51:44'::timestamp AND u.Reputation<=101 AND u.CreationDate>='2011-12-27 18:54:39'::timestamp AND u.CreationDate<='2014-08-08 12:49:32'::timestamp AND b.Date>='2012-11-18 13:52:29'::timestamp AND b.Date<='2013-06-02 08:30:40'::timestamp;

-- train q121
SELECT COUNT(*) FROM postHistory as ph, posts as p, users as u, badges as b WHERE b.UserId = u.Id AND p.OwnerUserId = u.Id AND ph.UserId = u.Id AND ph.PostHistoryTypeId=1 AND ph.CreationDate>='2012-07-25 23:18:13'::timestamp AND ph.CreationDate<='2014-09-14 02:54:13'::timestamp AND p.AnswerCount<=1 AND p.CommentCount>=0 AND p.CommentCount<=5 AND p.FavoriteCount>=0 AND p.FavoriteCount<=1 AND p.CreationDate<='2014-06-18 20:06:36'::timestamp AND u.Reputation<=101 AND u.CreationDate>='2013-05-14 19:05:54'::timestamp AND u.CreationDate<='2014-09-14 01:01:44'::timestamp AND b.Date>='2012-11-18 13:52:29'::timestamp AND b.Date<='2013-11-18 20:16:10'::timestamp;

-- train q82
SELECT COUNT(*) FROM comments as c, postHistory as ph, users as u WHERE c.UserId = u.Id AND ph.UserId = u.Id AND c.CreationDate>='2011-05-30 17:17:48'::timestamp AND c.CreationDate<='2014-09-14 02:04:27'::timestamp AND u.Reputation>=1 AND u.Views<=4 AND u.UpVotes>=0 AND u.CreationDate>='2013-11-15 10:35:16'::timestamp;

-- train q82
SELECT COUNT(*) FROM comments as c, postHistory as ph, users as u WHERE c.UserId = u.Id AND ph.UserId = u.Id AND c.CreationDate>='2011-05-30 17:17:48'::timestamp AND c.CreationDate<='2014-09-14 02:04:27'::timestamp AND u.Reputation>=1 AND u.Views<=8 AND u.UpVotes>=0 AND u.CreationDate>='2011-08-15 19:36:17'::timestamp;

-- train q82
SELECT COUNT(*) FROM comments as c, postHistory as ph, users as u WHERE c.UserId = u.Id AND ph.UserId = u.Id AND c.CreationDate>='2013-08-10 10:42:04'::timestamp AND c.CreationDate<='2012-11-12 19:23:15'::timestamp AND u.Reputation>=1 AND u.Views<=3 AND u.UpVotes>=0 AND u.CreationDate>='2011-02-11 21:16:21'::timestamp;

-- train q82
SELECT COUNT(*) FROM comments as c, postHistory as ph, users as u WHERE c.UserId = u.Id AND ph.UserId = u.Id AND c.CreationDate>='2011-10-04 13:23:07'::timestamp AND c.CreationDate<='2013-08-10 10:42:04'::timestamp AND u.Reputation>=6 AND u.Views<=0 AND u.UpVotes>=0 AND u.CreationDate>='2013-01-16 20:29:10'::timestamp;

-- train q82
SELECT COUNT(*) FROM comments as c, postHistory as ph, users as u WHERE c.UserId = u.Id AND ph.UserId = u.Id AND c.CreationDate>='2011-01-19 20:40:12'::timestamp AND c.CreationDate<='2014-06-12 00:55:46'::timestamp AND u.Reputation>=1 AND u.Views<=20932 AND u.UpVotes>=0 AND u.CreationDate>='2013-11-15 10:35:16'::timestamp;

-- train q82
SELECT COUNT(*) FROM comments as c, postHistory as ph, users as u WHERE c.UserId = u.Id AND ph.UserId = u.Id AND c.CreationDate>='2012-08-31 10:10:28'::timestamp AND c.CreationDate<='2013-02-02 20:12:58'::timestamp AND u.Reputation>=1 AND u.Views<=0 AND u.UpVotes>=0 AND u.CreationDate>='2011-12-27 18:54:39'::timestamp;

-- train q82
SELECT COUNT(*) FROM comments as c, postHistory as ph, users as u WHERE c.UserId = u.Id AND ph.UserId = u.Id AND c.CreationDate>='2012-04-20 08:35:37'::timestamp AND c.CreationDate<='2014-07-29 16:38:44'::timestamp AND u.Reputation>=1 AND u.Views<=0 AND u.UpVotes>=0 AND u.CreationDate>='2012-07-30 02:08:59'::timestamp;

-- train q82
SELECT COUNT(*) FROM comments as c, postHistory as ph, users as u WHERE c.UserId = u.Id AND ph.UserId = u.Id AND c.CreationDate>='2012-11-12 19:23:15'::timestamp AND c.CreationDate<='2014-09-14 02:04:27'::timestamp AND u.Reputation>=16 AND u.Views<=3 AND u.UpVotes>=0 AND u.CreationDate>='2012-10-29 20:13:00'::timestamp;

-- train q82
SELECT COUNT(*) FROM comments as c, postHistory as ph, users as u WHERE c.UserId = u.Id AND ph.UserId = u.Id AND c.CreationDate>='2012-01-28 20:58:07'::timestamp AND c.CreationDate<='2014-06-12 00:55:46'::timestamp AND u.Reputation>=1 AND u.Views<=8 AND u.UpVotes>=0 AND u.CreationDate>='2010-07-19 06:55:26'::timestamp;

-- train q82
SELECT COUNT(*) FROM comments as c, postHistory as ph, users as u WHERE c.UserId = u.Id AND ph.UserId = u.Id AND c.CreationDate>='2012-04-20 08:35:37'::timestamp AND c.CreationDate<='2013-08-10 10:42:04'::timestamp AND u.Reputation>=1 AND u.Views<=18 AND u.UpVotes>=0 AND u.CreationDate>='2012-04-12 19:30:57'::timestamp;

-- train q83
SELECT COUNT(*) FROM comments as c, postHistory as ph, users as u WHERE c.UserId = u.Id AND ph.UserId = u.Id AND u.Reputation>=1 AND u.Reputation<=101 AND u.Views<=0 AND u.DownVotes>=0 AND u.CreationDate<='2014-02-16 15:09:45'::timestamp;

-- train q83
SELECT COUNT(*) FROM comments as c, postHistory as ph, users as u WHERE c.UserId = u.Id AND ph.UserId = u.Id AND u.Reputation>=1 AND u.Reputation<=6 AND u.Views<=8 AND u.DownVotes>=0 AND u.CreationDate<='2013-09-19 17:00:57'::timestamp;

-- train q83
SELECT COUNT(*) FROM comments as c, postHistory as ph, users as u WHERE c.UserId = u.Id AND ph.UserId = u.Id AND u.Reputation>=1 AND u.Reputation<=26 AND u.Views<=0 AND u.DownVotes>=0 AND u.CreationDate<='2014-02-16 15:09:45'::timestamp;

-- train q83
SELECT COUNT(*) FROM comments as c, postHistory as ph, users as u WHERE c.UserId = u.Id AND ph.UserId = u.Id AND u.Reputation>=1 AND u.Reputation<=51 AND u.Views<=1 AND u.DownVotes>=0 AND u.CreationDate<='2014-09-14 01:01:44'::timestamp;

-- train q83
SELECT COUNT(*) FROM comments as c, postHistory as ph, users as u WHERE c.UserId = u.Id AND ph.UserId = u.Id AND u.Reputation>=1 AND u.Reputation<=101 AND u.Views<=20932 AND u.DownVotes>=0 AND u.CreationDate<='2014-08-08 12:49:32'::timestamp;

-- train q83
SELECT COUNT(*) FROM comments as c, postHistory as ph, users as u WHERE c.UserId = u.Id AND ph.UserId = u.Id AND u.Reputation>=1 AND u.Reputation<=101 AND u.Views<=0 AND u.DownVotes>=0 AND u.CreationDate<='2014-09-14 01:01:44'::timestamp;

-- train q83
SELECT COUNT(*) FROM comments as c, postHistory as ph, users as u WHERE c.UserId = u.Id AND ph.UserId = u.Id AND u.Reputation>=1 AND u.Reputation<=6 AND u.Views<=20932 AND u.DownVotes>=0 AND u.CreationDate<='2014-01-10 18:05:07'::timestamp;

-- train q83
SELECT COUNT(*) FROM comments as c, postHistory as ph, users as u WHERE c.UserId = u.Id AND ph.UserId = u.Id AND u.Reputation>=1 AND u.Reputation<=101 AND u.Views<=18 AND u.DownVotes>=0 AND u.CreationDate<='2014-04-28 14:38:07'::timestamp;

-- train q83
SELECT COUNT(*) FROM comments as c, postHistory as ph, users as u WHERE c.UserId = u.Id AND ph.UserId = u.Id AND u.Reputation>=1 AND u.Reputation<=101 AND u.Views<=3 AND u.DownVotes>=0 AND u.CreationDate<='2014-06-03 08:58:38'::timestamp;

-- train q83
SELECT COUNT(*) FROM comments as c, postHistory as ph, users as u WHERE c.UserId = u.Id AND ph.UserId = u.Id AND u.Reputation>=1 AND u.Reputation<=101 AND u.Views<=1 AND u.DownVotes>=0 AND u.CreationDate<='2014-09-14 01:01:44'::timestamp;

-- train q133
SELECT COUNT(*) FROM comments as c, postHistory as ph, badges as b, votes as v, users as u WHERE ph.UserId = u.Id AND v.UserId = u.Id AND c.UserId = u.Id AND b.UserId = u.Id AND b.Date>='2012-11-18 13:52:29'::timestamp AND v.BountyAmount>=50 AND v.CreationDate>='2012-07-08 00:00:00'::timestamp AND v.CreationDate<='2013-08-24 00:00:00'::timestamp AND u.DownVotes>=0 AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.UpVotes<=11442 AND u.CreationDate<='2014-01-10 18:05:07'::timestamp;

-- train q133
SELECT COUNT(*) FROM comments as c, postHistory as ph, badges as b, votes as v, users as u WHERE ph.UserId = u.Id AND v.UserId = u.Id AND c.UserId = u.Id AND b.UserId = u.Id AND b.Date>='2013-01-30 21:16:06'::timestamp AND v.BountyAmount>=50 AND v.CreationDate>='2012-11-26 00:00:00'::timestamp AND v.CreationDate<='2014-07-22 00:00:00'::timestamp AND u.DownVotes>=0 AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.UpVotes<=0 AND u.CreationDate<='2013-11-15 10:35:16'::timestamp;

-- train q133
SELECT COUNT(*) FROM comments as c, postHistory as ph, badges as b, votes as v, users as u WHERE ph.UserId = u.Id AND v.UserId = u.Id AND c.UserId = u.Id AND b.UserId = u.Id AND b.Date>='2013-06-02 08:30:40'::timestamp AND v.BountyAmount>=50 AND v.CreationDate>='2011-10-26 00:00:00'::timestamp AND v.CreationDate<='2013-04-26 00:00:00'::timestamp AND u.DownVotes>=0 AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.UpVotes<=0 AND u.CreationDate<='2013-07-15 02:31:25'::timestamp;

-- train q133
SELECT COUNT(*) FROM comments as c, postHistory as ph, badges as b, votes as v, users as u WHERE ph.UserId = u.Id AND v.UserId = u.Id AND c.UserId = u.Id AND b.UserId = u.Id AND b.Date>='2011-01-04 03:20:19'::timestamp AND v.BountyAmount>=50 AND v.CreationDate>='2011-03-18 00:00:00'::timestamp AND v.CreationDate<='2012-09-14 00:00:00'::timestamp AND u.DownVotes>=0 AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.UpVotes<=3 AND u.CreationDate<='2014-06-03 08:58:38'::timestamp;

-- train q133
SELECT COUNT(*) FROM comments as c, postHistory as ph, badges as b, votes as v, users as u WHERE ph.UserId = u.Id AND v.UserId = u.Id AND c.UserId = u.Id AND b.UserId = u.Id AND b.Date>='2011-07-01 17:18:21'::timestamp AND v.BountyAmount>=50 AND v.CreationDate>='2010-11-10 00:00:00'::timestamp AND v.CreationDate<='2014-04-08 00:00:00'::timestamp AND u.DownVotes>=0 AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.UpVotes<=9 AND u.CreationDate<='2014-01-10 18:05:07'::timestamp;

-- train q133
SELECT COUNT(*) FROM comments as c, postHistory as ph, badges as b, votes as v, users as u WHERE ph.UserId = u.Id AND v.UserId = u.Id AND c.UserId = u.Id AND b.UserId = u.Id AND b.Date>='2011-01-04 03:20:19'::timestamp AND v.BountyAmount>=50 AND v.CreationDate>='2011-10-26 00:00:00'::timestamp AND v.CreationDate<='2014-01-04 00:00:00'::timestamp AND u.DownVotes>=0 AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.UpVotes<=0 AND u.CreationDate<='2014-03-25 19:35:42'::timestamp;

-- train q133
SELECT COUNT(*) FROM comments as c, postHistory as ph, badges as b, votes as v, users as u WHERE ph.UserId = u.Id AND v.UserId = u.Id AND c.UserId = u.Id AND b.UserId = u.Id AND b.Date>='2011-11-17 23:53:38'::timestamp AND v.BountyAmount>=50 AND v.CreationDate>='2011-10-26 00:00:00'::timestamp AND v.CreationDate<='2012-11-26 00:00:00'::timestamp AND u.DownVotes>=0 AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.UpVotes<=9 AND u.CreationDate<='2013-11-15 10:35:16'::timestamp;

-- train q133
SELECT COUNT(*) FROM comments as c, postHistory as ph, badges as b, votes as v, users as u WHERE ph.UserId = u.Id AND v.UserId = u.Id AND c.UserId = u.Id AND b.UserId = u.Id AND b.Date>='2012-09-06 10:09:57'::timestamp AND v.BountyAmount>=50 AND v.CreationDate>='2012-09-14 00:00:00'::timestamp AND v.CreationDate<='2014-02-18 00:00:00'::timestamp AND u.DownVotes>=0 AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.UpVotes<=0 AND u.CreationDate<='2014-02-16 15:09:45'::timestamp;

-- train q133
SELECT COUNT(*) FROM comments as c, postHistory as ph, badges as b, votes as v, users as u WHERE ph.UserId = u.Id AND v.UserId = u.Id AND c.UserId = u.Id AND b.UserId = u.Id AND b.Date>='2012-09-06 10:09:57'::timestamp AND v.BountyAmount>=50 AND v.CreationDate>='2011-03-18 00:00:00'::timestamp AND v.CreationDate<='2014-01-04 00:00:00'::timestamp AND u.DownVotes>=0 AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.UpVotes<=0 AND u.CreationDate<='2014-02-16 15:09:45'::timestamp;

-- train q133
SELECT COUNT(*) FROM comments as c, postHistory as ph, badges as b, votes as v, users as u WHERE ph.UserId = u.Id AND v.UserId = u.Id AND c.UserId = u.Id AND b.UserId = u.Id AND b.Date>='2012-03-11 12:03:02'::timestamp AND v.BountyAmount>=50 AND v.CreationDate>='2012-07-08 00:00:00'::timestamp AND v.CreationDate<='2013-08-24 00:00:00'::timestamp AND u.DownVotes>=0 AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.UpVotes<=2 AND u.CreationDate<='2013-09-19 17:00:57'::timestamp;

-- train q37
SELECT COUNT(*) FROM comments as c, postLinks as pl, postHistory as ph, votes as v WHERE pl.PostId = c.PostId AND c.PostId = ph.PostId AND ph.PostId = v.PostId AND ph.CreationDate>='2013-05-17 08:19:48'::timestamp AND ph.CreationDate<='2014-05-06 12:40:26'::timestamp;

-- train q37
SELECT COUNT(*) FROM comments as c, postLinks as pl, postHistory as ph, votes as v WHERE pl.PostId = c.PostId AND c.PostId = ph.PostId AND ph.PostId = v.PostId AND ph.CreationDate>='2011-01-24 07:48:41'::timestamp AND ph.CreationDate<='2014-05-06 12:40:26'::timestamp;

-- train q37
SELECT COUNT(*) FROM comments as c, postLinks as pl, postHistory as ph, votes as v WHERE pl.PostId = c.PostId AND c.PostId = ph.PostId AND ph.PostId = v.PostId AND ph.CreationDate>='2013-05-17 08:19:48'::timestamp AND ph.CreationDate<='2014-07-31 22:42:34'::timestamp;

-- train q37
SELECT COUNT(*) FROM comments as c, postLinks as pl, postHistory as ph, votes as v WHERE pl.PostId = c.PostId AND c.PostId = ph.PostId AND ph.PostId = v.PostId AND ph.CreationDate>='2013-05-17 08:19:48'::timestamp AND ph.CreationDate<='2014-03-25 00:33:01'::timestamp;

-- train q37
SELECT COUNT(*) FROM comments as c, postLinks as pl, postHistory as ph, votes as v WHERE pl.PostId = c.PostId AND c.PostId = ph.PostId AND ph.PostId = v.PostId AND ph.CreationDate>='2009-02-02 14:21:12'::timestamp AND ph.CreationDate<='2014-06-18 13:33:16'::timestamp;

-- train q37
SELECT COUNT(*) FROM comments as c, postLinks as pl, postHistory as ph, votes as v WHERE pl.PostId = c.PostId AND c.PostId = ph.PostId AND ph.PostId = v.PostId AND ph.CreationDate>='2012-05-07 03:11:00'::timestamp AND ph.CreationDate<='2014-02-11 05:09:03'::timestamp;

-- train q37
SELECT COUNT(*) FROM comments as c, postLinks as pl, postHistory as ph, votes as v WHERE pl.PostId = c.PostId AND c.PostId = ph.PostId AND ph.PostId = v.PostId AND ph.CreationDate>='2012-12-27 00:14:56'::timestamp AND ph.CreationDate<='2014-09-14 02:54:13'::timestamp;

-- train q37
SELECT COUNT(*) FROM comments as c, postLinks as pl, postHistory as ph, votes as v WHERE pl.PostId = c.PostId AND c.PostId = ph.PostId AND ph.PostId = v.PostId AND ph.CreationDate>='2011-01-24 07:48:41'::timestamp AND ph.CreationDate<='2014-06-18 13:33:16'::timestamp;

-- train q37
SELECT COUNT(*) FROM comments as c, postLinks as pl, postHistory as ph, votes as v WHERE pl.PostId = c.PostId AND c.PostId = ph.PostId AND ph.PostId = v.PostId AND ph.CreationDate>='2012-10-11 15:16:49'::timestamp AND ph.CreationDate<='2014-05-06 12:40:26'::timestamp;

-- train q37
SELECT COUNT(*) FROM comments as c, postLinks as pl, postHistory as ph, votes as v WHERE pl.PostId = c.PostId AND c.PostId = ph.PostId AND ph.PostId = v.PostId AND ph.CreationDate>='2013-09-02 11:29:35'::timestamp AND ph.CreationDate<='2013-09-02 11:29:35'::timestamp;

-- train q22
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.UserId = p.OwnerUserId AND p.OwnerUserId = u.Id AND p.CommentCount>=0 AND p.CommentCount<=1 AND u.CreationDate>='2010-07-19 06:55:26'::timestamp AND u.CreationDate<='2014-06-03 08:58:38'::timestamp;

-- train q22
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.UserId = p.OwnerUserId AND p.OwnerUserId = u.Id AND p.CommentCount>=0 AND p.CommentCount<=1 AND u.CreationDate>='2013-09-19 17:00:57'::timestamp AND u.CreationDate<='2013-05-14 19:05:54'::timestamp;

-- train q22
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.UserId = p.OwnerUserId AND p.OwnerUserId = u.Id AND p.CommentCount>=1 AND p.CommentCount<=5 AND u.CreationDate>='2012-07-30 02:08:59'::timestamp AND u.CreationDate<='2014-09-14 01:01:44'::timestamp;

-- train q22
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.UserId = p.OwnerUserId AND p.OwnerUserId = u.Id AND p.CommentCount>=0 AND p.CommentCount<=1 AND u.CreationDate>='2012-04-12 19:30:57'::timestamp AND u.CreationDate<='2013-03-18 20:44:37'::timestamp;

-- train q22
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.UserId = p.OwnerUserId AND p.OwnerUserId = u.Id AND p.CommentCount>=0 AND p.CommentCount<=4 AND u.CreationDate>='2011-08-15 19:36:17'::timestamp AND u.CreationDate<='2014-04-28 14:38:07'::timestamp;

-- train q22
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.UserId = p.OwnerUserId AND p.OwnerUserId = u.Id AND p.CommentCount>=1 AND p.CommentCount<=7 AND u.CreationDate>='2012-10-29 20:13:00'::timestamp AND u.CreationDate<='2014-08-08 12:49:32'::timestamp;

-- train q22
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.UserId = p.OwnerUserId AND p.OwnerUserId = u.Id AND p.CommentCount>=0 AND p.CommentCount<=4 AND u.CreationDate>='2010-07-19 06:55:26'::timestamp AND u.CreationDate<='2014-06-03 08:58:38'::timestamp;

-- train q22
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.UserId = p.OwnerUserId AND p.OwnerUserId = u.Id AND p.CommentCount>=0 AND p.CommentCount<=2 AND u.CreationDate>='2012-10-29 20:13:00'::timestamp AND u.CreationDate<='2013-09-19 17:00:57'::timestamp;

-- train q22
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.UserId = p.OwnerUserId AND p.OwnerUserId = u.Id AND p.CommentCount>=0 AND p.CommentCount<=1 AND u.CreationDate>='2013-05-14 19:05:54'::timestamp AND u.CreationDate<='2014-04-28 14:38:07'::timestamp;

-- train q22
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.UserId = p.OwnerUserId AND p.OwnerUserId = u.Id AND p.CommentCount>=1 AND p.CommentCount<=45 AND u.CreationDate>='2013-05-14 19:05:54'::timestamp AND u.CreationDate<='2013-03-18 20:44:37'::timestamp;

-- train q5
SELECT COUNT(*) FROM comments as c, votes as v WHERE c.UserId = v.UserId AND c.Score=4;

-- train q5
SELECT COUNT(*) FROM comments as c, votes as v WHERE c.UserId = v.UserId AND c.Score=2;

-- train q5
SELECT COUNT(*) FROM comments as c, votes as v WHERE c.UserId = v.UserId AND c.Score=3;

-- train q5
SELECT COUNT(*) FROM comments as c, votes as v WHERE c.UserId = v.UserId AND c.Score=1;

-- train q32
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE u.Id = v.UserId AND v.UserId = ph.UserId AND ph.UserId =c.UserId AND c.CreationDate>='2011-01-19 20:40:12'::timestamp AND c.CreationDate<='2013-06-17 03:23:06'::timestamp AND ph.CreationDate>='2011-06-11 20:40:55'::timestamp AND ph.CreationDate<='2013-09-02 11:29:35'::timestamp AND v.CreationDate>='2013-04-26 00:00:00'::timestamp AND v.CreationDate<='2013-04-26 00:00:00'::timestamp AND u.Views>=0 AND u.Views<=0 AND u.DownVotes>=0 AND u.DownVotes<=0 AND u.UpVotes<=0;

-- train q32
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE u.Id = v.UserId AND v.UserId = ph.UserId AND ph.UserId =c.UserId AND c.CreationDate>='2012-04-20 08:35:37'::timestamp AND c.CreationDate<='2014-03-10 15:31:38'::timestamp AND ph.CreationDate>='2012-05-07 03:11:00'::timestamp AND ph.CreationDate<='2012-12-27 00:14:56'::timestamp AND v.CreationDate>='2012-09-14 00:00:00'::timestamp AND v.CreationDate<='2014-07-22 00:00:00'::timestamp AND u.Views>=0 AND u.Views<=1 AND u.DownVotes>=0 AND u.DownVotes<=0 AND u.UpVotes<=1;

-- train q32
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE u.Id = v.UserId AND v.UserId = ph.UserId AND ph.UserId =c.UserId AND c.CreationDate>='2013-08-10 10:42:04'::timestamp AND c.CreationDate<='2014-09-14 02:04:27'::timestamp AND ph.CreationDate>='2011-06-11 20:40:55'::timestamp AND ph.CreationDate<='2014-03-25 00:33:01'::timestamp AND v.CreationDate>='2009-02-02 00:00:00'::timestamp AND v.CreationDate<='2014-01-04 00:00:00'::timestamp AND u.Views>=1 AND u.Views<=20932 AND u.DownVotes>=0 AND u.DownVotes<=0 AND u.UpVotes<=0;

-- train q32
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE u.Id = v.UserId AND v.UserId = ph.UserId AND ph.UserId =c.UserId AND c.CreationDate>='2012-08-31 10:10:28'::timestamp AND c.CreationDate<='2013-02-02 20:12:58'::timestamp AND ph.CreationDate>='2012-05-07 03:11:00'::timestamp AND ph.CreationDate<='2013-03-15 11:56:33'::timestamp AND v.CreationDate>='2009-02-02 00:00:00'::timestamp AND v.CreationDate<='2014-02-18 00:00:00'::timestamp AND u.Views>=0 AND u.Views<=18 AND u.DownVotes>=0 AND u.DownVotes<=0 AND u.UpVotes<=0;

-- train q32
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE u.Id = v.UserId AND v.UserId = ph.UserId AND ph.UserId =c.UserId AND c.CreationDate>='2012-06-25 15:50:24'::timestamp AND c.CreationDate<='2012-11-12 19:23:15'::timestamp AND ph.CreationDate>='2009-02-02 14:21:12'::timestamp AND ph.CreationDate<='2014-03-25 00:33:01'::timestamp AND v.CreationDate>='2012-02-07 00:00:00'::timestamp AND v.CreationDate<='2013-04-26 00:00:00'::timestamp AND u.Views>=0 AND u.Views<=3 AND u.DownVotes>=0 AND u.DownVotes<=0 AND u.UpVotes<=0;

-- train q32
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE u.Id = v.UserId AND v.UserId = ph.UserId AND ph.UserId =c.UserId AND c.CreationDate>='2011-05-30 17:17:48'::timestamp AND c.CreationDate<='2014-03-10 15:31:38'::timestamp AND ph.CreationDate>='2013-07-12 12:21:54'::timestamp AND ph.CreationDate<='2013-09-02 11:29:35'::timestamp AND v.CreationDate>='2012-11-26 00:00:00'::timestamp AND v.CreationDate<='2014-04-08 00:00:00'::timestamp AND u.Views>=0 AND u.Views<=0 AND u.DownVotes>=0 AND u.DownVotes<=0 AND u.UpVotes<=2;

-- train q32
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE u.Id = v.UserId AND v.UserId = ph.UserId AND ph.UserId =c.UserId AND c.CreationDate>='2012-01-28 20:58:07'::timestamp AND c.CreationDate<='2013-06-17 03:23:06'::timestamp AND ph.CreationDate>='2012-02-07 13:46:01'::timestamp AND ph.CreationDate<='2013-10-28 01:49:50'::timestamp AND v.CreationDate>='2010-11-10 00:00:00'::timestamp AND v.CreationDate<='2013-04-26 00:00:00'::timestamp AND u.Views>=0 AND u.Views<=2 AND u.DownVotes>=0 AND u.DownVotes<=0 AND u.UpVotes<=9;

-- train q32
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE u.Id = v.UserId AND v.UserId = ph.UserId AND ph.UserId =c.UserId AND c.CreationDate>='2011-01-19 20:40:12'::timestamp AND c.CreationDate<='2014-09-14 02:04:27'::timestamp AND ph.CreationDate>='2012-10-11 15:16:49'::timestamp AND ph.CreationDate<='2012-12-27 00:14:56'::timestamp AND v.CreationDate>='2011-03-18 00:00:00'::timestamp AND v.CreationDate<='2014-01-04 00:00:00'::timestamp AND u.Views>=0 AND u.Views<=18 AND u.DownVotes>=0 AND u.DownVotes<=0 AND u.UpVotes<=0;

-- train q32
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE u.Id = v.UserId AND v.UserId = ph.UserId AND ph.UserId =c.UserId AND c.CreationDate>='2013-04-12 14:14:02'::timestamp AND c.CreationDate<='2013-08-10 10:42:04'::timestamp AND ph.CreationDate>='2009-02-02 14:21:12'::timestamp AND ph.CreationDate<='2014-03-25 00:33:01'::timestamp AND v.CreationDate>='2011-10-26 00:00:00'::timestamp AND v.CreationDate<='2013-02-12 00:00:00'::timestamp AND u.Views>=0 AND u.Views<=0 AND u.DownVotes>=0 AND u.DownVotes<=0 AND u.UpVotes<=0;

-- train q32
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE u.Id = v.UserId AND v.UserId = ph.UserId AND ph.UserId =c.UserId AND c.CreationDate>='2012-08-31 10:10:28'::timestamp AND c.CreationDate<='2012-11-12 19:23:15'::timestamp AND ph.CreationDate>='2012-12-27 00:14:56'::timestamp AND ph.CreationDate<='2014-03-25 00:33:01'::timestamp AND v.CreationDate>='2013-04-26 00:00:00'::timestamp AND v.CreationDate<='2013-08-24 00:00:00'::timestamp AND u.Views>=0 AND u.Views<=3 AND u.DownVotes>=0 AND u.DownVotes<=0 AND u.UpVotes<=2;

-- train q33
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE u.Id = v.UserId AND v.UserId = ph.UserId AND ph.UserId =c.UserId AND v.BountyAmount>=50 AND v.CreationDate>='2011-03-18 00:00:00'::timestamp AND v.CreationDate<='2014-05-30 00:00:00'::timestamp AND u.Reputation>=1 AND u.Views>=0 AND u.Views<=3 AND u.UpVotes=4 AND u.CreationDate>='2013-03-18 20:44:37'::timestamp AND u.CreationDate<='2013-07-15 02:31:25'::timestamp;

-- train q33
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE u.Id = v.UserId AND v.UserId = ph.UserId AND ph.UserId =c.UserId AND v.BountyAmount>=50 AND v.CreationDate>='2012-11-26 00:00:00'::timestamp AND v.CreationDate<='2014-02-18 00:00:00'::timestamp AND u.Reputation>=1 AND u.Views>=0 AND u.Views<=4 AND u.UpVotes=3 AND u.CreationDate>='2013-01-16 20:29:10'::timestamp AND u.CreationDate<='2013-11-15 10:35:16'::timestamp;

-- train q33
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE u.Id = v.UserId AND v.UserId = ph.UserId AND ph.UserId =c.UserId AND v.BountyAmount>=50 AND v.CreationDate>='2013-02-12 00:00:00'::timestamp AND v.CreationDate<='2012-11-26 00:00:00'::timestamp AND u.Reputation>=1 AND u.Views>=0 AND u.Views<=1 AND u.UpVotes=4 AND u.CreationDate>='2013-01-16 20:29:10'::timestamp AND u.CreationDate<='2013-03-18 20:44:37'::timestamp;

-- train q33
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE u.Id = v.UserId AND v.UserId = ph.UserId AND ph.UserId =c.UserId AND v.BountyAmount>=50 AND v.CreationDate>='2012-11-26 00:00:00'::timestamp AND v.CreationDate<='2014-07-22 00:00:00'::timestamp AND u.Reputation>=11 AND u.Views>=0 AND u.Views<=1 AND u.UpVotes=2 AND u.CreationDate>='2013-03-18 20:44:37'::timestamp AND u.CreationDate<='2014-09-14 01:01:44'::timestamp;

-- train q33
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE u.Id = v.UserId AND v.UserId = ph.UserId AND ph.UserId =c.UserId AND v.BountyAmount>=50 AND v.CreationDate>='2011-07-09 00:00:00'::timestamp AND v.CreationDate<='2012-11-26 00:00:00'::timestamp AND u.Reputation>=1 AND u.Views>=0 AND u.Views<=1 AND u.UpVotes=1 AND u.CreationDate>='2011-02-11 21:16:21'::timestamp AND u.CreationDate<='2014-08-08 12:49:32'::timestamp;

-- train q33
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE u.Id = v.UserId AND v.UserId = ph.UserId AND ph.UserId =c.UserId AND v.BountyAmount>=50 AND v.CreationDate>='2010-11-10 00:00:00'::timestamp AND v.CreationDate<='2014-05-30 00:00:00'::timestamp AND u.Reputation>=1 AND u.Views>=1 AND u.Views<=1 AND u.UpVotes=4 AND u.CreationDate>='2012-04-12 19:30:57'::timestamp AND u.CreationDate<='2014-01-10 18:05:07'::timestamp;

-- train q33
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE u.Id = v.UserId AND v.UserId = ph.UserId AND ph.UserId =c.UserId AND v.BountyAmount>=0 AND v.CreationDate>='2012-02-07 00:00:00'::timestamp AND v.CreationDate<='2014-02-18 00:00:00'::timestamp AND u.Reputation>=11 AND u.Views>=0 AND u.Views<=4 AND u.UpVotes=3 AND u.CreationDate>='2012-10-29 20:13:00'::timestamp AND u.CreationDate<='2013-07-15 02:31:25'::timestamp;

-- train q33
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE u.Id = v.UserId AND v.UserId = ph.UserId AND ph.UserId =c.UserId AND v.BountyAmount>=50 AND v.CreationDate>='2010-11-10 00:00:00'::timestamp AND v.CreationDate<='2012-09-14 00:00:00'::timestamp AND u.Reputation>=1 AND u.Views>=0 AND u.Views<=18 AND u.UpVotes=4 AND u.CreationDate>='2011-02-11 21:16:21'::timestamp AND u.CreationDate<='2013-05-14 19:05:54'::timestamp;

-- train q33
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE u.Id = v.UserId AND v.UserId = ph.UserId AND ph.UserId =c.UserId AND v.BountyAmount>=50 AND v.CreationDate>='2010-11-10 00:00:00'::timestamp AND v.CreationDate<='2012-11-26 00:00:00'::timestamp AND u.Reputation>=26 AND u.Views>=0 AND u.Views<=0 AND u.UpVotes=0 AND u.CreationDate>='2012-10-29 20:13:00'::timestamp AND u.CreationDate<='2013-11-15 10:35:16'::timestamp;

-- train q33
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE u.Id = v.UserId AND v.UserId = ph.UserId AND ph.UserId =c.UserId AND v.BountyAmount>=50 AND v.CreationDate>='2011-07-09 00:00:00'::timestamp AND v.CreationDate<='2013-04-26 00:00:00'::timestamp AND u.Reputation>=11 AND u.Views>=0 AND u.Views<=2 AND u.UpVotes=3 AND u.CreationDate>='2013-05-14 19:05:54'::timestamp AND u.CreationDate<='2014-03-25 19:35:42'::timestamp;

-- train q93
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.UserId = u.Id AND p.OwnerUserId = u.Id AND p.PostTypeId=5 AND p.CreationDate<='2014-05-06 17:16:49'::timestamp AND u.Views>=1;

-- train q93
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.UserId = u.Id AND p.OwnerUserId = u.Id AND p.PostTypeId=4 AND p.CreationDate<='2013-02-15 03:31:21'::timestamp AND u.Views>=0;

-- train q93
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.UserId = u.Id AND p.OwnerUserId = u.Id AND p.PostTypeId=4 AND p.CreationDate<='2013-04-21 19:38:59'::timestamp AND u.Views>=0;

-- train q93
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.UserId = u.Id AND p.OwnerUserId = u.Id AND p.PostTypeId=5 AND p.CreationDate<='2013-02-15 03:31:21'::timestamp AND u.Views>=1;

-- train q93
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.UserId = u.Id AND p.OwnerUserId = u.Id AND p.PostTypeId=1 AND p.CreationDate<='2014-05-06 17:16:49'::timestamp AND u.Views>=0;

-- train q93
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.UserId = u.Id AND p.OwnerUserId = u.Id AND p.PostTypeId=6 AND p.CreationDate<='2014-03-25 12:00:30'::timestamp AND u.Views>=0;

-- train q93
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.UserId = u.Id AND p.OwnerUserId = u.Id AND p.PostTypeId=6 AND p.CreationDate<='2013-10-20 19:44:22'::timestamp AND u.Views>=0;

-- train q93
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.UserId = u.Id AND p.OwnerUserId = u.Id AND p.PostTypeId=2 AND p.CreationDate<='2014-06-18 20:06:36'::timestamp AND u.Views>=0;

-- train q93
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.UserId = u.Id AND p.OwnerUserId = u.Id AND p.PostTypeId=1 AND p.CreationDate<='2014-09-14 02:09:23'::timestamp AND u.Views>=0;

-- train q93
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.UserId = u.Id AND p.OwnerUserId = u.Id AND p.PostTypeId=5 AND p.CreationDate<='2013-04-21 19:38:59'::timestamp AND u.Views>=0;

-- train q94
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.UserId = u.Id AND p.OwnerUserId = u.Id AND p.CommentCount>=0 AND u.CreationDate>='2011-08-15 19:36:17'::timestamp;

-- train q94
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.UserId = u.Id AND p.OwnerUserId = u.Id AND p.CommentCount>=0 AND u.CreationDate>='2012-04-12 19:30:57'::timestamp;

-- train q94
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.UserId = u.Id AND p.OwnerUserId = u.Id AND p.CommentCount>=0 AND u.CreationDate>='2012-10-29 20:13:00'::timestamp;

-- train q94
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.UserId = u.Id AND p.OwnerUserId = u.Id AND p.CommentCount>=0 AND u.CreationDate>='2013-07-15 02:31:25'::timestamp;

-- train q94
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.UserId = u.Id AND p.OwnerUserId = u.Id AND p.CommentCount>=0 AND u.CreationDate>='2012-07-30 02:08:59'::timestamp;

-- train q94
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.UserId = u.Id AND p.OwnerUserId = u.Id AND p.CommentCount>=1 AND u.CreationDate>='2012-04-12 19:30:57'::timestamp;

-- train q94
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.UserId = u.Id AND p.OwnerUserId = u.Id AND p.CommentCount>=0 AND u.CreationDate>='2010-07-19 06:55:26'::timestamp;

-- train q94
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.UserId = u.Id AND p.OwnerUserId = u.Id AND p.CommentCount>=1 AND u.CreationDate>='2012-10-29 20:13:00'::timestamp;

-- train q94
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.UserId = u.Id AND p.OwnerUserId = u.Id AND p.CommentCount>=0 AND u.CreationDate>='2013-01-16 20:29:10'::timestamp;

-- train q94
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.UserId = u.Id AND p.OwnerUserId = u.Id AND p.CommentCount>=0 AND u.CreationDate>='2013-11-15 10:35:16'::timestamp;

-- train q69
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, badges as b WHERE p.Id = c.PostId AND p.Id = pl.RelatedPostId AND p.Id = ph.PostId AND p.Id = v.PostId AND b.UserId = c.UserId AND c.Score=0 AND p.Score<=1 AND p.ViewCount<=175495 AND pl.LinkTypeId=1 AND v.CreationDate<='2012-11-26 00:00:00'::timestamp;

-- train q69
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, badges as b WHERE p.Id = c.PostId AND p.Id = pl.RelatedPostId AND p.Id = ph.PostId AND p.Id = v.PostId AND b.UserId = c.UserId AND c.Score=4 AND p.Score<=2 AND p.ViewCount<=228 AND pl.LinkTypeId=3 AND v.CreationDate<='2014-01-04 00:00:00'::timestamp;

-- train q69
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, badges as b WHERE p.Id = c.PostId AND p.Id = pl.RelatedPostId AND p.Id = ph.PostId AND p.Id = v.PostId AND b.UserId = c.UserId AND c.Score=3 AND p.Score<=5 AND p.ViewCount<=373 AND pl.LinkTypeId=3 AND v.CreationDate<='2014-02-18 00:00:00'::timestamp;

-- train q69
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, badges as b WHERE p.Id = c.PostId AND p.Id = pl.RelatedPostId AND p.Id = ph.PostId AND p.Id = v.PostId AND b.UserId = c.UserId AND c.Score=4 AND p.Score<=192 AND p.ViewCount<=373 AND pl.LinkTypeId=1 AND v.CreationDate<='2013-10-28 00:00:00'::timestamp;

-- train q69
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, badges as b WHERE p.Id = c.PostId AND p.Id = pl.RelatedPostId AND p.Id = ph.PostId AND p.Id = v.PostId AND b.UserId = c.UserId AND c.Score=3 AND p.Score<=1 AND p.ViewCount<=288 AND pl.LinkTypeId=3 AND v.CreationDate<='2013-02-12 00:00:00'::timestamp;

-- train q69
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, badges as b WHERE p.Id = c.PostId AND p.Id = pl.RelatedPostId AND p.Id = ph.PostId AND p.Id = v.PostId AND b.UserId = c.UserId AND c.Score=1 AND p.Score<=2 AND p.ViewCount<=185 AND pl.LinkTypeId=1 AND v.CreationDate<='2014-07-22 00:00:00'::timestamp;

-- train q69
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, badges as b WHERE p.Id = c.PostId AND p.Id = pl.RelatedPostId AND p.Id = ph.PostId AND p.Id = v.PostId AND b.UserId = c.UserId AND c.Score=4 AND p.Score<=2 AND p.ViewCount<=107 AND pl.LinkTypeId=1 AND v.CreationDate<='2013-10-28 00:00:00'::timestamp;

-- train q69
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, badges as b WHERE p.Id = c.PostId AND p.Id = pl.RelatedPostId AND p.Id = ph.PostId AND p.Id = v.PostId AND b.UserId = c.UserId AND c.Score=4 AND p.Score<=9 AND p.ViewCount<=107 AND pl.LinkTypeId=3 AND v.CreationDate<='2014-09-14 00:00:00'::timestamp;

-- train q69
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, badges as b WHERE p.Id = c.PostId AND p.Id = pl.RelatedPostId AND p.Id = ph.PostId AND p.Id = v.PostId AND b.UserId = c.UserId AND c.Score=4 AND p.Score<=2 AND p.ViewCount<=90 AND pl.LinkTypeId=1 AND v.CreationDate<='2012-11-26 00:00:00'::timestamp;

-- train q69
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, badges as b WHERE p.Id = c.PostId AND p.Id = pl.RelatedPostId AND p.Id = ph.PostId AND p.Id = v.PostId AND b.UserId = c.UserId AND c.Score=1 AND p.Score<=1 AND p.ViewCount<=128 AND pl.LinkTypeId=1 AND v.CreationDate<='2013-04-26 00:00:00'::timestamp;

-- train q71
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, badges as b WHERE p.Id = c.PostId AND p.Id = pl.RelatedPostId AND p.Id = ph.PostId AND p.Id = v.PostId AND b.UserId = c.UserId AND p.CommentCount>=0 AND ph.PostHistoryTypeId=4 AND v.VoteTypeId=3;

-- train q71
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, badges as b WHERE p.Id = c.PostId AND p.Id = pl.RelatedPostId AND p.Id = ph.PostId AND p.Id = v.PostId AND b.UserId = c.UserId AND p.CommentCount>=2 AND ph.PostHistoryTypeId=2 AND v.VoteTypeId=1;

-- train q71
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, badges as b WHERE p.Id = c.PostId AND p.Id = pl.RelatedPostId AND p.Id = ph.PostId AND p.Id = v.PostId AND b.UserId = c.UserId AND p.CommentCount>=0 AND ph.PostHistoryTypeId=5 AND v.VoteTypeId=1;

-- train q71
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, badges as b WHERE p.Id = c.PostId AND p.Id = pl.RelatedPostId AND p.Id = ph.PostId AND p.Id = v.PostId AND b.UserId = c.UserId AND p.CommentCount>=0 AND ph.PostHistoryTypeId=5 AND v.VoteTypeId=3;

-- train q71
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, badges as b WHERE p.Id = c.PostId AND p.Id = pl.RelatedPostId AND p.Id = ph.PostId AND p.Id = v.PostId AND b.UserId = c.UserId AND p.CommentCount>=0 AND ph.PostHistoryTypeId=2 AND v.VoteTypeId=3;

-- train q71
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, badges as b WHERE p.Id = c.PostId AND p.Id = pl.RelatedPostId AND p.Id = ph.PostId AND p.Id = v.PostId AND b.UserId = c.UserId AND p.CommentCount>=0 AND ph.PostHistoryTypeId=5 AND v.VoteTypeId=2;

-- train q71
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, badges as b WHERE p.Id = c.PostId AND p.Id = pl.RelatedPostId AND p.Id = ph.PostId AND p.Id = v.PostId AND b.UserId = c.UserId AND p.CommentCount>=0 AND ph.PostHistoryTypeId=4 AND v.VoteTypeId=2;

-- train q71
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, badges as b WHERE p.Id = c.PostId AND p.Id = pl.RelatedPostId AND p.Id = ph.PostId AND p.Id = v.PostId AND b.UserId = c.UserId AND p.CommentCount>=0 AND ph.PostHistoryTypeId=3 AND v.VoteTypeId=16;

-- train q71
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, badges as b WHERE p.Id = c.PostId AND p.Id = pl.RelatedPostId AND p.Id = ph.PostId AND p.Id = v.PostId AND b.UserId = c.UserId AND p.CommentCount>=0 AND ph.PostHistoryTypeId=2 AND v.VoteTypeId=2;

-- train q71
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v, badges as b WHERE p.Id = c.PostId AND p.Id = pl.RelatedPostId AND p.Id = ph.PostId AND p.Id = v.PostId AND b.UserId = c.UserId AND p.CommentCount>=0 AND ph.PostHistoryTypeId=3 AND v.VoteTypeId=3;

-- train q31
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE u.Id = v.UserId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND v.CreationDate<='2012-09-14 00:00:00'::timestamp AND p.Score<=3 AND p.AnswerCount<=1 AND b.Date>='2013-09-24 02:09:52'::timestamp AND b.Date<='2014-01-14 22:31:05'::timestamp AND u.CreationDate>='2011-08-15 19:36:17'::timestamp;

-- train q31
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE u.Id = v.UserId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND v.CreationDate<='2014-05-30 00:00:00'::timestamp AND p.Score<=1 AND p.AnswerCount<=1 AND b.Date>='2011-01-04 03:20:19'::timestamp AND b.Date<='2014-04-07 18:51:36'::timestamp AND u.CreationDate>='2011-12-27 18:54:39'::timestamp;

-- train q31
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE u.Id = v.UserId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND v.CreationDate<='2014-07-22 00:00:00'::timestamp AND p.Score<=192 AND p.AnswerCount<=1 AND b.Date>='2011-07-01 17:18:21'::timestamp AND b.Date<='2014-08-01 17:46:43'::timestamp AND u.CreationDate>='2010-07-19 06:55:26'::timestamp;

-- train q31
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE u.Id = v.UserId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND v.CreationDate<='2014-05-30 00:00:00'::timestamp AND p.Score<=1 AND p.AnswerCount<=1 AND b.Date>='2011-11-17 23:53:38'::timestamp AND b.Date<='2014-04-07 18:51:36'::timestamp AND u.CreationDate>='2013-09-19 17:00:57'::timestamp;

-- train q31
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE u.Id = v.UserId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND v.CreationDate<='2014-01-04 00:00:00'::timestamp AND p.Score<=4 AND p.AnswerCount<=1 AND b.Date>='2011-11-17 23:53:38'::timestamp AND b.Date<='2014-05-19 14:48:50'::timestamp AND u.CreationDate>='2011-02-11 21:16:21'::timestamp;

-- train q31
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE u.Id = v.UserId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND v.CreationDate<='2012-11-26 00:00:00'::timestamp AND p.Score<=1 AND p.AnswerCount<=136 AND b.Date>='2012-09-06 10:09:57'::timestamp AND b.Date<='2013-11-18 20:16:10'::timestamp AND u.CreationDate>='2011-12-27 18:54:39'::timestamp;

-- train q31
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE u.Id = v.UserId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND v.CreationDate<='2013-04-26 00:00:00'::timestamp AND p.Score<=2 AND p.AnswerCount<=1 AND b.Date>='2012-06-11 05:01:45'::timestamp AND b.Date<='2014-05-19 14:48:50'::timestamp AND u.CreationDate>='2011-12-27 18:54:39'::timestamp;

-- train q31
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE u.Id = v.UserId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND v.CreationDate<='2013-04-26 00:00:00'::timestamp AND p.Score<=9 AND p.AnswerCount<=1 AND b.Date>='2012-09-06 10:09:57'::timestamp AND b.Date<='2014-05-19 14:48:50'::timestamp AND u.CreationDate>='2013-07-15 02:31:25'::timestamp;

-- train q31
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE u.Id = v.UserId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND v.CreationDate<='2014-02-18 00:00:00'::timestamp AND p.Score<=1 AND p.AnswerCount<=1 AND b.Date>='2011-07-01 17:18:21'::timestamp AND b.Date<='2013-11-18 20:16:10'::timestamp AND u.CreationDate>='2011-08-15 19:36:17'::timestamp;

-- train q31
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE u.Id = v.UserId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND v.CreationDate<='2013-04-26 00:00:00'::timestamp AND p.Score<=2 AND p.AnswerCount<=136 AND b.Date>='2012-09-06 10:09:57'::timestamp AND b.Date<='2013-01-30 21:16:06'::timestamp AND u.CreationDate>='2011-02-11 21:16:21'::timestamp;

-- train q103
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE u.Id = b.UserId AND u.Id = p.OwnerUserId AND u.Id = v.UserId AND p.PostTypeId=5 AND p.CommentCount>=1 AND p.CommentCount<=7 AND u.Reputation>=16 AND u.DownVotes>=0 AND u.DownVotes<=0;

-- train q103
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE u.Id = b.UserId AND u.Id = p.OwnerUserId AND u.Id = v.UserId AND p.PostTypeId=2 AND p.CommentCount>=1 AND p.CommentCount<=7 AND u.Reputation>=1 AND u.DownVotes>=0 AND u.DownVotes<=0;

-- train q103
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE u.Id = b.UserId AND u.Id = p.OwnerUserId AND u.Id = v.UserId AND p.PostTypeId=5 AND p.CommentCount>=0 AND p.CommentCount<=5 AND u.Reputation>=1 AND u.DownVotes>=0 AND u.DownVotes<=0;

-- train q103
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE u.Id = b.UserId AND u.Id = p.OwnerUserId AND u.Id = v.UserId AND p.PostTypeId=1 AND p.CommentCount>=0 AND p.CommentCount<=4 AND u.Reputation>=1 AND u.DownVotes>=0 AND u.DownVotes<=0;

-- train q103
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE u.Id = b.UserId AND u.Id = p.OwnerUserId AND u.Id = v.UserId AND p.PostTypeId=2 AND p.CommentCount>=1 AND p.CommentCount<=1 AND u.Reputation>=6 AND u.DownVotes>=0 AND u.DownVotes<=0;

-- train q103
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE u.Id = b.UserId AND u.Id = p.OwnerUserId AND u.Id = v.UserId AND p.PostTypeId=1 AND p.CommentCount>=1 AND p.CommentCount<=2 AND u.Reputation>=16 AND u.DownVotes>=0 AND u.DownVotes<=0;

-- train q103
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE u.Id = b.UserId AND u.Id = p.OwnerUserId AND u.Id = v.UserId AND p.PostTypeId=5 AND p.CommentCount>=0 AND p.CommentCount<=45 AND u.Reputation>=11 AND u.DownVotes>=0 AND u.DownVotes<=1920;

-- train q103
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE u.Id = b.UserId AND u.Id = p.OwnerUserId AND u.Id = v.UserId AND p.PostTypeId=6 AND p.CommentCount>=0 AND p.CommentCount<=1 AND u.Reputation>=1 AND u.DownVotes>=0 AND u.DownVotes<=0;

-- train q103
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE u.Id = b.UserId AND u.Id = p.OwnerUserId AND u.Id = v.UserId AND p.PostTypeId=1 AND p.CommentCount>=1 AND p.CommentCount<=4 AND u.Reputation>=6 AND u.DownVotes>=0 AND u.DownVotes<=0;

-- train q103
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE u.Id = b.UserId AND u.Id = p.OwnerUserId AND u.Id = v.UserId AND p.PostTypeId=4 AND p.CommentCount>=0 AND p.CommentCount<=45 AND u.Reputation>=6 AND u.DownVotes>=0 AND u.DownVotes<=0;

-- train q3
SELECT COUNT(*) FROM comments as c, postHistory as ph WHERE c.UserId = ph.UserId AND c.Score=1 AND ph.PostHistoryTypeId=4;

-- train q3
SELECT COUNT(*) FROM comments as c, postHistory as ph WHERE c.UserId = ph.UserId AND c.Score=4 AND ph.PostHistoryTypeId=5;

-- train q3
SELECT COUNT(*) FROM comments as c, postHistory as ph WHERE c.UserId = ph.UserId AND c.Score=2 AND ph.PostHistoryTypeId=4;

-- train q3
SELECT COUNT(*) FROM comments as c, postHistory as ph WHERE c.UserId = ph.UserId AND c.Score=3 AND ph.PostHistoryTypeId=4;

-- train q3
SELECT COUNT(*) FROM comments as c, postHistory as ph WHERE c.UserId = ph.UserId AND c.Score=0 AND ph.PostHistoryTypeId=4;

-- train q3
SELECT COUNT(*) FROM comments as c, postHistory as ph WHERE c.UserId = ph.UserId AND c.Score=1 AND ph.PostHistoryTypeId=5;

-- train q3
SELECT COUNT(*) FROM comments as c, postHistory as ph WHERE c.UserId = ph.UserId AND c.Score=4 AND ph.PostHistoryTypeId=1;

-- train q3
SELECT COUNT(*) FROM comments as c, postHistory as ph WHERE c.UserId = ph.UserId AND c.Score=3 AND ph.PostHistoryTypeId=5;

-- train q3
SELECT COUNT(*) FROM comments as c, postHistory as ph WHERE c.UserId = ph.UserId AND c.Score=0 AND ph.PostHistoryTypeId=2;

-- train q3
SELECT COUNT(*) FROM comments as c, postHistory as ph WHERE c.UserId = ph.UserId AND c.Score=0 AND ph.PostHistoryTypeId=3;

-- train q4
SELECT COUNT(*) FROM comments as c, postHistory as ph WHERE c.UserId = ph.UserId AND ph.PostHistoryTypeId=5 AND ph.CreationDate>='2012-02-07 13:46:01'::timestamp;

-- train q4
SELECT COUNT(*) FROM comments as c, postHistory as ph WHERE c.UserId = ph.UserId AND ph.PostHistoryTypeId=2 AND ph.CreationDate>='2013-07-12 12:21:54'::timestamp;

-- train q4
SELECT COUNT(*) FROM comments as c, postHistory as ph WHERE c.UserId = ph.UserId AND ph.PostHistoryTypeId=1 AND ph.CreationDate>='2012-10-11 15:16:49'::timestamp;

-- train q4
SELECT COUNT(*) FROM comments as c, postHistory as ph WHERE c.UserId = ph.UserId AND ph.PostHistoryTypeId=5 AND ph.CreationDate>='2011-01-24 07:48:41'::timestamp;

-- train q4
SELECT COUNT(*) FROM comments as c, postHistory as ph WHERE c.UserId = ph.UserId AND ph.PostHistoryTypeId=2 AND ph.CreationDate>='2009-02-02 14:21:12'::timestamp;

-- train q4
SELECT COUNT(*) FROM comments as c, postHistory as ph WHERE c.UserId = ph.UserId AND ph.PostHistoryTypeId=1 AND ph.CreationDate>='2012-12-27 00:14:56'::timestamp;

-- train q4
SELECT COUNT(*) FROM comments as c, postHistory as ph WHERE c.UserId = ph.UserId AND ph.PostHistoryTypeId=3 AND ph.CreationDate>='2013-09-02 11:29:35'::timestamp;

-- train q4
SELECT COUNT(*) FROM comments as c, postHistory as ph WHERE c.UserId = ph.UserId AND ph.PostHistoryTypeId=2 AND ph.CreationDate>='2011-10-14 08:09:28'::timestamp;

-- train q4
SELECT COUNT(*) FROM comments as c, postHistory as ph WHERE c.UserId = ph.UserId AND ph.PostHistoryTypeId=4 AND ph.CreationDate>='2012-12-27 00:14:56'::timestamp;

-- train q4
SELECT COUNT(*) FROM comments as c, postHistory as ph WHERE c.UserId = ph.UserId AND ph.PostHistoryTypeId=3 AND ph.CreationDate>='2012-02-07 13:46:01'::timestamp;

-- train q100
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND p.Score>=0 AND p.Score<=3 AND p.ViewCount>=76 AND p.CreationDate<='2014-09-14 02:09:23'::timestamp AND u.Reputation>=1 AND u.CreationDate>='2013-09-19 17:00:57'::timestamp AND u.CreationDate<='2014-02-16 15:09:45'::timestamp;

-- train q100
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND p.Score>=2 AND p.Score<=2 AND p.ViewCount>=64 AND p.CreationDate<='2014-03-25 12:00:30'::timestamp AND u.Reputation>=26 AND u.CreationDate>='2011-02-11 21:16:21'::timestamp AND u.CreationDate<='2014-04-28 14:38:07'::timestamp;

-- train q100
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND p.Score>=0 AND p.Score<=1 AND p.ViewCount>=153 AND p.CreationDate<='2014-02-08 20:51:44'::timestamp AND u.Reputation>=1 AND u.CreationDate>='2010-07-19 06:55:26'::timestamp AND u.CreationDate<='2014-07-07 19:57:03'::timestamp;

-- train q100
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND p.Score>=2 AND p.Score<=2 AND p.ViewCount>=53 AND p.CreationDate<='2013-06-23 12:50:00'::timestamp AND u.Reputation>=1 AND u.CreationDate>='2013-11-15 10:35:16'::timestamp AND u.CreationDate<='2013-09-19 17:00:57'::timestamp;

-- train q100
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND p.Score>=0 AND p.Score<=2 AND p.ViewCount>=36 AND p.CreationDate<='2013-10-20 19:44:22'::timestamp AND u.Reputation>=1 AND u.CreationDate>='2013-07-15 02:31:25'::timestamp AND u.CreationDate<='2014-02-16 15:09:45'::timestamp;

-- train q100
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND p.Score>=0 AND p.Score<=3 AND p.ViewCount>=76 AND p.CreationDate<='2014-03-25 12:00:30'::timestamp AND u.Reputation>=1 AND u.CreationDate>='2013-11-15 10:35:16'::timestamp AND u.CreationDate<='2014-02-16 15:09:45'::timestamp;

-- train q100
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND p.Score>=0 AND p.Score<=6 AND p.ViewCount>=128 AND p.CreationDate<='2013-02-15 03:31:21'::timestamp AND u.Reputation>=1 AND u.CreationDate>='2010-07-19 06:55:26'::timestamp AND u.CreationDate<='2014-06-03 08:58:38'::timestamp;

-- train q100
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND p.Score>=1 AND p.Score<=2 AND p.ViewCount>=185 AND p.CreationDate<='2014-05-06 17:16:49'::timestamp AND u.Reputation>=1 AND u.CreationDate>='2011-12-27 18:54:39'::timestamp AND u.CreationDate<='2013-07-15 02:31:25'::timestamp;

-- train q100
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND p.Score>=-19 AND p.Score<=6 AND p.ViewCount>=36 AND p.CreationDate<='2014-02-08 20:51:44'::timestamp AND u.Reputation>=26 AND u.CreationDate>='2013-07-15 02:31:25'::timestamp AND u.CreationDate<='2014-02-16 15:09:45'::timestamp;

-- train q100
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND p.Score>=0 AND p.Score<=5 AND p.ViewCount>=90 AND p.CreationDate<='2014-09-14 02:09:23'::timestamp AND u.Reputation>=1 AND u.CreationDate>='2013-05-14 19:05:54'::timestamp AND u.CreationDate<='2013-09-19 17:00:57'::timestamp;

-- train q101
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND c.Score=4 AND c.CreationDate<='2013-06-17 03:23:06'::timestamp AND p.Score>=1 AND p.Score<=2 AND p.CommentCount<=3 AND p.CreationDate<='2012-11-26 23:54:07'::timestamp AND v.CreationDate<='2014-05-30 00:00:00'::timestamp AND u.DownVotes>=0;

-- train q101
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND c.Score=1 AND c.CreationDate<='2014-04-26 01:11:26'::timestamp AND p.Score>=0 AND p.Score<=2 AND p.CommentCount<=3 AND p.CreationDate<='2014-08-01 05:10:10'::timestamp AND v.CreationDate<='2014-09-14 00:00:00'::timestamp AND u.DownVotes>=0;

-- train q101
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND c.Score=2 AND c.CreationDate<='2014-01-24 21:50:43'::timestamp AND p.Score>=0 AND p.Score<=2 AND p.CommentCount<=1 AND p.CreationDate<='2013-04-21 19:38:59'::timestamp AND v.CreationDate<='2012-11-26 00:00:00'::timestamp AND u.DownVotes>=0;

-- train q101
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND c.Score=4 AND c.CreationDate<='2013-10-08 14:15:52'::timestamp AND p.Score>=0 AND p.Score<=3 AND p.CommentCount<=45 AND p.CreationDate<='2013-10-20 19:44:22'::timestamp AND v.CreationDate<='2013-04-26 00:00:00'::timestamp AND u.DownVotes>=0;

-- train q101
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND c.Score=3 AND c.CreationDate<='2013-11-29 17:06:35'::timestamp AND p.Score>=0 AND p.Score<=1 AND p.CommentCount<=2 AND p.CreationDate<='2014-02-08 20:51:44'::timestamp AND v.CreationDate<='2013-06-29 00:00:00'::timestamp AND u.DownVotes>=0;

-- train q101
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND c.Score=0 AND c.CreationDate<='2014-06-12 00:55:46'::timestamp AND p.Score>=1 AND p.Score<=6 AND p.CommentCount<=2 AND p.CreationDate<='2014-06-18 20:06:36'::timestamp AND v.CreationDate<='2013-06-29 00:00:00'::timestamp AND u.DownVotes>=0;

-- train q101
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND c.Score=3 AND c.CreationDate<='2013-06-17 03:23:06'::timestamp AND p.Score>=1 AND p.Score<=1 AND p.CommentCount<=2 AND p.CreationDate<='2014-08-01 05:10:10'::timestamp AND v.CreationDate<='2014-05-30 00:00:00'::timestamp AND u.DownVotes>=0;

-- train q101
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND c.Score=0 AND c.CreationDate<='2014-01-24 21:50:43'::timestamp AND p.Score>=0 AND p.Score<=4 AND p.CommentCount<=1 AND p.CreationDate<='2013-10-20 19:44:22'::timestamp AND v.CreationDate<='2014-09-14 00:00:00'::timestamp AND u.DownVotes>=0;

-- train q101
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND c.Score=2 AND c.CreationDate<='2014-07-29 16:38:44'::timestamp AND p.Score>=1 AND p.Score<=1 AND p.CommentCount<=2 AND p.CreationDate<='2014-05-06 17:16:49'::timestamp AND v.CreationDate<='2013-02-12 00:00:00'::timestamp AND u.DownVotes>=0;

-- train q101
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, users as u WHERE u.Id = p.OwnerUserId AND p.Id = v.PostId AND p.Id = c.PostId AND c.Score=3 AND c.CreationDate<='2013-04-12 14:14:02'::timestamp AND p.Score>=0 AND p.Score<=1 AND p.CommentCount<=45 AND p.CreationDate<='2013-04-21 19:38:59'::timestamp AND v.CreationDate<='2013-10-28 00:00:00'::timestamp AND u.DownVotes>=0;

-- train q57
SELECT COUNT(*) FROM tags as t, posts as p, users as u, votes as v, badges as b WHERE p.Id = t.ExcerptPostId AND u.Id = v.UserId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND u.Views>=0 AND u.Views<=1 AND u.UpVotes>=0 AND u.CreationDate<='2014-06-03 08:58:38'::timestamp AND v.BountyAmount>=0 AND v.BountyAmount<=50 AND b.Date<='2014-04-07 18:51:36'::timestamp;

-- train q57
SELECT COUNT(*) FROM tags as t, posts as p, users as u, votes as v, badges as b WHERE p.Id = t.ExcerptPostId AND u.Id = v.UserId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND u.Views>=0 AND u.Views<=3 AND u.UpVotes>=0 AND u.CreationDate<='2014-02-16 15:09:45'::timestamp AND v.BountyAmount>=50 AND v.BountyAmount<=50 AND b.Date<='2013-01-30 21:16:06'::timestamp;

-- train q57
SELECT COUNT(*) FROM tags as t, posts as p, users as u, votes as v, badges as b WHERE p.Id = t.ExcerptPostId AND u.Id = v.UserId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND u.Views>=1 AND u.Views<=0 AND u.UpVotes>=0 AND u.CreationDate<='2014-06-03 08:58:38'::timestamp AND v.BountyAmount>=50 AND v.BountyAmount<=50 AND b.Date<='2014-01-14 22:31:05'::timestamp;

-- train q57
SELECT COUNT(*) FROM tags as t, posts as p, users as u, votes as v, badges as b WHERE p.Id = t.ExcerptPostId AND u.Id = v.UserId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND u.Views>=0 AND u.Views<=8 AND u.UpVotes>=0 AND u.CreationDate<='2014-06-03 08:58:38'::timestamp AND v.BountyAmount>=50 AND v.BountyAmount<=100 AND b.Date<='2014-04-07 18:51:36'::timestamp;

-- train q57
SELECT COUNT(*) FROM tags as t, posts as p, users as u, votes as v, badges as b WHERE p.Id = t.ExcerptPostId AND u.Id = v.UserId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND u.Views>=0 AND u.Views<=8 AND u.UpVotes>=0 AND u.CreationDate<='2013-07-15 02:31:25'::timestamp AND v.BountyAmount>=50 AND v.BountyAmount<=50 AND b.Date<='2013-06-02 08:30:40'::timestamp;

-- train q57
SELECT COUNT(*) FROM tags as t, posts as p, users as u, votes as v, badges as b WHERE p.Id = t.ExcerptPostId AND u.Id = v.UserId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND u.Views>=0 AND u.Views<=4 AND u.UpVotes>=0 AND u.CreationDate<='2014-06-03 08:58:38'::timestamp AND v.BountyAmount>=50 AND v.BountyAmount<=100 AND b.Date<='2014-09-14 02:31:28'::timestamp;

-- train q57
SELECT COUNT(*) FROM tags as t, posts as p, users as u, votes as v, badges as b WHERE p.Id = t.ExcerptPostId AND u.Id = v.UserId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND u.Views>=0 AND u.Views<=5 AND u.UpVotes>=0 AND u.CreationDate<='2014-01-10 18:05:07'::timestamp AND v.BountyAmount>=50 AND v.BountyAmount<=100 AND b.Date<='2014-04-07 18:51:36'::timestamp;

-- train q57
SELECT COUNT(*) FROM tags as t, posts as p, users as u, votes as v, badges as b WHERE p.Id = t.ExcerptPostId AND u.Id = v.UserId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND u.Views>=0 AND u.Views<=1 AND u.UpVotes>=0 AND u.CreationDate<='2014-02-16 15:09:45'::timestamp AND v.BountyAmount>=50 AND v.BountyAmount<=50 AND b.Date<='2014-01-14 22:31:05'::timestamp;

-- train q57
SELECT COUNT(*) FROM tags as t, posts as p, users as u, votes as v, badges as b WHERE p.Id = t.ExcerptPostId AND u.Id = v.UserId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND u.Views>=0 AND u.Views<=0 AND u.UpVotes>=0 AND u.CreationDate<='2014-03-25 19:35:42'::timestamp AND v.BountyAmount>=50 AND v.BountyAmount<=50 AND b.Date<='2014-01-14 22:31:05'::timestamp;

-- train q57
SELECT COUNT(*) FROM tags as t, posts as p, users as u, votes as v, badges as b WHERE p.Id = t.ExcerptPostId AND u.Id = v.UserId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND u.Views>=0 AND u.Views<=5 AND u.UpVotes>=0 AND u.CreationDate<='2014-03-25 19:35:42'::timestamp AND v.BountyAmount>=50 AND v.BountyAmount<=500 AND b.Date<='2013-01-30 21:16:06'::timestamp;

-- train q6
SELECT COUNT(*) FROM badges as b, posts as p WHERE b.UserId = p.OwnerUserId AND b.Date<='2014-09-14 02:31:28'::timestamp AND p.AnswerCount>=1 AND p.AnswerCount<=1 AND p.CommentCount>=0 AND p.CommentCount<=45;

-- train q6
SELECT COUNT(*) FROM badges as b, posts as p WHERE b.UserId = p.OwnerUserId AND b.Date<='2013-09-24 02:09:52'::timestamp AND p.AnswerCount>=0 AND p.AnswerCount<=1 AND p.CommentCount>=0 AND p.CommentCount<=5;

-- train q6
SELECT COUNT(*) FROM badges as b, posts as p WHERE b.UserId = p.OwnerUserId AND b.Date<='2014-05-19 14:48:50'::timestamp AND p.AnswerCount>=1 AND p.AnswerCount<=136 AND p.CommentCount>=1 AND p.CommentCount<=2;

-- train q6
SELECT COUNT(*) FROM badges as b, posts as p WHERE b.UserId = p.OwnerUserId AND b.Date<='2013-11-18 20:16:10'::timestamp AND p.AnswerCount>=1 AND p.AnswerCount<=1 AND p.CommentCount>=0 AND p.CommentCount<=3;

-- train q6
SELECT COUNT(*) FROM badges as b, posts as p WHERE b.UserId = p.OwnerUserId AND b.Date<='2014-09-14 02:31:28'::timestamp AND p.AnswerCount>=0 AND p.AnswerCount<=3 AND p.CommentCount>=0 AND p.CommentCount<=3;

-- train q6
SELECT COUNT(*) FROM badges as b, posts as p WHERE b.UserId = p.OwnerUserId AND b.Date<='2014-04-07 18:51:36'::timestamp AND p.AnswerCount>=1 AND p.AnswerCount<=1 AND p.CommentCount>=0 AND p.CommentCount<=4;

-- train q6
SELECT COUNT(*) FROM badges as b, posts as p WHERE b.UserId = p.OwnerUserId AND b.Date<='2014-04-07 18:51:36'::timestamp AND p.AnswerCount>=1 AND p.AnswerCount<=136 AND p.CommentCount>=1 AND p.CommentCount<=2;

-- train q6
SELECT COUNT(*) FROM badges as b, posts as p WHERE b.UserId = p.OwnerUserId AND b.Date<='2014-06-30 18:24:30'::timestamp AND p.AnswerCount>=1 AND p.AnswerCount<=2 AND p.CommentCount>=1 AND p.CommentCount<=1;

-- train q6
SELECT COUNT(*) FROM badges as b, posts as p WHERE b.UserId = p.OwnerUserId AND b.Date<='2014-01-14 22:31:05'::timestamp AND p.AnswerCount>=0 AND p.AnswerCount<=1 AND p.CommentCount>=0 AND p.CommentCount<=45;

-- train q6
SELECT COUNT(*) FROM badges as b, posts as p WHERE b.UserId = p.OwnerUserId AND b.Date<='2014-01-14 22:31:05'::timestamp AND p.AnswerCount>=1 AND p.AnswerCount<=1 AND p.CommentCount>=0 AND p.CommentCount<=7;

-- train q46
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v WHERE p.Id = c.PostId AND p.Id = pl.PostId AND p.Id = ph.PostId AND p.Id = v.PostId AND c.CreationDate>='2011-01-19 20:40:12'::timestamp AND p.Score<=3 AND p.FavoriteCount>=0 AND p.FavoriteCount<=3 AND p.CreationDate>='2013-02-15 03:31:21'::timestamp AND p.CreationDate<='2013-06-23 12:50:00'::timestamp AND pl.LinkTypeId=1 AND pl.CreationDate<='2014-05-20 18:00:57'::timestamp AND ph.CreationDate>='2012-12-27 00:14:56'::timestamp AND v.CreationDate<='2014-09-14 00:00:00'::timestamp;

-- train q46
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v WHERE p.Id = c.PostId AND p.Id = pl.PostId AND p.Id = ph.PostId AND p.Id = v.PostId AND c.CreationDate>='2009-02-02 14:45:19'::timestamp AND p.Score<=1 AND p.FavoriteCount>=0 AND p.FavoriteCount<=233 AND p.CreationDate>='2012-03-28 16:24:28'::timestamp AND p.CreationDate<='2014-06-18 20:06:36'::timestamp AND pl.LinkTypeId=1 AND pl.CreationDate<='2013-04-04 23:36:21'::timestamp AND ph.CreationDate>='2011-06-11 20:40:55'::timestamp AND v.CreationDate<='2014-02-18 00:00:00'::timestamp;

-- train q46
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v WHERE p.Id = c.PostId AND p.Id = pl.PostId AND p.Id = ph.PostId AND p.Id = v.PostId AND c.CreationDate>='2012-01-28 20:58:07'::timestamp AND p.Score<=4 AND p.FavoriteCount>=1 AND p.FavoriteCount<=8 AND p.CreationDate>='2013-08-20 10:19:52'::timestamp AND p.CreationDate<='2013-02-15 03:31:21'::timestamp AND pl.LinkTypeId=3 AND pl.CreationDate<='2014-08-02 21:16:08'::timestamp AND ph.CreationDate>='2012-10-11 15:16:49'::timestamp AND v.CreationDate<='2013-08-24 00:00:00'::timestamp;

-- train q46
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v WHERE p.Id = c.PostId AND p.Id = pl.PostId AND p.Id = ph.PostId AND p.Id = v.PostId AND c.CreationDate>='2011-05-30 17:17:48'::timestamp AND p.Score<=3 AND p.FavoriteCount>=1 AND p.FavoriteCount<=1 AND p.CreationDate>='2012-09-11 15:29:12'::timestamp AND p.CreationDate<='2014-08-01 05:10:10'::timestamp AND pl.LinkTypeId=1 AND pl.CreationDate<='2014-02-08 14:31:25'::timestamp AND ph.CreationDate>='2011-10-14 08:09:28'::timestamp AND v.CreationDate<='2013-08-24 00:00:00'::timestamp;

-- train q46
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v WHERE p.Id = c.PostId AND p.Id = pl.PostId AND p.Id = ph.PostId AND p.Id = v.PostId AND c.CreationDate>='2012-11-12 19:23:15'::timestamp AND p.Score<=3 AND p.FavoriteCount>=2 AND p.FavoriteCount<=8 AND p.CreationDate>='2011-12-23 07:35:55'::timestamp AND p.CreationDate<='2013-04-21 19:38:59'::timestamp AND pl.LinkTypeId=1 AND pl.CreationDate<='2014-05-20 18:00:57'::timestamp AND ph.CreationDate>='2012-05-07 03:11:00'::timestamp AND v.CreationDate<='2012-11-26 00:00:00'::timestamp;

-- train q46
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v WHERE p.Id = c.PostId AND p.Id = pl.PostId AND p.Id = ph.PostId AND p.Id = v.PostId AND c.CreationDate>='2013-02-02 20:12:58'::timestamp AND p.Score<=2 AND p.FavoriteCount>=1 AND p.FavoriteCount<=2 AND p.CreationDate>='2011-04-26 07:24:16'::timestamp AND p.CreationDate<='2013-12-14 13:32:25'::timestamp AND pl.LinkTypeId=3 AND pl.CreationDate<='2014-01-03 14:55:25'::timestamp AND ph.CreationDate>='2013-09-02 11:29:35'::timestamp AND v.CreationDate<='2014-09-14 00:00:00'::timestamp;

-- train q46
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v WHERE p.Id = c.PostId AND p.Id = pl.PostId AND p.Id = ph.PostId AND p.Id = v.PostId AND c.CreationDate>='2011-10-04 13:23:07'::timestamp AND p.Score<=1 AND p.FavoriteCount>=1 AND p.FavoriteCount<=1 AND p.CreationDate>='2013-02-15 03:31:21'::timestamp AND p.CreationDate<='2014-08-01 05:10:10'::timestamp AND pl.LinkTypeId=3 AND pl.CreationDate<='2013-06-08 16:55:53'::timestamp AND ph.CreationDate>='2013-09-02 11:29:35'::timestamp AND v.CreationDate<='2013-06-29 00:00:00'::timestamp;

-- train q46
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v WHERE p.Id = c.PostId AND p.Id = pl.PostId AND p.Id = ph.PostId AND p.Id = v.PostId AND c.CreationDate>='2013-08-10 10:42:04'::timestamp AND p.Score<=1 AND p.FavoriteCount>=1 AND p.FavoriteCount<=3 AND p.CreationDate>='2011-04-26 07:24:16'::timestamp AND p.CreationDate<='2013-10-20 19:44:22'::timestamp AND pl.LinkTypeId=1 AND pl.CreationDate<='2014-08-02 21:16:08'::timestamp AND ph.CreationDate>='2012-12-27 00:14:56'::timestamp AND v.CreationDate<='2013-04-26 00:00:00'::timestamp;

-- train q46
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v WHERE p.Id = c.PostId AND p.Id = pl.PostId AND p.Id = ph.PostId AND p.Id = v.PostId AND c.CreationDate>='2011-01-19 20:40:12'::timestamp AND p.Score<=3 AND p.FavoriteCount>=0 AND p.FavoriteCount<=8 AND p.CreationDate>='2012-03-28 16:24:28'::timestamp AND p.CreationDate<='2013-04-21 19:38:59'::timestamp AND pl.LinkTypeId=1 AND pl.CreationDate<='2014-06-25 13:05:06'::timestamp AND ph.CreationDate>='2013-07-12 12:21:54'::timestamp AND v.CreationDate<='2013-02-12 00:00:00'::timestamp;

-- train q46
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, votes as v WHERE p.Id = c.PostId AND p.Id = pl.PostId AND p.Id = ph.PostId AND p.Id = v.PostId AND c.CreationDate>='2012-01-28 20:58:07'::timestamp AND p.Score<=3 AND p.FavoriteCount>=1 AND p.FavoriteCount<=5 AND p.CreationDate>='2009-02-02 14:21:12'::timestamp AND p.CreationDate<='2014-03-25 12:00:30'::timestamp AND pl.LinkTypeId=1 AND pl.CreationDate<='2013-09-20 11:04:34'::timestamp AND ph.CreationDate>='2013-09-02 11:29:35'::timestamp AND v.CreationDate<='2013-04-26 00:00:00'::timestamp;

-- train q109
SELECT COUNT(*) FROM comments as c, postLinks as pl, postHistory as ph, votes as v, posts as p WHERE pl.PostId = p.Id AND c.PostId = p.Id AND v.PostId = p.Id AND ph.PostId = p.Id AND c.Score=2 AND pl.CreationDate>='2012-03-15 19:56:37'::timestamp AND pl.CreationDate<='2014-03-10 20:22:46'::timestamp AND v.CreationDate>='2012-07-08 00:00:00'::timestamp AND v.CreationDate<='2014-02-18 00:00:00'::timestamp;

-- train q109
SELECT COUNT(*) FROM comments as c, postLinks as pl, postHistory as ph, votes as v, posts as p WHERE pl.PostId = p.Id AND c.PostId = p.Id AND v.PostId = p.Id AND ph.PostId = p.Id AND c.Score=0 AND pl.CreationDate>='2011-05-10 17:09:25'::timestamp AND pl.CreationDate<='2013-09-20 11:04:34'::timestamp AND v.CreationDate>='2012-09-14 00:00:00'::timestamp AND v.CreationDate<='2014-01-04 00:00:00'::timestamp;

-- train q109
SELECT COUNT(*) FROM comments as c, postLinks as pl, postHistory as ph, votes as v, posts as p WHERE pl.PostId = p.Id AND c.PostId = p.Id AND v.PostId = p.Id AND ph.PostId = p.Id AND c.Score=2 AND pl.CreationDate>='2013-06-08 16:55:53'::timestamp AND pl.CreationDate<='2014-04-17 15:33:46'::timestamp AND v.CreationDate>='2010-11-10 00:00:00'::timestamp AND v.CreationDate<='2014-09-14 00:00:00'::timestamp;

-- train q109
SELECT COUNT(*) FROM comments as c, postLinks as pl, postHistory as ph, votes as v, posts as p WHERE pl.PostId = p.Id AND c.PostId = p.Id AND v.PostId = p.Id AND ph.PostId = p.Id AND c.Score=1 AND pl.CreationDate>='2013-02-18 03:03:17'::timestamp AND pl.CreationDate<='2014-04-17 15:33:46'::timestamp AND v.CreationDate>='2011-10-26 00:00:00'::timestamp AND v.CreationDate<='2013-10-28 00:00:00'::timestamp;

-- train q109
SELECT COUNT(*) FROM comments as c, postLinks as pl, postHistory as ph, votes as v, posts as p WHERE pl.PostId = p.Id AND c.PostId = p.Id AND v.PostId = p.Id AND ph.PostId = p.Id AND c.Score=1 AND pl.CreationDate>='2012-06-15 12:31:41'::timestamp AND pl.CreationDate<='2014-08-02 21:16:08'::timestamp AND v.CreationDate>='2012-09-14 00:00:00'::timestamp AND v.CreationDate<='2014-02-18 00:00:00'::timestamp;

-- train q109
SELECT COUNT(*) FROM comments as c, postLinks as pl, postHistory as ph, votes as v, posts as p WHERE pl.PostId = p.Id AND c.PostId = p.Id AND v.PostId = p.Id AND ph.PostId = p.Id AND c.Score=2 AND pl.CreationDate>='2011-05-10 17:09:25'::timestamp AND pl.CreationDate<='2014-04-17 15:33:46'::timestamp AND v.CreationDate>='2011-03-18 00:00:00'::timestamp AND v.CreationDate<='2014-05-30 00:00:00'::timestamp;

-- train q109
SELECT COUNT(*) FROM comments as c, postLinks as pl, postHistory as ph, votes as v, posts as p WHERE pl.PostId = p.Id AND c.PostId = p.Id AND v.PostId = p.Id AND ph.PostId = p.Id AND c.Score=0 AND pl.CreationDate>='2013-09-20 11:04:34'::timestamp AND pl.CreationDate<='2014-01-03 14:55:25'::timestamp AND v.CreationDate>='2012-11-26 00:00:00'::timestamp AND v.CreationDate<='2013-04-26 00:00:00'::timestamp;

-- train q109
SELECT COUNT(*) FROM comments as c, postLinks as pl, postHistory as ph, votes as v, posts as p WHERE pl.PostId = p.Id AND c.PostId = p.Id AND v.PostId = p.Id AND ph.PostId = p.Id AND c.Score=1 AND pl.CreationDate>='2013-06-08 16:55:53'::timestamp AND pl.CreationDate<='2014-08-02 21:16:08'::timestamp AND v.CreationDate>='2011-03-18 00:00:00'::timestamp AND v.CreationDate<='2013-10-28 00:00:00'::timestamp;

-- train q109
SELECT COUNT(*) FROM comments as c, postLinks as pl, postHistory as ph, votes as v, posts as p WHERE pl.PostId = p.Id AND c.PostId = p.Id AND v.PostId = p.Id AND ph.PostId = p.Id AND c.Score=3 AND pl.CreationDate>='2011-11-07 20:52:19'::timestamp AND pl.CreationDate<='2014-02-08 14:31:25'::timestamp AND v.CreationDate>='2012-02-07 00:00:00'::timestamp AND v.CreationDate<='2013-02-12 00:00:00'::timestamp;

-- train q109
SELECT COUNT(*) FROM comments as c, postLinks as pl, postHistory as ph, votes as v, posts as p WHERE pl.PostId = p.Id AND c.PostId = p.Id AND v.PostId = p.Id AND ph.PostId = p.Id AND c.Score=1 AND pl.CreationDate>='2010-07-21 14:47:33'::timestamp AND pl.CreationDate<='2014-08-02 21:16:08'::timestamp AND v.CreationDate>='2009-02-02 00:00:00'::timestamp AND v.CreationDate<='2013-10-28 00:00:00'::timestamp;

-- train q86
SELECT COUNT(*) FROM comments as c, votes as v, users as u, posts as p WHERE c.PostId = p.Id AND u.Id = c.UserId AND v.PostId = p.Id AND c.Score=0 AND u.Views>=0 AND u.Views<=1;

-- train q86
SELECT COUNT(*) FROM comments as c, votes as v, users as u, posts as p WHERE c.PostId = p.Id AND u.Id = c.UserId AND v.PostId = p.Id AND c.Score=0 AND u.Views>=1 AND u.Views<=0;

-- train q86
SELECT COUNT(*) FROM comments as c, votes as v, users as u, posts as p WHERE c.PostId = p.Id AND u.Id = c.UserId AND v.PostId = p.Id AND c.Score=4 AND u.Views>=1 AND u.Views<=8;

-- train q86
SELECT COUNT(*) FROM comments as c, votes as v, users as u, posts as p WHERE c.PostId = p.Id AND u.Id = c.UserId AND v.PostId = p.Id AND c.Score=2 AND u.Views>=1 AND u.Views<=2;

-- train q86
SELECT COUNT(*) FROM comments as c, votes as v, users as u, posts as p WHERE c.PostId = p.Id AND u.Id = c.UserId AND v.PostId = p.Id AND c.Score=0 AND u.Views>=0 AND u.Views<=0;

-- train q86
SELECT COUNT(*) FROM comments as c, votes as v, users as u, posts as p WHERE c.PostId = p.Id AND u.Id = c.UserId AND v.PostId = p.Id AND c.Score=3 AND u.Views>=0 AND u.Views<=0;

-- train q86
SELECT COUNT(*) FROM comments as c, votes as v, users as u, posts as p WHERE c.PostId = p.Id AND u.Id = c.UserId AND v.PostId = p.Id AND c.Score=4 AND u.Views>=1 AND u.Views<=20932;

-- train q86
SELECT COUNT(*) FROM comments as c, votes as v, users as u, posts as p WHERE c.PostId = p.Id AND u.Id = c.UserId AND v.PostId = p.Id AND c.Score=1 AND u.Views>=0 AND u.Views<=1;

-- train q86
SELECT COUNT(*) FROM comments as c, votes as v, users as u, posts as p WHERE c.PostId = p.Id AND u.Id = c.UserId AND v.PostId = p.Id AND c.Score=2 AND u.Views>=0 AND u.Views<=0;

-- train q86
SELECT COUNT(*) FROM comments as c, votes as v, users as u, posts as p WHERE c.PostId = p.Id AND u.Id = c.UserId AND v.PostId = p.Id AND c.Score=3 AND u.Views>=0 AND u.Views<=2;

-- train q53
SELECT COUNT(*) FROM comments as c, postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND p.Id = c.PostId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND c.CreationDate<='2014-04-26 01:11:26'::timestamp AND p.ViewCount>=36 AND u.Reputation>=1;

-- train q53
SELECT COUNT(*) FROM comments as c, postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND p.Id = c.PostId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND c.CreationDate<='2014-03-10 15:31:38'::timestamp AND p.ViewCount>=153 AND u.Reputation>=1;

-- train q53
SELECT COUNT(*) FROM comments as c, postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND p.Id = c.PostId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND c.CreationDate<='2014-07-29 16:38:44'::timestamp AND p.ViewCount>=90 AND u.Reputation>=1;

-- train q53
SELECT COUNT(*) FROM comments as c, postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND p.Id = c.PostId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND c.CreationDate<='2014-07-29 16:38:44'::timestamp AND p.ViewCount>=76 AND u.Reputation>=16;

-- train q53
SELECT COUNT(*) FROM comments as c, postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND p.Id = c.PostId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND c.CreationDate<='2012-11-12 19:23:15'::timestamp AND p.ViewCount>=1 AND u.Reputation>=1;

-- train q53
SELECT COUNT(*) FROM comments as c, postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND p.Id = c.PostId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND c.CreationDate<='2013-10-08 14:15:52'::timestamp AND p.ViewCount>=153 AND u.Reputation>=1;

-- train q53
SELECT COUNT(*) FROM comments as c, postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND p.Id = c.PostId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND c.CreationDate<='2013-08-10 10:42:04'::timestamp AND p.ViewCount>=153 AND u.Reputation>=1;

-- train q53
SELECT COUNT(*) FROM comments as c, postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND p.Id = c.PostId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND c.CreationDate<='2013-04-12 14:14:02'::timestamp AND p.ViewCount>=1 AND u.Reputation>=1;

-- train q53
SELECT COUNT(*) FROM comments as c, postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND p.Id = c.PostId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND c.CreationDate<='2013-06-17 03:23:06'::timestamp AND p.ViewCount>=76 AND u.Reputation>=1;

-- train q53
SELECT COUNT(*) FROM comments as c, postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND p.Id = c.PostId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND c.CreationDate<='2013-06-17 03:23:06'::timestamp AND p.ViewCount>=28 AND u.Reputation>=1;

-- train q54
SELECT COUNT(*) FROM comments as c, postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND p.Id = c.PostId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND pl.LinkTypeId=1 AND pl.CreationDate>='2013-02-18 03:03:17'::timestamp AND p.Score=4 AND p.ViewCount>=44 AND p.FavoriteCount>=1 AND u.CreationDate>='2013-01-16 20:29:10'::timestamp;

-- train q54
SELECT COUNT(*) FROM comments as c, postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND p.Id = c.PostId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND pl.LinkTypeId=3 AND pl.CreationDate>='2012-03-15 19:56:37'::timestamp AND p.Score=3 AND p.ViewCount>=53 AND p.FavoriteCount>=1 AND u.CreationDate>='2013-09-19 17:00:57'::timestamp;

-- train q54
SELECT COUNT(*) FROM comments as c, postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND p.Id = c.PostId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND pl.LinkTypeId=3 AND pl.CreationDate>='2012-03-15 19:56:37'::timestamp AND p.Score=0 AND p.ViewCount>=1 AND p.FavoriteCount>=1 AND u.CreationDate>='2013-11-15 10:35:16'::timestamp;

-- train q54
SELECT COUNT(*) FROM comments as c, postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND p.Id = c.PostId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND pl.LinkTypeId=3 AND pl.CreationDate>='2013-06-08 16:55:53'::timestamp AND p.Score=1 AND p.ViewCount>=90 AND p.FavoriteCount>=1 AND u.CreationDate>='2013-03-18 20:44:37'::timestamp;

-- train q54
SELECT COUNT(*) FROM comments as c, postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND p.Id = c.PostId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND pl.LinkTypeId=3 AND pl.CreationDate>='2012-11-13 10:18:17'::timestamp AND p.Score=2 AND p.ViewCount>=90 AND p.FavoriteCount>=1 AND u.CreationDate>='2011-02-11 21:16:21'::timestamp;

-- train q54
SELECT COUNT(*) FROM comments as c, postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND p.Id = c.PostId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND pl.LinkTypeId=3 AND pl.CreationDate>='2012-06-15 12:31:41'::timestamp AND p.Score=3 AND p.ViewCount>=107 AND p.FavoriteCount>=0 AND u.CreationDate>='2012-10-29 20:13:00'::timestamp;

-- train q54
SELECT COUNT(*) FROM comments as c, postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND p.Id = c.PostId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND pl.LinkTypeId=1 AND pl.CreationDate>='2012-09-01 05:33:57'::timestamp AND p.Score=2 AND p.ViewCount>=1 AND p.FavoriteCount>=1 AND u.CreationDate>='2013-09-19 17:00:57'::timestamp;

-- train q54
SELECT COUNT(*) FROM comments as c, postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND p.Id = c.PostId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND pl.LinkTypeId=1 AND pl.CreationDate>='2011-11-07 20:52:19'::timestamp AND p.Score=0 AND p.ViewCount>=64 AND p.FavoriteCount>=1 AND u.CreationDate>='2010-07-19 06:55:26'::timestamp;

-- train q54
SELECT COUNT(*) FROM comments as c, postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND p.Id = c.PostId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND pl.LinkTypeId=1 AND pl.CreationDate>='2013-02-18 03:03:17'::timestamp AND p.Score=3 AND p.ViewCount>=107 AND p.FavoriteCount>=1 AND u.CreationDate>='2013-01-16 20:29:10'::timestamp;

-- train q54
SELECT COUNT(*) FROM comments as c, postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND p.Id = c.PostId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND pl.LinkTypeId=1 AND pl.CreationDate>='2012-03-15 19:56:37'::timestamp AND p.Score=4 AND p.ViewCount>=1 AND p.FavoriteCount>=1 AND u.CreationDate>='2013-01-16 20:29:10'::timestamp;

-- train q40
SELECT COUNT(*) FROM postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND pl.LinkTypeId=3 AND p.Score>=1 AND p.CommentCount<=3 AND p.CreationDate>='2009-02-02 14:21:12'::timestamp AND p.CreationDate<='2013-06-23 12:50:00'::timestamp AND u.Views<=1 AND u.CreationDate>='2013-07-15 02:31:25'::timestamp AND u.CreationDate<='2014-07-07 19:57:03'::timestamp;

-- train q40
SELECT COUNT(*) FROM postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND pl.LinkTypeId=1 AND p.Score>=0 AND p.CommentCount<=45 AND p.CreationDate>='2012-03-28 16:24:28'::timestamp AND p.CreationDate<='2013-08-20 10:19:52'::timestamp AND u.Views<=18 AND u.CreationDate>='2013-01-16 20:29:10'::timestamp AND u.CreationDate<='2013-03-18 20:44:37'::timestamp;

-- train q40
SELECT COUNT(*) FROM postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND pl.LinkTypeId=1 AND p.Score>=-19 AND p.CommentCount<=5 AND p.CreationDate>='2011-08-31 21:28:05'::timestamp AND p.CreationDate<='2014-05-06 17:16:49'::timestamp AND u.Views<=18 AND u.CreationDate>='2013-01-16 20:29:10'::timestamp AND u.CreationDate<='2013-03-18 20:44:37'::timestamp;

-- train q40
SELECT COUNT(*) FROM postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND pl.LinkTypeId=1 AND p.Score>=2 AND p.CommentCount<=4 AND p.CreationDate>='2012-03-28 16:24:28'::timestamp AND p.CreationDate<='2013-02-15 03:31:21'::timestamp AND u.Views<=1 AND u.CreationDate>='2012-07-30 02:08:59'::timestamp AND u.CreationDate<='2013-07-15 02:31:25'::timestamp;

-- train q40
SELECT COUNT(*) FROM postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND pl.LinkTypeId=1 AND p.Score>=1 AND p.CommentCount<=0 AND p.CreationDate>='2012-03-28 16:24:28'::timestamp AND p.CreationDate<='2014-08-01 05:10:10'::timestamp AND u.Views<=20932 AND u.CreationDate>='2012-07-30 02:08:59'::timestamp AND u.CreationDate<='2013-09-19 17:00:57'::timestamp;

-- train q40
SELECT COUNT(*) FROM postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND pl.LinkTypeId=1 AND p.Score>=0 AND p.CommentCount<=1 AND p.CreationDate>='2012-11-26 23:54:07'::timestamp AND p.CreationDate<='2013-08-20 10:19:52'::timestamp AND u.Views<=18 AND u.CreationDate>='2012-04-12 19:30:57'::timestamp AND u.CreationDate<='2014-03-25 19:35:42'::timestamp;

-- train q40
SELECT COUNT(*) FROM postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND pl.LinkTypeId=1 AND p.Score>=1 AND p.CommentCount<=1 AND p.CreationDate>='2012-09-11 15:29:12'::timestamp AND p.CreationDate<='2013-12-14 13:32:25'::timestamp AND u.Views<=4 AND u.CreationDate>='2011-02-11 21:16:21'::timestamp AND u.CreationDate<='2013-07-15 02:31:25'::timestamp;

-- train q40
SELECT COUNT(*) FROM postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND pl.LinkTypeId=1 AND p.Score>=1 AND p.CommentCount<=45 AND p.CreationDate>='2013-02-15 03:31:21'::timestamp AND p.CreationDate<='2012-11-26 23:54:07'::timestamp AND u.Views<=0 AND u.CreationDate>='2013-01-16 20:29:10'::timestamp AND u.CreationDate<='2014-06-03 08:58:38'::timestamp;

-- train q40
SELECT COUNT(*) FROM postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND pl.LinkTypeId=3 AND p.Score>=0 AND p.CommentCount<=1 AND p.CreationDate>='2011-08-31 21:28:05'::timestamp AND p.CreationDate<='2014-02-08 20:51:44'::timestamp AND u.Views<=3 AND u.CreationDate>='2011-02-11 21:16:21'::timestamp AND u.CreationDate<='2014-03-25 19:35:42'::timestamp;

-- train q40
SELECT COUNT(*) FROM postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND pl.LinkTypeId=3 AND p.Score>=1 AND p.CommentCount<=3 AND p.CreationDate>='2011-12-23 07:35:55'::timestamp AND p.CreationDate<='2014-03-25 12:00:30'::timestamp AND u.Views<=5 AND u.CreationDate>='2013-11-15 10:35:16'::timestamp AND u.CreationDate<='2013-09-19 17:00:57'::timestamp;

-- train q41
SELECT COUNT(*) FROM postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND pl.CreationDate<='2014-02-08 14:31:25'::timestamp AND p.Score>=1 AND p.Score<=2 AND p.AnswerCount<=1 AND p.CommentCount=3 AND p.FavoriteCount>=1 AND p.FavoriteCount<=8 AND u.Views<=1 AND u.DownVotes>=0 AND u.CreationDate>='2012-04-12 19:30:57'::timestamp AND u.CreationDate<='2014-02-16 15:09:45'::timestamp AND b.Date<='2014-08-01 17:46:43'::timestamp;

-- train q41
SELECT COUNT(*) FROM postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND pl.CreationDate<='2014-09-13 20:54:31'::timestamp AND p.Score>=1 AND p.Score<=1 AND p.AnswerCount<=136 AND p.CommentCount=1 AND p.FavoriteCount>=1 AND p.FavoriteCount<=1 AND u.Views<=3 AND u.DownVotes>=0 AND u.CreationDate>='2011-08-15 19:36:17'::timestamp AND u.CreationDate<='2014-08-08 12:49:32'::timestamp AND b.Date<='2013-11-18 20:16:10'::timestamp;

-- train q41
SELECT COUNT(*) FROM postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND pl.CreationDate<='2013-07-24 12:34:55'::timestamp AND p.Score>=0 AND p.Score<=6 AND p.AnswerCount<=3 AND p.CommentCount=4 AND p.FavoriteCount>=1 AND p.FavoriteCount<=1 AND u.Views<=0 AND u.DownVotes>=0 AND u.CreationDate>='2013-05-14 19:05:54'::timestamp AND u.CreationDate<='2013-03-18 20:44:37'::timestamp AND b.Date<='2014-01-14 22:31:05'::timestamp;

-- train q41
SELECT COUNT(*) FROM postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND pl.CreationDate<='2013-11-13 10:12:40'::timestamp AND p.Score>=2 AND p.Score<=2 AND p.AnswerCount<=1 AND p.CommentCount=2 AND p.FavoriteCount>=1 AND p.FavoriteCount<=2 AND u.Views<=0 AND u.DownVotes>=0 AND u.CreationDate>='2013-07-15 02:31:25'::timestamp AND u.CreationDate<='2013-03-18 20:44:37'::timestamp AND b.Date<='2013-04-05 02:42:47'::timestamp;

-- train q41
SELECT COUNT(*) FROM postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND pl.CreationDate<='2013-06-08 16:55:53'::timestamp AND p.Score>=0 AND p.Score<=9 AND p.AnswerCount<=1 AND p.CommentCount=0 AND p.FavoriteCount>=1 AND p.FavoriteCount<=2 AND u.Views<=1 AND u.DownVotes>=0 AND u.CreationDate>='2010-07-19 06:55:26'::timestamp AND u.CreationDate<='2014-06-03 08:58:38'::timestamp AND b.Date<='2014-05-19 14:48:50'::timestamp;

-- train q41
SELECT COUNT(*) FROM postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND pl.CreationDate<='2013-07-24 12:34:55'::timestamp AND p.Score>=1 AND p.Score<=192 AND p.AnswerCount<=1 AND p.CommentCount=2 AND p.FavoriteCount>=1 AND p.FavoriteCount<=1 AND u.Views<=0 AND u.DownVotes>=0 AND u.CreationDate>='2011-02-11 21:16:21'::timestamp AND u.CreationDate<='2014-02-16 15:09:45'::timestamp AND b.Date<='2013-01-30 21:16:06'::timestamp;

-- train q41
SELECT COUNT(*) FROM postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND pl.CreationDate<='2014-06-25 13:05:06'::timestamp AND p.Score>=2 AND p.Score<=3 AND p.AnswerCount<=1 AND p.CommentCount=1 AND p.FavoriteCount>=0 AND p.FavoriteCount<=1 AND u.Views<=0 AND u.DownVotes>=0 AND u.CreationDate>='2013-01-16 20:29:10'::timestamp AND u.CreationDate<='2014-08-08 12:49:32'::timestamp AND b.Date<='2014-01-14 22:31:05'::timestamp;

-- train q41
SELECT COUNT(*) FROM postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND pl.CreationDate<='2014-02-08 14:31:25'::timestamp AND p.Score>=1 AND p.Score<=9 AND p.AnswerCount<=1 AND p.CommentCount=4 AND p.FavoriteCount>=0 AND p.FavoriteCount<=1 AND u.Views<=8 AND u.DownVotes>=0 AND u.CreationDate>='2012-07-30 02:08:59'::timestamp AND u.CreationDate<='2013-07-15 02:31:25'::timestamp AND b.Date<='2013-06-02 08:30:40'::timestamp;

-- train q41
SELECT COUNT(*) FROM postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND pl.CreationDate<='2014-09-13 20:54:31'::timestamp AND p.Score>=0 AND p.Score<=9 AND p.AnswerCount<=1 AND p.CommentCount=0 AND p.FavoriteCount>=1 AND p.FavoriteCount<=8 AND u.Views<=0 AND u.DownVotes>=0 AND u.CreationDate>='2013-07-15 02:31:25'::timestamp AND u.CreationDate<='2014-08-08 12:49:32'::timestamp AND b.Date<='2014-08-01 17:46:43'::timestamp;

-- train q41
SELECT COUNT(*) FROM postLinks as pl, posts as p, users as u, badges as b WHERE p.Id = pl.RelatedPostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND pl.CreationDate<='2013-06-08 16:55:53'::timestamp AND p.Score>=1 AND p.Score<=4 AND p.AnswerCount<=1 AND p.CommentCount=4 AND p.FavoriteCount>=0 AND p.FavoriteCount<=1 AND u.Views<=1 AND u.DownVotes>=0 AND u.CreationDate>='2012-07-30 02:08:59'::timestamp AND u.CreationDate<='2014-08-08 12:49:32'::timestamp AND b.Date<='2014-02-24 20:20:58'::timestamp;

-- train q7
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl WHERE c.UserId = p.OwnerUserId AND p.Id = pl.PostId AND p.CommentCount<=2 AND p.CreationDate>='2011-04-26 07:24:16'::timestamp AND p.CreationDate<='2013-12-14 13:32:25'::timestamp;

-- train q7
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl WHERE c.UserId = p.OwnerUserId AND p.Id = pl.PostId AND p.CommentCount<=5 AND p.CreationDate>='2009-02-02 14:21:12'::timestamp AND p.CreationDate<='2013-02-15 03:31:21'::timestamp;

-- train q7
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl WHERE c.UserId = p.OwnerUserId AND p.Id = pl.PostId AND p.CommentCount<=3 AND p.CreationDate>='2010-11-26 16:46:09'::timestamp AND p.CreationDate<='2013-10-20 19:44:22'::timestamp;

-- train q7
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl WHERE c.UserId = p.OwnerUserId AND p.Id = pl.PostId AND p.CommentCount<=4 AND p.CreationDate>='2009-02-02 14:21:12'::timestamp AND p.CreationDate<='2014-03-25 12:00:30'::timestamp;

-- train q7
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl WHERE c.UserId = p.OwnerUserId AND p.Id = pl.PostId AND p.CommentCount<=1 AND p.CreationDate>='2009-02-02 14:21:12'::timestamp AND p.CreationDate<='2013-02-15 03:31:21'::timestamp;

-- train q7
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl WHERE c.UserId = p.OwnerUserId AND p.Id = pl.PostId AND p.CommentCount<=2 AND p.CreationDate>='2012-06-19 22:27:44'::timestamp AND p.CreationDate<='2013-02-15 03:31:21'::timestamp;

-- train q7
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl WHERE c.UserId = p.OwnerUserId AND p.Id = pl.PostId AND p.CommentCount<=1 AND p.CreationDate>='2010-11-26 16:46:09'::timestamp AND p.CreationDate<='2014-03-25 12:00:30'::timestamp;

-- train q7
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl WHERE c.UserId = p.OwnerUserId AND p.Id = pl.PostId AND p.CommentCount<=2 AND p.CreationDate>='2009-02-02 14:21:12'::timestamp AND p.CreationDate<='2014-05-06 17:16:49'::timestamp;

-- train q7
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl WHERE c.UserId = p.OwnerUserId AND p.Id = pl.PostId AND p.CommentCount<=1 AND p.CreationDate>='2011-08-31 21:28:05'::timestamp AND p.CreationDate<='2014-06-18 20:06:36'::timestamp;

-- train q7
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl WHERE c.UserId = p.OwnerUserId AND p.Id = pl.PostId AND p.CommentCount<=5 AND p.CreationDate>='2013-02-15 03:31:21'::timestamp AND p.CreationDate<='2012-11-26 23:54:07'::timestamp;

-- train q8
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl WHERE c.UserId = p.OwnerUserId AND p.Id = pl.PostId AND c.Score=1 AND p.CreationDate>='2012-03-28 16:24:28'::timestamp AND p.CreationDate<='2013-12-14 13:32:25'::timestamp AND pl.LinkTypeId=1 AND pl.CreationDate>='2012-03-15 19:56:37'::timestamp;

-- train q8
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl WHERE c.UserId = p.OwnerUserId AND p.Id = pl.PostId AND c.Score=0 AND p.CreationDate>='2010-11-26 16:46:09'::timestamp AND p.CreationDate<='2013-04-21 19:38:59'::timestamp AND pl.LinkTypeId=1 AND pl.CreationDate>='2013-11-13 10:12:40'::timestamp;

-- train q8
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl WHERE c.UserId = p.OwnerUserId AND p.Id = pl.PostId AND c.Score=4 AND p.CreationDate>='2011-08-31 21:28:05'::timestamp AND p.CreationDate<='2013-12-14 13:32:25'::timestamp AND pl.LinkTypeId=1 AND pl.CreationDate>='2013-02-18 03:03:17'::timestamp;

-- train q8
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl WHERE c.UserId = p.OwnerUserId AND p.Id = pl.PostId AND c.Score=3 AND p.CreationDate>='2012-09-11 15:29:12'::timestamp AND p.CreationDate<='2013-12-14 13:32:25'::timestamp AND pl.LinkTypeId=3 AND pl.CreationDate>='2013-06-08 16:55:53'::timestamp;

-- train q8
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl WHERE c.UserId = p.OwnerUserId AND p.Id = pl.PostId AND c.Score=1 AND p.CreationDate>='2012-06-19 22:27:44'::timestamp AND p.CreationDate<='2013-02-15 03:31:21'::timestamp AND pl.LinkTypeId=3 AND pl.CreationDate>='2012-03-15 19:56:37'::timestamp;

-- train q8
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl WHERE c.UserId = p.OwnerUserId AND p.Id = pl.PostId AND c.Score=0 AND p.CreationDate>='2013-06-23 12:50:00'::timestamp AND p.CreationDate<='2013-08-20 10:19:52'::timestamp AND pl.LinkTypeId=3 AND pl.CreationDate>='2013-02-18 03:03:17'::timestamp;

-- train q8
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl WHERE c.UserId = p.OwnerUserId AND p.Id = pl.PostId AND c.Score=3 AND p.CreationDate>='2013-06-23 12:50:00'::timestamp AND p.CreationDate<='2013-02-15 03:31:21'::timestamp AND pl.LinkTypeId=3 AND pl.CreationDate>='2011-05-10 17:09:25'::timestamp;

-- train q8
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl WHERE c.UserId = p.OwnerUserId AND p.Id = pl.PostId AND c.Score=0 AND p.CreationDate>='2012-06-19 22:27:44'::timestamp AND p.CreationDate<='2014-05-06 17:16:49'::timestamp AND pl.LinkTypeId=3 AND pl.CreationDate>='2011-05-10 17:09:25'::timestamp;

-- train q8
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl WHERE c.UserId = p.OwnerUserId AND p.Id = pl.PostId AND c.Score=0 AND p.CreationDate>='2012-03-28 16:24:28'::timestamp AND p.CreationDate<='2013-06-23 12:50:00'::timestamp AND pl.LinkTypeId=1 AND pl.CreationDate>='2013-02-18 03:03:17'::timestamp;

-- train q8
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl WHERE c.UserId = p.OwnerUserId AND p.Id = pl.PostId AND c.Score=4 AND p.CreationDate>='2011-08-31 21:28:05'::timestamp AND p.CreationDate<='2014-05-06 17:16:49'::timestamp AND pl.LinkTypeId=1 AND pl.CreationDate>='2013-06-08 16:55:53'::timestamp;

-- train q15
SELECT COUNT(*) FROM comments as c, badges as b, users as u WHERE u.Id = c.UserId AND c.UserId = b.UserId AND c.Score=1 AND c.CreationDate>='2009-02-02 14:45:19'::timestamp AND b.Date>='2013-01-30 21:16:06'::timestamp AND b.Date<='2014-08-01 17:46:43'::timestamp AND u.UpVotes>=0;

-- train q15
SELECT COUNT(*) FROM comments as c, badges as b, users as u WHERE u.Id = c.UserId AND c.UserId = b.UserId AND c.Score=1 AND c.CreationDate>='2011-10-04 13:23:07'::timestamp AND b.Date>='2010-07-19 19:39:07'::timestamp AND b.Date<='2013-01-30 21:16:06'::timestamp AND u.UpVotes>=0;

-- train q15
SELECT COUNT(*) FROM comments as c, badges as b, users as u WHERE u.Id = c.UserId AND c.UserId = b.UserId AND c.Score=1 AND c.CreationDate>='2013-06-17 03:23:06'::timestamp AND b.Date>='2012-06-11 05:01:45'::timestamp AND b.Date<='2013-07-29 16:38:09'::timestamp AND u.UpVotes>=0;

-- train q15
SELECT COUNT(*) FROM comments as c, badges as b, users as u WHERE u.Id = c.UserId AND c.UserId = b.UserId AND c.Score=2 AND c.CreationDate>='2012-08-31 10:10:28'::timestamp AND b.Date>='2013-07-29 16:38:09'::timestamp AND b.Date<='2013-11-18 20:16:10'::timestamp AND u.UpVotes>=0;

-- train q15
SELECT COUNT(*) FROM comments as c, badges as b, users as u WHERE u.Id = c.UserId AND c.UserId = b.UserId AND c.Score=3 AND c.CreationDate>='2012-01-28 20:58:07'::timestamp AND b.Date>='2012-06-11 05:01:45'::timestamp AND b.Date<='2013-07-29 16:38:09'::timestamp AND u.UpVotes>=0;

-- train q15
SELECT COUNT(*) FROM comments as c, badges as b, users as u WHERE u.Id = c.UserId AND c.UserId = b.UserId AND c.Score=2 AND c.CreationDate>='2013-08-10 10:42:04'::timestamp AND b.Date>='2012-06-11 05:01:45'::timestamp AND b.Date<='2013-07-29 16:38:09'::timestamp AND u.UpVotes>=0;

-- train q15
SELECT COUNT(*) FROM comments as c, badges as b, users as u WHERE u.Id = c.UserId AND c.UserId = b.UserId AND c.Score=4 AND c.CreationDate>='2012-06-25 15:50:24'::timestamp AND b.Date>='2011-11-17 23:53:38'::timestamp AND b.Date<='2014-05-19 14:48:50'::timestamp AND u.UpVotes>=0;

-- train q15
SELECT COUNT(*) FROM comments as c, badges as b, users as u WHERE u.Id = c.UserId AND c.UserId = b.UserId AND c.Score=3 AND c.CreationDate>='2012-08-31 10:10:28'::timestamp AND b.Date>='2011-01-04 03:20:19'::timestamp AND b.Date<='2014-02-24 20:20:58'::timestamp AND u.UpVotes>=0;

-- train q15
SELECT COUNT(*) FROM comments as c, badges as b, users as u WHERE u.Id = c.UserId AND c.UserId = b.UserId AND c.Score=2 AND c.CreationDate>='2012-08-31 10:10:28'::timestamp AND b.Date>='2013-04-05 02:42:47'::timestamp AND b.Date<='2013-11-18 20:16:10'::timestamp AND u.UpVotes>=0;

-- train q15
SELECT COUNT(*) FROM comments as c, badges as b, users as u WHERE u.Id = c.UserId AND c.UserId = b.UserId AND c.Score=3 AND c.CreationDate>='2012-06-25 15:50:24'::timestamp AND b.Date>='2013-09-24 02:09:52'::timestamp AND b.Date<='2014-09-14 02:31:28'::timestamp AND u.UpVotes>=0;

-- train q16
SELECT COUNT(*) FROM comments as c, badges as b, users as u WHERE u.Id = c.UserId AND c.UserId = b.UserId AND c.Score=2 AND b.Date>='2012-11-18 13:52:29'::timestamp AND u.DownVotes>=0 AND u.UpVotes>=0 AND u.UpVotes<=0 AND u.CreationDate>='2011-12-27 18:54:39'::timestamp AND u.CreationDate<='2014-03-25 19:35:42'::timestamp;

-- train q16
SELECT COUNT(*) FROM comments as c, badges as b, users as u WHERE u.Id = c.UserId AND c.UserId = b.UserId AND c.Score=0 AND b.Date>='2012-09-06 10:09:57'::timestamp AND u.DownVotes>=0 AND u.UpVotes>=0 AND u.UpVotes<=0 AND u.CreationDate>='2011-12-27 18:54:39'::timestamp AND u.CreationDate<='2014-07-07 19:57:03'::timestamp;

-- train q16
SELECT COUNT(*) FROM comments as c, badges as b, users as u WHERE u.Id = c.UserId AND c.UserId = b.UserId AND c.Score=1 AND b.Date>='2010-07-19 19:39:07'::timestamp AND u.DownVotes>=0 AND u.UpVotes>=0 AND u.UpVotes<=0 AND u.CreationDate>='2013-07-15 02:31:25'::timestamp AND u.CreationDate<='2013-07-15 02:31:25'::timestamp;

-- train q16
SELECT COUNT(*) FROM comments as c, badges as b, users as u WHERE u.Id = c.UserId AND c.UserId = b.UserId AND c.Score=4 AND b.Date>='2012-03-11 12:03:02'::timestamp AND u.DownVotes>=0 AND u.UpVotes>=0 AND u.UpVotes<=0 AND u.CreationDate>='2013-05-14 19:05:54'::timestamp AND u.CreationDate<='2013-11-15 10:35:16'::timestamp;

-- train q16
SELECT COUNT(*) FROM comments as c, badges as b, users as u WHERE u.Id = c.UserId AND c.UserId = b.UserId AND c.Score=0 AND b.Date>='2011-07-01 17:18:21'::timestamp AND u.DownVotes>=0 AND u.UpVotes>=0 AND u.UpVotes<=3 AND u.CreationDate>='2013-07-15 02:31:25'::timestamp AND u.CreationDate<='2013-03-18 20:44:37'::timestamp;

-- train q16
SELECT COUNT(*) FROM comments as c, badges as b, users as u WHERE u.Id = c.UserId AND c.UserId = b.UserId AND c.Score=3 AND b.Date>='2012-11-18 13:52:29'::timestamp AND u.DownVotes>=0 AND u.UpVotes>=0 AND u.UpVotes<=9 AND u.CreationDate>='2012-04-12 19:30:57'::timestamp AND u.CreationDate<='2013-09-19 17:00:57'::timestamp;

-- train q16
SELECT COUNT(*) FROM comments as c, badges as b, users as u WHERE u.Id = c.UserId AND c.UserId = b.UserId AND c.Score=2 AND b.Date>='2012-06-11 05:01:45'::timestamp AND u.DownVotes>=0 AND u.UpVotes>=0 AND u.UpVotes<=11442 AND u.CreationDate>='2011-02-11 21:16:21'::timestamp AND u.CreationDate<='2014-01-10 18:05:07'::timestamp;

-- train q16
SELECT COUNT(*) FROM comments as c, badges as b, users as u WHERE u.Id = c.UserId AND c.UserId = b.UserId AND c.Score=3 AND b.Date>='2012-09-06 10:09:57'::timestamp AND u.DownVotes>=0 AND u.UpVotes>=0 AND u.UpVotes<=0 AND u.CreationDate>='2013-05-14 19:05:54'::timestamp AND u.CreationDate<='2013-11-15 10:35:16'::timestamp;

-- train q16
SELECT COUNT(*) FROM comments as c, badges as b, users as u WHERE u.Id = c.UserId AND c.UserId = b.UserId AND c.Score=4 AND b.Date>='2010-07-19 19:39:07'::timestamp AND u.DownVotes>=0 AND u.UpVotes>=0 AND u.UpVotes<=11442 AND u.CreationDate>='2010-07-19 06:55:26'::timestamp AND u.CreationDate<='2014-04-28 14:38:07'::timestamp;

-- train q16
SELECT COUNT(*) FROM comments as c, badges as b, users as u WHERE u.Id = c.UserId AND c.UserId = b.UserId AND c.Score=2 AND b.Date>='2011-11-17 23:53:38'::timestamp AND u.DownVotes>=0 AND u.UpVotes>=0 AND u.UpVotes<=0 AND u.CreationDate>='2012-04-12 19:30:57'::timestamp AND u.CreationDate<='2014-01-10 18:05:07'::timestamp;

-- train q25
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, votes as v WHERE p.Id = c.PostId AND c.PostId = pl.PostId AND pl.PostId = v.PostId AND c.CreationDate>='2012-06-25 15:50:24'::timestamp AND p.Score>=2 AND v.VoteTypeId=2 AND v.CreationDate<='2013-04-26 00:00:00'::timestamp;

-- train q25
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, votes as v WHERE p.Id = c.PostId AND c.PostId = pl.PostId AND pl.PostId = v.PostId AND c.CreationDate>='2012-01-28 20:58:07'::timestamp AND p.Score>=2 AND v.VoteTypeId=16 AND v.CreationDate<='2012-09-14 00:00:00'::timestamp;

-- train q25
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, votes as v WHERE p.Id = c.PostId AND c.PostId = pl.PostId AND pl.PostId = v.PostId AND c.CreationDate>='2013-08-10 10:42:04'::timestamp AND p.Score>=0 AND v.VoteTypeId=2 AND v.CreationDate<='2013-06-29 00:00:00'::timestamp;

-- train q25
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, votes as v WHERE p.Id = c.PostId AND c.PostId = pl.PostId AND pl.PostId = v.PostId AND c.CreationDate>='2013-02-02 20:12:58'::timestamp AND p.Score>=1 AND v.VoteTypeId=2 AND v.CreationDate<='2014-04-08 00:00:00'::timestamp;

-- train q25
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, votes as v WHERE p.Id = c.PostId AND c.PostId = pl.PostId AND pl.PostId = v.PostId AND c.CreationDate>='2011-01-19 20:40:12'::timestamp AND p.Score>=1 AND v.VoteTypeId=5 AND v.CreationDate<='2014-02-18 00:00:00'::timestamp;

-- train q25
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, votes as v WHERE p.Id = c.PostId AND c.PostId = pl.PostId AND pl.PostId = v.PostId AND c.CreationDate>='2013-08-10 10:42:04'::timestamp AND p.Score>=1 AND v.VoteTypeId=1 AND v.CreationDate<='2013-02-12 00:00:00'::timestamp;

-- train q25
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, votes as v WHERE p.Id = c.PostId AND c.PostId = pl.PostId AND pl.PostId = v.PostId AND c.CreationDate>='2012-11-12 19:23:15'::timestamp AND p.Score>=1 AND v.VoteTypeId=3 AND v.CreationDate<='2014-01-04 00:00:00'::timestamp;

-- train q25
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, votes as v WHERE p.Id = c.PostId AND c.PostId = pl.PostId AND pl.PostId = v.PostId AND c.CreationDate>='2012-11-12 19:23:15'::timestamp AND p.Score>=0 AND v.VoteTypeId=2 AND v.CreationDate<='2014-04-08 00:00:00'::timestamp;

-- train q25
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, votes as v WHERE p.Id = c.PostId AND c.PostId = pl.PostId AND pl.PostId = v.PostId AND c.CreationDate>='2013-02-02 20:12:58'::timestamp AND p.Score>=1 AND v.VoteTypeId=16 AND v.CreationDate<='2013-08-24 00:00:00'::timestamp;

-- train q25
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, votes as v WHERE p.Id = c.PostId AND c.PostId = pl.PostId AND pl.PostId = v.PostId AND c.CreationDate>='2011-10-04 13:23:07'::timestamp AND p.Score>=2 AND v.VoteTypeId=5 AND v.CreationDate<='2014-04-08 00:00:00'::timestamp;

-- train q28
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE p.Id = v.PostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND p.Score<=1 AND u.Reputation>=16;

-- train q28
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE p.Id = v.PostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND p.Score<=2 AND u.Reputation>=6;

-- train q28
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE p.Id = v.PostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND p.Score<=5 AND u.Reputation>=16;

-- train q28
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE p.Id = v.PostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND p.Score<=2 AND u.Reputation>=1;

-- train q28
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE p.Id = v.PostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND p.Score<=2 AND u.Reputation>=11;

-- train q28
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE p.Id = v.PostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND p.Score<=3 AND u.Reputation>=26;

-- train q28
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE p.Id = v.PostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND p.Score<=3 AND u.Reputation>=1;

-- train q28
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE p.Id = v.PostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND p.Score<=1 AND u.Reputation>=1;

-- train q28
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE p.Id = v.PostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND p.Score<=2 AND u.Reputation>=16;

-- train q28
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE p.Id = v.PostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND p.Score<=4 AND u.Reputation>=6;

-- train q29
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE p.Id = v.PostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND v.CreationDate<='2014-02-18 00:00:00'::timestamp AND p.PostTypeId=2 AND p.Score>=1 AND p.FavoriteCount>=1 AND p.FavoriteCount<=3 AND b.Date>='2012-03-11 12:03:02'::timestamp AND b.Date<='2013-04-05 02:42:47'::timestamp AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.CreationDate>='2012-10-29 20:13:00'::timestamp;

-- train q29
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE p.Id = v.PostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND v.CreationDate<='2013-10-28 00:00:00'::timestamp AND p.PostTypeId=2 AND p.Score>=-19 AND p.FavoriteCount>=0 AND p.FavoriteCount<=2 AND b.Date>='2013-06-02 08:30:40'::timestamp AND b.Date<='2013-07-29 16:38:09'::timestamp AND u.DownVotes<=1920 AND u.UpVotes>=0 AND u.CreationDate>='2011-02-11 21:16:21'::timestamp;

-- train q29
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE p.Id = v.PostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND v.CreationDate<='2014-01-04 00:00:00'::timestamp AND p.PostTypeId=4 AND p.Score>=1 AND p.FavoriteCount>=1 AND p.FavoriteCount<=2 AND b.Date>='2013-07-29 16:38:09'::timestamp AND b.Date<='2013-07-29 16:38:09'::timestamp AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.CreationDate>='2011-12-27 18:54:39'::timestamp;

-- train q29
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE p.Id = v.PostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND v.CreationDate<='2013-10-28 00:00:00'::timestamp AND p.PostTypeId=5 AND p.Score>=2 AND p.FavoriteCount>=2 AND p.FavoriteCount<=5 AND b.Date>='2013-04-05 02:42:47'::timestamp AND b.Date<='2014-04-07 18:51:36'::timestamp AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.CreationDate>='2013-07-15 02:31:25'::timestamp;

-- train q29
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE p.Id = v.PostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND v.CreationDate<='2013-10-28 00:00:00'::timestamp AND p.PostTypeId=4 AND p.Score>=0 AND p.FavoriteCount>=1 AND p.FavoriteCount<=233 AND b.Date>='2011-11-17 23:53:38'::timestamp AND b.Date<='2014-01-14 22:31:05'::timestamp AND u.DownVotes<=1920 AND u.UpVotes>=0 AND u.CreationDate>='2013-03-18 20:44:37'::timestamp;

-- train q29
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE p.Id = v.PostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND v.CreationDate<='2013-02-12 00:00:00'::timestamp AND p.PostTypeId=4 AND p.Score>=2 AND p.FavoriteCount>=1 AND p.FavoriteCount<=3 AND b.Date>='2013-07-29 16:38:09'::timestamp AND b.Date<='2014-06-30 18:24:30'::timestamp AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.CreationDate>='2013-05-14 19:05:54'::timestamp;

-- train q29
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE p.Id = v.PostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND v.CreationDate<='2014-09-14 00:00:00'::timestamp AND p.PostTypeId=2 AND p.Score>=2 AND p.FavoriteCount>=1 AND p.FavoriteCount<=1 AND b.Date>='2012-03-11 12:03:02'::timestamp AND b.Date<='2013-06-02 08:30:40'::timestamp AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.CreationDate>='2013-11-15 10:35:16'::timestamp;

-- train q29
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE p.Id = v.PostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND v.CreationDate<='2012-11-26 00:00:00'::timestamp AND p.PostTypeId=6 AND p.Score>=1 AND p.FavoriteCount>=1 AND p.FavoriteCount<=2 AND b.Date>='2013-06-02 08:30:40'::timestamp AND b.Date<='2014-06-30 18:24:30'::timestamp AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.CreationDate>='2012-04-12 19:30:57'::timestamp;

-- train q29
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE p.Id = v.PostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND v.CreationDate<='2014-04-08 00:00:00'::timestamp AND p.PostTypeId=1 AND p.Score>=1 AND p.FavoriteCount>=1 AND p.FavoriteCount<=8 AND b.Date>='2013-06-02 08:30:40'::timestamp AND b.Date<='2014-08-01 17:46:43'::timestamp AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.CreationDate>='2011-12-27 18:54:39'::timestamp;

-- train q29
SELECT COUNT(*) FROM votes as v, posts as p, badges as b, users as u WHERE p.Id = v.PostId AND u.Id = p.OwnerUserId AND u.Id = b.UserId AND v.CreationDate<='2014-05-30 00:00:00'::timestamp AND p.PostTypeId=4 AND p.Score>=-19 AND p.FavoriteCount>=2 AND p.FavoriteCount<=1 AND b.Date>='2012-09-06 10:09:57'::timestamp AND b.Date<='2014-01-14 22:31:05'::timestamp AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.CreationDate>='2012-10-29 20:13:00'::timestamp;

-- train q21
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.PostId = p.Id AND v.UserId = u.Id AND v.CreationDate<='2013-02-12 00:00:00'::timestamp AND p.Score>=1 AND p.CreationDate>='2013-06-23 12:50:00'::timestamp AND p.CreationDate<='2013-12-14 13:32:25'::timestamp AND u.UpVotes>=0 AND u.CreationDate>='2012-04-12 19:30:57'::timestamp AND u.CreationDate<='2014-03-25 19:35:42'::timestamp;

-- train q21
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.PostId = p.Id AND v.UserId = u.Id AND v.CreationDate<='2013-04-26 00:00:00'::timestamp AND p.Score>=2 AND p.CreationDate>='2012-09-11 15:29:12'::timestamp AND p.CreationDate<='2014-09-14 02:09:23'::timestamp AND u.UpVotes>=0 AND u.CreationDate>='2011-08-15 19:36:17'::timestamp AND u.CreationDate<='2014-04-28 14:38:07'::timestamp;

-- train q21
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.PostId = p.Id AND v.UserId = u.Id AND v.CreationDate<='2014-02-18 00:00:00'::timestamp AND p.Score>=0 AND p.CreationDate>='2013-08-20 10:19:52'::timestamp AND p.CreationDate<='2012-11-26 23:54:07'::timestamp AND u.UpVotes>=0 AND u.CreationDate>='2012-07-30 02:08:59'::timestamp AND u.CreationDate<='2014-06-03 08:58:38'::timestamp;

-- train q21
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.PostId = p.Id AND v.UserId = u.Id AND v.CreationDate<='2013-10-28 00:00:00'::timestamp AND p.Score>=0 AND p.CreationDate>='2011-04-26 07:24:16'::timestamp AND p.CreationDate<='2014-09-14 02:09:23'::timestamp AND u.UpVotes>=0 AND u.CreationDate>='2011-12-27 18:54:39'::timestamp AND u.CreationDate<='2013-09-19 17:00:57'::timestamp;

-- train q21
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.PostId = p.Id AND v.UserId = u.Id AND v.CreationDate<='2014-07-22 00:00:00'::timestamp AND p.Score>=-19 AND p.CreationDate>='2012-06-19 22:27:44'::timestamp AND p.CreationDate<='2014-08-01 05:10:10'::timestamp AND u.UpVotes>=0 AND u.CreationDate>='2012-10-29 20:13:00'::timestamp AND u.CreationDate<='2014-06-03 08:58:38'::timestamp;

-- train q21
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.PostId = p.Id AND v.UserId = u.Id AND v.CreationDate<='2014-01-04 00:00:00'::timestamp AND p.Score>=-19 AND p.CreationDate>='2012-03-28 16:24:28'::timestamp AND p.CreationDate<='2014-03-25 12:00:30'::timestamp AND u.UpVotes>=0 AND u.CreationDate>='2013-09-19 17:00:57'::timestamp AND u.CreationDate<='2013-05-14 19:05:54'::timestamp;

-- train q21
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.PostId = p.Id AND v.UserId = u.Id AND v.CreationDate<='2014-05-30 00:00:00'::timestamp AND p.Score>=1 AND p.CreationDate>='2009-02-02 14:21:12'::timestamp AND p.CreationDate<='2013-04-21 19:38:59'::timestamp AND u.UpVotes>=0 AND u.CreationDate>='2011-02-11 21:16:21'::timestamp AND u.CreationDate<='2013-05-14 19:05:54'::timestamp;

-- train q21
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.PostId = p.Id AND v.UserId = u.Id AND v.CreationDate<='2014-09-14 00:00:00'::timestamp AND p.Score>=-19 AND p.CreationDate>='2012-09-11 15:29:12'::timestamp AND p.CreationDate<='2013-04-21 19:38:59'::timestamp AND u.UpVotes>=0 AND u.CreationDate>='2013-09-19 17:00:57'::timestamp AND u.CreationDate<='2014-01-10 18:05:07'::timestamp;

-- train q21
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.PostId = p.Id AND v.UserId = u.Id AND v.CreationDate<='2013-10-28 00:00:00'::timestamp AND p.Score>=1 AND p.CreationDate>='2011-04-26 07:24:16'::timestamp AND p.CreationDate<='2013-04-21 19:38:59'::timestamp AND u.UpVotes>=0 AND u.CreationDate>='2010-07-19 06:55:26'::timestamp AND u.CreationDate<='2014-03-25 19:35:42'::timestamp;

-- train q21
SELECT COUNT(*) FROM votes as v, posts as p, users as u WHERE v.PostId = p.Id AND v.UserId = u.Id AND v.CreationDate<='2013-04-26 00:00:00'::timestamp AND p.Score>=0 AND p.CreationDate>='2013-08-20 10:19:52'::timestamp AND p.CreationDate<='2013-06-23 12:50:00'::timestamp AND u.UpVotes>=0 AND u.CreationDate>='2013-07-15 02:31:25'::timestamp AND u.CreationDate<='2014-09-14 01:01:44'::timestamp;

-- train q130
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, users as u WHERE pl.RelatedPostId = p.Id AND u.Id= c.UserId AND c.PostId = p.Id AND ph.PostId = p.Id AND c.CreationDate>='2012-11-12 19:23:15'::timestamp AND c.CreationDate<='2013-11-29 17:06:35'::timestamp AND p.CommentCount>=0 AND p.CommentCount<=45 AND pl.LinkTypeId=3 AND ph.CreationDate>='2013-05-17 08:19:48'::timestamp AND u.Reputation>=1 AND u.Reputation<=6 AND u.DownVotes>=0 AND u.DownVotes<=0;

-- train q130
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, users as u WHERE pl.RelatedPostId = p.Id AND u.Id= c.UserId AND c.PostId = p.Id AND ph.PostId = p.Id AND c.CreationDate>='2013-02-02 20:12:58'::timestamp AND c.CreationDate<='2013-10-08 14:15:52'::timestamp AND p.CommentCount>=0 AND p.CommentCount<=2 AND pl.LinkTypeId=1 AND ph.CreationDate>='2012-12-27 00:14:56'::timestamp AND u.Reputation>=11 AND u.Reputation<=116 AND u.DownVotes>=0 AND u.DownVotes<=0;

-- train q130
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, users as u WHERE pl.RelatedPostId = p.Id AND u.Id= c.UserId AND c.PostId = p.Id AND ph.PostId = p.Id AND c.CreationDate>='2009-02-02 14:45:19'::timestamp AND c.CreationDate<='2013-11-29 17:06:35'::timestamp AND p.CommentCount>=0 AND p.CommentCount<=45 AND pl.LinkTypeId=3 AND ph.CreationDate>='2011-06-11 20:40:55'::timestamp AND u.Reputation>=1 AND u.Reputation<=116 AND u.DownVotes>=0 AND u.DownVotes<=0;

-- train q130
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, users as u WHERE pl.RelatedPostId = p.Id AND u.Id= c.UserId AND c.PostId = p.Id AND ph.PostId = p.Id AND c.CreationDate>='2012-04-20 08:35:37'::timestamp AND c.CreationDate<='2014-01-24 21:50:43'::timestamp AND p.CommentCount>=1 AND p.CommentCount<=2 AND pl.LinkTypeId=3 AND ph.CreationDate>='2011-01-24 07:48:41'::timestamp AND u.Reputation>=11 AND u.Reputation<=6 AND u.DownVotes>=0 AND u.DownVotes<=1920;

-- train q130
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, users as u WHERE pl.RelatedPostId = p.Id AND u.Id= c.UserId AND c.PostId = p.Id AND ph.PostId = p.Id AND c.CreationDate>='2012-11-12 19:23:15'::timestamp AND c.CreationDate<='2014-03-10 15:31:38'::timestamp AND p.CommentCount>=0 AND p.CommentCount<=5 AND pl.LinkTypeId=1 AND ph.CreationDate>='2012-12-27 00:14:56'::timestamp AND u.Reputation>=16 AND u.Reputation<=11 AND u.DownVotes>=0 AND u.DownVotes<=0;

-- train q130
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, users as u WHERE pl.RelatedPostId = p.Id AND u.Id= c.UserId AND c.PostId = p.Id AND ph.PostId = p.Id AND c.CreationDate>='2011-01-19 20:40:12'::timestamp AND c.CreationDate<='2013-04-12 14:14:02'::timestamp AND p.CommentCount>=0 AND p.CommentCount<=45 AND pl.LinkTypeId=1 AND ph.CreationDate>='2013-09-02 11:29:35'::timestamp AND u.Reputation>=1 AND u.Reputation<=101 AND u.DownVotes>=0 AND u.DownVotes<=0;

-- train q130
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, users as u WHERE pl.RelatedPostId = p.Id AND u.Id= c.UserId AND c.PostId = p.Id AND ph.PostId = p.Id AND c.CreationDate>='2012-06-25 15:50:24'::timestamp AND c.CreationDate<='2014-09-14 02:04:27'::timestamp AND p.CommentCount>=0 AND p.CommentCount<=1 AND pl.LinkTypeId=3 AND ph.CreationDate>='2013-09-02 11:29:35'::timestamp AND u.Reputation>=6 AND u.Reputation<=87393 AND u.DownVotes>=0 AND u.DownVotes<=0;

-- train q130
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, users as u WHERE pl.RelatedPostId = p.Id AND u.Id= c.UserId AND c.PostId = p.Id AND ph.PostId = p.Id AND c.CreationDate>='2012-04-20 08:35:37'::timestamp AND c.CreationDate<='2013-02-02 20:12:58'::timestamp AND p.CommentCount>=0 AND p.CommentCount<=2 AND pl.LinkTypeId=3 AND ph.CreationDate>='2011-06-11 20:40:55'::timestamp AND u.Reputation>=16 AND u.Reputation<=101 AND u.DownVotes>=0 AND u.DownVotes<=0;

-- train q130
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, users as u WHERE pl.RelatedPostId = p.Id AND u.Id= c.UserId AND c.PostId = p.Id AND ph.PostId = p.Id AND c.CreationDate>='2013-04-12 14:14:02'::timestamp AND c.CreationDate<='2014-09-14 02:04:27'::timestamp AND p.CommentCount>=0 AND p.CommentCount<=1 AND pl.LinkTypeId=1 AND ph.CreationDate>='2011-06-11 20:40:55'::timestamp AND u.Reputation>=6 AND u.Reputation<=51 AND u.DownVotes>=0 AND u.DownVotes<=0;

-- train q130
SELECT COUNT(*) FROM comments as c, posts as p, postLinks as pl, postHistory as ph, users as u WHERE pl.RelatedPostId = p.Id AND u.Id= c.UserId AND c.PostId = p.Id AND ph.PostId = p.Id AND c.CreationDate>='2012-08-31 10:10:28'::timestamp AND c.CreationDate<='2014-07-29 16:38:44'::timestamp AND p.CommentCount>=1 AND p.CommentCount<=7 AND pl.LinkTypeId=1 AND ph.CreationDate>='2013-09-02 11:29:35'::timestamp AND u.Reputation>=6 AND u.Reputation<=87393 AND u.DownVotes>=0 AND u.DownVotes<=1920;

-- train q55
SELECT COUNT(*) FROM tags as t, posts as p, users as u, postHistory as ph, badges as b WHERE p.Id = t.ExcerptPostId AND u.Id = ph.UserId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND p.CommentCount>=0 AND u.DownVotes<=0 AND b.Date<='2013-06-02 08:30:40'::timestamp;

-- train q55
SELECT COUNT(*) FROM tags as t, posts as p, users as u, postHistory as ph, badges as b WHERE p.Id = t.ExcerptPostId AND u.Id = ph.UserId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND p.CommentCount>=1 AND u.DownVotes<=0 AND b.Date<='2013-04-05 02:42:47'::timestamp;

-- train q55
SELECT COUNT(*) FROM tags as t, posts as p, users as u, postHistory as ph, badges as b WHERE p.Id = t.ExcerptPostId AND u.Id = ph.UserId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND p.CommentCount>=1 AND u.DownVotes<=0 AND b.Date<='2013-09-24 02:09:52'::timestamp;

-- train q55
SELECT COUNT(*) FROM tags as t, posts as p, users as u, postHistory as ph, badges as b WHERE p.Id = t.ExcerptPostId AND u.Id = ph.UserId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND p.CommentCount>=0 AND u.DownVotes<=0 AND b.Date<='2014-08-01 17:46:43'::timestamp;

-- train q55
SELECT COUNT(*) FROM tags as t, posts as p, users as u, postHistory as ph, badges as b WHERE p.Id = t.ExcerptPostId AND u.Id = ph.UserId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND p.CommentCount>=1 AND u.DownVotes<=0 AND b.Date<='2014-04-07 18:51:36'::timestamp;

-- train q55
SELECT COUNT(*) FROM tags as t, posts as p, users as u, postHistory as ph, badges as b WHERE p.Id = t.ExcerptPostId AND u.Id = ph.UserId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND p.CommentCount>=0 AND u.DownVotes<=0 AND b.Date<='2014-04-07 18:51:36'::timestamp;

-- train q55
SELECT COUNT(*) FROM tags as t, posts as p, users as u, postHistory as ph, badges as b WHERE p.Id = t.ExcerptPostId AND u.Id = ph.UserId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND p.CommentCount>=0 AND u.DownVotes<=0 AND b.Date<='2014-09-14 02:31:28'::timestamp;

-- train q55
SELECT COUNT(*) FROM tags as t, posts as p, users as u, postHistory as ph, badges as b WHERE p.Id = t.ExcerptPostId AND u.Id = ph.UserId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND p.CommentCount>=1 AND u.DownVotes<=0 AND b.Date<='2014-06-30 18:24:30'::timestamp;

-- train q55
SELECT COUNT(*) FROM tags as t, posts as p, users as u, postHistory as ph, badges as b WHERE p.Id = t.ExcerptPostId AND u.Id = ph.UserId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND p.CommentCount>=0 AND u.DownVotes<=0 AND b.Date<='2013-07-29 16:38:09'::timestamp;

-- train q55
SELECT COUNT(*) FROM tags as t, posts as p, users as u, postHistory as ph, badges as b WHERE p.Id = t.ExcerptPostId AND u.Id = ph.UserId AND u.Id = b.UserId AND u.Id = p.OwnerUserId AND p.CommentCount>=0 AND u.DownVotes<=0 AND b.Date<='2013-01-30 21:16:06'::timestamp;

-- train q104
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE v.UserId = u.Id AND c.UserId = u.Id AND ph.UserId = u.Id AND ph.CreationDate>='2009-02-02 14:21:12'::timestamp AND ph.CreationDate<='2013-07-12 12:21:54'::timestamp AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.UpVotes<=0;

-- train q104
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE v.UserId = u.Id AND c.UserId = u.Id AND ph.UserId = u.Id AND ph.CreationDate>='2012-07-25 23:18:13'::timestamp AND ph.CreationDate<='2013-10-28 01:49:50'::timestamp AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.UpVotes<=0;

-- train q104
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE v.UserId = u.Id AND c.UserId = u.Id AND ph.UserId = u.Id AND ph.CreationDate>='2013-09-02 11:29:35'::timestamp AND ph.CreationDate<='2012-12-27 00:14:56'::timestamp AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.UpVotes<=0;

-- train q104
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE v.UserId = u.Id AND c.UserId = u.Id AND ph.UserId = u.Id AND ph.CreationDate>='2011-01-24 07:48:41'::timestamp AND ph.CreationDate<='2013-03-15 11:56:33'::timestamp AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.UpVotes<=0;

-- train q104
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE v.UserId = u.Id AND c.UserId = u.Id AND ph.UserId = u.Id AND ph.CreationDate>='2012-05-07 03:11:00'::timestamp AND ph.CreationDate<='2014-06-18 13:33:16'::timestamp AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.UpVotes<=2;

-- train q104
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE v.UserId = u.Id AND c.UserId = u.Id AND ph.UserId = u.Id AND ph.CreationDate>='2012-02-07 13:46:01'::timestamp AND ph.CreationDate<='2013-12-18 19:08:02'::timestamp AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.UpVotes<=0;

-- train q104
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE v.UserId = u.Id AND c.UserId = u.Id AND ph.UserId = u.Id AND ph.CreationDate>='2013-05-17 08:19:48'::timestamp AND ph.CreationDate<='2013-09-02 11:29:35'::timestamp AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.UpVotes<=0;

-- train q104
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE v.UserId = u.Id AND c.UserId = u.Id AND ph.UserId = u.Id AND ph.CreationDate>='2012-10-11 15:16:49'::timestamp AND ph.CreationDate<='2014-03-25 00:33:01'::timestamp AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.UpVotes<=0;

-- train q104
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE v.UserId = u.Id AND c.UserId = u.Id AND ph.UserId = u.Id AND ph.CreationDate>='2013-03-15 11:56:33'::timestamp AND ph.CreationDate<='2012-12-27 00:14:56'::timestamp AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.UpVotes<=2;

-- train q104
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE v.UserId = u.Id AND c.UserId = u.Id AND ph.UserId = u.Id AND ph.CreationDate>='2013-09-02 11:29:35'::timestamp AND ph.CreationDate<='2014-09-14 02:54:13'::timestamp AND u.DownVotes<=0 AND u.UpVotes>=0 AND u.UpVotes<=0;

-- train q106
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE v.UserId = u.Id AND c.UserId = u.Id AND ph.UserId = u.Id AND c.CreationDate<='2014-07-29 16:38:44'::timestamp AND ph.PostHistoryTypeId=1 AND u.Reputation>=1 AND u.Views>=0 AND u.DownVotes>=0 AND u.UpVotes>=0 AND u.UpVotes<=0 AND u.CreationDate>='2013-11-15 10:35:16'::timestamp AND u.CreationDate<='2013-11-15 10:35:16'::timestamp;

-- train q106
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE v.UserId = u.Id AND c.UserId = u.Id AND ph.UserId = u.Id AND c.CreationDate<='2014-09-14 02:04:27'::timestamp AND ph.PostHistoryTypeId=2 AND u.Reputation>=1 AND u.Views>=1 AND u.DownVotes>=0 AND u.UpVotes>=0 AND u.UpVotes<=0 AND u.CreationDate>='2013-01-16 20:29:10'::timestamp AND u.CreationDate<='2014-08-08 12:49:32'::timestamp;

-- train q106
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE v.UserId = u.Id AND c.UserId = u.Id AND ph.UserId = u.Id AND c.CreationDate<='2014-06-12 00:55:46'::timestamp AND ph.PostHistoryTypeId=3 AND u.Reputation>=16 AND u.Views>=0 AND u.DownVotes>=0 AND u.UpVotes>=0 AND u.UpVotes<=0 AND u.CreationDate>='2013-09-19 17:00:57'::timestamp AND u.CreationDate<='2014-03-25 19:35:42'::timestamp;

-- train q106
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE v.UserId = u.Id AND c.UserId = u.Id AND ph.UserId = u.Id AND c.CreationDate<='2014-06-12 00:55:46'::timestamp AND ph.PostHistoryTypeId=5 AND u.Reputation>=1 AND u.Views>=0 AND u.DownVotes>=0 AND u.UpVotes>=0 AND u.UpVotes<=0 AND u.CreationDate>='2011-02-11 21:16:21'::timestamp AND u.CreationDate<='2013-11-15 10:35:16'::timestamp;

-- train q106
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE v.UserId = u.Id AND c.UserId = u.Id AND ph.UserId = u.Id AND c.CreationDate<='2013-10-08 14:15:52'::timestamp AND ph.PostHistoryTypeId=3 AND u.Reputation>=1 AND u.Views>=0 AND u.DownVotes>=0 AND u.UpVotes>=0 AND u.UpVotes<=0 AND u.CreationDate>='2011-02-11 21:16:21'::timestamp AND u.CreationDate<='2014-08-08 12:49:32'::timestamp;

-- train q106
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE v.UserId = u.Id AND c.UserId = u.Id AND ph.UserId = u.Id AND c.CreationDate<='2013-10-08 14:15:52'::timestamp AND ph.PostHistoryTypeId=2 AND u.Reputation>=16 AND u.Views>=0 AND u.DownVotes>=0 AND u.UpVotes>=0 AND u.UpVotes<=0 AND u.CreationDate>='2013-01-16 20:29:10'::timestamp AND u.CreationDate<='2014-08-08 12:49:32'::timestamp;

-- train q106
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE v.UserId = u.Id AND c.UserId = u.Id AND ph.UserId = u.Id AND c.CreationDate<='2014-03-10 15:31:38'::timestamp AND ph.PostHistoryTypeId=3 AND u.Reputation>=6 AND u.Views>=0 AND u.DownVotes>=0 AND u.UpVotes>=0 AND u.UpVotes<=0 AND u.CreationDate>='2011-12-27 18:54:39'::timestamp AND u.CreationDate<='2013-07-15 02:31:25'::timestamp;

-- train q106
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE v.UserId = u.Id AND c.UserId = u.Id AND ph.UserId = u.Id AND c.CreationDate<='2013-04-12 14:14:02'::timestamp AND ph.PostHistoryTypeId=1 AND u.Reputation>=1 AND u.Views>=0 AND u.DownVotes>=0 AND u.UpVotes>=0 AND u.UpVotes<=0 AND u.CreationDate>='2013-05-14 19:05:54'::timestamp AND u.CreationDate<='2014-04-28 14:38:07'::timestamp;

-- train q106
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE v.UserId = u.Id AND c.UserId = u.Id AND ph.UserId = u.Id AND c.CreationDate<='2013-06-17 03:23:06'::timestamp AND ph.PostHistoryTypeId=5 AND u.Reputation>=1 AND u.Views>=0 AND u.DownVotes>=0 AND u.UpVotes>=0 AND u.UpVotes<=11442 AND u.CreationDate>='2013-07-15 02:31:25'::timestamp AND u.CreationDate<='2014-01-10 18:05:07'::timestamp;

-- train q106
SELECT COUNT(*) FROM comments as c, postHistory as ph, votes as v, users as u WHERE v.UserId = u.Id AND c.UserId = u.Id AND ph.UserId = u.Id AND c.CreationDate<='2014-03-10 15:31:38'::timestamp AND ph.PostHistoryTypeId=1 AND u.Reputation>=6 AND u.Views>=0 AND u.DownVotes>=0 AND u.UpVotes>=0 AND u.UpVotes<=0 AND u.CreationDate>='2011-12-27 18:54:39'::timestamp AND u.CreationDate<='2014-07-07 19:57:03'::timestamp;

-- train q49
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, badges as b, users as u WHERE u.Id =c.UserId AND c.UserId = p.OwnerUserId AND p.OwnerUserId = v.UserId AND v.UserId = b.UserId AND c.Score=0 AND p.Score>=1 AND p.Score<=3 AND p.CreationDate>='2010-11-26 16:46:09'::timestamp AND p.CreationDate<='2013-10-20 19:44:22'::timestamp AND v.BountyAmount<=500 AND b.Date<='2014-04-07 18:51:36'::timestamp AND u.DownVotes<=0 AND u.CreationDate>='2013-11-15 10:35:16'::timestamp AND u.CreationDate<='2013-03-18 20:44:37'::timestamp;

-- train q49
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, badges as b, users as u WHERE u.Id =c.UserId AND c.UserId = p.OwnerUserId AND p.OwnerUserId = v.UserId AND v.UserId = b.UserId AND c.Score=0 AND p.Score>=2 AND p.Score<=9 AND p.CreationDate>='2011-12-23 07:35:55'::timestamp AND p.CreationDate<='2013-04-21 19:38:59'::timestamp AND v.BountyAmount<=100 AND b.Date<='2014-08-01 17:46:43'::timestamp AND u.DownVotes<=0 AND u.CreationDate>='2013-07-15 02:31:25'::timestamp AND u.CreationDate<='2014-04-28 14:38:07'::timestamp;

-- train q49
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, badges as b, users as u WHERE u.Id =c.UserId AND c.UserId = p.OwnerUserId AND p.OwnerUserId = v.UserId AND v.UserId = b.UserId AND c.Score=4 AND p.Score>=0 AND p.Score<=5 AND p.CreationDate>='2011-12-23 07:35:55'::timestamp AND p.CreationDate<='2014-05-06 17:16:49'::timestamp AND v.BountyAmount<=50 AND b.Date<='2014-01-14 22:31:05'::timestamp AND u.DownVotes<=0 AND u.CreationDate>='2011-12-27 18:54:39'::timestamp AND u.CreationDate<='2014-08-08 12:49:32'::timestamp;

-- train q49
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, badges as b, users as u WHERE u.Id =c.UserId AND c.UserId = p.OwnerUserId AND p.OwnerUserId = v.UserId AND v.UserId = b.UserId AND c.Score=1 AND p.Score>=0 AND p.Score<=3 AND p.CreationDate>='2013-06-23 12:50:00'::timestamp AND p.CreationDate<='2013-12-14 13:32:25'::timestamp AND v.BountyAmount<=50 AND b.Date<='2014-09-14 02:31:28'::timestamp AND u.DownVotes<=0 AND u.CreationDate>='2012-04-12 19:30:57'::timestamp AND u.CreationDate<='2014-07-07 19:57:03'::timestamp;

-- train q49
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, badges as b, users as u WHERE u.Id =c.UserId AND c.UserId = p.OwnerUserId AND p.OwnerUserId = v.UserId AND v.UserId = b.UserId AND c.Score=3 AND p.Score>=1 AND p.Score<=5 AND p.CreationDate>='2009-02-02 14:21:12'::timestamp AND p.CreationDate<='2014-06-18 20:06:36'::timestamp AND v.BountyAmount<=50 AND b.Date<='2014-08-01 17:46:43'::timestamp AND u.DownVotes<=0 AND u.CreationDate>='2013-05-14 19:05:54'::timestamp AND u.CreationDate<='2013-09-19 17:00:57'::timestamp;

-- train q49
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, badges as b, users as u WHERE u.Id =c.UserId AND c.UserId = p.OwnerUserId AND p.OwnerUserId = v.UserId AND v.UserId = b.UserId AND c.Score=0 AND p.Score>=1 AND p.Score<=2 AND p.CreationDate>='2012-11-26 23:54:07'::timestamp AND p.CreationDate<='2014-05-06 17:16:49'::timestamp AND v.BountyAmount<=100 AND b.Date<='2013-11-18 20:16:10'::timestamp AND u.DownVotes<=0 AND u.CreationDate>='2013-01-16 20:29:10'::timestamp AND u.CreationDate<='2013-09-19 17:00:57'::timestamp;

-- train q49
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, badges as b, users as u WHERE u.Id =c.UserId AND c.UserId = p.OwnerUserId AND p.OwnerUserId = v.UserId AND v.UserId = b.UserId AND c.Score=2 AND p.Score>=0 AND p.Score<=5 AND p.CreationDate>='2012-06-19 22:27:44'::timestamp AND p.CreationDate<='2014-05-06 17:16:49'::timestamp AND v.BountyAmount<=100 AND b.Date<='2014-08-01 17:46:43'::timestamp AND u.DownVotes<=0 AND u.CreationDate>='2013-01-16 20:29:10'::timestamp AND u.CreationDate<='2014-09-14 01:01:44'::timestamp;

-- train q49
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, badges as b, users as u WHERE u.Id =c.UserId AND c.UserId = p.OwnerUserId AND p.OwnerUserId = v.UserId AND v.UserId = b.UserId AND c.Score=3 AND p.Score>=0 AND p.Score<=192 AND p.CreationDate>='2013-06-23 12:50:00'::timestamp AND p.CreationDate<='2014-03-25 12:00:30'::timestamp AND v.BountyAmount<=50 AND b.Date<='2014-04-07 18:51:36'::timestamp AND u.DownVotes<=0 AND u.CreationDate>='2010-07-19 06:55:26'::timestamp AND u.CreationDate<='2013-09-19 17:00:57'::timestamp;

-- train q49
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, badges as b, users as u WHERE u.Id =c.UserId AND c.UserId = p.OwnerUserId AND p.OwnerUserId = v.UserId AND v.UserId = b.UserId AND c.Score=0 AND p.Score>=0 AND p.Score<=1 AND p.CreationDate>='2009-02-02 14:21:12'::timestamp AND p.CreationDate<='2014-08-01 05:10:10'::timestamp AND v.BountyAmount<=150 AND b.Date<='2013-11-18 20:16:10'::timestamp AND u.DownVotes<=0 AND u.CreationDate>='2013-07-15 02:31:25'::timestamp AND u.CreationDate<='2013-03-18 20:44:37'::timestamp;

-- train q49
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, badges as b, users as u WHERE u.Id =c.UserId AND c.UserId = p.OwnerUserId AND p.OwnerUserId = v.UserId AND v.UserId = b.UserId AND c.Score=3 AND p.Score>=1 AND p.Score<=1 AND p.CreationDate>='2012-09-11 15:29:12'::timestamp AND p.CreationDate<='2013-06-23 12:50:00'::timestamp AND v.BountyAmount<=500 AND b.Date<='2014-01-14 22:31:05'::timestamp AND u.DownVotes<=0 AND u.CreationDate>='2013-09-19 17:00:57'::timestamp AND u.CreationDate<='2014-03-25 19:35:42'::timestamp;

-- train q50
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, badges as b, users as u WHERE u.Id =c.UserId AND c.UserId = p.OwnerUserId AND p.OwnerUserId = v.UserId AND v.UserId = b.UserId AND c.Score=3 AND p.Score>=0 AND p.Score<=2 AND p.ViewCount<=1094 AND p.CommentCount=1 AND p.FavoriteCount>=1 AND u.Reputation>=1 AND u.Reputation<=87393 AND u.Views>=0 AND u.Views<=0;

-- train q50
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, badges as b, users as u WHERE u.Id =c.UserId AND c.UserId = p.OwnerUserId AND p.OwnerUserId = v.UserId AND v.UserId = b.UserId AND c.Score=4 AND p.Score>=1 AND p.Score<=2 AND p.ViewCount<=1094 AND p.CommentCount=4 AND p.FavoriteCount>=0 AND u.Reputation>=1 AND u.Reputation<=51 AND u.Views>=0 AND u.Views<=1;

-- train q50
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, badges as b, users as u WHERE u.Id =c.UserId AND c.UserId = p.OwnerUserId AND p.OwnerUserId = v.UserId AND v.UserId = b.UserId AND c.Score=2 AND p.Score>=0 AND p.Score<=2 AND p.ViewCount<=107 AND p.CommentCount=2 AND p.FavoriteCount>=1 AND u.Reputation>=6 AND u.Reputation<=101 AND u.Views>=0 AND u.Views<=0;

-- train q50
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, badges as b, users as u WHERE u.Id =c.UserId AND c.UserId = p.OwnerUserId AND p.OwnerUserId = v.UserId AND v.UserId = b.UserId AND c.Score=0 AND p.Score>=2 AND p.Score<=5 AND p.ViewCount<=228 AND p.CommentCount=3 AND p.FavoriteCount>=0 AND u.Reputation>=1 AND u.Reputation<=51 AND u.Views>=0 AND u.Views<=20932;

-- train q50
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, badges as b, users as u WHERE u.Id =c.UserId AND c.UserId = p.OwnerUserId AND p.OwnerUserId = v.UserId AND v.UserId = b.UserId AND c.Score=3 AND p.Score>=2 AND p.Score<=1 AND p.ViewCount<=153 AND p.CommentCount=3 AND p.FavoriteCount>=1 AND u.Reputation>=16 AND u.Reputation<=101 AND u.Views>=0 AND u.Views<=0;

-- train q50
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, badges as b, users as u WHERE u.Id =c.UserId AND c.UserId = p.OwnerUserId AND p.OwnerUserId = v.UserId AND v.UserId = b.UserId AND c.Score=2 AND p.Score>=1 AND p.Score<=2 AND p.ViewCount<=493 AND p.CommentCount=4 AND p.FavoriteCount>=0 AND u.Reputation>=1 AND u.Reputation<=116 AND u.Views>=0 AND u.Views<=18;

-- train q50
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, badges as b, users as u WHERE u.Id =c.UserId AND c.UserId = p.OwnerUserId AND p.OwnerUserId = v.UserId AND v.UserId = b.UserId AND c.Score=0 AND p.Score>=2 AND p.Score<=192 AND p.ViewCount<=493 AND p.CommentCount=3 AND p.FavoriteCount>=1 AND u.Reputation>=6 AND u.Reputation<=101 AND u.Views>=0 AND u.Views<=0;

-- train q50
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, badges as b, users as u WHERE u.Id =c.UserId AND c.UserId = p.OwnerUserId AND p.OwnerUserId = v.UserId AND v.UserId = b.UserId AND c.Score=3 AND p.Score>=1 AND p.Score<=4 AND p.ViewCount<=185 AND p.CommentCount=0 AND p.FavoriteCount>=1 AND u.Reputation>=26 AND u.Reputation<=101 AND u.Views>=0 AND u.Views<=0;

-- train q50
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, badges as b, users as u WHERE u.Id =c.UserId AND c.UserId = p.OwnerUserId AND p.OwnerUserId = v.UserId AND v.UserId = b.UserId AND c.Score=2 AND p.Score>=-19 AND p.Score<=5 AND p.ViewCount<=1094 AND p.CommentCount=3 AND p.FavoriteCount>=1 AND u.Reputation>=26 AND u.Reputation<=116 AND u.Views>=0 AND u.Views<=20932;

-- train q50
SELECT COUNT(*) FROM comments as c, posts as p, votes as v, badges as b, users as u WHERE u.Id =c.UserId AND c.UserId = p.OwnerUserId AND p.OwnerUserId = v.UserId AND v.UserId = b.UserId AND c.Score=1 AND p.Score>=0 AND p.Score<=9 AND p.ViewCount<=703 AND p.CommentCount=0 AND p.FavoriteCount>=1 AND u.Reputation>=1 AND u.Reputation<=101 AND u.Views>=1 AND u.Views<=1;

-- train q12
SELECT COUNT(*) FROM comments as c, postHistory as ph, users as u WHERE u.Id = c.UserId AND c.UserId = ph.UserId AND u.Reputation>=1 AND u.Reputation<=26 AND u.UpVotes<=3 AND u.CreationDate>='2013-03-18 20:44:37'::timestamp AND u.CreationDate<='2014-07-07 19:57:03'::timestamp;

-- train q12
SELECT COUNT(*) FROM comments as c, postHistory as ph, users as u WHERE u.Id = c.UserId AND c.UserId = ph.UserId AND u.Reputation>=1 AND u.Reputation<=11 AND u.UpVotes<=0 AND u.CreationDate>='2010-07-19 06:55:26'::timestamp AND u.CreationDate<='2014-01-10 18:05:07'::timestamp;

-- train q12
SELECT COUNT(*) FROM comments as c, postHistory as ph, users as u WHERE u.Id = c.UserId AND c.UserId = ph.UserId AND u.Reputation>=1 AND u.Reputation<=26 AND u.UpVotes<=3 AND u.CreationDate>='2013-11-15 10:35:16'::timestamp AND u.CreationDate<='2014-03-25 19:35:42'::timestamp;

-- train q12
SELECT COUNT(*) FROM comments as c, postHistory as ph, users as u WHERE u.Id = c.UserId AND c.UserId = ph.UserId AND u.Reputation>=16 AND u.Reputation<=101 AND u.UpVotes<=0 AND u.CreationDate>='2013-03-18 20:44:37'::timestamp AND u.CreationDate<='2013-03-18 20:44:37'::timestamp;

-- train q12
SELECT COUNT(*) FROM comments as c, postHistory as ph, users as u WHERE u.Id = c.UserId AND c.UserId = ph.UserId AND u.Reputation>=11 AND u.Reputation<=101 AND u.UpVotes<=0 AND u.CreationDate>='2013-01-16 20:29:10'::timestamp AND u.CreationDate<='2014-01-10 18:05:07'::timestamp;

-- train q12
SELECT COUNT(*) FROM comments as c, postHistory as ph, users as u WHERE u.Id = c.UserId AND c.UserId = ph.UserId AND u.Reputation>=1 AND u.Reputation<=51 AND u.UpVotes<=0 AND u.CreationDate>='2010-07-19 06:55:26'::timestamp AND u.CreationDate<='2014-06-03 08:58:38'::timestamp;

-- train q12
SELECT COUNT(*) FROM comments as c, postHistory as ph, users as u WHERE u.Id = c.UserId AND c.UserId = ph.UserId AND u.Reputation>=1 AND u.Reputation<=6 AND u.UpVotes<=1 AND u.CreationDate>='2013-07-15 02:31:25'::timestamp AND u.CreationDate<='2014-01-10 18:05:07'::timestamp;

-- train q12
SELECT COUNT(*) FROM comments as c, postHistory as ph, users as u WHERE u.Id = c.UserId AND c.UserId = ph.UserId AND u.Reputation>=11 AND u.Reputation<=101 AND u.UpVotes<=11442 AND u.CreationDate>='2011-02-11 21:16:21'::timestamp AND u.CreationDate<='2013-03-18 20:44:37'::timestamp;

-- train q12
SELECT COUNT(*) FROM comments as c, postHistory as ph, users as u WHERE u.Id = c.UserId AND c.UserId = ph.UserId AND u.Reputation>=1 AND u.Reputation<=116 AND u.UpVotes<=1 AND u.CreationDate>='2011-12-27 18:54:39'::timestamp AND u.CreationDate<='2013-05-14 19:05:54'::timestamp;

-- train q12
SELECT COUNT(*) FROM comments as c, postHistory as ph, users as u WHERE u.Id = c.UserId AND c.UserId = ph.UserId AND u.Reputation>=1 AND u.Reputation<=101 AND u.UpVotes<=0 AND u.CreationDate>='2013-07-15 02:31:25'::timestamp AND u.CreationDate<='2014-02-16 15:09:45'::timestamp;

-- train q17
SELECT COUNT(*) FROM posts as p, tags as t, votes as v WHERE p.Id = t.ExcerptPostId AND p.OwnerUserId = v.UserId AND p.CreationDate>='2012-11-26 23:54:07'::timestamp;

-- train q17
SELECT COUNT(*) FROM posts as p, tags as t, votes as v WHERE p.Id = t.ExcerptPostId AND p.OwnerUserId = v.UserId AND p.CreationDate>='2012-06-19 22:27:44'::timestamp;

-- train q17
SELECT COUNT(*) FROM posts as p, tags as t, votes as v WHERE p.Id = t.ExcerptPostId AND p.OwnerUserId = v.UserId AND p.CreationDate>='2013-08-20 10:19:52'::timestamp;

-- train q17
SELECT COUNT(*) FROM posts as p, tags as t, votes as v WHERE p.Id = t.ExcerptPostId AND p.OwnerUserId = v.UserId AND p.CreationDate>='2011-12-23 07:35:55'::timestamp;

-- train q17
SELECT COUNT(*) FROM posts as p, tags as t, votes as v WHERE p.Id = t.ExcerptPostId AND p.OwnerUserId = v.UserId AND p.CreationDate>='2012-03-28 16:24:28'::timestamp;

-- train q17
SELECT COUNT(*) FROM posts as p, tags as t, votes as v WHERE p.Id = t.ExcerptPostId AND p.OwnerUserId = v.UserId AND p.CreationDate>='2011-04-26 07:24:16'::timestamp;

-- train q17
SELECT COUNT(*) FROM posts as p, tags as t, votes as v WHERE p.Id = t.ExcerptPostId AND p.OwnerUserId = v.UserId AND p.CreationDate>='2013-06-23 12:50:00'::timestamp;

-- train q17
SELECT COUNT(*) FROM posts as p, tags as t, votes as v WHERE p.Id = t.ExcerptPostId AND p.OwnerUserId = v.UserId AND p.CreationDate>='2013-04-21 19:38:59'::timestamp;

-- train q17
SELECT COUNT(*) FROM posts as p, tags as t, votes as v WHERE p.Id = t.ExcerptPostId AND p.OwnerUserId = v.UserId AND p.CreationDate>='2010-11-26 16:46:09'::timestamp;

-- train q17
SELECT COUNT(*) FROM posts as p, tags as t, votes as v WHERE p.Id = t.ExcerptPostId AND p.OwnerUserId = v.UserId AND p.CreationDate>='2009-02-02 14:21:12'::timestamp;

-- train q48
SELECT COUNT(*) FROM comments as c, postHistory as ph, badges as b, votes as v, users as u WHERE u.Id =b.UserId AND b.UserId = ph.UserId AND ph.UserId = v.UserId AND v.UserId = c.UserId AND c.CreationDate>='2012-11-12 19:23:15'::timestamp AND ph.PostHistoryTypeId=3 AND u.UpVotes=1;

-- train q48
SELECT COUNT(*) FROM comments as c, postHistory as ph, badges as b, votes as v, users as u WHERE u.Id =b.UserId AND b.UserId = ph.UserId AND ph.UserId = v.UserId AND v.UserId = c.UserId AND c.CreationDate>='2012-11-12 19:23:15'::timestamp AND ph.PostHistoryTypeId=4 AND u.UpVotes=2;

-- train q48
SELECT COUNT(*) FROM comments as c, postHistory as ph, badges as b, votes as v, users as u WHERE u.Id =b.UserId AND b.UserId = ph.UserId AND ph.UserId = v.UserId AND v.UserId = c.UserId AND c.CreationDate>='2013-08-10 10:42:04'::timestamp AND ph.PostHistoryTypeId=4 AND u.UpVotes=1;

-- train q48
SELECT COUNT(*) FROM comments as c, postHistory as ph, badges as b, votes as v, users as u WHERE u.Id =b.UserId AND b.UserId = ph.UserId AND ph.UserId = v.UserId AND v.UserId = c.UserId AND c.CreationDate>='2013-06-17 03:23:06'::timestamp AND ph.PostHistoryTypeId=1 AND u.UpVotes=1;

-- train q48
SELECT COUNT(*) FROM comments as c, postHistory as ph, badges as b, votes as v, users as u WHERE u.Id =b.UserId AND b.UserId = ph.UserId AND ph.UserId = v.UserId AND v.UserId = c.UserId AND c.CreationDate>='2011-05-30 17:17:48'::timestamp AND ph.PostHistoryTypeId=2 AND u.UpVotes=3;

-- train q48
SELECT COUNT(*) FROM comments as c, postHistory as ph, badges as b, votes as v, users as u WHERE u.Id =b.UserId AND b.UserId = ph.UserId AND ph.UserId = v.UserId AND v.UserId = c.UserId AND c.CreationDate>='2013-04-12 14:14:02'::timestamp AND ph.PostHistoryTypeId=5 AND u.UpVotes=1;

-- train q48
SELECT COUNT(*) FROM comments as c, postHistory as ph, badges as b, votes as v, users as u WHERE u.Id =b.UserId AND b.UserId = ph.UserId AND ph.UserId = v.UserId AND v.UserId = c.UserId AND c.CreationDate>='2013-08-10 10:42:04'::timestamp AND ph.PostHistoryTypeId=4 AND u.UpVotes=4;

-- train q48
SELECT COUNT(*) FROM comments as c, postHistory as ph, badges as b, votes as v, users as u WHERE u.Id =b.UserId AND b.UserId = ph.UserId AND ph.UserId = v.UserId AND v.UserId = c.UserId AND c.CreationDate>='2012-11-12 19:23:15'::timestamp AND ph.PostHistoryTypeId=5 AND u.UpVotes=3;

-- train q48
SELECT COUNT(*) FROM comments as c, postHistory as ph, badges as b, votes as v, users as u WHERE u.Id =b.UserId AND b.UserId = ph.UserId AND ph.UserId = v.UserId AND v.UserId = c.UserId AND c.CreationDate>='2011-10-04 13:23:07'::timestamp AND ph.PostHistoryTypeId=2 AND u.UpVotes=0;

-- train q48
SELECT COUNT(*) FROM comments as c, postHistory as ph, badges as b, votes as v, users as u WHERE u.Id =b.UserId AND b.UserId = ph.UserId AND ph.UserId = v.UserId AND v.UserId = c.UserId AND c.CreationDate>='2012-06-25 15:50:24'::timestamp AND ph.PostHistoryTypeId=2 AND u.UpVotes=1;

-- train q13
SELECT COUNT(*) FROM comments as c, votes as v, users as u WHERE u.Id = c.UserId AND u.Id = v.UserId AND c.CreationDate>='2012-01-28 20:58:07'::timestamp AND u.Reputation>=1 AND u.Reputation<=116;

-- train q13
SELECT COUNT(*) FROM comments as c, votes as v, users as u WHERE u.Id = c.UserId AND u.Id = v.UserId AND c.CreationDate>='2013-04-12 14:14:02'::timestamp AND u.Reputation>=26 AND u.Reputation<=11;

-- train q13
SELECT COUNT(*) FROM comments as c, votes as v, users as u WHERE u.Id = c.UserId AND u.Id = v.UserId AND c.CreationDate>='2013-04-12 14:14:02'::timestamp AND u.Reputation>=1 AND u.Reputation<=116;

-- train q13
SELECT COUNT(*) FROM comments as c, votes as v, users as u WHERE u.Id = c.UserId AND u.Id = v.UserId AND c.CreationDate>='2011-05-30 17:17:48'::timestamp AND u.Reputation>=1 AND u.Reputation<=116;

-- train q13
SELECT COUNT(*) FROM comments as c, votes as v, users as u WHERE u.Id = c.UserId AND u.Id = v.UserId AND c.CreationDate>='2013-06-17 03:23:06'::timestamp AND u.Reputation>=1 AND u.Reputation<=11;

-- train q13
SELECT COUNT(*) FROM comments as c, votes as v, users as u WHERE u.Id = c.UserId AND u.Id = v.UserId AND c.CreationDate>='2011-01-19 20:40:12'::timestamp AND u.Reputation>=6 AND u.Reputation<=16;

-- train q13
SELECT COUNT(*) FROM comments as c, votes as v, users as u WHERE u.Id = c.UserId AND u.Id = v.UserId AND c.CreationDate>='2013-06-17 03:23:06'::timestamp AND u.Reputation>=1 AND u.Reputation<=6;

-- train q13
SELECT COUNT(*) FROM comments as c, votes as v, users as u WHERE u.Id = c.UserId AND u.Id = v.UserId AND c.CreationDate>='2012-06-25 15:50:24'::timestamp AND u.Reputation>=1 AND u.Reputation<=116;

-- train q13
SELECT COUNT(*) FROM comments as c, votes as v, users as u WHERE u.Id = c.UserId AND u.Id = v.UserId AND c.CreationDate>='2013-02-02 20:12:58'::timestamp AND u.Reputation>=1 AND u.Reputation<=26;

-- train q13
SELECT COUNT(*) FROM comments as c, votes as v, users as u WHERE u.Id = c.UserId AND u.Id = v.UserId AND c.CreationDate>='2011-05-30 17:17:48'::timestamp AND u.Reputation>=1 AND u.Reputation<=87393;

-- train q14
SELECT COUNT(*) FROM comments as c, votes as v, users as u WHERE u.Id = c.UserId AND u.Id = v.UserId AND c.CreationDate>='2011-01-19 20:40:12'::timestamp AND c.CreationDate<='2014-04-26 01:11:26'::timestamp AND v.BountyAmount<=150 AND u.UpVotes=4 AND u.CreationDate<='2013-09-19 17:00:57'::timestamp;

-- train q14
SELECT COUNT(*) FROM comments as c, votes as v, users as u WHERE u.Id = c.UserId AND u.Id = v.UserId AND c.CreationDate>='2012-01-28 20:58:07'::timestamp AND c.CreationDate<='2014-07-29 16:38:44'::timestamp AND v.BountyAmount<=50 AND u.UpVotes=2 AND u.CreationDate<='2014-08-08 12:49:32'::timestamp;

-- train q14
SELECT COUNT(*) FROM comments as c, votes as v, users as u WHERE u.Id = c.UserId AND u.Id = v.UserId AND c.CreationDate>='2011-01-19 20:40:12'::timestamp AND c.CreationDate<='2014-06-12 00:55:46'::timestamp AND v.BountyAmount<=100 AND u.UpVotes=2 AND u.CreationDate<='2014-03-25 19:35:42'::timestamp;

-- train q14
SELECT COUNT(*) FROM comments as c, votes as v, users as u WHERE u.Id = c.UserId AND u.Id = v.UserId AND c.CreationDate>='2013-04-12 14:14:02'::timestamp AND c.CreationDate<='2013-11-29 17:06:35'::timestamp AND v.BountyAmount<=50 AND u.UpVotes=0 AND u.CreationDate<='2013-11-15 10:35:16'::timestamp;

-- train q14
SELECT COUNT(*) FROM comments as c, votes as v, users as u WHERE u.Id = c.UserId AND u.Id = v.UserId AND c.CreationDate>='2011-10-04 13:23:07'::timestamp AND c.CreationDate<='2013-10-08 14:15:52'::timestamp AND v.BountyAmount<=50 AND u.UpVotes=2 AND u.CreationDate<='2014-01-10 18:05:07'::timestamp;

-- train q14
SELECT COUNT(*) FROM comments as c, votes as v, users as u WHERE u.Id = c.UserId AND u.Id = v.UserId AND c.CreationDate>='2013-08-10 10:42:04'::timestamp AND c.CreationDate<='2014-06-12 00:55:46'::timestamp AND v.BountyAmount<=100 AND u.UpVotes=4 AND u.CreationDate<='2013-07-15 02:31:25'::timestamp;

-- train q14
SELECT COUNT(*) FROM comments as c, votes as v, users as u WHERE u.Id = c.UserId AND u.Id = v.UserId AND c.CreationDate>='2011-05-30 17:17:48'::timestamp AND c.CreationDate<='2014-03-10 15:31:38'::timestamp AND v.BountyAmount<=100 AND u.UpVotes=2 AND u.CreationDate<='2013-07-15 02:31:25'::timestamp;

-- train q14
SELECT COUNT(*) FROM comments as c, votes as v, users as u WHERE u.Id = c.UserId AND u.Id = v.UserId AND c.CreationDate>='2009-02-02 14:45:19'::timestamp AND c.CreationDate<='2014-06-12 00:55:46'::timestamp AND v.BountyAmount<=50 AND u.UpVotes=0 AND u.CreationDate<='2014-06-03 08:58:38'::timestamp;

-- train q14
SELECT COUNT(*) FROM comments as c, votes as v, users as u WHERE u.Id = c.UserId AND u.Id = v.UserId AND c.CreationDate>='2012-04-20 08:35:37'::timestamp AND c.CreationDate<='2012-11-12 19:23:15'::timestamp AND v.BountyAmount<=50 AND u.UpVotes=2 AND u.CreationDate<='2013-09-19 17:00:57'::timestamp;

-- train q14
SELECT COUNT(*) FROM comments as c, votes as v, users as u WHERE u.Id = c.UserId AND u.Id = v.UserId AND c.CreationDate>='2012-06-25 15:50:24'::timestamp AND c.CreationDate<='2013-11-29 17:06:35'::timestamp AND v.BountyAmount<=100 AND u.UpVotes=3 AND u.CreationDate<='2014-06-03 08:58:38'::timestamp;
