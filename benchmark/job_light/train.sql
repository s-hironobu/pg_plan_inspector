-- JOB-light training workload for pgpi (benchmark/job_light): queries of the MSCN training set
-- (Kipf et al., "Learned Cardinalities: Estimating Correlated Joins with Deep Learning", CIDR 2019)
-- (https://github.com/andreaskipf/learnedcardinalities, commit 50073283eacdea300d1dcd815c720f64e3d2d400, MIT license: LICENSE-learnedcardinalities), data/train.csv, written as SQL.
-- 100 queries of each of its 21 join graphs (0 to 2 joins), drawn at random, in round-robin
-- order so that the first N queries cover every graph evenly; queries with a predicate on
-- cast_info.person_id are left out (the column is not imported).
-- '-- train m12' names the next query: its line in train.csv.

-- train m788
SELECT COUNT(*) FROM cast_info ci WHERE ci.role_id<10;

-- train m51377
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id<32111 AND mc.company_type_id=1;

-- train m69548
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id<89;

-- train m35
SELECT COUNT(*) FROM movie_info_idx mi_idx WHERE mi_idx.info_type_id=101;

-- train m43937
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<3919;

-- train m16367
SELECT COUNT(*) FROM title t WHERE t.kind_id<7 AND t.production_year=1971;

-- train m13828
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id<7 AND t.production_year<1998;

-- train m47044
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id<7 AND mc.company_id=6 AND mc.company_type_id>1;

-- train m75831
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.production_year=1999 AND mi.info_type_id=107;

-- train m23294
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.production_year=1999 AND mi_idx.info_type_id=100;

-- train m3785
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.production_year<1981 AND mk.keyword_id=2855;

-- train m48113
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id=1 AND mc.company_id>1877 AND mc.company_type_id=2 AND ci.role_id=1;

-- train m507
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND mi.info_type_id>8;

-- train m81333
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>1 AND t.production_year>2005 AND ci.role_id=1;

-- train m69614
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND ci.role_id>5;

-- train m88593
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id<7 AND t.production_year=1984 AND mi.info_type_id=1;

-- train m98315
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND mc.company_id=2266 AND mc.company_type_id=1;

-- train m80533
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.production_year<2012 AND mc.company_type_id<2 AND mk.keyword_id<13036;

-- train m30406
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>1 AND t.production_year<2000 AND mi.info_type_id=1;

-- train m10101
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id=4 AND t.production_year>1910 AND mi.info_type_id<4 AND mk.keyword_id>20321;

-- train m68707
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND t.production_year>2007 AND mi_idx.info_type_id>99 AND mk.keyword_id=1135;

-- train m15652
SELECT COUNT(*) FROM cast_info ci WHERE ci.role_id<7;

-- train m20029
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id>3733 AND mc.company_type_id=2;

-- train m21773
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id=80;

-- train m9
SELECT COUNT(*) FROM movie_info_idx mi_idx WHERE mi_idx.info_type_id>99;

-- train m44917
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id>78904;

-- train m6487
SELECT COUNT(*) FROM title t WHERE t.production_year<1936;

-- train m68024
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id>4 AND t.production_year>1962 AND ci.role_id>2;

-- train m49533
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id=7 AND t.production_year=1951;

-- train m92425
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.production_year>2011 AND mi.info_type_id=6;

-- train m4611
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id>4 AND t.production_year>1992 AND mi_idx.info_type_id<101;

-- train m90410
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id=7 AND t.production_year>2008 AND mk.keyword_id<2349;

-- train m81271
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id>1 AND t.production_year<2008;

-- train m97372
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND ci.role_id=10 AND mi.info_type_id=16;

-- train m94221
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>2 AND t.production_year=1999 AND ci.role_id>4;

-- train m5632
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.production_year>2011;

-- train m91903
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id<7 AND mc.company_id=67467 AND mi.info_type_id=3;

-- train m63533
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year>2006 AND mi_idx.info_type_id>99;

-- train m9119
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mc.company_id<19;

-- train m41249
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year=2005 AND mi_idx.info_type_id=101;

-- train m72987
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.production_year=2008 AND mi.info_type_id=98;

-- train m22003
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND mk.keyword_id<18303;

-- train m4735
SELECT COUNT(*) FROM cast_info ci WHERE ci.role_id>8;

-- train m54961
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id<94782;

-- train m57073
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id=72;

-- train m86792
SELECT COUNT(*) FROM movie_info_idx mi_idx WHERE mi_idx.info_type_id>112;

-- train m682
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<479;

-- train m13066
SELECT COUNT(*) FROM title t WHERE t.kind_id>3 AND t.production_year=2003;

-- train m27733
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.production_year<1936;

-- train m34364
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND mc.company_id>1727 AND mc.company_type_id=2;

-- train m46903
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id=4 AND mi.info_type_id>16;

-- train m6667
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id=1 AND t.production_year<1951;

-- train m73447
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.production_year=1984 AND mk.keyword_id>29028;

-- train m26945
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND ci.role_id=8;

-- train m18893
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND mi.info_type_id=18;

-- train m42066
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.production_year>2005 AND ci.role_id=8;

-- train m83851
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id=1 AND t.production_year=1994;

-- train m76814
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id<7 AND t.production_year<1975 AND mc.company_id>11780 AND mc.company_type_id<2 AND mi.info_type_id<107;

-- train m14556
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND mc.company_id<225 AND mc.company_type_id<2 AND mi_idx.info_type_id=101;

-- train m87167
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mc.company_id=8750;

-- train m68778
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.production_year<1962 AND mi.info_type_id>16 AND mi_idx.info_type_id>99;

-- train m3498
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND mk.keyword_id<585;

-- train m50831
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND t.production_year>1971 AND mi_idx.info_type_id=101;

-- train m623
SELECT COUNT(*) FROM cast_info ci WHERE ci.role_id<6;

-- train m42483
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id=17975 AND mc.company_type_id<2;

-- train m659
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id>105;

-- train m14787
SELECT COUNT(*) FROM movie_info_idx mi_idx WHERE mi_idx.info_type_id=112;

-- train m6350
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<1508;

-- train m64983
SELECT COUNT(*) FROM title t WHERE t.production_year>1921;

-- train m51109
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id<7 AND t.production_year>2006 AND ci.role_id=6;

-- train m64379
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id=7 AND t.production_year=2010 AND mc.company_id<91142;

-- train m81581
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id<7 AND t.production_year<1993;

-- train m83550
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.production_year<1914 AND mi_idx.info_type_id>100;

-- train m73936
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id>4 AND t.production_year=2012 AND mk.keyword_id=19209;

-- train m61827
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id=1 AND t.production_year>2008 AND ci.role_id=2;

-- train m25746
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id=1 AND ci.role_id=5 AND mi.info_type_id=3;

-- train m12504
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND ci.role_id>2 AND mi_idx.info_type_id>100;

-- train m87082
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND mk.keyword_id>2243;

-- train m92392
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND mc.company_id>11940;

-- train m28022
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND mc.company_id>21726;

-- train m49131
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id>1 AND t.production_year>2004 AND mc.company_type_id>1;

-- train m77186
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year>2004 AND mi.info_type_id=4;

-- train m98130
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND t.production_year>1964;

-- train m24139
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND t.production_year<2007 AND mi_idx.info_type_id<100;

-- train m219
SELECT COUNT(*) FROM cast_info ci WHERE ci.role_id=4;

-- train m23503
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id=11213;

-- train m48963
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id>35;

-- train m26
SELECT COUNT(*) FROM movie_info_idx mi_idx WHERE mi_idx.info_type_id<101;

-- train m79407
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<39588;

-- train m6971
SELECT COUNT(*) FROM title t WHERE t.kind_id>3 AND t.production_year<1993;

-- train m56728
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id>1 AND t.production_year>1913;

-- train m53761
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id<7 AND t.production_year=2007 AND mc.company_id<15571 AND mc.company_type_id=2;

-- train m71694
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id<7 AND t.production_year<1976;

-- train m97049
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id>1 AND t.production_year=2011 AND mi_idx.info_type_id=100;

-- train m30874
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id=7 AND t.production_year<1981 AND mk.keyword_id>1117;

-- train m38911
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.production_year>1999;

-- train m90563
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.production_year=2001 AND mi.info_type_id>8;

-- train m51999
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>1 AND ci.role_id>10 AND mi_idx.info_type_id<100;

-- train m70249
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.production_year>2010 AND ci.role_id=7;

-- train m10496
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND mc.company_id=530 AND mc.company_type_id<2;

-- train m41115
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.production_year<2012 AND mi_idx.info_type_id>100;

-- train m51348
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.production_year>1916 AND mc.company_id>329 AND mc.company_type_id=1;

-- train m27529
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>1 AND t.production_year<2004 AND mi.info_type_id<8;

-- train m82434
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND t.production_year>2010 AND mk.keyword_id>19659;

-- train m93233
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id>3 AND t.production_year=1998 AND mi_idx.info_type_id>99;

-- train m299
SELECT COUNT(*) FROM cast_info ci WHERE ci.role_id<5;

-- train m31025
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id>12063 AND mc.company_type_id>1;

-- train m97
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id=5;

-- train m267
SELECT COUNT(*) FROM movie_info_idx mi_idx WHERE mi_idx.info_type_id>100;

-- train m40714
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id=116053;

-- train m65136
SELECT COUNT(*) FROM title t WHERE t.kind_id<4 AND t.production_year=2005;

-- train m23712
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id=1 AND t.production_year>1989;

-- train m14464
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.production_year>1984 AND mc.company_id=424 AND mc.company_type_id<2;

-- train m88065
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id=1 AND t.production_year>1986 AND mi.info_type_id<4;

-- train m37688
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year=1991 AND mi_idx.info_type_id=101;

-- train m22881
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND mk.keyword_id<11537;

-- train m48826
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.production_year>2002;

-- train m79519
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.production_year>1999 AND mi.info_type_id>4;

-- train m87720
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>2 AND mi_idx.info_type_id>100;

-- train m84635
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND mk.keyword_id=571;

-- train m80022
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.production_year=2013 AND mc.company_type_id<2;

-- train m99573
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year=2006 AND mc.company_type_id=2;

-- train m69938
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mk.keyword_id>67469;

-- train m60098
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year<2012 AND mi.info_type_id>8;

-- train m50581
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND mk.keyword_id<3393;

-- train m94260
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND mk.keyword_id>831;

-- train m1289
SELECT COUNT(*) FROM cast_info ci WHERE ci.role_id=9;

-- train m24074
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id=27 AND mc.company_type_id<2;

-- train m45408
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id=81;

-- train m205
SELECT COUNT(*) FROM movie_info_idx mi_idx WHERE mi_idx.info_type_id>101;

-- train m53649
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id=14204;

-- train m43863
SELECT COUNT(*) FROM title t WHERE t.production_year>1919;

-- train m76378
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.production_year>2007 AND ci.role_id=2;

-- train m21537
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id=7 AND t.production_year<2010;

-- train m22803
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.production_year>0 AND mi.info_type_id=15;

-- train m4775
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year<2008 AND mi_idx.info_type_id>101;

-- train m59269
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND mk.keyword_id>10018;

-- train m83891
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND mc.company_id<846 AND mc.company_type_id<2 AND ci.role_id>2;

-- train m98283
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id=1 AND t.production_year>1950;

-- train m5877
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.production_year>1910;

-- train m5082
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id>4 AND ci.role_id<3 AND mk.keyword_id<29338;

-- train m87925
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND mc.company_id=14539 AND mi.info_type_id>4;

-- train m29420
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.production_year=2003 AND mi_idx.info_type_id=100;

-- train m24151
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND mc.company_id<19462 AND mc.company_type_id<2 AND mk.keyword_id>2553;

-- train m33711
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.production_year=1967 AND mi.info_type_id>16;

-- train m2114
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id>1;

-- train m45004
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id=1 AND t.production_year=1915;

-- train m962
SELECT COUNT(*) FROM cast_info ci WHERE ci.role_id<4;

-- train m98984
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id<13086 AND mc.company_type_id>1;

-- train m48995
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id<41;

-- train m42
SELECT COUNT(*) FROM movie_info_idx mi_idx WHERE mi_idx.info_type_id=100;

-- train m47251
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<2489;

-- train m746
SELECT COUNT(*) FROM title t WHERE t.kind_id>1 AND t.production_year=1996;

-- train m66512
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id=7 AND t.production_year>1976 AND ci.role_id=1;

-- train m78606
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.production_year>2002 AND mc.company_id=11346 AND mc.company_type_id=1;

-- train m56369
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND mi.info_type_id>102;

-- train m45657
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.production_year=1908;

-- train m17571
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id<7 AND t.production_year>0;

-- train m39211
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND mc.company_type_id<2 AND ci.role_id<8;

-- train m98568
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND ci.role_id=2 AND mi.info_type_id=3;

-- train m82375
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<3 AND t.production_year<2013 AND ci.role_id<10;

-- train m92588
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND t.production_year=1960;

-- train m43755
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id=7 AND t.production_year<1997 AND mi.info_type_id=15;

-- train m26921
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND mc.company_id=11691 AND mc.company_type_id<2 AND mi_idx.info_type_id=101;

-- train m38968
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.production_year>2004 AND mc.company_id>2057 AND mk.keyword_id=3069;

-- train m78930
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.production_year<2002 AND mi_idx.info_type_id=100;

-- train m49469
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND mi.info_type_id<16 AND mk.keyword_id<3219;

-- train m47091
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND t.production_year<2000 AND mi_idx.info_type_id<100 AND mk.keyword_id<4837;

-- train m3699
SELECT COUNT(*) FROM cast_info ci WHERE ci.role_id=8;

-- train m11432
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id=14568 AND mc.company_type_id=2;

-- train m14116
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id<61;

-- train m173
SELECT COUNT(*) FROM movie_info_idx mi_idx WHERE mi_idx.info_type_id=99;

-- train m35003
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id>28455;

-- train m13389
SELECT COUNT(*) FROM title t WHERE t.kind_id>4 AND t.production_year=2010;

-- train m27407
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id>3 AND t.production_year=2008 AND ci.role_id=3;

-- train m13155
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND mc.company_id<7848;

-- train m38999
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.production_year<2012 AND mi.info_type_id>1;

-- train m32783
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year>1914;

-- train m73090
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND mk.keyword_id<2830;

-- train m13025
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id=1 AND mc.company_type_id=1;

-- train m17470
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id=7 AND t.production_year=2009 AND mi.info_type_id<8;

-- train m59512
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=2 AND t.production_year<2010 AND mi_idx.info_type_id=99;

-- train m37227
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id>1 AND mk.keyword_id>22233;

-- train m65648
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id=3 AND t.production_year=2012 AND mi.info_type_id>17;

-- train m89264
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year>1985 AND mc.company_id<11069 AND mc.company_type_id<2;

-- train m99576
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mk.keyword_id>1301;

-- train m82191
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year>2007 AND mi_idx.info_type_id>100;

-- train m69646
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND t.production_year=1984 AND mi.info_type_id=7;

-- train m34334
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND t.production_year=2010 AND mi_idx.info_type_id>100;

-- train m4163
SELECT COUNT(*) FROM cast_info ci WHERE ci.role_id>9;

-- train m19013
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id>91 AND mc.company_type_id=1;

-- train m36278
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id<81;

-- train m41
SELECT COUNT(*) FROM movie_info_idx mi_idx WHERE mi_idx.info_type_id<100;

-- train m16244
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id=23078;

-- train m4586
SELECT COUNT(*) FROM title t WHERE t.kind_id>1 AND t.production_year<2006;

-- train m580
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.production_year<2005;

-- train m45847
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id<7 AND t.production_year<1968;

-- train m71230
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id<7 AND t.production_year>2012 AND mi.info_type_id=97;

-- train m4701
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year=2010;

-- train m61302
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id<4 AND t.production_year<1997 AND mk.keyword_id>10991;

-- train m96918
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id=6 AND mc.company_id<215460 AND mc.company_type_id>1;

-- train m48888
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.production_year<1975;

-- train m73853
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year>1972 AND mi_idx.info_type_id<101;

-- train m13276
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.production_year<1950 AND ci.role_id>1;

-- train m11428
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND mc.company_id=8488 AND mc.company_type_id>1;

-- train m22476
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=2 AND t.production_year>1900 AND mc.company_type_id=1 AND mi_idx.info_type_id=99;

-- train m87131
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND t.production_year>2006 AND mc.company_id<272 AND mk.keyword_id=1302;

-- train m60702
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<4 AND t.production_year>2011 AND mi.info_type_id=16 AND mi_idx.info_type_id<100;

-- train m91992
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND t.production_year>1999 AND mi.info_type_id<18 AND mk.keyword_id=14142;

-- train m21252
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id>1 AND t.production_year>1981;

-- train m7932
SELECT COUNT(*) FROM cast_info ci WHERE ci.role_id=7;

-- train m51294
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id>1310 AND mc.company_type_id=2;

-- train m4968
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id<78;

-- train m31879
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<55122;

-- train m84839
SELECT COUNT(*) FROM title t WHERE t.kind_id<7 AND t.production_year<1917;

-- train m28870
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.production_year<1972 AND ci.role_id=2;

-- train m26964
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND mc.company_id=12474;

-- train m70390
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id=7 AND t.production_year=2011 AND mi.info_type_id=13;

-- train m47835
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year>2002 AND mi_idx.info_type_id=100;

-- train m44066
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id=4 AND t.production_year=2011 AND mk.keyword_id<12423;

-- train m86911
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND mc.company_id<2831 AND ci.role_id=4;

-- train m86362
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id=1 AND mi.info_type_id=4;

-- train m97494
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year<1993 AND mi_idx.info_type_id<101;

-- train m28201
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.production_year>1960 AND mk.keyword_id<2890;

-- train m34083
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id=2 AND t.production_year=2007 AND mi.info_type_id=2;

-- train m54386
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND mc.company_id=3855 AND mc.company_type_id<2 AND mi_idx.info_type_id>99;

-- train m34700
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id>1 AND t.production_year>2007 AND mc.company_id>80476;

-- train m16823
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year>1999;

-- train m93859
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.production_year=2002 AND mi.info_type_id<7 AND mk.keyword_id<826;

-- train m61099
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.production_year>2007 AND mk.keyword_id=236;

-- train m740
SELECT COUNT(*) FROM cast_info ci WHERE ci.role_id>4;

-- train m33966
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id<19325;

-- train m1945
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id=9;

-- train m58427
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<9923;

-- train m4467
SELECT COUNT(*) FROM title t WHERE t.kind_id=7 AND t.production_year>0;

-- train m23021
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.production_year<1954;

-- train m11514
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND mc.company_id>11288;

-- train m15951
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id=7 AND mi.info_type_id<15;

-- train m34047
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id<4 AND t.production_year<2011;

-- train m96440
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND mk.keyword_id<28156;

-- train m87150
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id=7 AND mc.company_id=34097;

-- train m3701
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND ci.role_id=3;

-- train m3545
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>1 AND ci.role_id>6;

-- train m88412
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id>1 AND t.production_year<1971;

-- train m47249
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id>1 AND mc.company_id>44168 AND mc.company_type_id<2;

-- train m70772
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year<1965 AND mi_idx.info_type_id=100;

-- train m7956
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND t.production_year=2001 AND mk.keyword_id>61665;

-- train m68591
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND mi.info_type_id>17 AND mi_idx.info_type_id<101;

-- train m1436
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND mi.info_type_id>1 AND mk.keyword_id<7984;

-- train m3846
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND t.production_year=2013 AND mi_idx.info_type_id=99;

-- train m222
SELECT COUNT(*) FROM cast_info ci WHERE ci.role_id<11;

-- train m37917
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id<17206 AND mc.company_type_id<2;

-- train m12979
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id<57;

-- train m6285
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id=12033;

-- train m39309
SELECT COUNT(*) FROM title t WHERE t.kind_id=3 AND t.production_year>2001;

-- train m21484
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id<7 AND t.production_year=2003;

-- train m19168
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND mc.company_id>424;

-- train m6333
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id=7 AND t.production_year=2011;

-- train m6933
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year>2010 AND mi_idx.info_type_id>101;

-- train m35013
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.production_year<2002 AND mk.keyword_id=6239;

-- train m83322
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id>2 AND t.production_year=2006;

-- train m91262
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.production_year>2008 AND mi.info_type_id<2;

-- train m60898
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=3 AND t.production_year=2006 AND mi_idx.info_type_id<100;

-- train m66945
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id<2 AND t.production_year>2012 AND mk.keyword_id<2648;

-- train m13084
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id=1 AND t.production_year=1900 AND mi.info_type_id>2;

-- train m23470
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year>1938;

-- train m16896
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mc.company_id=1434;

-- train m60339
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.production_year>2011 AND mi.info_type_id=3;

-- train m29518
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id=6 AND t.production_year>1962 AND mi.info_type_id<4 AND mk.keyword_id>10866;

-- train m71385
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id<4 AND t.production_year<2006;

-- train m1743
SELECT COUNT(*) FROM cast_info ci WHERE ci.role_id=6;

-- train m56007
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id>1627 AND mc.company_type_id=2;

-- train m30771
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id=71;

-- train m47845
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<643;

-- train m71597
SELECT COUNT(*) FROM title t WHERE t.kind_id=4 AND t.production_year>2006;

-- train m60982
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id=7 AND t.production_year<1932;

-- train m17131
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND mc.company_id=82573 AND mc.company_type_id=2;

-- train m35039
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.production_year>2012 AND mi.info_type_id<7;

-- train m78123
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id=4 AND t.production_year<1994;

-- train m70428
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.production_year>0 AND mk.keyword_id>1756;

-- train m7518
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id<7 AND t.production_year<2009 AND mc.company_type_id>1;

-- train m87304
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id<7 AND t.production_year=1965 AND mi.info_type_id<15;

-- train m76615
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year>1923 AND ci.role_id>5 AND mi_idx.info_type_id>99;

-- train m9254
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND ci.role_id<2 AND mk.keyword_id<500;

-- train m19148
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND mc.company_id<69 AND mc.company_type_id>1;

-- train m58602
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year=1998 AND mc.company_id<32755 AND mi_idx.info_type_id<101;

-- train m86034
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id=1 AND t.production_year<2006 AND mc.company_type_id>1 AND mk.keyword_id<335;

-- train m6898
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year<2004 AND mi.info_type_id>2 AND mi_idx.info_type_id<100;

-- train m31242
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.production_year<2007 AND mi.info_type_id=18 AND mk.keyword_id=797;

-- train m23349
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND mi_idx.info_type_id=99 AND mk.keyword_id>16618;

-- train m51
SELECT COUNT(*) FROM cast_info ci WHERE ci.role_id>2;

-- train m81193
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id<151482;

-- train m6980
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id=90;

-- train m11730
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<13886;

-- train m74936
SELECT COUNT(*) FROM title t WHERE t.kind_id=1 AND t.production_year=1922;

-- train m16123
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id=3 AND t.production_year=2001;

-- train m8051
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.production_year=2011 AND mc.company_id>424;

-- train m57709
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id=7 AND t.production_year=1967;

-- train m43030
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year>2006 AND mi_idx.info_type_id=100;

-- train m4751
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.production_year<1961;

-- train m78726
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND mc.company_id<79657 AND ci.role_id=3;

-- train m57900
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id>3 AND ci.role_id=3 AND mi.info_type_id>16;

-- train m9467
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.production_year<1951;

-- train m23185
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id=1 AND t.production_year>1997;

-- train m13496
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND mc.company_id>16774 AND mc.company_type_id=1 AND mi.info_type_id<6;

-- train m65261
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND mc.company_id>11138 AND mi_idx.info_type_id>99;

-- train m57915
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mc.company_id=59033 AND mc.company_type_id>1 AND mk.keyword_id>85;

-- train m78949
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>2 AND t.production_year<2011 AND mi_idx.info_type_id<101;

-- train m17519
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id<3 AND t.production_year=1988;

-- train m40046
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND mi_idx.info_type_id<100 AND mk.keyword_id=12189;

-- train m2330
SELECT COUNT(*) FROM cast_info ci WHERE ci.role_id=5;

-- train m52202
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id<9346 AND mc.company_type_id<2;

-- train m79109
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id>50;

-- train m48621
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<7396;

-- train m8144
SELECT COUNT(*) FROM title t WHERE t.production_year>1987;

-- train m88583
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.production_year<1972;

-- train m70483
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND mc.company_id=62606;

-- train m13569
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.production_year=2012 AND mi.info_type_id=15;

-- train m78243
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year=1989;

-- train m54549
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id=7 AND t.production_year>1977 AND mk.keyword_id>5614;

-- train m56422
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND mc.company_id=5005 AND mc.company_type_id<2;

-- train m51338
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id=7 AND t.production_year<2012;

-- train m29885
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>2 AND t.production_year>2007 AND ci.role_id<2 AND mi_idx.info_type_id<101;

-- train m207
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.production_year=2006;

-- train m49472
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND mc.company_id<2690 AND mc.company_type_id<2 AND mi.info_type_id<8;

-- train m78291
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year=2013 AND mi_idx.info_type_id=101;

-- train m89126
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND mc.company_id>15121 AND mc.company_type_id>1 AND mk.keyword_id=1617;

-- train m47125
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year>1990 AND mi.info_type_id>6 AND mi_idx.info_type_id<101;

-- train m79309
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id>1 AND t.production_year=2012 AND mi.info_type_id=16;

-- train m60991
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND mi_idx.info_type_id>100 AND mk.keyword_id=20003;

-- train m358
SELECT COUNT(*) FROM cast_info ci WHERE ci.role_id>10;

-- train m65633
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id<110599 AND mc.company_type_id>1;

-- train m28733
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id=73;

-- train m1610
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id=15837;

-- train m20463
SELECT COUNT(*) FROM title t WHERE t.kind_id=1 AND t.production_year>1987;

-- train m57317
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id=1 AND t.production_year<1984;

-- train m21227
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND mc.company_id>3296;

-- train m9012
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id>1 AND t.production_year>1999;

-- train m45135
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year=2006 AND mi_idx.info_type_id>101;

-- train m39223
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id=7 AND t.production_year>1899 AND mk.keyword_id<15748;

-- train m14156
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id=7 AND t.production_year<1952 AND ci.role_id<5;

-- train m31706
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND ci.role_id>3 AND mi.info_type_id<16;

-- train m47647
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=4 AND t.production_year<2007 AND mi_idx.info_type_id>99;

-- train m47524
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND mk.keyword_id=2218;

-- train m33722
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id<4 AND t.production_year>2006 AND mc.company_type_id<2;

-- train m37325
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND mc.company_id<1085 AND mc.company_type_id>1 AND mi_idx.info_type_id<101;

-- train m46365
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND mc.company_id>160 AND mc.company_type_id=2 AND mk.keyword_id>25672;

-- train m32018
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year=1957 AND mi.info_type_id>16;

-- train m81925
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND mi.info_type_id<2 AND mk.keyword_id>10648;

-- train m51747
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.production_year=1927;

-- train m913
SELECT COUNT(*) FROM cast_info ci WHERE ci.role_id=2;

-- train m58634
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id=9174;

-- train m2545
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id>15;

-- train m3378
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id>26410;

-- train m42174
SELECT COUNT(*) FROM title t WHERE t.kind_id<2 AND t.production_year<1914;

-- train m31615
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id=7 AND t.production_year>1992;

-- train m68808
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id=7 AND mc.company_id=2164 AND mc.company_type_id=1;

-- train m8575
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id=7 AND t.production_year<1978 AND mi.info_type_id=15;

-- train m18156
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.production_year=1986 AND mi_idx.info_type_id<100;

-- train m663
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id=1 AND t.production_year=1970 AND mk.keyword_id<43826;

-- train m27352
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id>4 AND t.production_year=2010;

-- train m44399
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id=1 AND mi.info_type_id<106;

-- train m13190
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<3 AND t.production_year>2008 AND ci.role_id<2;

-- train m21817
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND mk.keyword_id=393;

-- train m55594
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id<7 AND t.production_year>1992 AND mc.company_id<3973 AND mi.info_type_id>18;

-- train m59073
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND mi_idx.info_type_id=99;

-- train m53304
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id>3 AND mc.company_id=1191;

-- train m18578
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year>1990;

-- train m62250
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND t.production_year<1966 AND mk.keyword_id<1749;

-- train m29152
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND t.production_year>0 AND mk.keyword_id<43262;

-- train m8669
SELECT COUNT(*) FROM cast_info ci WHERE ci.role_id>6;

-- train m68252
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id=3665;

-- train m523
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id=16;

-- train m28678
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id>5740;

-- train m54218
SELECT COUNT(*) FROM title t WHERE t.kind_id>1 AND t.production_year<1917;

-- train m30805
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.production_year=1967 AND ci.role_id>2;

-- train m40239
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id>1 AND t.production_year>1965 AND mc.company_id>14982 AND mc.company_type_id<2;

-- train m837
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id<7 AND t.production_year>1952;

-- train m26824
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.production_year=2011 AND mi_idx.info_type_id<100;

-- train m86528
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND mk.keyword_id<8286;

-- train m49230
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id=7 AND t.production_year<2007 AND mc.company_type_id=1;

-- train m74320
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND ci.role_id=1 AND mi.info_type_id=16;

-- train m15243
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.production_year<2012;

-- train m83305
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND t.production_year=2011 AND mk.keyword_id=382;

-- train m31761
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id>1 AND mi.info_type_id<3;

-- train m83886
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND mc.company_id>17820 AND mi_idx.info_type_id=101;

-- train m9209
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mc.company_id>127324;

-- train m91259
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.production_year<2008 AND mi.info_type_id>1 AND mi_idx.info_type_id=101;

-- train m82047
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND mi.info_type_id=2 AND mk.keyword_id<82;

-- train m80082
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id=1 AND mi_idx.info_type_id=99 AND mk.keyword_id>137;

-- train m72
SELECT COUNT(*) FROM cast_info ci WHERE ci.role_id=1;

-- train m66964
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id>5486;

-- train m7059
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id<94;

-- train m58284
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<23925;

-- train m3015
SELECT COUNT(*) FROM title t WHERE t.kind_id=7 AND t.production_year=1955;

-- train m49661
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id=4 AND t.production_year=1954 AND ci.role_id<10;

-- train m52056
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id>1 AND t.production_year<1994;

-- train m6817
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id=1 AND mi.info_type_id<17;

-- train m40786
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year<2009 AND mi_idx.info_type_id>100;

-- train m65251
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id=1 AND mk.keyword_id>382;

-- train m47009
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id=1 AND t.production_year>2010 AND mc.company_id>12681 AND mc.company_type_id=2 AND ci.role_id=3;

-- train m31231
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id<7 AND mi.info_type_id=1;

-- train m30166
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND ci.role_id<2 AND mi_idx.info_type_id=100;

-- train m21471
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.production_year=1995;

-- train m11545
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id=1 AND t.production_year>2005 AND mc.company_type_id=2;

-- train m38769
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND mc.company_id>123693 AND mc.company_type_id=2;

-- train m91373
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.production_year<2001 AND mk.keyword_id>3761;

-- train m46613
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year>2000;

-- train m14475
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id>3;

-- train m87909
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.production_year<2011 AND mi_idx.info_type_id<100 AND mk.keyword_id>11587;

-- train m1075
SELECT COUNT(*) FROM cast_info ci WHERE ci.role_id<2;

-- train m14418
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id>2042;

-- train m253
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id<47;

-- train m42489
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<29414;

-- train m13633
SELECT COUNT(*) FROM title t WHERE t.kind_id=7 AND t.production_year<1964;

-- train m20234
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id>1 AND t.production_year=2009 AND ci.role_id>1;

-- train m91574
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id<7 AND t.production_year=1960 AND mc.company_id>82604;

-- train m61394
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.production_year<1902 AND mi.info_type_id>106;

-- train m12141
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year<2003;

-- train m17052
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.production_year<2005 AND mk.keyword_id>1382;

-- train m59930
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND mc.company_id<2891;

-- train m22131
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.production_year>2011 AND ci.role_id>10;

-- train m6070
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND ci.role_id=3 AND mi_idx.info_type_id=101;

-- train m33973
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND ci.role_id=1 AND mk.keyword_id<1570;

-- train m13479
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id=7 AND mc.company_id<1371;

-- train m43166
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year>2011 AND mc.company_type_id=2 AND mi_idx.info_type_id=100;

-- train m45532
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mk.keyword_id>2148;

-- train m16354
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>1 AND t.production_year=2000 AND mi.info_type_id<8 AND mi_idx.info_type_id=99;

-- train m77011
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND mi.info_type_id=4 AND mk.keyword_id=3960;

-- train m98400
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND mk.keyword_id=416;

-- train m3392
SELECT COUNT(*) FROM cast_info ci WHERE ci.role_id<3;

-- train m18823
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id<425;

-- train m14386
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id=67;

-- train m74807
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id>30982;

-- train m6883
SELECT COUNT(*) FROM title t WHERE t.kind_id=7 AND t.production_year=2007;

-- train m88914
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.production_year>2005 AND ci.role_id>8;

-- train m57206
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id=1 AND t.production_year>1985 AND mc.company_id<70779 AND mc.company_type_id<2;

-- train m47052
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id>3 AND t.production_year<1988 AND mi.info_type_id<16;

-- train m11404
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id<6;

-- train m24556
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND mk.keyword_id<49890;

-- train m39670
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.production_year=2007 AND ci.role_id>1;

-- train m15654
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id>4 AND ci.role_id<9 AND mi.info_type_id=7;

-- train m74690
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND mi_idx.info_type_id=99;

-- train m28695
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id>1 AND mk.keyword_id>107;

-- train m29971
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND mc.company_id<99398 AND mc.company_type_id=2 AND mi.info_type_id=1;

-- train m76494
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND mc.company_id>13114 AND mi_idx.info_type_id=99;

-- train m26598
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND mk.keyword_id=7320;

-- train m26265
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>4 AND mi.info_type_id>17;

-- train m59321
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND mk.keyword_id=6194;

-- train m83861
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND mk.keyword_id=2868;

-- train m55
SELECT COUNT(*) FROM cast_info ci WHERE ci.role_id=3;

-- train m14727
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id>156959 AND mc.company_type_id>1;

-- train m20488
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id=64;

-- train m63671
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id=1736;

-- train m84087
SELECT COUNT(*) FROM title t WHERE t.kind_id=1 AND t.production_year<1974;

-- train m58050
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id=7 AND t.production_year=1994;

-- train m97135
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND mc.company_id=573 AND mc.company_type_id<2;

-- train m66108
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id=1 AND t.production_year<1997 AND mi.info_type_id<2;

-- train m11083
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id>2 AND t.production_year=2012 AND mi_idx.info_type_id<101;

-- train m7223
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND mk.keyword_id=5807;

-- train m72720
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND mc.company_id<11360;

-- train m98098
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.production_year<2012 AND mi.info_type_id>15;

-- train m83640
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND ci.role_id<8;

-- train m82899
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND ci.role_id=9 AND mk.keyword_id=6262;

-- train m49746
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id=1 AND t.production_year>2012 AND mc.company_id>16708 AND mc.company_type_id=2;

-- train m58704
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND mc.company_id<207396;

-- train m54532
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mc.company_id>924;

-- train m73475
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year<2011 AND mi_idx.info_type_id=101;

-- train m22680
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id>1 AND t.production_year>1973;

-- train m74004
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.production_year<2012 AND mk.keyword_id=981;

-- train m14
SELECT COUNT(*) FROM cast_info ci WHERE ci.role_id=10;

-- train m28802
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id<133993;

-- train m30787
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id<75;

-- train m23066
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<13967;

-- train m2109
SELECT COUNT(*) FROM title t WHERE t.production_year>1914;

-- train m4160
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id>1 AND t.production_year<1961;

-- train m16248
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.production_year<1974;

-- train m13493
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id=1 AND t.production_year>1996 AND mi.info_type_id<9;

-- train m336
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year>2007;

-- train m83434
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND mk.keyword_id<6277;

-- train m56918
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND mc.company_id>846 AND mc.company_type_id=2;

-- train m86657
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND ci.role_id=1 AND mi.info_type_id<105;

-- train m77307
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=2 AND t.production_year<2008 AND mi_idx.info_type_id>99;

-- train m38476
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND mk.keyword_id>2899;

-- train m54497
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id<7 AND t.production_year<2010 AND mc.company_id<225 AND mc.company_type_id=1;

-- train m47903
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>1 AND t.production_year>2007 AND mc.company_id=329 AND mi_idx.info_type_id<100;

-- train m13960
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mk.keyword_id<13395;

-- train m10578
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.production_year>1988;

-- train m55103
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id>1 AND mk.keyword_id=1951;

-- train m31855
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id=3 AND mk.keyword_id<8749;

-- train m3551
SELECT COUNT(*) FROM cast_info ci WHERE ci.role_id<8;

-- train m67206
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id>33 AND mc.company_type_id>1;

-- train m36715
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id=88;

-- train m69229
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<3289;

-- train m39462
SELECT COUNT(*) FROM title t WHERE t.kind_id=7 AND t.production_year>1948;

-- train m70953
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id=1 AND t.production_year>2001 AND ci.role_id>10;

-- train m62018
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.production_year>2000 AND mc.company_id<72205 AND mc.company_type_id=1;

-- train m55338
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id<2 AND t.production_year=1975 AND mi.info_type_id=98;

-- train m48044
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.production_year<2005 AND mi_idx.info_type_id>101;

-- train m55921
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.production_year=2012 AND mk.keyword_id>944;

-- train m41819
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id<7 AND t.production_year=1966 AND mc.company_id<27705 AND mc.company_type_id<2;

-- train m21118
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.production_year>2006 AND ci.role_id>1 AND mi.info_type_id>2;

-- train m18179
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>3 AND t.production_year=2012;

-- train m52947
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND t.production_year=2013;

-- train m77926
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id>4 AND t.production_year<1958 AND mi.info_type_id>98;

-- train m46023
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year<2007 AND mc.company_id<70905 AND mc.company_type_id<2 AND mi_idx.info_type_id<101;

-- train m84089
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mk.keyword_id<26673;

-- train m73133
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>2 AND t.production_year<1994 AND mi_idx.info_type_id=101;

-- train m25252
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id=1 AND t.production_year<2007 AND mi.info_type_id<97;

-- train m44000
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.production_year<1983 AND mi_idx.info_type_id=101 AND mk.keyword_id=2644;

-- train m21122
SELECT COUNT(*) FROM cast_info ci WHERE ci.role_id=11;

-- train m56190
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id>76369 AND mc.company_type_id>1;

-- train m289
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id>6;

-- train m74856
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id>1420;

-- train m2673
SELECT COUNT(*) FROM title t WHERE t.kind_id=7 AND t.production_year=1962;

-- train m59505
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id>1 AND t.production_year=1969 AND ci.role_id=6;

-- train m19898
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id=1 AND t.production_year>1977 AND mc.company_id=174 AND mc.company_type_id=1;

-- train m34839
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id=7 AND t.production_year<2012 AND mi.info_type_id>7;

-- train m2842
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.production_year>2000 AND mi_idx.info_type_id<101;

-- train m45715
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id>3 AND t.production_year=2008 AND mk.keyword_id>11483;

-- train m56024
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id=1 AND t.production_year>2009 AND mc.company_type_id<2 AND ci.role_id=10;

-- train m65035
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.production_year>1989 AND mi.info_type_id>4;

-- train m85269
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year<2012 AND mi_idx.info_type_id>99;

-- train m36659
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND ci.role_id>1 AND mk.keyword_id=9913;

-- train m59159
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.production_year=1996 AND mc.company_id<19 AND mi.info_type_id=1;

-- train m32139
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year>2008 AND mc.company_type_id<2 AND mi_idx.info_type_id=100;

-- train m79360
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mc.company_id<1527 AND mk.keyword_id>20778;

-- train m34971
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.production_year=1991 AND mi.info_type_id=4;

-- train m85033
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND mk.keyword_id>26350;

-- train m82013
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.production_year<2012 AND mi_idx.info_type_id=101 AND mk.keyword_id=3393;

-- train m3360
SELECT COUNT(*) FROM cast_info ci WHERE ci.role_id<9;

-- train m83560
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id>841;

-- train m6723
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id<62;

-- train m28548
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<132;

-- train m3451
SELECT COUNT(*) FROM title t WHERE t.kind_id<7 AND t.production_year<1982;

-- train m3999
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id>2 AND t.production_year<2008 AND ci.role_id<2;

-- train m73601
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id>1 AND mc.company_id<468;

-- train m42557
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id<2 AND t.production_year>1897;

-- train m29026
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id>1 AND t.production_year<2008 AND mi_idx.info_type_id<100;

-- train m45252
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id=2 AND mk.keyword_id=3857;

-- train m44665
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND mc.company_id=166 AND mc.company_type_id<2;

-- train m65044
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id>3 AND mi.info_type_id=17;

-- train m13537
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.production_year<1976 AND ci.role_id=1 AND mi_idx.info_type_id<101;

-- train m64961
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND mk.keyword_id>11793;

-- train m79851
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.production_year=2010 AND mc.company_id=63237 AND mc.company_type_id>1;

-- train m32977
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND mc.company_id<1781 AND mi_idx.info_type_id>100;

-- train m50862
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mc.company_type_id<2 AND mk.keyword_id<4472;

-- train m52564
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND mi.info_type_id<8 AND mi_idx.info_type_id<101;

-- train m39228
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id<6 AND t.production_year=2003 AND mi.info_type_id=2 AND mk.keyword_id<359;

-- train m80364
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND mi_idx.info_type_id=100 AND mk.keyword_id=117;

-- train m11409
SELECT COUNT(*) FROM cast_info ci WHERE ci.role_id>7;

-- train m82565
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id=3881 AND mc.company_type_id>1;

-- train m27832
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id=50;

-- train m265
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id=3666;

-- train m17453
SELECT COUNT(*) FROM title t WHERE t.kind_id>1 AND t.production_year<2010;

-- train m24523
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id=7 AND t.production_year>1997;

-- train m85604
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.production_year<2013 AND mc.company_id=164689 AND mc.company_type_id>1;

-- train m56994
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id=7 AND t.production_year>2011 AND mi.info_type_id=1;

-- train m7850
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year=1976 AND mi_idx.info_type_id=99;

-- train m92793
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id>4 AND t.production_year<2012 AND mk.keyword_id<4801;

-- train m93230
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id=1 AND t.production_year=2007 AND mc.company_id<2690;

-- train m70763
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id=1 AND t.production_year<1967;

-- train m95256
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year=1995;

-- train m32481
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND ci.role_id=8 AND mk.keyword_id>32850;

-- train m11475
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND mc.company_id=7 AND mi.info_type_id<4;

-- train m27792
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND mc.company_id>11 AND mc.company_type_id<2;

-- train m15125
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mc.company_id=160 AND mc.company_type_id<2 AND mk.keyword_id=9841;

-- train m79341
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.production_year>1997 AND mi_idx.info_type_id>101;

-- train m39959
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id=1 AND mi.info_type_id>16;

-- train m95095
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id<4 AND mi_idx.info_type_id>99 AND mk.keyword_id>24382;

-- train m1287
SELECT COUNT(*) FROM cast_info ci WHERE ci.role_id>3;

-- train m35877
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id>1284 AND mc.company_type_id=1;

-- train m95
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id<18;

-- train m49835
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id=14889;

-- train m55238
SELECT COUNT(*) FROM title t WHERE t.kind_id>2 AND t.production_year=2002;

-- train m57284
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.production_year<1956 AND ci.role_id=1;

-- train m13435
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.production_year>2013 AND mc.company_id<11138 AND mc.company_type_id>1;

-- train m60239
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.production_year>2012 AND mi.info_type_id>16;

-- train m43015
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.production_year<2006 AND mi_idx.info_type_id=100;

-- train m87623
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id=7 AND mk.keyword_id<2292;

-- train m66173
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id<7 AND t.production_year>1998 AND mc.company_id>480 AND ci.role_id>3;

-- train m11215
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.production_year<2006 AND ci.role_id<10 AND mi.info_type_id<8;

-- train m18359
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.production_year>1996;

-- train m12486
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND t.production_year>2010 AND mk.keyword_id>4895;

-- train m28549
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id=7 AND t.production_year<2003 AND mc.company_id>171;

-- train m17056
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.production_year>2007 AND mc.company_id<137 AND mc.company_type_id<2;

-- train m97772
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.production_year>2007 AND mc.company_id<2253 AND mc.company_type_id>1;

-- train m60872
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<4 AND t.production_year<2007;

-- train m42712
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND mi.info_type_id=104 AND mk.keyword_id>2193;

-- train m585
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND t.production_year=1969 AND mi_idx.info_type_id=100;

-- train m4659
SELECT COUNT(*) FROM cast_info ci WHERE ci.role_id>5;

-- train m55691
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id>60724;

-- train m711
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id=94;

-- train m40468
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id>20521;

-- train m95411
SELECT COUNT(*) FROM title t WHERE t.kind_id>2 AND t.production_year>1921;

-- train m16892
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id=1 AND t.production_year>1990;

-- train m13395
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.production_year=2004 AND mc.company_id>12253 AND mc.company_type_id=2;

-- train m14953
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.production_year<1912 AND mi.info_type_id>107;

-- train m6130
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id>1 AND t.production_year>1984;

-- train m93804
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id=1 AND t.production_year>2009 AND mk.keyword_id=3138;

-- train m9507
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id<7 AND t.production_year=2008;

-- train m39711
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id>1 AND t.production_year<1942 AND ci.role_id>1;

-- train m87418
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<4 AND t.production_year>1953;

-- train m13729
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND ci.role_id>8 AND mk.keyword_id=122578;

-- train m76402
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id>3 AND mi.info_type_id>8;

-- train m92735
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>3 AND t.production_year>2003 AND mc.company_id>19;

-- train m23793
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND t.production_year>1999 AND mc.company_id<125791;

-- train m66047
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>1 AND t.production_year<1998 AND mi.info_type_id=7 AND mi_idx.info_type_id=99;

-- train m39077
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND mi.info_type_id<4 AND mk.keyword_id=398;

-- train m12023
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id=1 AND t.production_year>2008 AND mi_idx.info_type_id<101;

-- train m1443
SELECT COUNT(*) FROM cast_info ci WHERE ci.role_id>1;

-- train m9879
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id<7527 AND mc.company_type_id=1;

-- train m35806
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id>67;

-- train m53974
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id>47389;

-- train m46186
SELECT COUNT(*) FROM title t WHERE t.kind_id=1 AND t.production_year>1989;

-- train m7990
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id>2;

-- train m717
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.production_year=2000;

-- train m80008
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id>1 AND t.production_year>1970 AND mi.info_type_id>6;

-- train m71160
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id=1 AND t.production_year<1912;

-- train m1706
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id<7 AND mk.keyword_id<872;

-- train m82378
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id<7 AND t.production_year>1944 AND mc.company_type_id<2;

-- train m6669
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.production_year=1918 AND mi.info_type_id<104;

-- train m89668
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>2 AND t.production_year>1998 AND ci.role_id=1 AND mi_idx.info_type_id<100;

-- train m31582
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND mk.keyword_id>21542;

-- train m3329
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.production_year=2008 AND mi.info_type_id<16;

-- train m92810
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.production_year>2011 AND mi_idx.info_type_id<100;

-- train m37222
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mk.keyword_id=3636;

-- train m12361
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND mi.info_type_id<6;

-- train m91477
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.production_year=1989;

-- train m83438
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.production_year>1920 AND mi_idx.info_type_id=101;

-- train m66547
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id<159;

-- train m29254
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id=51;

-- train m19419
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<64206;

-- train m231
SELECT COUNT(*) FROM title t WHERE t.production_year=1985;

-- train m51961
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.production_year>1917;

-- train m52164
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id=7 AND t.production_year<1998 AND mc.company_id>36 AND mc.company_type_id<2;

-- train m50526
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id>4 AND t.production_year>2006 AND mi.info_type_id=7;

-- train m13849
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.production_year=2007 AND mi_idx.info_type_id>101;

-- train m71047
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.production_year=1980;

-- train m43873
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND ci.role_id<6;

-- train m88320
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.production_year<2011 AND ci.role_id>1 AND mi.info_type_id<8;

-- train m93326
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year>2012 AND ci.role_id>1;

-- train m43936
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND t.production_year>2003;

-- train m96638
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.production_year=2003 AND mi.info_type_id>16;

-- train m41757
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<2 AND t.production_year>1977;

-- train m16289
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mk.keyword_id=5703;

-- train m40316
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year=2007 AND mi.info_type_id=4 AND mi_idx.info_type_id>100;

-- train m6343
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND mk.keyword_id=6265;

-- train m98555
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id<3 AND mk.keyword_id=1415;

-- train m5619
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id=66845 AND mc.company_type_id>1;

-- train m10143
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id>51;

-- train m9484
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id>12189;

-- train m37859
SELECT COUNT(*) FROM title t WHERE t.kind_id=1 AND t.production_year>2013;

-- train m69720
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id=1 AND t.production_year=2004;

-- train m29223
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND mc.company_id<1102;

-- train m5572
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.production_year=1991;

-- train m69825
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.production_year>2002 AND mi_idx.info_type_id<101;

-- train m86960
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND mk.keyword_id<21125;

-- train m87569
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.production_year>2008;

-- train m18505
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND ci.role_id>6;

-- train m56632
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.production_year<1973 AND mi_idx.info_type_id=101;

-- train m16202
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.production_year<2009 AND ci.role_id=1 AND mk.keyword_id>17013;

-- train m40495
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id<7 AND t.production_year>2007 AND mc.company_id>11774 AND mc.company_type_id<2;

-- train m28192
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND mc.company_id>201;

-- train m93405
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND t.production_year<2001 AND mk.keyword_id=2562;

-- train m59777
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.production_year=2010 AND mi.info_type_id<6;

-- train m90670
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND mi.info_type_id=16 AND mk.keyword_id>7156;

-- train m81919
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.production_year=2006 AND mi_idx.info_type_id>99 AND mk.keyword_id<1138;

-- train m37927
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id=93316 AND mc.company_type_id>1;

-- train m4453
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id<107;

-- train m19411
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id=3678;

-- train m612
SELECT COUNT(*) FROM title t WHERE t.kind_id<7;

-- train m13624
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id=4 AND t.production_year=2010;

-- train m64897
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.production_year>1969 AND mc.company_type_id>1;

-- train m71664
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.production_year<2007 AND mi.info_type_id=2;

-- train m17944
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id<3 AND t.production_year=2001;

-- train m17399
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id=4 AND t.production_year=2006;

-- train m86329
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND mc.company_id<72776;

-- train m58085
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id>1 AND mi.info_type_id<4;

-- train m84897
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year>1961;

-- train m28231
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.production_year=1971 AND mk.keyword_id>3636;

-- train m22906
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.production_year=2012;

-- train m61731
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year>2009 AND mc.company_type_id<2 AND mi_idx.info_type_id=99;

-- train m34285
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND t.production_year<1914 AND mc.company_id<159 AND mk.keyword_id>1561;

-- train m34905
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND mi.info_type_id<18 AND mi_idx.info_type_id>99;

-- train m34311
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id=1 AND t.production_year>2008 AND mi.info_type_id>90;

-- train m11629
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND mk.keyword_id>96787;

-- train m85046
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id=11690;

-- train m18853
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id=35;

-- train m81440
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<18585;

-- train m46919
SELECT COUNT(*) FROM title t WHERE t.kind_id<3 AND t.production_year=2010;

-- train m28037
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id=7 AND t.production_year=2008 AND ci.role_id<2;

-- train m95121
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND mc.company_id>11203 AND mc.company_type_id=2;

-- train m65927
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.production_year<1973 AND mi.info_type_id>1;

-- train m83777
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id=3 AND t.production_year=2006 AND mi_idx.info_type_id<101;

-- train m14828
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.production_year=1947;

-- train m30109
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND ci.role_id<2;

-- train m14920
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id<7 AND t.production_year>2005;

-- train m26869
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=1 AND t.production_year<1997 AND mi_idx.info_type_id>100;

-- train m23261
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND t.production_year<2011;

-- train m79705
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.production_year=1998 AND mc.company_id>9221 AND mi.info_type_id=2;

-- train m64518
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=3 AND mc.company_id=125 AND mc.company_type_id>1;

-- train m83365
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND t.production_year>2001 AND mc.company_id<1705 AND mc.company_type_id=1;

-- train m24329
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.production_year<1969;

-- train m7404
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND t.production_year<1964;

-- train m25702
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND mk.keyword_id=3846;

-- train m28780
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id=23468 AND mc.company_type_id>1;

-- train m30383
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id<110;

-- train m74714
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id=7168;

-- train m16883
SELECT COUNT(*) FROM title t WHERE t.kind_id=1 AND t.production_year=1956;

-- train m6581
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.production_year>1912;

-- train m55791
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id<7 AND t.production_year>2005 AND mc.company_id<71881 AND mc.company_type_id=2;

-- train m15446
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id>2 AND t.production_year<2002;

-- train m29878
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id>2 AND t.production_year>1961;

-- train m61261
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.production_year<2007 AND mk.keyword_id<16385;

-- train m35597
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id=4 AND mc.company_id>3349;

-- train m19176
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND mi.info_type_id=98;

-- train m64162
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year=1969;

-- train m57644
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.production_year>2010 AND mk.keyword_id>4194;

-- train m30669
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND mc.company_id<1283;

-- train m45727
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND mc.company_id<7669 AND mi_idx.info_type_id=99;

-- train m4167
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id>4 AND t.production_year>1995 AND mk.keyword_id=393;

-- train m2322
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year=2006;

-- train m14699
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.production_year>2000 AND mi.info_type_id<4 AND mk.keyword_id<6099;

-- train m98621
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND mk.keyword_id<901;

-- train m25502
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id>594;

-- train m86733
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id>78;

-- train m407
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id=3329;

-- train m84862
SELECT COUNT(*) FROM title t WHERE t.kind_id<2 AND t.production_year<2004;

-- train m95374
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.production_year>1933 AND ci.role_id>8;

-- train m56372
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.production_year<2010 AND mc.company_id<119220;

-- train m72447
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id<3 AND t.production_year<1996 AND mi.info_type_id<16;

-- train m38238
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.production_year=1952 AND mi_idx.info_type_id<100;

-- train m62665
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.production_year<2014;

-- train m1095
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.production_year>1991 AND ci.role_id>3;

-- train m74971
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id=7 AND t.production_year=2006;

-- train m2409
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.production_year>2010;

-- train m8719
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND mk.keyword_id>29029;

-- train m56884
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND mc.company_id<62949 AND mc.company_type_id<2 AND mi.info_type_id>5;

-- train m67495
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>3 AND t.production_year>1928 AND mi_idx.info_type_id=100;

-- train m35901
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND mc.company_id<11 AND mc.company_type_id<2 AND mk.keyword_id=335;

-- train m22511
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>4 AND t.production_year=2010 AND mi.info_type_id=3;

-- train m45990
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.production_year>2013 AND mi.info_type_id>3;

-- train m84640
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND mk.keyword_id>3219;

-- train m56428
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id<13365;

-- train m4327
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id<40;

-- train m19612
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<2298;

-- train m375
SELECT COUNT(*) FROM title t WHERE t.production_year<2008;

-- train m61332
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id<7 AND t.production_year<1978;

-- train m36993
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id<3 AND mc.company_id<26253;

-- train m85730
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id=1 AND t.production_year<1930 AND mi.info_type_id>3;

-- train m50256
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year<2003 AND mi_idx.info_type_id=101;

-- train m59152
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id=1 AND t.production_year<2004;

-- train m30961
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.production_year<2011;

-- train m65303
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.production_year>2005 AND mi.info_type_id<16;

-- train m39723
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND ci.role_id=8 AND mi_idx.info_type_id>100;

-- train m17815
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id<2 AND ci.role_id>1 AND mk.keyword_id>9369;

-- train m55479
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id<7 AND t.production_year=1983 AND mi.info_type_id>3;

-- train m71883
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>1 AND mc.company_id>11147;

-- train m64072
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mc.company_id<76704 AND mk.keyword_id<2273;

-- train m10727
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=1 AND mi.info_type_id=16 AND mi_idx.info_type_id<100;

-- train m48693
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.production_year<1909 AND mi.info_type_id>16;

-- train m22072
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND t.production_year>1998;

-- train m92692
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id=4617;

-- train m104
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id>1;

-- train m10507
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<4372;

-- train m39687
SELECT COUNT(*) FROM title t WHERE t.kind_id=7 AND t.production_year<1996;

-- train m14248
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id=7 AND t.production_year>2013;

-- train m19969
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id<7 AND mc.company_id<6806;

-- train m63567
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id=1 AND t.production_year<2008 AND mi.info_type_id<4;

-- train m8254
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.production_year>1985 AND mi_idx.info_type_id>101;

-- train m5777
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND mk.keyword_id=10323;

-- train m93368
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.production_year>2007 AND mc.company_id>25393 AND mc.company_type_id<2;

-- train m74055
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.production_year>2007 AND mi.info_type_id>6;

-- train m32042
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=2;

-- train m74267
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND ci.role_id=8 AND mk.keyword_id<7286;

-- train m39100
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id=3 AND t.production_year>2006 AND mi.info_type_id>16;

-- train m93891
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.production_year=2008 AND mc.company_type_id<2;

-- train m98417
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND mc.company_id<3763 AND mk.keyword_id<5943;

-- train m47033
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>2 AND t.production_year=2001 AND mi_idx.info_type_id<101;

-- train m28009
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id>1 AND t.production_year>2004 AND mi.info_type_id=5 AND mk.keyword_id=335;

-- train m26832
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id=4 AND mi_idx.info_type_id=99;

-- train m92077
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id=577 AND mc.company_type_id<2;

-- train m135
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id=18;

-- train m34165
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<2905;

-- train m60504
SELECT COUNT(*) FROM title t WHERE t.kind_id<2 AND t.production_year<1934;

-- train m30879
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.production_year<1914 AND ci.role_id>1;

-- train m60253
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id<4 AND t.production_year=1951;

-- train m39907
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id>1 AND t.production_year=2009 AND mi.info_type_id<4;

-- train m50281
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id>4 AND t.production_year>1999 AND mi_idx.info_type_id=99;

-- train m32547
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND mk.keyword_id<51;

-- train m89423
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id<7 AND t.production_year<2002 AND ci.role_id>1;

-- train m40636
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.production_year=1969;

-- train m5789
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>1 AND ci.role_id>1;

-- train m44851
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND mk.keyword_id=11393;

-- train m56566
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND mc.company_id<11145 AND mi.info_type_id=2;

-- train m36531
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND mc.company_id<10707;

-- train m9414
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND t.production_year=1953 AND mc.company_id>36392 AND mk.keyword_id<11662;

-- train m27175
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>1 AND t.production_year>2011 AND mi.info_type_id=8;

-- train m94851
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND mk.keyword_id=4227;

-- train m87668
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND mi_idx.info_type_id<100 AND mk.keyword_id=2853;

-- train m75052
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id<596 AND mc.company_type_id=1;

-- train m1437
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id>94;

-- train m52016
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<7665;

-- train m41138
SELECT COUNT(*) FROM title t WHERE t.kind_id>2 AND t.production_year>2000;

-- train m83218
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.production_year>1973 AND ci.role_id=2;

-- train m7626
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.production_year<1977;

-- train m17842
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.production_year>1980;

-- train m18738
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.production_year<1979 AND mi_idx.info_type_id=99;

-- train m7282
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id=7 AND t.production_year>2013;

-- train m94839
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id=7 AND ci.role_id<9;

-- train m70602
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.production_year<1990;

-- train m98231
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.production_year<1990 AND mi_idx.info_type_id>99;

-- train m48661
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND mk.keyword_id=1655;

-- train m99627
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id=7 AND t.production_year>1931 AND mc.company_type_id=1 AND mi.info_type_id>16;

-- train m22346
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND mc.company_type_id=2 AND mi_idx.info_type_id=100;

-- train m11821
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mk.keyword_id=335;

-- train m33610
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.production_year>1896 AND mi_idx.info_type_id<101;

-- train m24233
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND mk.keyword_id=11582;

-- train m79251
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.production_year>1992 AND mi_idx.info_type_id=99;

-- train m71313
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id<109202 AND mc.company_type_id<2;

-- train m60586
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id<43;

-- train m57785
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<4487;

-- train m10021
SELECT COUNT(*) FROM title t WHERE t.kind_id=2 AND t.production_year>2009;

-- train m24404
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id=1 AND t.production_year<1977 AND ci.role_id>1;

-- train m17312
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id>1 AND t.production_year>1990 AND mc.company_id=12871 AND mc.company_type_id=1;

-- train m62380
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id=1 AND t.production_year=1976;

-- train m5385
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year<2013 AND mi_idx.info_type_id<100;

-- train m83238
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND mk.keyword_id<4745;

-- train m51438
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id>1 AND mc.company_type_id>1 AND ci.role_id=1;

-- train m44747
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id=7 AND t.production_year<1983 AND mi.info_type_id=3;

-- train m68701
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND ci.role_id=7;

-- train m7399
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND ci.role_id>1 AND mk.keyword_id>6931;

-- train m41928
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.production_year>2001 AND mc.company_id<164509;

-- train m39456
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND mc.company_id>4436 AND mi_idx.info_type_id>101;

-- train m67227
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mc.company_id>137 AND mc.company_type_id>1 AND mk.keyword_id=16264;

-- train m14326
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.production_year>1981;

-- train m15062
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND mi.info_type_id<3 AND mk.keyword_id>2292;

-- train m53656
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id=1 AND t.production_year<2000 AND mi_idx.info_type_id>99;

-- train m44753
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id>13928;

-- train m51640
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id=86;

-- train m2707
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id=8679;

-- train m2619
SELECT COUNT(*) FROM title t WHERE t.production_year=1982;

-- train m8387
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id<7 AND t.production_year=2012 AND ci.role_id=1;

-- train m93356
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id=7 AND t.production_year>1959;

-- train m17493
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.production_year<1962 AND mi.info_type_id=4;

-- train m44761
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.production_year=1899 AND mi_idx.info_type_id=100;

-- train m35937
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id=7 AND mk.keyword_id=542;

-- train m81826
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.production_year>2006 AND ci.role_id>1;

-- train m97277
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id=1 AND t.production_year=1997;

-- train m26803
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>1 AND t.production_year>1968 AND mi_idx.info_type_id=101;

-- train m17951
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.production_year<2012 AND mk.keyword_id=245;

-- train m8547
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.production_year<2012;

-- train m61202
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND mc.company_id<7699 AND mc.company_type_id=2;

-- train m85362
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mc.company_id=51 AND mk.keyword_id=4472;

-- train m34377
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.production_year=2007 AND mi.info_type_id=2 AND mi_idx.info_type_id<100;

-- train m12911
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND t.production_year<2013 AND mi.info_type_id=18 AND mk.keyword_id>7078;

-- train m89318
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND t.production_year=1998 AND mi_idx.info_type_id=100;

-- train m48144
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id=10999 AND mc.company_type_id<2;

-- train m15943
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id<80;

-- train m43806
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<67265;

-- train m95605
SELECT COUNT(*) FROM title t WHERE t.kind_id>4 AND t.production_year=2004;

-- train m9378
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.production_year=2013 AND ci.role_id<9;

-- train m65442
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND mc.company_id>9328;

-- train m42738
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id=7 AND t.production_year=1997 AND mi.info_type_id>4;

-- train m54399
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year<2008 AND mi_idx.info_type_id>100;

-- train m38409
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND mk.keyword_id>8481;

-- train m49787
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND mc.company_id>15761 AND mc.company_type_id=2;

-- train m6438
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.production_year<1976 AND mi.info_type_id<4;

-- train m38699
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.production_year=1896 AND ci.role_id=6 AND mi_idx.info_type_id<101;

-- train m60196
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND ci.role_id>10 AND mk.keyword_id=931;

-- train m78333
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.production_year>2008 AND mc.company_type_id=2 AND mi.info_type_id>6;

-- train m33445
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=1 AND t.production_year>2010;

-- train m38105
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND t.production_year>1946 AND mc.company_id=6 AND mc.company_type_id=1;

-- train m75894
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year=1971 AND mi_idx.info_type_id=101;

-- train m40056
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND t.production_year=1969 AND mi.info_type_id>13 AND mk.keyword_id>10468;

-- train m6489
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND t.production_year>2007 AND mi_idx.info_type_id<100 AND mk.keyword_id<6243;

-- train m11019
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id<11135;

-- train m14142
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id=96;

-- train m79138
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<10922;

-- train m4247
SELECT COUNT(*) FROM title t WHERE t.kind_id<7 AND t.production_year=1970;

-- train m90279
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id<4 AND t.production_year=1991;

-- train m9281
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND mc.company_id<596;

-- train m62129
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id>1 AND t.production_year>1967 AND mi.info_type_id<15;

-- train m27413
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.production_year>1989 AND mi_idx.info_type_id>101;

-- train m93599
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.production_year<2012 AND mk.keyword_id=870;

-- train m71307
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.production_year>1979 AND mc.company_id>54429 AND mc.company_type_id>1;

-- train m14242
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id<4 AND t.production_year<2004;

-- train m54815
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year<2012 AND ci.role_id<2;

-- train m57100
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND mk.keyword_id=842;

-- train m54130
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND mc.company_id>1621 AND mc.company_type_id>1 AND mi.info_type_id=16;

-- train m88316
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND mc.company_id=86981 AND mc.company_type_id=2;

-- train m46370
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id=1 AND t.production_year=2010 AND mc.company_id<4880 AND mc.company_type_id=2;

-- train m6433
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year=1973 AND mi.info_type_id=3;

-- train m38526
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND mk.keyword_id<89251;

-- train m11558
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id=4 AND mk.keyword_id>5360;

-- train m74077
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id>21587 AND mc.company_type_id>1;

-- train m25105
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id>88;

-- train m1602
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id=76890;

-- train m39038
SELECT COUNT(*) FROM title t WHERE t.kind_id>4 AND t.production_year>1992;

-- train m68655
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id<3 AND ci.role_id>1;

-- train m26541
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id<7 AND t.production_year>2010 AND mc.company_id<2;

-- train m95444
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id=7 AND t.production_year>1954 AND mi.info_type_id<98;

-- train m92200
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year>1943 AND mi_idx.info_type_id<101;

-- train m70943
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id<3 AND t.production_year<1992 AND mk.keyword_id=1294;

-- train m36596
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id=1 AND mc.company_id=6778 AND ci.role_id<2;

-- train m28373
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id>1 AND mi.info_type_id<8;

-- train m62319
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.production_year>1995 AND mi_idx.info_type_id<101;

-- train m86579
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND ci.role_id=1 AND mk.keyword_id<14508;

-- train m26938
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND mc.company_id<584 AND mc.company_type_id=2;

-- train m2338
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND mc.company_id<79838 AND mc.company_type_id=2;

-- train m19157
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND mc.company_type_id=2;

-- train m97635
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>1 AND t.production_year>1943 AND mi.info_type_id=16 AND mi_idx.info_type_id=99;

-- train m22900
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.production_year<2010 AND mi.info_type_id>8;

-- train m1243
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.production_year>2007 AND mi_idx.info_type_id=101 AND mk.keyword_id<7818;

-- train m35684
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id>11705 AND mc.company_type_id>1;

-- train m38902
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id=54;

-- train m50475
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id>8039;

-- train m881
SELECT COUNT(*) FROM title t WHERE t.kind_id=7 AND t.production_year<1917;

-- train m63376
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id=3 AND t.production_year>2012 AND ci.role_id<2;

-- train m28170
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id>1 AND t.production_year>1959;

-- train m87085
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id>1 AND t.production_year=1957 AND mi.info_type_id=7;

-- train m65474
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.production_year<1999 AND mi_idx.info_type_id=99;

-- train m19757
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.production_year=1964 AND mk.keyword_id>1450;

-- train m57214
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.production_year=2012 AND mc.company_id<6 AND mc.company_type_id=2;

-- train m50664
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id<2 AND t.production_year=2003 AND mi.info_type_id=16;

-- train m19012
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND ci.role_id<10;

-- train m83263
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND ci.role_id=10 AND mk.keyword_id>824;

-- train m32644
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.production_year<2008 AND mc.company_type_id=1 AND mi.info_type_id<7;

-- train m94331
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year=1994 AND mc.company_id=115 AND mi_idx.info_type_id=99;

-- train m97209
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mc.company_id>88222 AND mc.company_type_id>1;

-- train m82890
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.production_year>2001 AND mi.info_type_id=3;

-- train m90455
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.production_year<2006 AND mi.info_type_id=3;

-- train m80595
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND mk.keyword_id<365;

-- train m21274
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id=2601 AND mc.company_type_id<2;

-- train m819
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id>107;

-- train m69264
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id=71547;

-- train m91440
SELECT COUNT(*) FROM title t WHERE t.kind_id<7 AND t.production_year>1927;

-- train m2234
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id>1 AND t.production_year=1992;

-- train m22739
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.production_year=2003 AND mc.company_type_id=1;

-- train m7135
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id=7 AND t.production_year>1967;

-- train m96703
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.production_year>1963 AND mi_idx.info_type_id>99;

-- train m60506
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND mk.keyword_id>72217;

-- train m15851
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.production_year>1960 AND ci.role_id=1;

-- train m1214
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.production_year<2008 AND mi.info_type_id=7;

-- train m88032
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=4 AND t.production_year>1987;

-- train m254
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND mk.keyword_id<3660;

-- train m90988
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.production_year>2005 AND mi.info_type_id>1;

-- train m18941
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.production_year<1994;

-- train m75883
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND t.production_year<2009 AND mc.company_id=14171 AND mc.company_type_id<2;

-- train m22554
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.production_year>1911 AND mi_idx.info_type_id=100;

-- train m85037
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND mi.info_type_id=2 AND mk.keyword_id>20324;

-- train m73678
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND mk.keyword_id=1620;

-- train m8704
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id>1402;

-- train m452
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id=15;

-- train m83950
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<50131;

-- train m59290
SELECT COUNT(*) FROM title t WHERE t.kind_id<3 AND t.production_year=1977;

-- train m57624
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id=1 AND t.production_year=1980 AND ci.role_id>2;

-- train m57013
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id=1 AND mc.company_id>97932 AND mc.company_type_id=2;

-- train m80463
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id>1 AND t.production_year=1959;

-- train m10134
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year>2006 AND mi_idx.info_type_id>100;

-- train m66420
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id=1 AND t.production_year=1985 AND mk.keyword_id=2545;

-- train m22463
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND mc.company_id<15194;

-- train m13295
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id=2 AND t.production_year<2006;

-- train m62139
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.production_year=2009;

-- train m80070
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id=3 AND t.production_year>2007 AND mk.keyword_id>1381;

-- train m45496
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.production_year<2009 AND mi.info_type_id=16;

-- train m10806
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.production_year>2009 AND mi_idx.info_type_id<101;

-- train m77100
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND t.production_year<1974 AND mc.company_type_id=2;

-- train m2714
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=1 AND t.production_year=1962 AND mi_idx.info_type_id=99;

-- train m21749
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.production_year=2008 AND mi.info_type_id=2;

-- train m48090
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND mk.keyword_id<8202;

-- train m20744
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id>1868 AND mc.company_type_id>1;

-- train m39978
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id<54;

-- train m25421
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<5199;

-- train m81061
SELECT COUNT(*) FROM title t WHERE t.kind_id>3 AND t.production_year>2000;

-- train m65189
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id<4 AND t.production_year=2010;

-- train m88049
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id=7 AND t.production_year>1979 AND mc.company_type_id=1;

-- train m75171
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id<7 AND t.production_year=1987;

-- train m63076
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id=1 AND t.production_year>2010 AND mi_idx.info_type_id=99;

-- train m1133
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND mk.keyword_id=19806;

-- train m68130
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id<7 AND t.production_year>2004 AND mc.company_id=71884 AND mc.company_type_id>1;

-- train m15805
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id=4 AND mi.info_type_id<7;

-- train m98530
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.production_year<1991 AND mi_idx.info_type_id>101;

-- train m94951
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND mk.keyword_id>317;

-- train m18849
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.production_year>1984 AND mc.company_id>2425 AND mi.info_type_id<5;

-- train m39539
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND mc.company_id>11;

-- train m26755
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mc.company_id>432 AND mc.company_type_id=1;

-- train m87978
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.production_year>1976 AND mi.info_type_id=4;

-- train m99293
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND mk.keyword_id>57842;

-- train m85985
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND mi_idx.info_type_id=100 AND mk.keyword_id=4194;

-- train m10041
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id=680 AND mc.company_type_id=2;

-- train m6745
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id<91;

-- train m47463
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<63804;

-- train m86954
SELECT COUNT(*) FROM title t WHERE t.kind_id>1 AND t.production_year=1979;

-- train m85777
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.production_year>1911;

-- train m18811
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND mc.company_id>11345 AND mc.company_type_id>1;

-- train m57395
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id=1 AND t.production_year=1984 AND mi.info_type_id=8;

-- train m22933
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year>1967;

-- train m18741
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id>1 AND t.production_year=1992;

-- train m12462
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND mc.company_id>9471 AND mc.company_type_id>1;

-- train m58927
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.production_year=1964 AND ci.role_id=10 AND mi.info_type_id<4;

-- train m72901
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.production_year=1955 AND ci.role_id<4 AND mi_idx.info_type_id>100;

-- train m97115
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND mk.keyword_id<6611;

-- train m3616
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND mc.company_id>297 AND mc.company_type_id=2 AND mi.info_type_id>16;

-- train m38006
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND mc.company_id=125803;

-- train m6926
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mc.company_id<71146 AND mc.company_type_id<2 AND mk.keyword_id>10279;

-- train m33471
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.production_year=2000;

-- train m5498
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND mi.info_type_id>3;

-- train m27048
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND t.production_year<1995 AND mk.keyword_id>39198;

-- train m66481
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id>12784 AND mc.company_type_id=2;

-- train m3848
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id>108;

-- train m51581
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<230;

-- train m97035
SELECT COUNT(*) FROM title t WHERE t.kind_id=2 AND t.production_year=2013;

-- train m35186
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.production_year<1997 AND ci.role_id<6;

-- train m88425
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND mc.company_id>2763;

-- train m32309
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id<7 AND t.production_year>2011 AND mi.info_type_id=106;

-- train m23555
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.production_year>1969 AND mi_idx.info_type_id<100;

-- train m71983
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id=7 AND t.production_year<1977 AND mk.keyword_id>19164;

-- train m62866
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id=3 AND t.production_year>2009;

-- train m68754
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.production_year>1926;

-- train m77598
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year=1964;

-- train m83038
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.production_year=2009 AND mk.keyword_id=34504;

-- train m98923
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.production_year>2005 AND mc.company_id>10797;

-- train m22685
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND mc.company_id<11302 AND mc.company_type_id=2 AND mi_idx.info_type_id=99;

-- train m8524
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mk.keyword_id<11267;

-- train m89392
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND mi.info_type_id>1 AND mi_idx.info_type_id=100;

-- train m68212
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND mk.keyword_id<37381;

-- train m92408
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.production_year>0 AND mk.keyword_id<1957;

-- train m71862
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id<45811 AND mc.company_type_id=1;

-- train m14423
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id<88;

-- train m29088
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id=6019;

-- train m2702
SELECT COUNT(*) FROM title t WHERE t.production_year<1977;

-- train m96090
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id<7 AND t.production_year=2007 AND ci.role_id<2;

-- train m77963
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND mc.company_id<219807 AND mc.company_type_id=1;

-- train m99022
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id<7 AND t.production_year<2011 AND mi.info_type_id=3;

-- train m94456
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.production_year>2001 AND mi_idx.info_type_id=101;

-- train m70289
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id>1 AND mk.keyword_id>486;

-- train m66397
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND ci.role_id=11;

-- train m62938
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.production_year>1984;

-- train m44080
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=1 AND t.production_year=1986 AND mi_idx.info_type_id<101;

-- train m82609
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND mk.keyword_id>4581;

-- train m73268
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id>1 AND t.production_year<2005;

-- train m82241
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.production_year<2010 AND mc.company_id>65601 AND mc.company_type_id=1;

-- train m19871
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND t.production_year>1986;

-- train m85180
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=4 AND t.production_year>2012 AND mi.info_type_id=5;

-- train m29978
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND mi.info_type_id>16 AND mk.keyword_id=7384;

-- train m31139
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND mk.keyword_id<33091;

-- train m38537
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id<2231;

-- train m82018
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id=84;

-- train m50099
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id>1767;

-- train m678
SELECT COUNT(*) FROM title t WHERE t.production_year<1987;

-- train m12103
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id>4 AND t.production_year>2011;

-- train m84219
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id<2 AND t.production_year<1999 AND mc.company_id>126671 AND mc.company_type_id=2;

-- train m98440
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id>2 AND t.production_year<1999;

-- train m57171
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.production_year>1981 AND mi_idx.info_type_id<101;

-- train m87303
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id=7 AND t.production_year>2006 AND mk.keyword_id>20338;

-- train m41247
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND mc.company_id<233862 AND ci.role_id>2;

-- train m2876
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.production_year=2009 AND ci.role_id>4 AND mi.info_type_id>2;

-- train m5076
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND mi_idx.info_type_id>100;

-- train m51364
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id>6 AND ci.role_id>1 AND mk.keyword_id=3508;

-- train m63493
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id>1 AND mi.info_type_id<8;

-- train m670
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>1 AND t.production_year>2012;

-- train m23194
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND t.production_year>2010 AND mc.company_id>3729 AND mc.company_type_id<2 AND mk.keyword_id>34413;

-- train m44706
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>1 AND t.production_year>2010 AND mi.info_type_id=3 AND mi_idx.info_type_id=99;

-- train m91164
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND mk.keyword_id=7777;

-- train m94430
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id>1 AND t.production_year<1989 AND mi_idx.info_type_id=100;

-- train m66216
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id=14673 AND mc.company_type_id<2;

-- train m76196
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id=87;

-- train m44763
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<20722;

-- train m7133
SELECT COUNT(*) FROM title t WHERE t.kind_id>1 AND t.production_year<1988;

-- train m62933
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id>4 AND t.production_year=2012;

-- train m19041
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND mc.company_id<121370;

-- train m84955
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id<7 AND t.production_year<1961 AND mi.info_type_id<16;

-- train m28054
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id>1 AND t.production_year=2000;

-- train m4308
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id<7 AND t.production_year=2012 AND mk.keyword_id>6120;

-- train m40983
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id<7 AND mc.company_id=220708;

-- train m25453
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id<3 AND t.production_year<1964 AND ci.role_id=1;

-- train m75604
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>1 AND ci.role_id=3;

-- train m67056
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.production_year>2011 AND ci.role_id=4;

-- train m69954
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND mc.company_id>9353 AND mc.company_type_id>1 AND mi.info_type_id=8;

-- train m89268
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND mc.company_id=3800 AND mi_idx.info_type_id=99;

-- train m56499
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND mk.keyword_id<6054;

-- train m69535
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND mi.info_type_id=16 AND mi_idx.info_type_id=100;

-- train m57654
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND mk.keyword_id<3339;

-- train m4425
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.production_year>1996 AND mi_idx.info_type_id=99 AND mk.keyword_id<4933;

-- train m99645
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id>352 AND mc.company_type_id>1;

-- train m30685
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id<45;

-- train m8880
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id>5792;

-- train m28346
SELECT COUNT(*) FROM title t WHERE t.kind_id>1 AND t.production_year<1975;

-- train m55619
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id=7 AND t.production_year>2010 AND ci.role_id>4;

-- train m26367
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND mc.company_id<21948;

-- train m10240
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id=7 AND t.production_year<2003 AND mi.info_type_id>2;

-- train m19061
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.production_year<2007 AND mi_idx.info_type_id<101;

-- train m80255
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.production_year>2005 AND mk.keyword_id=8998;

-- train m51491
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id=1 AND t.production_year>2000 AND ci.role_id=2;

-- train m77435
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id>1 AND t.production_year=2005 AND mi.info_type_id=16;

-- train m56298
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.production_year=2006 AND ci.role_id=1 AND mi_idx.info_type_id=99;

-- train m83016
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND t.production_year>1996 AND ci.role_id=4;

-- train m44861
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id<7 AND mc.company_id=109318 AND mi.info_type_id<8;

-- train m98153
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=1 AND t.production_year>2007 AND mc.company_id=115 AND mc.company_type_id>1;

-- train m47944
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.production_year=1937;

-- train m88818
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year>1992 AND mi.info_type_id>3 AND mi_idx.info_type_id<100;

-- train m99288
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.production_year>2012 AND mi.info_type_id<4;

-- train m858
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.production_year=1996 AND mi_idx.info_type_id<100 AND mk.keyword_id<398;

-- train m60111
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id>8651;

-- train m378
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id=66;

-- train m66763
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<4604;

-- train m5865
SELECT COUNT(*) FROM title t WHERE t.kind_id=2 AND t.production_year=1998;

-- train m69444
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.production_year=1928;

-- train m39391
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.production_year<2012 AND mc.company_id=57518;

-- train m70219
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id=7 AND t.production_year<1983;

-- train m38260
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year>1973 AND mi_idx.info_type_id<101;

-- train m46447
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND mk.keyword_id=1049;

-- train m72174
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id<7 AND t.production_year>2001 AND mc.company_id<13515 AND mc.company_type_id=2;

-- train m79995
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.production_year>2007 AND ci.role_id=3 AND mi.info_type_id=16;

-- train m19126
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.production_year=2004 AND mi_idx.info_type_id<101;

-- train m1288
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND mk.keyword_id=4678;

-- train m83143
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.production_year>1973 AND mc.company_type_id>1 AND mi.info_type_id=7;

-- train m77710
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>1 AND t.production_year>2007 AND mc.company_id<13951;

-- train m8652
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.production_year>2008 AND mk.keyword_id=486;

-- train m93366
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.production_year>2002 AND mi_idx.info_type_id>100;

-- train m99208
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND mi.info_type_id>8 AND mk.keyword_id>398;

-- train m31506
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id>1 AND t.production_year=2009 AND mk.keyword_id<355;

-- train m56722
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id=11778 AND mc.company_type_id<2;

-- train m6931
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id<103;

-- train m1674
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id=2879;

-- train m30394
SELECT COUNT(*) FROM title t WHERE t.kind_id>1 AND t.production_year=2000;

-- train m84136
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id>3 AND t.production_year>2008 AND ci.role_id<8;

-- train m54087
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id=7 AND t.production_year>1980 AND mc.company_type_id=2;

-- train m22590
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.production_year<2011 AND mi.info_type_id<2;

-- train m41174
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id>4 AND t.production_year=1976 AND mi_idx.info_type_id<100;

-- train m4875
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND mk.keyword_id>186;

-- train m19145
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id=4 AND t.production_year>1967 AND mc.company_id>656;

-- train m331
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id>3 AND t.production_year=1977;

-- train m88933
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND ci.role_id>4 AND mi_idx.info_type_id=101;

-- train m9144
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.production_year=1946 AND mk.keyword_id=724;

-- train m53361
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND mc.company_id<11244 AND mc.company_type_id=2;

-- train m31439
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND mc.company_id<14075 AND mc.company_type_id=1 AND mi_idx.info_type_id<101;

-- train m62477
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND t.production_year=2009 AND mc.company_id<11538;

-- train m93319
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year<1986 AND mi.info_type_id<7;

-- train m91658
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND t.production_year=2005 AND mi.info_type_id>93;

-- train m82055
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND mk.keyword_id<75850;

-- train m20964
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id>68185 AND mc.company_type_id<2;

-- train m63338
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id>57;

-- train m40296
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id>1517;

-- train m42543
SELECT COUNT(*) FROM title t WHERE t.kind_id<6;

-- train m12482
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.production_year>1973;

-- train m11513
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.production_year>2008 AND mc.company_type_id=2;

-- train m1484
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.production_year=1956 AND mi.info_type_id=3;

-- train m81723
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id=1 AND t.production_year<2001 AND mi_idx.info_type_id=100;

-- train m23876
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND mk.keyword_id=8071;

-- train m62135
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id=7 AND ci.role_id>10;

-- train m7622
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.production_year<2009;

-- train m7416
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year>1969 AND ci.role_id=1;

-- train m57775
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND t.production_year=2012 AND ci.role_id>3 AND mk.keyword_id=382;

-- train m7859
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND mc.company_id>65022 AND mi.info_type_id=3;

-- train m68839
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year>2003 AND mc.company_id>142799 AND mc.company_type_id=2 AND mi_idx.info_type_id>101;

-- train m40981
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND t.production_year>2000 AND mk.keyword_id<2294;

-- train m29606
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.production_year=2001 AND mi_idx.info_type_id=101;

-- train m42514
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND t.production_year=2008;

-- train m78698
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id=4 AND mk.keyword_id>335;

-- train m2125
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id<12744 AND mc.company_type_id=2;

-- train m150
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id>18;

-- train m81219
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<3658;

-- train m17605
SELECT COUNT(*) FROM title t WHERE t.production_year<1989;

-- train m81596
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id>4 AND t.production_year<2010;

-- train m72537
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id>1 AND t.production_year<2009 AND mc.company_id>16014 AND mc.company_type_id<2;

-- train m56505
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id<3 AND t.production_year=1982;

-- train m3144
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.production_year>2000 AND mi_idx.info_type_id=99;

-- train m63864
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id<7 AND t.production_year<2009 AND mk.keyword_id=1714;

-- train m66325
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.production_year>1980 AND mc.company_id>401 AND mc.company_type_id=1;

-- train m17937
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.production_year>2010 AND ci.role_id=2;

-- train m75608
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year<1975 AND ci.role_id>2;

-- train m44658
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND ci.role_id=2 AND mk.keyword_id>4030;

-- train m65512
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id<7 AND t.production_year=1964 AND mc.company_type_id=2 AND mi.info_type_id=13;

-- train m23864
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.production_year=2003 AND mi_idx.info_type_id=99;

-- train m86216
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mc.company_id=42685;

-- train m54190
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year>1915 AND mi_idx.info_type_id>100;

-- train m41848
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id>2 AND mi.info_type_id=16 AND mk.keyword_id<1923;

-- train m96525
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND mk.keyword_id>16391;

-- train m15644
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id<81185;

-- train m19
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id<16;

-- train m42655
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id=10582;

-- train m2351
SELECT COUNT(*) FROM title t WHERE t.kind_id=7 AND t.production_year=1958;

-- train m15456
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id>1 AND t.production_year<2013 AND ci.role_id>8;

-- train m15098
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id=7 AND t.production_year=2010 AND mc.company_id=49;

-- train m96983
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id<7 AND t.production_year<1916 AND mi.info_type_id<13;

-- train m15780
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year=2013;

-- train m97873
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND mk.keyword_id<1042;

-- train m91835
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND mc.company_id>643 AND mc.company_type_id<2;

-- train m73758
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.production_year=2010 AND ci.role_id>2;

-- train m23917
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year>1982 AND mi_idx.info_type_id=99;

-- train m41715
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND mk.keyword_id=76100;

-- train m19887
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND mc.company_id=36759 AND mc.company_type_id<2 AND mi.info_type_id=1;

-- train m18173
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=1 AND t.production_year<1993;

-- train m71186
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mc.company_id<114 AND mc.company_type_id=1 AND mk.keyword_id=1240;

-- train m34616
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND mi.info_type_id>4 AND mi_idx.info_type_id=99;

-- train m61021
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND t.production_year=2001 AND mk.keyword_id<100590;

-- train m91739
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND t.production_year>1958 AND mk.keyword_id=21304;

-- train m9540
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id=68863 AND mc.company_type_id>1;

-- train m57970
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id>87;

-- train m66421
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id>7324;

-- train m23305
SELECT COUNT(*) FROM title t WHERE t.production_year<1983;

-- train m4323
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND ci.role_id=9;

-- train m66003
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.production_year<2011 AND mc.company_id=3900 AND mc.company_type_id=2;

-- train m3077
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id<2 AND t.production_year<2008;

-- train m36248
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id=1 AND t.production_year>2005 AND mi_idx.info_type_id<101;

-- train m99970
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id=7 AND mk.keyword_id=20099;

-- train m54506
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id=1 AND t.production_year=2012 AND mc.company_id<312 AND mc.company_type_id=2 AND ci.role_id<3;

-- train m87908
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id<3 AND t.production_year>1918 AND ci.role_id>1 AND mi.info_type_id<16;

-- train m998
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND ci.role_id=3 AND mi_idx.info_type_id<101;

-- train m14428
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND mk.keyword_id<24653;

-- train m66181
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND mc.company_id>228 AND mc.company_type_id=1;

-- train m64136
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year=1958;

-- train m22369
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id=1 AND t.production_year=1973 AND mc.company_id>113810 AND mc.company_type_id=2;

-- train m27018
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>1 AND t.production_year>1969 AND mi_idx.info_type_id=101;

-- train m37313
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND mi.info_type_id<16 AND mk.keyword_id=107;

-- train m85775
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.production_year>1994;

-- train m99712
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id>13643;

-- train m103
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id<3;

-- train m42867
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id>14542;

-- train m27097
SELECT COUNT(*) FROM title t WHERE t.production_year>1958;

-- train m88056
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id=3 AND t.production_year>2009 AND ci.role_id=1;

-- train m33355
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.production_year=2008 AND mc.company_type_id>1;

-- train m84826
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id>4 AND t.production_year=2013 AND mi.info_type_id>9;

-- train m69350
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id>1 AND t.production_year<2010 AND mi_idx.info_type_id<101;

-- train m27152
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id<2 AND t.production_year=2010 AND mk.keyword_id<4333;

-- train m68516
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND mc.company_id>724;

-- train m82141
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id>3 AND t.production_year=1982 AND ci.role_id<2 AND mi.info_type_id=2;

-- train m96293
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year>1983;

-- train m93282
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND ci.role_id<9 AND mk.keyword_id>817;

-- train m56313
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.production_year<2012 AND mi.info_type_id>8;

-- train m40220
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.production_year>1990 AND mc.company_type_id>1 AND mi_idx.info_type_id>101;

-- train m73106
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mc.company_id=8887 AND mk.keyword_id>2488;

-- train m77350
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>3 AND t.production_year>2009;

-- train m66067
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND mk.keyword_id>20319;

-- train m33128
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND t.production_year<2001;

-- train m98341
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id<34998;

-- train m59329
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id>76;

-- train m61950
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<15771;

-- train m27584
SELECT COUNT(*) FROM title t WHERE t.kind_id<7 AND t.production_year=1994;

-- train m75702
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id<3 AND t.production_year<1957;

-- train m74056
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.production_year>2004 AND mc.company_type_id>1;

-- train m94950
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.production_year<1998 AND mi.info_type_id<18;

-- train m70691
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id>6 AND t.production_year<2005;

-- train m24792
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND mk.keyword_id>10286;

-- train m5088
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND mc.company_type_id<2;

-- train m75755
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id=7 AND ci.role_id<4 AND mi.info_type_id=94;

-- train m39299
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year<1972;

-- train m95713
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id=1 AND ci.role_id>10;

-- train m62480
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id=7 AND mc.company_id>32794 AND mc.company_type_id=2;

-- train m55760
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.production_year=1898;

-- train m43317
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mc.company_id>91 AND mc.company_type_id=2 AND mk.keyword_id>34582;

-- train m24497
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.production_year<2006 AND mi_idx.info_type_id=99;

-- train m53248
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.production_year>1958 AND mk.keyword_id>8263;

-- train m77340
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND mk.keyword_id=5282;

-- train m24394
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id=68;

-- train m8549
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id<11;

-- train m96859
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<52738;

-- train m879
SELECT COUNT(*) FROM title t WHERE t.production_year<2011;

-- train m4665
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id=7 AND t.production_year<1995;

-- train m83917
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id<7 AND mc.company_id>73891 AND mc.company_type_id>1;

-- train m52176
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id=7 AND t.production_year>1954;

-- train m88152
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id>1 AND t.production_year=1999 AND mi_idx.info_type_id<101;

-- train m92990
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.production_year=2005 AND mk.keyword_id>283;

-- train m31419
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id=7 AND mc.company_id=1588;

-- train m74957
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.production_year>1937 AND ci.role_id<10;

-- train m43369
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.production_year<1982;

-- train m51191
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND t.production_year>2002 AND mk.keyword_id=16178;

-- train m39720
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND mc.company_id>63831 AND mi.info_type_id=17;

-- train m84746
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND mc.company_id>11695 AND mc.company_type_id=1 AND mi_idx.info_type_id=100;

-- train m27631
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mc.company_id>78883 AND mc.company_type_id=2;

-- train m51193
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<4 AND mi.info_type_id=8 AND mi_idx.info_type_id<100;

-- train m56688
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id=1 AND t.production_year>1993 AND mi.info_type_id>8;

-- train m95696
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND mi_idx.info_type_id>101 AND mk.keyword_id>65109;

-- train m79363
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id=7876 AND mc.company_type_id=1;

-- train m14983
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id=11;

-- train m45747
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id>8712;

-- train m89654
SELECT COUNT(*) FROM title t WHERE t.kind_id>4 AND t.production_year>1929;

-- train m18558
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id=7 AND t.production_year<2013;

-- train m26228
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id<4 AND t.production_year>0 AND mc.company_id<14069;

-- train m1547
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id=3 AND t.production_year=1999;

-- train m40307
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id=1 AND t.production_year>1995;

-- train m52080
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND mk.keyword_id=3300;

-- train m55198
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND mc.company_id<3662 AND mc.company_type_id<2 AND ci.role_id>8;

-- train m81524
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id<4 AND ci.role_id>1 AND mi.info_type_id=16;

-- train m17136
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.production_year>2012;

-- train m95391
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.production_year<1988;

-- train m87385
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.production_year=1996;

-- train m76472
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year=1954 AND mi_idx.info_type_id=99;

-- train m34756
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id>1 AND t.production_year<2009 AND mc.company_id=160 AND mk.keyword_id>4951;

-- train m21945
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.production_year<2006 AND mi.info_type_id=76;

-- train m12326
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.production_year>2001 AND mi.info_type_id<2;

-- train m51136
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.production_year>2002 AND mi_idx.info_type_id=101 AND mk.keyword_id<18557;

-- train m54244
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id>180 AND mc.company_type_id=1;

-- train m42825
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id>46;

-- train m56179
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<2086;

-- train m38962
SELECT COUNT(*) FROM title t WHERE t.kind_id<7 AND t.production_year<2003;

-- train m79079
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.production_year=2012 AND ci.role_id<2;

-- train m14665
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id=1 AND t.production_year<1996;

-- train m72482
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id=7 AND t.production_year=2012 AND mi.info_type_id>18;

-- train m70438
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.production_year<2003 AND mi_idx.info_type_id=99;

-- train m54554
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id=1 AND mk.keyword_id<1741;

-- train m11218
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id=4 AND t.production_year>2003 AND mc.company_id>68;

-- train m70588
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id<7 AND t.production_year<1972;

-- train m35044
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year>2002 AND mi_idx.info_type_id>101;

-- train m70983
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND ci.role_id<4 AND mk.keyword_id=3764;

-- train m57246
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND mc.company_id>2561 AND mc.company_type_id=1 AND mi.info_type_id<16;

-- train m94662
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year<2005 AND mc.company_id>46287 AND mc.company_type_id>1 AND mi_idx.info_type_id=99;

-- train m86552
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mk.keyword_id=490;

-- train m39965
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.production_year=1953 AND mi.info_type_id>9;

-- train m75931
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.production_year>2012 AND mi.info_type_id<16 AND mk.keyword_id<2042;

-- train m58664
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND mk.keyword_id<64431;

-- train m29317
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id=25542 AND mc.company_type_id<2;

-- train m3
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id<4;

-- train m57564
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id=3401;

-- train m79835
SELECT COUNT(*) FROM title t WHERE t.kind_id=2 AND t.production_year=1965;

-- train m77728
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id>1 AND t.production_year<1991;

-- train m11394
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id=2 AND t.production_year<2012;

-- train m92866
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id=7 AND t.production_year=2008 AND mi.info_type_id=4;

-- train m87638
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id=1 AND t.production_year<1992 AND mi_idx.info_type_id>99;

-- train m81942
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id=1 AND mk.keyword_id<196;

-- train m56664
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id<7 AND t.production_year>1981;

-- train m21085
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id=4;

-- train m60857
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year<2008 AND mi_idx.info_type_id=99;

-- train m34829
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND mk.keyword_id=3244;

-- train m42203
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.production_year=1995;

-- train m44640
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year>2007 AND mc.company_id<9551 AND mc.company_type_id=1 AND mi_idx.info_type_id=100;

-- train m58775
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mc.company_id>202135;

-- train m66969
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.production_year>2012 AND mi.info_type_id=16;

-- train m48682
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.production_year<2011 AND mi.info_type_id<17 AND mk.keyword_id>1749;

-- train m89931
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND t.production_year<2013 AND mi_idx.info_type_id<101;

-- train m54265
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id=73762;

-- train m98297
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id<46;

-- train m36493
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<7984;

-- train m624
SELECT COUNT(*) FROM title t WHERE t.production_year<1984;

-- train m26364
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.production_year=1972 AND ci.role_id=1;

-- train m89320
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id=7 AND mc.company_id=685 AND mc.company_type_id=1;

-- train m88177
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.production_year=1915 AND mi.info_type_id>8;

-- train m54606
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.production_year>1919 AND mi_idx.info_type_id<101;

-- train m55002
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.production_year=1970 AND mk.keyword_id<3908;

-- train m91452
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id<7 AND t.production_year>2009 AND mc.company_type_id<2;

-- train m14079
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id>4 AND ci.role_id<2;

-- train m42966
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.production_year=1977;

-- train m39672
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id>4 AND t.production_year>1953;

-- train m75200
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND mc.company_id<11391 AND mc.company_type_id>1;

-- train m26202
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year=2012;

-- train m3133
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id=3 AND mc.company_id<8416 AND mc.company_type_id=2 AND mk.keyword_id=13406;

-- train m50834
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.production_year=1978;

-- train m77294
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.production_year=1968 AND mi.info_type_id=15 AND mk.keyword_id>230;

-- train m20698
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.production_year<2005 AND mi_idx.info_type_id>101;

-- train m57715
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id<1758;

-- train m30216
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id=63;

-- train m3639
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id>114657;

-- train m55865
SELECT COUNT(*) FROM title t WHERE t.kind_id<6 AND t.production_year=2004;

-- train m98468
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id=7 AND t.production_year>1936;

-- train m83691
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.production_year>2007 AND mc.company_id>12871;

-- train m19359
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.production_year=1969;

-- train m10497
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.production_year=1992 AND mi_idx.info_type_id>101;

-- train m76338
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id=7 AND t.production_year<2002 AND mk.keyword_id>382;

-- train m7081
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND mc.company_id>121222 AND ci.role_id>3;

-- train m47274
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND ci.role_id>1 AND mi.info_type_id>18;

-- train m37976
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.production_year<2009 AND mi_idx.info_type_id=101;

-- train m33173
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND t.production_year>1935 AND ci.role_id<2 AND mk.keyword_id>5706;

-- train m99971
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id<3 AND t.production_year>2001;

-- train m38254
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.production_year=2005 AND mc.company_type_id=1;

-- train m3339
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mc.company_id<596 AND mc.company_type_id<2;

-- train m15172
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=1 AND mi.info_type_id>18;

-- train m53188
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND t.production_year<2003 AND mk.keyword_id=18401;

-- train m3452
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND mi_idx.info_type_id<100 AND mk.keyword_id=15206;

-- train m80797
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id<331 AND mc.company_type_id>1;

-- train m41963
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id=92;

-- train m56700
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<26737;

-- train m45614
SELECT COUNT(*) FROM title t WHERE t.kind_id>1 AND t.production_year<1990;

-- train m61998
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id>2 AND t.production_year=1978;

-- train m61949
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id=1 AND t.production_year=2004;

-- train m7681
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id=7 AND t.production_year>1984;

-- train m28996
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.production_year=1989;

-- train m9488
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id<7 AND mk.keyword_id<1578;

-- train m77451
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id=7 AND t.production_year>2009;

-- train m18039
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id<7 AND t.production_year<1994 AND mi.info_type_id=7;

-- train m14105
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year=2010 AND mi_idx.info_type_id<100;

-- train m59837
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND mk.keyword_id<34852;

-- train m41391
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id=7 AND t.production_year>2009 AND mc.company_id=6 AND mc.company_type_id=1 AND mi.info_type_id=5;

-- train m13079
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.production_year=2010 AND mc.company_id<133486 AND mc.company_type_id=2;

-- train m86284
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND mc.company_id=40775;

-- train m3406
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND mi.info_type_id=16 AND mi_idx.info_type_id<101;

-- train m94548
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.production_year=2007 AND mk.keyword_id<5810;

-- train m56348
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND mk.keyword_id<7683;

-- train m34812
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id>7302 AND mc.company_type_id>1;

-- train m493
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id=2;

-- train m54745
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<1748;

-- train m10701
SELECT COUNT(*) FROM title t WHERE t.kind_id=7 AND t.production_year<1989;

-- train m52729
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.production_year>2002 AND ci.role_id>6;

-- train m3568
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id<7 AND t.production_year=2010;

-- train m2514
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.production_year>1985;

-- train m91254
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year=1968 AND mi_idx.info_type_id<101;

-- train m58
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id=7 AND t.production_year>2011;

-- train m43877
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.production_year>2011 AND mc.company_id>3755;

-- train m62011
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id<7 AND mi.info_type_id<4;

-- train m98281
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=3 AND ci.role_id=2;

-- train m93376
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.production_year=2006 AND ci.role_id<5;

-- train m71953
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.production_year=2012 AND mi.info_type_id<16;

-- train m18312
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.production_year=1999 AND mc.company_type_id=1;

-- train m86762
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id>1 AND mk.keyword_id>13541;

-- train m22415
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=1 AND t.production_year<2007 AND mi.info_type_id=7 AND mi_idx.info_type_id=100;

-- train m9817
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND mi.info_type_id<15 AND mk.keyword_id=14713;

-- train m45269
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND mk.keyword_id>580;

-- train m18665
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id=157729 AND mc.company_type_id>1;

-- train m32871
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id>81;

-- train m43273
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<545;

-- train m72249
SELECT COUNT(*) FROM title t WHERE t.kind_id=3 AND t.production_year=1967;

-- train m73940
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id<3 AND ci.role_id<3;

-- train m8881
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id=7 AND t.production_year>2011 AND mc.company_id=6;

-- train m61544
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id<7 AND t.production_year=2004 AND mi.info_type_id=8;

-- train m53860
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.production_year<1977 AND mi_idx.info_type_id<101;

-- train m27480
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND mk.keyword_id<2487;

-- train m50559
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id<3 AND t.production_year>1995 AND ci.role_id>8;

-- train m98101
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id=7 AND t.production_year>1956;

-- train m69275
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>3 AND t.production_year>1957 AND ci.role_id=2;

-- train m92988
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND t.production_year=1920;

-- train m69894
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND mc.company_id=466 AND mc.company_type_id<2 AND mi.info_type_id>3;

-- train m3008
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=3 AND t.production_year=2005;

-- train m70805
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mc.company_id>13146;

-- train m52081
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.production_year>2009 AND mi.info_type_id>2 AND mi_idx.info_type_id=100;

-- train m69336
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND t.production_year=1973 AND mi.info_type_id>7 AND mk.keyword_id=99;

-- train m53453
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.production_year<2008 AND mk.keyword_id>21188;

-- train m28271
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id>329 AND mc.company_type_id<2;

-- train m11782
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id>96;

-- train m41127
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id>75418;

-- train m69040
SELECT COUNT(*) FROM title t WHERE t.kind_id>1 AND t.production_year>1950;

-- train m5235
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id=7 AND t.production_year<1991;

-- train m37108
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND mc.company_id=11371 AND mc.company_type_id<2;

-- train m28669
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id<7 AND t.production_year<2014;

-- train m5542
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.production_year<1970;

-- train m54052
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND mk.keyword_id=4228;

-- train m89117
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.production_year=2011 AND ci.role_id>1;

-- train m42840
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.production_year<1999 AND ci.role_id=2;

-- train m73818
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year>1987 AND ci.role_id<10 AND mi_idx.info_type_id=101;

-- train m15986
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.production_year<2011 AND mk.keyword_id<1382;

-- train m92774
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id=1 AND mc.company_type_id=1 AND mi.info_type_id>6;

-- train m67538
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND mc.company_id=7457;

-- train m99437
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.production_year<2009 AND mk.keyword_id=872;

-- train m6098
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND mi_idx.info_type_id=100;

-- train m88288
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND t.production_year>1946 AND mi.info_type_id>3;

-- train m61899
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.production_year=1972 AND mi_idx.info_type_id>100;

-- train m55576
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id=89263;

-- train m39262
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id<71;

-- train m84941
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id=3659;

-- train m2528
SELECT COUNT(*) FROM title t WHERE t.production_year<1986;

-- train m94212
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.production_year<1977 AND ci.role_id=3;

-- train m92231
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND mc.company_id=40741 AND mc.company_type_id<2;

-- train m53205
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id=7 AND t.production_year=2003 AND mi.info_type_id=103;

-- train m7773
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year>2006 AND mi_idx.info_type_id=101;

-- train m68897
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id>2 AND t.production_year>2013;

-- train m33289
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id>4 AND t.production_year>2008;

-- train m50041
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id<7 AND t.production_year=2006;

-- train m14977
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<6 AND t.production_year=1998 AND mi_idx.info_type_id=101;

-- train m12929
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id>1 AND t.production_year>2012;

-- train m8475
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id=1 AND t.production_year=2004 AND mc.company_id<82600 AND mi.info_type_id<4;

-- train m17797
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year>2005 AND mc.company_id<27;

-- train m76176
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mc.company_id>12688 AND mc.company_type_id<2;

-- train m38371
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year=1974 AND mi.info_type_id>8;

-- train m89414
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND t.production_year>2010 AND mi.info_type_id=7 AND mk.keyword_id=807;

-- train m39992
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND mk.keyword_id<3639;

-- train m79651
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id=11538 AND mc.company_type_id<2;

-- train m2007
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id>17;

-- train m23352
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<12931;

-- train m24522
SELECT COUNT(*) FROM title t WHERE t.kind_id=7 AND t.production_year=1971;

-- train m24125
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id<7 AND t.production_year>2014;

-- train m3651
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id<7 AND mc.company_id>160;

-- train m66136
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id=7 AND t.production_year<1994 AND mi.info_type_id<16;

-- train m65078
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year<2003 AND mi_idx.info_type_id=101;

-- train m87747
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND mk.keyword_id>918;

-- train m35653
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.production_year<1973 AND mc.company_id=164 AND mc.company_type_id<2;

-- train m42144
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id<7 AND ci.role_id>1;

-- train m86503
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>1 AND t.production_year=2000 AND mi_idx.info_type_id>100;

-- train m82103
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND mk.keyword_id=6312;

-- train m97968
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id<7 AND t.production_year<1972 AND mc.company_type_id=2;

-- train m52331
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year>1914 AND mc.company_id<5503 AND mc.company_type_id=1;

-- train m56543
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mc.company_id>11313;

-- train m37964
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=1 AND mi.info_type_id>2;

-- train m7343
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND mi.info_type_id>3 AND mk.keyword_id>12338;

-- train m70009
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND mi_idx.info_type_id>100 AND mk.keyword_id>889;

-- train m57794
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id<3909 AND mc.company_type_id<2;

-- train m13513
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id=104;

-- train m93297
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id>555;

-- train m2030
SELECT COUNT(*) FROM title t WHERE t.kind_id<7 AND t.production_year=1963;

-- train m19894
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id>3 AND t.production_year=2007;

-- train m13776
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND mc.company_id=15240 AND mc.company_type_id=1;

-- train m23796
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id=7 AND t.production_year<1968 AND mi.info_type_id=8;

-- train m22117
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.production_year>1992 AND mi_idx.info_type_id=99;

-- train m54048
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND mk.keyword_id<58445;

-- train m87992
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.production_year>2012 AND mc.company_id>45523 AND mc.company_type_id>1;

-- train m71387
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.production_year>1999 AND mi.info_type_id=35;

-- train m41536
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.production_year<1983;

-- train m61124
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND t.production_year<2006 AND mk.keyword_id>2488;

-- train m50263
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id>1 AND t.production_year<2008;

-- train m57796
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>1 AND t.production_year=1968 AND mc.company_id<846 AND mc.company_type_id>1;

-- train m15754
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mc.company_id=1711;

-- train m6852
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND mi.info_type_id=16 AND mi_idx.info_type_id<100;

-- train m29692
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id>3 AND mi.info_type_id=8;

-- train m80214
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND mi_idx.info_type_id=100 AND mk.keyword_id<16264;

-- train m68906
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id>96660;

-- train m2762
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id=108;

-- train m64242
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<24621;

-- train m20534
SELECT COUNT(*) FROM title t WHERE t.kind_id=1 AND t.production_year>1992;

-- train m91390
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.production_year=1998 AND ci.role_id>1;

-- train m99962
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id=7 AND mc.company_id<71923;

-- train m37936
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.production_year<2008 AND mi.info_type_id=1;

-- train m32444
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id=1 AND t.production_year>2010 AND mi_idx.info_type_id=101;

-- train m73330
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id<7 AND t.production_year<2007 AND mk.keyword_id>6366;

-- train m91751
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND mc.company_id>12662;

-- train m46789
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id>1 AND t.production_year<2005;

-- train m27581
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND ci.role_id<10 AND mi_idx.info_type_id<101;

-- train m89977
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND mk.keyword_id>3995;

-- train m54898
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.production_year<1999 AND mc.company_type_id=2;

-- train m28554
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year<1986 AND mc.company_type_id=2;

-- train m49771
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mk.keyword_id=6224;

-- train m3578
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>1 AND t.production_year=1995 AND mi.info_type_id>7 AND mi_idx.info_type_id>99;

-- train m37178
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.production_year=1995 AND mk.keyword_id<3266;

-- train m19397
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND mi_idx.info_type_id>99;

-- train m60325
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id>21770;

-- train m5093
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id>11;

-- train m94264
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id=13798;

-- train m91185
SELECT COUNT(*) FROM title t WHERE t.kind_id=4 AND t.production_year>1989;

-- train m75579
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id>3 AND ci.role_id>2;

-- train m91670
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND mc.company_id=29990 AND mc.company_type_id<2;

-- train m16678
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.production_year>1974 AND mi.info_type_id=16;

-- train m67340
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id=4 AND t.production_year<1998 AND mi_idx.info_type_id<100;

-- train m89581
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id=7 AND mk.keyword_id>2172;

-- train m96696
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id<7 AND t.production_year<2012 AND mc.company_id>80151;

-- train m31401
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.production_year>2010 AND ci.role_id=1 AND mi.info_type_id<2;

-- train m36521
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND ci.role_id=1 AND mi_idx.info_type_id>100;

-- train m33560
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.production_year=2011 AND ci.role_id=4;

-- train m21724
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND mc.company_type_id=1 AND mi.info_type_id>4;

-- train m24585
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.production_year>1940 AND mc.company_id<2065 AND mi_idx.info_type_id=100;

-- train m34282
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.production_year<1997 AND mc.company_id>116508 AND mc.company_type_id=2 AND mk.keyword_id<22333;

-- train m12032
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=1 AND mi.info_type_id<7 AND mi_idx.info_type_id<100;

-- train m84850
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id>3 AND mi.info_type_id=7 AND mk.keyword_id<579;

-- train m93121
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.production_year<1965 AND mi_idx.info_type_id<101 AND mk.keyword_id>917;

-- train m24193
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id<1052;

-- train m23782
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id=70;

-- train m82296
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id>1531;

-- train m96331
SELECT COUNT(*) FROM title t WHERE t.kind_id<7 AND t.production_year>1913;

-- train m4297
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id=3 AND t.production_year<2012;

-- train m76351
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id=1 AND mc.company_id=20445 AND mc.company_type_id=1;

-- train m96866
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id=1 AND t.production_year=1930;

-- train m15046
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id>4 AND t.production_year=2013 AND mi_idx.info_type_id<100;

-- train m7862
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id<7 AND t.production_year<1964 AND mk.keyword_id>24882;

-- train m11135
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id<7 AND t.production_year>2012;

-- train m17451
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id=7 AND t.production_year=2012 AND ci.role_id<10 AND mi.info_type_id>8;

-- train m12176
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=1 AND ci.role_id<10;

-- train m88112
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND t.production_year<2008 AND mk.keyword_id<2055;

-- train m87598
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND mc.company_id=11203 AND mi.info_type_id>46;

-- train m18954
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>1 AND t.production_year<2006 AND mc.company_id=1451 AND mi_idx.info_type_id=99;

-- train m75026
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mc.company_id>128 AND mk.keyword_id>95;

-- train m21592
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.production_year=1996 AND mi_idx.info_type_id=101;

-- train m80075
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id<4 AND mk.keyword_id<9960;

-- train m78037
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id<6 AND mk.keyword_id<1091;

-- train m11928
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id=174917 AND mc.company_type_id=2;

-- train m9672
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id<108;

-- train m62175
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<67291;

-- train m73988
SELECT COUNT(*) FROM title t WHERE t.kind_id>2 AND t.production_year>2013;

-- train m28392
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.production_year=2008 AND ci.role_id=1;

-- train m65452
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND mc.company_id>11961 AND mc.company_type_id<2;

-- train m27120
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.production_year=2003 AND mi.info_type_id>7;

-- train m87716
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.production_year<1941 AND mi_idx.info_type_id<100;

-- train m10959
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id<7 AND t.production_year>1978 AND mk.keyword_id<13597;

-- train m7047
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id=7 AND t.production_year=2007 AND mc.company_type_id=1;

-- train m75376
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id=1 AND ci.role_id=10 AND mi.info_type_id<8;

-- train m41403
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>1 AND t.production_year>2007 AND mi_idx.info_type_id>99;

-- train m85334
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND t.production_year<2011 AND ci.role_id<11;

-- train m63920
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.production_year>1977 AND mi.info_type_id<16;

-- train m43853
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND mc.company_id>1684 AND mc.company_type_id=2 AND mi_idx.info_type_id=100;

-- train m59224
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mc.company_id>13452;

-- train m59924
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.production_year=1911;

-- train m59236
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND t.production_year<2000 AND mi.info_type_id=7;

-- train m67829
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND t.production_year=1993 AND mk.keyword_id=1548;

-- train m48618
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id<1824 AND mc.company_type_id>1;

-- train m62805
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id<84;

-- train m57419
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id>7778;

-- train m86466
SELECT COUNT(*) FROM title t WHERE t.kind_id=7 AND t.production_year<1974;

-- train m74760
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.production_year=1965 AND ci.role_id>2;

-- train m16527
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id=7 AND t.production_year<2007 AND mc.company_id<222758;

-- train m24702
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id<7 AND t.production_year>2004;

-- train m9483
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id>3 AND t.production_year=2012 AND mi_idx.info_type_id>100;

-- train m41044
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id=7 AND t.production_year>1971;

-- train m6625
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND mc.company_id<108635 AND ci.role_id=4;

-- train m29838
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.production_year=2004 AND mi.info_type_id=7;

-- train m89274
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year>2012 AND mi_idx.info_type_id>99;

-- train m89425
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND mk.keyword_id=1143;

-- train m15147
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id<7 AND t.production_year<1981 AND mc.company_type_id>1 AND mi.info_type_id>7;

-- train m57494
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND mc.company_id>265 AND mc.company_type_id=1;

-- train m64163
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id>1 AND mk.keyword_id>8982;

-- train m39705
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year=2010 AND mi.info_type_id=8;

-- train m59703
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND mi.info_type_id=16 AND mk.keyword_id<91576;

-- train m18035
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND mk.keyword_id<2042;

-- train m15978
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id<1435 AND mc.company_type_id<2;

-- train m428
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id<2;

-- train m62887
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<45184;

-- train m21906
SELECT COUNT(*) FROM title t WHERE t.kind_id=4 AND t.production_year=2013;

-- train m18731
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id=3 AND t.production_year<1999;

-- train m83590
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id<4 AND t.production_year>2010 AND mc.company_type_id=1;

-- train m5062
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND mi.info_type_id<6;

-- train m55998
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.production_year>1973 AND mi_idx.info_type_id=101;

-- train m37001
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.production_year=2012 AND mk.keyword_id>107;

-- train m51685
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.production_year>1994;

-- train m9288
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.production_year=2012;

-- train m9755
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.production_year=2013;

-- train m67467
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND mk.keyword_id<3254;

-- train m70290
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND mc.company_id=16322 AND mi.info_type_id>61;

-- train m18265
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND mc.company_id=4589 AND mc.company_type_id<2 AND mi_idx.info_type_id=99;

-- train m27395
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id=1 AND mc.company_id>14307 AND mc.company_type_id<2;

-- train m29338
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND mi.info_type_id=18 AND mi_idx.info_type_id<100;

-- train m30181
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND t.production_year<2009 AND mk.keyword_id=15565;

-- train m47867
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND t.production_year=1966;

-- train m8829
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id<71404;

-- train m38019
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id=41;

-- train m21985
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<1183;

-- train m15186
SELECT COUNT(*) FROM title t WHERE t.kind_id<7 AND t.production_year<1988;

-- train m98558
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id=7 AND t.production_year<2004 AND ci.role_id=10;

-- train m21984
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id>3 AND mc.company_id<19082;

-- train m3851
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id<7 AND t.production_year<2001 AND mi.info_type_id>16;

-- train m19891
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id<2 AND t.production_year<2010 AND mi_idx.info_type_id<101;

-- train m24661
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND mk.keyword_id<1935;

-- train m65851
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id>1 AND t.production_year<1976 AND mc.company_id>13486 AND mc.company_type_id>1;

-- train m60177
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id<7 AND t.production_year<1977 AND ci.role_id=1 AND mi.info_type_id>16;

-- train m12863
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=1 AND mi_idx.info_type_id>101;

-- train m53260
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND mk.keyword_id<1603;

-- train m64178
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.production_year>1957 AND mc.company_id>2870 AND mc.company_type_id<2;

-- train m36387
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.production_year<2007;

-- train m83588
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND mc.company_id<117117 AND mc.company_type_id=2 AND mk.keyword_id=11277;

-- train m51834
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>1 AND t.production_year<2012 AND mi.info_type_id=16 AND mi_idx.info_type_id>99;

-- train m88460
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.production_year=1995 AND mk.keyword_id>7602;

-- train m61450
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND t.production_year=1978;

-- train m99623
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id>18028 AND mc.company_type_id<2;

-- train m172
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id=105;

-- train m11923
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id>397;

-- train m20490
SELECT COUNT(*) FROM title t WHERE t.kind_id>1 AND t.production_year<2013;

-- train m62547
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id=1 AND t.production_year=2006 AND ci.role_id>10;

-- train m29545
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id<7 AND t.production_year>1998 AND mc.company_type_id>1;

-- train m96954
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.production_year>2012 AND mi.info_type_id=15;

-- train m29430
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id=1 AND t.production_year>2011 AND mi_idx.info_type_id>100;

-- train m19504
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND mk.keyword_id>121331;

-- train m30985
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id<7 AND t.production_year=2003 AND ci.role_id=2;

-- train m54094
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id=7 AND t.production_year>2012 AND mi.info_type_id=16;

-- train m20379
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year=2013 AND ci.role_id=2 AND mi_idx.info_type_id<100;

-- train m17238
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND mk.keyword_id=6509;

-- train m26829
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id=7 AND t.production_year=2012 AND mc.company_id<11165;

-- train m76372
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>4 AND t.production_year>2009 AND mc.company_id>15193 AND mc.company_type_id=1;

-- train m68327
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mc.company_id>114956 AND mk.keyword_id<12109;

-- train m73197
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.production_year>1976 AND mi.info_type_id>7;

-- train m61929
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND mk.keyword_id=20311;

-- train m56952
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND mk.keyword_id=1732;

-- train m31970
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id>97744 AND mc.company_type_id=2;

-- train m563
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id=107;

-- train m5079
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<6427;

-- train m17561
SELECT COUNT(*) FROM title t WHERE t.kind_id=1 AND t.production_year=1996;

-- train m97605
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id<7 AND t.production_year<1980 AND ci.role_id>2;

-- train m3998
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id=1 AND t.production_year>2010 AND mc.company_id>8292;

-- train m41908
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.production_year<2002 AND mi.info_type_id>18;

-- train m65857
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year<1985;

-- train m86522
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id<7 AND t.production_year=2013 AND mk.keyword_id>2990;

-- train m78315
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.production_year<2006 AND ci.role_id<2;

-- train m47064
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id>1 AND t.production_year<2010 AND mi.info_type_id<7;

-- train m15222
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=1 AND t.production_year=2010 AND ci.role_id=1 AND mi_idx.info_type_id>100;

-- train m97917
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id>1 AND t.production_year>1997 AND mk.keyword_id<8797;

-- train m26586
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id>1 AND t.production_year>1915 AND mc.company_id=47098 AND mc.company_type_id=1;

-- train m52889
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND mc.company_id=312;

-- train m24843
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mk.keyword_id=29033;

-- train m6043
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND mi.info_type_id>8;

-- train m43122
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id=2 AND t.production_year<2000 AND mk.keyword_id<3430;

-- train m3638
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id=1 AND t.production_year>1964;

-- train m26764
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id<132871;

-- train m25075
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id>91;

-- train m3303
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id>12015;

-- train m86119
SELECT COUNT(*) FROM title t WHERE t.kind_id=2 AND t.production_year<1998;

-- train m5215
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id<7 AND t.production_year>1992;

-- train m87982
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id<7 AND t.production_year=1996 AND mc.company_type_id=2;

-- train m19037
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id=7 AND t.production_year=1944;

-- train m4629
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id=1 AND t.production_year<2011 AND mi_idx.info_type_id>99;

-- train m28630
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id=1 AND t.production_year>1998 AND mk.keyword_id<56;

-- train m33444
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id=7 AND t.production_year>1989 AND mc.company_type_id=1;

-- train m32518
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id<3 AND t.production_year>2012 AND mi.info_type_id>2;

-- train m47367
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year>1978;

-- train m30844
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.production_year=2006 AND mk.keyword_id<643;

-- train m50061
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.production_year>1998 AND mc.company_id<13549 AND mc.company_type_id=1 AND mi.info_type_id=16;

-- train m99663
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year>2006 AND mc.company_id<6 AND mc.company_type_id=2;

-- train m65777
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.production_year<2002 AND mc.company_id>439 AND mc.company_type_id<2 AND mk.keyword_id<1740;

-- train m53264
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.production_year=2009;

-- train m14224
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND mi.info_type_id=16 AND mk.keyword_id<10175;

-- train m18055
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.production_year=1953 AND mk.keyword_id=7877;

-- train m57012
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id>44168 AND mc.company_type_id=1;

-- train m5355
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id>97;

-- train m45903
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id>10904;

-- train m11330
SELECT COUNT(*) FROM title t WHERE t.kind_id=1 AND t.production_year<1961;

-- train m52185
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id<2 AND t.production_year=2003 AND ci.role_id>2;

-- train m68051
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND mc.company_id<141906;

-- train m86398
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.production_year=2012 AND mi.info_type_id<2;

-- train m77491
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year>0 AND mi_idx.info_type_id=100;

-- train m92401
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id<7 AND t.production_year=1979;

-- train m52860
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND mc.company_id>16288 AND mc.company_type_id=1 AND ci.role_id>1;

-- train m46344
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND ci.role_id>3 AND mi.info_type_id>17;

-- train m61577
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=2 AND t.production_year=1954;

-- train m94111
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND t.production_year<1995 AND mk.keyword_id>22896;

-- train m58295
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id<7 AND mc.company_id=198011;

-- train m91096
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND mc.company_id<16798 AND mc.company_type_id=2;

-- train m87985
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND t.production_year<2009 AND mc.company_id>159108 AND mk.keyword_id<496;

-- train m97413
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=1 AND t.production_year>2005 AND mi.info_type_id<17;

-- train m74991
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id<2 AND t.production_year=1995 AND mi.info_type_id>5;

-- train m47217
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND mi_idx.info_type_id<101 AND mk.keyword_id>7592;

-- train m564
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id=13165;

-- train m18137
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id>42;

-- train m4137
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id=217;

-- train m4107
SELECT COUNT(*) FROM title t WHERE t.production_year>1994;

-- train m60290
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id=7 AND ci.role_id>10;

-- train m14310
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id<7 AND t.production_year>2008;

-- train m48765
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.production_year=1960 AND mi.info_type_id>107;

-- train m9930
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.production_year<2012 AND mi_idx.info_type_id<101;

-- train m93270
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id<7 AND t.production_year=2006 AND mk.keyword_id<382;

-- train m60062
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.production_year>1989 AND mc.company_id=7575 AND ci.role_id>1;

-- train m87924
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id=1 AND t.production_year>2012 AND ci.role_id=8;

-- train m90375
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>1 AND t.production_year>1994;

-- train m2194
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND mk.keyword_id<11429;

-- train m19635
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.production_year<2012 AND mc.company_id=110311 AND mc.company_type_id=2 AND mi.info_type_id>1;

-- train m76502
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND mc.company_id<21033 AND mc.company_type_id=2 AND mi_idx.info_type_id<101;

-- train m97371
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id=4 AND mk.keyword_id<3658;

-- train m25978
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND mi.info_type_id=16;

-- train m94686
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id=2 AND mi.info_type_id=8;

-- train m80072
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id>4 AND t.production_year>1963;

-- train m51845
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id<222427;

-- train m14830
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id>45;

-- train m97187
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<2092;

-- train m53746
SELECT COUNT(*) FROM title t WHERE t.kind_id<4 AND t.production_year<1925;

-- train m27396
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id<3 AND t.production_year<2005 AND ci.role_id=1;

-- train m56073
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND mc.company_id<12529 AND mc.company_type_id=1;

-- train m18508
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id>4 AND t.production_year>1913 AND mi.info_type_id<3;

-- train m17390
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id>1 AND t.production_year=2008;

-- train m94471
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id<7 AND mk.keyword_id=3130;

-- train m95222
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id>1 AND t.production_year=2012 AND mc.company_id=686;

-- train m14032
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id<7 AND t.production_year<2008;

-- train m64890
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<4 AND t.production_year>2012 AND ci.role_id>10 AND mi_idx.info_type_id=99;

-- train m61881
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.production_year<1981 AND mk.keyword_id<8193;

-- train m21311
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id<4 AND t.production_year>2003 AND mc.company_id=137 AND mc.company_type_id=1 AND mi.info_type_id>16;

-- train m22096
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND mc.company_id>62824 AND mc.company_type_id=2;

-- train m99493
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND t.production_year=1964 AND mc.company_id>76143 AND mc.company_type_id>1;

-- train m17311
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.production_year<1993 AND mi.info_type_id=3 AND mi_idx.info_type_id=101;

-- train m44208
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND t.production_year<1966 AND mi.info_type_id=105;

-- train m43689
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.production_year>2007 AND mk.keyword_id<11285;

-- train m55156
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id=851 AND mc.company_type_id=1;

-- train m2873
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id<104;

-- train m52315
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id=85758;

-- train m32054
SELECT COUNT(*) FROM title t WHERE t.kind_id<7 AND t.production_year=1974;

-- train m44244
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.production_year>2004 AND ci.role_id=1;

-- train m82016
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id<7 AND t.production_year>1931;

-- train m56763
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.production_year>2012 AND mi.info_type_id<3;

-- train m81464
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.production_year<1952 AND mi_idx.info_type_id>101;

-- train m68174
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND mk.keyword_id>5591;

-- train m70512
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.production_year<1929 AND ci.role_id>1;

-- train m54768
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id=7 AND t.production_year>0 AND ci.role_id=6;

-- train m41814
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.production_year<1996;

-- train m26835
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.production_year<2002 AND ci.role_id>1;

-- train m42164
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.production_year>2008 AND mi.info_type_id>7;

-- train m71706
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<4 AND t.production_year>2001 AND mc.company_id<11694;

-- train m60286
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mc.company_id>11 AND mc.company_type_id=2;

-- train m41021
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.production_year<2013 AND mi.info_type_id<107;

-- train m35036
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.production_year>2010 AND mi.info_type_id<8;

-- train m89536
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND mi_idx.info_type_id=100 AND mk.keyword_id>92271;

-- train m66248
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id<21427;

-- train m12869
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id=77;

-- train m31484
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<5952;

-- train m32447
SELECT COUNT(*) FROM title t WHERE t.kind_id=7 AND t.production_year>1980;

-- train m67095
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id=7 AND t.production_year>1967 AND ci.role_id>3;

-- train m89097
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id<2 AND t.production_year>2010;

-- train m88397
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.production_year>2013 AND mi.info_type_id=4;

-- train m47746
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year<1995 AND mi_idx.info_type_id=99;

-- train m29817
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.production_year>0 AND mk.keyword_id=50942;

-- train m97149
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND mc.company_id=34 AND ci.role_id=1;

-- train m80860
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id<7 AND t.production_year=2011 AND ci.role_id>9;

-- train m76375
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>1 AND t.production_year<2005 AND ci.role_id=2;

-- train m8231
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND t.production_year=2012 AND mk.keyword_id>12109;

-- train m98760
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND mc.company_id>55195 AND mc.company_type_id=2;

-- train m85597
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year>1977 AND mc.company_id<10584 AND mi_idx.info_type_id=99;

-- train m59148
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mk.keyword_id>498;

-- train m38101
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.production_year<2010 AND mi.info_type_id<2 AND mi_idx.info_type_id=100;

-- train m15666
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.production_year=1927;

-- train m87956
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND t.production_year>1921 AND mk.keyword_id=786;

-- train m90597
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id>11705;

-- train m11610
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id>90;

-- train m77564
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<5972;

-- train m26937
SELECT COUNT(*) FROM title t WHERE t.kind_id=7 AND t.production_year=1978;

-- train m84207
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id>2 AND ci.role_id>3;

-- train m80067
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id=1 AND t.production_year=1991 AND mc.company_id>65563 AND mc.company_type_id<2;

-- train m24642
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.production_year<1982 AND mi.info_type_id>8;

-- train m93332
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year=2001 AND mi_idx.info_type_id<100;

-- train m74865
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id>1 AND t.production_year=1983 AND mk.keyword_id<4297;

-- train m39037
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.production_year<1973 AND mc.company_id<11 AND ci.role_id<5;

-- train m48706
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id<7 AND t.production_year<2006 AND mi.info_type_id=2;

-- train m89873
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>1 AND t.production_year>1914 AND ci.role_id<6 AND mi_idx.info_type_id=99;

-- train m32043
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id=2 AND t.production_year=2011 AND mk.keyword_id>7821;

-- train m19925
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND mc.company_id<65035;

-- train m25804
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.production_year=2008 AND mc.company_id<895 AND mc.company_type_id>1;

-- train m4823
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND mk.keyword_id=89689;

-- train m23391
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>1 AND t.production_year<1956 AND mi.info_type_id<2;

-- train m238
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id<4 AND t.production_year>2008 AND mk.keyword_id=3509;

-- train m32691
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND mk.keyword_id=47;

-- train m45186
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id<329 AND mc.company_type_id>1;

-- train m12400
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id<102;

-- train m34011
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<3549;

-- train m9608
SELECT COUNT(*) FROM title t WHERE t.kind_id<3 AND t.production_year>1972;

-- train m27807
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id<2 AND t.production_year>2008;

-- train m41685
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id<7 AND mc.company_id<223780;

-- train m41662
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id=4 AND t.production_year=1995;

-- train m78353
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year<2001 AND mi_idx.info_type_id>100;

-- train m59962
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id=7 AND mk.keyword_id>2024;

-- train m24841
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.production_year=1998 AND mc.company_id>74 AND ci.role_id=1;

-- train m37185
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id<4 AND ci.role_id>4 AND mi.info_type_id<8;

-- train m43869
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<3 AND t.production_year=2003 AND ci.role_id>10 AND mi_idx.info_type_id>99;

-- train m86693
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND ci.role_id=3 AND mk.keyword_id=2562;

-- train m89835
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND mc.company_id<3075;

-- train m14298
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND mc.company_id=70994;

-- train m20238
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mc.company_id<219228;

-- train m67677
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year>2008 AND mi.info_type_id>5;

-- train m88165
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id>1 AND mi.info_type_id=7;

-- train m48550
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.production_year=2004 AND mk.keyword_id>13699;

-- train m34522
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id<17089 AND mc.company_type_id<2;

-- train m30
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id<98;

-- train m54089
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<47802;

-- train m7351
SELECT COUNT(*) FROM title t WHERE t.kind_id<7 AND t.production_year<1973;

-- train m6959
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.production_year>2009;

-- train m51653
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.production_year<2001 AND mc.company_id=19;

-- train m80720
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id=7 AND t.production_year>1967 AND mi.info_type_id>7;

-- train m6108
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id>1 AND t.production_year>1975;

-- train m31637
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id=7 AND t.production_year>2010 AND mk.keyword_id<7861;

-- train m27362
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id<7 AND t.production_year<1999 AND mc.company_id>59 AND mc.company_type_id<2;

-- train m87191
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id<7 AND t.production_year=1979 AND mi.info_type_id<5;

-- train m19107
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=1 AND t.production_year<1953;

-- train m48071
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.production_year=2007 AND ci.role_id<2;

-- train m90150
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id=4 AND mc.company_type_id=2;

-- train m56626
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND mc.company_id>82331;

-- train m174
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.production_year<2011 AND mc.company_id=12929 AND mc.company_type_id<2 AND mk.keyword_id<3905;

-- train m2087
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.production_year=1995 AND mi.info_type_id<16;

-- train m27620
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND t.production_year>1986 AND mi.info_type_id<16;

-- train m48674
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND mi_idx.info_type_id>100 AND mk.keyword_id>3573;

-- train m71320
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id<63654;

-- train m28444
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id<92;

-- train m60163
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id=40337;

-- train m6185
SELECT COUNT(*) FROM title t WHERE t.kind_id>1 AND t.production_year<2001;

-- train m47663
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id=7 AND t.production_year>1990;

-- train m61345
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND mc.company_id<3270 AND mc.company_type_id=1;

-- train m75163
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id=7 AND t.production_year=2011 AND mi.info_type_id>3;

-- train m13954
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.production_year=1993;

-- train m22791
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND mk.keyword_id<52;

-- train m13046
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.production_year<2008 AND mc.company_id<6 AND mc.company_type_id<2 AND ci.role_id=10;

-- train m46746
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id=1 AND mi.info_type_id=16;

-- train m69060
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=1 AND t.production_year=1995 AND ci.role_id>1;

-- train m8315
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND mk.keyword_id>4333;

-- train m85345
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND mc.company_id<27608 AND mi.info_type_id=90;

-- train m16214
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id>1 AND t.production_year<2007 AND mc.company_id=72168 AND mc.company_type_id=2 AND mi_idx.info_type_id=100;

-- train m97390
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mc.company_id<33283 AND mc.company_type_id=1;

-- train m9411
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND mi.info_type_id<4 AND mi_idx.info_type_id=101;

-- train m90090
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND mi.info_type_id=18 AND mk.keyword_id=16264;

-- train m96451
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id=3 AND t.production_year<1973 AND mi_idx.info_type_id=99;

-- train m35963
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id<15839 AND mc.company_type_id>1;

-- train m752
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id<5;

-- train m34846
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id<30234;

-- train m24204
SELECT COUNT(*) FROM title t WHERE t.kind_id<7 AND t.production_year=1957;

-- train m52209
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.kind_id>1 AND t.production_year=1983;

-- train m7197
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.kind_id=1 AND t.production_year>1991;

-- train m36942
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id<7 AND t.production_year<2009 AND mi.info_type_id<3;

-- train m25263
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.production_year>2011 AND mi_idx.info_type_id>100;

-- train m1714
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id<7 AND t.production_year>2008 AND mk.keyword_id=20094;

-- train m54288
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id<4 AND t.production_year=2005 AND mc.company_type_id=2;

-- train m12465
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.production_year>1981 AND ci.role_id>1;

-- train m43851
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year=2000 AND ci.role_id>8 AND mi_idx.info_type_id<101;

-- train m84751
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND mk.keyword_id>353;

-- train m3089
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND mc.company_id>7282;

-- train m24806
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.production_year=2013 AND mc.company_type_id=2;

-- train m38473
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id=4 AND mc.company_id=11663 AND mc.company_type_id<2;

-- train m33564
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.production_year<2011 AND mi.info_type_id<3;

-- train m914
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.production_year>1980 AND mk.keyword_id=33558;

-- train m99529
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND t.kind_id=2 AND t.production_year<1978 AND mi_idx.info_type_id>99;

-- train m66694
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id>2165 AND mc.company_type_id<2;

-- train m21500
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id=61;

-- train m42001
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id=11186;

-- train m99062
SELECT COUNT(*) FROM title t WHERE t.kind_id<2 AND t.production_year>1929;

-- train m14859
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.production_year<1980;

-- train m11547
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.production_year<2009;

-- train m82616
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id<7 AND t.production_year=2011 AND mi.info_type_id=18;

-- train m93057
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id>4 AND t.production_year>1986 AND mi_idx.info_type_id<100;

-- train m59086
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.production_year>2012 AND mk.keyword_id<3593;

-- train m11367
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id>1 AND t.production_year<1949;

-- train m61485
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id=1 AND t.production_year=2002;

-- train m39347
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year=1962 AND ci.role_id<2 AND mi_idx.info_type_id<100;

-- train m62625
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id=1 AND t.production_year>1980;

-- train m32903
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id<7 AND t.production_year<1981 AND mi.info_type_id>15;

-- train m69749
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND mc.company_id<13114 AND mc.company_type_id=2;

-- train m43051
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND mc.company_type_id=2 AND mk.keyword_id<13483;

-- train m56914
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id=7 AND t.production_year=1992 AND mi.info_type_id<8;

-- train m39156
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND mi.info_type_id<107;

-- train m1203
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND mi_idx.info_type_id>99 AND mk.keyword_id=22245;

-- train m53210
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id<25414 AND mc.company_type_id=2;

-- train m269
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id>2;

-- train m76551
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id>8156;

-- train m95882
SELECT COUNT(*) FROM title t WHERE t.kind_id>6 AND t.production_year>1978;

-- train m48389
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.production_year>2004 AND ci.role_id=3;

-- train m15050
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND mc.company_id<121506;

-- train m42425
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.production_year<1989 AND mi.info_type_id=98;

-- train m92874
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.production_year>1970 AND mi_idx.info_type_id=101;

-- train m63610
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.production_year<2006 AND mk.keyword_id=13039;

-- train m84179
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.kind_id=7 AND t.production_year=2005;

-- train m60210
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.kind_id>3 AND t.production_year=2009;

-- train m40127
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.production_year<2002 AND ci.role_id>2;

-- train m4054
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND ci.role_id=5;

-- train m18624
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND mc.company_id=394;

-- train m95876
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND mc.company_id=72377 AND mc.company_type_id=2 AND mi_idx.info_type_id=99;

-- train m90209
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND mk.keyword_id<2446;

-- train m264
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND mi.info_type_id=7;

-- train m87051
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.production_year>1987;

-- train m72352
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND mk.keyword_id<26486;

-- train m97742
SELECT COUNT(*) FROM movie_companies mc WHERE mc.company_id>419 AND mc.company_type_id=2;

-- train m1638
SELECT COUNT(*) FROM movie_info mi WHERE mi.info_type_id>47;

-- train m78699
SELECT COUNT(*) FROM movie_keyword mk WHERE mk.keyword_id=8658;

-- train m55314
SELECT COUNT(*) FROM title t WHERE t.kind_id=3 AND t.production_year<1971;

-- train m11973
SELECT COUNT(*) FROM title t,cast_info ci WHERE t.id=ci.movie_id AND t.production_year<2007;

-- train m4981
SELECT COUNT(*) FROM title t,movie_companies mc WHERE t.id=mc.movie_id AND t.production_year<2010;

-- train m21245
SELECT COUNT(*) FROM title t,movie_info mi WHERE t.id=mi.movie_id AND t.kind_id>1 AND t.production_year<2009 AND mi.info_type_id>7;

-- train m95383
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx WHERE t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year<2009 AND mi_idx.info_type_id>101;

-- train m51966
SELECT COUNT(*) FROM title t,movie_keyword mk WHERE t.id=mk.movie_id AND t.kind_id=3 AND t.production_year=2011 AND mk.keyword_id<3658;

-- train m78834
SELECT COUNT(*) FROM title t,movie_companies mc,cast_info ci WHERE t.id=mc.movie_id AND t.id=ci.movie_id AND t.production_year>1989 AND mc.company_id<191 AND ci.role_id>2;

-- train m80637
SELECT COUNT(*) FROM title t,cast_info ci,movie_info mi WHERE t.id=ci.movie_id AND t.id=mi.movie_id AND t.production_year=2002 AND mi.info_type_id=16;

-- train m48766
SELECT COUNT(*) FROM title t,cast_info ci,movie_info_idx mi_idx WHERE t.id=ci.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<2 AND t.production_year<2011 AND ci.role_id=1 AND mi_idx.info_type_id<101;

-- train m92745
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk WHERE t.id=ci.movie_id AND t.id=mk.movie_id AND t.kind_id=7 AND t.production_year<2008 AND ci.role_id=2;

-- train m38852
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.kind_id>1 AND t.production_year<1989 AND mc.company_type_id=2;

-- train m33683
SELECT COUNT(*) FROM title t,movie_companies mc,movie_info_idx mi_idx WHERE t.id=mc.movie_id AND t.id=mi_idx.movie_id AND t.kind_id<7 AND t.production_year=2011 AND mc.company_type_id>1;

-- train m44879
SELECT COUNT(*) FROM title t,movie_companies mc,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mk.movie_id AND t.kind_id>1 AND mc.company_id=42738 AND mc.company_type_id>1 AND mk.keyword_id<359;

-- train m75506
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND mi.info_type_id=103;

-- train m24185
SELECT COUNT(*) FROM title t,movie_info mi,movie_keyword mk WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.production_year=2006;

-- train m1717
SELECT COUNT(*) FROM title t,movie_info_idx mi_idx,movie_keyword mk WHERE t.id=mi_idx.movie_id AND t.id=mk.movie_id AND mk.keyword_id>18046;
