# The estimators

pgpi bundles seven estimation methods as plugins, one file each. This page summarizes how each works and how it differs from its paper. The head of each file has the details.

Three are the project's own (feedback, hres, matcomp). One is simplified from SQL Server (ce-feedback). Three follow published papers (litecard, nngp, grasp).

`example/my_estimator.py` is a template for your own estimator, not a bundled plugin. See [README 6.3](../../../README.md#63-writing-your-own-plugin).

## Common to all

- **What they learn from.** The nodes of executed plans that pgpi captured, with their actual row counts, and PostgreSQL's statistics as recorded in the dataset. The benchmarks use a few hundred queries. No plugin reads table data. Many papers train on generated workloads, labeled with true cardinalities and often far larger.
- **When they learn.** In batches. `fit()` runs on the dataset when the estimation server starts. New observations take effect after a collect and a restart.
- **How an answer is used.** Each answer replaces PostgreSQL's estimate at one intervention point (`base`, `join`, ...; see [README 1.3](../../../README.md#13-intervene-what-can-be-changed)). For a join, PostgreSQL has already computed its estimate from the inputs as answered. A plugin that answers with a factor on PostgreSQL's estimate therefore corrects only what the inputs do not already carry. feedback, hres and litecard take this into account, each in its own way.
- **Not the authors' code.** The plugins of published methods are written from the papers. No code of the authors was used, and none has been checked against the results its paper reports.
- **Shortened training in the benchmarks.** `SPEC_EXTRA` and `QUICK_EXTRA` in `lib/bench.py` set `grasp:epochs=100,restarts=1` for the normal runs. With `--quick`, they set `grasp:epochs=30,restarts=1` and `matcomp:restarts=1`.

## feedback (this project)

Remembers what it observed, and corrects PostgreSQL's estimate by the same amount. It is the simplest learning loop.

It keeps two memories:

- **A question seen before:** the observed rows. "Seen before" means the same descriptor, literals included.
- **A question of a known template:** a correction factor on PostgreSQL's estimate, the geometric mean of the observed corrections. A template is the same question up to its literals: `region = 'r3'` and `region = 'r7'` share one.

For a join, the factor corrects only the join's own selectivity. That is true(S)/pg(S) divided by the corrections of its base relations, since PostgreSQL's estimate of the join already carries those. `feedback:naive` learns true(S)/pg(S) and corrects the inputs twice, to show the difference.

The idea of correcting estimates with execution feedback goes back to LEO (Stillger et al., VLDB 2001).

## hres (this project)

Learns the residual of PostgreSQL's estimate. With no residual learned, the answer is PostgreSQL's estimate itself.

The residual is *r* = log(actual / PostgreSQL's estimate). A positive *r* means PostgreSQL underestimated. For a join, the residual is that of the join's own selectivity.

Each observation is filed at four levels of patterns, from specific to coarse:

- L4: the same question
- L3: the same question up to literals
- L2: the same tables with conditions on the same columns
- L1: the same tables

The estimate at a level is shrunk towards that of the coarser level by partial pooling. A pattern seen once therefore moves the answer little.

The correction applied is what remains certain, given the spread of the residuals within the pattern. Downward corrections (towards underestimates, the riskier side) need more evidence.

An example of a method built with this framework.

## matcomp (this project)

Completes a matrix of join couplings from the joins observed, with a low-rank model.

The **coupling** of two join inputs is log(|L join R| / (|L| |R|)). It is the unknown entry of a matrix. Its rows and columns are what was filtered on each side, for example region r3 on one side and category c7 on the other.

The model has:

- a bias per join edge.
- a bias and an embedding per predicate.

A new literal keeps its template's bias and adds no interaction, which is PostgreSQL's independence assumption. Base relations are corrected by feedback (`base=feedback`).

## ce-feedback (simplified from SQL Server)

Switches PostgreSQL's assumptions per query, like SQL Server 2022's CE feedback.

It works per query (queryid). When executions show a large error on the operators an assumption governs, the next executions try the alternative. It keeps the alternative only if the error went down and the execution time did not regress. Two assumptions are switched:

- **Correlation of filters on one table:** one step toward full correlation, or toward independence.
- **Containment of equi-joins:** the other model.

Differs from SQL Server:

- Only these two assumptions, with four of its hints. Row-goal feedback is not reproduced.
- The rest of SQL Server's estimator is not reproduced, and the statistics are PostgreSQL's.
- Queries are told apart by queryid, not by a query hash.
- One round of analysis runs per `fit` (or `python3 -m pgpi cefeedback`), not in the background.
- Hints are kept in a JSON file, not in the Query Store.

Not in the benchmarks' default list of plugins.

## litecard (follows the paper)

LiteCard (Yi et al., 2025) trains many small local regressors, one per recurring subquery pattern.

The answer to a question comes in four steps:

1. **Graph.** The subquery becomes a graph: the parse tree of its predicates, merged with tables, aliases and columns.
2. **Patterns.** The graph is hashed (MD5, by two passes of propagation) at three levels. **H3**: only the literals differ. **H2**: the operators are also ignored. **H1**: only the tables and column types count.
3. **Regression.** The question is answered at the most specific level with enough history (thresholds 10, 50, 100). Gradient-boosted trees are fitted for the question on the observations of its pattern, on the raw row count. A Gaussian kernel on the features weights the observations (Appendix D: f1 to f5, f8; column range and distinct count come from `pg_statistic`).
4. **Abstention and bias correction.** Without enough history, the plugin abstains. For an inner join, it then applies the bias correction of Appendix B.4. A random number decides, with the probability that PostgreSQL underestimates a subquery with that many joins. If the correction applies, PostgreSQL's estimate is multiplied by the average q-error of those underestimates (Table 5).

Differs from the paper:

- **Injection.** The paper injects every subquery's estimate into PostgreSQL. In the planner, a join is estimated from inputs already answered. The plugin recovers PostgreSQL's estimate with uncorrected inputs from the earlier requests of the same planning, and corrects that once (`_own_estimate`). A join whose inputs it has not seen is not corrected. Table 5 is the paper's, measured on its IMDb workload, not on the database at hand (see [Case: litecard on ceb_1a](../../../benchmark/ANALYSIS.md#case-litecard-on-ceb_1a)).
- **Where the paper does not say.** The graph is built from PostgreSQL's deparsed clauses, with casts removed and `= ANY` turned back into IN lists. The root node, the operators other than the five ordinal ones, ties in the canonical order and timestamps are encoded in ways the paper leaves open. The kernel width is not given, so the features are min-max scaled over the bucket before the kernel.
- **Learning.** Batch learning instead of online. The regressor is fitted per question, as in the paper, but its answer is kept for a repeated question.

## nngp (follows the paper)

NNGP (Zhao et al., SIGMOD 2022) is a Gaussian process whose kernel is that of an infinitely wide ReLU network.

It works on a fixed-length encoding of the question. For each attribute, the encoding holds the normalized range of its condition, or a bitmap (or factorized bitmap) of the values it allows. Each joinable pair of attributes takes 3 bits. Inference is exact, and the answer is the posterior mean and variance of the log cardinality.

Differs from the paper:

- **Chosen here, not given in the paper.**
  - the depth (one hidden layer), the prior variances and the label noise.
  - the encoding of attributes without a condition (the whole domain).
  - the domain of categorical attributes (the values seen in the workload).
  - which numeric attributes are categorical (those only compared with `=` or `IN`).
  - the size from which a factorized bitmap is used. It is large here, so that the slices read as integers do not outweigh the other positions in the kernel.
- **Added.** One bit per relation, since pgpi asks about every subquery. And no answer for a set of relations that no observation covered. PostgreSQL never executes some subqueries, and on CEB 1a the process answered those with confidence and far off (`SEEN_RELATIONS`).
- **Limits.** Questions outside the encoding are left to PostgreSQL: LIKE, OR, attributes not seen in training, parameterized questions. At most 4,000 observations are used, because exact inference is cubic. The kernel is computed with NumPy in closed form, instead of Neural Tangents.

## grasp (re-implementation, differs)

GRASP (Wu et al., PVLDB 2025) composes per-table primitives that are trained end to end from cardinalities.

There are two primitives:

- **CardEst:** the cardinality of a single-table query. ArCDF, an autoregressive model of the joint CDF, handles range predicates. MSCN's set encoding handles the others.
- **A learned count sketch per join key:** a softmax vector that stands for the distribution of the key in the query's result.

A join is estimated from the single-table cardinalities and the inner products of the sketches. Join shapes never seen in training can therefore be answered.

Differs from the paper:

- **Labels.** By default, every node pgpi observed is a label: intermediate joins, base relations, parameterized scans (`labels=all`). The paper uses the final cardinality of each query (`labels=final`).
- **Sketch width.** 64 by default with node labels. The paper uses 100 to 3000.
- **Range and other predicates.** The paper does not say how to combine them. Here, the estimate is P(other) times P(range | other), with ArCDF conditioned on the encoding of the other predicates.
- **Parameterized questions.** The outer relations contribute only their key sketches. Nodes that stopped early can be used as lower bounds (`censored=1`).
- **IN lists.** The paper does not describe them. Here, each value of the list sets its own hashed position of the predicate's encoding (multi-hot), so that lists sharing values look alike. With `in_split=1` (off by default), a target table's IN list is instead split into its values. The question is estimated once per value, and the estimates are added, in training and in estimation. The values of one column are disjoint, so the sum is exact, and a list never seen is answered from its values.
