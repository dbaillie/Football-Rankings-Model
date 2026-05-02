Agent Instructions: Implement GCAM Connectivity Layer on Top of Existing Glicko-2 System
You are working in an existing codebase that already contains a standard Glicko-2 rating algorithm.
Your task is to extend the system with a GCAM layer: Global Community Alignment Model.
GCAM is not a replacement for Glicko-2. It is an additional layer that measures how globally reliable each rating is, based on the connectivity of the player/team to the wider competitive ecosystem.
The base Glicko-2 system estimates performance strength.
GCAM should estimate evidence quality, global calibration, uncertainty, and trust.
---
1. Overall Objective
Implement a system that separates:
```text
raw rating
```
from:
```text
globally trusted rating
```
The existing Glicko-2 rating should remain the raw estimate of strength.
The new GCAM layer should produce:
```text
connectivity score
structural uncertainty
trust factor
baseline-adjusted rating
power score for downstream use
diagnostics
```
The purpose is to prevent isolated competitive pools from inflating while still allowing strong competitors from less-connected areas to demonstrate strength when sufficient evidence exists.
---
2. Core Principle
Do not treat low connectivity as evidence that a player/team is weak.
Treat low connectivity as evidence that the rating is less globally certain.
The language in the implementation, comments, and outputs should use terms such as:
```text
connectivity
trust
calibration
evidence quality
global comparability
uncertainty
```
Avoid language such as:
```text
penalty
punishment
weak league
bad region
```
---
3. Data Model
The GCAM layer should be built around a generic interaction model.
The same logic should work for:
```text
WAGR / golf
football
other sports
```
Use generic terminology:
```text
entity = player or team
community = league, country, region, tour, federation, college system, etc.
interaction = match, fixture, pairwise comparison, shared event result
rating period = week, month, season-week, or other update period
```
The input to GCAM should represent interactions between two entities.
Each interaction should identify:
```text
rating period
entity A
entity B
community of entity A
community of entity B
result for entity A
result for entity B
interaction type
interaction weight
event or fixture identifier, where available
date, where available
```
For WAGR/golf, interactions will usually come from pairwise comparisons generated from event finish positions.
For football, interactions will usually come from fixtures.
---
4. Required GCAM Outputs
For each entity and rating period, the system should produce:
```text
entity identifier
rating period
raw Glicko-2 rating
raw Glicko-2 RD
raw Glicko-2 volatility, if available
number of interactions
number of distinct opponents
number of distinct opponent communities
entropy
normalised entropy
volume trust
direct connectivity
community connectivity
effective connectivity
structural RD
total RD
trust factor
baseline rating
adjusted rating
power score
```
For community-level analysis, the system should also produce:
```text
community identifier
rating period
number of internal interactions
number of external interactions
number of external communities encountered
community entropy
normalised community entropy
community volume trust
community connectivity
raw mean rating
adjusted mean rating
baseline rating
```
---
5. Rolling Connectivity Window
Connectivity should be calculated over a rolling window.
For WAGR, a sensible default is the previous 104 weeks.
For football, this could be the previous two seasons or previous 104 weeks.
The implementation should not simply subtract YYYYWW values numerically. It should use proper date or week-index logic.
The rolling window should be configurable.
---
6. Entity Connectivity
For each entity, calculate how widely its interactions are spread across opponent communities.
The idea is:
```text
An entity that only plays opponents from one community has low connectivity.
An entity that plays opponents from many communities has higher connectivity.
An entity that plays many interactions has more reliable connectivity than one with only a few interactions.
```
The system should calculate:
```text
interaction count by opponent community
total interaction count
share of interactions by opponent community
entropy of opponent-community distribution
normalised entropy
volume trust
effective connectivity
```
Normalised entropy should be zero where an entity has only one observed opponent community or no interactions.
Volume trust should increase with the number of interactions and saturate towards one.
Effective connectivity should combine normalised entropy and volume trust, with a configurable floor so no entity has zero connectivity.
---
7. Community Connectivity
For each community, calculate how well it is connected to other communities.
For football, this is especially important.
Examples:
```text
A domestic league with many clubs playing continental fixtures against many different leagues is highly connected.
A domestic league with only one club playing a couple of qualifiers is less connected.
A fully domestic-only league has low external connectivity.
```
Community connectivity should be based on:
```text
external interactions
distribution of external interactions by opponent community
community entropy
normalised community entropy
external interaction volume
community volume trust
community connectivity floor
```
An interaction is external when the two entities are from different communities.
---
8. Effective Connectivity
Each entity should receive an effective connectivity score.
This can be based on:
```text
direct connectivity from the entity’s own interactions
community connectivity from the entity’s league/region/tour/etc.
```
For the first implementation, use a simple weighted blend of direct connectivity and community connectivity.
The blend should be configurable.
The effective connectivity score should always be bounded between a configurable floor and one.
The purpose of effective connectivity is to answer:
```text
How globally informative is this entity’s evidence?
```
---
9. Connectivity-Weighted Interactions
The system should be capable of using connectivity to weight rating updates.
For an interaction between entity A and entity B, compute a connectivity interaction weight based on the average effective connectivity of the two entities.
High-connectivity interactions should carry more global calibration information.
Low-connectivity interactions should still count, but should not move global ratings as aggressively.
This should be optional.
The first implementation may apply GCAM only after raw Glicko-2 ratings are calculated.
A later implementation can feed connectivity weights into the Glicko-2 update process itself.
The system should therefore support two modes:
```text
post-hoc mode:
    run Glicko-2 normally, then apply GCAM adjustment to outputs

weighted-update mode:
    use GCAM connectivity weights inside the Glicko-2 update process
```
Start with post-hoc mode unless the existing Glicko-2 engine already supports weighted interactions cleanly.
---
10. Structural Uncertainty
GCAM should add the concept of structural uncertainty.
Standard Glicko-2 RD reflects uncertainty from rating evidence and inactivity.
GCAM structural RD reflects uncertainty from poor global connectivity.
An entity can play frequently and still have high structural uncertainty if all evidence comes from an isolated pool.
Structural RD should increase as effective connectivity decreases.
Total RD should combine:
```text
raw Glicko-2 RD
structural RD
```
The combined total RD should be used for:
```text
diagnostics
trust factor
adjusted rating
power score
presentation
```
Do not necessarily replace the internal Glicko-2 RD with structural RD unless explicitly designed to do so.
---
11. Trust Factor
Each entity should receive a trust factor.
The trust factor should represent how much of the raw Glicko-2 rating should be trusted as globally comparable.
Trust should be higher when:
```text
effective connectivity is high
total RD is low
interaction volume is sufficient
```
Trust should be lower when:
```text
effective connectivity is low
total RD is high
interaction volume is limited
```
Trust should be bounded between a configurable floor and one.
This trust factor will drive rating shrinkage.
---
12. Baseline Rating
The adjusted rating should shrink the raw rating towards a baseline.
The baseline should be configurable.
Support at least three baseline modes:
```text
global baseline
community baseline
blend of community and global baseline
```
The global baseline is the mean or neutral rating across all entities.
The community baseline is the mean or neutral rating for the entity’s own community.
The blended baseline combines the two.
For poorly connected communities, the baseline should lean more towards the global baseline.
For well-connected communities, the baseline can place more trust in the community baseline.
The first implementation may use a fixed blend. A later version can make the blend dynamic based on community connectivity.
---
13. Adjusted Rating
The adjusted rating should combine:
```text
raw Glicko-2 rating
baseline rating
trust factor
```
High-trust entities should retain most of their raw rating.
Low-trust entities should be pulled more strongly towards the baseline.
This should produce a rating that is more globally comparable than the raw rating alone.
The adjusted rating is the main GCAM output.
---
14. Power Score
The system should produce a power score for downstream use.
For WAGR, this could feed event strength.
For football, this could feed league power rankings or club power rankings.
The power score should be based on:
```text
adjusted rating
total RD / confidence
effective connectivity
```
The exact formula should be configurable.
Support modes such as:
```text
adjusted rating only
adjusted rating adjusted by RD confidence
adjusted rating adjusted by RD confidence and connectivity
```
The default should probably use adjusted rating and RD confidence.
---
15. WAGR Application
For WAGR/golf, GCAM should be able to support:
```text
players as entities
events as sources of pairwise interactions
countries, regions, tours, federations, or competition systems as communities
weekly rating periods
rolling 104-week connectivity
```
GCAM should help identify:
```text
players with high raw ratings but low global evidence
players with strong global connectivity
regions or tours that are well connected
regions or tours that are relatively isolated
events whose strength is inflated by isolated ratings
events whose strength is supported by globally connected players
```
The intended pipeline is:
```text
event results
pairwise interactions
raw Glicko-2 ratings
connectivity features
structural RD
trust factor
adjusted rating
player power
event strength
points
ranking
```
---
16. Football Application
For football, GCAM should be able to support:
```text
teams as entities
fixtures as interactions
domestic leagues as communities
UEFA continental competitions as cross-community links
weekly or season-week rating periods
```
Domestic fixtures mostly provide internal ordering within a league.
Continental fixtures provide calibration between leagues.
The system should distinguish between:
```text
domestic league matches
domestic cup matches
Champions League matches
Europa League matches
Conference League matches
qualifiers
friendlies, if included
```
Each match type should have a configurable interaction weight.
Continental matches should generally carry more cross-community calibration value than domestic matches.
Friendlies should be excluded or given very low weight by default.
---
17. Football-Specific Interpretation
The football version should be able to answer:
```text
Which leagues are strongest after adjusting for continental evidence?
Which teams are dominant domestically but globally uncertain?
Which teams carry the most cross-league calibration?
Which leagues are internally strong but externally weakly tested?
Which teams look different under raw ratings versus trust-adjusted ratings?
```
Examples of intended logic:
```text
A top club from a highly connected league should have high effective connectivity even if it does not personally play every European opponent.

A mid-table team from a highly connected league may inherit some league-level connectivity through domestic matches against European-calibrated teams.

A dominant team from a poorly connected league may have a high raw rating but lower trust-adjusted rating unless it performs well in continental competition.

A league with many clubs playing continental fixtures across many opponent leagues should receive higher community connectivity.

A league with only limited qualifier participation should receive lower community connectivity.
```
---
18. Diagnostics
Add diagnostics that make the system explainable.
At minimum, produce tables showing:
```text
highest connectivity entities
lowest connectivity entities
largest downward rating adjustments
largest upward or least-shrunk ratings
highest raw ratings with low trust
highest adjusted ratings
communities with highest connectivity
communities with lowest connectivity
communities with largest raw-to-adjusted shifts
```
For football, additionally show:
```text
leagues most connected through continental fixtures
teams carrying most cross-league calibration
teams that rely mostly on domestic evidence
domestic leagues with high internal ratings but low external calibration
```
For WAGR, additionally show:
```text
players with high ratings but low global connectivity
players whose ratings are well supported by international evidence
events whose field strength is most affected by GCAM
regions or tours with low calibration
```
---
19. Implementation Structure
The GCAM logic should be modular.
Do not bury the calculations inside the Glicko-2 engine.
Recommended separation:
```text
Glicko-2 engine:
    raw rating calculations

GCAM connectivity module:
    entropy, volume trust, entity connectivity, community connectivity

GCAM adjustment module:
    structural RD, trust factor, baseline, adjusted rating, power score

GCAM pipeline module:
    orchestrates the full post-hoc or weighted-update flow

Sport adapters:
    convert WAGR or football data into the generic interaction format
```
The core GCAM modules should be sport-agnostic.
Sport-specific assumptions should live only in adapters or configuration.
---
20. Testing Requirements
Add tests for the GCAM logic.
The tests should confirm:
```text
Entropy is zero when all interactions are in one community.
Entropy is high when interactions are evenly spread across several communities.
Volume trust increases with interaction count.
Effective connectivity never falls below the configured floor.
Effective connectivity never exceeds one.
Structural RD is zero or near zero for fully connected entities.
Structural RD is high for low-connectivity entities.
Total RD is never lower than raw Glicko-2 RD.
Trust factor is bounded between its floor and one.
Adjusted rating equals raw rating when trust is one.
Adjusted rating equals baseline when trust is zero.
Adjusted rating moves towards baseline as trust decreases.
High-connectivity interactions receive more weight than low-connectivity interactions.
Community connectivity increases with broader external fixture exposure.
```
Existing Glicko-2 tests must continue to pass.
---
21. Implementation Phases
Implement in phases.
Phase 1: Post-Hoc GCAM
Do not modify the Glicko-2 update algorithm.
Add:
```text
connectivity calculation
community connectivity
structural RD
total RD
trust factor
baseline calculation
adjusted rating
power score
diagnostics
```
This phase should produce usable outputs immediately.
Phase 2: Weighted Update Mode
If practical, add optional connectivity-weighted interactions to the Glicko-2 update process.
This should remain optional and configurable.
Phase 3: WAGR Adapter
Add or refine conversion from WAGR event results into pairwise interactions.
Phase 4: Football Adapter
Add conversion from football fixture data into generic interactions.
Phase 5: Reporting Outputs
Add summary tables and diagnostics suitable for dashboards or notebooks.
---
22. Acceptance Criteria
The task is complete when:
```text
Existing Glicko-2 behaviour is preserved.
GCAM can run as a post-hoc layer.
The system outputs raw and adjusted ratings.
The system outputs connectivity and trust metrics.
The system outputs structural RD and total RD.
The system outputs entity-level diagnostics.
The system outputs community-level diagnostics.
The core GCAM logic is sport-agnostic.
Football and WAGR assumptions are isolated in adapters or configuration.
The implementation is configurable.
There are tests for the core GCAM calculations.
The documentation explains how to interpret raw rating, adjusted rating, connectivity, trust, and structural RD.
```
---
23. Do Not Overbuild the First Version
Do not add the following in the first pass:
```text
market value priors
transfer graph links
squad-strength models
xG models
player-level football modelling
graph neural networks
Bayesian hierarchical models
automatic community detection
```
The first version should be deliberately simple:
```text
raw Glicko-2
+
rolling connectivity
+
entropy
+
volume trust
+
structural RD
+
trust-based shrinkage
+
adjusted rating
+
power score
```
---
24. Final Implementation Intent
The final system should let us say:
```text
This entity has a high raw rating, but we have limited global evidence, so the adjusted rating is more conservative.

This entity has strong cross-community evidence, so its raw rating is trusted more.

This community is well calibrated through external interactions.

This community is internally active but globally uncertain.

The adjusted ratings are more globally comparable than raw Glicko-2 ratings alone.
```
The goal is not to punish isolated entities.
The goal is to make the rating system honest about the quality and global comparability of the evidence.