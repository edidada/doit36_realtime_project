# Public option benchmark

Source: [QuantLib 1.40 European option tests](https://github.com/lballabio/QuantLib/blob/06edddbdb85f006c43a449b5fc846a00c67477f3/test-suite/europeanoption.cpp#L261-L315).

- Pinned commit: `06edddbdb85f006c43a449b5fc846a00c67477f3` (tag `v1.40`).
- Downloaded/selected: 2026-10-29, first 8 entries of the 43-entry `testValues` array, preserving source order.
- Upstream C++ UTF-8 content SHA-256: `131e88f0ec15940d9b5d35fbe65ab7c19f456419adea11a5c1c63ba59a121335`.
- `quantlib-european-options.csv` maps upstream `type, strike, s, q, r, t, v, result, tol` directly to named CSV columns. Only `case_id` and uppercase option type formatting were added. No prices, rates, volatility, expected values or tolerances were invented.
- Upstream identifies the original numerical examples as E.G. Haug, *Option pricing formulas* (1998). These are published numerical regression vectors in an open-source project, **not actual option quotes, customer payments, or current market data**.
- `QuantLib-LICENSE.TXT` is the unmodified upstream license/copyright file and is distributed alongside the data in the JAR.
- Local `valuationDate=2026-01-01` is fixed application metadata, not an observed quote date; maturity is the upstream year fraction. The snapshot is `quantlib-1.40-test-values`.
- Default Monte Carlo settings (100000 paths, seed 42, one worker) belong to this application, not the upstream fixture. The analytic result is checked against upstream tolerance `1e-4`; Monte Carlo uncertainty is reported separately, never substituted for the analytic tolerance.

Run offline after Maven dependencies are installed:

```powershell
mvn -B -ntp -pl fin-risk-engine -am verify
mvn -q -f fin-risk-engine/pom.xml exec:java
```
