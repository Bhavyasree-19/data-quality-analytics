# Quality Policy

## Severity levels
- **Critical**: Breaks reporting or compliance (must be fixed immediately)
- **High**: Material data correctness risk
- **Medium**: Noticeable quality issues, but not system-breaking
- **Low**: Outliers or minor quality signals

## SLA thresholds
Defined in `config/quality_profile.yml`:
- Minimum pass rate: 95%
- Maximum critical failures: 0

If either threshold is violated, the run is marked **SLA FAIL**.

## Check mapping
Severity per rule type is configured in `config/quality_profile.yml`.
