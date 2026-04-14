## Summary

Describe the problem and the change clearly.

## Testing

List the validation you actually ran.

- [ ] `mvn clean test -B`
- [ ] `mvn spotless:check -B`
- [ ] `mvn verify -P wildfly-managed,postgresql -B -pl ratchet-testsuite,ratchet-coverage -am`
- [ ] `cd website && npm ci && npm run build`

## Docs

- [ ] Docs updated where behavior, configuration, logs, or examples changed
- [ ] No docs update needed

## Review Notes

- [ ] Breaking change
- [ ] Public API or SPI change
- [ ] Security-sensitive change
- [ ] Follow-up work remains

## Additional Context

Anything reviewers should pay attention to.
