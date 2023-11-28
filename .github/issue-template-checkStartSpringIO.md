---
title: Check start.spring.io
---

### Context
[Failed Run](https://github.com/{{ env.GITHUB_REPOSITORY }}/actions/runs/{{ env.GITHUB_RUN_ID }})
Workflow name = `{{ env.GITHUB_WORKFLOW }}`
Job -           `{{ env.GITHUB_JOB }}`
### Error Info
See Failed Run link above for more info, but the likely culprit is a new version of Spring Boot has been released and we need to open an issue over at [start.spring.io](https://github.com/spring-io/start.spring.io/issues) to have them re-enable Solace.