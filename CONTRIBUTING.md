# Contributing to our project #

Thanks for your help improving the project!

## Getting Help ##

If you have a question about Kubecost or have encountered problems using it,
you can start by asking a question on [Slack](https://join.slack.com/t/kubecost/shared_invite/enQtNTA2MjQ1NDUyODE5LTg0MzYyMDIzN2E4M2M5OTE3NjdmODJlNzBjZGY1NjQ3MThlODVjMGY3NWZlNjQ5NjIwNDc2NGU3MWNiM2E5Mjc) or via email at [team@kubecost.com](team@kubecost.com)

## Certification of Origin ##

By contributing to this project you certify that your contribution was created in whole or in part by you and that you have the right to submit it under the open source license indicated in the project. In other words, please confirm that you, as a contributor, have the legal right to make the contribution. 

## Committing ###

Please write a commit message with Fixes Issue # if there is an outstanding issue that is fixed. It’s okay to submit a PR without a corresponding issue, just please try be detailed in the description about the problem you’re addressing.

Please run go fmt on the project directory. Lint can be okay (for example, comments on exported functions are nice but not required on the server). 

Integration testing is coming soon! When these exist make sure the integration tests pass :). For now, if you need help manually testing reach out to us on [Slack](https://join.slack.com/t/kubecost/shared_invite/enQtNTA2MjQ1NDUyODE5LTg0MzYyMDIzN2E4M2M5OTE3NjdmODJlNzBjZGY1NjQ3MThlODVjMGY3NWZlNjQ5NjIwNDc2NGU3MWNiM2E5Mjc). To generalize, if you hit the /costDataModel?timeWindow=1d endpoint and haven’t changed allocation code, the project isn't completely broken.
