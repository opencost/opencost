# Automated Testing in OpenCost

This document governs OpenCost's approach to automated testing. OpenCost has two main components of automated testing, Integration Tests and Unit Tests.

Unit Tests are designed to test small pieces of code. They make extensive use of mocks and other synthetic items. These tests are designed to be run quickly and easily on developers' machines. 

Integration Tests are designed to test the functionality of groups of units of code working together. These are typically more complex tests that require more setup than usual. In the context of OpenCost, this means typically we are querying against a real Prometheus with real data. 

## OpenCost Automation Pipeline

OpenCost will execute its unit and integration tests in accordance with the following pipeline architecture:

![OpenCost Test Architecture](OC%20Test%20Arch.png)

The tests will execute on contributed code at different stages in the pipeline depending on whether or not the contributor of the code is a maintainer or is a third party. If the contributor is a third party, the Integration Tests will not be executed on the code until it is in the merge queue, so that unreviewed/unapproved code will not be able to execute tests or access integration test clusters. 