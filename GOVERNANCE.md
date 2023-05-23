# OpenCost Governance

> **Note**
> OpenCost community governance is a work in progress as we expand beyond the initial Kubecost stewardship. Expanding the committers and maintainers to additional organizations will allow the project to become more self-managing.

This document attempts to clarify how the [OpenCost](https://github.com/opencost/) projects are maintained. Anyone interested in improving the project may join the community, contribute to the project, and participate in shaping future releases. This document attempts to outline the general participation structure and set expectations within the project community.

## Code of Conduct

OpenCost community members are expected to adhere to our published [Code of Conduct](CODE_OF_CONDUCT.md).

## Roles and Responsibilities

There are 4 levels of membership in the OpenCost community.

### Contributor

Contributors are community members who contribute in concrete ways to the project. Anyone can contribute to the project and become a contributor, regardless of their skillset. There is no expectation of commitment to the project, no specific skill requirements, and no selection process. There are many ways to contribute to the project, which may be one or more of the following (but not limited to):

- Participate in community discussions in the `#opencost` channel in [CNCF Slack](https://slack.cncf.io/) or at [community meetings](https://bit.ly/opencost-meeting)
- Report, comment on, and sometimes resolve Issues
- Occasionally submit PRs
- Try out new releases
- Contribute to the documentation
- Improve the OpenCost website
- Promote the project in public
- Help other users

For first-time contributors, it’s recommended to start by going through [Contributing to OpenCost](CONTRIBUTING.md) and joining our community Slack channel.

### Member

[Members](https://github.com/orgs/opencost/people) are continuously active contributors in the community. There are multiple ways to stay "active" and engaged with us - contributing to codes, raising issues, writing tutorials and case studies, and even answering questions.

To become an OpenCost member, you are expected to:

- Make multiple contributions, which may be one or more of the following (but not limited to):
    - Authored PRs on GitHub.
    - Filed, or commented on Issues on GitHub.
    - Join community discussions (e.g. community meetings, Slack).
- Sponsored by at least 1 OpenCost [maintainer or committer](MAINTAINERS.md)

Contributors that meet the above requirements will then be invited to the GitHub organization "OpenCost" by a sponsor, and there would be an announcement published in the slack channel [(#opencost)](https://slack.cncf.io/).

Members are expected to respond to issues and PRs assigned to them, and be the owners of the code or docs they have contributed. Members that have not contributed to the project or community for over 6 months may lose their membership.

### Committer

Committers are active community members who have shown that they are committed to the success of the project through ongoing engagement with the community. Committership allows contributors to more easily carry on with their project-related activities by giving them direct access to the project’s resources.

Committers are granted `Triage` permissions for the OpenCost repository.

Typically, a potential committer needs to show that they have a sufficient understanding of the project, its objectives, and its strategy. To become a committer, you are expected to:

- Be an OpenCost member.
- Express interest to the existing maintainers that you are interested in becoming a committer.
- Have contributed 5 or more substantial PRs.
- Have an above-average understanding of the project codebase, its goals, and directions.

Members that meet the above requirements will be nominated by an existing maintainer to become a committer. It is recommended to describe the reasons for the nomination and the contribution of the nominee in the PR. The existing maintainers will confer and decide whether to grant committer status or not.

Committers are expected to review issues and PRs. While committership indicates a valued member of the community who has demonstrated a healthy respect for the project’s aims and objectives, their work continues to be reviewed by the community before acceptance in an official release.

### Maintainer

Maintainers are first and foremost committers that have shown they are committed to the long term success of a project. They are the planners and designers of the OpenCost project. Maintainership is about building trust with the current maintainers of the project and being a person that they can depend on to make decisions in the best interest of the project in a consistent manner.

Maintainers are granted `Write` permissions for the OpenCost repository.

Committers wanting to become maintainers are expected to:

- Enable adoptions or ecosystems.
- Collaborate well.
- Demonstrate a deep and comprehensive understanding of OpenCost's architecture, technical goals, and directions.
- Actively engage with major OpenCost feature proposals and implementations.

A new maintainer must be nominated by an existing maintainer. The nominating maintainer will create a PR to update the [Maintainers List](MAINTAINERS.md). It is recommended to describe the reasons for the nomination and the contribution of the nominee in the PR. Upon consensus of incumbent maintainers, the PR will be approved and the new maintainer becomes active.

## Policies and Procedures

### Community Meetings

The [OpenCost Community Meeting](https://bit.ly/opencost-meeting) is every 2 weeks at 1pm Pacific. Community Members are encouraged to attend and discuss development, events, and any other OpenCost community items of interest.

### Issue/PR Timelines

Best efforts will be given to respond to new Issues and PRs within 24 hours on weekdays.

### Approving PRs

PRs may be merged only after receiving at least one approvals from committers or maintainers. However, maintainers can sidestep this rule under justifiable circumstances. For example:

- If a CI tool is broken, may override the tool to still submit the change.
- Minor typos or fixes for broken tests.
- The change was approved through other means than the standard process.

### Decision Making Process

Ideally, all project decisions are resolved by consensus via a PR or GitHub issue. Any of the day-to-day project maintenance can be done by a [lazy consensus model](https://communitymgt.fandom.com/wiki/Lazy_consensus).

Community or project level decisions such as RFC submission, creating a new project, maintainer promotion, and major updates on GOVERNANCE must be brought to broader awareness of the community via community meetings, GitHub discussions, and the Slack channel. A supermajority (2/3) approval from Maintainers is required for such approvals.

In general, we prefer that technical issues and maintainer membership are amicably worked out between the persons involved. If a dispute cannot be decided independently, the maintainers can be called in to resolve the issue by voting. For voting, a specific statement of what is being voted on should be added to the relevant GitHub issue or PR, and a link to that issue or PR added to the maintainers meeting agenda document. Maintainers should indicate their yes/no vote on that issue or PR, and after a suitable period of time, the votes will be tallied and the outcome noted.

### Conflict resolution and voting

In general, we prefer that technical issues and membership are amicably worked out between the persons involved. If a dispute cannot be decided independently, the sponsors and core maintainers can be called in to decide an issue. If the sponsors and maintainers themselves cannot decide an issue, the issue will be resolved by voting.

In all cases in this document where voting is mentioned, the voting process is a simple majority in which each sponsor receives two votes and each core maintainer receives one vote. If such a majority is reached, the vote is said to have _passed_.

### Proposal process

> **Note**
> We intend to use a Request for Comments (RFC) process for any substantial changes to OpenCost, but this has yet to be developed.

### Inactivity

It is important for contributors to be and stay active to set an example and show commitment to the project. Inactivity is harmful to the project as it may lead to unexpected delays, contributor attrition, and a loss of trust in the project.

Inactivity is measured by periods of no contributions without explanation, for longer than:

- Maintainer: 3 months
- Committer: 6 months
- Member: 12 months

Consequences of being inactive include:

- Involuntary removal or demotion
- Being asked to move to Emeritus status

### Involuntary Removal or Demotion

Involuntary removal/demotion of a contributor happens when responsibilities and requirements aren't being met. This may include repeated patterns of inactivity, extended period of inactivity, a period of failing to meet the requirements of your role, and/or a violation of the Code of Conduct. This process is important because it protects the community and its deliverables while also opens up opportunities for new contributors to step in.

Involuntary removal or demotion is handled through a vote by a majority of the current Maintainers.

### Stepping Down/Emeritus Process

If and when Contributors' commitment levels change, Contributors can consider stepping down (moving down the Contributor ladder) vs moving to emeritus status (completely stepping away from the project).

Contact the Maintainers about changing to Emeritus status, or reducing your contributor level. An Emeritus list will be added to the [MAINTAINERS.md](MAINTAINERS.md)
