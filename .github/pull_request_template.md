 # Pull Request Merge Checklist
           
Task backlog identifier related to this PR: ---

---

## As a DEVELOPER, I confirm that this PR **doesn't contain**:
- [ ] Code lines with syntax errors
- [ ] Methods and variables names that aren't clear or doesn't mean anything
- [ ] Code that is out of the standard (Example: **PEP-8** for Python code)
- [ ] Files with **imports** errors or without any utility
- [ ] **TODOs** left behind
- [ ] Tests that are being skipped
- [ ] Methods placed on incorrect files or folders (Example: methods that are local, but should be placed on /utils)
- [ ] Functions, methods or API responses that are publicly returning sensitive information

## As a DEVELOPER, I confirm that I:
- [ ] Followed the Simbiose's standard for branch and commit naming
- [ ] Concluded the task and achieved the goals according to the task planning I defined before starting to code
- [ ] Added all necessary database changes to the [SQL Migrations](https://github.com/s1mbi0se/sql-migrations)
- [ ] Created tests for every new flow and method on this Pull Request
- [ ] Added the necessary DEBUG logs
- [ ] Checked my code using the Simbiose's Corner Cases Guide
- [ ] Merged the **develop** branch to this PR/branch before sending the PR for review
- [ ] Solved all possible merge conflicts before sending the PR for review
- [ ] Corrected all the problems and code smells identified by the quality tools
- [ ] Manually executed the necessary tests locally to make sure my changes didn't break anything
- [ ] All unitary and integration tests ran by the CI system are passing
- [ ] Check the performance of my changes or additions and they didn't make the system slower (I profiled the code)
- [ ] Updated the necessary documentation (Example: OpenAPI, Docstrings, Metadata Database Modeling)
- [ ] Made the self-review of the code before sending the PR for review
- [ ] Sent this PR for a peer-review
- [ ] Made all the corrections appointed during the peer-review
- [ ] Confirmed that this PR merge will not cause other systems or dependencies to stop working (Example: ShannonDB Python Client and ShannonDB)
- [ ] Checked all GitHub Actions related to this PR that can block my merge

---  

## As the TEAM MANAGER, I confirm that I:
- [ ] Checked that the code was developed following the best practices for the language, with the best possible readability and using the less possible quantity of code lines and complexity. 
- [ ] Checked if the Pull Request and commit messages follow the Simbiose’s defined standard 
- [ ] Checked if there is no possible merge conflicts before reviewing the PR
- [ ] Checked if the purpose of the task was reached with this PR
- [ ] Checked that the code was developed following the best practices for the language, with the best possible readability and using the less possible quantity of code lines and complexity. 
- [ ] Checked if all related PRs were created when other components were affected by this task (e.g. database changes on [SQL Migrations](https://github.com/s1mbi0se/sql-migrations))
- [ ] Checked that were created unit, integration and chaos tests for every new flow and method on this Pull Request
- [ ] Checked that no sensible information (e.g. users security data, internal security data, etc) was exposed by this PR
- [ ] Checked that code coverage for this PR is higher than 80%.
- [ ] Checked that there are no problems and code smells identified by the quality and security tools
- [ ] Checked that all files were created on the proper folders according to the Simbiose’s defined standard
- [ ] Checked that all classes, methods and variables on this PR follow the Simbiose’s pattern for this programming language
- [ ] Checked that all unitary and integration tests ran by the CI system are passing
- [ ] Checked that there are no performance issues created by these changes or additions
- [ ] Checked that all documentation is up to date according to this change (Example: OpenAPI, Docstrings, Metadata Database Modeling)
- [ ] Checked that this PR merge will not cause other systems or dependencies to stop working (Example: ShannonDB Python Client and ShannonDB)
- [ ] Checked all GitHub Actions related to this PR that can block the merge

---  

## As the QUALITY RESPONSIBLE, I confirm that I:
- [ ] Checked if the Pull Request and commit messages follow the Simbiose’s defined standard 
- [ ] Checked if there is no possible merge conflicts before reviewing the PR
- [ ] Checked that were created unit, integration and chaos tests for every new flow and method on this Pull Request
- [ ] Checked that no sensible information (e.g. users security data, internal security data, etc) was exposed by this PR
- [ ] Checked that code coverage for this PR is higher than 80%.
- [ ] Checked that there are no problems and code smells identified by the quality and security tools
- [ ] Checked that all files were created on the proper folders according to the Simbiose’s defined standard
- [ ] Checked that all classes, methods and variables on this PR follow the Simbiose’s code standard for this programming language
- [ ] Checked that all unitary and integration tests ran by the CI system are passing
- [ ] Checked that there are no performance issues created by these changes or additions
- [ ] Checked that all documentation is up to date according to this change (Example: OpenAPI, Docstrings, Metadata Database Modeling)
- [ ] Checked that this PR merge will not cause other systems or dependencies to stop working (Example: ShannonDB Python Client and ShannonDB)
- [ ] Checked all GitHub Actions related to this PR that can block the merge


## Important links:
1. [Simbiose Quality Guidelines](https://github.com/s1mbi0se/company/wiki/Quality-Topics#table-of-contents)
2. [Metadata Database Modeling](https://docs.google.com/spreadsheets/d/1Y8_k5xV86kTjgfjbi-PGsJcubUKhWlT2x8ecpnMlyR0/edit?usp=sharing)
3. [Simbiose's Corner Cases Guide](https://www.simbioseventures.com/documentos-e-guias/procedimentos-e-checklists/corner-cases-guide)
