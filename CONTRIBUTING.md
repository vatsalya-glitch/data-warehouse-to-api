# Contributing

Thank you for your interest in contributing! This document outlines guidelines and expectations.

## Reporting Issues

### Bug Reports
When reporting a bug, include:
- **Environment**: Warehouse type, serving database, versions
- **Steps to reproduce**: Clear instructions to trigger the bug
- **Expected behavior**: What should happen
- **Actual behavior**: What actually happened
- **Config**: Relevant portions of your config (sanitized of secrets)

### Feature Requests
Describe:
- **Problem**: What need does this solve?
- **Proposed solution**: How you imagine it working
- **Alternatives considered**: Other approaches you thought about
- **Use case**: Concrete example of when this would help

## Making Code Changes

### Before You Start
- Check open issues and PRs to avoid duplicate work
- Open an issue for major changes to discuss approach first
- For small fixes, feel free to open a PR directly

### Setup
1. Fork the repository
2. Clone your fork: `git clone https://github.com/YOUR_GITHUB_USERNAME/data-warehouse-to-api.git`
3. Create a feature branch: `git checkout -b feature/your-feature-name`

### Code Style
- **SQL**: Follow the conventions in `examples/` — uppercase keywords, clear indentation
- **Python/Other**: Follow PEP 8 and project conventions
- **Docs**: Keep examples and documentation up-to-date with code changes

### Testing
- If adding SQL examples, test against a sample warehouse/database setup
- If modifying documentation, check that links and references still work
- For new features, update the reliability checklist if applicable

### Commit Messages
- Use present tense: "Add feature" not "Added feature"
- Be descriptive: "Add retry logic for staging table import" not "Fix"
- Reference issues: "Fixes #123" when closing an issue
- Keep first line under 72 characters

### Pull Request Guidelines

**Before opening:**
- Rebase onto the latest `main` branch
- Run final checks on documentation and examples
- Test your changes against the reliability checklist where applicable

**PR title**: Concise, present tense. Examples:
- "Add dry-run validation for Phase 1"
- "Document atomic swap transaction requirements"
- "Fix typo in config schema"

**PR description**: Include:
- **What**: What does this change do?
- **Why**: Why is this change needed?
- **Checklist**: 
  - [ ] Tests added/updated (if applicable)
  - [ ] Documentation updated
  - [ ] Examples updated (if applicable)
  - [ ] Reliability checklist reviewed

### What Gets Merged
We welcome:
- **Bug fixes** with clear reproduction and fix explanation
- **Documentation improvements** that clarify the pattern or add examples
- **New examples** for additional warehouse/serving database combinations
- **Configuration enhancements** that make the pattern more flexible
- **Reliability improvements** that strengthen the pattern

We're cautious about:
- **Major architectural changes**: Discuss in an issue first
- **New dependencies**: Prefer approaches that don't add dependencies
- **Breaking changes**: Must be discussed and justified

## Documentation Contributions

### Adding a New Example
1. Create a new file in `examples/` describing your warehouse + serving database combo
2. Follow the pattern in `examples/bigquery_postgres_example.sql`:
   - Clear Phase 1, 2, 3 sections
   - Comments explaining warehouse-specific features
   - Inline Python/pseudocode where orchestration is needed
3. Update `README.md` to reference the new example
4. Open a PR with a description of the warehouse/database combo

### Improving Docs
- Clarifications and typo fixes: Open a PR directly
- Major restructuring: Open an issue for discussion first
- New sections: Discuss in an issue before writing

## Code of Conduct

- Be respectful and inclusive
- Assume good intent in discussions
- Provide constructive feedback
- Keep conversations focused on the project

## Questions?

- Check existing issues and documentation first
- Open an issue with the "question" label if you don't find an answer
- Be as specific as possible about what you're trying to do

Thank you for contributing!
