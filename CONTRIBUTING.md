# Contributing Guide

Thank you for considering contributing to this project! To ensure consistency and maintain quality across all services, please follow the standards below.

---

## 📦 Project Structure

- Code lives in logical modules under `/app` or relevant directories
- Tests live in `/tests`, and test files/functions should be prefixed with `test_`

---

## 🔧 Development Setup

1. Clone the repo
2. Create and activate a virtual environment:
    ```bash
    python3.11 -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt
    ```
3. Use the `Makefile` for common tasks:
    ```bash
    make test      # run tests
    make lint      # run flake8 + mypy
    make format    # auto-format code
    ```

---

## 🎨 Coding Standards

We use the following tools and conventions:

- **Black** for code formatting
- **Isort** for import ordering
- **Flake8** for linting
- **Mypy** for type checking
- **Pytest** for testing

Run `make format` and `make lint` before opening a PR.

---

## ✅ Git and PR Conventions

- Create a feature branch (`feature/some-feature`) or fix branch (`fix/some-bug`)
- Open a pull request to `main` or `dev`
- Write clear, concise PR titles and descriptions
- Reference related issues (if any)
- Tag reviewers if applicable

---

## 🔍 Code Review Guidelines

- Code should be easy to read, testable, and follow existing conventions
- Avoid large PRs when possible — keep them focused and reviewable
- Leave comments in your PR for anything unusual or worth explaining
- Ensure CI passes before requesting review

---

## 🧪 Writing Tests

- All new features should include tests
- Use `pytest` and place tests in the `tests/` folder
- Prefer functional or integration tests that simulate real use

---

## 🧼 Commit Messages (optional but encouraged)

Follow conventional commit format:

