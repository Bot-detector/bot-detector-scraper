# Scraper
## working with uv
creating a virtual env
```sh
uv venv .venv
```
adding a dependency
```sh
uv add <package>
```
adding a dev dependency
```sh
uv add --dev <package>
```
keeping requirements.txt up to date
```sh
uv pip compile pyproject.toml -o requirements.txt
```
