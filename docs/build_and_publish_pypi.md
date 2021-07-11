# Build and Publish package to PyPI repository

## Build

Run build script in this repository.
If you encounted `already exists` errors, You have to remove `build` and `dist` directories before building.

```sh
git checkout vX.X.X
bash tools/build_package.sh
```

## Publish

After build package, publish to PyPI repository.
You should publish to Test PyPI repository first.

### Test

Upload to test repository.

```sh
twine upload --repository-url https://test.pypi.org/legacy/ dist/*
```

Then check to be able to install.

```sh
pip install --upgrade --index-url https://test.pypi.org/simple/ scopt
```

### Production

```sh
twine upload dist/*
```
