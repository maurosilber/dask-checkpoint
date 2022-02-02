import sys

python = sys.version_info

if python < (3, 10):
    import typing

    import typing_extensions

    typing.ParamSpec = typing_extensions.ParamSpec
