import sys

python = sys.version_info

if python < (3, 10):
    import typing

    import typing_extensions

    typing.ParamSpec = typing_extensions.ParamSpec

if python < (3, 8):
    import functools

    import cached_property

    functools.cached_property = cached_property.cached_property
    typing.Protocol = typing_extensions.Protocol
    typing.runtime_checkable = typing_extensions.runtime_checkable
