from __future__ import annotations

from typing import TypeVar

try:
    from typing import Protocol, runtime_checkable
except ImportError:
    from typing_extensions import Protocol, runtime_checkable

import cloudpickle
import zstandard

T = TypeVar("T")
E = TypeVar("E")


@runtime_checkable
class Encoder(Protocol[T, E]):
    def encode(x: T) -> E:
        ...

    def decode(x: E) -> T:
        ...


@runtime_checkable
class Serializer(Protocol[T]):
    def dumps(x: T) -> bytes:
        ...

    def loads(x: bytes) -> T:
        ...


@runtime_checkable
class Compressor(Protocol):
    def compress(x: bytes) -> bytes:
        ...

    def decompress(x: bytes) -> bytes:
        ...


@runtime_checkable
class Encrypter(Protocol):
    def encrypt(x: bytes) -> bytes:
        ...

    def decrypt(x: bytes) -> bytes:
        ...


class DefaultEncoder(Encoder[T, bytes]):
    __slots__ = (
        "encoders",
        "serializer",
        "compressor",
        "encrypter",
    )

    def __init__(
        self,
        *,
        encoders: tuple[Encoder] = (),
        serializer: Serializer | None = cloudpickle,
        compressor: Compressor | None = zstandard,
        encrypter: Encrypter | None = None,
    ):
        self.encoders = encoders
        self.serializer = serializer
        self.compressor = compressor
        self.encrypter = encrypter

    def encode(self, value: T) -> bytes:
        """Encode the result of Task.run to bytes.

        Default encoder:
            encode -> ... -> encode -> dumps -> compress -> encrypt
        """
        for encoder in self.encoders:
            value = encoder.encode(value)
        if self.serializer is not None:
            value = self.serializer.dumps(value)
        if self.compressor is not None:
            value = self.compressor.compress(value)
        if self.encrypter is not None:
            value = self.encrypter.encrypt(value)
        return value

    def decode(self, value: bytes) -> T:
        """Decode the result of Task.run from bytes.

        Default decoder:
            decrypt -> decompress -> loads -> decode -> ... -> decode
        """
        if self.encrypter is not None:
            value = self.encrypter.decrypt(value)
        if self.compressor is not None:
            value = self.compressor.decompress(value)
        if self.serializer is not None:
            value = self.serializer.loads(value)
        if self.encoders is not None:
            for encoder in self.encoders:
                value = encoder.decode(value)
        return value

    def __repr__(self):
        encoders = list(map(_get_name, self.encoders))
        if self.serializer is not None:
            encoders.append(f"serializer={_get_name(self.serializer)}")
        if self.compressor is not None:
            encoders.append(f"compressor={_get_name(self.compressor)}")
        if self.encrypter is not None:
            encoders.append(f"encrypter={_get_name(self.encrypter)}")
        encoders = ", ".join(encoders)
        return f"Encode({encoders})"


def _get_name(x: type | object):
    try:
        return x.__name__
    except AttributeError:
        return x.__class__.__name__
