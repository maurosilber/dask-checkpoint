from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, TypeVar, runtime_checkable

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


@dataclass(slots=True, frozen=True, kw_only=True)
class DefaultEncoder(Encoder[T, bytes]):
    encoders: tuple[Encoder] = ()
    serializer: Serializer | None = cloudpickle
    compressor: Compressor | None = zstandard
    encrypter: Encrypter | None = None

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
