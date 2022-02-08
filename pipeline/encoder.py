from __future__ import annotations

from typing import Generic, Optional, Protocol, TypeVar, runtime_checkable

import cloudpickle
import zstandard

T = TypeVar("T")


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


class DefaultEncoder(Generic[T]):
    serializer: Optional[Serializer] = cloudpickle
    compressor: Optional[Compressor] = zstandard
    encrypter: Optional[Encrypter] = None

    @classmethod
    def encode(cls, value: T) -> bytes:
        """Encode the result of Task.run to bytes.

        Default encoder:
            dumps -> compress -> encrypt
        """
        if cls.serializer is not None:
            value = cls.serializer.dumps(value)
        if cls.compressor is not None:
            value = cls.compressor.compress(value)
        if cls.encrypter is not None:
            value = cls.encrypter.encrypt(value)
        return value

    @classmethod
    def decode(cls, value: bytes) -> T:
        """Decode the result of Task.run from bytes.

        Default decoder:
            decrypt -> decompress -> loads
        """
        if cls.encrypter is not None:
            value = cls.encrypter.decrypt(value)
        if cls.compressor is not None:
            value = cls.compressor.decompress(value)
        if cls.serializer is not None:
            value = cls.serializer.loads(value)
        return value

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()

        # Validate serializer, compressor, encrypter
        if cls.serializer is not None and not isinstance(cls.serializer, Serializer):
            raise TypeError(
                f"{cls}.serializer must implement dumps and loads or be None."
            )

        if cls.compressor is not None and not isinstance(cls.compressor, Compressor):
            raise TypeError(
                f"{cls}.compressor must implement compress and decompress or be None."
            )

        if cls.encrypter is not None and not isinstance(cls.encrypter, Encrypter):
            raise TypeError(
                f"{cls}.encrypter must implement encrypt and decrypt or be None."
            )
