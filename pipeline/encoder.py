from __future__ import annotations

from typing import Optional, Protocol, TypeVar, runtime_checkable

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
    encoders: Optional[tuple[Encoder]] = None
    serializer: Optional[Serializer] = cloudpickle
    compressor: Optional[Compressor] = zstandard
    encrypter: Optional[Encrypter] = None

    @classmethod
    def encode(cls, value: T) -> bytes:
        """Encode the result of Task.run to bytes.

        Default encoder:
            encode -> ... -> encode -> dumps -> compress -> encrypt
        """
        if cls.encoders is not None:
            for encoder in cls.encoders:
                value = encoder.encode(value)
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
            decrypt -> decompress -> loads -> decode -> ... -> decode
        """
        if cls.encrypter is not None:
            value = cls.encrypter.decrypt(value)
        if cls.compressor is not None:
            value = cls.compressor.decompress(value)
        if cls.serializer is not None:
            value = cls.serializer.loads(value)
        if cls.encoders is not None:
            for encoder in cls.encoders:
                value = encoder.decode(value)
        return value

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()

        # Validate encoders, serializer, compressor, encrypter
        if cls.encoders is not None:
            if not isinstance(cls.encoders, tuple):
                raise TypeError(
                    f"{cls}.encoders must be a tuple[Encoder, ...] or None."
                )

            for encoder in cls.encoders:
                if not isinstance(encoder, Encoder):
                    raise TypeError(
                        f"{encoder} in {cls} must implement encode and decode."
                    )

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
