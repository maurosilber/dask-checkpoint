from dataclasses import dataclass
from typing import Dict

import numpy as np

from .core import LocalTarget


@dataclass
class NpyTarget(LocalTarget):
    extension: str = ".npy"
    mmap_mode: str = "r"
    allow_pickle: bool = False

    def save(self, result: np.ndarray):
        self.make_parents()
        np.save(self.path, result, allow_pickle=self.allow_pickle)

    def load(self) -> np.ndarray:
        return np.load(
            self.path, mmap_mode=self.mmap_mode, allow_pickle=self.allow_pickle
        )


@dataclass
class NpzTarget(LocalTarget):
    extension: str = ".npz"
    compressed: bool = True

    def save(self, result: Dict[str, np.ndarray]):
        self.make_parents()
        if self.compressed:
            np.savez_compressed(self.path, **result)
        else:
            np.savez(self.path, **result)

    def load(self) -> Dict[str, np.ndarray]:
        return dict(np.load(self.path))
