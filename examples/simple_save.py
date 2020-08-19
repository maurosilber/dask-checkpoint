import numpy as np
from pipeline import Load, Save, Task, dependency
from pipeline.targets.core import DictTarget


class Image(Task):
    """Load image from path."""

    path: str

    def run(self, path) -> np.ndarray:
        print("Loading image.")
        return np.array([1, 1, 2, 3, 5])  # We didn't really load anything


class Background(Task):
    image: np.ndarray

    def run(self, image) -> float:
        print("Calculating background.")
        return np.min(image)

    @property
    def target(self):
        return DictTarget(self.key)


class CorrectedImage(Task):
    image: np.ndarray

    def run(self, image, background) -> np.ndarray:
        print("Correcting image.")
        return image - background

    @dependency
    def background(self) -> float:
        return Background(image=self.image)


if __name__ == "__main__":
    image = Image(path="image.npy")

    CorrectedImage(image=image).compute()

    print()

    with Save():  # Saves Background in DictTarget
        CorrectedImage(image=image).compute()

    print()

    with Load():  # Loads Background from DictTarget
        CorrectedImage(image=image).compute()

    print()

    # Calculates Background
    CorrectedImage(image=image).compute()
