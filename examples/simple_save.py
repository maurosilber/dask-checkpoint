import numpy as np
from pipeline import Task, dependency
from pipeline.serializer import NumpyNPY
from pipeline.storage import FSStorage


class Image(Task):
    """Load image from path."""

    @staticmethod
    def run(path) -> np.ndarray:
        print("Loading image.")
        return np.array([1, 1, 2, 3, 5])  # We didn't really load anything


class Background(Task, save=True):
    serializers = (NumpyNPY,)

    @staticmethod
    def run(image) -> float:
        print("Calculating background.")
        return np.min(image)


class CorrectedImage(Task):
    @staticmethod
    def run(image, background) -> np.ndarray:
        print("Correcting image.")
        return image - background

    @dependency
    def background(self) -> float:
        return Background(image=self.image)


if __name__ == "__main__":
    image = Image(path="image.npy")

    CorrectedImage(image=image).compute()

    print()

    # Dict-based storage
    ds = FSStorage("memory://")

    with ds:  # Saves Background
        CorrectedImage(image=image).compute()

    print()

    with ds:  # Loads Background
        CorrectedImage(image=image).compute()

    print()

    # Calculates Background
    CorrectedImage(image=image).compute()
