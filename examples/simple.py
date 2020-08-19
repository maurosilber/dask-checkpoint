import numpy as np
from dask import compute
from pipeline import Task, dependency


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

    # Automatic background from dependency
    corrected_1 = CorrectedImage(image=image).compute()
    print(corrected_1, end="\n\n")

    # Manual background
    corrected_2 = CorrectedImage(image=image, background=2).compute()
    print(corrected_2, end="\n\n")

    # Min and max of CorrectedImage (calculates CorrectedImage twice)"
    task = CorrectedImage(image=image)
    min, max = task.min().compute(), task.max().compute()
    print("(min, max) =", (min, max), end="\n\n")

    # Min and max reusing intermediate results (calculates CorrectedImage once)
    task = CorrectedImage(image=image)
    min, max = compute(task.min(), task.max())
    print("(min, max) =", (min, max))
