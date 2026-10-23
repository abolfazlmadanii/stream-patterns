from typing import Protocol, TypeVar

InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")


class Oracle(Protocol[InputT, OutputT]):
    """Reference hook for comparing pipeline output to trusted expectations."""

    def expected(self, inputs: list[InputT]) -> list[OutputT]:
        """Return the expected outputs for the given inputs in order."""
        ...
