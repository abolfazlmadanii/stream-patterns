from streampatterns.testkit.generator.seeded_event_generator import SeededEventGenerator


def test_same_seed_produces_identical_sequence() -> None:
    seed = 42
    assert SeededEventGenerator(seed).next_n(5) == SeededEventGenerator(seed).next_n(5)


def test_different_seeds_diverge() -> None:
    assert SeededEventGenerator(1).next_n(3) != SeededEventGenerator(2).next_n(3)
