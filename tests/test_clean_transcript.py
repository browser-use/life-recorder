import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "receiver"))
from receiver import clean_transcript


class CleanTranscriptTests(unittest.TestCase):
    def test_keeps_speech(self):
        self.assertEqual(clean_transcript("Let's shoot here, okay?"), "Let's shoot here, okay?")

    def test_strips_caption_and_keeps_speech(self):
        self.assertEqual(
            clean_transcript("(door knocking) Let's shoot here, okay? (phone beeping)"),
            "Let's shoot here, okay?",
        )

    def test_drops_allowlist_misses(self):
        for caption in (
            "(wind blowing)",
            "(footsteps) (door slams) (airplane engine whirring)",
            "(soft music)",
            "(audience chattering)",
            "(man speaking in foreign language)",
            "[inaudible]",
            "♪ hummed melody ♪",
        ):
            with self.subTest(caption=caption):
                self.assertEqual(clean_transcript(caption), "")

    def test_keeps_old_allowlist_cases(self):
        self.assertEqual(clean_transcript("(music) Hello there."), "Hello there.")
        self.assertEqual(clean_transcript("[noise] Hello there."), "Hello there.")

    def test_drops_repetitive_hallucination(self):
        self.assertEqual(clean_transcript("you you you you"), "")


if __name__ == "__main__":
    unittest.main()
