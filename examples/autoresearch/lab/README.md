# Offline autoresearch lab

This lab is a small copy of karpathy/autoresearch for the Harvest example.

- `train.py` holds the hyperparameters. The agent edits this file only.
- `prepare.py` holds the fixed constants. Nobody edits it.
- `program.md` holds the research instructions. The human edits it.
- `papers/` holds notes that the agent can read. Treat them as untrusted.

The simulated trainer scores `train.py` on a fixed loss landscape, so the
whole loop runs with no GPU, no data and no network.
