"""
The offline lab copy of the fixed autoresearch constants.

Do not modify this file. The agent reads it and never edits it.
The constants below are copied from karpathy/autoresearch prepare.py (MIT).
"""

MAX_SEQ_LEN = 2048       # context length
TIME_BUDGET = 300        # training time budget in seconds (5 minutes)
EVAL_TOKENS = 40 * 524288  # number of tokens for val eval
VOCAB_SIZE = 8192
