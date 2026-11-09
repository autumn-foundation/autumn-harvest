# recall-eval

Measure what a durable agent forgets across ops (issue #2455).

The eval runs each item of the built-in fixture under four memory configs:
the window only, progress notes, store memory, and episodes. It scores the last answer of
each item with `ExactMatch` and prints a Markdown section.

Run it on the scripted model. It needs no key and no network:

```sh
cargo run -p recall-eval -- --scripted
```

Run it on Claude. Each call costs money:

```sh
ANTHROPIC_API_KEY=... cargo run -p recall-eval -- --model claude-opus-5-5 --db recall.db
```

Run it on any OpenAI-compatible server with `--base-url`. `--model` then
names a model of that server. A key in `OPENAI_API_KEY` is sent when it is
set:

```sh
cargo run -p recall-eval -- --base-url http://localhost:11434/v1 --model qwen2.5:7b
```

Add the printed section to `docs/performance-agent-recall.md`. A rerun with
the same `--db` file and the same inputs calls no model for a finished item.
A failed item stays failed on that file. After a crash, run `memory` and
`episodes` again on a new file, because their stores are in memory.
