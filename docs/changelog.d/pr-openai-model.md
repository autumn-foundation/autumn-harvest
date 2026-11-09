## An OpenAI-compatible `AgentModel`, and one provider layer

The `openai` feature of `autumn-harvest-agent` adds `openai::OpenAiModel`.
It speaks the Chat Completions API at a base URL that the app sets, so one
model reaches OpenAI, OpenRouter, Ollama, vLLM, LM Studio and a LiteLLM
proxy. The design is in `docs/design/DESIGN-openai-model.md`. The guide is
`docs/agent-adapter.md`, section "An OpenAI-compatible model".

- `OpenAiModel::new(base_url, model)` takes no key, as a local server
  needs. `api_key(key)` sends a bearer key. `with_env_key` reads
  `OPENAI_API_KEY` when it is set. `header(name, value)` adds a header,
  such as the `HTTP-Referer` and `X-Title` of OpenRouter.
- `chat` sends one request. `chat_stream` streams it, and asks for the
  usage in a last chunk. A stream that breaks off is a `Transport` error.
- Tool calls go out with their arguments as a JSON string. A string that is
  not JSON is a `Decode` error, so no tool runs on a value that the model
  did not send. A missing call id becomes `call_{index}`.
- `model_id` is `{model}@{host}`, so two servers do not share cache entries.
  It passes the cost ledger check. `price(Price)` prices each call. The
  default is unpriced.
- A draw goes as `seed`, so each draw is its own sample.

The new `provider` module holds what the two models share: the transport,
the error type, the body cap, the status table, the Server-Sent Events
framing and `Price`. The `anthropic` module uses it, and its public paths
are unchanged through re-exports.

`autoresearch` and `agent-mesh` take `--model openai` with `--base-url` and
`--openai-model`. `recall-eval` takes `--base-url`.

`litellm-rs` as a library was rejected: it is a gateway with 37 required
dependencies. A LiteLLM proxy serves the same API, so `OpenAiModel` reaches
it through its base URL.

Tests: `cargo test -p autumn-harvest-agent --features anthropic,openai`,
which runs both clients against a local HTTP server, with no key.
