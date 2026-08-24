# go-orchestrator

An embeddable, production-oriented Saga orchestration library for Go.

Runs in-process — no separate server, database, message broker, or
workflow cluster required for core functionality. Persistence and
observability are pluggable.

**Status: early work in progress.** The project is being built
incrementally, phase by phase. At this stage only the repository scaffold
exists; there is no saga engine yet.

## Install

```bash
go get github.com/adnanlari/go-orchestrator
```

## Documentation

See [ARCHITECTURE.md](ARCHITECTURE.md) for the design and current
implementation status.

## License

MIT — see [LICENSE](LICENSE).
