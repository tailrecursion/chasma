# Chasma

A tiny transactional actor runtime in Clojure, inspired by [David McClain's Lisp Actors](https://github.com/dbmcclain/Lisp-Actors/tree/main#).

**Note: This library is in active early development, highly experimental, and subject to change.**

## Problem

Coordinating concurrent workflows typically requires explicit thread management, shared-state locks, or callback graphs. These approaches are brittle under contention and make it difficult to reason about when effects become visible. Chasma provides per-actor behaviors, transactional turns, and automatic retries so message handlers can stay pure and deterministic.

## Quick Example

```clojure
(require '[tailrecursion.chasma :as ch])

(defn counter [n]
  (fn [cmd]
    (case cmd
      :inc (ch/become! ch/*self* (counter (inc n)))
      :get (ch/*reply* n)
      nil)))

(defn demo []
  (ch/start!)
  (let [c (ch/lane (ch/spawn (counter 0)))]
    (ch/send! c :inc)
    (ch/send! c :inc)
    ;; print 2
    (println (deref (ch/ask c :get))))
  (ch/shutdown!))

;; Mutually recursive even/odd predicates (SICP-style)
(def evenA (ch/spawn))
(def oddA  (ch/spawn))

(defn even-beh
  ([n] (even-beh n ch/*sender*))
  ([n cust]
   (if (zero? n)
     (ch/send! cust true)
     (ch/send! oddA (dec n) cust))))

(defn odd-beh
  ([n] (odd-beh n ch/*sender*))
  ([n cust]
   (if (zero? n)
     (ch/send! cust false)
     (ch/send! evenA (dec n) cust))))

(ch/become! evenA even-beh oddA odd-beh)

(ch/start!)
(println @(ch/ask evenA 42)) ;; => true
(println @(ch/ask oddA 17)) ;; => true
(ch/shutdown!)
```

## API Overview

| Var | Description |
| --- | ----------- |
| `start!` | Starts the dispatcher; idempotent. |
| `shutdown!` | Stops the dispatcher and shuts down the worker pool. |
| `spawn behavior-fn` | Creates an actor whose behavior is a variadic fn. Returns an `Actor` record. |
| `lane actor` | Creates a private serialized target for an actor. |
| `send! target & msg` | Enqueues a message to an actor or lane target. |
| `become! actor new-beh ...` | Schedules one or more behavior changes. Inside a turn it buffers until commit; outside it applies atomically. |
| `ask target & msg` | Sends a request to an actor or lane target and returns a `java.util.concurrent.CompletableFuture` (supports `deref`). |
| `on-commit! & body` | Defers the body so it runs once, after the current turn commits successfully (must be called inside a turn). |

## Execution Model

- Each delivery runs inside an implicit transaction buffer. All outbound `send!` and `ask` deliveries and any number of `become!` updates made during a behavior execute only after the behavior returns successfully.
- A thrown exception or compare-and-set failure during commit discards the buffered effects and re-enqueues the message with bounded exponential backoff.
- `on-commit!` schedules irreversible effects (logging, IO, etc.) and can only be invoked inside a turn; failures before commit drop the effect entirely. Once state has committed, `on-commit!` failures are reported and do not retry the turn.
- `lane` creates a serialized target/capability for an actor. Mail sent through one lane is processed FIFO, one full delivery turn at a time; separate lanes for the same actor remain independent.
- Messages sent while an actor is executing through a lane preserve that lane as the reply capability, so callees can reply through the same serialized target.
- Work is pulled from a single queue by a fixed thread pool; actors are identity envelopes around mutable behavior atoms.

## Dynamic Vars

Available inside every behavior:

| Var | Meaning |
| --- | ------- |
| `*self*` | The actor envelope currently executing. |
| `*sender*` | Reply target/capability for the current message, such as an actor, lane, or `nil`. |
| `*reply*` | Convenience fn `(fn [v])` that sends a reply back to `*sender*`. |
| `*tx*` | Internal transaction buffer (implementation detail, provided for completeness). |

## Development

The project is built with `deps.edn`. Run the test suite with:

```bash
clojure -M:test
```

## License

MIT
