
# Hypercomm::Aggregation Library

## Introduction
This is the aggregation/aggregator component of the [Hypercomm suite of libraries](https://github.com/jszaday/hypercomm) for Charm++. It supports virtual routing, and chare-arrays, chare-groups, and chare-node-groups.

## Usage
To use the library, declare an `initproc` calling `aggregation::initialize` within your `.ci` file:

```cpp
namespace aggregation {
	initproc void initialize(void);
}
 ```
 
 Then, include `hypercomm/aggregation.hpp` within your C++ code.

We recommend using a `std::unique_ptr` to hold your aggregator, declaring it as such:


```cpp
std::unique_ptr<aggregator_t> aggregator;
...
aggregator = std::unique_ptr<aggregator_t>(new aggregator_t(
	/* message threshold, e.g. 100 */,
	/* flush timeout, e.g. 0.1s */,
	[this](void*  msg) { /* endpoint functionality */ },
	/* node level, true/false */
	/* ccd periodic condition, e.g. 10ms, idle, etc. */));
);
```

Send calls should go through the aggregator itself, e.g.:
	
```cpp
aggregator.send(idx, ...);
proxy->ckLocalBranch()->aggregator->send(idx, ...);
```

In node-level scenarios, the aggregator is thread-safe and does not need an external lock 😊

## Usage Notes
All PEs should initialize all their aggregators in the same order, creating one instance for each endpoint per PE (e.g., ep1 then ep2 for all PEs). This is to ensure their sequential IDs are assigned consistently. In the future, we may add an option for users to specify an endpoint ID. We recommend using a node/group or `Ckpv` variables to hold aggregators, then accessing them via the `ckLocalBranch` or `CkpvAccess` respectively. 

## Compile-Time Options

Hypercomm has a handful of compile-time configuration options:

- `HYPERCOMM_NODE_AWARE` — A boolean flag (0 or 1) that indicates whether the aggregator should be node-aware in SMP scenarios.
- `HYPERCOMM_TRACING_ON` — Enables Hypercomm's internal tracing routines, this generates various end-of-run summary statistics.
