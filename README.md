
# The Hypercomm Aggregator

## Introduction
This is the aggregation/aggregator component of the Hypercomm set of libraries for Charm++. It is currently limited to aggregating messages for `group` and `nodegroup` chare-collectives, but eventually it will be extended to chare-arrays as well. To support chare-arrays, some degree of topological awareness and/or location manager information will be necessary; towards that end, the `VirtualRouter` component of TRAM could be incorporated into this library.

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
