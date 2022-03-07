---
title: throttle
type: processor
status: deprecated
categories: ["Utility"]
---

<!--
     THIS FILE IS AUTOGENERATED!

     To make changes please edit the contents of:
     lib/processor/throttle.go
-->

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

:::warning DEPRECATED
This component is deprecated and will be removed in the next major version release. Please consider moving onto [alternative components](#alternatives).
:::

Throttles the throughput of a pipeline to a maximum of one message batch per
period. This throttle is per processing pipeline, and therefore four threads
each with a throttle would result in four times the rate specified.

```yaml
# Config fields, showing default values
label: ""
throttle:
  period: 100us
```

The period should be specified as a time duration string. For example, '1s'
would be 1 second, '10ms' would be 10 milliseconds, etc.

### Alternatives

It's recommended that you use the [`rate_limit` processor](/docs/components/processors/rate_limit) instead.

## Fields

### `period`

The period to throttle to.


Type: `string`  
Default: `"100us"`  

