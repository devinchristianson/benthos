---
title: subprocess
type: input
categories: ["Utility"]
beta: true
---

<!--
     THIS FILE IS AUTOGENERATED!

     To make changes please edit the contents of:
     lib/input/subprocess.go
-->

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

BETA: This component is experimental and therefore subject to change outside of
major version releases.

Executes a command, runs it as a subprocess, and consumes messages from it over stdout.


<Tabs defaultValue="common" values={[
  { label: 'Common', value: 'common', },
  { label: 'Advanced', value: 'advanced', },
]}>

<TabItem value="common">

```yaml
# Common config fields, showing default values
input:
  subprocess:
    name: ""
    args: []
    codec: lines
    restart_on_exit: false
```

</TabItem>
<TabItem value="advanced">

```yaml
# All config fields, showing default values
input:
  subprocess:
    name: ""
    args: []
    codec: lines
    restart_on_exit: false
    max_buffer: 65536
```

</TabItem>
</Tabs>

Messages are consumed according to a specified codec. The command is executed once and if it terminates the input also closes down gracefully. Alternatively, the field `restart_on_close` can be set to `true` in order to have Benthos re-execute the command each time it stops.

The field `max_buffer` defines the maximum message size able to be read from the subprocess. This value should be set significantly above the real expected maximum message size.

The execution environment of the subprocess is the same as the Benthos instance, including environment variables and the current working directory.

## Fields

### `name`

The command to execute as a subprocess.


Type: `string`  
Default: `""`  

```yaml
# Examples

name: cat

name: sed

name: awk
```

### `args`

A list of arguments to provide the command.


Type: `array`  
Default: `[]`  

### `codec`

The way in which messages should be consumed from the subprocess.


Type: `string`  
Default: `"lines"`  
Options: `lines`.

### `restart_on_exit`

Whether the command should be re-executed each time the subprocess ends.


Type: `bool`  
Default: `false`  

### `max_buffer`

The maximum expected size of an individual message.


Type: `number`  
Default: `65536`  

