# DODO Demo

This folder includes the DODO demo.
It simulates a three-node cluster on the local machine using 3 actor systems running in different folders and with different ports.
Each actor systems employs two workers.
Logs are written to two separate files keeping the console clear.

## Preparation

1. Build the DODO algorithm and package it as jar file:

   ```bash
   sbt clean assembly
   ```

   The resulting fat jar is placed in the sbt target folder: `target/scala-2.12/dodo-assembly-<version>.jar`.

2. Copy over the jar file into the demo folder and rename it to `dodo.jar`:

   ```bash
   cp target/scala-2.12/dodo-assembly-<version>.jar demo/dodo.jar
   ```

3. Change to the demo directory and run the `reset_env.sh` script.
   It prepares the demo environment by

   - creating a folder per simulated node,
   - creating the empty log files, and
   - replacing the `dodo.jar` in each folder with the one in den root demo folder.

   ```bash
   cd demo
   ./reset-env.sh
   ```

## Running the demo

Please make sure you completed all the [preparation steps](#preparation) before continuing with the following steps:

1. Use a terminal multiplexer or open four terminal windows/panes

2. For the first three terminals tail the `dodo.log`-files of the three nodes:

   ```bash
   tail -f node<i>/dodo.log
   ```

3. Use the last terminal to start the cluster and to show the resource usage:

   ```bash
   ./start.sh && htop
   ```

4. **Optional**
   You can use the command `start-single.sh && tail -f node4/dodo.log` to start a fourth node and
   watch the logs of it.
   This node is configured to load the data and the reduced columns from the cluster and then start work stealing.

If the actor systems do not terminate or you want to stop them prematurely, use the script `kill.sh`.